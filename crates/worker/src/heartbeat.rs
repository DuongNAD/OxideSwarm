//! Heartbeat subsystem for periodic liveness reporting and lock-free load telemetry.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, trace};
use uuid::Uuid;

use rusty_grid_core::capabilities::{MobileCapabilities, MobileLiveStatus};
use rusty_grid_core::WorkerMessage;

/// Returns current system wall-clock epoch timestamp in seconds.
pub fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Lock-free metrics and telemetry for worker heartbeat tracking.
#[derive(Debug, Default)]
pub struct HeartbeatTracker {
    /// Number of tasks currently executing on this worker (shared with runner).
    pub active_tasks: Arc<AtomicUsize>,
    /// UNIX timestamp in seconds of the last emitted heartbeat.
    pub last_sent_sec: AtomicU64,
    /// UNIX timestamp in seconds of the last received HeartbeatAck from master.
    pub last_ack_sec: AtomicU64,
    /// Total count of sent heartbeats across session.
    pub total_sent: AtomicU64,
    /// Total count of acknowledged heartbeats across session.
    pub total_acks: AtomicU64,
    /// Estimated round-trip latency in milliseconds.
    pub last_latency_ms: AtomicU64,
}

impl HeartbeatTracker {
    pub fn new(active_tasks: Arc<AtomicUsize>) -> Self {
        Self {
            active_tasks,
            last_sent_sec: AtomicU64::new(0),
            last_ack_sec: AtomicU64::new(0),
            total_sent: AtomicU64::new(0),
            total_acks: AtomicU64::new(0),
            last_latency_ms: AtomicU64::new(0),
        }
    }

    /// Records transmission of a heartbeat message.
    pub fn record_sent(&self, now_sec: u64) {
        self.last_sent_sec.store(now_sec, Ordering::Release);
        self.total_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Records receipt of a HeartbeatAck, calculating round-trip latency.
    pub fn record_ack(&self, ack_timestamp: u64, now_sec: u64) {
        self.last_ack_sec.store(now_sec, Ordering::Release);
        self.total_acks.fetch_add(1, Ordering::Relaxed);

        if now_sec >= ack_timestamp {
            let latency_sec = now_sec - ack_timestamp;
            self.last_latency_ms
                .store(latency_sec * 1000, Ordering::Release);
        }
    }

    /// Evaluates whether the connection to Master is healthy based on maximum silent period.
    pub fn is_healthy(&self, max_silence_secs: u64) -> bool {
        let last_ack = self.last_ack_sec.load(Ordering::Acquire);
        if last_ack == 0 {
            let last_sent = self.last_sent_sec.load(Ordering::Acquire);
            if last_sent == 0 {
                return true;
            }
            let now = current_epoch_secs();
            return now.saturating_sub(last_sent) <= max_silence_secs;
        }

        let now = current_epoch_secs();
        now.saturating_sub(last_ack) <= max_silence_secs
    }

    /// Returns seconds elapsed since last received heartbeat ack.
    pub fn seconds_since_last_ack(&self) -> u64 {
        let last_ack = self.last_ack_sec.load(Ordering::Acquire);
        if last_ack == 0 {
            return 0;
        }
        current_epoch_secs().saturating_sub(last_ack)
    }

    /// Sets the active task count directly.
    pub fn set_active_tasks(&self, count: usize) {
        self.active_tasks.store(count, Ordering::Release);
    }

    /// Atomically increments the active task count and returns the new count.
    pub fn increment_active_tasks(&self) -> usize {
        self.active_tasks.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Atomically decrements the active task count and returns the new count.
    pub fn decrement_active_tasks(&self) -> usize {
        self.active_tasks
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |val| {
                Some(val.saturating_sub(1))
            })
            .unwrap_or(0)
            .saturating_sub(1)
    }

    /// Returns the current number of active tasks.
    pub fn active_tasks(&self) -> usize {
        self.active_tasks.load(Ordering::Acquire)
    }
}

/// RAII handle for managing and cancelling the background heartbeat task.
pub struct HeartbeatHandle {
    tracker: Arc<HeartbeatTracker>,
    stop_tx: Option<oneshot::Sender<()>>,
    join_handle: JoinHandle<()>,
}

impl HeartbeatHandle {
    /// Signals the heartbeat task to stop and consumes the handle.
    pub async fn stop(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.join_handle.await;
    }

    /// Provides access to the heartbeat tracker.
    pub fn tracker(&self) -> &Arc<HeartbeatTracker> {
        &self.tracker
    }
}

static TELEMETRY_CACHE: std::sync::Mutex<(f32, u64)> = std::sync::Mutex::new((0.0, 8192));
static TELEMETRY_SAMPLER: std::sync::Once = std::sync::Once::new();

fn ensure_telemetry_sampler_started() {
    // `call_once` also makes a concurrent first caller wait until the cache is seeded.
    TELEMETRY_SAMPLER.call_once(|| {
        // Seed the cache before the first heartbeat reads it. Starting from the (0 %, 8192 MB)
        // placeholder made a freshly started worker report 8 GB free whatever the machine has,
        // and the master's RAM gate trusts that figure. (CPU needs two samples, so it starts at 0.)
        // Only memory is read here, which is cheap; the CPU sampler is set up on its own thread.
        let mut mem = sysinfo::System::new();
        mem.refresh_memory();
        if let Ok(mut cache) = TELEMETRY_CACHE.lock() {
            *cache = (0.0, mem.available_memory() / (1024 * 1024));
        }
        std::thread::spawn(move || {
            let mut sys = sysinfo::System::new_with_specifics(
                sysinfo::RefreshKind::new()
                    .with_cpu(sysinfo::CpuRefreshKind::new().with_cpu_usage())
                    .with_memory(sysinfo::MemoryRefreshKind::new().with_ram()),
            );
            loop {
                sys.refresh_cpu_specifics(sysinfo::CpuRefreshKind::new().with_cpu_usage());
                sys.refresh_memory();
                let cpu_usage_pct = sys.global_cpu_info().cpu_usage();
                let ram_available_mb = sys.available_memory() / (1024 * 1024);
                if let Ok(mut cache) = TELEMETRY_CACHE.lock() {
                    *cache = (cpu_usage_pct, ram_available_mb);
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        });
    });
}

fn get_system_telemetry() -> (f32, u64) {
    ensure_telemetry_sampler_started();
    if let Ok(cache) = TELEMETRY_CACHE.lock() {
        *cache
    } else {
        (0.0, 8192)
    }
}

/// A write of at least this many bytes still waiting to go out is bulk traffic (a task result or
/// checkpoint), as opposed to a heartbeat or a progress report.
pub const BULK_WRITE_BYTES: usize = 16 * 1024;

/// Receive/transmit activity clock of one Master connection, used to detect a peer that has
/// silently died (power loss, Wi-Fi drop, suspended laptop: no FIN, no RST).
///
/// Heartbeats written into the kernel buffer succeed against a dead peer, so *sending* them proves
/// nothing and the only reliable sign of life is bytes arriving. One exception is made for bulk
/// writes (see [`BULK_WRITE_BYTES`]): while a multi-MB task result is being uploaded the Master
/// cannot answer anything, because it has no complete heartbeat to acknowledge yet, and for as long
/// as the bytes it sent are still travelling (the sender's kernel buffer outlives the `write`
/// call, so the tail of the frame reaches the peer well after the upload "finished"). Progress of
/// a bulk write therefore counts as life, from its last accepted byte; a stalled upload stops
/// counting and is detected like any other silence.
#[derive(Debug)]
pub struct IoClock {
    epoch: Instant,
    last_rx_nanos: AtomicU64,
    last_bulk_tx_nanos: AtomicU64,
}

impl IoClock {
    /// Creates a clock whose last activity is "now" (the connection was just opened).
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            epoch: Instant::now(),
            last_rx_nanos: AtomicU64::new(0),
            last_bulk_tx_nanos: AtomicU64::new(0),
        })
    }

    fn now_nanos(&self) -> u64 {
        self.epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    /// Records that bytes were received.
    pub fn touch_rx(&self) {
        self.last_rx_nanos
            .fetch_max(self.now_nanos(), Ordering::Relaxed);
    }

    /// Records that the socket accepted part of a bulk write.
    pub fn touch_bulk_tx(&self) {
        self.last_bulk_tx_nanos
            .fetch_max(self.now_nanos(), Ordering::Relaxed);
    }

    /// Time since the last sign of life: the last received byte, or the last accepted byte of a
    /// bulk write, whichever is more recent.
    pub fn silence(&self) -> Duration {
        let last = self
            .last_rx_nanos
            .load(Ordering::Relaxed)
            .max(self.last_bulk_tx_nanos.load(Ordering::Relaxed));
        Duration::from_nanos(self.now_nanos().saturating_sub(last))
    }
}

/// `AsyncRead + AsyncWrite` adapter that stamps an [`IoClock`]: on every successful non-empty read,
/// and on every successful write that was part of a bulk frame.
pub struct StampedStream<S> {
    inner: S,
    clock: Arc<IoClock>,
}

impl<S> StampedStream<S> {
    /// Wraps `inner`, stamping `clock` as data moves.
    pub fn new(inner: S, clock: Arc<IoClock>) -> Self {
        Self { inner, clock }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for StampedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            if buf.filled().len() > before {
                self.clock.touch_rx();
            }
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for StampedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            if *n > 0 && buf.len() >= BULK_WRITE_BYTES {
                self.clock.touch_bulk_tx();
            }
        }
        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(n)) = &poll {
            if *n > 0 && bufs.iter().map(|b| b.len()).sum::<usize>() >= BULK_WRITE_BYTES {
                self.clock.touch_bulk_tx();
            }
        }
        poll
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// Live battery / thermal state of the device, shared between whoever can read it (the platform
/// app on Android, a poller on a laptop) and the running worker.
///
/// `Register` carries a snapshot taken when the worker connected. A phone that registered at 90%
/// and has drained to 4% since must stop receiving builds, so the worker re-reads this handle:
/// to gate incoming tasks locally and to tell the Master about every change
/// (`WorkerMessage::MobileTelemetry`). Cloning is cheap; all clones see the same state.
#[derive(Debug, Clone, Default)]
pub struct MobileTelemetryHandle {
    inner: Arc<RwLock<Option<MobileCapabilities>>>,
}

impl MobileTelemetryHandle {
    /// A handle starting at `initial` (`None` for a device without battery data).
    pub fn new(initial: Option<MobileCapabilities>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial)),
        }
    }

    /// The current state, `None` while nothing is known.
    pub fn snapshot(&self) -> Option<MobileCapabilities> {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Replaces the whole state (descriptors included).
    pub fn set(&self, mobile: Option<MobileCapabilities>) {
        *self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = mobile;
    }

    /// Replaces the fast-changing readings. The OS / SoC descriptors are kept; if nothing was set
    /// before, they read "unknown". This is what an app calls from its battery / thermal callbacks.
    pub fn update(&self, status: MobileLiveStatus) {
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(match guard.as_ref() {
            Some(current) => current.with_live_status(status),
            None => MobileCapabilities {
                os_version: "unknown".into(),
                soc_model: "unknown".into(),
                battery_pct: status.battery_pct,
                is_charging: status.is_charging,
                thermal_throttled: status.thermal_throttled,
            },
        });
    }

    /// The fast-changing readings alone, `None` while nothing is known.
    pub fn live_status(&self) -> Option<MobileLiveStatus> {
        self.snapshot().map(|m| m.live_status())
    }

    /// Replaces OS / SoC descriptors that are still "unknown" (an app pushed battery readings
    /// before the worker had probed the device) with the probed ones; real values are kept.
    pub fn fill_unknown_descriptors(&self, probed: &MobileCapabilities) {
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(current) = guard.as_mut() {
            if current.os_version == "unknown" {
                current.os_version = probed.os_version.clone();
            }
            if current.soc_model == "unknown" {
                current.soc_model = probed.soc_model.clone();
            }
        }
    }
}

/// Re-reads the device's battery / thermal state every `interval` with `probe` (run on a blocking
/// thread: it reads sysfs and may spawn `pmset` / `dumpsys`) and stores it in `handle`. A probe
/// that finds nothing (`None`) leaves the previous reading alone. Dropping or aborting the
/// returned task stops the polling.
pub fn spawn_mobile_poller(
    handle: MobileTelemetryHandle,
    interval: Duration,
    probe: fn() -> Option<MobileLiveStatus>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick is immediate, but the snapshot was just taken: wait one full period.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Ok(Some(status)) = tokio::task::spawn_blocking(probe).await {
                handle.update(status);
            }
        }
    })
}

/// Spawns the background periodic heartbeat sender task.
pub fn spawn_heartbeat_task(
    worker_id: Uuid,
    interval: Duration,
    outbound_tx: mpsc::Sender<WorkerMessage>,
    tracker: Arc<HeartbeatTracker>,
) -> HeartbeatHandle {
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let tracker_clone = Arc::clone(&tracker);

    let join_handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // Skip the immediate initial tick so first heartbeat is sent after interval has elapsed
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let now_sec = current_epoch_secs();
                    let active = tracker_clone.active_tasks.load(Ordering::Relaxed);

                    // Read current system telemetry from non-blocking background sampler
                    let (cpu_usage_pct, ram_available_mb) = get_system_telemetry();

                    let msg = WorkerMessage::Heartbeat {
                        worker_id,
                        timestamp: now_sec,
                        active_tasks: active,
                        cpu_usage_pct,
                        ram_available_mb,
                    };

                    trace!(
                        worker_id = %worker_id,
                        timestamp = now_sec,
                        active_tasks = active,
                        cpu_usage_pct,
                        ram_available_mb,
                        "Dispatching periodic heartbeat"
                    );

                    // Never wait for room: while the writer is stuck in a large frame the queue
                    // fills up, and a backlog of stale beats delivered in a burst afterwards says
                    // nothing the next fresh beat does not. A full queue means a beat (or other
                    // control traffic) is already waiting, so this one is simply skipped.
                    match outbound_tx.try_send(msg) {
                        Ok(()) => tracker_clone.record_sent(now_sec),
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            debug!(worker_id = %worker_id, "Control queue full; skipping this heartbeat");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            debug!(worker_id = %worker_id, "Outbound message channel closed; terminating heartbeat task");
                            break;
                        }
                    }
                }
                _ = &mut stop_rx => {
                    debug!(worker_id = %worker_id, "Heartbeat stop signal received; terminating task");
                    break;
                }
            }
        }
    });

    HeartbeatHandle {
        tracker,
        stop_tx: Some(stop_tx),
        join_handle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_heartbeat_tracker_metrics() {
        let active = Arc::new(AtomicUsize::new(3));
        let tracker = HeartbeatTracker::new(active);

        assert_eq!(tracker.total_sent.load(Ordering::Relaxed), 0);
        assert_eq!(tracker.total_acks.load(Ordering::Relaxed), 0);

        let t1 = 1000;
        tracker.record_sent(t1);
        assert_eq!(tracker.total_sent.load(Ordering::Relaxed), 1);
        assert_eq!(tracker.last_sent_sec.load(Ordering::Relaxed), t1);

        tracker.record_ack(t1, t1 + 1);
        assert_eq!(tracker.total_acks.load(Ordering::Relaxed), 1);
        assert_eq!(tracker.last_latency_ms.load(Ordering::Relaxed), 1000);
    }

    #[tokio::test]
    async fn test_spawn_heartbeat_task_fires_and_stops() {
        let active = Arc::new(AtomicUsize::new(1));
        let tracker = Arc::new(HeartbeatTracker::new(active));
        let (tx, mut rx) = mpsc::channel(16);
        let worker_id = Uuid::new_v4();

        let handle = spawn_heartbeat_task(
            worker_id,
            Duration::from_millis(30),
            tx,
            Arc::clone(&tracker),
        );

        // Receive first heartbeat
        let msg = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("timeout")
            .expect("some message");

        match msg {
            WorkerMessage::Heartbeat {
                worker_id: wid,
                active_tasks,
                ..
            } => {
                assert_eq!(wid, worker_id);
                assert_eq!(active_tasks, 1);
            }
            other => panic!("Expected Heartbeat message, got {other:?}"),
        }

        handle.stop().await;
    }

    #[tokio::test]
    async fn test_heartbeat_never_blocks_on_a_full_control_queue() {
        let active = Arc::new(AtomicUsize::new(0));
        let tracker = Arc::new(HeartbeatTracker::new(active));
        // Capacity 1 and nobody reading: the writer is stuck in a large frame.
        let (tx, mut rx) = mpsc::channel(1);
        let handle = spawn_heartbeat_task(
            Uuid::new_v4(),
            Duration::from_millis(20),
            tx,
            Arc::clone(&tracker),
        );
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Beats that found the queue full were skipped, not queued up behind it.
        assert_eq!(
            tracker.total_sent.load(Ordering::Relaxed),
            1,
            "only the beat that fitted was sent"
        );
        // And the task is still responsive: it stops promptly instead of hanging on a send.
        tokio::time::timeout(Duration::from_secs(1), handle.stop())
            .await
            .expect("heartbeat task must stay stoppable while the queue is full");
        assert!(matches!(rx.try_recv(), Ok(WorkerMessage::Heartbeat { .. })));
        assert!(rx.try_recv().is_err(), "no backlog of stale beats");
    }

    #[tokio::test]
    async fn test_heartbeat_task_ends_when_the_queue_is_closed() {
        let tracker = Arc::new(HeartbeatTracker::new(Arc::new(AtomicUsize::new(0))));
        let (tx, rx) = mpsc::channel(4);
        drop(rx);
        let handle = spawn_heartbeat_task(Uuid::new_v4(), Duration::from_millis(10), tx, tracker);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(handle.join_handle.is_finished());
        handle.stop().await;
    }

    #[test]
    fn test_telemetry_is_seeded_before_the_first_heartbeat_reads_it() {
        // `ensure_telemetry_sampler_started` fills the cache synchronously, so the first reading
        // is the machine's real free memory, never the 8192 MB placeholder for a machine that
        // does not have exactly that much.
        // (0 is legitimate: sysinfo reports no "available" figure on some macOS versions, and the
        // master treats 0 as unknown. What must not appear is a made-up number.)
        let (_, ram_mb) = get_system_telemetry();
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        let total_mb = sys.total_memory() / (1024 * 1024);
        let available_mb = sys.available_memory() / (1024 * 1024);
        assert!(
            ram_mb <= total_mb.max(1),
            "free {ram_mb} MB cannot exceed total {total_mb} MB"
        );
        // The bound above cannot catch the placeholder on a machine with more than 8 GB, so also
        // compare with a fresh reading: the cached one is at most 500 ms old and stays close,
        // while the placeholder is far off unless the machine happens to have about 8 GB free.
        let tolerance_mb = (total_mb / 4).max(1024);
        assert!(
            available_mb == 0 || ram_mb.abs_diff(available_mb) <= tolerance_mb,
            "first reading of {ram_mb} MB free is far from the {available_mb} MB actually free"
        );
    }

    #[tokio::test]
    async fn test_io_clock_counts_received_bytes_and_bulk_write_progress_only() {
        let clock = IoClock::new();
        tokio::time::sleep(Duration::from_millis(60)).await;
        let idle = clock.silence();
        assert!(
            idle >= Duration::from_millis(50),
            "idle connection ages: {idle:?}"
        );

        // Received bytes are life.
        clock.touch_rx();
        assert!(clock.silence() < Duration::from_millis(40));

        // Bulk upload progress is life too, until it stalls.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(clock.silence() >= Duration::from_millis(50));
        clock.touch_bulk_tx();
        assert!(clock.silence() < Duration::from_millis(40));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            clock.silence() >= Duration::from_millis(50),
            "a stalled upload stops counting"
        );
    }

    #[tokio::test]
    async fn test_stamped_stream_stamps_reads_and_only_bulk_writes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (a, mut b) = tokio::io::duplex(256 * 1024);
        let clock = IoClock::new();
        let mut stamped = StampedStream::new(a, Arc::clone(&clock));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(clock.silence() >= Duration::from_millis(50));

        // Reading data from the peer is a sign of life.
        b.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        stamped.read_exact(&mut buf).await.unwrap();
        assert!(clock.silence() < Duration::from_millis(40));

        // A heartbeat-sized write succeeds into a dead peer's buffer too, so it proves nothing...
        tokio::time::sleep(Duration::from_millis(60)).await;
        stamped.write_all(b"pong").await.unwrap();
        assert!(
            clock.silence() >= Duration::from_millis(50),
            "small writes are not life"
        );

        // ...a bulk write does (the peer is draining a frame).
        stamped
            .write_all(&vec![7u8; BULK_WRITE_BYTES * 2])
            .await
            .unwrap();
        assert!(
            clock.silence() < Duration::from_millis(40),
            "bulk write progress is life"
        );
    }

    #[test]
    fn test_mobile_handle_update_keeps_descriptors_and_creates_when_empty() {
        let handle = MobileTelemetryHandle::default();
        assert!(handle.snapshot().is_none());
        assert!(handle.live_status().is_none());

        // Nothing known yet: an app's first callback creates the entry.
        handle.update(MobileLiveStatus {
            battery_pct: Some(61),
            is_charging: Some(true),
            thermal_throttled: false,
        });
        let first = handle.snapshot().unwrap();
        assert_eq!(first.battery_pct, Some(61));
        assert_eq!(first.os_version, "unknown");

        // Once descriptors are set, updates keep them.
        handle.set(Some(MobileCapabilities {
            os_version: "Android 14".into(),
            soc_model: "Tensor".into(),
            battery_pct: Some(61),
            is_charging: Some(true),
            thermal_throttled: false,
        }));
        let clone = handle.clone();
        clone.update(MobileLiveStatus {
            battery_pct: Some(3),
            is_charging: Some(false),
            thermal_throttled: true,
        });
        let snap = handle.snapshot().unwrap();
        assert_eq!(
            (snap.os_version.as_str(), snap.soc_model.as_str()),
            ("Android 14", "Tensor")
        );
        assert_eq!(
            handle.live_status().unwrap().battery_pct,
            Some(3),
            "clones share state"
        );
        assert!(snap.thermal_throttled);
    }

    fn fake_probe() -> Option<MobileLiveStatus> {
        Some(MobileLiveStatus {
            battery_pct: Some(7),
            is_charging: Some(false),
            thermal_throttled: true,
        })
    }

    fn silent_probe() -> Option<MobileLiveStatus> {
        None
    }

    #[tokio::test]
    async fn test_mobile_poller_refreshes_the_shared_reading() {
        let handle = MobileTelemetryHandle::new(Some(MobileCapabilities {
            os_version: "macOS".into(),
            soc_model: "M2".into(),
            battery_pct: Some(100),
            is_charging: Some(true),
            thermal_throttled: false,
        }));
        let poller = spawn_mobile_poller(handle.clone(), Duration::from_millis(20), fake_probe);
        tokio::time::sleep(Duration::from_millis(300)).await;
        poller.abort();
        let snap = handle.snapshot().unwrap();
        assert_eq!(
            snap.battery_pct,
            Some(7),
            "the startup snapshot must not stay frozen"
        );
        assert_eq!(snap.is_charging, Some(false));
        assert!(snap.thermal_throttled);
        assert_eq!(snap.os_version, "macOS");
    }

    #[tokio::test]
    async fn test_mobile_poller_keeps_the_old_reading_when_the_probe_finds_nothing() {
        let handle = MobileTelemetryHandle::new(Some(MobileCapabilities {
            os_version: "x".into(),
            soc_model: "y".into(),
            battery_pct: Some(42),
            is_charging: None,
            thermal_throttled: false,
        }));
        let poller = spawn_mobile_poller(handle.clone(), Duration::from_millis(10), silent_probe);
        tokio::time::sleep(Duration::from_millis(120)).await;
        poller.abort();
        assert_eq!(handle.snapshot().unwrap().battery_pct, Some(42));
    }

    #[test]
    fn test_decrement_active_tasks_saturating_no_underflow() {
        let active = Arc::new(AtomicUsize::new(0));
        let tracker = HeartbeatTracker::new(active);

        // Decrementing from 0 should stay at 0 and return 0
        assert_eq!(tracker.decrement_active_tasks(), 0);
        assert_eq!(tracker.active_tasks(), 0);

        // Increment to 2, decrement twice to 0, decrement again stays at 0
        tracker.increment_active_tasks();
        tracker.increment_active_tasks();
        assert_eq!(tracker.active_tasks(), 2);
        assert_eq!(tracker.decrement_active_tasks(), 1);
        assert_eq!(tracker.decrement_active_tasks(), 0);
        assert_eq!(tracker.decrement_active_tasks(), 0);
        assert_eq!(tracker.active_tasks(), 0);
    }
}
