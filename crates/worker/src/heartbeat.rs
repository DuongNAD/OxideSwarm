//! Heartbeat subsystem for periodic liveness reporting and lock-free load telemetry.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, trace};
use uuid::Uuid;

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
static TELEMETRY_INITIALIZED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn ensure_telemetry_sampler_started() {
    if TELEMETRY_INITIALIZED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        std::thread::spawn(|| {
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
    }
}

fn get_system_telemetry() -> (f32, u64) {
    ensure_telemetry_sampler_started();
    if let Ok(cache) = TELEMETRY_CACHE.lock() {
        *cache
    } else {
        (0.0, 8192)
    }
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

                    tracker_clone.record_sent(now_sec);

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

                    if outbound_tx.send(msg).await.is_err() {
                        debug!(worker_id = %worker_id, "Outbound message channel closed; terminating heartbeat task");
                        break;
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
