//! In-process worker simulation and network-impairment primitives (`SCALE-1`).
//!
//! These building blocks let load drivers and integration tests run hundreds of
//! real [`crate::client::WorkerClient`] protocol loops inside a single process
//! against a real master without spawning child processes or polluting disk,
//! while shaping TCP streams with configurable latency, jitter, packet loss,
//! and mass-disconnect events.

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use rusty_grid_core::task::TaskId;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

const RX_CHANNEL_CAPACITY: usize = 64;
const READ_CHUNK_SIZE: usize = 16 * 1024;
const MAX_TX_QUEUE_BYTES: usize = 512 * 1024;

static GLOBAL_STREAM_SEQ: AtomicU64 = AtomicU64::new(1);

/// Configuration for running tasks through an in-process synthetic runner instead of spawning OS
/// processes or materializing sandbox directories.
#[derive(Clone)]
pub struct FakeRunnerConfig {
    /// Artificial sleep duration simulating task execution before reporting completion.
    pub execution_delay: Duration,
    /// Optional dynamic delay override in nanoseconds updated atomically between benchmark phases.
    pub shared_delay_nanos: Option<Arc<AtomicU64>>,
    /// Static stdout bytes attached to simulated task completions.
    pub stdout_payload: Vec<u8>,
    /// Optional hook invoked synchronously as soon as a simulated task starts running.
    pub on_task_started: Option<Arc<dyn Fn(TaskId) + Send + Sync>>,
}

impl Default for FakeRunnerConfig {
    fn default() -> Self {
        Self {
            execution_delay: Duration::ZERO,
            shared_delay_nanos: None,
            stdout_payload: b"ok\n".to_vec(),
            on_task_started: None,
        }
    }
}

impl FakeRunnerConfig {
    /// Creates a fake runner configuration with the given simulated execution delay.
    pub fn new(execution_delay: Duration) -> Self {
        Self {
            execution_delay,
            ..Self::default()
        }
    }

    /// Sets the simulated execution delay for each task.
    pub fn with_execution_delay(mut self, delay: Duration) -> Self {
        self.execution_delay = delay;
        self
    }

    /// Sets a dynamic per-task delay override in nanoseconds read at task execution time.
    pub fn with_shared_delay_nanos(mut self, slot: Arc<AtomicU64>) -> Self {
        self.shared_delay_nanos = Some(slot);
        self
    }

    /// Returns the active execution delay, preferring [`Self::shared_delay_nanos`] when present.
    pub fn effective_execution_delay(&self) -> Duration {
        self.shared_delay_nanos
            .as_ref()
            .map_or(self.execution_delay, |slot| {
                Duration::from_nanos(slot.load(Ordering::Relaxed))
            })
    }

    /// Sets the static stdout payload returned by simulated task executions.
    pub fn with_stdout(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.stdout_payload = payload.into();
        self
    }

    /// Attaches a synchronous callback invoked when the runner starts a simulated task.
    pub fn with_on_task_started<F>(mut self, callback: F) -> Self
    where
        F: Fn(TaskId) + Send + Sync + 'static,
    {
        self.on_task_started = Some(Arc::new(callback));
        self
    }
}

impl fmt::Debug for FakeRunnerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeRunnerConfig")
            .field("execution_delay", &self.execution_delay)
            .field("has_shared_delay_nanos", &self.shared_delay_nanos.is_some())
            .field("stdout_bytes", &self.stdout_payload.len())
            .field("has_on_task_started", &self.on_task_started.is_some())
            .finish()
    }
}

/// Snapshot of aggregate traffic and fault counters from [`SimulatedNetworkControl`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SimulatedNetworkSnapshot {
    /// Total bytes read from underlying transports across all wrapped streams.
    pub rx_bytes: u64,
    /// Total bytes written to underlying transports across all wrapped streams.
    pub tx_bytes: u64,
    /// Number of simulated packet-loss / stall / drop events triggered.
    pub loss_events: u64,
    /// Number of [`SimulatedNetworkControl::disconnect_all`] broadcasts triggered.
    pub disconnects_triggered: u64,
    /// Number of wrapped streams opened against this controller.
    pub streams_opened: u64,
    /// Number of worker sessions currently registered and active.
    pub active_sessions: u64,
    /// Cumulative number of completed registration handshakes across connects and reconnects.
    pub total_handshakes: u64,
}

#[derive(Debug)]
struct ControlInner {
    rx_bytes: AtomicU64,
    tx_bytes: AtomicU64,
    loss_events: AtomicU64,
    disconnects_triggered: AtomicU64,
    streams_opened: AtomicU64,
    active_sessions: AtomicU64,
    total_handshakes: AtomicU64,
    disconnect_tx: watch::Sender<u64>,
}

/// Shared coordinator and byte-counter handle for a fleet of [`SimulatedLinkStream`] links.
#[derive(Debug, Clone)]
pub struct SimulatedNetworkControl {
    inner: Arc<ControlInner>,
}

/// RAII guard tracking an active registered worker session inside [`SimulatedNetworkControl`].
#[derive(Debug)]
pub(crate) struct SimulatedSessionGuard {
    control: SimulatedNetworkControl,
}

impl Drop for SimulatedSessionGuard {
    fn drop(&mut self) {
        self.control
            .inner
            .active_sessions
            .fetch_sub(1, Ordering::AcqRel);
    }
}

impl Default for SimulatedNetworkControl {
    fn default() -> Self {
        Self::new()
    }
}

impl SimulatedNetworkControl {
    /// Creates a new shared network controller with zeroed counters.
    pub fn new() -> Self {
        let (disconnect_tx, _disconnect_rx) = watch::channel(0u64);
        Self {
            inner: Arc::new(ControlInner {
                rx_bytes: AtomicU64::new(0),
                tx_bytes: AtomicU64::new(0),
                loss_events: AtomicU64::new(0),
                disconnects_triggered: AtomicU64::new(0),
                streams_opened: AtomicU64::new(0),
                active_sessions: AtomicU64::new(0),
                total_handshakes: AtomicU64::new(0),
                disconnect_tx,
            }),
        }
    }

    /// Total bytes received from underlying streams across all links sharing this controller.
    pub fn rx_bytes(&self) -> u64 {
        self.inner.rx_bytes.load(Ordering::Relaxed)
    }

    /// Total bytes transmitted to underlying streams across all links sharing this controller.
    pub fn tx_bytes(&self) -> u64 {
        self.inner.tx_bytes.load(Ordering::Relaxed)
    }

    /// Total loss events (retransmit stalls or forced connection drops) triggered across links.
    pub fn loss_events(&self) -> u64 {
        self.inner.loss_events.load(Ordering::Relaxed)
    }

    /// Number of mass-disconnect broadcasts issued via [`Self::disconnect_all`].
    pub fn disconnects_triggered(&self) -> u64 {
        self.inner.disconnects_triggered.load(Ordering::Relaxed)
    }

    /// Number of wrapped streams created under this controller.
    pub fn streams_opened(&self) -> u64 {
        self.inner.streams_opened.load(Ordering::Relaxed)
    }

    /// Number of worker sessions currently registered and active.
    pub fn active_sessions(&self) -> u64 {
        self.inner.active_sessions.load(Ordering::Acquire)
    }

    /// Cumulative number of completed registration handshakes across connects and reconnects.
    pub fn total_handshakes(&self) -> u64 {
        self.inner.total_handshakes.load(Ordering::Acquire)
    }

    /// Captures all traffic and fault counters together.
    pub fn snapshot(&self) -> SimulatedNetworkSnapshot {
        SimulatedNetworkSnapshot {
            rx_bytes: self.rx_bytes(),
            tx_bytes: self.tx_bytes(),
            loss_events: self.loss_events(),
            disconnects_triggered: self.disconnects_triggered(),
            streams_opened: self.streams_opened(),
            active_sessions: self.active_sessions(),
            total_handshakes: self.total_handshakes(),
        }
    }

    pub(crate) fn enter_session(&self) -> SimulatedSessionGuard {
        self.inner.active_sessions.fetch_add(1, Ordering::AcqRel);
        self.inner.total_handshakes.fetch_add(1, Ordering::AcqRel);
        SimulatedSessionGuard {
            control: self.clone(),
        }
    }

    /// Zeroes `rx_bytes`, `tx_bytes`, and `loss_events` for isolating subsequent test phases.
    pub fn reset_counters(&self) {
        self.inner.rx_bytes.store(0, Ordering::Relaxed);
        self.inner.tx_bytes.store(0, Ordering::Relaxed);
        self.inner.loss_events.store(0, Ordering::Relaxed);
    }

    /// Immediately severs every currently active [`SimulatedLinkStream`] bound to this controller.
    ///
    /// Streams wrapped *after* this call stay connected until a subsequent `disconnect_all()`.
    pub fn disconnect_all(&self) {
        self.inner
            .disconnects_triggered
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .disconnect_tx
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }

    fn add_rx_bytes(&self, bytes: u64) {
        self.inner.rx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn add_tx_bytes(&self, bytes: u64) {
        self.inner.tx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn inc_loss_events(&self) {
        self.inner.loss_events.fetch_add(1, Ordering::Relaxed);
    }

    fn next_stream_seq(&self) -> u64 {
        self.inner.streams_opened.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn subscribe_disconnect(&self) -> watch::Receiver<u64> {
        self.inner.disconnect_tx.subscribe()
    }
}

/// Fault injected when a simulated link chunk samples a loss event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LinkLossAction {
    /// Models TCP packet loss + retransmission recovery by adding `stall_delay` to chunk delivery.
    #[default]
    RetransmitStall,
    /// Immediately terminates the wrapped connection with `ConnectionReset`.
    DropConnection,
}

/// Link-shaping parameters applied independently to both directions of a wrapped stream.
#[derive(Debug, Clone)]
pub struct SimulatedLinkConfig {
    /// Fixed one-way propagation delay added to every chunk (round-trip delay is `2 * base_latency`).
    pub base_latency: Duration,
    /// Maximum uniform per-chunk jitter added on top of `base_latency` while preserving FIFO order.
    pub jitter: Duration,
    /// Probability in `[0.0, 1.0]` of injecting a loss event on each read/write chunk.
    pub loss_probability: f64,
    /// Behavior triggered when a loss event occurs.
    pub loss_action: LinkLossAction,
    /// Extra delay injected when `loss_action` is [`LinkLossAction::RetransmitStall`].
    pub stall_delay: Duration,
    /// Deterministic PRNG seed mixed with the per-stream sequence number.
    pub seed: u64,
    /// Optional fleet controller for byte accounting and mass disconnects.
    pub control: Option<SimulatedNetworkControl>,
}

impl Default for SimulatedLinkConfig {
    fn default() -> Self {
        Self {
            base_latency: Duration::ZERO,
            jitter: Duration::ZERO,
            loss_probability: 0.0,
            loss_action: LinkLossAction::RetransmitStall,
            stall_delay: Duration::from_millis(50),
            seed: 0x9E37_79B9_7F4A_7C15,
            control: None,
        }
    }
}

impl SimulatedLinkConfig {
    /// Sets the fixed one-way latency per direction.
    pub fn with_base_latency(mut self, base_latency: Duration) -> Self {
        self.base_latency = base_latency;
        self
    }

    /// Sets the maximum uniform jitter added per chunk while preserving FIFO delivery.
    pub fn with_jitter(mut self, jitter: Duration) -> Self {
        self.jitter = jitter;
        self
    }

    /// Sets the per-chunk loss probability, clamped to `[0.0, 1.0]`.
    pub fn with_loss_probability(mut self, prob: f64) -> Self {
        self.loss_probability = if prob.is_nan() {
            0.0
        } else {
            prob.clamp(0.0, 1.0)
        };
        self
    }

    /// Selects whether loss events stall the stream or reset the connection.
    pub fn with_loss_action(mut self, action: LinkLossAction) -> Self {
        self.loss_action = action;
        self
    }

    /// Sets the stall duration added on [`LinkLossAction::RetransmitStall`].
    pub fn with_stall_delay(mut self, stall_delay: Duration) -> Self {
        self.stall_delay = stall_delay;
        self
    }

    /// Sets the deterministic seed for jitter and loss sampling.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Attaches a shared [`SimulatedNetworkControl`] handle for counters and mass disconnects.
    pub fn with_control(mut self, control: SimulatedNetworkControl) -> Self {
        self.control = Some(control);
        self
    }
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_f64(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64) * (1.0 / ((1u64 << 53) as f64))
    }

    fn uniform_duration(&mut self, max: Duration) -> Duration {
        let max_nanos = max.as_nanos().min(u64::MAX as u128);
        if max_nanos == 0 {
            return Duration::ZERO;
        }
        let span = max_nanos + 1;
        let scaled = ((self.next_u64() as u128).saturating_mul(span)) >> 64;
        Duration::from_nanos(scaled as u64)
    }
}

struct DirectionImpairment {
    config: SimulatedLinkConfig,
    rng: SplitMix64,
    last_release: Option<Instant>,
}

impl DirectionImpairment {
    fn new(config: SimulatedLinkConfig, stream_seed: u64) -> Self {
        Self {
            config,
            rng: SplitMix64::new(stream_seed),
            last_release: None,
        }
    }

    fn schedule_chunk(&mut self, now: Instant) -> Result<Option<Instant>, ()> {
        let mut extra = Duration::ZERO;
        if self.config.loss_probability > 0.0 && self.rng.next_f64() < self.config.loss_probability
        {
            if let Some(ref ctrl) = self.config.control {
                ctrl.inc_loss_events();
            }
            match self.config.loss_action {
                LinkLossAction::DropConnection => return Err(()),
                LinkLossAction::RetransmitStall => {
                    extra = self.config.stall_delay;
                }
            }
        }

        let delay =
            self.config.base_latency + self.rng.uniform_duration(self.config.jitter) + extra;
        // Fast path avoids creating timer state when zero delay is configured and nothing is queued.
        if delay.is_zero() && self.last_release.is_none_or(|last| last <= now) {
            self.last_release = Some(now);
            return Ok(None);
        }

        let target = now + delay;
        // Clamp to last_release so per-chunk jitter delays never reorder FIFO byte streams.
        let release_at = match self.last_release {
            Some(last) => target.max(last),
            None => target,
        };
        self.last_release = Some(release_at);
        Ok((release_at > now).then_some(release_at))
    }
}

#[derive(Default)]
struct TxShared {
    chunks: VecDeque<(Vec<u8>, u64)>,
    queued_bytes: usize,
    written_seq: u64,
    flushed_seq: u64,
    flush_requested: bool,
    shutdown_requested: bool,
    shutdown_done: bool,
    error: Option<(io::ErrorKind, String)>,
    caller_waker: Option<Waker>,
}

impl TxShared {
    fn set_error(&mut self, kind: io::ErrorKind, msg: String) {
        if self.error.is_none() {
            self.error = Some((kind, msg));
        }
        if let Some(waker) = self.caller_waker.take() {
            waker.wake();
        }
    }

    fn take_error(&self) -> Option<io::Error> {
        self.error
            .as_ref()
            .map(|(kind, msg)| io::Error::new(*kind, msg.clone()))
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Async stream wrapper applying latency, jitter, packet loss, byte counters, and instant
/// disconnect injection to any underlying `AsyncRead + AsyncWrite` transport.
pub struct SimulatedLinkStream {
    rx_rx: mpsc::Receiver<io::Result<Vec<u8>>>,
    rx_buf: Vec<u8>,
    rx_pos: usize,
    rx_eof: bool,
    tx_shared: Arc<Mutex<TxShared>>,
    tx_notify: Arc<Notify>,
    disconnected: Arc<AtomicBool>,
    _rx_task: AbortOnDrop,
    _tx_task: AbortOnDrop,
}

impl fmt::Debug for SimulatedLinkStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimulatedLinkStream")
            .field("disconnected", &self.disconnected.load(Ordering::Relaxed))
            .field("rx_eof", &self.rx_eof)
            .finish()
    }
}

impl SimulatedLinkStream {
    /// Wraps `inner` with simulated link impairment driven by background Tokio pump tasks.
    ///
    /// Must be called inside an active Tokio runtime.
    pub fn wrap<S>(inner: S, config: SimulatedLinkConfig) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let seq = config
            .control
            .as_ref()
            .map(SimulatedNetworkControl::next_stream_seq)
            .unwrap_or_else(|| GLOBAL_STREAM_SEQ.fetch_add(1, Ordering::Relaxed));

        let rx_seed = config.seed ^ seq.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let tx_seed = config.seed ^ seq.wrapping_mul(0xD6E8_FEB8_6659_FD93);

        let rx_disconnect = config
            .control
            .as_ref()
            .map(SimulatedNetworkControl::subscribe_disconnect);
        let tx_disconnect = config
            .control
            .as_ref()
            .map(SimulatedNetworkControl::subscribe_disconnect);

        let (inner_read, inner_write) = tokio::io::split(inner);
        let (rx_tx, rx_rx) = mpsc::channel(RX_CHANNEL_CAPACITY);
        let tx_shared = Arc::new(Mutex::new(TxShared::default()));
        let tx_notify = Arc::new(Notify::new());
        let disconnected = Arc::new(AtomicBool::new(false));

        let rx_impairment = DirectionImpairment::new(config.clone(), rx_seed);
        let tx_impairment = DirectionImpairment::new(config, tx_seed);

        let rx_task = tokio::spawn(run_rx_pump(
            inner_read,
            rx_tx,
            rx_impairment,
            rx_disconnect,
            Arc::clone(&disconnected),
        ));
        let tx_task = tokio::spawn(run_tx_pump(
            inner_write,
            Arc::clone(&tx_shared),
            Arc::clone(&tx_notify),
            tx_impairment,
            tx_disconnect,
            Arc::clone(&disconnected),
        ));

        Self {
            rx_rx,
            rx_buf: Vec::new(),
            rx_pos: 0,
            rx_eof: false,
            tx_shared,
            tx_notify,
            disconnected,
            _rx_task: AbortOnDrop(rx_task),
            _tx_task: AbortOnDrop(tx_task),
        }
    }
}

fn disconnect_err() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionReset,
        "simulated link disconnected",
    )
}

fn drop_connection_err(direction: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionReset,
        format!("simulated {direction} packet loss forced connection drop"),
    )
}

async fn next_disconnect(rx: &mut Option<watch::Receiver<u64>>) {
    if let Some(watcher) = rx.as_mut() {
        if watcher.changed().await.is_ok() {
            return;
        }
        *rx = None;
    }
    std::future::pending::<()>().await;
}

async fn wait_release_or_disconnect(
    release_at: Option<Instant>,
    disconnect_rx: &mut Option<watch::Receiver<u64>>,
) -> bool {
    if let Some(watcher) = disconnect_rx.as_ref() {
        if matches!(watcher.has_changed(), Ok(true)) {
            return false;
        }
    }
    let Some(deadline) = release_at else {
        return true;
    };
    let sleep = tokio::time::sleep_until(deadline);
    if disconnect_rx.is_some() {
        tokio::select! {
            biased;
            _ = next_disconnect(disconnect_rx) => false,
            _ = sleep => true,
        }
    } else {
        sleep.await;
        true
    }
}

async fn run_rx_pump<R>(
    mut inner_read: R,
    rx_tx: mpsc::Sender<io::Result<Vec<u8>>>,
    mut impairment: DirectionImpairment,
    mut disconnect_rx: Option<watch::Receiver<u64>>,
    disconnected: Arc<AtomicBool>,
) where
    R: AsyncRead + Unpin,
{
    let mut buf = vec![0u8; READ_CHUNK_SIZE];
    loop {
        let read_res = tokio::select! {
            biased;
            _ = next_disconnect(&mut disconnect_rx) => {
                disconnected.store(true, Ordering::Release);
                let _ = rx_tx.send(Err(disconnect_err())).await;
                break;
            }
            res = inner_read.read(&mut buf) => res,
        };

        match read_res {
            Ok(0) => break,
            Ok(n) => {
                if let Some(ref ctrl) = impairment.config.control {
                    ctrl.add_rx_bytes(n as u64);
                }
                let release_at = match impairment.schedule_chunk(Instant::now()) {
                    Ok(t) => t,
                    Err(()) => {
                        disconnected.store(true, Ordering::Release);
                        let _ = rx_tx.send(Err(drop_connection_err("rx"))).await;
                        break;
                    }
                };
                if !wait_release_or_disconnect(release_at, &mut disconnect_rx).await {
                    disconnected.store(true, Ordering::Release);
                    let _ = rx_tx.send(Err(disconnect_err())).await;
                    break;
                }
                let chunk = buf[..n].to_vec();
                let sent = tokio::select! {
                    biased;
                    _ = next_disconnect(&mut disconnect_rx) => {
                        disconnected.store(true, Ordering::Release);
                        false
                    }
                    res = rx_tx.send(Ok(chunk)) => res.is_ok(),
                };
                if !sent {
                    break;
                }
            }
            Err(err) => {
                let _ = rx_tx.send(Err(err)).await;
                break;
            }
        }
    }
}

async fn run_tx_pump<W>(
    mut inner_write: W,
    tx_shared: Arc<Mutex<TxShared>>,
    tx_notify: Arc<Notify>,
    mut impairment: DirectionImpairment,
    mut disconnect_rx: Option<watch::Receiver<u64>>,
    disconnected: Arc<AtomicBool>,
) where
    W: AsyncWrite + Unpin,
{
    let mut last_written_seq: u64 = 0;

    loop {
        let notified = tx_notify.notified();
        let (next_chunk, need_flush, need_shutdown, target_flush_seq) = {
            let mut guard = tx_shared.lock().expect("tx_shared mutex poisoned");
            if guard.error.is_some() {
                break;
            }
            if let Some((chunk, seq)) = guard.chunks.pop_front() {
                guard.queued_bytes = guard.queued_bytes.saturating_sub(chunk.len());
                if guard.queued_bytes < MAX_TX_QUEUE_BYTES {
                    if let Some(waker) = guard.caller_waker.take() {
                        waker.wake();
                    }
                }
                (Some((chunk, seq)), false, false, 0)
            } else {
                let need_flush = (guard.flush_requested || guard.shutdown_requested)
                    && guard.flushed_seq < last_written_seq;
                let need_shutdown = guard.shutdown_requested && !guard.shutdown_done && !need_flush;
                (None, need_flush, need_shutdown, last_written_seq)
            }
        };

        if let Some((chunk, seq)) = next_chunk {
            let release_at = match impairment.schedule_chunk(Instant::now()) {
                Ok(t) => t,
                Err(()) => {
                    disconnected.store(true, Ordering::Release);
                    let mut guard = tx_shared.lock().expect("tx_shared mutex poisoned");
                    guard.set_error(
                        io::ErrorKind::ConnectionReset,
                        drop_connection_err("tx").to_string(),
                    );
                    break;
                }
            };
            if !wait_release_or_disconnect(release_at, &mut disconnect_rx).await {
                disconnected.store(true, Ordering::Release);
                let mut guard = tx_shared.lock().expect("tx_shared mutex poisoned");
                guard.set_error(io::ErrorKind::ConnectionReset, disconnect_err().to_string());
                break;
            }

            let write_res = tokio::select! {
                biased;
                _ = next_disconnect(&mut disconnect_rx) => {
                    disconnected.store(true, Ordering::Release);
                    Err(disconnect_err())
                }
                res = inner_write.write_all(&chunk) => res,
            };
            match write_res {
                Ok(()) => {
                    if let Some(ref ctrl) = impairment.config.control {
                        ctrl.add_tx_bytes(chunk.len() as u64);
                    }
                    last_written_seq = seq;
                }
                Err(err) => {
                    let mut guard = tx_shared.lock().expect("tx_shared mutex poisoned");
                    guard.set_error(err.kind(), err.to_string());
                    break;
                }
            }
            continue;
        }

        if need_flush {
            let flush_res = tokio::select! {
                biased;
                _ = next_disconnect(&mut disconnect_rx) => {
                    disconnected.store(true, Ordering::Release);
                    Err(disconnect_err())
                }
                res = inner_write.flush() => res,
            };
            match flush_res {
                Ok(()) => {
                    let mut guard = tx_shared.lock().expect("tx_shared mutex poisoned");
                    guard.flushed_seq = guard.flushed_seq.max(target_flush_seq);
                    if guard.chunks.is_empty() && guard.flushed_seq == guard.written_seq {
                        guard.flush_requested = false;
                        if let Some(waker) = guard.caller_waker.take() {
                            waker.wake();
                        }
                    }
                }
                Err(err) => {
                    let mut guard = tx_shared.lock().expect("tx_shared mutex poisoned");
                    guard.set_error(err.kind(), err.to_string());
                    break;
                }
            }
            continue;
        }

        if need_shutdown {
            let shutdown_res = tokio::select! {
                biased;
                _ = next_disconnect(&mut disconnect_rx) => {
                    disconnected.store(true, Ordering::Release);
                    Err(disconnect_err())
                }
                res = inner_write.shutdown() => res,
            };
            let mut guard = tx_shared.lock().expect("tx_shared mutex poisoned");
            match shutdown_res {
                Ok(()) => {
                    guard.shutdown_done = true;
                    if let Some(waker) = guard.caller_waker.take() {
                        waker.wake();
                    }
                }
                Err(err) => {
                    guard.set_error(err.kind(), err.to_string());
                }
            }
            break;
        }

        tokio::select! {
            biased;
            _ = next_disconnect(&mut disconnect_rx) => {
                disconnected.store(true, Ordering::Release);
                let mut guard = tx_shared.lock().expect("tx_shared mutex poisoned");
                guard.set_error(io::ErrorKind::ConnectionReset, disconnect_err().to_string());
                break;
            }
            _ = notified => {}
        }
    }
}

impl AsyncRead for SimulatedLinkStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.disconnected.load(Ordering::Acquire) {
            return Poll::Ready(Err(disconnect_err()));
        }
        if buf.remaining() == 0 || self.rx_eof {
            return Poll::Ready(Ok(()));
        }

        loop {
            if self.rx_pos < self.rx_buf.len() {
                let n = (self.rx_buf.len() - self.rx_pos).min(buf.remaining());
                buf.put_slice(&self.rx_buf[self.rx_pos..self.rx_pos + n]);
                self.rx_pos += n;
                if self.rx_pos == self.rx_buf.len() {
                    self.rx_buf.clear();
                    self.rx_pos = 0;
                }
                return Poll::Ready(Ok(()));
            }

            match self.rx_rx.poll_recv(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    self.rx_buf = chunk;
                    self.rx_pos = 0;
                }
                Poll::Ready(Some(Err(err))) => {
                    self.rx_eof = true;
                    return Poll::Ready(Err(err));
                }
                Poll::Ready(None) => {
                    self.rx_eof = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {
                    if self.disconnected.load(Ordering::Acquire) {
                        return Poll::Ready(Err(disconnect_err()));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

impl AsyncWrite for SimulatedLinkStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.disconnected.load(Ordering::Acquire) {
            return Poll::Ready(Err(disconnect_err()));
        }
        let mut guard = self.tx_shared.lock().expect("tx_shared mutex poisoned");
        if let Some(err) = guard.take_error() {
            return Poll::Ready(Err(err));
        }
        if guard.shutdown_requested {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "simulated link already shut down",
            )));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if guard.queued_bytes >= MAX_TX_QUEUE_BYTES {
            guard.caller_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let next_seq = guard.written_seq.saturating_add(1);
        guard.written_seq = next_seq;
        guard.queued_bytes += buf.len();
        guard.chunks.push_back((buf.to_vec(), next_seq));
        drop(guard);

        self.tx_notify.notify_one();
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.disconnected.load(Ordering::Acquire) {
            return Poll::Ready(Err(disconnect_err()));
        }
        let mut guard = self.tx_shared.lock().expect("tx_shared mutex poisoned");
        if let Some(err) = guard.take_error() {
            return Poll::Ready(Err(err));
        }
        if guard.chunks.is_empty() && guard.flushed_seq == guard.written_seq {
            return Poll::Ready(Ok(()));
        }
        guard.flush_requested = true;
        guard.caller_waker = Some(cx.waker().clone());
        drop(guard);

        self.tx_notify.notify_one();
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.disconnected.load(Ordering::Acquire) {
            return Poll::Ready(Err(disconnect_err()));
        }
        let mut guard = self.tx_shared.lock().expect("tx_shared mutex poisoned");
        if let Some(err) = guard.take_error() {
            return Poll::Ready(Err(err));
        }
        if guard.shutdown_done {
            return Poll::Ready(Ok(()));
        }
        guard.shutdown_requested = true;
        guard.caller_waker = Some(cx.waker().clone());
        drop(guard);

        self.tx_notify.notify_one();
        Poll::Pending
    }
}
