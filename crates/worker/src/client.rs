//! Worker TCP client, registration handshake, and communication event loop.
//!
//! # Connection lifecycle
//!
//! One *session* is one registered connection to the Master. Its inbound frames are read by one
//! task per lane, its outbound frames are written by one task per lane, and a single loop in
//! [`WorkerClient::run`]'s helper `run_session` reacts to what arrives. That split matters:
//!
//! * A multi-megabyte `TaskResult` being written no longer stops the worker from reading
//!   (`CancelTask`, `Shutdown`, `HeartbeatAck`), and heartbeats and other control traffic always
//!   go out *between* frames ahead of queued task data (a frame already on the wire cannot be
//!   interleaved on one TCP stream; the Master counts received bytes as liveness meanwhile).
//! * A Master that vanishes without a FIN or RST is noticed by [`IoClock`]: no byte received for
//!   several heartbeat intervals (progress of a bulk upload counts as life, a stalled one does not).
//! * When a session ends, **every task still running is cancelled** and awaited before the worker
//!   reconnects. The Master binds each result to the session the task was assigned in and drops
//!   results from any other, and it has already requeued the work, so letting the old runs finish
//!   would only burn CPU (and battery) on output nobody can receive, and would collide with a
//!   re-dispatch of the same task id (same sandbox directory). Results are therefore never
//!   buffered across reconnects.
//! * A `Shutdown` frame from the Master is a disconnect, not an order for the worker to exit: the
//!   worker waits and reconnects (a restarted Master is the common case). Only the local shutdown
//!   signal ends [`WorkerClient::run`].

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use rusty_grid_core::capabilities::{detect_live_mobile_status, MobileLiveStatus, WorkerCapabilities};
use rusty_grid_core::cluster_auth::{client_handshake, ClusterSecret, WIRE_FORMAT_AUTH};
use rusty_grid_core::discovery::{is_valid_host, sanitize_host_port, sanitize_http_origin};
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::protocol::{
    master_features, MasterMessage, MessageReader, MessageTransport, MessageWriter, ProtocolError,
    WireCodec, WorkerMessage,
};
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskResult, TaskStatus};

use crate::backoff::{BackoffConfig, ExponentialBackoff};
use crate::gpu::{apply_gpu_policy, probe_real_gpu, GpuNotice, GpuRequest, NoticeLevel};
use crate::heartbeat::{
    current_epoch_secs, spawn_heartbeat_task, spawn_mobile_poller, HeartbeatTracker, IoClock,
    MobileTelemetryHandle, StampedStream,
};
use crate::identity::{default_worker_id_path, load_or_create_worker_id};
use crate::process_tree::ProcessPriority;
use crate::runner::{RunnerConfig, TaskRunner, DEFAULT_MAX_OUTPUT_BYTES, EXIT_CODE_CANCELLED};
use crate::sandbox::SandboxConfig;

/// Execution handle for an actively running task on this worker.
pub struct TaskExecutionHandle {
    pub task_id: TaskId,
    pub cancel_tx: Option<oneshot::Sender<String>>,
    pub abort_handle: tokio::task::AbortHandle,
    pub child_pid: Arc<AtomicU32>,
    pub started_at_epoch_sec: u64,
    /// Which run of `task_id` this is. A task id can be run twice on one worker (a re-dispatch
    /// after a reconnect); the old run's cleanup must only ever remove *its own* table entry.
    pub attempt: u64,
    /// When set, the run's final result is discarded instead of being sent: the run was cancelled
    /// by the worker itself (connection lost, superseded), not by the Master's `CancelTask`, and a
    /// stray exit-130 result would make the Master record the task as cancelled.
    pub suppress_result: Arc<AtomicBool>,
    /// Completion of the run's task, awaited when the run is cancelled so that its process tree,
    /// sandbox directory and concurrency permit are gone before anything replaces it.
    pub join: Option<JoinHandle<()>>,
}

/// Thread-safe registry of active tasks executing on the worker.
pub type ActiveTaskTable = Arc<RwLock<HashMap<TaskId, TaskExecutionHandle>>>;

/// Default upper bound for establishing the transport to the Master (TCP connect or P2P dial).
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Minimum pause before reconnecting after the Master refused the cluster secret (or demanded one
/// this worker does not have). Retrying faster cannot succeed with the same secret and would only
/// hammer the Master (which itself delays every failed authentication).
pub const AUTH_FAILURE_MIN_BACKOFF: Duration = Duration::from_secs(30);

/// A Master that sends nothing for this many heartbeat intervals is considered gone.
pub const DEAD_PEER_HEARTBEAT_FACTOR: u32 = 4;

/// Floor of the dead-peer timeout, so a 1 s heartbeat interval cannot make the worker twitchy.
pub const MIN_DEAD_PEER_TIMEOUT: Duration = Duration::from_secs(2);

/// How long cancelled tasks get to kill their process tree and clean their sandbox when a
/// session ends, before they are aborted outright.
pub const TASK_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the session loop compares the shared mobile telemetry with what the Master last heard.
pub const MOBILE_TELEMETRY_CHECK_INTERVAL: Duration = Duration::from_secs(2);

/// How often a worker that reads its own battery / thermal state (no platform app feeding it)
/// re-reads it.
pub const MOBILE_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long a closing session lets its writers flush the farewell message.
const FAREWELL_TIMEOUT: Duration = Duration::from_secs(1);

/// Capacity of the per-session control queue (heartbeats, telemetry, farewell).
const CONTROL_QUEUE_CAPACITY: usize = 16;

/// Capacity of the per-session data queue (task progress, results, checkpoints).
const DATA_QUEUE_CAPACITY: usize = 64;

/// After this many consecutive failed P2P dials the log escalates to an error: the ticket is
/// almost certainly stale (the Master restarted with a new identity), not momentarily unlucky.
#[cfg(feature = "p2p")]
const P2P_STALE_ERROR_AFTER: u32 = 3;

/// Resolves when shutdown was requested (the flag is `true`) or the sender was dropped.
/// Cancel-safe, so it can sit in a `select!` next to any connect/handshake future.
async fn shutdown_requested(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Task ids remembered after a `CancelTask` that found nothing to cancel (see
/// `WorkerClient::handle_cancel_task`).
const CANCELLED_UNSEEN_MEMORY: usize = 256;

/// Removes the table entry of `task_id` only if it still belongs to run `attempt`.
async fn remove_active(table: &ActiveTaskTable, task_id: TaskId, attempt: u64) {
    let mut lock = table.write().await;
    if lock.get(&task_id).is_some_and(|h| h.attempt == attempt) {
        lock.remove(&task_id);
    }
}

/// RAII guard ensuring active task counter is decremented and table entry is pruned on completion or abort.
struct ActiveTaskGuard {
    task_id: TaskId,
    attempt: u64,
    heartbeat_tracker: Arc<HeartbeatTracker>,
    active_task_table: ActiveTaskTable,
    _permit: OwnedSemaphorePermit,
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.heartbeat_tracker.decrement_active_tasks();
        let table = Arc::clone(&self.active_task_table);
        let (task_id, attempt) = (self.task_id, self.attempt);
        tokio::spawn(async move {
            remove_active(&table, task_id, attempt).await;
        });
    }
}

/// Aborts the wrapped task when dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Configuration options for initializing and connecting a Worker client.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Address of the Master node to connect to (e.g. "127.0.0.1:8088"; the dashboard is on :8080).
    pub master_addr: String,
    /// Optional P2P connection ticket for connecting over iroh QUIC NAT traversal. When
    /// `master_addr` is also set, a failed P2P dial falls back to TCP.
    pub p2p_ticket: Option<String>,
    /// Worker UUID. If `None`, it is read from `worker_id_file` / the default id file when
    /// persistence is enabled (see [`WorkerConfig::with_persistent_worker_id`]), otherwise generated
    /// once at client initialization; either way it is preserved across reconnects.
    pub worker_id: Option<Uuid>,
    /// File the worker id is persisted in (created with mode 0600). Setting it enables persistence.
    pub worker_id_file: Option<PathBuf>,
    /// Persist the worker id at `worker_id_file`, `RUSTY_GRID_WORKER_ID_FILE`, or
    /// `~/.oxideswarm/worker_id[.<name>]`, in that order. Off by default so that library users and
    /// tests never touch the real home directory; the CLI turns it on.
    pub persist_worker_id: bool,
    /// Optional human-readable worker name override (CLI `--name`).
    pub name: Option<String>,
    /// Optional CPU core count override (CLI `--cores`).
    pub cores: Option<usize>,
    /// Optional total RAM MB override (CLI `--ram-mb`).
    pub ram_mb: Option<u64>,
    /// Explicit physical GPU presence toggle (CLI `--gpu`). `Some(true)` is a request, not a
    /// claim: the worker advertises a real GPU only if its binary has the wgpu engine (feature
    /// `gpu-wgpu`) and an adapter starts, and otherwise advertises none and says why
    /// ([`WorkerClient::gpu_notices`]).
    pub gpu: Option<bool>,
    /// Look for a real GPU even without `--gpu` (the CLI turns this on, unless `--no-gpu`).
    /// Off by default so that library users and tests never get GPU workers they did not ask for;
    /// `RUSTY_GRID_ENABLE_PHYSICAL_GPU=1` has the same effect. Needs the `gpu-wgpu` feature.
    pub auto_detect_gpu: bool,
    /// Explicitly disable all GPU capabilities (CLI `--no-gpu`).
    pub no_gpu: bool,
    /// Whether to advertise simulated GPU compute capabilities (CLI `--simulate-gpu`).
    pub simulate_gpu: bool,
    /// Custom GPU model/device name (CLI `--gpu-name`).
    pub gpu_name: Option<String>,
    /// Maximum concurrent task execution limit (CLI `--max-concurrency`). When unset the worker
    /// leaves one core free (see [`default_max_concurrency`]) so that it never saturates the
    /// machine it shares with its owner.
    pub max_concurrency: Option<usize>,
    /// CPU priority of task processes. `None`: `RUSTY_GRID_TASK_PRIORITY`, else below normal.
    pub process_priority: Option<ProcessPriority>,
    /// Fallback heartbeat interval if not specified by Master in `RegisterAck` (default: 3s).
    pub default_heartbeat_interval: Duration,
    /// Maximum duration to await Master response during registration handshake (default: 5s).
    pub handshake_timeout: Duration,
    /// Maximum duration to establish the transport to the Master: TCP connect or P2P dial
    /// (default: 10s). A hung SYN would otherwise block for the OS timeout (about 2 minutes).
    pub connect_timeout: Duration,
    /// How long the Master may stay completely silent before the connection is dropped and
    /// re-established. `None` (default): [`DEAD_PEER_HEARTBEAT_FACTOR`] heartbeat intervals, at
    /// least [`MIN_DEAD_PEER_TIMEOUT`].
    pub dead_peer_timeout: Option<Duration>,
    /// Shared cluster secret. When set, the worker proves knowledge of it to the Master
    /// (`rusty_grid_core::cluster_auth`) on every connect and reconnect, TCP and P2P alike, before
    /// it sends `Register`, and refuses a Master that cannot prove it holds the same secret. A
    /// worker without a secret only joins masters that run without one.
    pub cluster_secret: Option<ClusterSecret>,
    /// Live battery / thermal state shared with whoever can read it (the Android app, via the JNI
    /// bridge). When set, the worker neither polls the OS itself nor trusts the snapshot taken at
    /// startup: it registers with the handle's state, gates incoming tasks on it, and reports every
    /// change to the Master. When `None` (default) a battery-powered host is polled every
    /// [`MOBILE_POLL_INTERVAL`].
    pub mobile_telemetry: Option<MobileTelemetryHandle>,
    /// Optional categorization tags for worker routing.
    pub tags: Vec<String>,
    /// Base directory for task sandbox isolation.
    pub sandbox_base_dir: Option<PathBuf>,
    /// Whether to keep sandbox directories after task completion.
    pub keep_sandboxes: bool,
    /// Maximum bytes to buffer for stdout and stderr before truncation.
    pub max_output_bytes: usize,
    /// Whether to enable standby HTTP redirection portal on worker node (default: true).
    pub enable_redirection_portal: bool,
    /// Port for standby HTTP redirection portal (default: 8080).
    pub redirection_portal_port: u16,
    /// Wire serialization codec (Bincode or Json, default: Bincode).
    pub wire_codec: WireCodec,
}

impl WorkerConfig {
    pub fn new(master_addr: impl Into<String>) -> Self {
        Self {
            master_addr: master_addr.into(),
            p2p_ticket: None,
            worker_id: None,
            worker_id_file: None,
            persist_worker_id: false,
            name: None,
            cores: None,
            ram_mb: None,
            gpu: None,
            auto_detect_gpu: false,
            no_gpu: false,
            simulate_gpu: false,
            gpu_name: None,
            max_concurrency: None,
            process_priority: None,
            default_heartbeat_interval: Duration::from_secs(3),
            handshake_timeout: Duration::from_secs(5),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            dead_peer_timeout: None,
            cluster_secret: None,
            mobile_telemetry: None,
            tags: Vec::new(),
            sandbox_base_dir: None,
            keep_sandboxes: false,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            enable_redirection_portal: true,
            redirection_portal_port: 8080,
            wire_codec: WireCodec::default(),
        }
    }

    pub fn with_redirection_portal(mut self, enable: bool) -> Self {
        self.enable_redirection_portal = enable;
        self
    }

    pub fn with_redirection_portal_port(mut self, port: u16) -> Self {
        self.redirection_portal_port = port;
        self
    }

    pub fn with_p2p_ticket(mut self, ticket: impl Into<String>) -> Self {
        self.p2p_ticket = Some(ticket.into());
        self
    }

    pub fn with_worker_id(mut self, id: Uuid) -> Self {
        self.worker_id = Some(id);
        self
    }

    /// Persists the worker id in `path` (and enables persistence).
    pub fn with_worker_id_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.worker_id_file = Some(path.into());
        self.persist_worker_id = true;
        self
    }

    /// Keeps the worker id across restarts: in `RUSTY_GRID_WORKER_ID_FILE` if set, otherwise in
    /// `~/.oxideswarm/worker_id` (`worker_id.<name>` for a worker with an explicit name, so
    /// several named workers on one machine stay distinct). If the file cannot be used the worker
    /// warns and runs with an id that lasts only as long as the process.
    pub fn with_persistent_worker_id(mut self) -> Self {
        self.persist_worker_id = true;
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_cores(mut self, cores: usize) -> Self {
        self.cores = Some(cores);
        self
    }

    pub fn with_ram_mb(mut self, ram_mb: u64) -> Self {
        self.ram_mb = Some(ram_mb);
        self
    }

    pub fn with_gpu(mut self, gpu: bool) -> Self {
        self.gpu = Some(gpu);
        self
    }

    /// Looks for a real GPU at start-up even without `--gpu` (see [`WorkerConfig::auto_detect_gpu`]).
    pub fn with_gpu_autodetect(mut self, auto_detect: bool) -> Self {
        self.auto_detect_gpu = auto_detect;
        self
    }

    pub fn with_no_gpu(mut self, no_gpu: bool) -> Self {
        self.no_gpu = no_gpu;
        self
    }

    pub fn with_simulate_gpu(mut self, simulate_gpu: bool) -> Self {
        self.simulate_gpu = simulate_gpu;
        self
    }

    pub fn with_gpu_name(mut self, name: impl Into<String>) -> Self {
        self.gpu_name = Some(name.into());
        self
    }

    pub fn with_max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = Some(max);
        self
    }

    pub fn with_process_priority(mut self, priority: ProcessPriority) -> Self {
        self.process_priority = Some(priority);
        self
    }

    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Overrides how long a completely silent Master is tolerated before the worker reconnects.
    pub fn with_dead_peer_timeout(mut self, timeout: Duration) -> Self {
        self.dead_peer_timeout = Some(timeout);
        self
    }

    /// Makes the worker authenticate to the Master with `secret` before registering.
    pub fn with_cluster_secret(mut self, secret: ClusterSecret) -> Self {
        self.cluster_secret = Some(secret);
        self
    }

    /// Lets a platform app feed live battery / thermal state into the running worker.
    pub fn with_mobile_telemetry(mut self, handle: MobileTelemetryHandle) -> Self {
        self.mobile_telemetry = Some(handle);
        self
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn with_sandbox_base_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.sandbox_base_dir = Some(path.into());
        self
    }

    pub fn with_keep_sandboxes(mut self, keep: bool) -> Self {
        self.keep_sandboxes = keep;
        self
    }

    pub fn with_max_output_bytes(mut self, bytes: usize) -> Self {
        self.max_output_bytes = bytes;
        self
    }

    pub fn with_wire_codec(mut self, codec: WireCodec) -> Self {
        self.wire_codec = codec;
        self
    }
}

/// Concurrent tasks a worker runs by default on a machine with `detected_cores` cores: one less
/// than the cores, never fewer than one. A build or compute job on every core would starve the
/// owner's own work on the phone, the gaming PC or the Mac.
pub fn default_max_concurrency(detected_cores: usize) -> usize {
    detected_cores.saturating_sub(1).max(1)
}

/// Picks the worker id: explicit id, then the persisted one, then a fresh random one.
fn resolve_worker_id(config: &WorkerConfig) -> Uuid {
    if let Some(id) = config.worker_id {
        return id;
    }
    if config.persist_worker_id || config.worker_id_file.is_some() {
        let path = config
            .worker_id_file
            .clone()
            .or_else(|| default_worker_id_path(config.name.as_deref()));
        match path {
            Some(path) => {
                if let Some(id) = load_or_create_worker_id(&path) {
                    info!(
                        worker_id = %id,
                        path = %path.display(),
                        "Using the persistent worker id (several workers on one machine need distinct --name values, or they would share it)"
                    );
                    return id;
                }
                warn!(
                    path = %path.display(),
                    "Could not persist the worker id; using one that lasts only until this process exits"
                );
            }
            None => warn!(
                "No home directory to keep the worker id in (set RUSTY_GRID_WORKER_ID_FILE); using one that lasts only until this process exits"
            ),
        }
    }
    Uuid::new_v4()
}

/// Autonomous Worker client managing connection, registration, task execution, and communication with Master.
pub struct WorkerClient {
    config: WorkerConfig,
    worker_id: Uuid,
    capabilities: WorkerCapabilities,
    /// What start-up had to say about the GPU (see [`WorkerClient::gpu_notices`]).
    gpu_notices: Vec<GpuNotice>,
    active_tasks: Arc<AtomicUsize>,
    heartbeat_tracker: Arc<HeartbeatTracker>,
    backoff_config: BackoffConfig,
    runner: Arc<TaskRunner>,
    active_task_table: ActiveTaskTable,
    concurrency_semaphore: Arc<Semaphore>,
    master_beacon: Arc<RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>>,
    /// Live battery / thermal state; `capabilities.mobile` is only the snapshot taken at startup.
    mobile: MobileTelemetryHandle,
    /// True when the platform app feeds `mobile`, so the worker must not poll the OS itself.
    mobile_fed_externally: bool,
    /// Base directory the runner creates `<task id>` sandboxes in.
    sandbox_base_dir: PathBuf,
    /// Source of [`TaskExecutionHandle::attempt`].
    next_attempt: Arc<AtomicU64>,
    /// The last few task ids the Master cancelled before this worker had heard of them, oldest
    /// first: an `AssignTask` for one of them that arrives later is refused, not run.
    cancelled_unseen: std::sync::Mutex<VecDeque<TaskId>>,
    /// Set when the last session ended because the Master announced its own shutdown.
    master_shutdown_announced: bool,
    /// Whether the Master of the current connection announced that it understands
    /// `WorkerMessage::MobileTelemetry` (see `rusty_grid_core::protocol::master_features`). A
    /// Master built before that variant existed treats it as a corrupt frame and drops the worker.
    master_takes_mobile_telemetry: AtomicBool,
    #[cfg(feature = "p2p")]
    p2p_endpoint: Arc<tokio::sync::Mutex<Option<iroh::Endpoint>>>,
    #[cfg(feature = "p2p")]
    p2p_conn: Arc<tokio::sync::RwLock<Option<iroh::endpoint::Connection>>>,
    /// Consecutive failed P2P dials (reset by a successful one).
    #[cfg(feature = "p2p")]
    p2p_failures: AtomicU32,
}

/// Connection stream mode to Master: single stream (direct TCP) or 2-lane QUIC multiplexed streams (iroh P2P).
pub enum WorkerStreamMode {
    Single(rusty_grid_core::transport::GridStream),
    #[cfg(feature = "p2p")]
    Multiplexed {
        control: rusty_grid_core::transport::GridStream,
        data: rusty_grid_core::transport::GridStream,
    },
}

/// What the lane tasks of a session report to its loop.
enum Inbound {
    /// A decoded frame from the Master (boxed: a `Task` makes it much larger than the other events).
    Message(Box<MasterMessage>),
    /// The Master closed the lane (EOF).
    Closed(&'static str),
    /// Reading from or writing to the lane failed.
    Failed(&'static str, ProtocolError),
}

/// A receiver whose sender is gone: a writer lane that does not serve this queue.
fn closed_queue() -> mpsc::Receiver<WorkerMessage> {
    let (_tx, rx) = mpsc::channel(1);
    rx
}

/// Writes queued messages to one lane until told to close or until a write fails.
///
/// The control queue (heartbeats, telemetry, the farewell) is always drained before the next data
/// message (progress, results, checkpoints) is started, so a burst of results can never push a
/// heartbeat back by more than the one frame already on the wire. A write failure is reported
/// through `events`. On `closing` the writer first flushes the control queue and then returns.
async fn lane_writer<W: AsyncWrite + Unpin>(
    mut writer: MessageWriter<W>,
    mut control_rx: mpsc::Receiver<WorkerMessage>,
    mut data_rx: mpsc::Receiver<WorkerMessage>,
    lane: &'static str,
    mut closing: watch::Receiver<bool>,
    events: mpsc::Sender<Inbound>,
) {
    loop {
        let msg = match control_rx.try_recv() {
            Ok(msg) => msg,
            Err(_) => {
                if *closing.borrow() {
                    return;
                }
                tokio::select! {
                    biased;
                    Some(msg) = control_rx.recv() => msg,
                    changed = closing.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        continue;
                    }
                    Some(msg) = data_rx.recv() => msg,
                    else => return,
                }
            }
        };
        if let Err(e) = writer.send_msg(&msg).await {
            let _ = events.send(Inbound::Failed(lane, e)).await;
            return;
        }
    }
}

/// Reads frames from one lane and forwards them to the session loop, in their own task so that
/// the loop never has to cancel a half-finished read.
fn spawn_lane_reader<R: AsyncRead + Unpin + Send + 'static>(
    mut reader: MessageReader<R>,
    lane: &'static str,
    events: mpsc::Sender<Inbound>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let event = match reader.recv_msg::<MasterMessage>().await {
                Ok(Some(msg)) => Inbound::Message(Box::new(msg)),
                Ok(None) => Inbound::Closed(lane),
                Err(e) => Inbound::Failed(lane, e),
            };
            let last = !matches!(event, Inbound::Message(_));
            if events.send(event).await.is_err() || last {
                return;
            }
        }
    })
}

/// The tasks and queues of one registered connection.
struct Session {
    control_tx: mpsc::Sender<WorkerMessage>,
    data_tx: mpsc::Sender<WorkerMessage>,
    control_rx: Option<mpsc::Receiver<WorkerMessage>>,
    data_rx: Option<mpsc::Receiver<WorkerMessage>>,
    inbound_tx: mpsc::Sender<Inbound>,
    inbound_rx: mpsc::Receiver<Inbound>,
    closing_tx: watch::Sender<bool>,
    writers: Vec<JoinHandle<()>>,
    readers: Vec<JoinHandle<()>>,
    clock: Arc<IoClock>,
}

impl Session {
    fn new(clock: Arc<IoClock>) -> Self {
        let (control_tx, control_rx) = mpsc::channel(CONTROL_QUEUE_CAPACITY);
        let (data_tx, data_rx) = mpsc::channel(DATA_QUEUE_CAPACITY);
        let (inbound_tx, inbound_rx) = mpsc::channel(64);
        let (closing_tx, _) = watch::channel(false);
        Self {
            control_tx,
            data_tx,
            control_rx: Some(control_rx),
            data_rx: Some(data_rx),
            inbound_tx,
            inbound_rx,
            closing_tx,
            writers: Vec::new(),
            readers: Vec::new(),
            clock,
        }
    }

    /// Starts the reader and writer tasks of one lane. `control` / `data` say which outbound
    /// queues this lane serves: a single TCP stream serves both, a QUIC session one each.
    fn attach_lane<W, R>(
        &mut self,
        writer: MessageWriter<W>,
        reader: MessageReader<R>,
        lane: &'static str,
        control: bool,
        data: bool,
    ) where
        W: AsyncWrite + Unpin + Send + 'static,
        R: AsyncRead + Unpin + Send + 'static,
    {
        let control_rx = control
            .then(|| self.control_rx.take())
            .flatten()
            .unwrap_or_else(closed_queue);
        let data_rx = data
            .then(|| self.data_rx.take())
            .flatten()
            .unwrap_or_else(closed_queue);
        self.writers.push(tokio::spawn(lane_writer(
            writer,
            control_rx,
            data_rx,
            lane,
            self.closing_tx.subscribe(),
            self.inbound_tx.clone(),
        )));
        self.readers.push(spawn_lane_reader(
            reader,
            lane,
            self.inbound_tx.clone(),
        ));
    }

    fn stop_lanes(&self) {
        for task in self.writers.iter().chain(self.readers.iter()) {
            task.abort();
        }
    }

    /// Ends the session: queues `farewell`, lets the writers flush the control queue (bounded),
    /// then stops every lane task, which closes the sockets.
    async fn close(mut self, farewell: Option<WorkerMessage>) {
        if let Some(msg) = farewell {
            let _ = self.control_tx.try_send(msg);
        }
        let _ = self.closing_tx.send(true);
        let flush = async {
            for writer in &mut self.writers {
                let _ = writer.await;
            }
        };
        let _ = tokio::time::timeout(FAREWELL_TIMEOUT, flush).await;
        self.stop_lanes();
    }
}

/// The lane tasks are spawned, so they would outlive a dropped `run()` future (a caller aborting
/// the worker task, a panic) and keep the socket open: the Master would never see the connection
/// end. Dropping the session therefore stops them, which closes the socket exactly as dropping
/// the connection did when the lanes were polled inside the future.
impl Drop for Session {
    fn drop(&mut self) {
        self.stop_lanes();
    }
}

/// `RUSTY_GRID_ENABLE_PHYSICAL_GPU=1`: look for a real GPU without `--gpu` (the library-level
/// opt-in; the CLI always looks unless `--no-gpu`).
fn physical_gpu_requested_by_env() -> bool {
    std::env::var("RUSTY_GRID_ENABLE_PHYSICAL_GPU").as_deref() == Ok("1")
}

impl WorkerClient {
    /// Initializes a new WorkerClient, detecting host hardware and honoring CLI overrides.
    pub fn new(config: WorkerConfig) -> Self {
        let worker_id = resolve_worker_id(&config);
        let overrides = rusty_grid_core::capabilities::HardwareOverrides {
            name: config.name.clone(),
            cores: config.cores,
            ram_mb: config.ram_mb,
            gpu: config.gpu,
            no_gpu: config.no_gpu,
            simulate_gpu: config.simulate_gpu,
            gpu_name: config.gpu_name.clone(),
        };
        let mut capabilities =
            WorkerCapabilities::detect_with_overrides(overrides).with_tags(config.tags.clone());
        // A real GPU is advertised only if this binary can compute on one (see `gpu`).
        let gpu_request = GpuRequest {
            no_gpu: config.no_gpu,
            simulate_gpu: config.simulate_gpu,
            gpu: config.gpu,
            auto_detect: config.auto_detect_gpu || physical_gpu_requested_by_env(),
            gpu_name: config.gpu_name.clone(),
        };
        let gpu_notices = apply_gpu_policy(&mut capabilities, &gpu_request, probe_real_gpu);
        for notice in &gpu_notices {
            match notice.level {
                NoticeLevel::Info => info!("{}", notice.message),
                NoticeLevel::Warn => warn!("{}", notice.message),
            }
        }

        let (mobile, mobile_fed_externally) = match config.mobile_telemetry.clone() {
            Some(handle) => {
                // The app may only learn the live readings after the worker is built, or may have
                // pushed them before it existed; either way fill the gaps with what the OS probe
                // found (descriptors) rather than advertising nothing.
                match (handle.snapshot(), capabilities.mobile.as_ref()) {
                    (None, probed) => handle.set(probed.cloned()),
                    (Some(_), Some(probed)) => handle.fill_unknown_descriptors(probed),
                    (Some(_), None) => {}
                }
                (handle, true)
            }
            None => (MobileTelemetryHandle::new(capabilities.mobile.clone()), false),
        };

        let active_tasks = Arc::new(AtomicUsize::new(0));
        let heartbeat_tracker = Arc::new(HeartbeatTracker::new(Arc::clone(&active_tasks)));

        let mut sandbox_cfg = SandboxConfig::default();
        if let Some(ref sb) = config.sandbox_base_dir {
            sandbox_cfg.base_dir = sb.clone();
        }
        sandbox_cfg.keep_sandboxes = config.keep_sandboxes;
        let sandbox_base_dir = sandbox_cfg.base_dir.clone();

        let mut runner_cfg =
            RunnerConfig::new(sandbox_cfg).with_max_output_bytes(config.max_output_bytes);
        if let Some(priority) = config.process_priority {
            runner_cfg = runner_cfg.with_process_priority(priority);
        }
        let runner = Arc::new(TaskRunner::new(worker_id, capabilities.clone(), runner_cfg));
        let active_task_table = Arc::new(RwLock::new(HashMap::new()));
        let concurrency_permits = config
            .max_concurrency
            .unwrap_or_else(|| {
                // Cores the owner named (`--cores`) are capacity they chose to offer; only the
                // detected count gets the headroom.
                if config.cores.is_some() {
                    capabilities.cpu_cores
                } else {
                    default_max_concurrency(capabilities.cpu_cores)
                }
            })
            .max(1);
        let concurrency_semaphore = Arc::new(Semaphore::new(concurrency_permits));

        let initial_beacon = if !config.master_addr.is_empty()
            && !config.master_addr.eq_ignore_ascii_case("auto")
            && !config.master_addr.starts_with("auto:")
        {
            let host = if let Some(idx) = config.master_addr.find(':') {
                &config.master_addr[..idx]
            } else {
                &config.master_addr
            };
            Some(rusty_grid_core::discovery::MasterBeacon::new(
                config.master_addr.clone(),
                format!("http://{}:8080", host),
                "KnownMaster",
                0,
            ))
        } else {
            None
        };
        let master_beacon = Arc::new(RwLock::new(initial_beacon));

        info!(
            worker_id = %worker_id,
            name = %capabilities.name,
            cpu_cores = capabilities.cpu_cores,
            ram_mb = capabilities.ram_mb,
            has_gpu = capabilities.has_gpu,
            is_simulated_gpu = capabilities.is_simulated_gpu,
            "Worker client initialized"
        );

        Self {
            config,
            worker_id,
            capabilities,
            gpu_notices,
            active_tasks,
            heartbeat_tracker,
            backoff_config: BackoffConfig::default(),
            runner,
            active_task_table,
            concurrency_semaphore,
            master_beacon,
            mobile,
            mobile_fed_externally,
            sandbox_base_dir,
            next_attempt: Arc::new(AtomicU64::new(0)),
            cancelled_unseen: std::sync::Mutex::new(VecDeque::new()),
            master_shutdown_announced: false,
            master_takes_mobile_telemetry: AtomicBool::new(false),
            #[cfg(feature = "p2p")]
            p2p_endpoint: Arc::new(tokio::sync::Mutex::new(None)),
            #[cfg(feature = "p2p")]
            p2p_conn: Arc::new(tokio::sync::RwLock::new(None)),
            #[cfg(feature = "p2p")]
            p2p_failures: AtomicU32::new(0),
        }
    }

    /// Returns the active P2P path information, if connected via P2P.
    #[cfg(feature = "p2p")]
    pub async fn active_p2p_path_info(&self) -> Option<rusty_grid_core::transport::P2pPathInfo> {
        let guard = self.p2p_conn.read().await;
        guard
            .as_ref()
            .and_then(rusty_grid_core::transport::inspect_connection_paths)
    }

    /// Returns a shared reference to the active P2P connection handle.
    #[cfg(feature = "p2p")]
    pub fn p2p_connection(&self) -> Arc<tokio::sync::RwLock<Option<iroh::endpoint::Connection>>> {
        Arc::clone(&self.p2p_conn)
    }

    /// Sets an explicit pre-configured P2P endpoint (e.g. for testing relay fallback or specific network constraints).
    #[cfg(feature = "p2p")]
    pub fn with_p2p_endpoint(self, endpoint: iroh::Endpoint) -> Self {
        if let Ok(mut guard) = self.p2p_endpoint.try_lock() {
            *guard = Some(endpoint);
        }
        self
    }


    /// Returns a shared reference to the discovered master beacon state.
    pub fn master_beacon(&self) -> Arc<RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>> {
        Arc::clone(&self.master_beacon)
    }

    /// Handle to the live battery / thermal state of this device. A platform app calls
    /// [`MobileTelemetryHandle::update`] on it from its battery and thermal callbacks (or passes
    /// its own handle in through [`WorkerConfig::with_mobile_telemetry`]); the running worker
    /// reports every change to the Master and refuses tasks on a critically low battery.
    pub fn mobile_telemetry(&self) -> MobileTelemetryHandle {
        self.mobile.clone()
    }

    /// Convenience constructor initializing WorkerClient directly with common CLI options.
    pub fn from_options(
        master_addr: impl Into<String>,
        name: Option<String>,
        cores: Option<usize>,
        simulate_gpu: bool,
    ) -> Self {
        let mut config = WorkerConfig::new(master_addr);
        if let Some(n) = name {
            config = config.with_name(n);
        }
        if let Some(c) = cores {
            config = config.with_cores(c);
        }
        config = config.with_simulate_gpu(simulate_gpu);
        Self::new(config)
    }

    /// Alias for `from_options`.
    pub fn with_options(
        master_addr: impl Into<String>,
        name: Option<String>,
        cores: Option<usize>,
        simulate_gpu: bool,
    ) -> Self {
        Self::from_options(master_addr, name, cores, simulate_gpu)
    }

    /// Returns the persistent worker UUID.
    pub fn worker_id(&self) -> Uuid {
        self.worker_id
    }

    /// Returns the advertised worker capabilities.
    pub fn capabilities(&self) -> &WorkerCapabilities {
        &self.capabilities
    }

    /// What start-up decided about the GPU and why: the real adapter in use, or, if the owner
    /// asked for a GPU that cannot be used, a warning saying how to get one. The CLI prints these.
    pub fn gpu_notices(&self) -> &[GpuNotice] {
        &self.gpu_notices
    }

    /// Returns a reference to the active tasks counter.
    pub fn active_tasks_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.active_tasks)
    }

    /// Returns a reference to the heartbeat tracker.
    pub fn heartbeat_tracker(&self) -> &Arc<HeartbeatTracker> {
        &self.heartbeat_tracker
    }

    /// Returns a reference to the inner TaskRunner.
    pub fn runner(&self) -> &Arc<TaskRunner> {
        &self.runner
    }

    /// Returns a reference to the active task table.
    pub fn active_task_table(&self) -> &ActiveTaskTable {
        &self.active_task_table
    }

    /// Returns a reference to the concurrency semaphore.
    pub fn concurrency_semaphore(&self) -> &Arc<Semaphore> {
        &self.concurrency_semaphore
    }

    /// Sets the reconnection backoff configuration.
    pub fn set_backoff_config(&mut self, config: BackoffConfig) {
        self.backoff_config = config;
    }

    /// Returns the active backoff configuration.
    pub fn backoff_config(&self) -> &BackoffConfig {
        &self.backoff_config
    }

    /// How long the Master may stay silent before the connection is declared dead.
    fn dead_peer_timeout(&self, heartbeat_interval: Duration) -> Duration {
        self.config.dead_peer_timeout.unwrap_or_else(|| {
            heartbeat_interval
                .saturating_mul(DEAD_PEER_HEARTBEAT_FACTOR)
                .max(MIN_DEAD_PEER_TIMEOUT)
        })
    }

    /// Primary entry point: runs the worker supervisor loop with automatic reconnection,
    /// exponential backoff, and jitter. Returns when the shutdown signal fires (or its sender is
    /// dropped), or with `GridError::Config` for a configuration no retry can fix (an unparsable
    /// P2P ticket). A Master going away, in any manner, only ever makes the worker reconnect.
    pub async fn run(&mut self, mut shutdown_rx: watch::Receiver<bool>) -> GridResult<()> {
        #[cfg(feature = "p2p")]
        if let Some(ticket) = self.config.p2p_ticket.as_deref() {
            if let Err(e) = rusty_grid_core::transport::parse_p2p_ticket(ticket) {
                let err = GridError::Config(format!("Invalid P2P ticket: {e}"));
                error!(worker_id = %self.worker_id, error = %err, "The configured P2P ticket cannot be used; fix --p2p-ticket (copy it again from the Master)");
                return Err(err);
            }
        }

        // A sandbox directory nobody can write to would fail every process task one by one, far
        // from its cause; refuse to start instead, with the fix in the message.
        if let Err(e) = self.runner.config().sandbox_config.ensure_usable().await {
            let err = GridError::Config(e.to_string());
            error!(worker_id = %self.worker_id, error = %err, "Task sandboxes cannot be created");
            return Err(err);
        }

        let mut backoff = ExponentialBackoff::new(self.backoff_config.clone());

        if self.config.enable_redirection_portal {
            let portal_port = self.config.redirection_portal_port;
            let portal_shutdown_rx = shutdown_rx.clone();
            let beacon_ref = Arc::clone(&self.master_beacon);
            let secret = self.config.cluster_secret.clone();
            tokio::spawn(async move {
                spawn_worker_redirection_portal_with_secret(
                    portal_port,
                    beacon_ref,
                    secret,
                    portal_shutdown_rx,
                )
                .await;
            });
        }

        // A worker that reads its own battery (a laptop, Termux) keeps the shared reading fresh;
        // one fed by a platform app is updated by that app instead.
        let _mobile_poller = (!self.mobile_fed_externally && self.mobile.snapshot().is_some())
            .then(|| {
                AbortOnDrop(spawn_mobile_poller(
                    self.mobile.clone(),
                    MOBILE_POLL_INTERVAL,
                    detect_live_mobile_status,
                ))
            });

        loop {
            if *shutdown_rx.borrow() {
                info!(worker_id = %self.worker_id, "Worker shutdown requested; exiting supervisor");
                break;
            }

            self.master_shutdown_announced = false;
            let outcome = self.connect_and_run(&mut shutdown_rx, &mut backoff).await;

            // Whatever ended the session, nothing that is still running can deliver its result any
            // more (see the module docs): stop it cleanly before the next connection starts.
            self.cancel_all_active_tasks("connection to the Master ended")
                .await;

            match outcome {
                Ok(()) => {
                    info!(worker_id = %self.worker_id, "Worker connection ended cleanly");
                    break;
                }
                Err(e) => {
                    if *shutdown_rx.borrow() {
                        info!(worker_id = %self.worker_id, "Worker terminating on shutdown signal after connection notice");
                        break;
                    }

                    if let GridError::Config(_) = e {
                        error!(worker_id = %self.worker_id, error = %e, "Worker configuration error; not retrying");
                        return Err(e);
                    }

                    let auth_failure =
                        matches!(e, GridError::AuthFailed(_) | GridError::AuthRequired(_));
                    let mut delay_hint = None;
                    if auth_failure {
                        // One actionable line per attempt, and a long pause: the same secret
                        // cannot start working by itself, so there is nothing to gain from a hot loop.
                        error!(
                            worker_id = %self.worker_id,
                            error = %e,
                            retry_in_secs = AUTH_FAILURE_MIN_BACKOFF.as_secs(),
                            "Cluster authentication with the Master failed; this worker cannot register until it holds the Master's cluster secret (use --secret-file, OXIDESWARM_SECRET or ~/.oxideswarm/cluster.secret, then restart the worker)"
                        );
                        delay_hint = Some(AUTH_FAILURE_MIN_BACKOFF);
                    } else if self.master_shutdown_announced {
                        // Not an error and not a reason to exit: the Master is restarting (or
                        // going away for good). The ordinary backoff rhythm applies, so a restart
                        // is noticed within about a second and a long outage costs one cheap
                        // connection attempt every few seconds.
                        info!(
                            worker_id = %self.worker_id,
                            attempt = backoff.attempt() + 1,
                            "Master announced its shutdown; reconnecting with backoff until it is back"
                        );
                    } else {
                        warn!(
                            worker_id = %self.worker_id,
                            error = %e,
                            attempt = backoff.attempt() + 1,
                            "Connection dropped or failed; applying exponential backoff before reconnect"
                        );
                    }

                    *self.master_beacon.write().await = None;

                    let mut delay = backoff.next_delay();
                    if let Some(min_delay) = delay_hint {
                        delay = delay.max(min_delay);
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {},
                        res = shutdown_rx.changed() => {
                            if res.is_ok() && *shutdown_rx.borrow() {
                                info!(worker_id = %self.worker_id, "Shutdown signal received during backoff wait");
                                break;
                            } else if res.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Dispatches an assigned task asynchronously, managing permits, telemetry, and results.
    pub async fn handle_assign_task(&self, task: Task, outbound_tx: mpsc::Sender<WorkerMessage>) {
        let checkpoint = task.latest_checkpoint.clone();
        self.handle_assign_task_with_checkpoint(task, checkpoint, outbound_tx)
            .await;
    }

    /// Dispatches an assigned task asynchronously with checkpoint delta resumption.
    pub async fn handle_assign_task_with_checkpoint(
        &self,
        task: Task,
        checkpoint: Option<CheckpointData>,
        outbound_tx: mpsc::Sender<WorkerMessage>,
    ) {
        let task_id = task.id;
        let worker_id = self.worker_id;

        // Cancelled before it got here (see `cancelled_unseen`): report it cancelled instead of
        // running a task the Master has already given up on, which would also hold a slot the
        // Master believes is free.
        let was_cancelled = {
            let mut seen = self.cancelled_unseen.lock().unwrap_or_else(|e| e.into_inner());
            match seen.iter().position(|id| *id == task_id) {
                Some(pos) => {
                    seen.remove(pos);
                    true
                }
                None => false,
            }
        };
        if was_cancelled {
            warn!(task_id = %task_id, "Refusing an AssignTask for a task the Master already cancelled");
            let result = TaskResult::failure(
                worker_id,
                task_id,
                EXIT_CODE_CANCELLED,
                "",
                "",
                0,
                Some("Cancelled before it was assigned".to_string()),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        // Verify task requirements against worker capabilities
        if !self.capabilities.satisfies(&task.requirements) {
            warn!(task_id = %task_id, "Task requirements exceed worker capabilities");
            let result = TaskResult::failure(
                worker_id,
                task_id,
                1,
                "",
                "Task requirements exceed worker capabilities (CPU, RAM, or GPU)",
                0,
                Some("Worker capabilities insufficient for task requirements".to_string()),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        // Mobile constraints: reject task if battery < 15% and discharging. The reading is the
        // live one (platform callbacks / periodic poll), not the snapshot taken at startup.
        if let Some(mobile) = self.mobile.snapshot() {
            if let (Some(pct), Some(false)) = (mobile.battery_pct, mobile.is_charging) {
                if pct < 15 {
                    warn!(
                        task_id = %task_id,
                        pct = pct,
                        "Task rejected by worker: Critical battery preservation (<15% discharging)"
                    );
                    let result = TaskResult::failure(
                        worker_id,
                        task_id,
                        1,
                        "",
                        "Task rejected by worker: Critical battery preservation (<15% discharging)",
                        0,
                        Some("Battery critically low (<15% discharging)".to_string()),
                    );
                    let _ = outbound_tx.send(WorkerMessage::from(result)).await;
                    return;
                }
            }
        }

        // The same task id can arrive while an earlier run of it is still alive (a duplicate
        // delivery). Replacing the table entry would drop the old run's cancel handle and make it
        // look like a Master cancellation, so stop it explicitly first, silently.
        self.retire_previous_run(task_id).await;
        // A sandbox directory left behind by a killed worker process (or kept for debugging) would
        // make `Sandbox::create` fail with "already exists", and that failure is terminal.
        self.reclaim_stale_sandbox(task_id).await;

        let attempt = self.next_attempt.fetch_add(1, Ordering::Relaxed) + 1;
        let (cancel_tx, cancel_rx) = oneshot::channel::<String>();
        let child_pid = Arc::new(AtomicU32::new(0));
        let suppress_result = Arc::new(AtomicBool::new(false));

        let tracker = Arc::clone(&self.heartbeat_tracker);
        let table = Arc::clone(&self.active_task_table);
        let semaphore = Arc::clone(&self.concurrency_semaphore);
        let runner = Arc::clone(&self.runner);
        let child_pid_clone = Arc::clone(&child_pid);
        let suppress = Arc::clone(&suppress_result);

        // The table lock is held from before the spawn until the handle is inserted, so the run's
        // own cleanup (which needs the lock) can never overtake the insertion and leave a stale
        // entry behind.
        let mut lock = self.active_task_table.write().await;
        let runner_handle = tokio::spawn(async move {
            let mut cancel_rx = cancel_rx;
            let permit = tokio::select! {
                res = semaphore.acquire_owned() => match res {
                    Ok(p) => p,
                    Err(_) => return, // Worker shutting down
                },
                cancel_msg = &mut cancel_rx => {
                    let reason = cancel_msg.unwrap_or_else(|_| "Cancelled while queued".into());
                    let res = TaskResult::failure(
                        worker_id,
                        task_id,
                        EXIT_CODE_CANCELLED,
                        "",
                        "",
                        0,
                        Some(format!("Cancelled: {reason}")),
                    );
                    remove_active(&table, task_id, attempt).await;
                    if !suppress.load(Ordering::Acquire) {
                        let _ = outbound_tx.send(WorkerMessage::from(res)).await;
                    }
                    return;
                }
            };

            let _guard = ActiveTaskGuard {
                task_id,
                attempt,
                heartbeat_tracker: Arc::clone(&tracker),
                active_task_table: Arc::clone(&table),
                _permit: permit,
            };

            tracker.increment_active_tasks();

            let progress = WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status: TaskStatus::Running,
            };
            if outbound_tx.send(progress).await.is_err() {
                return;
            }

            let effective_checkpoint = checkpoint.or_else(|| task.latest_checkpoint.clone());
            let result = runner
                .execute_task_with_checkpoint(
                    &task,
                    effective_checkpoint.as_ref(),
                    Some(outbound_tx.clone()),
                    Some(child_pid_clone),
                    Some(cancel_rx),
                )
                .await;

            if !suppress.load(Ordering::Acquire) {
                let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            }
        });

        lock.insert(
            task_id,
            TaskExecutionHandle {
                task_id,
                cancel_tx: Some(cancel_tx),
                abort_handle: runner_handle.abort_handle(),
                child_pid,
                started_at_epoch_sec: current_epoch_secs(),
                attempt,
                suppress_result,
                join: Some(runner_handle),
            },
        );
    }

    /// Cancels an active task by signaling the cancellation channel and triggering abort if necessary.
    pub async fn handle_cancel_task(&self, task_id: TaskId, reason: Option<String>) {
        let mut lock = self.active_task_table.write().await;
        if let Some(mut handle) = lock.remove(&task_id) {
            let r = reason.unwrap_or_else(|| "Cancelled by master directive".into());
            info!(task_id = %task_id, reason = %r, "Cancelling active task");
            let sent = if let Some(tx) = handle.cancel_tx.take() {
                tx.send(r).is_ok()
            } else {
                false
            };
            if !sent {
                handle.abort_handle.abort();
            }
        } else {
            warn!(task_id = %task_id, "CancelTask received for unknown or already completed task");
            // The Master sends AssignTask before CancelTask, but a cancelled task that is still
            // on its way must not run once it arrives; a task id is never assigned again after
            // it was cancelled, so remembering the id is safe.
            let mut seen = self.cancelled_unseen.lock().unwrap_or_else(|e| e.into_inner());
            if !seen.contains(&task_id) {
                if seen.len() >= CANCELLED_UNSEEN_MEMORY {
                    seen.pop_front();
                }
                seen.push_back(task_id);
            }
        }
    }

    /// Stops a run the worker itself decided to end: its result will not be sent.
    /// Returns the run's join handle so the caller can wait for the cleanup.
    fn begin_stop(mut handle: TaskExecutionHandle, reason: &str) -> Option<JoinHandle<()>> {
        handle.suppress_result.store(true, Ordering::Release);
        let sent = handle
            .cancel_tx
            .take()
            .is_some_and(|tx| tx.send(reason.to_string()).is_ok());
        if !sent {
            handle.abort_handle.abort();
        }
        handle.join.take()
    }

    /// Cancels the previous run of `task_id`, if any, and waits until it is gone.
    async fn retire_previous_run(&self, task_id: TaskId) {
        let previous = self.active_task_table.write().await.remove(&task_id);
        let Some(previous) = previous else {
            return;
        };
        warn!(task_id = %task_id, "Task assigned again while its previous run is still active; cancelling the old run first");
        if let Some(mut join) = Self::begin_stop(previous, "superseded by a new assignment of the same task") {
            if tokio::time::timeout(TASK_DRAIN_TIMEOUT, &mut join).await.is_err() {
                warn!(task_id = %task_id, "Previous run did not stop in time; aborting it");
                join.abort();
            }
        }
    }

    /// Cancels every task still running on this worker, silently, and waits (bounded by
    /// [`TASK_DRAIN_TIMEOUT`]) until their process trees are killed and their sandboxes removed.
    /// Called whenever a session ends; returns how many tasks were running.
    ///
    /// No result is sent for them: the Master only accepts a result from the session a task was
    /// assigned in and requeues the task when that session ends, so a result could only be dropped.
    pub async fn cancel_all_active_tasks(&self, reason: &str) -> usize {
        let handles: Vec<TaskExecutionHandle> = {
            let mut lock = self.active_task_table.write().await;
            lock.drain().map(|(_, handle)| handle).collect()
        };
        if handles.is_empty() {
            return 0;
        }
        let count = handles.len();
        warn!(
            worker_id = %self.worker_id,
            tasks = count,
            reason = %reason,
            "Cancelling running tasks: their results can no longer be delivered, and the Master requeues them"
        );
        let joins: Vec<JoinHandle<()>> = handles
            .into_iter()
            .filter_map(|handle| Self::begin_stop(handle, reason))
            .collect();
        let deadline = tokio::time::Instant::now() + TASK_DRAIN_TIMEOUT;
        for mut join in joins {
            if tokio::time::timeout_at(deadline, &mut join).await.is_err() {
                warn!("A cancelled task did not stop within the drain timeout; aborting it");
                join.abort();
            }
        }
        count
    }

    /// Removes (or, with `keep_sandboxes`, moves aside) a `<task id>` sandbox directory that no
    /// run of this worker owns. Called only after any earlier run of the id has been awaited.
    async fn reclaim_stale_sandbox(&self, task_id: TaskId) {
        let path = self.sandbox_base_dir.join(task_id.to_string());
        if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return;
        }
        if self.config.keep_sandboxes {
            let aside = self.sandbox_base_dir.join(format!(
                "{task_id}.stale-{}",
                chrono::Utc::now().timestamp_millis()
            ));
            match tokio::fs::rename(&path, &aside).await {
                Ok(()) => info!(task_id = %task_id, moved_to = %aside.display(), "Moved a leftover sandbox of this task aside"),
                Err(e) => warn!(task_id = %task_id, error = %e, "Could not move a leftover sandbox aside"),
            }
        } else {
            match tokio::fs::remove_dir_all(&path).await {
                Ok(()) => info!(task_id = %task_id, path = %path.display(), "Removed a leftover sandbox of this task"),
                Err(e) => warn!(task_id = %task_id, error = %e, "Could not remove a leftover sandbox"),
            }
        }
    }

    /// True when a plain TCP address (or `auto` discovery) is configured next to a P2P ticket.
    #[cfg(feature = "p2p")]
    fn has_tcp_fallback(&self) -> bool {
        !self.config.master_addr.trim().is_empty()
    }

    /// Establishes an underlying stream to the Master (either P2P QUIC via ticket, or direct TCP).
    ///
    /// With a ticket the P2P dial comes first; if it fails and a TCP address is configured too
    /// (the Android app passes both) the worker falls back to TCP instead of retrying a stale
    /// ticket forever: after the Master restarts with a new identity the old ticket never works
    /// again, while its LAN address still does.
    async fn establish_stream(
        &self,
    ) -> GridResult<(WorkerStreamMode, String)> {
        #[cfg(feature = "p2p")]
        if self.config.p2p_ticket.is_some() {
            return match self.establish_p2p().await {
                Ok(established) => {
                    self.p2p_failures.store(0, Ordering::Relaxed);
                    Ok(established)
                }
                Err(e) => {
                    let failures = self.p2p_failures.fetch_add(1, Ordering::Relaxed) + 1;
                    if failures >= P2P_STALE_ERROR_AFTER {
                        error!(
                            worker_id = %self.worker_id,
                            failures,
                            error = %e,
                            "The P2P ticket keeps failing; the Master probably restarted with a new identity (copy its current ticket)"
                        );
                    }
                    if !self.has_tcp_fallback() {
                        return Err(e);
                    }
                    warn!(
                        error = %e,
                        master = %self.config.master_addr,
                        "P2P connect failed; falling back to the Master's TCP address"
                    );
                    self.establish_tcp().await
                }
            };
        }
        self.establish_tcp().await
    }

    /// Dials the Master over iroh QUIC with the configured ticket.
    #[cfg(feature = "p2p")]
    async fn establish_p2p(&self) -> GridResult<(WorkerStreamMode, String)> {
        let ticket_str = self.config.p2p_ticket.as_deref().unwrap_or_default();
        let node_addr = rusty_grid_core::transport::parse_p2p_ticket(ticket_str)
            .map_err(|e| GridError::Config(format!("Invalid P2P ticket: {e}")))?;
        info!("Connecting to Master via P2P NAT Traversal (iroh QUIC)...");

        let mut ep_guard = self.p2p_endpoint.lock().await;
        let (endpoint, is_cached) = match ep_guard.as_ref() {
            Some(ep) => (ep.clone(), true),
            None => {
                let ep = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
                    .alpns(vec![rusty_grid_core::transport::GRID_ALPN.to_vec()])
                    .bind()
                    .await
                    .map_err(|e| {
                        GridError::ConnectionFailed(format!(
                            "Failed to bind iroh endpoint: {e}"
                        ))
                    })?;
                *ep_guard = Some(ep.clone());
                (ep, false)
            }
        };
        drop(ep_guard);

        let connect_timeout = self.config.connect_timeout;
        let reset_cached_endpoint = || async {
            if is_cached {
                let mut guard = self.p2p_endpoint.lock().await;
                *guard = None;
            }
        };
        let conn = match tokio::time::timeout(
            connect_timeout,
            endpoint.connect(node_addr, rusty_grid_core::transport::GRID_ALPN),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                reset_cached_endpoint().await;
                return Err(GridError::ConnectionFailed(format!(
                    "P2P connect failed: {e}"
                )));
            }
            Err(_) => {
                reset_cached_endpoint().await;
                return Err(GridError::Timeout {
                    operation: "p2p_connect".into(),
                    duration_secs: connect_timeout.as_secs(),
                });
            }
        };

        // Open the two lanes: Control / Telemetry (Tag 0x01) and Data / Workload (Tag 0x02).
        // A peer that accepts the connection but never completes the stream setup must not
        // be able to park the worker here forever.
        let open_lanes = async {
            let (mut ctrl_send, ctrl_recv) = conn.open_bi().await.map_err(|e| {
                GridError::ConnectionFailed(format!(
                    "Failed to open bidirectional control stream: {e}"
                ))
            })?;
            tokio::io::AsyncWriteExt::write_all(
                &mut ctrl_send,
                &[rusty_grid_core::transport::STREAM_CONTROL],
            )
            .await
            .map_err(GridError::Io)?;
            tokio::io::AsyncWriteExt::flush(&mut ctrl_send)
                .await
                .map_err(GridError::Io)?;

            let (mut data_send, data_recv) = conn.open_bi().await.map_err(|e| {
                GridError::ConnectionFailed(format!(
                    "Failed to open bidirectional data stream: {e}"
                ))
            })?;
            tokio::io::AsyncWriteExt::write_all(
                &mut data_send,
                &[rusty_grid_core::transport::STREAM_DATA],
            )
            .await
            .map_err(GridError::Io)?;
            tokio::io::AsyncWriteExt::flush(&mut data_send)
                .await
                .map_err(GridError::Io)?;
            GridResult::Ok((ctrl_send, ctrl_recv, data_send, data_recv))
        };
        let (ctrl_send, ctrl_recv, data_send, data_recv) =
            tokio::time::timeout(connect_timeout, open_lanes)
                .await
                .map_err(|_| GridError::Timeout {
                    operation: "p2p_open_streams".into(),
                    duration_secs: connect_timeout.as_secs(),
                })??;

        let ctrl_bi = rusty_grid_core::transport::BiStream::new(ctrl_recv, ctrl_send);
        let data_bi = rusty_grid_core::transport::BiStream::new(data_recv, data_send);
        let path_info = rusty_grid_core::transport::inspect_connection_paths(&conn);
        if let Some(ref info) = path_info {
            info!(
                connection_type = %info.connection_type,
                is_relay = info.is_relay,
                is_ip = info.is_ip,
                rtt_ms = info.rtt_ms,
                remote = %info.remote_addr,
                "P2P transport path active (QUIC 2-lane multiplexed)"
            );
        }
        {
            let mut conn_guard = self.p2p_conn.write().await;
            *conn_guard = Some(conn.clone());
        }

        Ok((
            WorkerStreamMode::Multiplexed {
                control: rusty_grid_core::transport::GridStream::P2p(ctrl_bi),
                data: rusty_grid_core::transport::GridStream::P2p(data_bi),
            },
            format!("iroh://{:?}", conn.remote_id()),
        ))
    }

    /// Connects over plain TCP, discovering the Master first when the address is `auto`.
    async fn establish_tcp(&self) -> GridResult<(WorkerStreamMode, String)> {
        let mut target_addr = self.config.master_addr.clone();
        if target_addr.is_empty()
            || target_addr.eq_ignore_ascii_case("auto")
            || target_addr.starts_with("auto:")
        {
            let disc_port = if let Some(port_str) = target_addr.strip_prefix("auto:") {
                port_str
                    .parse::<u16>()
                    .unwrap_or(rusty_grid_core::DEFAULT_DISCOVERY_PORT)
            } else {
                rusty_grid_core::DEFAULT_DISCOVERY_PORT
            };
            info!(
                port = disc_port,
                authenticated = self.config.cluster_secret.is_some(),
                "Master address set to 'auto'; probing local network via UDP discovery..."
            );
            // With a cluster secret only beacons signed for this probe are accepted, so a LAN
            // host that does not know the secret cannot steer the worker to itself.
            match rusty_grid_core::discovery::discover_master_with_secret(
                Duration::from_secs(3),
                disc_port,
                self.config.cluster_secret.as_ref(),
            )
            .await
            {
                Some(beacon) => {
                    info!(
                        master = %beacon.cluster_addr,
                        hostname = %beacon.hostname,
                        "Discovered active Master via UDP LAN beacon"
                    );
                    *self.master_beacon.write().await = Some(beacon.clone());
                    target_addr = beacon.cluster_addr;
                }
                None => {
                    // No silent fallback to a guessed address: an unanswered probe means no
                    // (authentic) master is reachable, and the supervisor retries with backoff.
                    return Err(GridError::ConnectionFailed(format!(
                        "auto-discovery found no Master on UDP port {disc_port}{}; start a Master, or set --master HOST:8088",
                        if self.config.cluster_secret.is_some() {
                            " (only Masters holding the same cluster secret are accepted)"
                        } else {
                            ""
                        }
                    )));
                }
            }
        }

        let connect_timeout = self.config.connect_timeout;
        let stream = tokio::time::timeout(connect_timeout, TcpStream::connect(&target_addr))
            .await
            .map_err(|_| GridError::Timeout {
                operation: format!("tcp_connect {target_addr}"),
                duration_secs: connect_timeout.as_secs(),
            })?
            .map_err(GridError::Io)?;
        // TCP_NODELAY + keepalive: a Master that vanishes without a FIN (Wi-Fi drop, suspended
        // laptop) is noticed in about a minute instead of the OS default of hours.
        if let Err(e) = rusty_grid_core::transport::configure_tcp_stream(&stream) {
            debug!(error = %e, "Could not apply TCP nodelay/keepalive options to the Master socket");
        }
        let remote = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| target_addr.clone());

        if let Ok(peer) = stream.peer_addr() {
            let mut lock = self.master_beacon.write().await;
            if lock.is_none() {
                let peer_ip = peer.ip();
                *lock = Some(rusty_grid_core::discovery::MasterBeacon::new(
                    target_addr.clone(),
                    format!("http://{}:8080", peer_ip),
                    "MasterNode",
                    1,
                ));
            }
        }
        Ok((
            WorkerStreamMode::Single(rusty_grid_core::transport::GridStream::Tcp(stream)),
            remote,
        ))
    }

    /// Conducts a single connection lifecycle: TCP/P2P connect, registration handshake,
    /// heartbeat task spawning, and bidirectional message processing.
    async fn connect_and_run(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
        backoff: &mut ExponentialBackoff,
    ) -> GridResult<()> {
        // Discovery, TCP connect / P2P dial and the handshake are all raced against shutdown (and
        // bounded by `connect_timeout` / `handshake_timeout`), so stopping the worker never has to
        // wait for an unreachable Master.
        let established = tokio::select! {
            biased;
            _ = shutdown_requested(shutdown_rx) => None,
            res = self.establish_stream() => Some(res),
        };
        let Some(established) = established else {
            info!(worker_id = %self.worker_id, "Shutdown requested while connecting to Master");
            return Ok(());
        };
        let (stream_mode, remote_desc) = established?;
        info!(master = %remote_desc, worker_id = %self.worker_id, "Connected to Master; initiating handshake");

        let clock = IoClock::new();
        match stream_mode {
            WorkerStreamMode::Single(stream) => {
                let mut transport = MessageTransport::with_codec(
                    StampedStream::new(stream, Arc::clone(&clock)),
                    self.config.wire_codec,
                );

                // Cluster authentication (when a secret is configured) + atomic registration handshake
                let handshake = tokio::select! {
                    biased;
                    _ = shutdown_requested(shutdown_rx) => None,
                    res = self.perform_handshake(&mut transport) => Some(res),
                };
                let Some(handshake) = handshake else {
                    info!(worker_id = %self.worker_id, "Shutdown requested during handshake");
                    return Ok(());
                };
                let heartbeat_interval = handshake?;
                backoff.reset();
                info!(worker_id = %self.worker_id, "Registration accepted; backoff counter reset");

                // Split transport into independent reader and writer; one stream serves both queues.
                let (writer, reader) = transport.split();
                let mut session = Session::new(clock);
                session.attach_lane(writer, reader, "master", true, true);
                self.run_session(session, heartbeat_interval, shutdown_rx).await
            }
            #[cfg(feature = "p2p")]
            WorkerStreamMode::Multiplexed { control, data } => {
                let mut ctrl_transport = MessageTransport::with_codec(
                    StampedStream::new(control, Arc::clone(&clock)),
                    self.config.wire_codec,
                );
                let data_transport = MessageTransport::with_codec(
                    StampedStream::new(data, Arc::clone(&clock)),
                    self.config.wire_codec,
                );

                // Cluster authentication + handshake over the dedicated control stream (Tag 0x01)
                let handshake = tokio::select! {
                    biased;
                    _ = shutdown_requested(shutdown_rx) => None,
                    res = self.perform_handshake(&mut ctrl_transport) => Some(res),
                };
                let heartbeat_interval = match handshake {
                    Some(Ok(interval)) => interval,
                    outcome => {
                        // The QUIC connection stays cached in `p2p_conn` only while it is in use.
                        *self.p2p_conn.write().await = None;
                        return match outcome {
                            Some(Err(e)) => Err(e),
                            _ => {
                                info!(worker_id = %self.worker_id, "Shutdown requested during handshake");
                                Ok(())
                            }
                        };
                    }
                };
                backoff.reset();
                info!(worker_id = %self.worker_id, "Registration accepted over QUIC control stream; backoff counter reset");

                // Control traffic (heartbeats, telemetry) and bulk data (results) each get their own
                // stream *and* their own writer task, so a result upload cannot hold a heartbeat back.
                let (ctrl_writer, ctrl_reader) = ctrl_transport.split();
                let (data_writer, data_reader) = data_transport.split();
                let mut session = Session::new(clock);
                session.attach_lane(ctrl_writer, ctrl_reader, "control", true, false);
                session.attach_lane(data_writer, data_reader, "data", false, true);
                self.run_session(session, heartbeat_interval, shutdown_rx).await
            }
        }
    }

    /// The event loop of one registered connection: reacts to the Master's frames, watches for a
    /// silently dead peer, reports live mobile telemetry, and obeys the local shutdown signal.
    /// Returns `Ok(())` only for the local shutdown; every other way out is an error the
    /// supervisor answers by reconnecting.
    async fn run_session(
        &mut self,
        mut session: Session,
        heartbeat_interval: Duration,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> GridResult<()> {
        let worker_id = self.worker_id;
        let heartbeat_handle = spawn_heartbeat_task(
            worker_id,
            heartbeat_interval,
            session.control_tx.clone(),
            Arc::clone(&self.heartbeat_tracker),
        );

        let dead_after = self.dead_peer_timeout(heartbeat_interval);
        let mut watchdog = tokio::time::interval(
            (dead_after / 8).clamp(Duration::from_millis(25), Duration::from_secs(1)),
        );
        watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut telemetry = tokio::time::interval(MOBILE_TELEMETRY_CHECK_INTERVAL);
        telemetry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // `Register` just carried the current reading, so only a *change* is worth a message.
        let mut last_reported: Option<MobileLiveStatus> = self.mobile.live_status();
        // Only to a Master that said it can read the report (an older one would drop us for it).
        let report_telemetry = self.master_takes_mobile_telemetry.load(Ordering::Acquire);
        let mut farewell: Option<WorkerMessage> = None;

        let outcome: GridResult<()> = loop {
            tokio::select! {
                event = session.inbound_rx.recv() => {
                    match event {
                        Some(Inbound::Message(msg)) => match *msg {
                            MasterMessage::HeartbeatAck { timestamp } => {
                                self.heartbeat_tracker.record_ack(timestamp, current_epoch_secs());
                            }
                            MasterMessage::Shutdown { reason, .. } => {
                                // The Master is going away, not telling this worker to quit: say
                                // goodbye and reconnect (the supervisor waits for it to come back).
                                warn!(reason = %reason, "Master announced its shutdown; disconnecting and waiting for it to return");
                                self.master_shutdown_announced = true;
                                farewell = Some(WorkerMessage::Disconnecting {
                                    worker_id,
                                    reason: "Master ordered shutdown".into(),
                                });
                                break Err(GridError::ConnectionClosed);
                            }
                            MasterMessage::AssignTask { task } => {
                                debug!(task_id = %task.id, "AssignTask received; dispatching runner");
                                let checkpoint = task.latest_checkpoint.clone();
                                self.handle_assign_task_with_checkpoint(task, checkpoint, session.data_tx.clone()).await;
                            }
                            MasterMessage::AssignTaskWithCheckpoint { task, latest_checkpoint } => {
                                debug!(task_id = %task.id, "AssignTaskWithCheckpoint received; dispatching runner");
                                let checkpoint = latest_checkpoint.or_else(|| task.latest_checkpoint.clone());
                                self.handle_assign_task_with_checkpoint(task, checkpoint, session.data_tx.clone()).await;
                            }
                            MasterMessage::CancelTask { task_id, reason } => {
                                debug!(task_id = %task_id, reason = ?reason, "CancelTask received; aborting task");
                                self.handle_cancel_task(task_id, reason).await;
                            }
                            MasterMessage::RegisterAck { .. } => {
                                warn!("Unexpected RegisterAck during active connection");
                            }
                        },
                        Some(Inbound::Closed(lane)) => {
                            info!(lane, "Master closed the connection (EOF)");
                            break Err(GridError::ConnectionClosed);
                        }
                        Some(Inbound::Failed(lane, e)) => {
                            warn!(lane, error = %e, "Connection error");
                            break Err(e.into());
                        }
                        None => break Err(GridError::ConnectionClosed),
                    }
                }

                // Dead-peer watchdog: nothing received for `dead_after` (and no frame upload making
                // progress) means the Master is gone even though the socket still accepts writes.
                _ = watchdog.tick() => {
                    let silence = session.clock.silence();
                    if silence > dead_after {
                        warn!(
                            silent_secs = silence.as_secs_f32(),
                            limit_secs = dead_after.as_secs_f32(),
                            "Master has been silent for too long; dropping the connection to reconnect"
                        );
                        break Err(GridError::HeartbeatTimeout {
                            worker_id,
                            last_seen_secs: silence.as_secs(),
                        });
                    }
                }

                // Live battery / thermal state: tell the Master whenever it changed.
                _ = telemetry.tick(), if report_telemetry => {
                    let current = self.mobile.live_status();
                    if current != last_reported {
                        if let Some(status) = current {
                            let report = WorkerMessage::MobileTelemetry {
                                worker_id,
                                battery_pct: status.battery_pct,
                                is_charging: status.is_charging,
                                thermal_throttled: status.thermal_throttled,
                            };
                            if session.control_tx.try_send(report).is_ok() {
                                last_reported = current;
                            }
                        }
                    }
                }

                // External shutdown signal
                res = shutdown_rx.changed() => {
                    if res.is_ok() && *shutdown_rx.borrow() {
                        info!(worker_id = %worker_id, "Graceful worker shutdown initiated; notifying Master");
                        farewell = Some(WorkerMessage::Disconnecting {
                            worker_id,
                            reason: "Graceful worker shutdown".into(),
                        });
                        break Ok(());
                    } else if res.is_err() {
                        break Ok(());
                    }
                }
            }
        };

        // Cleanly terminate heartbeat task and the lanes
        heartbeat_handle.stop().await;
        session.close(farewell).await;
        #[cfg(feature = "p2p")]
        {
            let mut conn_guard = self.p2p_conn.write().await;
            *conn_guard = None;
        }

        outcome
    }

    /// Executes the cluster authentication (when a secret is configured) and the atomic
    /// registration handshake over the transport.
    ///
    /// With a secret, `client_handshake` runs first, so nothing about this worker is sent to a
    /// Master that cannot prove it holds the same secret. Without one, a Master that demands
    /// authentication answers the `Register` with a rejection that surfaces as
    /// `GridError::AuthRequired`. `AuthFailed`/`AuthRequired` are permanent for a given secret:
    /// the supervisor in [`WorkerClient::run`] backs off for at least [`AUTH_FAILURE_MIN_BACKOFF`].
    ///
    /// The capabilities in `Register` carry the *current* battery / thermal state, not the
    /// snapshot taken at startup.
    pub async fn perform_handshake<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        transport: &mut MessageTransport<S>,
    ) -> GridResult<Duration> {
        if let Some(secret) = self.config.cluster_secret.as_ref() {
            // The session key is unused until an AEAD record layer exists (see cluster_auth docs).
            let _session_key = tokio::time::timeout(
                self.config.handshake_timeout,
                client_handshake(transport, secret),
            )
            .await
            .map_err(|_| GridError::Timeout {
                operation: "cluster_authentication".into(),
                duration_secs: self.config.handshake_timeout.as_secs(),
            })??;
            debug!(worker_id = %self.worker_id, "Master proved knowledge of the cluster secret");
        }

        // Whatever the previous connection's Master could do says nothing about this one.
        self.master_takes_mobile_telemetry.store(false, Ordering::Release);
        let mut capabilities = self.capabilities.clone();
        capabilities.mobile = self.mobile.snapshot();
        let register_msg = WorkerMessage::Register {
            worker_id: self.worker_id,
            capabilities,
        };

        transport.send_msg(&register_msg).await.map_err(|e| {
            GridError::HandshakeFailed(format!("Failed to send Register message: {e}"))
        })?;

        debug!(worker_id = %self.worker_id, "WorkerMessage::Register dispatched; awaiting RegisterAck");

        let maybe_ack = tokio::time::timeout(
            self.config.handshake_timeout,
            transport.recv_msg::<MasterMessage>(),
        )
        .await
        .map_err(|_| GridError::Timeout {
            operation: "registration_handshake".into(),
            duration_secs: self.config.handshake_timeout.as_secs(),
        })?
        .map_err(|e| match e {
            // A Master that requires the cluster secret answers our un-authenticated Register
            // with an authentication REJECT frame; keep that actionable error intact.
            ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH) => GridError::from(e),
            e => GridError::HandshakeFailed(format!("Transport error awaiting RegisterAck: {e}")),
        })?
        .ok_or(GridError::ConnectionClosed)?;

        match maybe_ack {
            MasterMessage::RegisterAck {
                accepted,
                worker_id: ack_wid,
                heartbeat_interval_secs,
                message,
            } => {
                if !accepted {
                    let reason =
                        message.unwrap_or_else(|| "Registration rejected by master".into());
                    warn!(worker_id = %self.worker_id, reason = %reason, "Master rejected registration");
                    return Err(GridError::RegistrationRejected(reason));
                }

                if ack_wid != self.worker_id {
                    return Err(GridError::HandshakeFailed(format!(
                        "Worker ID mismatch in RegisterAck: expected {}, got {}",
                        self.worker_id, ack_wid
                    )));
                }

                let interval = if heartbeat_interval_secs > 0 {
                    Duration::from_secs(heartbeat_interval_secs)
                } else {
                    self.config.default_heartbeat_interval
                };

                let takes_telemetry =
                    master_features::announced(message.as_deref(), master_features::MOBILE_TELEMETRY);
                self.master_takes_mobile_telemetry
                    .store(takes_telemetry, Ordering::Release);
                if !takes_telemetry && self.mobile.live_status().is_some() {
                    info!(
                        worker_id = %self.worker_id,
                        "This Master does not announce live battery/thermal reports (older build?); sending them only at registration"
                    );
                }

                info!(
                    worker_id = %self.worker_id,
                    heartbeat_interval_secs = interval.as_secs(),
                    "Worker successfully registered with Master"
                );

                Ok(interval)
            }
            unexpected => Err(GridError::UnexpectedMessage {
                expected: "RegisterAck".into(),
                actual: unexpected.variant_name().into(),
            }),
        }
    }
}

/// Target coordinator reference for the worker standby redirection portal.
#[derive(Clone)]
pub enum PortalTarget {
    Static(String),
    Dynamic(Arc<tokio::sync::RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>>),
}

impl From<String> for PortalTarget {
    fn from(s: String) -> Self {
        PortalTarget::Static(s)
    }
}

impl From<&str> for PortalTarget {
    fn from(s: &str) -> Self {
        PortalTarget::Static(s.to_string())
    }
}

impl From<Arc<tokio::sync::RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>>>
    for PortalTarget
{
    fn from(a: Arc<tokio::sync::RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>>) -> Self {
        PortalTarget::Dynamic(a)
    }
}

/// Port of the Master's dashboard, which the portal sends visitors to.
const PORTAL_DASHBOARD_PORT: u16 = 8080;
/// The portal reads at most this much of a request (the request line and headers).
const PORTAL_MAX_REQUEST_BYTES: usize = 8192;
/// A client that has not sent its headers after this long is dropped.
const PORTAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
/// After answering, how long the portal drains the rest of the request so that closing the socket
/// does not turn into a TCP reset that wipes the response.
const PORTAL_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);
/// Simultaneous portal connections; the rest are dropped instead of piling up tasks and sockets.
const PORTAL_MAX_CONNECTIONS: usize = 64;

/// Host part of a `host[:port]` / `[v6][:port]` address, if it is a plain valid host.
fn portal_host_of(addr: &str) -> Option<String> {
    let host = if let Some(bracketed) = addr.strip_prefix('[') {
        bracketed.split_once(']')?.0
    } else {
        addr.split(':').next().unwrap_or_default()
    };
    is_valid_host(host).then(|| host.to_string())
}

/// `host` as it is written inside a URL authority (IPv6 literals in brackets).
fn url_host(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

/// True for hosts that only mean something on the machine they are read on.
fn is_local_only_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.eq_ignore_ascii_case("auto")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback() || ip.is_unspecified())
}

/// Host of a validated `scheme://host[:port]` origin, without brackets.
fn origin_host(origin: &str) -> Option<String> {
    portal_host_of(origin.split_once("://")?.1)
}

/// Turns a beacon into the `(dashboard origin, cluster address)` the portal reports, or `None`
/// when there is nothing safe and useful to send this visitor to. The dashboard origin is empty
/// when the Master has no dashboard that is reachable from the network.
///
/// Both strings come from a UDP datagram or a config file and are validated here (strict
/// `http(s)://host[:port]` and `host:port`), so they are safe to put into headers, HTML and JS.
/// A dashboard on loopback means nothing to a visitor on another machine; in that case the
/// cluster host is used instead, unless that is loopback as well (the Master runs on this very
/// machine, so the portal has nowhere better to point than itself).
fn portal_master_from_beacon(
    beacon: &rusty_grid_core::discovery::MasterBeacon,
    client_ip: IpAddr,
) -> Option<(String, String)> {
    let cluster = sanitize_host_port(&beacon.cluster_addr)?;
    let cluster_host = portal_host_of(&cluster)?;
    // An empty dashboard URL is the Master saying that no dashboard is reachable from the network
    // (it listens on loopback only, or is off). Guessing `http://<cluster host>:8080` would send
    // the visitor to a dead address: report the Master (empty dashboard) and let the portal keep
    // its standby page.
    if beacon.web_ui_url.trim().is_empty() {
        if is_local_only_host(&cluster_host) && !client_ip.is_loopback() {
            return None;
        }
        return Some((String::new(), cluster));
    }
    let advertised = sanitize_http_origin(&beacon.web_ui_url);
    let ui = match advertised {
        Some(ui)
            if client_ip.is_loopback()
                || !origin_host(&ui).is_some_and(|h| is_local_only_host(&h)) =>
        {
            ui
        }
        _ => {
            if is_local_only_host(&cluster_host) && !client_ip.is_loopback() {
                return None;
            }
            format!("http://{}:{}", url_host(&cluster_host), PORTAL_DASHBOARD_PORT)
        }
    };
    Some((ui, cluster))
}

async fn resolve_portal_master(
    target: &PortalTarget,
    secret: Option<&ClusterSecret>,
    client_ip: IpAddr,
) -> Option<(String, String)> {
    match target {
        PortalTarget::Dynamic(beacon_state) => {
            let guard = beacon_state.read().await;
            guard
                .as_ref()
                .and_then(|beacon| portal_master_from_beacon(beacon, client_ip))
        }
        PortalTarget::Static(addr) => {
            if addr.is_empty() || addr.eq_ignore_ascii_case("auto") || addr.starts_with("auto:") {
                let port = if let Some(port_str) = addr.strip_prefix("auto:") {
                    port_str
                        .parse::<u16>()
                        .unwrap_or(rusty_grid_core::DEFAULT_DISCOVERY_PORT)
                } else {
                    rusty_grid_core::DEFAULT_DISCOVERY_PORT
                };
                let beacon = rusty_grid_core::discovery::discover_master_with_secret(
                    Duration::from_millis(350),
                    port,
                    secret,
                )
                .await?;
                portal_master_from_beacon(&beacon, client_ip)
            } else {
                let cluster = sanitize_host_port(addr)
                    .or_else(|| is_valid_host(addr).then(|| format!("{addr}:8088")))?;
                let host = portal_host_of(&cluster)?;
                // The operator configured this address, but a loopback one is only meaningful to
                // a visitor on this machine.
                if is_local_only_host(&host) && !client_ip.is_loopback() {
                    return None;
                }
                Some((
                    format!("http://{}:{}", url_host(&host), PORTAL_DASHBOARD_PORT),
                    cluster,
                ))
            }
        }
    }
}

fn worker_portal_standby_html() -> String {
    r#"<!DOCTYPE html>
<html lang="vi" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
  <title>OxideSwarm Standby Node</title>
  <style>
    body { background:#090d16; color:#e2e8f0; font-family:-apple-system,BlinkMacSystemFont,sans-serif; text-align:center; padding:40px 16px; margin:0; }
    .card { max-width:480px; margin:0 auto; background:#111827; border:1px solid #1f293d; border-radius:12px; padding:24px; box-shadow:0 4px 20px rgba(0,0,0,0.5); }
    .title { font-size:18px; font-weight:700; color:#38bdf8; margin-bottom:12px; }
    .desc { font-size:14px; color:#94a3b8; line-height:1.6; margin-bottom:20px; }
    .spinner { display:inline-block; width:36px; height:36px; border:3px solid rgba(56,189,248,0.2); border-top-color:#38bdf8; border-radius:50%; animation:spin 1s linear infinite; margin-bottom:16px; }
    @keyframes spin { 100% { transform:rotate(360deg); } }
    .btn { display:inline-block; background:#0284c7; color:#fff; padding:10px 20px; border-radius:6px; text-decoration:none; font-weight:600; font-size:13px; cursor:pointer; border:none; }
    .status { font-size:12px; color:#f59e0b; margin-top:14px; font-family:monospace; }
  </style>
</head>
<body>
  <div class="card">
    <div class="spinner"></div>
    <div class="title">OxideSwarm Standby Node (Worker)</div>
    <div class="desc">Thiết bị này đang ở vai trò <strong>Worker</strong>. Đang tự động dò tìm Master đang hoạt động trong mạng LAN...</div>
    <button class="btn" onclick="probeCluster()">Thử kết nối lại ngay</button>
    <div class="status" id="scanStatus">Đang quét cụm...</div>
  </div>
  <script>
    // Asks this worker (same origin) which Master it is connected to; the answer is validated
    // server-side, and checked again here before the browser is sent anywhere.
    const safeOrigin = /^https?:\/\/[A-Za-z0-9._\-]+(:[0-9]{1,5})?$|^https?:\/\/\[[0-9A-Fa-f:.]+\](:[0-9]{1,5})?$/;
    async function probeCluster() {
      const s = document.getElementById('scanStatus');
      if (s) s.textContent = 'Đang kiểm tra các node...';
      try {
        const res = await fetch('/api/status', { cache: 'no-store' });
        if (res.ok) {
          const data = await res.json();
          const url = data && data.redirect_url;
          if (typeof url === 'string' && safeOrigin.test(url)) {
            if (s) s.textContent = 'Đã tìm thấy Master tại ' + url + '! Đang chuyển hướng...';
            window.location.replace(url);
            return;
          }
        }
      } catch (_) {}
      if (s) s.textContent = 'Chưa phát hiện Master. Sẽ tự động thử lại sau 2 giây...';
    }
    probeCluster();
    setInterval(probeCluster, 2000);
  </script>
</body>
</html>"#
    .to_string()
}

/// Wraps `body` in a complete `Connection: close` HTTP response. The portal never needs
/// cross-origin access (its standby page only talks to its own origin), so it sends no CORS header.
fn portal_http_response(status: &str, content_type: &str, extra_headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n{extra_headers}X-Content-Type-Options: nosniff\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// The JSON answer to `GET /api/status`.
fn portal_status_response(master: Option<&(String, String)>) -> String {
    let body = match master {
        // A known Master without a dashboard to send the visitor to.
        Some((web_ui, cluster)) if web_ui.is_empty() => serde_json::json!({
            "role": "WORKER_PORTAL",
            "status": "connected_to_master",
            "master_cluster_addr": cluster,
            "master_web_ui": null,
            "redirect_url": null,
        }),
        Some((web_ui, cluster)) => serde_json::json!({
            "role": "WORKER_PORTAL",
            "status": "connected_to_master",
            "master_cluster_addr": cluster,
            "master_web_ui": web_ui,
            "redirect_url": web_ui,
        }),
        None => serde_json::json!({
            "role": "WORKER_PORTAL",
            "status": "searching_master",
            "master_cluster_addr": null,
            "master_web_ui": null,
            "redirect_url": null,
        }),
    };
    portal_http_response("200 OK", "application/json", "", &body.to_string())
}

/// The redirect to the Master, or the standby page if either value fails validation. Re-validates
/// its inputs, so it is safe whatever the caller passes.
fn portal_redirect_response(web_ui_url: &str, cluster_addr: &str) -> String {
    let Some(web_ui) = sanitize_http_origin(web_ui_url) else {
        return portal_standby_response();
    };
    // The cluster address is informational; a value that fails validation is simply left out.
    let master_header = sanitize_host_port(cluster_addr)
        .map(|c| format!("X-OxideSwarm-Master: {c}\r\n"))
        .unwrap_or_default();
    // `web_ui` is `scheme://host[:port]` over [A-Za-z0-9._:-] and brackets: nothing in it can
    // end a header line, an HTML attribute or a JavaScript string.
    let html_body = format!(
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><meta http-equiv=\"refresh\" content=\"0; url={0}\"><title>OxideSwarm Auto-Redirect</title></head><body style=\"font-family:-apple-system,BlinkMacSystemFont,sans-serif;text-align:center;padding:50px;background:#090d16;color:#e2e8f0;\"><h2>OxideSwarm Node (Worker)</h2><p>Thiết bị này đang ở vai trò Worker. Đang chuyển hướng sang Master tại <a style=\"color:#38bdf8;\" href=\"{0}\">{0}</a>...</p><script>window.location.replace(\"{0}\");</script></body></html>",
        web_ui
    );
    portal_http_response(
        "307 Temporary Redirect",
        "text/html; charset=utf-8",
        &format!("Location: {web_ui}\r\n{master_header}"),
        &html_body,
    )
}

fn portal_standby_response() -> String {
    portal_http_response(
        "200 OK",
        "text/html; charset=utf-8",
        "",
        &worker_portal_standby_html(),
    )
}

/// Reads the request line (and headers) of a portal visitor: up to the blank line, at most
/// [`PORTAL_MAX_REQUEST_BYTES`], within [`PORTAL_REQUEST_TIMEOUT`]. Returns the request line.
async fn read_portal_request_line<S: AsyncRead + Unpin>(stream: &mut S) -> Option<String> {
    read_portal_request_line_with(stream, PORTAL_REQUEST_TIMEOUT).await
}

async fn read_portal_request_line_with<S: AsyncRead + Unpin>(
    stream: &mut S,
    timeout: Duration,
) -> Option<String> {
    use tokio::io::AsyncReadExt;
    let read_head = async {
        let mut head = Vec::with_capacity(512);
        let mut chunk = [0u8; 1024];
        loop {
            let n = stream.read(&mut chunk).await.ok()?;
            if n == 0 {
                break;
            }
            head.extend_from_slice(&chunk[..n]);
            let complete = head.windows(4).any(|w| w == b"\r\n\r\n")
                || head.windows(2).any(|w| w == b"\n\n");
            if complete || head.len() >= PORTAL_MAX_REQUEST_BYTES {
                break;
            }
        }
        head.truncate(PORTAL_MAX_REQUEST_BYTES);
        Some(head)
    };
    let head = tokio::time::timeout(timeout, read_head).await.ok()??;
    let text = String::from_utf8_lossy(&head);
    let line = text.lines().next()?.trim();
    (!line.is_empty()).then(|| line.to_string())
}

/// True if the request line asks for `/api/status` (with or without a query string).
fn is_status_request(request_line: &str) -> bool {
    let mut parts = request_line.split_whitespace();
    let _method = parts.next();
    let Some(target) = parts.next() else {
        return false;
    };
    let path = target.split(['?', '#']).next().unwrap_or_default();
    path == "/api/status"
}

async fn handle_portal_connection(
    mut stream: TcpStream,
    client_ip: IpAddr,
    target: PortalTarget,
    secret: Option<ClusterSecret>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let Some(request_line) = read_portal_request_line(&mut stream).await else {
        return;
    };
    let master = resolve_portal_master(&target, secret.as_ref(), client_ip).await;
    let response = if is_status_request(&request_line) {
        portal_status_response(master.as_ref())
    } else {
        match master {
            Some((web_ui_url, cluster_addr)) => portal_redirect_response(&web_ui_url, &cluster_addr),
            None => portal_standby_response(),
        }
    };
    let _ = stream.write_all(response.as_bytes()).await;
    // Half-close, then swallow whatever else the client sent: closing a socket that still has
    // unread data resets the connection and can discard the response before the client reads it.
    let _ = stream.shutdown().await;
    let mut sink = [0u8; 1024];
    let _ = tokio::time::timeout(PORTAL_DRAIN_TIMEOUT, async {
        while matches!(stream.read(&mut sink).await, Ok(n) if n > 0) {}
    })
    .await;
}

/// Spawns a lightweight HTTP portal on `portal_port` that redirects incoming browser visits
/// (e.g. from smartphones or bookmarks) to the active Master node.
pub async fn spawn_worker_redirection_portal(
    portal_port: u16,
    target: impl Into<PortalTarget>,
    shutdown_rx: watch::Receiver<bool>,
) {
    spawn_worker_redirection_portal_with_secret(portal_port, target, None, shutdown_rx).await;
}

/// Like [`spawn_worker_redirection_portal`], with the cluster secret to use when the portal has to
/// discover the Master itself (`auto` target): only a Master holding the secret is believed.
pub async fn spawn_worker_redirection_portal_with_secret(
    portal_port: u16,
    target: impl Into<PortalTarget>,
    secret: Option<ClusterSecret>,
    shutdown_rx: watch::Receiver<bool>,
) {
    use std::net::SocketAddr;
    let bind_addr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        portal_port,
    );
    let listener = match tokio::net::TcpListener::bind(bind_addr).await {
        Ok(l) => l,
        Err(e) => {
            debug!(
                port = portal_port,
                error = %e,
                "Worker standby redirection portal not started (port 8080 likely bound by Master or other service)"
            );
            return;
        }
    };

    info!(
        portal_port = portal_port,
        "Worker standby HTTP redirection portal active on port 8080"
    );

    run_portal(
        listener,
        target.into(),
        secret,
        shutdown_rx,
        PORTAL_MAX_CONNECTIONS,
    )
    .await;
}

/// Serves the portal on an already bound listener until shutdown, with at most
/// `max_connections` requests in flight.
async fn run_portal(
    listener: tokio::net::TcpListener,
    target: PortalTarget,
    secret: Option<ClusterSecret>,
    mut shutdown_rx: watch::Receiver<bool>,
    max_connections: usize,
) {
    let slots = Arc::new(Semaphore::new(max_connections));
    loop {
        tokio::select! {
            res = listener.accept() => {
                if let Ok((stream, client_addr)) = res {
                    // Over the cap the connection is simply dropped.
                    let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
                        continue;
                    };
                    let target = target.clone();
                    let secret = secret.clone();
                    tokio::spawn(async move {
                        handle_portal_connection(stream, client_addr.ip(), target, secret).await;
                        drop(slot);
                    });
                }
            }
            res = shutdown_rx.changed() => {
                if (res.is_ok() && *shutdown_rx.borrow()) || res.is_err() {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::task::TaskRequirements;

    #[test]
    fn test_worker_config_builder() {
        let id = Uuid::new_v4();
        let config = WorkerConfig::new("127.0.0.1:8088")
            .with_worker_id(id)
            .with_name("test-node")
            .with_cores(16)
            .with_simulate_gpu(true)
            .with_handshake_timeout(Duration::from_secs(10))
            .with_sandbox_base_dir(PathBuf::from("/tmp/custom_sb"))
            .with_keep_sandboxes(true)
            .with_max_output_bytes(5 * 1024 * 1024)
            .with_tags(vec!["fast".into(), "gpu".into()]);

        assert_eq!(config.master_addr, "127.0.0.1:8088");
        assert_eq!(config.worker_id, Some(id));
        assert_eq!(config.name.as_deref(), Some("test-node"));
        assert_eq!(config.cores, Some(16));
        assert!(config.simulate_gpu);
        assert_eq!(
            config.sandbox_base_dir,
            Some(PathBuf::from("/tmp/custom_sb"))
        );
        assert!(config.keep_sandboxes);
        assert_eq!(config.max_output_bytes, 5 * 1024 * 1024);
        assert_eq!(config.tags.len(), 2);
    }

    // ---- GPU advertising: `--gpu` is a request, only an engine that works is a claim ----------
    // (These read `RUSTY_GRID_ENABLE_PHYSICAL_GPU` through the policy; run them without it set.)

    fn gpu_test_client(config: WorkerConfig) -> WorkerClient {
        WorkerClient::new(config.with_name("gpu-policy-test").with_cores(2))
    }

    #[test]
    fn plain_worker_advertises_no_gpu_and_does_not_look_for_one() {
        // The library default: a test cluster on a laptop must not grow GPU workers, even in a
        // build that has the wgpu engine and a GPU in the machine.
        let client = gpu_test_client(WorkerConfig::new("127.0.0.1:9999"));
        assert!(!client.capabilities().can_execute_gpu());
        assert!(client.capabilities().gpu_device_name.is_none());
        assert!(client.gpu_notices().is_empty());
    }

    #[test]
    fn gpu_flag_advertises_a_gpu_only_if_this_binary_can_use_one() {
        let client = gpu_test_client(WorkerConfig::new("127.0.0.1:9999").with_gpu(true));
        let caps = client.capabilities();
        assert!(!caps.is_simulated_gpu, "--gpu never means the simulator");
        assert_eq!(caps.has_gpu, caps.can_execute_gpu());
        if !crate::gpu::WGPU_COMPILED {
            // Used to be: has_gpu = true, name "Physical GPU (Manual Override)", tasks run on the CPU.
            assert!(!caps.has_gpu);
            assert!(caps.gpu_device_name.is_none());
            let notices = client.gpu_notices();
            assert_eq!(notices.len(), 1, "{notices:?}");
            assert_eq!(notices[0].level, NoticeLevel::Warn);
            assert!(notices[0].message.contains("--features gpu-wgpu"), "{}", notices[0].message);
        } else if caps.has_gpu {
            let name = caps.gpu_device_name.as_deref().expect("the adapter's name");
            assert_ne!(name, "Physical GPU (Manual Override)");
            assert!(client.gpu_notices()[0].message.contains(name));
        } else {
            assert_eq!(client.gpu_notices()[0].level, NoticeLevel::Warn);
        }
    }

    #[test]
    fn gpu_autodetect_follows_the_same_rule_and_is_quiet_when_nothing_was_insisted_on() {
        let client = gpu_test_client(WorkerConfig::new("127.0.0.1:9999").with_gpu_autodetect(true));
        let caps = client.capabilities();
        assert!(!caps.is_simulated_gpu);
        assert_eq!(caps.has_gpu, caps.can_execute_gpu());
        if !crate::gpu::WGPU_COMPILED {
            assert!(!caps.has_gpu);
        }
        // Without --gpu nothing is a warning: it just says what happened.
        assert!(client.gpu_notices().iter().all(|n| n.level == NoticeLevel::Info));
        assert_eq!(client.gpu_notices().len(), 1);
    }

    #[test]
    fn no_gpu_and_simulate_gpu_are_unaffected_by_the_engine() {
        let none = gpu_test_client(
            WorkerConfig::new("127.0.0.1:9999")
                .with_gpu(true)
                .with_gpu_autodetect(true)
                .with_no_gpu(true),
        );
        assert!(!none.capabilities().can_execute_gpu());

        let simulated = gpu_test_client(
            WorkerConfig::new("127.0.0.1:9999")
                .with_simulate_gpu(true)
                .with_gpu_autodetect(true),
        );
        let caps = simulated.capabilities();
        assert!(caps.has_gpu && caps.is_simulated_gpu);
        assert_eq!(caps.gpu_device_name.as_deref(), Some("Simulated Virtual GPU (Matrix Engine)"));
        assert!(simulated.gpu_notices()[0].message.contains("SIMULATED"));
    }

    #[test]
    fn test_worker_config_cluster_secret_and_connect_timeout() {
        let default = WorkerConfig::new("127.0.0.1:8088");
        assert!(default.cluster_secret.is_none());
        assert_eq!(default.connect_timeout, DEFAULT_CONNECT_TIMEOUT);

        let secret = ClusterSecret::from_bytes(&[7u8; 32]).unwrap();
        let config = WorkerConfig::new("127.0.0.1:8088")
            .with_cluster_secret(secret.clone())
            .with_connect_timeout(Duration::from_secs(2));
        assert_eq!(config.cluster_secret.as_ref(), Some(&secret));
        assert_eq!(config.connect_timeout, Duration::from_secs(2));
        // Debug output (e.g. in logs) never reveals the secret.
        assert!(!format!("{config:?}").contains(&secret.to_hex()));
    }

    #[tokio::test]
    async fn test_shutdown_requested_resolves_on_flag_or_dropped_sender_only() {
        // Pending while the flag is false and the sender is alive.
        let (tx, mut rx) = watch::channel(false);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), shutdown_requested(&mut rx))
                .await
                .is_err()
        );

        // Resolves when the flag flips ...
        let waiter = tokio::spawn(async move {
            shutdown_requested(&mut rx).await;
        });
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("flag must wake the waiter")
            .unwrap();

        // ... when it was already set before waiting, and when the sender is gone.
        let (tx, mut rx) = watch::channel(true);
        shutdown_requested(&mut rx).await;
        drop(tx);
        let (tx, mut rx) = watch::channel(false);
        drop(tx);
        shutdown_requested(&mut rx).await;
    }

    #[test]
    fn test_worker_client_initialization_preserves_id_and_capabilities() {
        let id = Uuid::new_v4();
        let config = WorkerConfig::new("127.0.0.1:9999")
            .with_worker_id(id)
            .with_name("gpu-worker")
            .with_cores(8)
            .with_simulate_gpu(true);

        let client = WorkerClient::new(config);
        assert_eq!(client.worker_id(), id);
        assert_eq!(client.capabilities().cpu_cores, 8);
        assert!(client.capabilities().can_execute_gpu());
        assert!(client.capabilities().is_simulated_gpu);
        assert_eq!(client.concurrency_semaphore().available_permits(), 8);
    }

    #[tokio::test]
    async fn test_worker_client_handle_assign_and_cancel_task() {
        let config = WorkerConfig::new("127.0.0.1:9999").with_cores(4);
        let client = WorkerClient::new(config);
        let (tx, mut rx) = mpsc::channel(16);

        // Sleep for 60s
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::command("sleep", vec!["60".into()]),
            TaskRequirements::generic(1, 30),
        );
        let task_id = task.id;

        client.handle_assign_task(task, tx).await;

        // Verify task progress message emitted
        let progress = rx.recv().await.expect("progress message");
        match progress {
            WorkerMessage::TaskProgress {
                task_id: pid,
                status,
                ..
            } => {
                assert_eq!(pid, task_id);
                assert_eq!(status, TaskStatus::Running);
            }
            other => panic!("Expected TaskProgress, got {other:?}"),
        }

        // Cancel the task
        client
            .handle_cancel_task(task_id, Some("test cancellation".into()))
            .await;

        // Verify task result emitted with cancelled exit code
        let result = rx.recv().await.expect("result message");
        match result {
            WorkerMessage::TaskResult {
                task_id: rid,
                exit_code,
                error,
                ..
            } => {
                assert_eq!(rid, task_id);
                assert_eq!(exit_code, crate::runner::EXIT_CODE_CANCELLED);
                assert_eq!(exit_code, 130);
                assert!(error.unwrap().contains("test cancellation"));
            }
            other => panic!("Expected TaskResult, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_assign_task_that_was_cancelled_before_it_arrived_is_refused_not_run() {
        let config = WorkerConfig::new("127.0.0.1:9999").with_cores(4);
        let client = WorkerClient::new(config);
        let (tx, mut rx) = mpsc::channel(16);
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::command("sleep", vec!["60".into()]),
            TaskRequirements::generic(1, 30),
        );
        let task_id = task.id;

        // The Master's CancelTask overtakes the task's AssignTask.
        client
            .handle_cancel_task(task_id, Some("client cancelled".into()))
            .await;
        client.handle_assign_task(task, tx.clone()).await;

        match rx.recv().await.expect("a cancelled result, not progress") {
            WorkerMessage::TaskResult {
                task_id: rid,
                exit_code,
                ..
            } => {
                assert_eq!(rid, task_id);
                assert_eq!(exit_code, crate::runner::EXIT_CODE_CANCELLED);
            }
            other => panic!("Expected a cancelled TaskResult, got {other:?}"),
        }
        assert!(
            client.active_task_table.read().await.is_empty(),
            "the cancelled task must not have been started"
        );

        // The memory is per cancel: a later, legitimate assignment of the same id runs.
        let again = Task::new(
            rusty_grid_core::task::TaskSpec::command("true", vec![]),
            TaskRequirements::generic(1, 30),
        );
        let mut again = again;
        again.id = task_id;
        client.handle_assign_task(again, tx).await;
        match rx.recv().await.expect("progress of the second run") {
            WorkerMessage::TaskProgress { task_id: pid, .. } => assert_eq!(pid, task_id),
            other => panic!("Expected TaskProgress, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_worker_client_concurrency_bounded_by_cores() {
        let config = WorkerConfig::new("127.0.0.1:9999").with_cores(2);
        let client = WorkerClient::new(config);
        assert_eq!(client.concurrency_semaphore().available_permits(), 2);
        assert_eq!(client.heartbeat_tracker().active_tasks(), 0);

        let (tx, mut rx) = mpsc::channel(16);

        // Spawn 2 tasks that take permits
        let t1 = Task::new(
            rusty_grid_core::task::TaskSpec::builtin_test("sleep", 0),
            TaskRequirements::generic(1, 10),
        );
        let t2 = Task::new(
            rusty_grid_core::task::TaskSpec::builtin_test("sleep", 0),
            TaskRequirements::generic(1, 10),
        );

        client.handle_assign_task(t1, tx.clone()).await;
        client.handle_assign_task(t2, tx.clone()).await;

        // Drain messages
        let mut progress_count = 0;
        let mut result_count = 0;

        for _ in 0..4 {
            match rx.recv().await.expect("message") {
                WorkerMessage::TaskProgress { .. } => progress_count += 1,
                WorkerMessage::TaskResult { .. } => result_count += 1,
                _ => {}
            }
        }

        assert_eq!(progress_count, 2);
        assert_eq!(result_count, 2);

        // Give a moment for ActiveTaskGuard drop tasks to execute
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(client.concurrency_semaphore().available_permits(), 2);
        assert_eq!(client.heartbeat_tracker().active_tasks(), 0);
    }

    #[tokio::test]
    async fn test_worker_client_mobile_battery_rejection() {
        let client = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999"));
        // Inject low battery mobile capabilities. The gate reads the live handle, not the
        // snapshot detected at startup, so that is where a platform app (or the poller) writes.
        client
            .mobile_telemetry()
            .set(Some(rusty_grid_core::capabilities::MobileCapabilities {
                os_version: "Android 14".into(),
                soc_model: "Snapdragon 8 Gen 3".into(),
                battery_pct: Some(10),    // Under 15%
                is_charging: Some(false), // Discharging
                thermal_throttled: false,
            }));

        let (tx, mut rx) = mpsc::channel(16);
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::builtin_test("sleep", 0),
            TaskRequirements::generic(1, 10),
        );
        let task_id = task.id;

        client.handle_assign_task(task, tx).await;

        let msg = rx.recv().await.expect("result message");
        match msg {
            WorkerMessage::TaskResult {
                task_id: rid,
                exit_code,
                error,
                ..
            } => {
                assert_eq!(rid, task_id);
                assert_eq!(exit_code, 1);
                assert!(error.unwrap().to_lowercase().contains("battery"));
            }
            other => panic!("Expected TaskResult, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_worker_client_backoff_reset_on_register_ack() {
        let (client_io, master_io) = tokio::io::duplex(4096);
        let config = WorkerConfig::new("127.0.0.1:9999");
        let client = WorkerClient::new(config);
        let worker_id = client.worker_id();

        // Simulate master receiving Register and responding with RegisterAck(accepted: true)
        tokio::spawn(async move {
            let mut master_transport = MessageTransport::new(master_io);
            let msg = master_transport
                .recv_msg::<WorkerMessage>()
                .await
                .unwrap()
                .unwrap();
            match msg {
                WorkerMessage::Register { worker_id: wid, .. } => {
                    assert_eq!(wid, worker_id);
                    master_transport
                        .send_msg(&MasterMessage::RegisterAck {
                            accepted: true,
                            worker_id,
                            heartbeat_interval_secs: 3,
                            message: None,
                        })
                        .await
                        .unwrap();
                }
                other => panic!("Expected Register, got {other:?}"),
            }
        });

        let mut client_transport = MessageTransport::new(client_io);
        let mut backoff = ExponentialBackoff::new(BackoffConfig::default());
        // Simulate previous reconnection failures incrementing backoff attempt
        backoff.next_delay();
        backoff.next_delay();
        assert_eq!(backoff.attempt(), 2);

        let res = client.perform_handshake(&mut client_transport).await;
        assert!(res.is_ok());
        backoff.reset();
        assert_eq!(backoff.attempt(), 0);
    }

    #[tokio::test]
    async fn test_worker_redirection_portal() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let test_port = 18080;
        let master_target = "192.168.1.123:8088".to_string();

        tokio::spawn(async move {
            spawn_worker_redirection_portal(test_port, master_target, shutdown_rx).await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // 1. Test browser navigation / redirect
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", test_port))
            .await
            .unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.contains("307 Temporary Redirect"));
        assert!(resp.contains("Location: http://192.168.1.123:8080"));

        // 2. Test /api/status probe
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", test_port))
            .await
            .unwrap();
        stream
            .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.contains("200 OK"));
        assert!(resp.contains("\"role\":\"WORKER_PORTAL\""));
        assert!(resp.contains("http://192.168.1.123:8080"));

        let _ = shutdown_tx.send(true);
    }

    // ------------------------------------------------------------------------------------------
    // Session lifecycle: task cleanup, duplicate ids, stale sandboxes
    // ------------------------------------------------------------------------------------------

    /// True while a process with `pid` exists (signal 0 only probes).
    #[cfg(unix)]
    fn process_alive(pid: u32) -> bool {
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    /// Waits for the `Running` progress message of `task_id`.
    async fn wait_running(rx: &mut mpsc::Receiver<WorkerMessage>, task_id: TaskId) {
        loop {
            match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Some(WorkerMessage::TaskProgress {
                    task_id: id,
                    status: TaskStatus::Running,
                    ..
                })) if id == task_id => return,
                Ok(Some(_)) => {}
                other => panic!("task {task_id} never started running: {other:?}"),
            }
        }
    }

    fn test_client(dir: &std::path::Path, cores: usize) -> WorkerClient {
        WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9999")
                .with_cores(cores)
                .with_sandbox_base_dir(dir),
        )
    }

    #[tokio::test]
    async fn test_reassigned_task_id_stops_the_old_run_silently_and_the_new_run_owns_the_slot() {
        let dir = tempfile::tempdir().unwrap();
        let client = test_client(dir.path(), 2);
        let (tx, mut rx) = mpsc::channel(32);

        let mut task = Task::new(
            rusty_grid_core::task::TaskSpec::command("sleep", vec!["30".into()]),
            TaskRequirements::generic(1, 60),
        );
        let task_id = task.id;
        client.handle_assign_task(task.clone(), tx.clone()).await;
        wait_running(&mut rx, task_id).await;

        // The same id again (a re-dispatch): the old run must be gone before the new one starts,
        // otherwise both fight over `<sandbox>/<task id>`.
        task.spec = rusty_grid_core::task::TaskSpec::command("echo", vec!["second".into()]);
        client.handle_assign_task(task.clone(), tx.clone()).await;

        let mut results = Vec::new();
        while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(800), rx.recv()).await {
            if let WorkerMessage::TaskResult { exit_code, stdout, error, .. } = msg {
                results.push((exit_code, String::from_utf8_lossy(&stdout.0).to_string(), error));
            }
        }
        assert_eq!(
            results.len(),
            1,
            "exactly one result: the new run's. The superseded run must not report a cancellation (the Master would record the task as cancelled): {results:?}"
        );
        assert_eq!(results[0].0, 0, "the re-dispatch must run, not fail: {results:?}");
        assert!(results[0].1.contains("second"));

        // The old run's cleanup cannot have removed anything of the new run, and both are gone now.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(client.active_task_table().read().await.is_empty());
        assert_eq!(client.concurrency_semaphore().available_permits(), 2);
    }

    #[tokio::test]
    async fn test_cleanup_of_an_old_run_never_removes_the_entry_of_a_newer_one() {
        let table: ActiveTaskTable = Arc::new(RwLock::new(HashMap::new()));
        let task_id = TaskId::new();
        let parked = tokio::spawn(std::future::pending::<()>());
        table.write().await.insert(
            task_id,
            TaskExecutionHandle {
                task_id,
                cancel_tx: None,
                abort_handle: parked.abort_handle(),
                child_pid: Arc::new(AtomicU32::new(0)),
                started_at_epoch_sec: 0,
                attempt: 2,
                suppress_result: Arc::new(AtomicBool::new(false)),
                join: None,
            },
        );

        // The first run of the id (attempt 1) finally ends and its guard cleans up after it.
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        drop(ActiveTaskGuard {
            task_id,
            attempt: 1,
            heartbeat_tracker: Arc::new(HeartbeatTracker::new(Arc::new(AtomicUsize::new(1)))),
            active_task_table: Arc::clone(&table),
            _permit: permit,
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            table.read().await.get(&task_id).map(|h| h.attempt),
            Some(2),
            "the old run's cleanup removed the new run's entry (which would also drop its cancel handle)"
        );

        // The run that owns the entry does remove it.
        remove_active(&table, task_id, 2).await;
        assert!(table.read().await.is_empty());
        parked.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_cancel_all_active_tasks_kills_processes_and_sends_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let client = test_client(dir.path(), 2);
        let (tx, mut rx) = mpsc::channel(32);

        let mut ids = Vec::new();
        let mut pids = Vec::new();
        for _ in 0..2 {
            let task = Task::new(
                rusty_grid_core::task::TaskSpec::command("sleep", vec!["30".into()]),
                TaskRequirements::generic(1, 60),
            );
            ids.push(task.id);
            client.handle_assign_task(task, tx.clone()).await;
        }
        for id in &ids {
            wait_running(&mut rx, *id).await;
        }
        for id in &ids {
            // The `Running` report precedes the spawn of the child by a moment.
            let mut pid = 0;
            for _ in 0..100 {
                pid = client.active_task_table().read().await[id].child_pid.load(Ordering::Acquire);
                if pid != 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_ne!(pid, 0, "child process never started");
            assert!(process_alive(pid));
            pids.push(pid);
        }

        let cancelled = client.cancel_all_active_tasks("test: connection lost").await;
        assert_eq!(cancelled, 2);
        assert!(client.active_task_table().read().await.is_empty());
        assert_eq!(client.concurrency_semaphore().available_permits(), 2, "permits released");
        assert_eq!(client.heartbeat_tracker().active_tasks(), 0);
        for pid in pids {
            assert!(!process_alive(pid), "child {pid} of a cancelled task must be dead, not orphaned");
        }
        // Their results could only go to the dead session: nothing is sent.
        assert!(
            tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err(),
            "no result may be sent for a task the worker cancelled itself"
        );
        // And with nothing running the call is a cheap no-op.
        assert_eq!(client.cancel_all_active_tasks("again").await, 0);
    }

    #[tokio::test]
    async fn test_cancel_all_active_tasks_also_stops_tasks_still_queued_for_a_permit() {
        let dir = tempfile::tempdir().unwrap();
        let client = test_client(dir.path(), 1);
        let (tx, mut rx) = mpsc::channel(32);
        let running = Task::new(
            rusty_grid_core::task::TaskSpec::command("sleep", vec!["30".into()]),
            TaskRequirements::generic(1, 60),
        );
        let queued = Task::new(
            rusty_grid_core::task::TaskSpec::command("sleep", vec!["30".into()]),
            TaskRequirements::generic(1, 60),
        );
        let running_id = running.id;
        client.handle_assign_task(running, tx.clone()).await;
        wait_running(&mut rx, running_id).await;
        client.handle_assign_task(queued, tx.clone()).await;
        assert_eq!(client.active_task_table().read().await.len(), 2);

        assert_eq!(client.cancel_all_active_tasks("test").await, 2);
        assert!(client.active_task_table().read().await.is_empty());
        assert_eq!(client.concurrency_semaphore().available_permits(), 1);
        assert!(tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err());
    }

    #[tokio::test]
    async fn test_a_leftover_sandbox_directory_does_not_fail_the_assignment() {
        let dir = tempfile::tempdir().unwrap();
        let client = test_client(dir.path(), 2);
        let (tx, mut rx) = mpsc::channel(16);
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::command("echo", vec!["fresh".into()]),
            TaskRequirements::generic(1, 30),
        );
        // What a worker killed mid-task (power loss, Android low-memory kill) leaves behind.
        let leftover = dir.path().join(task.id.to_string());
        std::fs::create_dir_all(&leftover).unwrap();
        std::fs::write(leftover.join("half_written.txt"), "stale").unwrap();

        client.handle_assign_task(task.clone(), tx).await;
        loop {
            match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Some(WorkerMessage::TaskResult { exit_code, stdout, error, .. })) => {
                    assert_eq!(
                        exit_code, 0,
                        "a stale sandbox directory must not turn into a terminal task failure: {error:?}"
                    );
                    assert!(String::from_utf8_lossy(&stdout.0).contains("fresh"));
                    break;
                }
                Ok(Some(_)) => {}
                other => panic!("no result: {other:?}"),
            }
        }
        assert!(!leftover.join("half_written.txt").exists());
    }

    #[tokio::test]
    async fn test_with_keep_sandboxes_a_leftover_directory_is_moved_aside_not_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let client = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9999")
                .with_cores(1)
                .with_sandbox_base_dir(dir.path())
                .with_keep_sandboxes(true),
        );
        let (tx, mut rx) = mpsc::channel(16);
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::command("echo", vec!["again".into()]),
            TaskRequirements::generic(1, 30),
        );
        let leftover = dir.path().join(task.id.to_string());
        std::fs::create_dir_all(&leftover).unwrap();
        std::fs::write(leftover.join("kept.txt"), "debug data").unwrap();

        client.handle_assign_task(task.clone(), tx).await;
        loop {
            if let Ok(Some(WorkerMessage::TaskResult { exit_code, .. })) =
                tokio::time::timeout(Duration::from_secs(5), rx.recv()).await
            {
                assert_eq!(exit_code, 0);
                break;
            }
        }
        let moved: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".stale-"))
            .collect();
        assert_eq!(moved.len(), 1, "the earlier attempt's data is preserved");
        assert!(moved[0].path().join("kept.txt").exists());
    }

    // ------------------------------------------------------------------------------------------
    // Lane writer: control traffic is never queued behind task data
    // ------------------------------------------------------------------------------------------

    fn big_result(worker_id: Uuid, bytes: usize) -> WorkerMessage {
        WorkerMessage::from(TaskResult::success(
            worker_id,
            TaskId::new(),
            "x".repeat(bytes),
            1,
            false,
        ))
    }

    struct LaneHarness {
        control_tx: mpsc::Sender<WorkerMessage>,
        data_tx: mpsc::Sender<WorkerMessage>,
        closing_tx: watch::Sender<bool>,
        events_rx: mpsc::Receiver<Inbound>,
        peer: MessageReader<tokio::io::DuplexStream>,
        task: JoinHandle<()>,
    }

    fn spawn_lane(pipe_capacity: usize) -> LaneHarness {
        let (a, b) = tokio::io::duplex(pipe_capacity);
        let (control_tx, control_rx) = mpsc::channel(8);
        let (data_tx, data_rx) = mpsc::channel(8);
        let (events_tx, events_rx) = mpsc::channel(8);
        let (closing_tx, closing_rx) = watch::channel(false);
        let task = tokio::spawn(lane_writer(
            MessageWriter::new(a),
            control_rx,
            data_rx,
            "test",
            closing_rx,
            events_tx,
        ));
        LaneHarness {
            control_tx,
            data_tx,
            closing_tx,
            events_rx,
            peer: MessageReader::new(b),
            task,
        }
    }

    #[tokio::test]
    async fn test_lane_writer_sends_control_messages_ahead_of_queued_data() {
        let mut lane = spawn_lane(1024);
        let worker_id = Uuid::new_v4();
        // `big` blocks mid-write on the tiny pipe (nobody reads yet); `small` waits behind it.
        lane.data_tx.send(big_result(worker_id, 200_000)).await.unwrap();
        lane.data_tx.send(big_result(worker_id, 10)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        // A heartbeat that arrives while `big` is on the wire ...
        lane.control_tx
            .send(WorkerMessage::Heartbeat {
                worker_id,
                timestamp: 1,
                active_tasks: 0,
                cpu_usage_pct: 0.0,
                ram_available_mb: 1,
            })
            .await
            .unwrap();

        // ... goes out right after it, ahead of the data message that was queued earlier.
        let mut order = Vec::new();
        for _ in 0..3 {
            let msg = tokio::time::timeout(Duration::from_secs(5), lane.peer.recv_msg::<WorkerMessage>())
                .await
                .expect("frame in time")
                .unwrap()
                .expect("frame");
            order.push(match msg {
                WorkerMessage::Heartbeat { .. } => "heartbeat".to_string(),
                WorkerMessage::TaskResult { stdout, .. } => format!("result:{}", stdout.0.len()),
                other => panic!("unexpected {other:?}"),
            });
        }
        assert_eq!(order, ["result:200000", "heartbeat", "result:10"]);
        lane.task.abort();
    }

    #[tokio::test]
    async fn test_lane_writer_reports_a_write_failure() {
        let mut lane = spawn_lane(1024);
        // The peer vanished: the next write must surface as an event, not be silently dropped.
        drop(std::mem::replace(&mut lane.peer, MessageReader::new(tokio::io::duplex(1).1)));
        lane.control_tx
            .send(WorkerMessage::Disconnecting {
                worker_id: Uuid::new_v4(),
                reason: "x".into(),
            })
            .await
            .unwrap();
        match tokio::time::timeout(Duration::from_secs(2), lane.events_rx.recv()).await {
            Ok(Some(Inbound::Failed("test", _))) => {}
            other => panic!("expected a write failure event, got {:?}", other.map(|o| o.is_some())),
        }
        lane.task.await.unwrap();
    }

    #[tokio::test]
    async fn test_lane_writer_flushes_pending_control_messages_before_closing() {
        let mut lane = spawn_lane(64 * 1024);
        let worker_id = Uuid::new_v4();
        lane.control_tx
            .send(WorkerMessage::Disconnecting {
                worker_id,
                reason: "farewell".into(),
            })
            .await
            .unwrap();
        lane.closing_tx.send(true).unwrap();
        // Data queued after the close request is not started.
        let _ = lane.data_tx.try_send(big_result(worker_id, 10));

        let msg = lane.peer.recv_msg::<WorkerMessage>().await.unwrap().unwrap();
        assert!(matches!(msg, WorkerMessage::Disconnecting { .. }));
        tokio::time::timeout(Duration::from_secs(2), lane.task)
            .await
            .expect("the writer exits once the control queue is flushed")
            .unwrap();
    }

    // ------------------------------------------------------------------------------------------
    // Registration, identity, timeouts
    // ------------------------------------------------------------------------------------------

    #[tokio::test]
    async fn test_register_carries_the_live_mobile_state_not_the_startup_snapshot() {
        let (client_io, master_io) = tokio::io::duplex(4096);
        let client = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999"));
        let worker_id = client.worker_id();
        // The phone has drained since the worker started.
        client
            .mobile_telemetry()
            .set(Some(rusty_grid_core::capabilities::MobileCapabilities {
                os_version: "Android 14".into(),
                soc_model: "Tensor".into(),
                battery_pct: Some(6),
                is_charging: Some(false),
                thermal_throttled: true,
            }));

        let master = tokio::spawn(async move {
            let mut master_transport = MessageTransport::new(master_io);
            let msg = master_transport.recv_msg::<WorkerMessage>().await.unwrap().unwrap();
            master_transport
                .send_msg(&MasterMessage::RegisterAck {
                    accepted: true,
                    worker_id,
                    heartbeat_interval_secs: 3,
                    message: None,
                })
                .await
                .unwrap();
            msg
        });

        let mut transport = MessageTransport::new(client_io);
        client.perform_handshake(&mut transport).await.expect("handshake");
        match master.await.unwrap() {
            WorkerMessage::Register { capabilities, .. } => {
                let mobile = capabilities.mobile.expect("mobile state in Register");
                assert_eq!(mobile.battery_pct, Some(6));
                assert_eq!(mobile.is_charging, Some(false));
                assert!(mobile.thermal_throttled);
            }
            other => panic!("expected Register, got {other:?}"),
        }
    }

    /// Runs the worker against a protocol-level Master that accepts it with `ack_message`, changes
    /// the device's battery reading, and returns the names of the non-heartbeat messages the
    /// Master receives while the worker has time to report it.
    async fn messages_after_a_battery_change(ack_message: Option<String>) -> Vec<&'static str> {
        let dir = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = MobileTelemetryHandle::default();
        let mut client = WorkerClient::new(
            WorkerConfig::new(addr.to_string())
                .with_cores(1)
                .with_mobile_telemetry(handle.clone())
                .with_sandbox_base_dir(dir.path())
                .with_redirection_portal(false),
        );
        let (stop_tx, stop_rx) = watch::channel(false);
        let worker = tokio::spawn(async move { client.run(stop_rx).await });

        let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("the worker connects")
            .unwrap();
        let mut master = MessageTransport::new(stream);
        let Some(WorkerMessage::Register { worker_id, .. }) =
            master.recv_msg::<WorkerMessage>().await.unwrap()
        else {
            panic!("expected Register");
        };
        master
            .send_msg(&MasterMessage::RegisterAck {
                accepted: true,
                worker_id,
                heartbeat_interval_secs: 1,
                message: ack_message,
            })
            .await
            .unwrap();
        // The first heartbeat proves the session is running (and has read the battery already, so
        // what follows is a change).
        loop {
            match tokio::time::timeout(Duration::from_secs(5), master.recv_msg::<WorkerMessage>())
                .await
                .expect("a heartbeat")
            {
                Ok(Some(WorkerMessage::Heartbeat { .. })) => break,
                Ok(Some(_)) => {}
                other => panic!("worker went away before its first heartbeat: {other:?}"),
            }
        }
        handle.update(MobileLiveStatus {
            battery_pct: Some(42),
            is_charging: Some(false),
            thermal_throttled: false,
        });

        let mut seen = Vec::new();
        let deadline = tokio::time::Instant::now() + MOBILE_TELEMETRY_CHECK_INTERVAL * 2;
        while let Ok(Ok(Some(msg))) =
            tokio::time::timeout_at(deadline, master.recv_msg::<WorkerMessage>()).await
        {
            if !matches!(msg, WorkerMessage::Heartbeat { .. }) {
                seen.push(msg.variant_name());
            }
        }
        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), worker).await;
        seen
    }

    #[tokio::test]
    async fn test_battery_changes_are_reported_to_a_master_that_announced_it_reads_them() {
        let announcement = master_features::announcement(master_features::SUPPORTED);
        let seen = messages_after_a_battery_change(Some(announcement)).await;
        assert!(seen.contains(&"MobileTelemetry"), "{seen:?}");
    }

    #[tokio::test]
    async fn test_battery_changes_are_not_sent_to_a_master_that_did_not_announce_it_reads_them() {
        // A Master built before WorkerMessage::MobileTelemetry cannot decode it and would drop
        // the worker (and fail its tasks over) on every battery tick; it sends no announcement.
        let seen = messages_after_a_battery_change(None).await;
        assert!(!seen.contains(&"MobileTelemetry"), "{seen:?}");
    }

    #[test]
    fn test_external_telemetry_handle_is_shared_with_the_client() {
        let handle = MobileTelemetryHandle::default();
        let client = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9999").with_mobile_telemetry(handle.clone()),
        );
        handle.update(MobileLiveStatus {
            battery_pct: Some(33),
            is_charging: Some(true),
            thermal_throttled: false,
        });
        assert_eq!(client.mobile_telemetry().live_status().unwrap().battery_pct, Some(33));
        assert!(client.mobile_fed_externally);
    }

    #[test]
    fn test_worker_id_is_persisted_only_when_asked_to() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("worker_id");

        // Default: a fresh id every time, and nothing is written.
        let a = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999")).worker_id();
        let b = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999")).worker_id();
        assert_ne!(a, b);

        // With a file: the same id across "restarts".
        let first = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_worker_id_file(&path));
        let second = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_worker_id_file(&path));
        assert_eq!(first.worker_id(), second.worker_id());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            first.worker_id().to_string()
        );

        // An explicit id still wins over the file, and leaves it alone.
        let explicit = Uuid::new_v4();
        let third = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9999")
                .with_worker_id_file(&path)
                .with_worker_id(explicit),
        );
        assert_eq!(third.worker_id(), explicit);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            first.worker_id().to_string()
        );
    }

    #[test]
    fn test_unusable_worker_id_file_falls_back_to_a_fresh_id() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, "a file, not a directory").unwrap();
        let path = blocker.join("worker_id");
        let a = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_worker_id_file(&path));
        let b = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_worker_id_file(&path));
        assert_ne!(a.worker_id(), b.worker_id(), "no persistence, but the worker still starts");
    }

    #[test]
    fn test_dead_peer_timeout_scales_with_the_heartbeat_interval() {
        let client = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999"));
        assert_eq!(client.dead_peer_timeout(Duration::from_secs(3)), Duration::from_secs(12));
        assert_eq!(
            client.dead_peer_timeout(Duration::from_millis(100)),
            MIN_DEAD_PEER_TIMEOUT,
            "never twitchier than the floor"
        );
        let tuned = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9999").with_dead_peer_timeout(Duration::from_millis(700)),
        );
        assert_eq!(tuned.dead_peer_timeout(Duration::from_secs(3)), Duration::from_millis(700));
    }

    #[cfg(feature = "p2p")]
    #[tokio::test]
    async fn test_run_refuses_an_unparsable_p2p_ticket_instead_of_retrying_forever() {
        let mut client = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9")
                .with_p2p_ticket("this is not a ticket")
                .with_redirection_portal(false),
        );
        let (_tx, rx) = watch::channel(false);
        let result = tokio::time::timeout(Duration::from_secs(5), client.run(rx))
            .await
            .expect("a configuration error must end run() at once, not loop");
        assert!(matches!(result, Err(GridError::Config(_))), "{result:?}");
    }

    #[tokio::test]
    async fn test_run_refuses_to_start_when_task_sandboxes_cannot_be_created() {
        let dir = tempfile::tempdir().unwrap();
        // A regular file where the base directory should be: not creatable, whoever runs the test.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let mut client = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9")
                .with_sandbox_base_dir(blocker.join("sandboxes"))
                .with_redirection_portal(false),
        );
        let (_tx, rx) = watch::channel(false);
        let result = tokio::time::timeout(Duration::from_secs(5), client.run(rx))
            .await
            .expect("an unusable sandbox directory must end run() at once, not loop");
        match result {
            Err(GridError::Config(msg)) => {
                assert!(msg.contains("not writable") && msg.contains("blocker"), "{msg}");
                assert!(msg.contains("--sandbox-base-dir"), "the fix must be in the message: {msg}");
            }
            other => panic!("expected a Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_default_max_concurrency_leaves_a_core_free() {
        assert_eq!(default_max_concurrency(0), 1);
        assert_eq!(default_max_concurrency(1), 1);
        assert_eq!(default_max_concurrency(2), 1);
        assert_eq!(default_max_concurrency(8), 7);
        assert_eq!(default_max_concurrency(16), 15);
    }

    #[test]
    fn test_concurrency_permits_default_to_cores_minus_one_unless_configured() {
        // Detected cores: headroom.
        let detected = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999"));
        assert_eq!(
            detected.concurrency_semaphore().available_permits(),
            default_max_concurrency(detected.capabilities().cpu_cores)
        );
        // Cores the owner named are offered in full; an explicit limit always wins.
        let named = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_cores(6));
        assert_eq!(named.concurrency_semaphore().available_permits(), 6);
        let limited = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9999").with_cores(6).with_max_concurrency(2),
        );
        assert_eq!(limited.concurrency_semaphore().available_permits(), 2);
        let all = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_max_concurrency(64));
        assert_eq!(all.concurrency_semaphore().available_permits(), 64);
    }

    #[test]
    fn test_process_priority_reaches_the_runner() {
        let client = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9999").with_process_priority(ProcessPriority::Idle),
        );
        assert_eq!(client.runner().config().process_priority, ProcessPriority::Idle);
    }

    // ------------------------------------------------------------------------------------------
    // Standby portal: validated URLs only, no hard-coded hosts, bounded requests
    // ------------------------------------------------------------------------------------------

    fn beacon(cluster: &str, ui: &str) -> rusty_grid_core::discovery::MasterBeacon {
        rusty_grid_core::discovery::MasterBeacon::new(cluster, ui, "h", 0)
    }

    fn remote_visitor() -> IpAddr {
        "192.168.50.7".parse().unwrap()
    }

    #[test]
    fn test_portal_resolution_never_invents_a_host() {
        // Master on this very machine (loopback) and a visitor from the LAN: nowhere useful to
        // send them. (This used to fall back to two hard-coded 192.168.1.x addresses.)
        let b = beacon("127.0.0.1:8088", "http://127.0.0.1:8080");
        assert_eq!(portal_master_from_beacon(&b, remote_visitor()), None);
        // A loopback visitor may be sent to loopback.
        assert_eq!(
            portal_master_from_beacon(&b, "127.0.0.1".parse().unwrap()),
            Some(("http://127.0.0.1:8080".into(), "127.0.0.1:8088".into()))
        );
        // Dashboard on loopback but the cluster address on the LAN: use the cluster host.
        let b = beacon("10.1.2.3:8088", "http://127.0.0.1:8080");
        assert_eq!(
            portal_master_from_beacon(&b, remote_visitor()),
            Some(("http://10.1.2.3:8080".into(), "10.1.2.3:8088".into()))
        );
        // A proper LAN dashboard URL is kept as advertised.
        let b = beacon("10.1.2.3:8088", "http://10.1.2.3:9090");
        assert_eq!(
            portal_master_from_beacon(&b, remote_visitor()),
            Some(("http://10.1.2.3:9090".into(), "10.1.2.3:8088".into()))
        );
        // IPv6 hosts are bracketed.
        let b = beacon("[fd00::5]:8088", "http://127.0.0.1:8080");
        assert_eq!(
            portal_master_from_beacon(&b, remote_visitor()),
            Some(("http://[fd00::5]:8080".into(), "[fd00::5]:8088".into()))
        );
    }

    #[test]
    fn test_portal_rejects_hostile_beacon_fields() {
        let visitor = remote_visitor();
        // An invalid dashboard URL is replaced by one derived from a valid cluster address...
        for ui in [
            "javascript:alert(1)",
            "http://10.1.2.3:8080\r\nSet-Cookie: pwn=1",
            "http://10.1.2.3\"><script>alert(1)</script>",
            "//evil.example",
            "http://user@10.1.2.3",
        ] {
            let b = beacon("10.1.2.3:8088", ui);
            assert_eq!(
                portal_master_from_beacon(&b, visitor),
                Some(("http://10.1.2.3:8080".into(), "10.1.2.3:8088".into())),
                "ui {ui:?}"
            );
        }
        // An *empty* dashboard URL is not a malformed one: the Master has no dashboard to offer,
        // so the visitor stays on the standby page instead of being sent to a guessed address.
        // (This used to be derived from the cluster host like the cases above.)
        let b = beacon("10.1.2.3:8088", "");
        assert_eq!(
            portal_master_from_beacon(&b, visitor),
            Some((String::new(), "10.1.2.3:8088".into()))
        );
        // (and a master on loopback tells a visitor from another machine nothing useful)
        let b = beacon("127.0.0.1:8088", "");
        assert_eq!(portal_master_from_beacon(&b, visitor), None);
        // ...and a hostile cluster address drops the beacon altogether.
        for cluster in [
            "10.1.2.3:8088\r\nX-Evil: 1",
            "evil\"host:8088",
            "<script>:8088",
            "10.1.2.3",
            "auto",
        ] {
            let b = beacon(cluster, "http://10.1.2.3:8080");
            assert_eq!(portal_master_from_beacon(&b, visitor), None, "cluster {cluster:?}");
        }
    }

    #[test]
    fn test_portal_responses_never_reflect_unvalidated_text() {
        for hostile in [
            "http://10.0.0.5:8080\r\nSet-Cookie: pwn=1",
            "javascript:alert(document.cookie)",
            "http://10.0.0.5\"><script>alert(1)</script>",
            "http://10.0.0.5';alert(1);//",
        ] {
            let response = portal_redirect_response(hostile, "10.0.0.5:8088\r\nX-Evil: 1");
            assert!(response.starts_with("HTTP/1.1 200 OK"), "falls back to the standby page: {hostile:?}");
            assert!(!response.contains("Set-Cookie"));
            assert!(!response.contains("X-Evil"));
            assert!(!response.contains("alert(document.cookie)"));
            assert!(!response.contains("Location:"));
        }
        // A valid origin with a hostile cluster address: still redirected, header omitted.
        let response = portal_redirect_response("http://10.0.0.5:8080", "10.0.0.5:8088\r\nX-Evil: 1");
        assert!(response.starts_with("HTTP/1.1 307 Temporary Redirect"));
        assert!(response.contains("Location: http://10.0.0.5:8080\r\n"));
        assert!(!response.contains("X-Evil"));
        assert!(!response.contains("X-OxideSwarm-Master"));
        // Only the response head can carry headers: exactly one blank line ends it.
        let head = response.split("\r\n\r\n").next().unwrap();
        assert!(!head.contains("<script>"));
    }

    #[test]
    fn test_portal_has_no_hard_coded_hosts_and_no_cors_wildcard() {
        let standby = portal_standby_response();
        assert!(!standby.contains("192.168."), "the standby page must not probe fixed hosts");
        assert!(standby.contains("fetch('/api/status'"), "it asks its own origin instead");
        assert!(standby.contains("probeCluster()"));
        assert!(!standby.to_ascii_lowercase().contains("access-control-allow-origin"));

        let status = portal_status_response(None);
        assert!(!status.contains("192.168."));
        assert!(!status.contains("candidate_nodes"));
        assert!(status.contains("\"status\":\"searching_master\""));
        let status = portal_status_response(Some(&("http://10.1.2.3:8080".into(), "10.1.2.3:8088".into())));
        assert!(status.contains("\"redirect_url\":\"http://10.1.2.3:8080\""));

        // The source of the portal itself carries no site-specific address either.
        let source = include_str!("client.rs");
        let code = source.split("#[cfg(test)]").next().unwrap();
        assert!(
            !code.lines().filter(|l| !l.trim_start().starts_with("//")).any(|l| l.contains("192.168.1.")),
            "client.rs must not hard-code LAN hosts"
        );
    }

    #[test]
    fn test_status_request_detection_is_exact() {
        assert!(is_status_request("GET /api/status HTTP/1.1"));
        assert!(is_status_request("GET /api/status?x=1 HTTP/1.1"));
        assert!(!is_status_request("GET /api/statusfoo HTTP/1.1"));
        assert!(!is_status_request("GET / HTTP/1.1"));
        assert!(!is_status_request("GET"));
        assert!(!is_status_request(""));
    }

    async fn portal_fixture(
        target: PortalTarget,
        max_connections: usize,
    ) -> (std::net::SocketAddr, watch::Sender<bool>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(run_portal(listener, target, None, shutdown_rx, max_connections));
        (addr, shutdown_tx)
    }

    async fn portal_get(addr: std::net::SocketAddr, request: &[u8]) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("portal answered in time")
            .expect("portal closed the connection cleanly (no reset)");
        String::from_utf8_lossy(&response).to_string()
    }

    #[tokio::test]
    async fn test_portal_serves_the_validated_redirect_end_to_end() {
        let state = Arc::new(RwLock::new(Some(beacon(
            "10.1.2.3:8088\r\nX-Evil: 1",
            "http://10.1.2.3:8080",
        ))));
        let (addr, shutdown) = portal_fixture(PortalTarget::Dynamic(Arc::clone(&state)), 8).await;

        // Hostile cluster address: no redirect, a standby page, and nothing injected.
        let response = portal_get(addr, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(!response.contains("X-Evil"));

        *state.write().await = Some(beacon("10.1.2.3:8088", "http://10.1.2.3:8080"));
        let response = portal_get(addr, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 307 Temporary Redirect"));
        assert!(response.contains("Location: http://10.1.2.3:8080\r\n"));
        assert!(response.contains("X-OxideSwarm-Master: 10.1.2.3:8088\r\n"));

        let _ = shutdown.send(true);
    }

    #[tokio::test]
    async fn test_portal_shows_the_standby_page_for_a_master_that_has_no_reachable_dashboard() {
        // A master whose dashboard listens on loopback advertises no dashboard URL.
        let state = Arc::new(RwLock::new(Some(beacon("10.1.2.3:8088", ""))));
        let (addr, shutdown) = portal_fixture(PortalTarget::Dynamic(Arc::clone(&state)), 8).await;
        let response = portal_get(addr, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{}", &response[..response.len().min(60)]);
        assert!(!response.contains("Location:"), "no redirect to a guessed address");
        // The status API still names the master that is being followed.
        let status = portal_get(addr, b"GET /api/status HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(status.contains("\"status\":\"connected_to_master\""), "{status}");
        assert!(status.contains("\"master_cluster_addr\":\"10.1.2.3:8088\""), "{status}");
        assert!(status.contains("\"redirect_url\":null"), "{status}");
        let _ = shutdown.send(true);
    }

    #[tokio::test]
    async fn test_portal_answers_requests_with_large_headers_without_a_reset() {
        let (addr, shutdown) = portal_fixture(PortalTarget::from("10.1.2.3:8088"), 8).await;
        // A browser with a big cookie jar sends far more than the first 1 KiB read of the old code.
        let mut request = b"GET / HTTP/1.1\r\nHost: x\r\nCookie: ".to_vec();
        request.extend_from_slice(&[b'a'; 6000]);
        request.extend_from_slice(b"\r\n\r\n");
        let response = portal_get(addr, &request).await;
        assert!(response.starts_with("HTTP/1.1 307 Temporary Redirect"), "{}", &response[..response.len().min(80)]);
        assert!(response.contains("Location: http://10.1.2.3:8080"));
        let _ = shutdown.send(true);
    }

    #[tokio::test]
    async fn test_portal_request_reader_gives_up_on_a_silent_client() {
        let (mut client, mut server) = tokio::io::duplex(64);
        // A client that connects and sends nothing must not hold a task and a socket forever.
        let started = std::time::Instant::now();
        let line = read_portal_request_line_with(&mut server, Duration::from_millis(150)).await;
        assert!(line.is_none());
        assert!(started.elapsed() < Duration::from_secs(2));
        // Headers that arrive in pieces are still assembled.
        use tokio::io::AsyncWriteExt;
        let reader = tokio::spawn(async move {
            read_portal_request_line_with(&mut server, Duration::from_secs(2)).await
        });
        client.write_all(b"GET /api/sta").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        client.write_all(b"tus HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
        assert_eq!(reader.await.unwrap().as_deref(), Some("GET /api/status HTTP/1.1"));
    }

    #[tokio::test]
    async fn test_portal_caps_concurrent_connections() {
        use tokio::io::AsyncReadExt;
        let (addr, shutdown) = portal_fixture(PortalTarget::from("10.1.2.3:8088"), 2).await;
        // Two clients that connect and say nothing occupy both slots...
        let _hold1 = TcpStream::connect(addr).await.unwrap();
        let _hold2 = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        // ...so a third is dropped at once instead of becoming another idle task.
        let mut third = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(Duration::from_secs(1), third.read(&mut buf))
            .await
            .expect("the surplus connection is closed promptly");
        assert!(matches!(n, Ok(0) | Err(_)));
        let _ = shutdown.send(true);
    }
}
