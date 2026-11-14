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

use rusty_grid_core::capabilities::{
    detect_live_mobile_status, suppress_windows_error_dialogs, MobileLiveStatus, WorkerCapabilities,
};
use rusty_grid_core::cluster_auth::{client_handshake, ClusterSecret, WIRE_FORMAT_AUTH};
use rusty_grid_core::discovery::{is_valid_host, sanitize_host_port, sanitize_http_origin};
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::noise_auth::{v3_client_handshake, HandshakePurpose, NodeKeypair, PublicKey};
use rusty_grid_core::protocol::{
    format_incompatible_version_error, is_protocol_version_compatible, master_features,
    EndpointRole, MasterMessage, MessageReader, MessageTransport, MessageWriter,
    ProtocolCapabilities, ProtocolError, WireCodec, WorkerMessage, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskResult, TaskStatus};

use crate::backoff::{BackoffConfig, ExponentialBackoff};
use crate::blob_cache::DEFAULT_BLOB_CACHE_MAX_BYTES;
use crate::blob_client::{fetch_task_inputs, upload_task_outputs, BlobClient};
use crate::gpu::{apply_gpu_policy, probe_real_gpu, GpuNotice, GpuRequest, NoticeLevel};
use crate::heartbeat::{
    current_epoch_secs, spawn_adaptive_heartbeat_task, spawn_mobile_poller, HeartbeatTracker,
    IoClock, MobileTelemetryHandle, StampedStream,
};
use crate::identity::{default_worker_id_path, load_or_create_worker_id};
use crate::isolation::IsolationLevel;
use crate::process_tree::ProcessPriority;
use crate::runner::{
    cancellation, RunnerConfig, TaskRunner, DEFAULT_MAX_OUTPUT_BYTES, EXIT_CODE_CANCELLED,
    EXIT_CODE_GENERAL_ERROR,
};
use crate::sandbox::SandboxConfig;
use crate::staging::{OutputLimits, DEFAULT_OUTPUT_LIMIT_BYTES};
use crate::workspace::{WorkspaceLimits, DEFAULT_MAX_WORKSPACES, DEFAULT_WORKSPACE_LIMIT_MB};

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

/// Protocol capabilities advertised by this worker build.
///
/// [`ProtocolCapabilities::THREE_D`] is advertised only when `rusty_grid_worker` is compiled with
/// the `3d` Cargo feature.
pub fn advertised_protocol_capabilities() -> ProtocolCapabilities {
    #[allow(unused_mut)]
    let mut caps = ProtocolCapabilities::all_supported();
    #[cfg(not(feature = "3d"))]
    caps.remove(ProtocolCapabilities::THREE_D);
    caps
}

/// Default upper bound for establishing the transport to the Master (TCP connect or P2P dial).
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Minimum pause before reconnecting after the Master refused the cluster secret (or demanded one
/// this worker does not have). Retrying faster cannot succeed with the same secret and would only
/// hammer the Master (which itself delays every failed authentication).
pub const AUTH_FAILURE_MIN_BACKOFF: Duration = Duration::from_secs(30);

type EvacuationHook = Box<dyn Fn() -> Option<String> + Send + Sync>;

static EVACUATION_REASON_HOOK: std::sync::RwLock<Option<EvacuationHook>> =
    std::sync::RwLock::new(None);

/// Registers an external hook for querying an active evacuation reason (e.g. from Android bridge).
pub fn set_evacuation_reason_hook<F: Fn() -> Option<String> + Send + Sync + 'static>(f: F) {
    if let Ok(mut guard) = EVACUATION_REASON_HOOK.write() {
        *guard = Some(Box::new(f));
    }
}

/// Retrieves the active evacuation reason if set by an evacuation hook.
pub fn current_evacuation_reason() -> Option<String> {
    if let Ok(guard) = EVACUATION_REASON_HOOK.read() {
        if let Some(hook) = guard.as_ref() {
            return hook();
        }
    }
    None
}

/// A Master that sends nothing for this many heartbeat intervals is considered gone.
pub const DEAD_PEER_HEARTBEAT_FACTOR: u32 = 4;

/// Floor of the dead-peer timeout, so a 1 s heartbeat interval cannot make the worker twitchy.
pub const MIN_DEAD_PEER_TIMEOUT: Duration = Duration::from_secs(2);

/// How long cancelled tasks get to kill their process tree and clean their sandbox when a
/// session ends, before they are aborted outright.
pub const TASK_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Safety margin subtracted from the Master's advertised `session-resume=<secs>` hold so the
/// worker stops or reconnects before the Master's hold timer expires (design 0046 D2.4).
pub const RESUME_SAFETY_MARGIN: Duration = Duration::from_secs(3);

/// Computes the worker's effective session-resume grace window (`design 0046 D4.3`):
/// `min(max(0, hold - dead_peer_timeout - 3s), grace_cap)`, or `Duration::ZERO` when `hold` is
/// absent, `<= dead_peer_timeout + 3s`, or `grace_cap == Some(Duration::ZERO)`.
pub fn compute_effective_resume_grace(
    hold: Option<Duration>,
    dead_peer_timeout: Duration,
    grace_cap: Option<Duration>,
) -> Duration {
    let Some(hold) = hold else {
        return Duration::ZERO;
    };
    let max_safe_grace = hold
        .saturating_sub(dead_peer_timeout)
        .saturating_sub(RESUME_SAFETY_MARGIN);
    let effective = match grace_cap {
        Some(cap) => cap.min(max_safe_grace),
        None => max_safe_grace,
    };
    if effective < Duration::from_secs(1) {
        Duration::ZERO
    } else {
        effective
    }
}

/// Maximum total bytes of task results buffered in the session-resume outbox (design 0046 D4).
pub const MAX_RESUME_OUTBOX_BYTES: usize = 32 * 1024 * 1024;

fn buffered_result_bytes(result: &TaskResult) -> usize {
    256usize
        .saturating_add(result.stdout.0.len())
        .saturating_add(result.stderr.0.len())
        .saturating_add(result.error.as_ref().map_or(0, String::len))
        .saturating_add(result.device_name.as_ref().map_or(0, String::len))
        .saturating_add(result.asset_bytes.as_ref().map_or(0, |b| b.0.len()))
        .saturating_add(
            result
                .artifacts
                .iter()
                .map(|a| a.path.len().saturating_add(a.data.len()))
                .sum::<usize>(),
        )
        .saturating_add(
            result
                .outputs
                .iter()
                .map(|o| o.path.len().saturating_add(64))
                .sum::<usize>(),
        )
}

/// Completed task result buffered in the worker's session-resume outbox until acknowledged by the
/// Master (`MasterMessage::CancelTask { reason: Some("oxs:result-ack:<token>") }`) or until the
/// resume window expires. Holds `output_pin` (`Arc<CachePin>`) so output blobs stay pinned in
/// `BlobCache` across transient session drops.
#[derive(Clone)]
struct BufferedTaskResult {
    task: Task,
    result: TaskResult,
    ack_token: u64,
    byte_size: usize,
    _output_pin: Option<Arc<crate::blob_cache::CachePin>>,
    used_blobs: bool,
}

impl BufferedTaskResult {
    fn new(
        task: Task,
        result: TaskResult,
        output_pin: Option<Arc<crate::blob_cache::CachePin>>,
        used_blobs: bool,
    ) -> Self {
        let ack_token = master_features::result_ack_token(&result);
        let byte_size = buffered_result_bytes(&result);
        Self {
            task,
            result,
            ack_token,
            byte_size,
            _output_pin: output_pin,
            used_blobs,
        }
    }
}

/// Bounded per-worker outbox for checkpoints and task results retained across transient session
/// drops (design 0046 D2.5 / D4).
struct PendingResumeOutbox {
    checkpoints: HashMap<TaskId, WorkerMessage>,
    results: Vec<BufferedTaskResult>,
    cancelled: std::collections::HashSet<TaskId>,
    max_results: usize,
    total_bytes: usize,
    max_bytes: usize,
}

impl Default for PendingResumeOutbox {
    fn default() -> Self {
        Self::with_capacity(8)
    }
}

impl PendingResumeOutbox {
    fn with_capacity(max_results: usize) -> Self {
        Self {
            checkpoints: HashMap::new(),
            results: Vec::new(),
            cancelled: std::collections::HashSet::new(),
            max_results: max_results.max(2),
            total_bytes: 0,
            max_bytes: MAX_RESUME_OUTBOX_BYTES,
        }
    }

    fn clear(&mut self) {
        self.checkpoints.clear();
        self.results.clear();
        self.cancelled.clear();
        self.total_bytes = 0;
    }

    fn push_result(&mut self, item: BufferedTaskResult) -> bool {
        let task_id = item.result.task_id;
        if self.cancelled.contains(&task_id) {
            return false;
        }
        let mut evicted_used_blobs = false;
        let mut removed_bytes = 0usize;
        self.results.retain(|r| {
            if r.result.task_id == task_id {
                evicted_used_blobs |= r.used_blobs;
                removed_bytes = removed_bytes.saturating_add(r.byte_size);
                false
            } else {
                true
            }
        });
        self.total_bytes = self.total_bytes.saturating_sub(removed_bytes);
        while self.results.len() >= self.max_results
            || (!self.results.is_empty()
                && self.total_bytes.saturating_add(item.byte_size) > self.max_bytes)
        {
            let evicted = self.results.remove(0);
            self.total_bytes = self.total_bytes.saturating_sub(evicted.byte_size);
            evicted_used_blobs |= evicted.used_blobs;
        }
        self.total_bytes = self.total_bytes.saturating_add(item.byte_size);
        self.results.push(item);
        evicted_used_blobs
    }

    fn update_result(&mut self, task_id: TaskId, prev_token: u64, result: TaskResult) -> u64 {
        let new_token = master_features::result_ack_token(&result);
        let new_size = buffered_result_bytes(&result);
        for item in &mut self.results {
            if item.result.task_id == task_id && item.ack_token == prev_token {
                self.total_bytes = self
                    .total_bytes
                    .saturating_sub(item.byte_size)
                    .saturating_add(new_size);
                item.result = result;
                item.ack_token = new_token;
                item.byte_size = new_size;
                break;
            }
        }
        new_token
    }

    fn ack_result(&mut self, task_id: TaskId, ack_token: Option<u64>) -> bool {
        let mut used_blobs = false;
        let mut removed_any = false;
        let mut removed_bytes = 0usize;
        self.results.retain(|item| {
            if item.result.task_id == task_id && ack_token.is_none_or(|t| item.ack_token == t) {
                used_blobs |= item.used_blobs;
                removed_any = true;
                removed_bytes = removed_bytes.saturating_add(item.byte_size);
                false
            } else {
                true
            }
        });
        self.total_bytes = self.total_bytes.saturating_sub(removed_bytes);
        if ack_token.is_none() || removed_any {
            self.checkpoints.remove(&task_id);
        }
        used_blobs
    }

    fn cancel_task(&mut self, task_id: TaskId) -> bool {
        self.checkpoints.remove(&task_id);
        let mut used_blobs = false;
        let mut removed_bytes = 0usize;
        self.results.retain(|item| {
            if item.result.task_id == task_id {
                used_blobs |= item.used_blobs;
                removed_bytes = removed_bytes.saturating_add(item.byte_size);
                false
            } else {
                true
            }
        });
        self.total_bytes = self.total_bytes.saturating_sub(removed_bytes);
        self.cancelled.insert(task_id);
        used_blobs
    }
}

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

/// RAII guard ensuring active task counter is decremented, output pins are released, and table entry is pruned on completion or abort.
struct ActiveTaskGuard {
    task_id: TaskId,
    attempt: u64,
    heartbeat_tracker: Arc<HeartbeatTracker>,
    active_task_table: ActiveTaskTable,
    runner: Option<Arc<TaskRunner>>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.heartbeat_tracker.decrement_active_tasks();
        if let Some(ref runner) = self.runner {
            runner.release_task_outputs(self.task_id);
        }
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
    /// When `true`, builds the worker's iroh P2P endpoint with
    /// `iroh::endpoint::presets::Minimal` (no n0 DERP relay or pkarr/DNS address lookup) so tests
    /// stay off the network. Production defaults to `false` (`presets::N0`); the CLI resolves
    /// `RUSTY_GRID_P2P_MINIMAL`.
    pub p2p_minimal_preset: bool,
    /// Optional custom iroh relay URL (`https://...`). When set, builds the worker's P2P endpoint
    /// with `iroh::RelayMode::Custom(iroh::RelayMap::from(url))` instead of `presets::N0`.
    pub p2p_relay_url: Option<String>,
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
    /// How strongly task processes are confined (CLI `--isolation`, default `off`; design 0033).
    /// A level this host cannot enforce makes [`WorkerClient::run`] fail at start-up.
    pub isolation: IsolationLevel,
    /// Files a confined task must not read even though they are outside the denied home: the
    /// cluster secret file and the config file in use (CLI, with `--isolation`).
    pub protected_files: Vec<std::path::PathBuf>,
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
    /// Shared cluster secret. When set (and `master_public_key` is not set), the worker proves
    /// knowledge of it to the Master (`rusty_grid_core::cluster_auth`) on every connect and
    /// reconnect, TCP and P2P alike, before it sends `Register`, and refuses a Master that cannot
    /// prove it holds the same secret. A worker without a secret or pinned master key only joins
    /// masters that run without authentication.
    pub cluster_secret: Option<ClusterSecret>,
    /// Static per-machine X25519 key pair (`~/.oxideswarm/node.key`) used for the OXS-AUTH v3
    /// (Noise `IK`) handshake when `master_public_key` is configured.
    pub node_key: Option<NodeKeypair>,
    /// Pinned static X25519 public key of the Master (`--master-key`, `RUSTY_GRID_MASTER_KEY`, or
    /// `master_key` in config). When set, the worker authenticates with OXS-AUTH v3 instead of v2.
    pub master_public_key: Option<PublicKey>,
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
    /// Bound of the blob cache of task input and output files in
    /// `<sandbox base>/cache/blobs` (default: [`DEFAULT_BLOB_CACHE_MAX_BYTES`]).
    pub blob_cache_bytes: u64,
    /// Most bytes of output files (`Task::output_globs`) one task may bring back (default:
    /// [`DEFAULT_OUTPUT_LIMIT_BYTES`]).
    pub output_limit_bytes: u64,
    /// Whether to enable standby HTTP redirection portal on worker node (default: true).
    pub enable_redirection_portal: bool,
    /// Port for standby HTTP redirection portal (default: 8080).
    pub redirection_portal_port: u16,
    /// Wire serialization codec (Bincode or Json, default: Bincode).
    pub wire_codec: WireCodec,
    /// Explicit Blender availability override (CLI `--blender`).
    pub blender: Option<bool>,
    /// Explicitly disable Blender capability (CLI `--no-blender`).
    pub no_blender: bool,
    /// Optional explicit path to Blender executable (CLI `--blender-path`).
    pub blender_path: Option<String>,
    /// Optional explicit Blender version string override (CLI `--blender-version`).
    pub blender_version: Option<String>,
    /// Optional explicit GPU VRAM in MB override (CLI `--vram-mb`).
    pub vram_mb: Option<u64>,
    /// Explicit CUDA availability override (CLI `--cuda`).
    pub cuda: Option<bool>,
    /// Explicitly disable CUDA capability (CLI `--no-cuda`).
    pub no_cuda: bool,
    /// Explicitly disable host battery and mobile capability probing (`RUSTY_GRID_NO_BATTERY`).
    pub no_battery: bool,
    /// Optional operator cap on the worker's session-resume grace window
    /// (`--session-resume-grace-secs` / `RUSTY_GRID_SESSION_RESUME_GRACE_SECS`). `None` uses the
    /// Master's advertised `session-resume=<secs>` bounded by the worker's dead-peer inequality;
    /// `Some(Duration::ZERO)` disables keeping tasks alive across disconnects.
    pub session_resume_grace_cap: Option<Duration>,
    /// Opt-in flag enabling persistent named workspaces (CLI `--workspaces`, design 0039).
    /// Off by default; only when `true` does the worker open its [`crate::workspace::WorkspaceStore`]
    /// at start-up and advertise [`ProtocolCapabilities::WORKSPACES`] in `Register`.
    pub workspaces: bool,
    /// Root directory of the persistent workspace store (CLI `--workspace-root`). When `None`,
    /// uses [`crate::workspace::default_root`].
    pub workspace_root: Option<PathBuf>,
    /// Maximum disk size per workspace in MiB (CLI `--workspace-limit-mb`, default:
    /// [`DEFAULT_WORKSPACE_LIMIT_MB`]).
    pub workspace_limit_mb: u64,
    /// Maximum number of workspaces kept on disk before LRU eviction (CLI `--max-workspaces`,
    /// default: [`DEFAULT_MAX_WORKSPACES`]).
    pub max_workspaces: usize,
}

impl WorkerConfig {
    pub fn new(master_addr: impl Into<String>) -> Self {
        Self {
            master_addr: master_addr.into(),
            p2p_ticket: None,
            p2p_minimal_preset: false,
            p2p_relay_url: None,
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
            blender: None,
            no_blender: false,
            blender_path: None,
            blender_version: None,
            vram_mb: None,
            cuda: None,
            no_cuda: false,
            no_battery: false,
            session_resume_grace_cap: None,
            workspaces: false,
            workspace_root: None,
            workspace_limit_mb: DEFAULT_WORKSPACE_LIMIT_MB,
            max_workspaces: DEFAULT_MAX_WORKSPACES,
            max_concurrency: None,
            process_priority: None,
            isolation: IsolationLevel::Off,
            protected_files: Vec::new(),
            default_heartbeat_interval: Duration::from_secs(3),
            handshake_timeout: Duration::from_secs(5),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            dead_peer_timeout: None,
            cluster_secret: None,
            node_key: None,
            master_public_key: None,
            mobile_telemetry: None,
            tags: Vec::new(),
            sandbox_base_dir: None,
            keep_sandboxes: false,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            blob_cache_bytes: DEFAULT_BLOB_CACHE_MAX_BYTES,
            output_limit_bytes: DEFAULT_OUTPUT_LIMIT_BYTES,
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

    /// Selects `iroh::endpoint::presets::Minimal` (no relay or external address lookup) for the
    /// worker's P2P endpoint when `minimal` is `true`, keeping tests off the network while
    /// production keeps `presets::N0` (`false`).
    pub fn with_p2p_minimal_preset(mut self, minimal: bool) -> Self {
        self.p2p_minimal_preset = minimal;
        self
    }

    /// Configures a custom iroh relay URL (`https://...`) so the worker's P2P endpoint uses
    /// `iroh::RelayMode::Custom` instead of `presets::N0`.
    pub fn with_p2p_relay_url(mut self, url: impl Into<String>) -> Self {
        self.p2p_relay_url = Some(url.into());
        self
    }

    /// Sets or clears the optional custom iroh relay URL (`https://...`).
    pub fn with_relay_url(mut self, url: Option<String>) -> Self {
        self.p2p_relay_url = url;
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

    pub fn with_blender(mut self, blender: bool) -> Self {
        self.blender = Some(blender);
        self
    }

    pub fn with_no_blender(mut self, no_blender: bool) -> Self {
        self.no_blender = no_blender;
        self
    }

    pub fn with_blender_path(mut self, path: impl Into<String>) -> Self {
        self.blender_path = Some(path.into());
        self
    }

    pub fn with_blender_version(mut self, version: impl Into<String>) -> Self {
        self.blender_version = Some(version.into());
        self
    }

    pub fn with_vram_mb(mut self, vram_mb: u64) -> Self {
        self.vram_mb = Some(vram_mb);
        self
    }

    pub fn with_cuda(mut self, cuda: bool) -> Self {
        self.cuda = Some(cuda);
        self
    }

    pub fn with_no_cuda(mut self, no_cuda: bool) -> Self {
        self.no_cuda = no_cuda;
        self
    }

    pub fn with_no_battery(mut self, no_battery: bool) -> Self {
        self.no_battery = no_battery;
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

    pub fn with_isolation(mut self, level: IsolationLevel) -> Self {
        self.isolation = level;
        self
    }

    pub fn with_protected_files(mut self, files: Vec<std::path::PathBuf>) -> Self {
        self.protected_files = files;
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

    /// Sets the worker's static X25519 key pair for OXS-AUTH v3 (Noise `IK`).
    pub fn with_node_key(mut self, node_key: NodeKeypair) -> Self {
        self.node_key = Some(node_key);
        self
    }

    /// Sets the Master's pinned static X25519 public key for OXS-AUTH v3 (Noise `IK`).
    pub fn with_master_public_key(mut self, master_public_key: PublicKey) -> Self {
        self.master_public_key = Some(master_public_key);
        self
    }

    /// Configures both the worker's `node.key` and the Master's pinned public key for OXS-AUTH v3.
    pub fn with_v3_auth(mut self, node_key: NodeKeypair, master_public_key: PublicKey) -> Self {
        self.node_key = Some(node_key);
        self.master_public_key = Some(master_public_key);
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

    pub fn with_blob_cache_bytes(mut self, bytes: u64) -> Self {
        self.blob_cache_bytes = bytes;
        self
    }

    pub fn with_output_limit_bytes(mut self, bytes: u64) -> Self {
        self.output_limit_bytes = bytes;
        self
    }

    pub fn with_wire_codec(mut self, codec: WireCodec) -> Self {
        self.wire_codec = codec;
        self
    }

    /// Caps how long the worker keeps running tasks alive across a disconnect
    /// (`Some(Duration::ZERO)` disables session resume on the worker; `None` uses the Master's
    /// advertised window bounded by the worker's dead-peer inequality).
    pub fn with_session_resume_grace_cap(mut self, cap: Option<Duration>) -> Self {
        self.session_resume_grace_cap = cap;
        self
    }

    /// Enables or disables persistent named workspaces on this worker (`--workspaces`).
    pub fn with_workspaces(mut self, enable: bool) -> Self {
        self.workspaces = enable;
        self
    }

    /// Overrides the root directory of the persistent workspace store (`--workspace-root`).
    pub fn with_workspace_root(mut self, root: Option<PathBuf>) -> Self {
        self.workspace_root = root;
        self
    }

    /// Sets the per-workspace disk quota in MiB (`--workspace-limit-mb`).
    pub fn with_workspace_limit_mb(mut self, limit_mb: u64) -> Self {
        self.workspace_limit_mb = limit_mb;
        self
    }

    /// Sets the maximum number of workspaces kept on disk before LRU eviction (`--max-workspaces`).
    pub fn with_max_workspaces(mut self, max: usize) -> Self {
        self.max_workspaces = max;
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
    /// Optional delay before sending the session's initial `MobileTelemetry` after `RegisterAck`
    /// (used by integration tests to simulate slow LTE / relay round trips).
    initial_telemetry_delay: Duration,
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
    /// Whether the Master of the current connection announced a blob store
    /// (`master_features::BLOBS`): only then are task inputs fetched and outputs uploaded.
    master_has_blobs: AtomicBool,
    /// Intersection of `ProtocolCapabilities` advertised by this worker and acknowledged by the
    /// Master in the current connection's `RegisterAck` (`features & all_supported()`).
    negotiated_features: AtomicU64,
    /// The blob requester of the current session, if its Master has a blob store.
    blob_client: Arc<std::sync::RwLock<Option<Arc<BlobClient>>>>,
    /// Current session's outbound data channel (`Some` while `run_session` is active).
    session_data_tx: Arc<std::sync::RwLock<Option<mpsc::Sender<WorkerMessage>>>>,
    /// `session-resume=<secs>` negotiated in the current session's `RegisterAck` (`0` if disabled).
    current_session_hold_secs: Arc<AtomicU64>,
    /// `session-resume=<secs>` negotiated in the most recent connected session.
    last_negotiated_hold_secs: AtomicU64,
    /// Heartbeat interval (in milliseconds) from the most recent `RegisterAck`.
    last_heartbeat_interval_ms: AtomicU64,
    /// True while disconnected within an active session-resume grace window.
    resume_window_active: Arc<AtomicBool>,
    /// Deadline after which kept-alive tasks and buffered outbox entries are cancelled/cleared.
    resume_deadline: Option<tokio::time::Instant>,
    /// Outbox for checkpoints and completed task results produced while disconnected within the
    /// session-resume grace window.
    pending_resume_outbox: Arc<std::sync::Mutex<PendingResumeOutbox>>,
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

/// Truncates `text` to at most `max_bytes` on a UTF-8 character boundary.
fn truncate_utf8_prefix(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Rewrites an oversized `TaskResult` into a failed `WorkerMessage::TaskResult` that fits within
/// `MAX_FRAME_SIZE` by dropping `asset_bytes` first, then `artifacts`, and then `stdout` and
/// `stderr`, stopping as soon as the serialized frame fits.
fn rewrite_oversized_task_result(
    result: TaskResult,
    size: usize,
    max: usize,
    codec: WireCodec,
    encrypted: bool,
) -> WorkerMessage {
    const MAX_PRESERVED_ERROR_BYTES: usize = 4096;

    let exit_code = if result.exit_code != 0 {
        result.exit_code
    } else {
        EXIT_CODE_GENERAL_ERROR
    };
    let overhead = if encrypted {
        rusty_grid_core::protocol::AEAD_RECORD_OVERHEAD
    } else {
        0
    };
    let fits = |msg: &WorkerMessage| {
        rusty_grid_core::protocol::serialize_message(msg, codec)
            .is_ok_and(|bytes| bytes.len().saturating_add(overhead) <= max)
    };

    let build = |keep_artifacts: bool, keep_streams: bool| -> WorkerMessage {
        let mut dropped = Vec::new();
        if result.asset_bytes.is_some() {
            dropped.push("asset_bytes");
        }
        if !keep_artifacts && !result.artifacts.is_empty() {
            dropped.push("artifacts");
        }
        if !keep_streams {
            if !result.stdout.is_empty() {
                dropped.push("stdout");
            }
            if !result.stderr.is_empty() {
                dropped.push("stderr");
            }
        }
        if dropped.is_empty() {
            dropped.push("stdout, stderr");
        }
        let reason = format!(
            "Task result size ({size} bytes) exceeds maximum frame limit ({max} bytes); dropped {}",
            dropped.join(", ")
        );
        let combined_error = match result.error.as_deref().filter(|s| !s.is_empty()) {
            Some(orig) => {
                let prefix = truncate_utf8_prefix(orig, MAX_PRESERVED_ERROR_BYTES);
                format!("{prefix}; {reason}")
            }
            None => reason.clone(),
        };
        let (stdout, stderr) = if keep_streams {
            let mut err_bytes = result.stderr.0.to_vec();
            if !err_bytes.is_empty() && !err_bytes.ends_with(b"\n") {
                err_bytes.push(b'\n');
            }
            err_bytes.extend_from_slice(reason.as_bytes());
            (
                result.stdout.clone(),
                rusty_grid_core::task::Bytes::from(err_bytes),
            )
        } else {
            (
                rusty_grid_core::task::Bytes::new(),
                rusty_grid_core::task::Bytes::from(reason),
            )
        };
        let mut candidate = TaskResult::failure(
            result.worker_id,
            result.task_id,
            exit_code,
            stdout,
            stderr,
            result.execution_time_ms,
            Some(combined_error),
        );
        candidate.is_gpu_executed = result.is_gpu_executed;
        candidate.device_name = result.device_name.clone();
        if keep_artifacts {
            candidate.artifacts = result.artifacts.clone();
        }
        candidate.outputs = result.outputs.clone();
        WorkerMessage::from(candidate)
    };

    if result.asset_bytes.is_some() {
        let candidate = build(true, true);
        if fits(&candidate) {
            return candidate;
        }
    }
    if !result.artifacts.is_empty() {
        let candidate = build(false, true);
        if fits(&candidate) {
            return candidate;
        }
    }
    build(false, false)
}

/// Writes queued messages to one lane until told to close or until a write fails.
///
/// The control queue (heartbeats, telemetry, the farewell) is always drained before the next data
/// message (progress, results, checkpoints) is started, so a burst of results can never push a
/// heartbeat back by more than the one frame already on the wire. An oversized `TaskResult` is
/// rewritten into a failure that fits (`rewrite_oversized_task_result`) and an oversized
/// `Checkpoint` is dropped with a warning; any other write failure is reported through `events`.
/// On `closing` the writer first flushes the control queue and then returns.
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
            if let ProtocolError::FrameTooLarge { size, max } = e {
                match msg {
                    WorkerMessage::TaskResult {
                        worker_id,
                        task_id,
                        exit_code,
                        stdout,
                        stderr,
                        execution_time_ms,
                        is_gpu_executed,
                        device_name,
                        error,
                        asset_bytes,
                        artifacts,
                        outputs,
                    } => {
                        warn!(
                            %worker_id,
                            %task_id,
                            size,
                            max,
                            "Task result exceeds maximum frame size; failing only this task"
                        );
                        let fallback = rewrite_oversized_task_result(
                            TaskResult {
                                worker_id,
                                task_id,
                                exit_code,
                                stdout,
                                stderr,
                                execution_time_ms,
                                is_gpu_executed,
                                device_name,
                                error,
                                asset_bytes,
                                artifacts,
                                outputs,
                            },
                            size,
                            max,
                            writer.codec(),
                            writer.is_encrypted(),
                        );
                        if let Err(fallback_err) = writer.send_msg(&fallback).await {
                            let _ = events.send(Inbound::Failed(lane, fallback_err)).await;
                            return;
                        }
                        continue;
                    }
                    WorkerMessage::Checkpoint {
                        task_id, sequence, ..
                    } => {
                        warn!(
                            %task_id,
                            sequence,
                            size,
                            max,
                            "Checkpoint exceeds maximum frame size; dropping oversized checkpoint"
                        );
                        continue;
                    }
                    _ => {}
                }
            }
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
        self.readers
            .push(spawn_lane_reader(reader, lane, self.inbound_tx.clone()));
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

/// Task runner tasks are spawned and hold a clone of `active_task_table`, so they would outlive a
/// dropped `WorkerClient` (for example when a caller aborts the task running `WorkerClient::run`)
/// and keep child processes alive in the background. Dropping `WorkerClient` signals any remaining
/// active runs to stop via their cancellation channel and drains them asynchronously so their
/// process trees and sandboxes clean up off the runtime thread before falling back to `abort()`.
impl Drop for WorkerClient {
    fn drop(&mut self) {
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            if let Ok(mut lock) = self.active_task_table.try_write() {
                for (_, handle) in lock.drain() {
                    if let Some(join) = Self::begin_stop(handle, "WorkerClient dropped") {
                        join.abort();
                    }
                }
            }
            return;
        };
        let mut joins = Vec::new();
        if let Ok(mut lock) = self.active_task_table.try_write() {
            for (_, handle) in lock.drain() {
                if let Some(join) = Self::begin_stop(handle, "WorkerClient dropped") {
                    joins.push(join);
                }
            }
        } else {
            let table = Arc::clone(&self.active_task_table);
            rt.spawn(async move {
                let handles: Vec<TaskExecutionHandle> = {
                    let mut lock = table.write().await;
                    lock.drain().map(|(_, h)| h).collect()
                };
                let joins: Vec<JoinHandle<()>> = handles
                    .into_iter()
                    .filter_map(|h| Self::begin_stop(h, "WorkerClient dropped"))
                    .collect();
                let deadline = tokio::time::Instant::now() + TASK_DRAIN_TIMEOUT;
                for mut join in joins {
                    if tokio::time::timeout_at(deadline, &mut join).await.is_err() {
                        join.abort();
                    }
                }
            });
            return;
        }
        if !joins.is_empty() {
            rt.spawn(async move {
                let deadline = tokio::time::Instant::now() + TASK_DRAIN_TIMEOUT;
                for mut join in joins {
                    if tokio::time::timeout_at(deadline, &mut join).await.is_err() {
                        join.abort();
                    }
                }
            });
        }
    }
}

/// What the worker advertises in `WorkerCapabilities::tags`: its system tags (`os:`, `arch:`, see
/// `rusty_grid_core::placement`), its effective task concurrency (`slots:<n>`), its isolation
/// level (`iso:<level>`), its probed host tools (`tool:<name>`), then the
/// configured user tags that are not among them.
///
/// On Windows, when the worker process is elevated (`elevated_on_windows`) and `isolation` is
/// `Fs` or `FsNet`, `"tool:cargo"` is omitted because opening `NUL` (`\??\NUL`, used by
/// `Stdio::null()` inside `cargo`) fails with `Access is denied (os error 5)` inside an
/// AppContainer under an elevated logon session (design 0033 D4).
fn advertised_tags(
    user_tags: &[String],
    slots: usize,
    isolation: IsolationLevel,
    tool_tags: &[String],
    elevated_on_windows: bool,
) -> Vec<String> {
    let drop_cargo = elevated_on_windows && isolation != IsolationLevel::Off;
    let mut tags = rusty_grid_core::placement::system_tags();
    tags.push(format!(
        "{}{}",
        rusty_grid_core::placement::SLOTS_TAG_PREFIX,
        slots.max(1)
    ));
    tags.push(isolation.tag());
    for tag in tool_tags {
        if drop_cargo && tag == "tool:cargo" {
            continue;
        }
        if tag.starts_with(rusty_grid_core::placement::TOOL_TAG_PREFIX) && !tags.contains(tag) {
            tags.push(tag.clone());
        }
    }
    for tag in user_tags {
        if !tags.contains(tag)
            && !tag.starts_with(rusty_grid_core::placement::SLOTS_TAG_PREFIX)
            && !tag.starts_with(rusty_grid_core::placement::TOOL_TAG_PREFIX)
        {
            tags.push(tag.clone());
        }
    }
    tags
}

/// `RUSTY_GRID_ENABLE_PHYSICAL_GPU=1`: look for a real GPU without `--gpu` (the library-level
/// opt-in; the CLI always looks unless `--no-gpu`).
fn physical_gpu_requested_by_env() -> bool {
    std::env::var("RUSTY_GRID_ENABLE_PHYSICAL_GPU").as_deref() == Ok("1")
}

impl WorkerClient {
    /// Initializes a new WorkerClient, detecting host hardware and honoring CLI overrides.
    pub fn new(config: WorkerConfig) -> Self {
        suppress_windows_error_dialogs();
        let worker_id = resolve_worker_id(&config);
        let overrides = rusty_grid_core::capabilities::HardwareOverrides {
            name: config.name.clone(),
            cores: config.cores,
            ram_mb: config.ram_mb,
            gpu: config.gpu,
            no_gpu: config.no_gpu,
            simulate_gpu: config.simulate_gpu,
            gpu_name: config.gpu_name.clone(),
            blender: config.blender,
            no_blender: config.no_blender,
            blender_path: config.blender_path.clone(),
            blender_version: config.blender_version.clone(),
            vram_mb: config.vram_mb,
            cuda: config.cuda,
            no_cuda: config.no_cuda,
            no_battery: config.no_battery,
        };
        let mut capabilities = WorkerCapabilities::detect_with_overrides(overrides);
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
        let tool_tags = capabilities.tags.clone();
        let elevated_on_windows = {
            #[cfg(windows)]
            {
                crate::isolation::windows::is_process_elevated()
            }
            #[cfg(not(windows))]
            {
                false
            }
        };
        if elevated_on_windows && config.isolation != IsolationLevel::Off {
            warn!(
                worker_id = %worker_id,
                isolation = %config.isolation,
                "Worker is running under an elevated Windows token with --isolation {}; cargo and other programs that open NUL (\\??\\NUL) will fail with Access is denied (os error 5) because the elevated logon session's DosDevices directory has no ACE for CurrentUser. Run the worker non-elevated; dropping tool:cargo from advertised tags",
                config.isolation
            );
        }
        capabilities = capabilities.with_tags(advertised_tags(
            &config.tags,
            concurrency_permits,
            config.isolation,
            &tool_tags,
            elevated_on_windows,
        ));
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
            None => (
                MobileTelemetryHandle::new(capabilities.mobile.clone()),
                false,
            ),
        };

        let active_tasks = Arc::new(AtomicUsize::new(0));
        let heartbeat_tracker = Arc::new(HeartbeatTracker::new(Arc::clone(&active_tasks)));

        let mut sandbox_cfg = SandboxConfig::default();
        if let Some(ref sb) = config.sandbox_base_dir {
            sandbox_cfg.base_dir = sb.clone();
        }
        sandbox_cfg.keep_sandboxes = config.keep_sandboxes;
        let sandbox_base_dir = sandbox_cfg.base_dir.clone();

        let mut runner_cfg = RunnerConfig::new(sandbox_cfg)
            .with_max_output_bytes(config.max_output_bytes)
            .with_blob_cache_bytes(config.blob_cache_bytes)
            .with_blender_path(config.blender_path.clone())
            .with_output_limits(OutputLimits {
                max_bytes: config.output_limit_bytes,
                ..OutputLimits::default()
            });
        if let Some(priority) = config.process_priority {
            runner_cfg = runner_cfg.with_process_priority(priority);
        }
        runner_cfg = runner_cfg
            .with_isolation(config.isolation)
            .with_protected_files(config.protected_files.clone())
            .with_workspaces(config.workspaces)
            .with_workspace_root(config.workspace_root.clone())
            .with_workspace_limits(WorkspaceLimits {
                limit_mb: config.workspace_limit_mb,
                max_workspaces: config.max_workspaces,
                ..WorkspaceLimits::default()
            });
        let runner = Arc::new(TaskRunner::new(worker_id, capabilities.clone(), runner_cfg));
        let active_task_table = Arc::new(RwLock::new(HashMap::new()));
        let concurrency_semaphore = Arc::new(Semaphore::new(concurrency_permits));

        let initial_beacon = if config.p2p_ticket.is_none()
            && !config.master_addr.is_empty()
            && !config.master_addr.eq_ignore_ascii_case("auto")
            && !config.master_addr.starts_with("auto:")
        {
            portal_host_of(config.master_addr.trim()).map(|host| {
                rusty_grid_core::discovery::MasterBeacon::new(
                    config.master_addr.clone(),
                    format!("http://{}:8080", url_host(&host)),
                    "KnownMaster",
                    0,
                )
            })
        } else {
            None
        };
        let master_beacon = Arc::new(RwLock::new(initial_beacon));

        if config.no_battery {
            warn!(
                worker_id = %worker_id,
                "Host battery and thermal protection is disabled (no_battery=true); this worker will accept tasks regardless of battery level or thermal state"
            );
        }
        if config.p2p_minimal_preset {
            warn!(
                worker_id = %worker_id,
                "P2P minimal preset is enabled (p2p_minimal_preset=true); DERP relay and pkarr/DNS address lookup are disabled"
            );
        }

        info!(
            worker_id = %worker_id,
            name = %capabilities.name,
            cpu_cores = capabilities.cpu_cores,
            ram_mb = capabilities.ram_mb,
            has_gpu = capabilities.has_gpu,
            is_simulated_gpu = capabilities.is_simulated_gpu,
            "Worker client initialized"
        );

        let mut initial_features = advertised_protocol_capabilities();
        if config.session_resume_grace_cap == Some(Duration::ZERO) {
            initial_features.remove(ProtocolCapabilities::SESSION_RESUME);
        }
        if config.workspaces {
            initial_features.insert(ProtocolCapabilities::WORKSPACES);
        }

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
            initial_telemetry_delay: Duration::ZERO,
            sandbox_base_dir,
            next_attempt: Arc::new(AtomicU64::new(0)),
            cancelled_unseen: std::sync::Mutex::new(VecDeque::new()),
            master_shutdown_announced: false,
            master_takes_mobile_telemetry: AtomicBool::new(false),
            master_has_blobs: AtomicBool::new(false),
            negotiated_features: AtomicU64::new(initial_features.bits()),
            blob_client: Arc::new(std::sync::RwLock::new(None)),
            session_data_tx: Arc::new(std::sync::RwLock::new(None)),
            current_session_hold_secs: Arc::new(AtomicU64::new(0)),
            last_negotiated_hold_secs: AtomicU64::new(0),
            last_heartbeat_interval_ms: AtomicU64::new(0),
            resume_window_active: Arc::new(AtomicBool::new(false)),
            resume_deadline: None,
            pending_resume_outbox: Arc::new(std::sync::Mutex::new(
                PendingResumeOutbox::with_capacity(2 * concurrency_permits.max(1)),
            )),
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
        config.enable_redirection_portal = false;
        if let Some(n) = name {
            config = config.with_name(n);
        }
        if let Some(c) = cores {
            config = config.with_cores(c);
        }
        config = config.with_simulate_gpu(simulate_gpu);
        Self::new(config)
    }

    /// Test-only constructor initializing `WorkerClient` with host battery probing, CLI hardware
    /// probes, and the redirection portal disabled so tests do not depend on the test runner's
    /// battery state or installed host tools.
    pub fn for_testing(
        master_addr: impl Into<String>,
        name: Option<String>,
        cores: Option<usize>,
        simulate_gpu: bool,
    ) -> Self {
        let mut config = WorkerConfig::new(master_addr)
            .with_ram_mb(4096)
            .with_no_battery(true)
            .with_no_gpu(!simulate_gpu)
            .with_no_blender(true)
            .with_no_cuda(true);
        config.enable_redirection_portal = false;
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

    /// Replaces the advertised [`WorkerCapabilities`] on this client (e.g. in tests that construct
    /// a worker with specific capability tags).
    pub fn with_capabilities(mut self, capabilities: WorkerCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Test hook scaling `BuiltinTest` sleep durations by `multiplier.max(1)` on this worker's
    /// [`TaskRunner`] (`docs/design/0060-speed-aware-placement.md` integration test). Exists for
    /// integration tests only and is not exposed via CLI, environment variables, or wire messages.
    #[doc(hidden)]
    pub fn with_test_sleep_multiplier(mut self, multiplier: u32) -> Self {
        let runner_cfg = self
            .runner
            .config()
            .clone()
            .with_test_sleep_multiplier(multiplier);
        self.runner = Arc::new(TaskRunner::new(
            self.worker_id,
            self.capabilities.clone(),
            runner_cfg,
        ));
        self
    }

    /// Test hook adding `offset_ms` to `BuiltinTest` sleep durations on this worker's
    /// [`TaskRunner`] (`docs/design/0060-speed-aware-placement.md` integration test). Exists for
    /// integration tests only and is not exposed via CLI, environment variables, or wire messages.
    #[doc(hidden)]
    pub fn with_test_sleep_offset_ms(mut self, offset_ms: u64) -> Self {
        let runner_cfg = self
            .runner
            .config()
            .clone()
            .with_test_sleep_offset_ms(offset_ms);
        self.runner = Arc::new(TaskRunner::new(
            self.worker_id,
            self.capabilities.clone(),
            runner_cfg,
        ));
        self
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

    /// Delays the session's initial `WorkerMessage::MobileTelemetry` frame after `RegisterAck` by
    /// `delay` (for testing slow LTE / relay round-trip delivery).
    #[doc(hidden)]
    pub fn set_initial_telemetry_delay(&mut self, delay: Duration) {
        self.initial_telemetry_delay = delay;
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

    /// Protocol capabilities advertised by this worker instance in `WorkerMessage::Register`.
    /// Omits `SESSION_RESUME` when `--session-resume-grace-secs 0` was configured, and adds
    /// `WORKSPACES` only when `--workspaces` (`WorkerConfig::workspaces`) is enabled.
    pub fn advertised_features(&self) -> ProtocolCapabilities {
        let mut caps = advertised_protocol_capabilities();
        if self.config.session_resume_grace_cap == Some(Duration::ZERO) {
            caps.remove(ProtocolCapabilities::SESSION_RESUME);
        }
        if self.config.workspaces {
            caps.insert(ProtocolCapabilities::WORKSPACES);
        }
        caps
    }

    /// Effective keepalive interval for the current connection, accounting for negotiated
    /// `ADAPTIVE_KEEPALIVE` and mobile live status (matching `run_session`'s dead-peer watchdog).
    fn current_keepalive_interval(&self, base_interval: Duration) -> Duration {
        let adaptive_keepalive = self
            .negotiated_features()
            .contains(ProtocolCapabilities::ADAPTIVE_KEEPALIVE);
        if !adaptive_keepalive {
            return base_interval;
        }
        let active = self.heartbeat_tracker.active_tasks.load(Ordering::Relaxed);
        self.mobile
            .live_status()
            .map(|status| {
                status
                    .recommended_keepalive_interval_or(0, base_interval)
                    .max(status.recommended_keepalive_interval_or(active, base_interval))
            })
            .unwrap_or(base_interval)
            .max(base_interval)
    }

    /// Computes this worker's effective session-resume grace window from the most recent
    /// `RegisterAck`'s `session-resume=<secs>` and heartbeat interval (`design 0046 D2.4`).
    pub fn effective_resume_grace(&self) -> Duration {
        let hold_secs = self.last_negotiated_hold_secs.load(Ordering::Acquire);
        let hold = (hold_secs > 0).then(|| Duration::from_secs(hold_secs));
        let hb_ms = self.last_heartbeat_interval_ms.load(Ordering::Acquire);
        let base_hb = if hb_ms > 0 {
            Duration::from_millis(hb_ms)
        } else {
            self.config.default_heartbeat_interval
        };
        let hb = self.current_keepalive_interval(base_hb);
        let dead_peer = self.dead_peer_timeout(hb);
        compute_effective_resume_grace(hold, dead_peer, self.config.session_resume_grace_cap)
    }

    /// Task IDs that are currently executing or have completed results waiting in the
    /// session-resume outbox.
    async fn resumable_task_ids(&self) -> Vec<TaskId> {
        let active_ids: Vec<TaskId> = {
            let table = self.active_task_table.read().await;
            table.keys().copied().collect()
        };
        let mut seen = std::collections::HashSet::with_capacity(active_ids.len());
        let mut running = Vec::with_capacity(active_ids.len());
        for id in active_ids {
            if seen.insert(id) {
                running.push(id);
            }
        }
        let outbox = self
            .pending_resume_outbox
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for item in &outbox.results {
            let id = item.result.task_id;
            if !outbox.cancelled.contains(&id) && seen.insert(id) {
                running.push(id);
            }
        }
        running
    }

    /// Ends the session-resume grace window: clears any buffered checkpoints/results (releasing
    /// their pinned output blobs) and cancels any tasks still running.
    async fn expire_resume_window(&self, reason: &str) {
        self.resume_window_active.store(false, Ordering::Release);
        let had_used_blobs = {
            let mut outbox = self
                .pending_resume_outbox
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let used_blobs = outbox.results.iter().any(|r| r.used_blobs);
            outbox.clear();
            used_blobs
        };
        self.cancel_all_active_tasks(reason).await;
        self.pending_resume_outbox
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        if had_used_blobs {
            if let Some(cache) = self.runner.blob_cache_if_open() {
                let _ = tokio::task::spawn_blocking(move || cache.evict_to_fit()).await;
            }
        }
    }

    /// Primary entry point: runs the worker supervisor loop with automatic reconnection,
    /// exponential backoff, and jitter. Returns when the shutdown signal fires (or its sender is
    /// dropped), or with `GridError::Config` for a configuration no retry can fix (an unparsable
    /// P2P ticket). A Master going away, in any manner, only ever makes the worker reconnect.
    pub async fn run(&mut self, mut shutdown_rx: watch::Receiver<bool>) -> GridResult<()> {
        suppress_windows_error_dialogs();

        #[cfg(feature = "p2p")]
        rusty_grid_core::transport::resolve_p2p_relay_mode(
            self.config.p2p_minimal_preset,
            self.config.p2p_relay_url.as_deref(),
        )?;

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

        // Isolation that was asked for and cannot be enforced must stop the worker here, not let
        // it run tasks with the owner's file access.
        if self.config.isolation != IsolationLevel::Off {
            if let Err(err) = crate::isolation::verify_isolation_support_with_config(
                self.config.isolation,
                &self.runner.config().sandbox_config.base_dir,
                &self.runner.config().host_dirs,
                &self.runner.config().protected_files,
            )
            .await
            {
                error!(worker_id = %self.worker_id, error = %err, "Task isolation cannot be enforced");
                return Err(err);
            }
        }

        // Only with `--workspaces` does the worker open the persistent workspace store at start-up
        // (and refuse to start with a clear error if its root fails the private-dir checks).
        if self.config.workspaces {
            if self.config.workspace_limit_mb == 0 {
                let err = GridError::Config(
                    "Invalid --workspace-limit-mb: must be greater than 0".to_string(),
                );
                error!(worker_id = %self.worker_id, error = %err, "Workspace configuration invalid");
                return Err(err);
            }
            if self.config.max_workspaces == 0 {
                let err = GridError::Config(
                    "Invalid --max-workspaces: must be greater than 0".to_string(),
                );
                error!(worker_id = %self.worker_id, error = %err, "Workspace configuration invalid");
                return Err(err);
            }
            if let Err(e) = self.runner.ensure_workspace_store().await {
                let err = GridError::Config(format!("Workspace store cannot be opened: {e}"));
                error!(worker_id = %self.worker_id, error = %err, "Persistent workspaces cannot be initialized");
                return Err(err);
            }
        }

        // A user tag that looks like a system one (`os:`, `link:`, ...) would mislead placement.
        if let Some(e) = self
            .config
            .tags
            .iter()
            .find_map(|t| rusty_grid_core::placement::validate_user_tag(t).err())
        {
            let err = GridError::Config(format!("Invalid worker tag: {e}"));
            error!(worker_id = %self.worker_id, error = %err, "The configured worker tags cannot be used");
            return Err(err);
        }

        let mut backoff = ExponentialBackoff::new(self.backoff_config.clone());

        let _portal_guard = self.config.enable_redirection_portal.then(|| {
            let portal_port = self.config.redirection_portal_port;
            let portal_shutdown_rx = shutdown_rx.clone();
            let beacon_ref = Arc::clone(&self.master_beacon);
            let secret = self.config.cluster_secret.clone();
            AbortOnDrop(tokio::spawn(async move {
                spawn_worker_redirection_portal_with_secret(
                    portal_port,
                    beacon_ref,
                    secret,
                    portal_shutdown_rx,
                )
                .await;
            }))
        });

        // A worker that reads its own battery (a laptop, Termux) keeps the shared reading fresh;
        // one fed by a platform app is updated by that app instead.
        let _mobile_poller = (!self.mobile_fed_externally
            && !self.mobile.is_externally_fed()
            && !self.config.no_battery
            && self.mobile.snapshot().is_some())
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
                self.resume_deadline = None;
                self.expire_resume_window("FAST_SHUTDOWN").await;
                break;
            }

            self.master_shutdown_announced = false;
            let outcome = self.connect_and_run(&mut shutdown_rx, &mut backoff).await;

            let is_shutdown = *shutdown_rx.borrow();
            let evac_reason = current_evacuation_reason();
            let cancel_reason = if let Some(ref r) = evac_reason {
                r.clone()
            } else if is_shutdown {
                "FAST_SHUTDOWN".to_string()
            } else {
                "connection to the Master ended".to_string()
            };

            let keep_for_resume = outcome.is_err()
                && !is_shutdown
                && evac_reason.is_none()
                && self
                    .resume_deadline
                    .is_some_and(|d| tokio::time::Instant::now() < d);
            if !keep_for_resume {
                self.resume_deadline = None;
                self.expire_resume_window(&cancel_reason).await;
            }

            match outcome {
                Ok(()) => {
                    info!(worker_id = %self.worker_id, "Worker connection ended cleanly");
                    break;
                }
                Err(e) => {
                    if *shutdown_rx.borrow() {
                        info!(worker_id = %self.worker_id, "Worker terminating on shutdown signal after connection notice");
                        self.resume_deadline = None;
                        self.expire_resume_window("FAST_SHUTDOWN").await;
                        break;
                    }

                    if let GridError::Config(_) = e {
                        error!(worker_id = %self.worker_id, error = %e, "Worker configuration error; not retrying");
                        self.resume_deadline = None;
                        self.expire_resume_window(&cancel_reason).await;
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
                    let sleep_fut = tokio::time::sleep(delay);
                    tokio::pin!(sleep_fut);
                    let mut should_break = false;
                    loop {
                        let resume_wait = async {
                            match self.resume_deadline {
                                Some(d) => tokio::time::sleep_until(d).await,
                                None => std::future::pending::<()>().await,
                            }
                        };
                        tokio::select! {
                            biased;
                            res = shutdown_rx.changed() => {
                                if res.is_ok() && *shutdown_rx.borrow() {
                                    info!(worker_id = %self.worker_id, "Shutdown signal received during backoff wait");
                                    self.resume_deadline = None;
                                    self.expire_resume_window("FAST_SHUTDOWN").await;
                                    should_break = true;
                                    break;
                                } else if res.is_err() {
                                    self.resume_deadline = None;
                                    self.expire_resume_window("FAST_SHUTDOWN").await;
                                    should_break = true;
                                    break;
                                }
                            }
                            _ = resume_wait => {
                                self.resume_deadline = None;
                                self.expire_resume_window("session-resume grace window expired").await;
                            }
                            _ = &mut sleep_fut => break,
                        }
                    }
                    if should_break {
                        break;
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
            let mut seen = self
                .cancelled_unseen
                .lock()
                .unwrap_or_else(|e| e.into_inner());
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

        let negotiated = self.negotiated_features();
        if task.workspace.is_some()
            && (!self.config.workspaces || !negotiated.contains(ProtocolCapabilities::WORKSPACES))
        {
            warn!(
                task_id = %task_id,
                "Refusing task with workspace: WORKSPACES is not enabled on this worker or was not negotiated in RegisterAck"
            );
            let result = TaskResult::failure(
                worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                "Task requires WORKSPACES capability, which is not enabled on this worker (start the worker with --workspaces)",
                0,
                Some("WORKSPACES capability not enabled or negotiated".to_string()),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        let has_workspace_tree = task
            .workspace
            .as_ref()
            .is_some_and(|ws| ws.source.is_some());

        if (!task.staged_files.is_empty() || !task.expected_outputs.is_empty())
            && !negotiated.contains(ProtocolCapabilities::FILE_STAGING)
        {
            warn!(
                task_id = %task_id,
                "Refusing task with staged_files/expected_outputs: FILE_STAGING was not acknowledged in RegisterAck"
            );
            let result = TaskResult::failure(
                worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                "Task requires FILE_STAGING capability, which was not negotiated in RegisterAck",
                0,
                Some(
                    "FILE_STAGING capability not negotiated with master in RegisterAck".to_string(),
                ),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        if (!task.inputs.is_empty() || !task.output_globs.is_empty() || has_workspace_tree)
            && !negotiated.contains(ProtocolCapabilities::BLOB_TRANSFER)
        {
            warn!(
                task_id = %task_id,
                "Refusing task with blob inputs/output_globs/workspace tree: BLOB_TRANSFER was not acknowledged in RegisterAck"
            );
            let result = TaskResult::failure(
                worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                "Task requires BLOB_TRANSFER capability, which was not negotiated in RegisterAck",
                0,
                Some(
                    "BLOB_TRANSFER capability not negotiated with master in RegisterAck"
                        .to_string(),
                ),
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
        // Nothing may touch the sandbox base (the reclaim below, the blob cache, the sandbox
        // itself) while another local user could have replaced its private root (design 0021).
        if let Err(e) = self
            .runner
            .config()
            .sandbox_config
            .ensure_private_root_recheck()
            .await
        {
            warn!(task_id = %task_id, error = %e, "Refusing a task: the sandbox base is not safe");
            let result = TaskResult::failure(
                worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                e.to_string(),
                0,
                Some(format!("Sandbox base: {e}")),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }
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
        let blob_client_slot = Arc::clone(&self.blob_client);
        let session_data_tx = Arc::clone(&self.session_data_tx);
        let current_session_hold_secs = Arc::clone(&self.current_session_hold_secs);
        let resume_window_active = Arc::clone(&self.resume_window_active);
        let pending_resume_outbox = Arc::clone(&self.pending_resume_outbox);
        let grace_enabled = self.config.session_resume_grace_cap != Some(Duration::ZERO);

        // The table lock is held from before the spawn until the handle is inserted, so the run's
        // own cleanup (which needs the lock) can never overtake the insertion and leave a stale
        // entry behind.
        let mut lock = self.active_task_table.write().await;
        let runner_handle = tokio::spawn(async move {
            let current_tx =
                |session_data_tx: &std::sync::RwLock<Option<mpsc::Sender<WorkerMessage>>>,
                 fallback_tx: &mpsc::Sender<WorkerMessage>|
                 -> Option<mpsc::Sender<WorkerMessage>> {
                    if let Some(tx) = session_data_tx
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone()
                    {
                        if !tx.is_closed() {
                            return Some(tx);
                        }
                    }
                    (!fallback_tx.is_closed()).then(|| fallback_tx.clone())
                };

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
                        if let Some(tx) = current_tx(&session_data_tx, &outbound_tx) {
                            let _ = tx.send(WorkerMessage::from(res)).await;
                        }
                    }
                    return;
                }
            };

            let mut _guard = ActiveTaskGuard {
                task_id,
                attempt,
                heartbeat_tracker: Arc::clone(&tracker),
                active_task_table: Arc::clone(&table),
                runner: Some(Arc::clone(&runner)),
                _permit: permit,
            };

            tracker.increment_active_tasks();

            let progress = WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status: TaskStatus::Running,
            };
            if let Some(tx) = current_tx(&session_data_tx, &outbound_tx) {
                if tx.send(progress).await.is_err() && !resume_window_active.load(Ordering::Acquire)
                {
                    return;
                }
            } else if !resume_window_active.load(Ordering::Acquire) {
                return;
            }

            // Every blob the inputs (and workspace tree, if any) need is in the cache (and pinned)
            // before the run starts.
            let mut cancel_slot = Some(cancel_rx);
            let _inputs = if task.inputs.is_empty() && !has_workspace_tree {
                None
            } else {
                let current_blob_client = blob_client_slot
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let fetch = async {
                    // Checked again: the task may have waited for a slot since it was assigned.
                    runner
                        .config()
                        .sandbox_config
                        .ensure_private_root_recheck()
                        .await
                        .map_err(|e| e.to_string())?;
                    let cache = runner.ensure_blob_cache().await?;
                    fetch_task_inputs(&task, current_blob_client.as_deref(), &cache).await
                };
                let failure = tokio::select! {
                    fetched = fetch => match fetched {
                        Ok(pin) => Ok(pin),
                        Err(e) => Err(TaskResult::failure(
                            worker_id,
                            task_id,
                            EXIT_CODE_GENERAL_ERROR,
                            "",
                            format!("Fetching input files failed: {e}"),
                            0,
                            Some(format!("Input files: {e}")),
                        )),
                    },
                    reason = cancellation(&mut cancel_slot) => Err(TaskResult::failure(
                        worker_id,
                        task_id,
                        EXIT_CODE_CANCELLED,
                        "",
                        "",
                        0,
                        Some(format!("Cancelled: {reason}")),
                    )),
                };
                match failure {
                    Ok(pin) => Some(pin),
                    Err(result) => {
                        if !suppress.load(Ordering::Acquire) {
                            if let Some(tx) = current_tx(&session_data_tx, &outbound_tx) {
                                let _ = tx.send(WorkerMessage::from(result)).await;
                            }
                        }
                        return;
                    }
                }
            };

            let effective_checkpoint = checkpoint.or_else(|| task.latest_checkpoint.clone());
            let (cp_tx, mut cp_rx) = mpsc::channel::<WorkerMessage>(16);
            let checkpoint_tx = negotiated
                .contains(ProtocolCapabilities::CHECKPOINT_DELTA)
                .then_some(cp_tx);
            let forward_checkpoint = |cp_msg: WorkerMessage| {
                let mut sent = false;
                if let Some(tx) = current_tx(&session_data_tx, &outbound_tx) {
                    if tx.try_send(cp_msg.clone()).is_ok() {
                        sent = true;
                    }
                }
                if !sent && resume_window_active.load(Ordering::Acquire) {
                    let mut outbox = pending_resume_outbox
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    if !outbox.cancelled.contains(&task_id) {
                        outbox.checkpoints.insert(task_id, cp_msg);
                    }
                }
            };
            let (exec_cancel_tx, exec_cancel_rx) = oneshot::channel::<String>();
            let mut exec_cancel_tx = Some(exec_cancel_tx);
            let mut cancelled_reason: Option<String> = None;
            let mut result = {
                let exec_fut = runner.execute_task_with_checkpoint(
                    &task,
                    effective_checkpoint.as_ref(),
                    checkpoint_tx,
                    Some(child_pid_clone),
                    Some(exec_cancel_rx),
                );
                tokio::pin!(exec_fut);
                loop {
                    tokio::select! {
                        res = &mut exec_fut => break res,
                        Some(cp_msg) = cp_rx.recv() => {
                            forward_checkpoint(cp_msg);
                        }
                        reason = cancellation(&mut cancel_slot) => {
                            if let Some(tx) = exec_cancel_tx.take() {
                                let _ = tx.send(reason.clone());
                            }
                            // Remember the cancellation even when `tx.send` succeeded: if the
                            // child process already exited and `execute_process_supervised` is
                            // draining pipes or collecting outputs, `exec_cancel_rx` is not
                            // polled again and `exec_fut` returns the process's exit code.
                            cancelled_reason = Some(reason);
                        }
                    }
                }
            };
            while let Ok(cp_msg) = cp_rx.try_recv() {
                forward_checkpoint(cp_msg);
            }

            // Output files reach the Master's blob store before the result that names them.
            let used_blobs = _inputs.is_some() || !result.outputs.is_empty();
            let output_pin = runner
                .blob_cache_if_open()
                .and_then(|cache| cache.take_task_outputs(task.id))
                .map(Arc::new);
            // Ownership of this attempt's output pin now lives in `output_pin`; prevent `_guard`
            // from releasing a subsequent retry attempt's `task_outputs` entry on drop.
            _guard.runner = None;
            let resume_enabled = grace_enabled
                && (current_session_hold_secs.load(Ordering::Acquire) > 0
                    || resume_window_active.load(Ordering::Acquire));
            let mut buffered_token = master_features::result_ack_token(&result);

            if cancelled_reason.is_none() && !suppress.load(Ordering::Acquire) {
                let (live_tx, evicted_used_blobs) = {
                    let mut outbox = pending_resume_outbox
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    let tx = current_tx(&session_data_tx, &outbound_tx);
                    let evicted = if resume_enabled {
                        outbox.push_result(BufferedTaskResult::new(
                            task.clone(),
                            result.clone(),
                            output_pin.clone(),
                            used_blobs,
                        ))
                    } else {
                        false
                    };
                    (tx, evicted)
                };
                if evicted_used_blobs {
                    if let Some(cache) = runner.blob_cache_if_open() {
                        let _ = tokio::task::spawn_blocking(move || cache.evict_to_fit()).await;
                    }
                }
                if live_tx.is_none() && resume_enabled {
                    drop(output_pin);
                    drop(_inputs);
                    drop(cancel_slot);
                    return;
                }

                let current_blob_client = blob_client_slot
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let had_outputs = !result.outputs.is_empty();
                if had_outputs {
                    match runner.ensure_blob_cache().await {
                        Ok(cache) => {
                            tokio::select! {
                                () = upload_task_outputs(&task, &mut result, current_blob_client.as_deref(), &cache) => {}
                                reason = cancellation(&mut cancel_slot) => {
                                    cancelled_reason = Some(reason);
                                }
                            }
                        }
                        Err(e) => {
                            result.outputs.clear();
                            result.error = Some(e);
                        }
                    }
                    if resume_enabled && cancelled_reason.is_none() {
                        buffered_token = pending_resume_outbox
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .update_result(task.id, buffered_token, result.clone());
                    }
                }

                let blob_client_closed = had_outputs
                    && current_blob_client
                        .as_ref()
                        .is_some_and(|client| client.is_closed());
                if cancelled_reason.is_none()
                    && !suppress.load(Ordering::Acquire)
                    && resume_enabled
                    && (blob_client_closed || current_tx(&session_data_tx, &outbound_tx).is_none())
                {
                    drop(output_pin);
                    drop(_inputs);
                    drop(cancel_slot);
                    return;
                }
            }

            if (cancelled_reason.is_some() || suppress.load(Ordering::Acquire)) && resume_enabled {
                pending_resume_outbox
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .ack_result(task.id, Some(buffered_token));
            }

            drop(output_pin);
            drop(_inputs);
            if used_blobs {
                if let Some(cache) = runner.blob_cache_if_open() {
                    let _ = tokio::task::spawn_blocking(move || cache.evict_to_fit()).await;
                }
            }

            if let Some(reason) = cancelled_reason {
                if result.exit_code != EXIT_CODE_CANCELLED {
                    result.exit_code = EXIT_CODE_CANCELLED;
                    result.error = Some(format!("Cancelled: {reason}"));
                }
                result.outputs.clear();
                result.artifacts.clear();
                result.asset_bytes = None;
            }

            if !suppress.load(Ordering::Acquire) {
                if let Some(tx) = current_tx(&session_data_tx, &outbound_tx) {
                    let _ = tx.send(WorkerMessage::from(result)).await;
                }
            }
            drop(cancel_slot);
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
        if let Some(ack_token) = reason
            .as_deref()
            .and_then(rusty_grid_core::protocol::master_features::parse_result_ack_reason)
        {
            let used_blobs = self
                .pending_resume_outbox
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .ack_result(task_id, ack_token);
            if used_blobs {
                if let Some(cache) = self.runner.blob_cache_if_open() {
                    let _ = tokio::task::spawn_blocking(move || cache.evict_to_fit()).await;
                }
            }
            return;
        }

        let (had_buffered, used_blobs) = {
            let mut outbox = self
                .pending_resume_outbox
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let had = outbox.checkpoints.contains_key(&task_id)
                || outbox.results.iter().any(|r| r.result.task_id == task_id);
            let used_blobs = outbox.cancel_task(task_id);
            (had, used_blobs)
        };
        if used_blobs {
            self.runner.release_task_outputs(task_id);
            if let Some(cache) = self.runner.blob_cache_if_open() {
                let _ = tokio::task::spawn_blocking(move || cache.evict_to_fit()).await;
            }
        }
        let mut lock = self.active_task_table.write().await;
        if let Some(mut handle) = lock.remove(&task_id) {
            let r = reason.unwrap_or_else(|| "Cancelled by master directive".into());
            if r.contains("no longer assigned to this worker") {
                handle.suppress_result.store(true, Ordering::Release);
            }
            info!(task_id = %task_id, reason = %r, "Cancelling active task");
            let sent = if let Some(tx) = handle.cancel_tx.take() {
                tx.send(r).is_ok()
            } else {
                false
            };
            if !sent {
                handle.abort_handle.abort();
            }
        } else if had_buffered {
            info!(
                task_id = %task_id,
                reason = ?reason,
                "Dropped buffered session-resume result for task cancelled by master"
            );
        } else {
            warn!(task_id = %task_id, "CancelTask received for unknown or already completed task");
            // The Master sends AssignTask before CancelTask, but a cancelled task that is still
            // on its way must not run once it arrives; a task id is never assigned again after
            // it was cancelled, so remembering the id is safe.
            let mut seen = self
                .cancelled_unseen
                .lock()
                .unwrap_or_else(|e| e.into_inner());
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
        if let Some(mut join) =
            Self::begin_stop(previous, "superseded by a new assignment of the same task")
        {
            if tokio::time::timeout(TASK_DRAIN_TIMEOUT, &mut join)
                .await
                .is_err()
            {
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
        let is_fast_evac = {
            let r = reason.to_ascii_uppercase();
            r.contains("EVAC")
                || r.contains("BATTERY")
                || r.contains("SHUTDOWN")
                || r.contains("FAST")
        };
        if is_fast_evac {
            for join in joins {
                join.abort();
            }
        } else {
            let deadline = tokio::time::Instant::now() + TASK_DRAIN_TIMEOUT;
            for mut join in joins {
                if tokio::time::timeout_at(deadline, &mut join).await.is_err() {
                    warn!("A cancelled task did not stop within the drain timeout; aborting it");
                    join.abort();
                }
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
                Ok(()) => {
                    info!(task_id = %task_id, moved_to = %aside.display(), "Moved a leftover sandbox of this task aside")
                }
                Err(e) => {
                    warn!(task_id = %task_id, error = %e, "Could not move a leftover sandbox aside")
                }
            }
        } else {
            match tokio::fs::remove_dir_all(&path).await {
                Ok(()) => {
                    info!(task_id = %task_id, path = %path.display(), "Removed a leftover sandbox of this task")
                }
                Err(e) => {
                    warn!(task_id = %task_id, error = %e, "Could not remove a leftover sandbox")
                }
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
    async fn establish_stream(&self) -> GridResult<(WorkerStreamMode, String)> {
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
                let ep = rusty_grid_core::transport::bind_p2p_endpoint_with_relay(
                    self.config.p2p_minimal_preset,
                    self.config.p2p_relay_url.as_deref(),
                )
                .await?;
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
        let conn = match rusty_grid_core::transport::dial_p2p_endpoint(
            &endpoint,
            node_addr,
            connect_timeout,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                reset_cached_endpoint().await;
                return Err(e);
            }
        };

        // Open the two lanes: Control / Telemetry (Tag 0x01) and Data / Workload (Tag 0x02).
        // A peer that accepts the connection but never completes the stream setup must not
        // be able to park the worker here forever.
        let open_lanes = async {
            let (ctrl_send, ctrl_recv) = rusty_grid_core::transport::open_p2p_tagged_stream(
                &conn,
                rusty_grid_core::transport::STREAM_CONTROL,
                "control",
            )
            .await?;
            let (data_send, data_recv) = rusty_grid_core::transport::open_p2p_tagged_stream(
                &conn,
                rusty_grid_core::transport::STREAM_DATA,
                "data",
            )
            .await?;
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
        let mut deadline = self.resume_deadline;
        let established = {
            let establish_fut = self.establish_stream();
            tokio::pin!(establish_fut);
            loop {
                let resume_wait = async {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending::<()>().await,
                    }
                };
                tokio::select! {
                    biased;
                    _ = shutdown_requested(shutdown_rx) => break None,
                    _ = resume_wait => {
                        deadline = None;
                        self.expire_resume_window("session-resume grace window expired").await;
                    }
                    res = &mut establish_fut => break Some(res),
                }
            }
        };
        self.resume_deadline = deadline;
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
                let mut deadline = self.resume_deadline;
                let handshake = {
                    let hs_fut = self.perform_handshake(&mut transport);
                    tokio::pin!(hs_fut);
                    loop {
                        let resume_wait = async {
                            match deadline {
                                Some(d) => tokio::time::sleep_until(d).await,
                                None => std::future::pending::<()>().await,
                            }
                        };
                        tokio::select! {
                            biased;
                            _ = shutdown_requested(shutdown_rx) => break None,
                            _ = resume_wait => {
                                deadline = None;
                                self.expire_resume_window("session-resume grace window expired").await;
                            }
                            res = &mut hs_fut => break Some(res),
                        }
                    }
                };
                self.resume_deadline = deadline;
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
                self.run_session(session, heartbeat_interval, shutdown_rx)
                    .await
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
                let mut deadline = self.resume_deadline;
                let handshake = {
                    let hs_fut = self.perform_handshake(&mut ctrl_transport);
                    tokio::pin!(hs_fut);
                    loop {
                        let resume_wait = async {
                            match deadline {
                                Some(d) => tokio::time::sleep_until(d).await,
                                None => std::future::pending::<()>().await,
                            }
                        };
                        tokio::select! {
                            biased;
                            _ = shutdown_requested(shutdown_rx) => break None,
                            _ = resume_wait => {
                                deadline = None;
                                self.expire_resume_window("session-resume grace window expired").await;
                            }
                            res = &mut hs_fut => break Some(res),
                        }
                    }
                };
                self.resume_deadline = deadline;
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
                self.run_session(session, heartbeat_interval, shutdown_rx)
                    .await
            }
        }
    }

    /// Returns the intersection of `ProtocolCapabilities` supported by this worker and
    /// acknowledged by the Master in the most recent `RegisterAck`.
    pub fn negotiated_features(&self) -> ProtocolCapabilities {
        ProtocolCapabilities::from_bits_truncate(self.negotiated_features.load(Ordering::Acquire))
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
        if self.current_session_hold_secs.load(Ordering::Acquire) == 0 {
            self.expire_resume_window("reconnected to a Master with session-resume disabled")
                .await;
        }
        // Blob requests ride the data queue: the QUIC data stream, never the control stream.
        // Snapshot the outbox and publish `blob_client` + `session_data_tx` while holding the
        // outbox lock before clearing `resume_window_active` so a task finishing right as the
        // session starts never sees both `session_data_tx == None` and `resume_window_active == false`.
        let blob_client = self
            .master_has_blobs
            .load(Ordering::Acquire)
            .then(|| BlobClient::new(session.data_tx.clone()));
        let (buffered_checkpoints, buffered_results) = {
            let mut outbox = self
                .pending_resume_outbox
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            outbox.cancelled.clear();
            let cps: Vec<(TaskId, WorkerMessage)> = outbox.checkpoints.drain().collect();
            let res: Vec<BufferedTaskResult> = outbox.results.clone();
            *self.blob_client.write().unwrap_or_else(|e| e.into_inner()) = blob_client.clone();
            *self
                .session_data_tx
                .write()
                .unwrap_or_else(|e| e.into_inner()) = Some(session.data_tx.clone());
            self.resume_window_active.store(false, Ordering::Release);
            (cps, res)
        };
        self.resume_deadline = None;

        let adaptive_keepalive = self
            .negotiated_features()
            .contains(ProtocolCapabilities::ADAPTIVE_KEEPALIVE);
        let heartbeat_handle = spawn_adaptive_heartbeat_task(
            worker_id,
            heartbeat_interval,
            session.control_tx.clone(),
            Arc::clone(&self.heartbeat_tracker),
            adaptive_keepalive.then(|| self.mobile.clone()),
        );

        let mut dead_after =
            self.dead_peer_timeout(self.current_keepalive_interval(heartbeat_interval));
        let mut watchdog = tokio::time::interval(
            (dead_after / 8).clamp(Duration::from_millis(25), Duration::from_secs(1)),
        );
        watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut telemetry = if self.initial_telemetry_delay > Duration::ZERO {
            tokio::time::interval_at(
                tokio::time::Instant::now() + self.initial_telemetry_delay,
                MOBILE_TELEMETRY_CHECK_INTERVAL,
            )
        } else {
            tokio::time::interval(MOBILE_TELEMETRY_CHECK_INTERVAL)
        };
        telemetry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // `Register` carries only `MobileCapabilities` (no `network_type`, `battery_temperature`,
        // or tiers), so send one `MobileTelemetry` right after `RegisterAck` when the Master
        // announced `mobile-telemetry` (unless a test configured `initial_telemetry_delay`).
        let mut last_reported: Option<MobileLiveStatus> = None;
        // Only to a Master that said it can read the report (an older one would drop us for it).
        let report_telemetry = self.master_takes_mobile_telemetry.load(Ordering::Acquire);
        if report_telemetry && self.initial_telemetry_delay.is_zero() {
            if let Some(status) = self.mobile.live_status() {
                let report = WorkerMessage::MobileTelemetry {
                    worker_id,
                    battery_pct: status.battery_pct,
                    is_charging: status.is_charging,
                    thermal_throttled: status.thermal_throttled,
                    battery_temperature: status.battery_temperature,
                    network_type: status.network_type.clone(),
                    battery_tier: status.battery_tier,
                    thermal_tier: status.thermal_tier,
                };
                if session.control_tx.try_send(report).is_ok() {
                    last_reported = Some(status);
                }
            }
        }
        let mut farewell: Option<WorkerMessage> = None;
        let _outbox_flush_guard = (!buffered_checkpoints.is_empty()
            || !buffered_results.is_empty())
        .then(|| {
            let data_tx = session.data_tx.clone();
            let flush_blob_client = blob_client.clone();
            let flush_runner = Arc::clone(&self.runner);
            let flush_outbox = Arc::clone(&self.pending_resume_outbox);
            AbortOnDrop(tokio::spawn(async move {
                for (tid, cp_msg) in buffered_checkpoints {
                    let cancelled = flush_outbox
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .cancelled
                        .contains(&tid);
                    if !cancelled && data_tx.send(cp_msg).await.is_err() {
                        return;
                    }
                }
                for item in buffered_results {
                    let still_pending = {
                        let outbox = flush_outbox.lock().unwrap_or_else(|e| e.into_inner());
                        !outbox.cancelled.contains(&item.task.id)
                            && outbox.results.iter().any(|r| {
                                r.result.task_id == item.task.id && r.ack_token == item.ack_token
                            })
                    };
                    if !still_pending {
                        continue;
                    }

                    let mut result = item.result.clone();
                    if !result.outputs.is_empty() {
                        match flush_runner.ensure_blob_cache().await {
                            Ok(cache) => {
                                upload_task_outputs(
                                    &item.task,
                                    &mut result,
                                    flush_blob_client.as_deref(),
                                    &cache,
                                )
                                .await;
                            }
                            Err(e) => {
                                result.outputs.clear();
                                result.error = Some(e);
                            }
                        }
                    }

                    if !item.result.outputs.is_empty()
                        && flush_blob_client
                            .as_ref()
                            .is_some_and(|client| client.is_closed())
                    {
                        return;
                    }

                    let current_token = {
                        let mut outbox = flush_outbox.lock().unwrap_or_else(|e| e.into_inner());
                        outbox.update_result(item.task.id, item.ack_token, result.clone())
                    };
                    let still_pending = {
                        let outbox = flush_outbox.lock().unwrap_or_else(|e| e.into_inner());
                        !outbox.cancelled.contains(&item.task.id)
                            && outbox.results.iter().any(|r| {
                                r.result.task_id == item.task.id && r.ack_token == current_token
                            })
                    };
                    if !still_pending {
                        continue;
                    }

                    if data_tx.send(WorkerMessage::from(result)).await.is_err() {
                        return;
                    }
                }
            }))
        });

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
                            MasterMessage::RoleHandoverAck { accepted, reason, .. } => {
                                info!(accepted, reason = ?reason, "Received RoleHandoverAck from master");
                            }
                            MasterMessage::UnknownVariant { tag, ref raw_bytes } => {
                                warn!(tag, bytes_len = raw_bytes.len(), "Ignoring unknown MasterMessage variant");
                            }
                            MasterMessage::BlobReply { request_id, reply } => {
                                let delivered = blob_client
                                    .as_ref()
                                    .is_some_and(|client| client.deliver(request_id, reply));
                                if !delivered {
                                    debug!(request_id, "Blob reply for a request nobody waits for");
                                }
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
                    dead_after = dead_after
                        .max(self.dead_peer_timeout(self.current_keepalive_interval(heartbeat_interval)));
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
                        if let Some(ref status) = current {
                            let report = WorkerMessage::MobileTelemetry {
                                worker_id,
                                battery_pct: status.battery_pct,
                                is_charging: status.is_charging,
                                thermal_throttled: status.thermal_throttled,
                                battery_temperature: status.battery_temperature,
                                network_type: status.network_type.clone(),
                                battery_tier: status.battery_tier,
                                thermal_tier: status.thermal_tier,
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
                        let reason = current_evacuation_reason()
                            .unwrap_or_else(|| "Graceful worker shutdown".to_string());
                        info!(worker_id = %worker_id, reason = %reason, "Worker shutdown initiated; notifying Master");
                        farewell = Some(WorkerMessage::Disconnecting {
                            worker_id,
                            reason,
                        });
                        break Ok(());
                    } else if res.is_err() {
                        break Ok(());
                    }
                }
            }
        };

        let grace = self.effective_resume_grace();
        if outcome.is_err()
            && !*shutdown_rx.borrow()
            && current_evacuation_reason().is_none()
            && grace > Duration::ZERO
        {
            self.resume_deadline = Some(tokio::time::Instant::now() + grace);
            self.resume_window_active.store(true, Ordering::Release);
        } else {
            self.resume_deadline = None;
            self.resume_window_active.store(false, Ordering::Release);
        }
        *self
            .session_data_tx
            .write()
            .unwrap_or_else(|e| e.into_inner()) = None;

        // Cleanly terminate heartbeat task and the lanes
        heartbeat_handle.stop().await;
        if let Some(client) = self
            .blob_client
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            client.close();
        }
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
        if let Some(master_pub) = self.config.master_public_key.as_ref() {
            let node_key = self.config.node_key.as_ref().ok_or_else(|| {
                GridError::Config(
                    "master_public_key is set on WorkerConfig without node_key; pass both via WorkerConfig::with_v3_auth".into(),
                )
            })?;
            let session_key = tokio::time::timeout(
                self.config.handshake_timeout,
                v3_client_handshake(transport, node_key, master_pub, HandshakePurpose::Member),
            )
            .await
            .map_err(|_| GridError::Timeout {
                operation: "cluster_authentication".into(),
                duration_secs: self.config.handshake_timeout.as_secs(),
            })??;
            debug!(
                worker_id = %self.worker_id,
                member_id = %node_key.member_id(),
                "Master authenticated via OXS-AUTH v3 (Noise IK)"
            );
            transport.enable_encryption(&session_key, EndpointRole::Client);
        } else if let Some(secret) = self.config.cluster_secret.as_ref() {
            // Upgrade transport to AEAD record layer upon successful authentication.
            let session_key = tokio::time::timeout(
                self.config.handshake_timeout,
                client_handshake(transport, secret),
            )
            .await
            .map_err(|_| GridError::Timeout {
                operation: "cluster_authentication".into(),
                duration_secs: self.config.handshake_timeout.as_secs(),
            })??;
            debug!(worker_id = %self.worker_id, "Master proved knowledge of the cluster secret");
            transport.enable_encryption(&session_key, EndpointRole::Client);
        }

        // Whatever the previous connection's Master could do says nothing about this one.
        self.master_takes_mobile_telemetry
            .store(false, Ordering::Release);
        self.master_has_blobs.store(false, Ordering::Release);
        self.current_session_hold_secs.store(0, Ordering::Release);
        self.negotiated_features.store(0, Ordering::Release);
        let advertised = self.advertised_features();
        let mut capabilities = self.capabilities.clone();
        capabilities.mobile = self.mobile.snapshot();
        let register_msg = WorkerMessage::Register {
            worker_id: self.worker_id,
            capabilities,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: advertised,
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
                protocol_version,
                features,
            } => {
                if !accepted {
                    let reason =
                        message.unwrap_or_else(|| "Registration rejected by master".into());
                    warn!(worker_id = %self.worker_id, reason = %reason, "Master rejected registration");
                    return Err(GridError::RegistrationRejected(reason));
                }

                if !is_protocol_version_compatible(protocol_version) {
                    return Err(GridError::HandshakeFailed(
                        format_incompatible_version_error("Master", protocol_version),
                    ));
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

                let negotiated = features & advertised;
                self.negotiated_features
                    .store(negotiated.bits(), Ordering::Release);

                let takes_telemetry = negotiated.contains(ProtocolCapabilities::MOBILE_TELEMETRY)
                    && master_features::announced(
                        message.as_deref(),
                        master_features::MOBILE_TELEMETRY,
                    );
                self.master_takes_mobile_telemetry
                    .store(takes_telemetry, Ordering::Release);
                let has_blobs = negotiated.contains(ProtocolCapabilities::BLOB_TRANSFER)
                    && master_features::announced(message.as_deref(), master_features::BLOBS);
                self.master_has_blobs.store(has_blobs, Ordering::Release);

                let hold_secs = if negotiated.contains(ProtocolCapabilities::SESSION_RESUME) {
                    master_features::parse_session_resume_hold_secs(message.as_deref()).unwrap_or(0)
                } else {
                    0
                };
                self.current_session_hold_secs
                    .store(hold_secs, Ordering::Release);
                self.last_negotiated_hold_secs
                    .store(hold_secs, Ordering::Release);
                self.last_heartbeat_interval_ms
                    .store(interval.as_millis() as u64, Ordering::Release);

                if !takes_telemetry && self.mobile.live_status().is_some() {
                    info!(
                        worker_id = %self.worker_id,
                        "This Master does not announce live battery/thermal reports (older build?); sending them only at registration"
                    );
                }

                if hold_secs > 0 {
                    if self
                        .resume_deadline
                        .is_some_and(|d| tokio::time::Instant::now() >= d)
                    {
                        self.expire_resume_window("session-resume grace window expired")
                            .await;
                    }
                    let running = self.resumable_task_ids().await;
                    let resume_msg = WorkerMessage::ResumeSession {
                        worker_id: self.worker_id,
                        running,
                    };
                    transport.send_msg(&resume_msg).await.map_err(|e| {
                        GridError::HandshakeFailed(format!(
                            "Failed to send ResumeSession message: {e}"
                        ))
                    })?;
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

/// True when `addr` targets a loopback master (`127.x.x.x`, `[::1]`, or `localhost`).
pub(crate) fn is_loopback_master_addr(addr: &str) -> bool {
    let Some(host) = portal_host_of(addr.trim()) else {
        return false;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.to_canonical().is_loopback())
}

/// Returns the socket address the standby redirection portal should bind for `target`.
///
/// When the worker targets a loopback master, we bind `127.0.0.1:<portal_port>` rather than
/// `0.0.0.0:<portal_port>` (and rather than skipping the portal altogether):
/// - A visitor on another machine cannot reach a loopback master anyway (and
///   [`resolve_portal_master`] already refuses to serve a loopback redirect to non-loopback
///   clients), so binding all interfaces only triggers a Windows Firewall prompt ("allow public
///   and private networks?") when a worker or test connects to `127.0.0.1`.
/// - Binding `127.0.0.1` instead of skipping the portal preserves local `/api/status` probing,
///   local browser redirects on the same host, and tests that exercise the portal against a
///   loopback master.
async fn portal_bind_addr_for_target(
    portal_port: u16,
    target: &PortalTarget,
) -> std::net::SocketAddr {
    let is_loopback = match target {
        PortalTarget::Static(addr) => is_loopback_master_addr(addr),
        PortalTarget::Dynamic(beacon_state) => beacon_state
            .read()
            .await
            .as_ref()
            .is_some_and(|beacon| is_loopback_master_addr(&beacon.cluster_addr)),
    };
    let bind_ip = if is_loopback {
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    } else {
        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
    };
    std::net::SocketAddr::new(bind_ip, portal_port)
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
            format!(
                "http://{}:{}",
                url_host(&cluster_host),
                PORTAL_DASHBOARD_PORT
            )
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
<html lang="en" data-theme="dark">
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
    <div class="desc">This device is operating as a <strong>Worker</strong>. Automatically discovering an active Master on the LAN...</div>
    <button class="btn" onclick="probeCluster()">Retry Connection Now</button>
    <div class="status" id="scanStatus">Scanning cluster...</div>
  </div>
  <script>
    // Asks this worker (same origin) which Master it is connected to; the answer is validated
    // server-side, and checked again here before the browser is sent anywhere.
    const safeOrigin = /^https?:\/\/[A-Za-z0-9._\-]+(:[0-9]{1,5})?$|^https?:\/\/\[[0-9A-Fa-f:.]+\](:[0-9]{1,5})?$/;
    async function probeCluster() {
      const s = document.getElementById('scanStatus');
      if (s) s.textContent = 'Checking cluster nodes...';
      try {
        const res = await fetch('/api/status', { cache: 'no-store' });
        if (res.ok) {
          const data = await res.json();
          const url = data && data.redirect_url;
          if (typeof url === 'string' && safeOrigin.test(url)) {
            if (s) s.textContent = 'Master found at ' + url + '! Redirecting...';
            window.location.replace(url);
            return;
          }
        }
      } catch (_) {}
      if (s) s.textContent = 'No Master detected yet. Retrying in 2 seconds...';
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
fn portal_http_response(
    status: &str,
    content_type: &str,
    extra_headers: &str,
    body: &str,
) -> String {
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
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><meta http-equiv=\"refresh\" content=\"0; url={0}\"><title>OxideSwarm Auto-Redirect</title></head><body style=\"font-family:-apple-system,BlinkMacSystemFont,sans-serif;text-align:center;padding:50px;background:#090d16;color:#e2e8f0;\"><h2>OxideSwarm Node (Worker)</h2><p>This device is operating as a Worker. Redirecting to Master at <a style=\"color:#38bdf8;\" href=\"{0}\">{0}</a>...</p><script>window.location.replace(\"{0}\");</script></body></html>",
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
            let complete =
                head.windows(4).any(|w| w == b"\r\n\r\n") || head.windows(2).any(|w| w == b"\n\n");
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
            Some((web_ui_url, cluster_addr)) => {
                portal_redirect_response(&web_ui_url, &cluster_addr)
            }
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
    let target = target.into();
    let bind_addr = portal_bind_addr_for_target(portal_port, &target).await;
    let listener = match tokio::net::TcpListener::bind(bind_addr).await {
        Ok(l) => l,
        Err(e) => {
            debug!(
                bind_addr = %bind_addr,
                port = portal_port,
                error = %e,
                "Worker standby redirection portal not started (port 8080 likely bound by Master or other service)"
            );
            return;
        }
    };

    info!(
        bind_addr = %bind_addr,
        portal_port = portal_port,
        "Worker standby HTTP redirection portal active"
    );

    run_portal(
        listener,
        target,
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
mod tests;
