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
//! * A Master that vanishes without a FIN or RST is noticed by [`crate::heartbeat::IoClock`]: no byte received for
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
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

use rusty_grid_core::capabilities::{
    detect_live_mobile_status, suppress_windows_error_dialogs, WorkerCapabilities,
};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::noise_auth::{NodeKeypair, PublicKey};
use rusty_grid_core::protocol::{ProtocolCapabilities, WireCodec, WorkerMessage};
use rusty_grid_core::task::TaskId;

use crate::backoff::{BackoffConfig, ExponentialBackoff};
use crate::blob_cache::DEFAULT_BLOB_CACHE_MAX_BYTES;
use crate::blob_client::BlobClient;
use crate::gpu::{apply_gpu_policy, probe_real_gpu, GpuNotice, GpuRequest, NoticeLevel};
use crate::heartbeat::{spawn_mobile_poller, HeartbeatTracker, MobileTelemetryHandle};
use crate::identity::{default_worker_id_path, load_or_create_worker_id};
use crate::isolation::IsolationLevel;
use crate::process_tree::ProcessPriority;
use crate::runner::{RunnerConfig, TaskRunner, DEFAULT_MAX_OUTPUT_BYTES};
use crate::sandbox::SandboxConfig;
use crate::staging::{OutputLimits, DEFAULT_OUTPUT_LIMIT_BYTES};
use crate::workspace::{WorkspaceLimits, DEFAULT_MAX_WORKSPACES, DEFAULT_WORKSPACE_LIMIT_MB};

mod portal;
mod session;
mod tasks;

#[allow(unused_imports)]
pub(crate) use portal::is_loopback_master_addr;
use portal::{portal_host_of, url_host};
pub use portal::{
    spawn_worker_redirection_portal, spawn_worker_redirection_portal_with_secret, PortalTarget,
};
pub use session::WorkerStreamMode;
pub use tasks::{compute_effective_resume_grace, MAX_RESUME_OUTBOX_BYTES, RESUME_SAFETY_MARGIN};

#[cfg(test)]
#[allow(unused_imports)]
pub(in crate::client) use {
    crate::runner::{EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR},
    portal::*,
    rusty_grid_core::capabilities::MobileLiveStatus,
    rusty_grid_core::protocol::{
        master_features, MasterMessage, MessageReader, MessageTransport, MessageWriter,
        PROTOCOL_VERSION_CURRENT,
    },
    rusty_grid_core::task::{Task, TaskResult, TaskStatus},
    session::*,
    std::net::IpAddr,
    tasks::*,
    tokio::net::TcpStream,
};

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

/// How often the session loop compares the shared mobile telemetry with what the Master last heard.
pub const MOBILE_TELEMETRY_CHECK_INTERVAL: Duration = Duration::from_secs(2);

/// How often a worker that reads its own battery / thermal state (no platform app feeding it)
/// re-reads it.
pub const MOBILE_POLL_INTERVAL: Duration = Duration::from_secs(30);

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
    /// Additional read-only directories accessible to isolated tasks (`--isolation-read-dir`,
    /// `RUSTY_GRID_ISOLATION_READ_DIRS`, `[worker].isolation_read_dirs`; design 0074).
    pub isolation_read_dirs: Vec<PathBuf>,
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
    /// Optional URL of rendezvous registry for cross-subnet discovery (`--rendezvous` /
    /// `OXIDESWARM_RENDEZVOUS`).
    pub rendezvous_url: Option<String>,
    /// Optional in-process synthetic task runner (`SCALE-1`).
    pub fake_runner: Option<crate::sim::FakeRunnerConfig>,
    /// Optional simulated link shaping and byte/disconnect instrumentation (`SCALE-1`).
    pub simulated_link: Option<crate::sim::SimulatedLinkConfig>,
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
            isolation_read_dirs: Vec::new(),
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
            rendezvous_url: None,
            fake_runner: None,
            simulated_link: None,
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

    /// Sets or clears the optional HTTP rendezvous registry URL (`http://...`).
    pub fn with_rendezvous_url(mut self, url: Option<String>) -> Self {
        self.rendezvous_url = url;
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

    pub fn with_isolation_read_dirs(mut self, dirs: Vec<PathBuf>) -> Self {
        self.isolation_read_dirs = dirs;
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

    /// Configures an in-process synthetic runner for simulated worker fleets (`SCALE-1`).
    pub fn with_fake_runner(mut self, fake: crate::sim::FakeRunnerConfig) -> Self {
        self.fake_runner = Some(fake);
        self
    }

    /// Configures simulated link shaping and instrumentation for the worker connection (`SCALE-1`).
    pub fn with_simulated_link(mut self, link: crate::sim::SimulatedLinkConfig) -> Self {
        self.simulated_link = Some(link);
        self
    }

    /// Builds a baseline simulated worker config with host hardware probes and portals disabled.
    pub fn simulated(
        master_addr: impl Into<String>,
        name: Option<String>,
        cores: Option<usize>,
        fake_runner: crate::sim::FakeRunnerConfig,
        simulated_link: Option<crate::sim::SimulatedLinkConfig>,
    ) -> Self {
        let mut config = Self::new(master_addr)
            .with_name(name.unwrap_or_else(|| "sim-worker".to_string()))
            .with_cores(cores.unwrap_or(4))
            .with_ram_mb(4096)
            .with_no_battery(true)
            .with_no_gpu(true)
            .with_no_blender(true)
            .with_no_cuda(true)
            .with_fake_runner(fake_runner);
        config.enable_redirection_portal = false;
        if let Some(link) = simulated_link {
            config = config.with_simulated_link(link);
        }
        config
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
    pending_resume_outbox: Arc<std::sync::Mutex<tasks::PendingResumeOutbox>>,
    #[cfg(feature = "p2p")]
    p2p_endpoint: Arc<tokio::sync::Mutex<Option<iroh::Endpoint>>>,
    #[cfg(feature = "p2p")]
    p2p_conn: Arc<tokio::sync::RwLock<Option<iroh::endpoint::Connection>>>,
    /// Consecutive failed P2P dials (reset by a successful one).
    #[cfg(feature = "p2p")]
    p2p_failures: AtomicU32,
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
        let host_dirs = crate::isolation::HostDirs::detect()
            .with_extra_read_only_dirs(config.isolation_read_dirs.clone());
        runner_cfg = runner_cfg
            .with_isolation(config.isolation)
            .with_host_dirs(host_dirs)
            .with_protected_files(config.protected_files.clone())
            .with_workspaces(config.workspaces)
            .with_workspace_root(config.workspace_root.clone())
            .with_workspace_limits(WorkspaceLimits {
                limit_mb: config.workspace_limit_mb,
                max_workspaces: config.max_workspaces,
                ..WorkspaceLimits::default()
            })
            .with_fake_runner(config.fake_runner.clone());
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
                tasks::PendingResumeOutbox::with_capacity(2 * concurrency_permits.max(1)),
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

    /// Constructs an in-process simulated `WorkerClient` with host probes and sandboxes bypassed
    /// for high-fanout load testing (`SCALE-1`).
    pub fn simulated(
        master_addr: impl Into<String>,
        name: Option<String>,
        cores: Option<usize>,
        fake_runner: crate::sim::FakeRunnerConfig,
        simulated_link: Option<crate::sim::SimulatedLinkConfig>,
    ) -> Self {
        Self::new(WorkerConfig::simulated(
            master_addr,
            name,
            cores,
            fake_runner,
            simulated_link,
        ))
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

    /// Validates any `--isolation-read-dir` paths against the worker's host, sandbox, workspace,
    /// and protected secret files (`design 0074`).
    pub fn validate_isolation_config(&self) -> GridResult<()> {
        crate::isolation::validate_extra_read_only_dirs(
            &self.runner.config().host_dirs.extra_read_only_dirs,
            &self.runner.config().host_dirs,
            Some(&self.runner.config().sandbox_config.base_dir),
            self.config.workspace_root.as_deref(),
            &self.runner.config().protected_files,
        )
        .map_err(GridError::Config)
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
        if self.runner.config().fake_runner.is_none() || self.config.sandbox_base_dir.is_some() {
            if let Err(e) = self.runner.config().sandbox_config.ensure_usable().await {
                let err = GridError::Config(e.to_string());
                error!(worker_id = %self.worker_id, error = %err, "Task sandboxes cannot be created");
                return Err(err);
            }
        }

        // Validate explicit --isolation-read-dir allowlist paths before probing or accepting tasks.
        self.validate_isolation_config()?;

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
}

#[cfg(test)]
mod tests;
