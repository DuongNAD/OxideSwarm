//! Master TCP server listening for worker connections, conducting registration handshakes,
//! managing ephemeral port file publication, dispatching bidirectional framed messaging,
//! and orchestrating task lifecycle and failover.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::AbortHandle;
use tracing::{debug, error, info, warn};
#[cfg(feature = "p2p")]
use uuid::Uuid;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::mapreduce::{MapReduceJobSpec, MapReduceResult};
use rusty_grid_core::noise_auth::{master_noise_key_path, NodeKeypair, PublicKey};
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskResult, TaskStatus};
#[cfg(feature = "p2p")]
use rusty_grid_core::transport::{
    BiStream, GridStream, GRID_ALPN, STREAM_CLIENT, STREAM_CONTROL, STREAM_DATA,
};
use rusty_grid_core::{GridError, GridResult, WireCodec};

#[path = "members.rs"]
pub mod members;
pub use members::{
    AuthMode, MemberEntry, MemberRole, MemberStore, MembersFile, MEMBERS_FILE_NAME,
    MEMBERS_FILE_VERSION,
};

mod client_and_members;
mod handshakes;
mod tasks_and_blobs;

#[cfg(test)]
mod auth_and_member_tests;
#[cfg(test)]
mod persistence_tests;
#[cfg(test)]
mod tests;

pub(crate) use client_and_members::handle_client_connection;
#[cfg(feature = "dashboard")]
pub(crate) use client_and_members::revoke_member_with_teardown;
#[cfg(feature = "dashboard")]
pub(crate) use client_and_members::run_member_store_blocking;
use client_and_members::{
    run_member_store_blocking as run_member_store_blocking_inner, spawn_member_store_resync,
    ENROLL_CONFIRM_TIMEOUT_SECS,
};
#[cfg(feature = "p2p")]
use handshakes::handle_multiplexed_connection;
#[cfg(feature = "p2p")]
pub(crate) use handshakes::P2pConnGuard;
pub(crate) use handshakes::{
    authenticate_and_read_first_message, drain_refusal_stream, reject_master_role_claim,
    send_refusal_and_drain, AbortOnDrop, MasterAuthContext,
};
use handshakes::{handle_connection, spawn_master_rendezvous_registration};
#[cfg(feature = "dashboard")]
pub(crate) use tasks_and_blobs::DashboardTx;
pub(crate) use tasks_and_blobs::{
    admit_blob_task, apply_mobile_telemetry, cancel_task_and_notify, failover_superseded_session,
    feature_announcement, handle_oversized_master_message, handle_session_task_message,
    handle_workers_revoked_with_broadcast, prune_closed_waiters, resolve_terminal_waiters,
    serve_worker_blob_request, BlobService,
};
pub use tasks_and_blobs::{
    handle_task_progress, handle_task_result, handle_worker_disconnect,
    handle_worker_disconnect_with_broadcast, handle_worker_disconnect_with_waiters,
    handle_worker_evacuate_with_broadcast,
};
#[cfg(feature = "dashboard")]
pub use tasks_and_blobs::{handle_task_progress_with_broadcast, handle_task_result_with_broadcast};
use tasks_and_blobs::{spawn_blob_store_sweeper, terminal_result};

use crate::auth::DevBypass;
use crate::blob_store::{BlobStore, BlobStoreConfig};
use crate::mapreduce::{MapReduceEngine, DEFAULT_MAPREDUCE_PARALLELISM};
use crate::queue::{
    QueueStats, TaskInfo, TaskQueue, DEFAULT_MAX_ACTIVE_BYTES, DEFAULT_MAX_TERMINAL_BYTES,
    DEFAULT_MAX_TERMINAL_TASKS,
};
#[cfg(not(feature = "dashboard"))]
use crate::reaper::spawn_reaper_with_queue;
use crate::reaper::ReaperConfig;
use crate::registry::{WorkerInfo, WorkerRegistry};
use crate::scheduler::{SchedulerConfig, WorkloadScheduler};

/// Type alias for client waiters awaiting task completion via oneshot channels.
pub type WaiterMap = Arc<RwLock<HashMap<TaskId, Vec<oneshot::Sender<TaskResult>>>>>;

/// Default cap on simultaneously open connections on the grid port (TCP and QUIC together).
pub const DEFAULT_MAX_CONNECTIONS: usize = 1024;

/// Default cap on connections that have been accepted but have not yet authenticated and sent
/// their first message. They hold a socket for up to the handshake timeout without proving
/// anything, so they get a much smaller budget than established connections.
pub const DEFAULT_MAX_PENDING_HANDSHAKES: usize = 64;

/// Default session-resume hold duration in seconds (`master --session-resume-hold-secs`, default: 30).
pub const DEFAULT_SESSION_RESUME_HOLD_SECS: u64 = 30;

/// Default session-resume hold duration (`Duration::from_secs(30)`).
pub const DEFAULT_SESSION_RESUME_HOLD: Duration =
    Duration::from_secs(DEFAULT_SESSION_RESUME_HOLD_SECS);

/// Default maximum tasks assigned per scheduling pass (`master --max-batch-size`, default: 1024).
pub const DEFAULT_MASTER_MAX_BATCH_SIZE: usize = 1024;

/// Configuration options for initializing and running a `MasterServer`.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Socket address to bind to (e.g. "127.0.0.1:0" for dynamic port, or "0.0.0.0:8080").
    pub bind_addr: SocketAddr,
    /// Optional filesystem path to write the dynamic bound port for zero-collision testing.
    pub port_file: Option<PathBuf>,
    /// Heartbeat interval in seconds advertised to workers in `RegisterAck` (default: 3).
    pub heartbeat_interval_secs: u64,
    /// Handshake negotiation timeout in seconds for Slowloris defense (default: 5).
    pub handshake_timeout_secs: u64,
    /// Whether to enable P2P NAT traversal listener (iroh).
    pub enable_p2p: bool,
    /// When `true`, builds the P2P endpoint with `iroh::endpoint::presets::Minimal` (no n0 DERP
    /// relay or pkarr/DNS address lookup) so tests stay completely off the network. Production
    /// defaults to `false` (`presets::N0`); the CLI resolves `RUSTY_GRID_P2P_MINIMAL`.
    pub p2p_minimal_preset: bool,
    /// Optional custom iroh relay URL (`https://...`). When set, builds the P2P endpoint with
    /// `iroh::RelayMode::Custom(iroh::RelayMap::from(url))` instead of `presets::N0`.
    pub p2p_relay_url: Option<String>,
    /// Optional explicit UDP socket address for the P2P endpoint (used by tests to keep a stable
    /// loopback UDP port across restarts when relays are disabled).
    pub p2p_bind_addr: Option<SocketAddr>,
    /// Optional path to write P2P ticket for worker connections.
    pub p2p_ticket_file: Option<PathBuf>,
    /// Address to bind the Web UI dashboard to (e.g. "0.0.0.0:8080").
    pub web_ui_addr: SocketAddr,
    /// Whether to enable the Web UI dashboard service (default: false for ephemeral/test configs).
    pub enable_web_ui: bool,
    /// Whether to enable UDP Master node discovery service (default: true for non-zero ports).
    pub enable_discovery: bool,
    /// UDP discovery port (default: 8089).
    pub discovery_port: u16,
    /// Optional path to load or persist the P2P secret key for stable identity across restarts.
    pub p2p_key_file: Option<PathBuf>,
    /// Wire serialization codec (Bincode or Json, default: Bincode).
    pub wire_codec: WireCodec,
    /// Maximum execution retries for failed or disconnected tasks (default: 3).
    pub max_retries: u32,
    /// Maximum infrastructure failovers per task: re-dispatches caused by a worker that dropped,
    /// timed out, re-registered or refused the task, counted apart from `max_retries`
    /// (default: [`crate::queue::DEFAULT_MAX_FAILOVERS`]). `max_retries = 0` still disables re-runs entirely.
    pub max_failovers: u32,
    /// Maximum uncharged voluntary worker drains (`WorkerMessage::Disconnecting`) per task
    /// (default: [`crate::queue::DEFAULT_MAX_EVACUATIONS`]). Past this cap, drains take the charged
    /// failover path.
    pub max_evacuations: u32,
    /// Optional TCP port for the embedded web observability dashboard (0 for ephemeral).
    pub dashboard_port: Option<u16>,
    /// Optional bind IP for the dashboard HTTP listener.
    pub dashboard_bind_ip: Option<std::net::IpAddr>,
    /// Optional filesystem path to write the dynamic bound dashboard port for zero-collision testing.
    pub dashboard_port_file: Option<PathBuf>,
    /// Whether to force ephemeral in-memory secret key generation instead of default persistence.
    pub ephemeral_key: bool,
    /// Authentication and RBAC configuration for web observability and REST endpoints.
    pub auth_config: crate::auth::AuthConfig,
    /// Shared cluster secret. When set, every TCP and QUIC peer must complete the OXS-AUTH
    /// handshake (`rusty_grid_core::cluster_auth`) before it can register as a worker or submit
    /// anything, the discovery beacon is signed, and the derived dashboard Admin token
    /// ([`crate::auth::dashboard_admin_token`]) is accepted. `None` (the default) leaves the
    /// grid port open, which is only appropriate for loopback-only masters and tests.
    pub cluster_secret: Option<ClusterSecret>,
    /// Maximum simultaneously open connections on the grid port, TCP and QUIC together; further
    /// peers are closed straight away (default: [`DEFAULT_MAX_CONNECTIONS`]).
    pub max_connections: usize,
    /// Maximum connections that are still in their pre-authentication handshake; further peers are
    /// closed straight away (default: [`DEFAULT_MAX_PENDING_HANDSHAKES`]).
    pub max_pending_handshakes: usize,
    /// Finished tasks (with their payloads and output) the queue retains, oldest evicted first
    /// (default: [`DEFAULT_MAX_TERMINAL_TASKS`]).
    pub max_terminal_tasks: usize,
    /// Byte budget for the retained finished tasks, oldest evicted first
    /// (default: [`DEFAULT_MAX_TERMINAL_BYTES`], 256 MiB).
    pub max_terminal_bytes: usize,
    /// Byte cap for active (non-terminal) tasks in the queue
    /// (default: [`DEFAULT_MAX_ACTIVE_BYTES`], 256 MiB).
    pub max_active_bytes: usize,
    /// Optional task-count capacity for active (non-terminal) tasks in the queue (`None` = unbounded by count).
    pub max_queue_size: Option<usize>,
    /// Tasks of one Map/Reduce job submitted at once (default: [`DEFAULT_MAPREDUCE_PARALLELISM`]);
    /// keep it above the cluster's total slots so every device works.
    pub mapreduce_parallelism: usize,
    /// Enables the loopback dev bypass automatically for a master that has neither a cluster
    /// secret nor any dashboard token, when its dashboard listener is bound to a loopback address,
    /// so a bare local master still has a usable dashboard (default: false; the CLI turns it on).
    /// See [`crate::auth::resolve_dashboard_auth`].
    pub implicit_local_dashboard: bool,
    /// Optional URL of rendezvous registry for cross-subnet discovery (or set OXIDESWARM_RENDEZVOUS).
    pub rendezvous_url: Option<String>,
    /// Where the blob store of task inputs and output files lives and how large it may grow (see
    /// [`crate::blob_store`]). `None` (the default) runs without one unless `state_dir` is set, in
    /// which case it defaults to `<state_dir>/blobs`. The CLI keeps one under `~/.oxideswarm/blobs`.
    pub blob_store: Option<BlobStoreConfig>,
    /// Optional directory for durable task queue persistence (`store.lock`, `snapshot.bin`,
    /// `journal.bin`; see [`crate::store::TaskStore`]). `None` (the default) keeps the queue
    /// purely in memory.
    pub state_dir: Option<PathBuf>,
    /// How long the master holds tasks of a `SESSION_RESUME`-capable worker on disconnect or
    /// restart before releasing or charging failover (`Duration::ZERO` disables session resume;
    /// defaults to 30 s when `state_dir` is set or in the CLI).
    pub session_resume_hold: Duration,
    /// True when `with_session_resume_hold` was called explicitly so `with_state_dir` does not
    /// overwrite an explicit `Duration::ZERO`.
    session_resume_hold_explicit: bool,
    /// Master authentication mode (`--auth secret|members|both`, default: [`AuthMode::Both`]).
    pub auth_mode: AuthMode,
    /// Static X25519 key pair for OXS-AUTH v3 (`master_noise.key`).
    pub master_noise_key: Option<NodeKeypair>,
    /// Optional path where `master_noise.key` is persisted before `CreateInvite` hands out the
    /// master's v3 public key (when not already saved at startup).
    pub master_noise_key_path: Option<PathBuf>,
    /// Optional path to the local host's `node.key` to auto-enrol as `local-host` (`admin`) when
    /// `CreateInvite` first persists `master_noise.key`.
    pub local_node_key_path: Option<PathBuf>,
    /// Enrolled member list (`members.json`).
    pub members: Arc<MemberStore>,
    /// True when `with_members` was called explicitly so `bind_full` does not overwrite it from `state_dir`.
    members_explicit: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let enable_web_ui = if std::env::var("RUSTY_GRID_DISABLE_WEB_UI").is_ok() {
            false
        } else {
            std::env::var("RUSTY_GRID_ENABLE_WEB_UI")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
        };
        Self {
            bind_addr: "127.0.0.1:0".parse().expect("valid loopback default"),
            port_file: None,
            heartbeat_interval_secs: 3,
            handshake_timeout_secs: 5,
            enable_p2p: false,
            p2p_minimal_preset: false,
            p2p_relay_url: None,
            p2p_bind_addr: None,
            p2p_ticket_file: None,
            // The dashboard is only reachable from other machines when its address is set
            // explicitly (`with_web_ui_addr` / `--web-ui-addr`).
            web_ui_addr: "127.0.0.1:8080".parse().expect("valid loopback default"),
            enable_web_ui,
            enable_discovery: false,
            discovery_port: rusty_grid_core::DEFAULT_DISCOVERY_PORT,
            p2p_key_file: None,
            wire_codec: WireCodec::default(),
            max_retries: 3,
            max_failovers: crate::queue::DEFAULT_MAX_FAILOVERS,
            max_evacuations: crate::queue::DEFAULT_MAX_EVACUATIONS,
            dashboard_port: None,
            dashboard_bind_ip: None,
            dashboard_port_file: None,
            ephemeral_key: false,
            auth_config: crate::auth::AuthConfig::default(),
            cluster_secret: None,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_pending_handshakes: DEFAULT_MAX_PENDING_HANDSHAKES,
            max_terminal_tasks: DEFAULT_MAX_TERMINAL_TASKS,
            max_terminal_bytes: DEFAULT_MAX_TERMINAL_BYTES,
            max_active_bytes: DEFAULT_MAX_ACTIVE_BYTES,
            max_queue_size: None,
            mapreduce_parallelism: DEFAULT_MAPREDUCE_PARALLELISM,
            implicit_local_dashboard: false,
            rendezvous_url: None,
            blob_store: None,
            state_dir: None,
            session_resume_hold: Duration::ZERO,
            session_resume_hold_explicit: false,
            auth_mode: AuthMode::default(),
            master_noise_key: None,
            master_noise_key_path: None,
            local_node_key_path: None,
            members: Arc::new(MemberStore::new()),
            members_explicit: false,
        }
    }
}

impl ServerConfig {
    /// Creates a new ServerConfig binding to the given socket address.
    pub fn new(bind_addr: SocketAddr) -> Self {
        let enable_discovery = if std::env::var("RUSTY_GRID_DISABLE_DISCOVERY").is_ok() {
            false
        } else {
            bind_addr.port() != 0
        };
        let mut cfg = Self {
            bind_addr,
            enable_discovery,
            ..Default::default()
        };
        if bind_addr.port() != 0 && std::env::var("RUSTY_GRID_DISABLE_WEB_UI").is_err() {
            cfg.enable_web_ui = true;
            if cfg.web_ui_addr.port() == bind_addr.port() {
                cfg.web_ui_addr.set_port(bind_addr.port() + 1);
            }
        }
        cfg
    }

    /// Parses a socket address string (e.g. "127.0.0.1:0") into a ServerConfig.
    pub fn from_addr(addr: &str) -> GridResult<Self> {
        let bind_addr: SocketAddr = addr.parse().map_err(|e| {
            GridError::Config(format!("Failed to parse server bind address '{addr}': {e}"))
        })?;
        Ok(Self::new(bind_addr))
    }

    /// Configures the ephemeral port file output path.
    pub fn with_port_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.port_file = Some(path.into());
        self
    }

    /// Sets the advertised heartbeat interval in seconds.
    pub fn with_heartbeat_interval(mut self, secs: u64) -> Self {
        self.heartbeat_interval_secs = secs;
        self
    }

    /// Sets the handshake watchdog timeout in seconds.
    pub fn with_handshake_timeout(mut self, secs: u64) -> Self {
        self.handshake_timeout_secs = secs;
        self
    }

    /// Enables or disables P2P listener mode.
    pub fn with_p2p(mut self, enable: bool) -> Self {
        self.enable_p2p = enable;
        self
    }

    /// Selects `iroh::endpoint::presets::Minimal` (no relay or external address lookup) for the
    /// P2P endpoint when `minimal` is `true`, keeping tests off the network while production
    /// keeps `presets::N0` (`false`).
    pub fn with_p2p_minimal_preset(mut self, minimal: bool) -> Self {
        self.p2p_minimal_preset = minimal;
        self
    }

    /// Configures a custom iroh relay URL (`https://...`) so the P2P endpoint uses
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

    /// Binds the P2P QUIC endpoint to a specific UDP socket address (clearing default `0.0.0.0:0`
    /// and `[::]:0` transports).
    pub fn with_p2p_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.p2p_bind_addr = Some(addr);
        self
    }

    /// Configures path where P2P ticket string is published.
    pub fn with_p2p_ticket_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.p2p_ticket_file = Some(path.into());
        self
    }

    /// Enables or disables the Web UI dashboard service.
    pub fn with_web_ui(mut self, enable: bool) -> Self {
        self.enable_web_ui = enable;
        self
    }

    /// Sets the socket address to bind the Web UI dashboard to (e.g. "127.0.0.1:0" for ephemeral binding).
    pub fn with_web_ui_addr(mut self, addr: SocketAddr) -> Self {
        self.web_ui_addr = addr;
        self
    }

    /// Enables or disables UDP Master discovery beacons.
    pub fn with_discovery(mut self, enable: bool) -> Self {
        self.enable_discovery = enable;
        self
    }

    /// Sets the UDP port for discovery beacons and probes.
    pub fn with_discovery_port(mut self, port: u16) -> Self {
        self.discovery_port = port;
        self
    }

    /// Configures path to load or persist the P2P secret key.
    pub fn with_p2p_key_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.p2p_key_file = Some(path.into());
        self
    }

    /// Sets the wire serialization codec (Bincode or Json).
    pub fn with_wire_codec(mut self, codec: WireCodec) -> Self {
        self.wire_codec = codec;
        self
    }

    /// Sets maximum execution retries for failed tasks.
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Sets the per-task cap on infrastructure failovers (see `ServerConfig::max_failovers`).
    pub fn with_max_failovers(mut self, failovers: u32) -> Self {
        self.max_failovers = failovers;
        self
    }

    /// Sets the per-task cap on uncharged voluntary worker drains (`WorkerMessage::Disconnecting`).
    pub fn with_max_evacuations(mut self, evacuations: u32) -> Self {
        self.max_evacuations = evacuations;
        self
    }

    /// Configures the dashboard port. If `0`, an ephemeral port is selected.
    pub fn with_dashboard_port(mut self, port: u16) -> Self {
        self.dashboard_port = Some(port);
        self
    }

    /// Configures the dashboard bind IP address.
    pub fn with_dashboard_bind_ip(mut self, ip: std::net::IpAddr) -> Self {
        self.dashboard_bind_ip = Some(ip);
        self
    }

    /// Configures the ephemeral dashboard port file output path.
    pub fn with_dashboard_port_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.dashboard_port_file = Some(path.into());
        self
    }

    /// Disables default secret key persistence to ~/.oxideswarm/master_key.bin when P2P is enabled.
    pub fn with_ephemeral_key(mut self, ephemeral: bool) -> Self {
        self.ephemeral_key = ephemeral;
        self
    }

    /// Configures the auth config for web UI and REST endpoints.
    pub fn with_auth_config(mut self, auth_config: crate::auth::AuthConfig) -> Self {
        self.auth_config = auth_config;
        self
    }

    /// Sets development mode on auth config.
    pub fn with_dev_mode(mut self, dev_mode: bool) -> Self {
        self.auth_config.dev_mode = dev_mode;
        self
    }

    /// Requires every peer of the grid port (TCP and QUIC) to prove knowledge of `secret`.
    pub fn with_cluster_secret(mut self, secret: ClusterSecret) -> Self {
        self.cluster_secret = Some(secret);
        self
    }

    /// Caps simultaneously open connections on the grid port (at least 1).
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max.max(1);
        self
    }

    /// Caps connections that have not finished their pre-authentication handshake (at least 1).
    pub fn with_max_pending_handshakes(mut self, max: usize) -> Self {
        self.max_pending_handshakes = max.max(1);
        self
    }

    /// Sets how many finished tasks the queue retains.
    pub fn with_max_terminal_tasks(mut self, max: usize) -> Self {
        self.max_terminal_tasks = max;
        self
    }

    /// Sets the byte budget of retained finished tasks.
    pub fn with_max_terminal_bytes(mut self, max: usize) -> Self {
        self.max_terminal_bytes = max;
        self
    }

    /// Sets the byte cap of active (non-terminal) tasks in the queue.
    pub fn with_max_active_bytes(mut self, max: usize) -> Self {
        self.max_active_bytes = max;
        self
    }

    /// Sets the task-count cap of active (non-terminal) tasks in the queue (`None` = unbounded by count).
    pub fn with_max_queue_size(mut self, max: Option<usize>) -> Self {
        self.max_queue_size = max;
        self
    }

    /// Sets how many tasks of one Map/Reduce job may be in flight at once (at least 1).
    pub fn with_mapreduce_parallelism(mut self, parallelism: usize) -> Self {
        self.mapreduce_parallelism = parallelism.max(1);
        self
    }

    /// Enables the automatic loopback dashboard access of a bare local master (see
    /// [`ServerConfig::implicit_local_dashboard`]).
    pub fn with_implicit_local_dashboard(mut self, enable: bool) -> Self {
        self.implicit_local_dashboard = enable;
        self
    }

    /// Sets the rendezvous registry URL for cross-subnet discovery (or set OXIDESWARM_RENDEZVOUS).
    pub fn with_rendezvous_url(mut self, url: impl Into<String>) -> Self {
        self.rendezvous_url = Some(url.into());
        self
    }

    /// Keeps task inputs and output files in a blob store at `dir` that holds at most `max_bytes`.
    pub fn with_blob_store(mut self, dir: impl Into<PathBuf>, max_bytes: u64) -> Self {
        self.blob_store = Some(BlobStoreConfig::new(dir, max_bytes));
        self
    }

    /// Configures the durable task queue state directory (`store.lock`, `snapshot.bin`,
    /// `journal.bin`). When `blob_store` is not configured explicitly, it defaults to
    /// `<dir>/blobs`, and when `session_resume_hold` was not set explicitly, it defaults to
    /// [`DEFAULT_SESSION_RESUME_HOLD`] (30 s).
    pub fn with_state_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.state_dir = Some(dir.into());
        if !self.session_resume_hold_explicit {
            self.session_resume_hold = DEFAULT_SESSION_RESUME_HOLD;
        }
        self
    }

    /// Configures the session-resume hold duration advertised in `RegisterAck` and applied to
    /// disconnected or restarted workers (`Duration::ZERO` disables session resume; clamped to
    /// [`rusty_grid_core::protocol::master_features::MAX_SESSION_RESUME_HOLD_SECS`]).
    pub fn with_session_resume_hold(mut self, hold: Duration) -> Self {
        let max_secs = rusty_grid_core::protocol::master_features::MAX_SESSION_RESUME_HOLD_SECS;
        let max_hold = Duration::from_secs(max_secs);
        if hold > max_hold {
            warn!(
                requested_secs = hold.as_secs(),
                max_secs = max_secs,
                "Requested session-resume hold exceeds maximum; clamping to {max_secs}s"
            );
        }
        self.session_resume_hold = if hold.is_zero() {
            Duration::ZERO
        } else {
            hold.min(max_hold)
        };
        self.session_resume_hold_explicit = true;
        self
    }

    /// Sets the master authentication mode (`secret`, `members`, or `both`).
    pub fn with_auth_mode(mut self, mode: AuthMode) -> Self {
        self.auth_mode = mode;
        self.members.set_auth_mode(mode);
        self
    }

    /// Sets the master's static X25519 key pair for OXS-AUTH v3 handshakes.
    pub fn with_master_noise_key(mut self, key: NodeKeypair) -> Self {
        self.master_noise_key = Some(key);
        self
    }

    /// Sets the path where `master_noise.key` is persisted on demand (e.g. before `CreateInvite`).
    pub fn with_master_noise_key_path(mut self, path: Option<PathBuf>) -> Self {
        self.master_noise_key_path = path;
        self
    }

    /// Sets the local host's `node.key` path to auto-enrol as `local-host` (`admin`) when OXS-AUTH
    /// v3 persistence is triggered on demand.
    pub fn with_local_node_key_path(mut self, path: Option<PathBuf>) -> Self {
        self.local_node_key_path = path;
        self
    }

    /// Sets the enrolled member store for OXS-AUTH v3 handshakes.
    pub fn with_members(mut self, members: MemberStore) -> Self {
        members.set_auth_mode(self.auth_mode);
        self.members = Arc::new(members);
        self.members_explicit = true;
        self
    }
}

/// Atomically writes the bound port number to `path` using a sibling temporary file.
pub async fn write_port_file(path: &Path, port: u16) -> GridResult<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| GridError::PortFile {
                    path: path.display().to_string(),
                    reason: format!("Failed to create parent directory: {e}"),
                })?;
        }
    }

    let tmp_path = path.with_extension(format!("tmp.{}", std::process::id()));
    let payload = format!("{}\n", port);

    tokio::fs::write(&tmp_path, payload.as_bytes())
        .await
        .map_err(|e| GridError::PortFile {
            path: tmp_path.display().to_string(),
            reason: format!("Failed to write temporary port file: {e}"),
        })?;

    tokio::fs::rename(&tmp_path, path)
        .await
        .map_err(|e| GridError::PortFile {
            path: path.display().to_string(),
            reason: format!("Failed to rename temporary port file to target: {e}"),
        })?;

    Ok(())
}

/// Removes the port file if it exists.
pub async fn remove_port_file(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

/// Master node TCP server accepting worker connections and managing cluster lifecycle.
pub struct MasterServer {
    config: ServerConfig,
    listener: TcpListener,
    local_addr: SocketAddr,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    #[cfg(feature = "p2p")]
    p2p_endpoint: Option<iroh::Endpoint>,
    #[cfg(feature = "p2p")]
    p2p_active_conns: Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    #[cfg(feature = "dashboard")]
    pub broadcast_tx:
        Option<tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>>,
    /// Master's active P2P ticket, if enabled.
    pub p2p_ticket: Option<String>,
    /// The blob store, when `ServerConfig::blob_store` names one.
    blob_store: Option<Arc<BlobStore>>,
    /// Shared OXS-AUTH v3 authentication, member store, and live member connection registry.
    auth_ctx: MasterAuthContext,
}

impl MasterServer {
    /// Binds TCP listener with provided registry, allocating default task queue and notify triggers.
    pub async fn bind(config: ServerConfig, registry: WorkerRegistry) -> GridResult<Self> {
        let queue = TaskQueue::with_retention_limits(
            crate::queue::RetryPolicy::default(),
            config.max_queue_size,
            config.max_terminal_tasks,
            config.max_terminal_bytes,
            config.max_active_bytes,
        );
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let waiters = Arc::new(RwLock::new(HashMap::new()));
        Self::bind_full(config, registry, queue, scheduler_notify, waiters).await
    }

    /// Full constructor with explicit subsystem references.
    pub async fn bind_full(
        mut config: ServerConfig,
        registry: WorkerRegistry,
        queue: TaskQueue,
        scheduler_notify: Arc<tokio::sync::Notify>,
        waiters: WaiterMap,
    ) -> GridResult<Self> {
        #[cfg(feature = "p2p")]
        rusty_grid_core::transport::resolve_p2p_relay_mode(
            config.p2p_minimal_preset,
            config.p2p_relay_url.as_deref(),
        )?;

        // A state directory on a volume that keeps no file permissions (exFAT, FAT32) would hold the
        // keys and the member list readable by every account, and a restart would refuse
        // `members.json`: refuse it before anything is read or written there (BUG-24, design 0085).
        if let Some(ref state_dir) = config.state_dir {
            rusty_grid_core::private_file::ensure_volume_keeps_permissions(
                state_dir,
                "state directory",
            )
            .map_err(GridError::Config)?;
        }

        if !config.members_explicit && config.auth_mode.allows_members() {
            if let Some(ref state_dir) = config.state_dir {
                config.members = Arc::new(MemberStore::load_from_dir(Some(state_dir))?);
            }
        }
        config.members.set_auth_mode(config.auth_mode);
        if config.master_noise_key_path.is_none()
            && !config.ephemeral_key
            && config.auth_mode.allows_members()
        {
            if let Some(ref state_dir) = config.state_dir {
                config.master_noise_key_path = master_noise_key_path(Some(state_dir));
            }
        }
        if config.master_noise_key.is_none() {
            if config.auth_mode.allows_members() {
                if let Some(ref state_dir) = config.state_dir {
                    if !config.ephemeral_key {
                        if let Some(key_path) = master_noise_key_path(Some(state_dir)) {
                            let (kp, _) = NodeKeypair::load_or_generate_file(&key_path)?;
                            config.master_noise_key = Some(kp);
                        }
                    }
                }
            }
            if config.master_noise_key.is_none() {
                config.master_noise_key = Some(NodeKeypair::generate()?);
            }
        }
        if let Some(ref kp) = config.master_noise_key {
            info!(
                master_public_key = %kp.public_key().to_hex(),
                fingerprint = %kp.fingerprint(),
                auth_mode = %config.auth_mode,
                members = config.members.len(),
                "Master OXS-AUTH v3 identity ready"
            );
        }

        // Opened first: a store that cannot be used (e.g. `store.lock` held by another master)
        // must fail before `BlobStore::open` scans `blobs/` or purges `blobs/tmp/`, and before
        // any ticket or port file is written.
        let opened_task_store = if let Some(ref state_dir) = config.state_dir {
            let state_dir_clone = state_dir.clone();
            let (store, recovered) = tokio::task::spawn_blocking(move || {
                crate::store::TaskStore::open(&state_dir_clone)
            })
            .await
            .map_err(|e| GridError::Config(format!("Task store start-up failed: {e}")))??;
            Some((store, recovered))
        } else {
            None
        };

        let effective_blob_store = config.blob_store.clone().or_else(|| {
            config.state_dir.as_ref().map(|dir| {
                BlobStoreConfig::new(
                    dir.join("blobs"),
                    crate::blob_store::DEFAULT_BLOB_STORE_MAX_BYTES,
                )
            })
        });
        let blob_store = match effective_blob_store {
            Some(store_config) => {
                let opened = tokio::task::spawn_blocking(move || {
                    BlobStore::open(&store_config).map_err(|e| (store_config.dir, e))
                })
                .await
                .map_err(|e| GridError::Config(format!("Blob store start-up failed: {e}")))?;
                Some(opened.map_err(|(dir, e)| {
                    GridError::Config(format!(
                        "Cannot use the blob store directory '{}': {e}",
                        dir.display()
                    ))
                })?)
            }
            None => None,
        };

        queue
            .set_session_resume_hold(config.session_resume_hold)
            .await;

        if let Some((store, recovered)) = opened_task_store {
            queue
                .attach_store_and_replay(store, recovered, config.session_resume_hold)
                .await?;
            if let Some(ref bstore) = blob_store {
                queue.repin_recovered_blobs(bstore).await;
            }
            if !queue.ready_task_ids(1).await.is_empty() {
                scheduler_notify.notify_one();
            }
        }

        #[cfg(feature = "p2p")]
        let p2p_active_conns = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        #[cfg(feature = "p2p")]
        let (p2p_endpoint, p2p_ticket) = if config.enable_p2p || config.p2p_key_file.is_some() {
            if config.p2p_minimal_preset {
                warn!(
                    "P2P minimal preset is enabled (p2p_minimal_preset=true); DERP relay and pkarr/DNS address lookup are disabled, so workers behind NAT cannot connect"
                );
            }
            let resolved_key_file = if config.ephemeral_key {
                config.p2p_key_file.clone()
            } else {
                config.p2p_key_file.clone().or_else(default_master_key_file)
            };
            let secret_key = resolve_p2p_secret_key(resolved_key_file.as_deref()).await?;
            let mut ep_builder = rusty_grid_core::transport::p2p_endpoint_builder(
                config.p2p_minimal_preset,
                config.p2p_relay_url.as_deref(),
            )?;
            if let Some(p2p_addr) = config.p2p_bind_addr {
                ep_builder = ep_builder
                    .clear_ip_transports()
                    .bind_addr(p2p_addr)
                    .map_err(|e| {
                        GridError::Config(format!("Invalid P2P bind address '{p2p_addr}': {e}"))
                    })?;
            }
            let endpoint = ep_builder
                .secret_key(secret_key)
                .alpns(vec![GRID_ALPN.to_vec()])
                .bind()
                .await
                .map_err(|e| GridError::Config(format!("Failed to bind iroh endpoint: {e}")))?;

            if !config.p2p_minimal_preset {
                // Await home DERP relay connection and STUN discovery before serializing ticket.
                // Timeout gracefully so offline/air-gapped environments don't hang indefinitely.
                match tokio::time::timeout(Duration::from_secs(5), endpoint.online()).await {
                    Ok(_) => {
                        info!(
                            "Master connected to home DERP relay and discovered public network paths"
                        );
                    }
                    Err(_) => {
                        warn!("Timed out awaiting DERP relay readiness; proceeding with initial discovered addresses");
                    }
                }
            }

            let mut addr = endpoint.addr();
            if resolved_key_file.is_some() {
                // If relay address was discovered, retain relay addresses for stable WAN connectivity
                // across restarts without pinning dead ephemeral ports (:0).
                let has_relay = addr.addrs.iter().any(|a| a.is_relay());
                if has_relay {
                    addr.addrs.retain(|a| a.is_relay());
                    info!(
                        relay_count = addr.addrs.len(),
                        "Preserved home DERP relay addresses in persistent P2P ticket"
                    );
                } else {
                    warn!(
                        "No DERP relay discovered before ticket generation; retaining {} direct address(es)",
                        addr.addrs.len()
                    );
                }
            }
            let ticket = rusty_grid_core::transport::serialize_p2p_ticket(&addr)?;
            info!(ticket = %ticket, "Master P2P endpoint established");

            if let Some(ref ticket_path) = config.p2p_ticket_file {
                if let Some(parent) = ticket_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        #[cfg(windows)]
                        let _ = rusty_grid_core::private_file::create_private_dir_all(parent);
                        #[cfg(not(windows))]
                        let _ = tokio::fs::create_dir_all(parent).await;
                    }
                }
                // Windows: the ticket file gets the protected owner-only DACL as well (SEC-9).
                #[cfg(windows)]
                rusty_grid_core::private_file::write_private_replace(
                    ticket_path,
                    ticket.as_bytes(),
                )
                .map_err(GridError::Io)?;
                #[cfg(not(windows))]
                tokio::fs::write(ticket_path, &ticket)
                    .await
                    .map_err(GridError::Io)?;
                info!(path = %ticket_path.display(), "Published P2P ticket to file");
            }

            (Some(endpoint), Some(ticket))
        } else {
            (None, None)
        };
        #[cfg(not(feature = "p2p"))]
        let p2p_ticket: Option<String> = None;

        let listener = match TcpListener::bind(config.bind_addr).await {
            Ok(l) => l,
            Err(e) => {
                #[cfg(feature = "p2p")]
                if let Some(ref ticket_path) = config.p2p_ticket_file {
                    let _ = tokio::fs::remove_file(ticket_path).await;
                }
                return Err(GridError::Io(e));
            }
        };
        let local_addr = match listener.local_addr() {
            Ok(a) => a,
            Err(e) => {
                #[cfg(feature = "p2p")]
                if let Some(ref ticket_path) = config.p2p_ticket_file {
                    let _ = tokio::fs::remove_file(ticket_path).await;
                }
                return Err(GridError::Io(e));
            }
        };

        if let Some(ref port_path) = config.port_file {
            if let Err(e) = write_port_file(port_path, local_addr.port()).await {
                #[cfg(feature = "p2p")]
                if let Some(ref ticket_path) = config.p2p_ticket_file {
                    let _ = tokio::fs::remove_file(ticket_path).await;
                }
                return Err(e);
            }
            info!(port = local_addr.port(), path = %port_path.display(), "Port file published");
        }

        let auth_ctx = MasterAuthContext {
            auth_mode: config.auth_mode,
            master_noise_key: config.master_noise_key.clone(),
            master_noise_key_path: config.master_noise_key_path.clone(),
            local_node_key_path: config.local_node_key_path.clone(),
            ephemeral_without_state_dir: config.ephemeral_key && config.state_dir.is_none(),
            members: Arc::clone(&config.members),
            member_connections: crate::member_connections::MemberConnections::default(),
            p2p_ticket: p2p_ticket.clone(),
            enroll_confirm_timeout: Duration::from_secs(ENROLL_CONFIRM_TIMEOUT_SECS),
        };

        Ok(Self {
            config,
            listener,
            local_addr,
            registry,
            queue,
            scheduler_notify,
            waiters,
            #[cfg(feature = "p2p")]
            p2p_endpoint,
            #[cfg(feature = "p2p")]
            p2p_active_conns,
            #[cfg(feature = "dashboard")]
            broadcast_tx: None,
            p2p_ticket,
            blob_store,
            auth_ctx,
        })
    }

    /// Convenience constructor binding to address string.
    pub async fn bind_addr(addr: &str) -> GridResult<Self> {
        let config = ServerConfig::from_addr(addr)?;
        let registry = WorkerRegistry::new();
        Self::bind(config, registry).await
    }

    /// Spawns full Master stack (TCP accept loop, background scheduler, and heartbeat reaper),
    /// returning an ergonomic, thread-safe `MasterHandle`.
    pub async fn spawn(config: ServerConfig) -> GridResult<MasterHandle> {
        Self::spawn_with_config(
            config,
            SchedulerConfig::default().with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE),
            ReaperConfig::default(),
        )
        .await
    }

    /// Spawns full Master stack with custom scheduler and reaper configurations.
    pub async fn spawn_with_config(
        config: ServerConfig,
        sched_config: SchedulerConfig,
        reaper_config: ReaperConfig,
    ) -> GridResult<MasterHandle> {
        sched_config.validate().map_err(GridError::Config)?;
        let registry = WorkerRegistry::new();
        let retry_policy = crate::queue::RetryPolicy {
            max_retries: config.max_retries,
            max_failovers: config.max_failovers,
            max_evacuations: config.max_evacuations,
            ..Default::default()
        };
        let queue = TaskQueue::with_retention_limits(
            retry_policy,
            config.max_queue_size,
            config.max_terminal_tasks,
            config.max_terminal_bytes,
            config.max_active_bytes,
        );
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let waiters = Arc::new(RwLock::new(HashMap::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        #[cfg(feature = "dashboard")]
        let web_ui_addr = config.web_ui_addr;
        #[cfg(feature = "dashboard")]
        let enable_web_ui = config.enable_web_ui;
        let enable_discovery = config.enable_discovery;
        let discovery_port = config.discovery_port;
        #[allow(unused_mut)]
        let mut server = Self::bind_full(
            config.clone(),
            registry.clone(),
            queue.clone(),
            scheduler_notify.clone(),
            Arc::clone(&waiters),
        )
        .await?;
        let server_addr = server.local_addr();
        #[cfg(feature = "p2p")]
        let p2p_endpoint = server.p2p_endpoint.clone();
        #[cfg(feature = "p2p")]
        let p2p_active_conns = Arc::clone(&server.p2p_active_conns);
        let p2p_ticket = server.p2p_ticket.clone();
        let blob_store = server.blob_store.clone();
        let master_noise_public_key = server
            .config
            .master_noise_key
            .as_ref()
            .map(NodeKeypair::public_key)
            .copied();
        let auth_mode = server.config.auth_mode;
        let members = Arc::clone(&server.config.members);
        let auth_ctx = server.auth_ctx.clone();

        #[cfg(feature = "dashboard")]
        let (dashboard_server_opt, broadcast_tx) = if enable_web_ui
            || config.dashboard_port.is_some()
        {
            let bind_addr = if let Some(dash_port) = config.dashboard_port {
                // Loopback unless the caller names an interface: the dashboard must not follow the
                // grid listener onto a LAN address by itself.
                let bind_ip = config.dashboard_bind_ip.unwrap_or_else(|| {
                    if server_addr.ip().to_canonical().is_loopback() {
                        server_addr.ip()
                    } else {
                        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                    }
                });
                SocketAddr::new(bind_ip, dash_port)
            } else {
                web_ui_addr
            };

            match TcpListener::bind(bind_addr).await {
                Ok(dash_listener) => {
                    let bound_dash_addr = dash_listener.local_addr().map_err(GridError::Io)?;
                    info!(addr = %bound_dash_addr, "Unified Web Observability & Control Dashboard listening");

                    if let Some(ref dp_file) = config.dashboard_port_file {
                        write_port_file(dp_file, bound_dash_addr.port()).await?;
                        info!(port = bound_dash_addr.port(), path = %dp_file.display(), "Dashboard port file published");
                    }

                    let (b_tx, _) = tokio::sync::broadcast::channel(256);
                    (Some((dash_listener, bound_dash_addr)), Some(b_tx))
                }
                Err(e) if config.dashboard_port.is_none() => {
                    warn!(
                        error = %e,
                        addr = %bind_addr,
                        "Web UI port binding failed; continuing master RPC without Web UI"
                    );
                    (None, None)
                }
                Err(e) => return Err(GridError::Io(e)),
            }
        } else {
            (None, None)
        };

        #[cfg(feature = "dashboard")]
        let dashboard_addr = dashboard_server_opt.as_ref().map(|(_, addr)| *addr);

        #[cfg(not(feature = "dashboard"))]
        let dashboard_addr: Option<SocketAddr> = None;

        #[cfg(feature = "dashboard")]
        if let Some(ref b_tx) = broadcast_tx {
            server.broadcast_tx = Some(b_tx.clone());
        }

        // Build scheduler before starting accept loop so its SpeedBook is attached to `registry`
        // before any worker connection is handled.
        let sched = Arc::new(
            WorkloadScheduler::new(
                sched_config,
                registry.clone(),
                queue.clone(),
                scheduler_notify.clone(),
            )
            .with_waiters(Arc::clone(&waiters)),
        );

        // 1. Spawn TCP server accept loop
        let srv_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = server.run(srv_shutdown_rx).await {
                error!(error = %e, "Master server accept loop terminated with error");
            }
        });

        // 2. Spawn Background Scheduler loop
        let mut sched_shutdown_rx = shutdown_rx.clone();
        #[cfg(feature = "dashboard")]
        let sched_broadcast_tx = broadcast_tx.clone();
        #[cfg(feature = "dashboard")]
        let sched_queue = queue.clone();
        let sched_trigger = scheduler_notify.clone();

        tokio::spawn(async move {
            info!(
                policy = ?sched.config().policy,
                tick_interval_ms = sched.config().tick_interval.as_millis(),
                "Starting WorkloadScheduler event-driven background loop"
            );

            let mut ticker = tokio::time::interval(sched.config().tick_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_pass: Option<std::time::Instant> = None;
            const MAX_CONSECUTIVE_BATCHES: usize = 16;

            loop {
                tokio::select! {
                    _ = sched_trigger.notified() => {
                        debug!("Scheduler loop woken by event notification");
                        let delay = last_pass.map_or(Duration::ZERO, |t| {
                            sched.config().micro_batch_window.saturating_sub(t.elapsed())
                        });
                        if delay > Duration::ZERO {
                            tokio::time::sleep(delay).await;
                        }
                        for _ in 0..MAX_CONSECUTIVE_BATCHES {
                            if *sched_shutdown_rx.borrow() {
                                break;
                            }
                            let batch_cap = sched.config().max_batch_size.max(1);
                            let res = sched.schedule_once().await;
                            last_pass = Some(std::time::Instant::now());
                            if let Ok(ref report) = res {
                                let _ = &report;
                                #[cfg(feature = "dashboard")]
                                if let Some(ref b_tx) = sched_broadcast_tx {
                                    if b_tx.receiver_count() > 0
                                        && (!report.assignments.is_empty()
                                            || !report.unfulfillable.is_empty())
                                    {
                                        for assignment in &report.assignments {
                                            if let Some(info) = sched_queue.get_task(&assignment.task_id).await {
                                                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
                                            }
                                        }
                                        for (task_id, _) in &report.unfulfillable {
                                            if let Some(info) = sched_queue.get_task(task_id).await {
                                                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
                                            }
                                        }
                                        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(sched_queue.stats().await));
                                    }
                                }
                            }
                            let filled = res
                                .as_ref()
                                .is_ok_and(|r| r.assignments.len() >= batch_cap);
                            if !filled {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                    _ = ticker.tick() => {
                        let res = sched.schedule_once().await;
                        last_pass = Some(std::time::Instant::now());
                        if let Ok(ref report) = res {
                            let _ = &report;
                            #[cfg(feature = "dashboard")]
                            if let Some(ref b_tx) = sched_broadcast_tx {
                                if b_tx.receiver_count() > 0
                                    && (!report.assignments.is_empty()
                                        || !report.unfulfillable.is_empty())
                                {
                                    for assignment in &report.assignments {
                                        if let Some(info) = sched_queue.get_task(&assignment.task_id).await {
                                            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
                                        }
                                    }
                                    for (task_id, _) in &report.unfulfillable {
                                        if let Some(info) = sched_queue.get_task(task_id).await {
                                            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
                                        }
                                    }
                                    let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(sched_queue.stats().await));
                                }
                            }
                        }
                    }

                    res = sched_shutdown_rx.changed() => {
                        if res.is_ok() && *sched_shutdown_rx.borrow() {
                            info!("WorkloadScheduler received shutdown signal; terminating");
                            break;
                        } else if res.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        // 3. Spawn Heartbeat Reaper with Queue Failover
        let reaper_shutdown_rx = shutdown_rx.clone();
        #[cfg(feature = "dashboard")]
        crate::reaper::spawn_reaper_with_broadcast(
            registry.clone(),
            queue.clone(),
            scheduler_notify.clone(),
            Some(Arc::clone(&waiters)),
            reaper_config,
            reaper_shutdown_rx,
            broadcast_tx.clone(),
        );
        #[cfg(not(feature = "dashboard"))]
        spawn_reaper_with_queue(
            registry.clone(),
            queue.clone(),
            scheduler_notify.clone(),
            Some(Arc::clone(&waiters)),
            reaper_config,
            reaper_shutdown_rx,
        );

        // The dev bypass is decided from where the dashboard listener really is bound: never on a
        // non-loopback address, automatically on a bare loopback master (no secret, no token).
        #[cfg(feature = "dashboard")]
        let (resolved_dashboard_auth, dashboard_dev_bypass) = match dashboard_addr {
            Some(addr) => {
                let (auth, bypass) = crate::auth::resolve_dashboard_auth(
                    config.auth_config.clone(),
                    config.cluster_secret.is_some(),
                    config.implicit_local_dashboard,
                    addr,
                );
                (Some(auth), bypass)
            }
            None => (None, DevBypass::Off),
        };
        #[cfg(not(feature = "dashboard"))]
        let dashboard_dev_bypass = DevBypass::Off;
        let handle = MasterHandle {
            server_addr,
            dashboard_addr,
            dashboard_dev_bypass,
            mapreduce_parallelism: config.mapreduce_parallelism,
            registry,
            queue,
            scheduler_notify,
            waiters,
            shutdown_tx,
            #[cfg(feature = "dashboard")]
            broadcast_tx: broadcast_tx.clone(),
            #[cfg(feature = "p2p")]
            p2p_endpoint,
            #[cfg(feature = "p2p")]
            p2p_active_conns,
            p2p_ticket: p2p_ticket.clone(),
            cluster_secret: config.cluster_secret.clone(),
            master_noise_public_key,
            auth_mode,
            members,
            blob_store: blob_store.clone(),
            auth_ctx,
        };

        // Pins of finished tasks are released here when no result released them (failover out of
        // retries, the reaper, a cancellation), and stalled uploads are dropped.
        if let Some(store) = blob_store {
            spawn_blob_store_sweeper(store, handle.queue().clone(), shutdown_rx.clone());
        }

        // 4. Spawn Unified Web Observability & Control Dashboard (if enabled)
        #[cfg(feature = "dashboard")]
        if let Some((dash_listener, bound_dash_addr)) = dashboard_server_opt {
            let b_tx = broadcast_tx.as_ref().unwrap().clone();
            let mut dashboard_auth =
                resolved_dashboard_auth.unwrap_or_else(|| config.auth_config.clone());
            let dev_bypass = dashboard_dev_bypass;
            if let Some(secret) = config.cluster_secret.as_ref() {
                // The dashboard Admin token is derived from the secret, never the secret itself.
                dashboard_auth = dashboard_auth.with_cluster_secret(secret);
            }
            if !bound_dash_addr.ip().to_canonical().is_loopback() {
                warn!(addr = %bound_dash_addr, "Dashboard is listening on a non-loopback address over plain HTTP: tokens and task output cross the network unencrypted; keep it on loopback and reach it through an SSH tunnel or VPN");
            }
            if dashboard_auth.enabled {
                match dev_bypass {
                    DevBypass::Explicit => warn!("Dashboard dev mode is ON: local same-origin requests get unauthenticated Admin access; never enable it on a shared machine"),
                    DevBypass::Implicit => warn!(addr = %bound_dash_addr, "Dashboard: no cluster secret and no token are configured, so this loopback-only dashboard trusts requests from this machine without a login; set a secret or --auth-token to require one"),
                    DevBypass::RefusedNonLoopback => warn!(addr = %bound_dash_addr, "Ignoring --dev / OXIDE_DEV_MODE: the dashboard listener is not bound to a loopback address, so unauthenticated access stays disabled"),
                    DevBypass::Off if dashboard_auth.tokens.is_empty() => warn!("Dashboard has no credentials configured, so every API request will be rejected with 401 (use a cluster secret, --auth-token, or --dev for local development)"),
                    DevBypass::Off => {}
                }
            }
            let auth_config = Arc::new(dashboard_auth);
            let router = crate::web_ui::create_web_ui_router_full(
                handle.registry().clone(),
                handle.queue().clone(),
                handle.scheduler_notify().clone(),
                Some(handle.clone()),
                p2p_ticket.clone(),
                b_tx,
                server_addr,
                Some(bound_dash_addr),
                std::time::Instant::now(),
                auth_config,
            );

            let mut dash_shutdown_rx = shutdown_rx.clone();
            let shutdown_signal = async move {
                while !*dash_shutdown_rx.borrow_and_update() {
                    if dash_shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            };

            tokio::spawn(async move {
                let app = router.into_make_service_with_connect_info::<SocketAddr>();
                if let Err(e) = axum::serve(dash_listener, app)
                    .with_graceful_shutdown(shutdown_signal)
                    .await
                {
                    warn!(error = %e, "Unified Web UI HTTP server terminated with error");
                }
            });
        }

        // 5. Spawn UDP Master Discovery Broadcaster & Active Probe Responder
        if enable_discovery {
            let disc_shutdown_rx = shutdown_rx.clone();
            let listen_ip = server_addr.ip();
            let cluster_port = server_addr.port();

            let disc_registry = handle.registry().clone();
            let disc_secret = config.cluster_secret.clone();
            tokio::spawn(async move {
                spawn_master_discovery_service(
                    listen_ip,
                    cluster_port,
                    dashboard_addr,
                    discovery_port,
                    disc_registry,
                    disc_secret,
                    disc_shutdown_rx,
                )
                .await;
            });
        }

        // 6. Spawn Rendezvous Registry Periodic Registration (if configured)
        let rendezvous_target = config.rendezvous_url.clone().or_else(|| {
            std::env::var("OXIDESWARM_RENDEZVOUS")
                .or_else(|_| std::env::var("RUSTY_GRID_RENDEZVOUS"))
                .ok()
                .filter(|s| !s.trim().is_empty())
        });
        if let Some(rendezvous_url) = rendezvous_target {
            let web_ui_url = dashboard_addr
                .map(|addr| {
                    let host = if addr.ip().is_unspecified() {
                        rusty_grid_core::discovery::get_local_ip_or_loopback()
                    } else {
                        addr.ip()
                    };
                    format!("http://{}:{}", host, addr.port())
                })
                .unwrap_or_default();
            let hostname =
                sysinfo::System::host_name().unwrap_or_else(|| "OxideMaster".to_string());
            let reg = handle.registry().clone();
            let sec = config.cluster_secret.clone();
            let rx = shutdown_rx.clone();
            let ticket = p2p_ticket.clone();
            let c_addr = if server_addr.ip().is_unspecified() {
                let local_ip = rusty_grid_core::discovery::get_local_ip_or_loopback();
                format!("{}:{}", local_ip, server_addr.port())
            } else {
                server_addr.to_string()
            };
            tokio::spawn(async move {
                spawn_master_rendezvous_registration(
                    rendezvous_url,
                    c_addr,
                    web_ui_url,
                    hostname,
                    ticket,
                    reg,
                    sec,
                    rx,
                )
                .await;
            });
        }

        Ok(handle)
    }

    /// Returns the local socket address this server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the bound TCP port number.
    pub fn port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Returns a reference to the associated `WorkerRegistry`.
    pub fn registry(&self) -> &WorkerRegistry {
        &self.registry
    }

    /// Returns a reference to the associated `TaskQueue`.
    pub fn queue(&self) -> &TaskQueue {
        &self.queue
    }

    /// Returns a reference to the scheduler notification trigger.
    pub fn scheduler_notify(&self) -> &Arc<tokio::sync::Notify> {
        &self.scheduler_notify
    }

    /// Returns a reference to the active P2P endpoint, if enabled.
    #[cfg(feature = "p2p")]
    pub fn p2p_endpoint(&self) -> Option<&iroh::Endpoint> {
        self.p2p_endpoint.as_ref()
    }

    /// Returns the active P2P connection registry mapping worker IDs to connections.
    #[cfg(feature = "p2p")]
    pub fn p2p_connection_map(
        &self,
    ) -> Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>> {
        Arc::clone(&self.p2p_active_conns)
    }

    /// Queries live P2P path and latency telemetry for a connected worker.
    #[cfg(feature = "p2p")]
    pub async fn get_worker_p2p_info(
        &self,
        worker_id: &Uuid,
    ) -> Option<rusty_grid_core::transport::P2pPathInfo> {
        let guard = self.p2p_active_conns.read().await;
        guard
            .get(worker_id)
            .and_then(rusty_grid_core::transport::inspect_connection_paths)
    }

    /// Returns the active P2P ticket for this master instance, if P2P is enabled.
    pub fn p2p_ticket(&self) -> Option<&str> {
        self.p2p_ticket.as_deref()
    }
}

/// Returns the platform-specific default master P2P secret key file path.
/// Unix / macOS: ~/.oxideswarm/master_key.bin
/// Windows: %USERPROFILE%\.oxideswarm\master_key.bin
pub fn default_master_key_file() -> Option<PathBuf> {
    if let Some(custom) = std::env::var_os("OXIDESWARM_HOME").filter(|s| !s.is_empty()) {
        return Some(PathBuf::from(custom).join("master_key.bin"));
    }
    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE")
            .or_else(|| std::env::var_os("HOME"))
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .map(|h| h.join(".oxideswarm").join("master_key.bin"))
    }
    #[cfg(not(windows))]
    {
        std::env::var_os("HOME")
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .map(|h| h.join(".oxideswarm").join("master_key.bin"))
    }
}

/// Makes an existing key file private: if group or others can read it, its mode becomes 0600.
/// Returns the old mode when it had to change it.
#[cfg(all(feature = "p2p", unix))]
fn tighten_key_permissions(path: &Path) -> std::io::Result<Option<u32>> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    if mode & 0o077 == 0 {
        return Ok(None);
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(Some(mode))
}

/// Creates the key file exclusively with mode 0600 (Windows: a protected DACL for the current
/// user, SYSTEM and Administrators, SEC-9) and writes `data`, so the key is never readable by
/// others, not even between creation and a later chmod.
#[cfg(feature = "p2p")]
fn write_new_private_key(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    #[cfg(windows)]
    let mut file = rusty_grid_core::private_file::create_private_new(path)?;
    #[cfg(not(windows))]
    let mut file = {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        opts.open(path)?
    };
    file.write_all(data)?;
    file.sync_all()
}

/// Resolves the P2P node secret key from file or generates a new one.
///
/// The key is the master's network identity (whoever holds it can impersonate the master to every
/// worker that dialled its ticket), so a new file is created readable by its owner only, and an
/// existing file that is readable by others is tightened to mode 0600 with a warning.
#[cfg(feature = "p2p")]
async fn resolve_p2p_secret_key(key_path_opt: Option<&Path>) -> GridResult<iroh::SecretKey> {
    match key_path_opt {
        Some(path) => {
            if tokio::fs::try_exists(path).await.unwrap_or(false) {
                #[cfg(unix)]
                match tighten_key_permissions(path) {
                    Ok(None) => {}
                    Ok(Some(old_mode)) => warn!(
                        path = %path.display(),
                        old_mode = format!("{old_mode:o}"),
                        "P2P master key file was readable by other users; tightened it to mode 600. Treat the key as exposed if other accounts share this machine: delete the file to get a new identity (and a new ticket)"
                    ),
                    Err(e) => warn!(
                        path = %path.display(),
                        error = %e,
                        "P2P master key file may be readable by other users and could not be made private; run `chmod 600` on it"
                    ),
                }
                #[cfg(windows)]
                rusty_grid_core::private_file::warn_if_readable_by_others(
                    path,
                    "P2P master key file",
                );
                let data = tokio::fs::read(path).await.map_err(GridError::Io)?;
                // 1. Raw 32-byte binary format (e.g. test_key.bin)
                if data.len() == 32 {
                    let mut bytes = [0u8; 32];
                    bytes.copy_from_slice(&data);
                    info!(path = %path.display(), "Loaded 32-byte binary P2P secret key");
                    return Ok(iroh::SecretKey::from_bytes(&bytes));
                }
                // 2. UTF-8 string format (hex or base32)
                if let Ok(s) = std::str::from_utf8(&data) {
                    let trimmed = s.trim();
                    if let Ok(sk) = trimmed.parse::<iroh::SecretKey>() {
                        info!(path = %path.display(), "Loaded string-encoded P2P secret key");
                        return Ok(sk);
                    }
                }
                // 3. Fallback slice parser
                iroh::SecretKey::try_from(data.as_slice()).map_err(|e| {
                    GridError::Config(format!(
                        "Invalid P2P secret key file '{}': {e}",
                        path.display()
                    ))
                })
            } else {
                let secret_key = iroh::SecretKey::generate();
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        let existed = parent.exists();
                        #[cfg(windows)]
                        rusty_grid_core::private_file::create_private_dir_all(parent)
                            .map_err(GridError::Io)?;
                        #[cfg(not(windows))]
                        tokio::fs::create_dir_all(parent)
                            .await
                            .map_err(GridError::Io)?;
                        #[cfg(unix)]
                        if !existed {
                            use std::os::unix::fs::PermissionsExt;
                            let _ = std::fs::set_permissions(
                                parent,
                                std::fs::Permissions::from_mode(0o700),
                            );
                        }
                        #[cfg(not(unix))]
                        let _ = existed;
                    }
                }
                write_new_private_key(path, &secret_key.to_bytes()).map_err(GridError::Io)?;
                info!(path = %path.display(), "Generated and saved new P2P secret key");
                Ok(secret_key)
            }
        }
        None => {
            let secret_key = iroh::SecretKey::generate();
            debug!("Generated ephemeral in-memory P2P secret key");
            Ok(secret_key)
        }
    }
}

/// Pause between `accept()` retries after an error, so that a persistent failure (out of file
/// descriptors, out of buffers) cannot spin the accept loop. Starts short, doubles up to a ceiling
/// and starts over after a successful accept.
#[derive(Debug)]
struct AcceptBackoff {
    next: Duration,
}

impl AcceptBackoff {
    const INITIAL: Duration = Duration::from_millis(50);
    const MAX: Duration = Duration::from_secs(1);

    fn new() -> Self {
        Self {
            next: Self::INITIAL,
        }
    }

    /// The pause to take after another failure; the following one is twice as long.
    fn failure(&mut self) -> Duration {
        let pause = self.next;
        self.next = (self.next * 2).min(Self::MAX);
        pause
    }

    fn reset(&mut self) {
        self.next = Self::INITIAL;
    }
}

/// The budgets that keep a flood of peers from exhausting descriptors and tasks: connections that
/// are open at all, and the smaller number that are still in their pre-authentication handshake.
#[derive(Clone)]
struct ConnectionLimits {
    connections: Arc<Semaphore>,
    handshakes: Arc<Semaphore>,
}

impl ConnectionLimits {
    fn new(max_connections: usize, max_pending_handshakes: usize) -> Self {
        Self {
            connections: Arc::new(Semaphore::new(max_connections.max(1))),
            handshakes: Arc::new(Semaphore::new(max_pending_handshakes.max(1))),
        }
    }

    /// Reserves one slot of each budget for a new peer, or `None` if either is used up. The
    /// connection permit lives as long as the connection; the handshake permit is dropped as soon
    /// as the peer has authenticated and sent its first message.
    fn admit(&self) -> Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let connection = Arc::clone(&self.connections).try_acquire_owned().ok()?;
        let handshake = Arc::clone(&self.handshakes).try_acquire_owned().ok()?;
        Some((connection, handshake))
    }
}

/// Rate-limits the "refused a connection" warning to one line per interval.
#[derive(Default)]
struct RejectLog {
    last: Option<std::time::Instant>,
    suppressed: u64,
}

impl RejectLog {
    fn rejected(&mut self, transport: &str) {
        let now = std::time::Instant::now();
        let due = match self.last {
            Some(last) => now.duration_since(last) >= Duration::from_secs(5),
            None => true,
        };
        if due {
            warn!(
                transport,
                also_refused = self.suppressed,
                "Connection budget exhausted; refusing new peers"
            );
            self.last = Some(now);
            self.suppressed = 0;
        } else {
            self.suppressed += 1;
        }
    }
}

impl MasterServer {
    /// Runs the master server accept loop until `shutdown_rx` signals termination.
    pub async fn run(self, mut shutdown_rx: watch::Receiver<bool>) -> GridResult<()> {
        info!(listen_addr = %self.local_addr, "Master server accept loop started");

        #[cfg(all(feature = "dashboard", feature = "p2p"))]
        let p2p_broadcast_tx = self.broadcast_tx.clone();

        // One connection budget for the whole grid port, shared by TCP and QUIC.
        let limits = ConnectionLimits::new(
            self.config.max_connections,
            self.config.max_pending_handshakes,
        );
        let auth_ctx = self.auth_ctx.clone();
        if auth_ctx.auth_mode.allows_members() {
            spawn_member_store_resync(Arc::clone(&auth_ctx.members), shutdown_rx.clone());
        }

        #[cfg(feature = "p2p")]
        if let Some(endpoint) = self.p2p_endpoint {
            let p2p_shutdown_rx = shutdown_rx.clone();
            let p2p_reg = self.registry.clone();
            let p2p_queue = self.queue.clone();
            let p2p_sched = self.scheduler_notify.clone();
            let p2p_waiters = Arc::clone(&self.waiters);
            let p2p_hb = self.config.heartbeat_interval_secs;
            let p2p_to = Duration::from_secs(self.config.handshake_timeout_secs);
            let p2p_codec = self.config.wire_codec;
            let p2p_conns = Arc::clone(&self.p2p_active_conns);
            let p2p_secret = self.config.cluster_secret.clone();
            let p2p_mapreduce_parallelism = self.config.mapreduce_parallelism;
            let p2p_limits = limits.clone();
            let p2p_blob_store = self.blob_store.clone();
            let p2p_auth_ctx = auth_ctx.clone();

            tokio::spawn(async move {
                let mut p2p_shutdown = p2p_shutdown_rx;
                let mut reject_log = RejectLog::default();
                loop {
                    if *p2p_shutdown.borrow_and_update() {
                        endpoint.close().await;
                        break;
                    }
                    tokio::select! {
                        incoming = endpoint.accept() => {
                            let Some(incoming) = incoming else {
                                // The endpoint was closed: nothing will ever be accepted again.
                                debug!("P2P endpoint closed; stopping the QUIC accept loop");
                                break;
                            };
                            let Some((conn_permit, handshake_permit)) = p2p_limits.admit() else {
                                reject_log.rejected("QUIC");
                                incoming.refuse();
                                continue;
                            };

                            let c_reg = p2p_reg.clone();
                            let c_q = p2p_queue.clone();
                            let c_sched = p2p_sched.clone();
                            let c_wait = Arc::clone(&p2p_waiters);
                            let c_shut = p2p_shutdown.clone();
                            let c_codec = p2p_codec;
                            let c_conns = Arc::clone(&p2p_conns);
                            let c_secret = p2p_secret.clone();
                            let c_blob_store = p2p_blob_store.clone();
                            let c_auth_ctx = p2p_auth_ctx.clone();
                            #[cfg(feature = "dashboard")]
                            let c_bcast = p2p_broadcast_tx.clone();

                            // The handler needs its own abort handle so the reaper (and a worker
                            // re-registering) can really tear the connection down; handed over
                            // through a channel because it only exists once the task is spawned.
                            //
                            // The QUIC handshake itself runs inside the spawned task: awaiting it
                            // here would let one stalled peer hold up every other new connection.
                            let (abort_tx, abort_rx) = oneshot::channel::<AbortHandle>();
                            let conn_task = tokio::spawn(async move {
                                let _conn_permit = conn_permit;
                                let abort_handle = abort_rx.await.ok();
                                let conn = match tokio::time::timeout(p2p_to, incoming).await {
                                    Ok(Ok(conn)) => conn,
                                    Ok(Err(e)) => {
                                        warn!(error = %e, "P2P connection accept failed");
                                        return;
                                    }
                                    Err(_) => {
                                        warn!(timeout_secs = p2p_to.as_secs(), "P2P connection handshake timed out");
                                        return;
                                    }
                                };
                                let c_conn = conn.clone();
                                let path_info = rusty_grid_core::transport::inspect_connection_paths(&conn);
                                let remote_addr: SocketAddr = if let Some(ref info) = path_info {
                                    if info.is_ip {
                                        info.remote_addr.parse().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap())
                                    } else {
                                        "127.0.0.1:0".parse().unwrap()
                                    }
                                } else {
                                    "127.0.0.1:0".parse().unwrap()
                                };

                                // A peer that completes the QUIC handshake and then opens no stream
                                // must not hold its slot for ever.
                                let (send1, mut recv1) = match tokio::time::timeout(p2p_to, conn.accept_bi()).await {
                                    Ok(Ok(s)) => s,
                                    Ok(Err(e)) => {
                                        warn!(error = %e, "Failed to accept bidirectional P2P stream");
                                        return;
                                    }
                                    Err(_) => {
                                        warn!(timeout_secs = p2p_to.as_secs(), "Timed out waiting for the first P2P stream");
                                        return;
                                    }
                                };

                                let mut tag1 = [0u8; 1];
                                let tag_read_res = tokio::time::timeout(
                                    p2p_to,
                                    tokio::io::AsyncReadExt::read_exact(&mut recv1, &mut tag1),
                                )
                                .await;

                                match tag_read_res {
                                    Ok(Ok(_))
                                        if tag1[0] == STREAM_CONTROL
                                            || tag1[0] == STREAM_DATA =>
                                    {
                                        let (send2, mut recv2) = match tokio::time::timeout(p2p_to, conn.accept_bi()).await {
                                            Ok(Ok(s)) => s,
                                            _ => {
                                                warn!("Failed to accept second P2P stream for multiplexed connection");
                                                return;
                                            }
                                        };
                                        let mut tag2 = [0u8; 1];
                                        if tokio::time::timeout(p2p_to, tokio::io::AsyncReadExt::read_exact(&mut recv2, &mut tag2)).await.is_err() {
                                            warn!("Failed to read tag on second P2P stream");
                                            return;
                                        }

                                        let (ctrl_stream, data_stream) = if tag1[0] == STREAM_CONTROL && tag2[0] == STREAM_DATA {
                                            (
                                                GridStream::P2p(BiStream::new(recv1, send1)),
                                                GridStream::P2p(BiStream::new(recv2, send2)),
                                            )
                                        } else if tag1[0] == STREAM_DATA && tag2[0] == STREAM_CONTROL {
                                            (
                                                GridStream::P2p(BiStream::new(recv2, send2)),
                                                GridStream::P2p(BiStream::new(recv1, send1)),
                                            )
                                        } else {
                                            warn!("Invalid stream tags for multiplexed connection: tag1={}, tag2={}", tag1[0], tag2[0]);
                                            return;
                                        };

                                        let _ = handle_multiplexed_connection(
                                            ctrl_stream,
                                            data_stream,
                                            remote_addr,
                                            c_reg,
                                            c_q,
                                            c_sched,
                                            c_wait,
                                            p2p_hb,
                                            p2p_to,
                                            c_shut,
                                            abort_handle,
                                            c_codec,
                                            c_secret,
                                            c_auth_ctx,
                                            Some(handshake_permit),
                                            c_blob_store,
                                            #[cfg(feature = "dashboard")]
                                            c_bcast,
                                            #[cfg(feature = "p2p")]
                                            Some(c_conn),
                                            #[cfg(feature = "p2p")]
                                            Some(c_conns),
                                        ).await;
                                    }
                                    Ok(Ok(_)) if tag1[0] == STREAM_CLIENT => {
                                        let stream = GridStream::P2p(BiStream::new(recv1, send1));
                                        let _ = handle_connection(
                                            stream,
                                            remote_addr,
                                            c_reg,
                                            c_q,
                                            c_sched,
                                            c_wait,
                                            p2p_hb,
                                            p2p_to,
                                            c_shut,
                                            abort_handle,
                                            c_codec,
                                            c_secret,
                                            c_auth_ctx,
                                            Some(handshake_permit),
                                            p2p_mapreduce_parallelism,
                                            c_blob_store,
                                            #[cfg(feature = "dashboard")]
                                            c_bcast,
                                            #[cfg(feature = "p2p")]
                                            Some(c_conn),
                                            #[cfg(feature = "p2p")]
                                            Some(c_conns),
                                        )
                                        .await;
                                    }
                                    _ => {
                                        warn!("Failed or untagged P2P stream; closing connection");
                                    }
                                }
                            });
                            let _ = abort_tx.send(conn_task.abort_handle());
                        }
                        res = p2p_shutdown.changed() => {
                            if res.is_err() || *p2p_shutdown.borrow() {
                                endpoint.close().await;
                                break;
                            }
                        }
                    }
                }
            });
        }

        let mut accept_backoff = AcceptBackoff::new();
        let mut reject_log = RejectLog::default();
        loop {
            if *shutdown_rx.borrow_and_update() {
                info!("Master server received shutdown signal; stopping accept loop");
                break;
            }
            tokio::select! {
                accept_res = self.listener.accept() => {
                    match accept_res {
                        Ok((stream, remote_addr)) => {
                            accept_backoff.reset();
                            let Some((conn_permit, handshake_permit)) = limits.admit() else {
                                // Over budget: refuse by closing, before spending a task on it.
                                reject_log.rejected("TCP");
                                drop(stream);
                                continue;
                            };
                            if let Err(e) = rusty_grid_core::transport::configure_tcp_stream(&stream) {
                                debug!(remote_addr = %remote_addr, error = %e, "Could not apply TCP nodelay/keepalive options");
                            }
                            let registry = self.registry.clone();
                            let queue = self.queue.clone();
                            let scheduler_notify = self.scheduler_notify.clone();
                            let waiters = Arc::clone(&self.waiters);
                            let heartbeat_interval = self.config.heartbeat_interval_secs;
                            let handshake_timeout = Duration::from_secs(self.config.handshake_timeout_secs);
                            let conn_shutdown_rx = shutdown_rx.clone();
                            let conn_wire_codec = self.config.wire_codec;
                            let conn_secret = self.config.cluster_secret.clone();
                            let conn_mapreduce_parallelism = self.config.mapreduce_parallelism;
                            let conn_blob_store = self.blob_store.clone();
                            let conn_auth_ctx = auth_ctx.clone();
                            #[cfg(feature = "dashboard")]
                            let conn_broadcast_tx = self.broadcast_tx.clone();
                            #[cfg(feature = "p2p")]
                            let conn_p2p_active = Arc::clone(&self.p2p_active_conns);

                            let (abort_tx, abort_rx) = oneshot::channel::<AbortHandle>();
                            let conn_handle = tokio::spawn(async move {
                                let _conn_permit = conn_permit;
                                let abort_handle = abort_rx.await.ok();
                                if let Err(e) = handle_connection(
                                    stream,
                                    remote_addr,
                                    registry,
                                    queue,
                                    scheduler_notify,
                                    waiters,
                                    heartbeat_interval,
                                    handshake_timeout,
                                    conn_shutdown_rx,
                                    abort_handle,
                                    conn_wire_codec,
                                    conn_secret,
                                    conn_auth_ctx,
                                    Some(handshake_permit),
                                    conn_mapreduce_parallelism,
                                    conn_blob_store,
                                    #[cfg(feature = "dashboard")]
                                    conn_broadcast_tx,
                                    #[cfg(feature = "p2p")]
                                    None,
                                    #[cfg(feature = "p2p")]
                                    Some(conn_p2p_active),
                                ).await {
                                    debug!(remote_addr = %remote_addr, error = %e, "Worker connection ended");
                                }
                            });
                            let _ = abort_tx.send(conn_handle.abort_handle());
                        }
                        Err(e) => {
                            // EMFILE/ENFILE/ENOBUFS and friends fail again immediately: without a
                            // pause this loop would spin at full CPU (and flood the log) while the
                            // very descriptors it needs are exhausted.
                            let pause = accept_backoff.failure();
                            error!(error = %e, retry_in_ms = pause.as_millis() as u64, "TCP accept failed");
                            tokio::select! {
                                _ = tokio::time::sleep(pause) => {}
                                res = shutdown_rx.changed() => {
                                    if res.is_err() || *shutdown_rx.borrow() {
                                        info!("Master server received shutdown signal; stopping accept loop");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                res = shutdown_rx.changed() => {
                    if res.is_ok() && *shutdown_rx.borrow() {
                        info!("Master server received shutdown signal; stopping accept loop");
                        break;
                    } else if res.is_err() {
                        break;
                    }
                }
            }
        }

        // Clean up port file on shutdown
        if let Some(ref port_path) = self.config.port_file {
            remove_port_file(port_path).await;
            info!(path = %port_path.display(), "Port file removed on shutdown");
        }

        // Clean up dashboard port file on shutdown
        if let Some(ref dp_path) = self.config.dashboard_port_file {
            remove_port_file(dp_path).await;
            info!(path = %dp_path.display(), "Dashboard port file removed on shutdown");
        }

        // Clean up P2P ticket file on shutdown only if running in ephemeral mode
        if self.config.ephemeral_key {
            if let Some(ref ticket_path) = self.config.p2p_ticket_file {
                let _ = tokio::fs::remove_file(ticket_path).await;
            }
        }

        self.queue.close_and_compact_store().await;
        if auth_ctx.auth_mode.allows_members() && auth_ctx.members.is_dirty() {
            let members_for_shutdown = Arc::clone(&auth_ctx.members);
            if let Err(e) =
                run_member_store_blocking_inner(move || members_for_shutdown.flush_dirty()).await
            {
                warn!(
                    error = %e,
                    "Failed to flush dirty members.json during master server shutdown"
                );
            }
        }

        Ok(())
    }
}

/// Thread-safe client and programmatic handle to a running Master node.
#[derive(Clone)]
pub struct MasterHandle {
    server_addr: SocketAddr,
    dashboard_addr: Option<SocketAddr>,
    dashboard_dev_bypass: DevBypass,
    mapreduce_parallelism: usize,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    shutdown_tx: watch::Sender<bool>,
    #[cfg(feature = "dashboard")]
    pub broadcast_tx:
        Option<tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>>,
    #[cfg(feature = "p2p")]
    p2p_endpoint: Option<iroh::Endpoint>,
    #[cfg(feature = "p2p")]
    p2p_active_conns: Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    /// Master's active P2P ticket, if enabled.
    pub p2p_ticket: Option<String>,
    pub cluster_secret: Option<ClusterSecret>,
    master_noise_public_key: Option<PublicKey>,
    auth_mode: AuthMode,
    members: Arc<MemberStore>,
    blob_store: Option<Arc<BlobStore>>,
    auth_ctx: MasterAuthContext,
}

impl MasterHandle {
    /// Returns the master's OXS-AUTH v3 static X25519 public key, if configured.
    pub fn master_noise_public_key(&self) -> Option<PublicKey> {
        self.master_noise_public_key
    }

    /// Returns the configured [`AuthMode`] for this master.
    pub fn auth_mode(&self) -> AuthMode {
        self.auth_mode
    }

    /// Returns the enrolled [`MemberStore`] for this master.
    pub fn members(&self) -> &Arc<MemberStore> {
        &self.members
    }

    /// Returns the shared OXS-AUTH v3 auth context (member store, connection registry, keys).
    #[allow(dead_code)]
    pub(crate) fn auth_ctx(&self) -> &MasterAuthContext {
        &self.auth_ctx
    }

    /// Returns the shared terminal task waiter map.
    #[allow(dead_code)]
    pub(crate) fn waiters(&self) -> &WaiterMap {
        &self.waiters
    }

    /// The blob store of task inputs and output files, if this master runs one.
    pub fn blob_store(&self) -> Option<&Arc<BlobStore>> {
        self.blob_store.as_ref()
    }

    /// Admits a task for submission through this handle (see `admit_blob_task`); returns
    /// whether the task id was new (so a refused submission can release its pins) and whether its
    /// tree manifests require `TREE_V2`.
    async fn admit(&self, task: &Task) -> GridResult<(bool, bool)> {
        if self.queue.get_task(&task.id).await.is_some() {
            return Ok((false, false));
        }
        let needs_tree_v2 = admit_blob_task(self.blob_store.as_ref(), task)
            .await
            .map_err(GridError::Config)?;
        Ok((true, needs_tree_v2))
    }

    /// Releases the pins taken by [`MasterHandle::admit`] for a task the queue refused.
    async fn release_admission(&self, task_id: &TaskId, admitted: bool) {
        if let (true, Some(store)) = (admitted, self.blob_store.as_ref()) {
            store.unpin_task(task_id).await;
        }
    }

    /// Returns the shared cluster secret, if configured.
    pub fn cluster_secret(&self) -> Option<&ClusterSecret> {
        self.cluster_secret.as_ref()
    }

    /// Returns the active P2P ticket for this master instance, if P2P is enabled.
    pub fn p2p_ticket(&self) -> Option<&str> {
        self.p2p_ticket.as_deref()
    }
    /// Returns the bound dashboard socket address, if the dashboard is enabled.
    pub fn dashboard_addr(&self) -> Option<SocketAddr> {
        self.dashboard_addr
    }

    /// Returns the bound dashboard TCP port number, if the dashboard is enabled.
    pub fn dashboard_port(&self) -> Option<u16> {
        self.dashboard_addr.map(|a| a.port())
    }

    /// Whether unauthenticated local requests to the dashboard are trusted, and why. It is
    /// [`DevBypass::Off`] when there is no dashboard.
    pub fn dashboard_dev_bypass(&self) -> DevBypass {
        self.dashboard_dev_bypass
    }

    /// Returns a clone of the broadcast sender for telemetry events, if available.
    #[cfg(feature = "dashboard")]
    pub fn broadcast_tx(
        &self,
    ) -> Option<tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>> {
        self.broadcast_tx.clone()
    }

    /// Returns a reference to the active P2P endpoint, if enabled.
    #[cfg(feature = "p2p")]
    pub fn p2p_endpoint(&self) -> Option<&iroh::Endpoint> {
        self.p2p_endpoint.as_ref()
    }

    /// Returns the active P2P connection registry mapping worker IDs to connections.
    #[cfg(feature = "p2p")]
    pub fn p2p_connection_map(
        &self,
    ) -> Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>> {
        Arc::clone(&self.p2p_active_conns)
    }

    /// Queries live P2P path and latency telemetry for a connected worker.
    #[cfg(feature = "p2p")]
    pub async fn get_worker_p2p_info(
        &self,
        worker_id: &Uuid,
    ) -> Option<rusty_grid_core::transport::P2pPathInfo> {
        let guard = self.p2p_active_conns.read().await;
        guard
            .get(worker_id)
            .and_then(rusty_grid_core::transport::inspect_connection_paths)
    }

    /// Submits a task with default priority (0).
    pub async fn submit_task(&self, task: Task) -> GridResult<TaskId> {
        let task_id = task.id;
        let (admitted, needs_tree_v2) = self.admit(&task).await?;
        let id = match self
            .queue
            .submit_with_priority_and_tree_v2(task, 0, needs_tree_v2)
            .await
        {
            Ok(id) => id,
            Err(e) => {
                self.release_admission(&task_id, admitted).await;
                return Err(e);
            }
        };
        self.scheduler_notify.notify_one();
        #[cfg(feature = "dashboard")]
        if let Some(ref b_tx) = self.broadcast_tx {
            if b_tx.receiver_count() > 0 {
                if let Some(info) = self.queue.get_task(&id).await {
                    let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                        info,
                    ));
                }
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                    self.queue.stats().await,
                ));
            }
        }
        Ok(id)
    }

    /// Submits a task with explicit priority.
    pub async fn submit_task_with_priority(&self, task: Task, priority: u32) -> GridResult<TaskId> {
        let task_id = task.id;
        let (admitted, needs_tree_v2) = self.admit(&task).await?;
        let id = match self
            .queue
            .submit_with_priority_and_tree_v2(task, priority, needs_tree_v2)
            .await
        {
            Ok(id) => id,
            Err(e) => {
                self.release_admission(&task_id, admitted).await;
                return Err(e);
            }
        };
        self.scheduler_notify.notify_one();
        #[cfg(feature = "dashboard")]
        if let Some(ref b_tx) = self.broadcast_tx {
            if b_tx.receiver_count() > 0 {
                if let Some(info) = self.queue.get_task(&id).await {
                    let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                        info,
                    ));
                }
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                    self.queue.stats().await,
                ));
            }
        }
        Ok(id)
    }

    /// Queries current task lifecycle status.
    pub async fn get_task_status(&self, task_id: TaskId) -> GridResult<TaskStatus> {
        let state = self
            .queue
            .get_state(&task_id)
            .await
            .ok_or(GridError::TaskNotFound(task_id.0))?;
        Ok(state.into())
    }

    /// Queries detailed task operational metadata.
    pub async fn get_task_info(&self, task_id: TaskId) -> GridResult<TaskInfo> {
        self.queue
            .get_task(&task_id)
            .await
            .ok_or(GridError::TaskNotFound(task_id.0))
    }

    /// Retrieves the latest verified mid-task checkpoint for a task, if available.
    pub async fn get_checkpoint(&self, task_id: &TaskId) -> Option<CheckpointData> {
        self.queue.get_checkpoint(task_id).await
    }

    /// Lists all tasks currently managed by the Master.
    pub async fn list_tasks(&self) -> GridResult<Vec<TaskInfo>> {
        Ok(self.queue.list_tasks().await)
    }

    /// Cancels a queued or active task, telling its worker to stop and waking every waiter.
    /// A task that already finished is left as it is (`Ok`), an unknown id is `TaskNotFound`.
    pub async fn cancel_task(&self, task_id: TaskId) -> GridResult<()> {
        cancel_task_and_notify(
            &self.registry,
            &self.queue,
            &self.scheduler_notify,
            &self.waiters,
            task_id,
            "Cancelled by user",
        )
        .await?;

        #[cfg(feature = "dashboard")]
        if let Some(ref b_tx) = self.broadcast_tx {
            if b_tx.receiver_count() > 0 {
                if let Some(info) = self.queue.get_task(&task_id).await {
                    let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                        info,
                    ));
                }
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                    self.queue.stats().await,
                ));
            }
        }
        Ok(())
    }

    /// Asynchronously awaits task completion, failure, or cancellation.
    pub async fn wait_task(
        &self,
        task_id: TaskId,
        timeout: Option<Duration>,
    ) -> GridResult<TaskResult> {
        // Fast-path: is task already terminal?
        if self.queue.get_task(&task_id).await.is_none() {
            return Err(GridError::TaskNotFound(task_id.0));
        }
        if let Some(res) = terminal_result(&self.queue, &task_id, None).await {
            return Ok(res);
        }

        // Slow-path: register oneshot channel
        let (tx, rx) = oneshot::channel();
        {
            let mut lock = self.waiters.write().await;
            // Re-check under the waiter lock: a task finishing now takes this lock to wake waiters,
            // so it either sees our sender or has already published its result.
            if let Some(res) = terminal_result(&self.queue, &task_id, None).await {
                return Ok(res);
            }
            lock.entry(task_id).or_default().push(tx);
        }

        let wait_fut = async { rx.await.map_err(|_| GridError::ConnectionClosed) };

        if let Some(duration) = timeout {
            match tokio::time::timeout(duration, wait_fut).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    // Our receiver is gone now; do not leave its sender in the map until the task ends.
                    prune_closed_waiters(&self.waiters, &task_id).await;
                    Err(GridError::Timeout {
                        operation: format!("wait_task({task_id})"),
                        duration_secs: duration.as_secs(),
                    })
                }
            }
        } else {
            wait_fut.await
        }
    }

    /// Lists snapshots of all registered workers.
    pub async fn list_workers(&self) -> GridResult<Vec<WorkerInfo>> {
        Ok(self.registry.list_all_workers().await)
    }

    /// Returns aggregated queue statistics.
    pub async fn queue_stats(&self) -> GridResult<QueueStats> {
        Ok(self.queue.stats().await)
    }

    /// Returns the Master TCP listener socket address.
    pub fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    /// Returns the Master TCP port.
    pub fn port(&self) -> u16 {
        self.server_addr.port()
    }

    /// Returns a reference to the `WorkerRegistry`.
    pub fn registry(&self) -> &WorkerRegistry {
        &self.registry
    }

    /// Returns a reference to the `TaskQueue`.
    pub fn queue(&self) -> &TaskQueue {
        &self.queue
    }

    /// Returns a reference to the scheduler notification trigger.
    pub fn scheduler_notify(&self) -> &Arc<tokio::sync::Notify> {
        &self.scheduler_notify
    }

    /// Signals all Master subsystems to shut down.
    pub fn shutdown(&self) -> GridResult<()> {
        self.queue.close_and_compact_store_sync();
        if self.auth_mode.allows_members() && self.members.is_dirty() {
            if let Err(e) = self.members.flush_dirty() {
                warn!(
                    error = %e,
                    "Failed to flush dirty members.json on shutdown"
                );
            }
        }
        self.shutdown_tx
            .send(true)
            .map_err(|_| GridError::ConnectionClosed)
    }

    /// Executes an in-memory Map/Reduce job across cluster workers.
    pub async fn execute_mapreduce(&self, spec: MapReduceJobSpec) -> GridResult<MapReduceResult> {
        let engine = MapReduceEngine::new(
            self.queue.clone(),
            Arc::clone(&self.scheduler_notify),
            Arc::clone(&self.waiters),
        )
        .with_registry(self.registry.clone())
        .with_parallelism(self.mapreduce_parallelism);
        engine.execute(spec).await
    }
}

/// The dashboard URL a discovery beacon advertises: where the dashboard really listens, as a peer
/// on the network would reach it. `lan_ip` stands in for a wildcard bind address.
///
/// Empty when no peer can reach the dashboard (it is disabled, or bound to loopback, the default):
/// a URL that points nowhere would send the standby portal's visitors and the Android app's
/// "Open dashboard" button to a dead address, or to whatever else holds that port. Receivers read
/// an empty value as "this master has no dashboard for you".
fn advertised_web_ui_url(dashboard_addr: Option<SocketAddr>, lan_ip: std::net::IpAddr) -> String {
    let Some(addr) = dashboard_addr else {
        return String::new();
    };
    let bound = addr.ip().to_canonical();
    let host = if bound.is_unspecified() {
        lan_ip.to_canonical()
    } else {
        bound
    };
    if host.is_loopback() || host.is_unspecified() {
        return String::new();
    }
    format!("http://{}", SocketAddr::new(host, addr.port()))
}

/// Returns the UDP socket address the Master discovery service should bind for `listen_ip`.
///
/// When the master's TCP listener is bound to a loopback address (`127.x.x.x` or `::1`), the
/// discovery socket binds to that loopback address (`<listen_ip>:<discovery_port>`) rather than
/// `0.0.0.0:<discovery_port>` so a loopback master never opens an all-interfaces UDP listener
/// (which triggers a Windows Firewall prompt on desktop and test runs) while still answering local
/// unicast discovery probes. Non-loopback and unspecified listen addresses bind
/// `0.0.0.0:<discovery_port>`.
fn discovery_bind_addr(listen_ip: std::net::IpAddr, discovery_port: u16) -> SocketAddr {
    let bind_ip = if listen_ip.to_canonical().is_loopback() {
        listen_ip
    } else {
        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
    };
    SocketAddr::new(bind_ip, discovery_port)
}

/// Spawns the Master discovery UDP service that broadcasts periodic beacons
/// and answers incoming client discovery queries.
async fn spawn_master_discovery_service(
    listen_ip: std::net::IpAddr,
    cluster_port: u16,
    dashboard_addr: Option<SocketAddr>,
    discovery_port: u16,
    registry: WorkerRegistry,
    cluster_secret: Option<ClusterSecret>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let is_loopback = listen_ip.to_canonical().is_loopback();
    let local_ip = if is_loopback {
        listen_ip
    } else if listen_ip.is_unspecified() {
        rusty_grid_core::discovery::get_local_ip_or_loopback()
    } else {
        listen_ip
    };
    let cluster_addr = format!("{}:{}", local_ip, cluster_port);
    let web_ui_url = advertised_web_ui_url(
        dashboard_addr,
        rusty_grid_core::discovery::get_local_ip_or_loopback(),
    );
    let hostname = sysinfo::System::host_name().unwrap_or_else(|| "OxideMaster".to_string());

    let bind_addr = discovery_bind_addr(listen_ip, discovery_port);
    let socket = match tokio::net::UdpSocket::bind(bind_addr).await {
        Ok(s) => s,
        Err(e) => {
            warn!(port = discovery_port, error = %e, "Could not bind Master UDP discovery listener");
            return;
        }
    };
    if !is_loopback {
        let _ = socket.set_broadcast(true);
    }

    info!(
        bind_addr = %bind_addr,
        discovery_port = discovery_port,
        cluster_addr = %cluster_addr,
        web_ui_url = %web_ui_url,
        "Master UDP Discovery beacon & query responder active"
    );

    let broadcast_target = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::BROADCAST),
        discovery_port,
    );
    let subnet_target = SocketAddr::new(
        rusty_grid_core::discovery::get_subnet_broadcast_ip(),
        discovery_port,
    );
    let loopback_target = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        discovery_port,
    );
    let mut ticker = tokio::time::interval(Duration::from_millis(1500));
    let mut buf = [0u8; 1024];

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                // A loopback-bound socket cannot send broadcast beacons (and a loopback master is
                // only reachable locally via unicast probes). Likewise, an unsolicited broadcast
                // cannot carry a MAC bound to a probe nonce, and clients holding the secret ignore
                // unsigned beacons, so a master with a secret does not advertise itself to the
                // whole LAN: it answers authenticated probes only.
                if is_loopback || cluster_secret.is_some() {
                    continue;
                }
                let workers_count = registry.list_workers().await.len();
                let beacon = rusty_grid_core::discovery::MasterBeacon::new(
                    &cluster_addr,
                    &web_ui_url,
                    &hostname,
                    workers_count,
                );
                if let Ok(bytes) = beacon.to_bytes() {
                    let _ = socket.send_to(&bytes, broadcast_target).await;
                    if subnet_target != broadcast_target {
                        let _ = socket.send_to(&bytes, subnet_target).await;
                    }
                    let _ = socket.send_to(&bytes, loopback_target).await;
                }
            }
            res = socket.recv_from(&mut buf) => {
                if let Ok((len, from)) = res {
                    // Plain `OX_DISCOVER` probes get an unsigned beacon (so a secret-less client
                    // learns where to knock and then gets an actionable AuthRequired error);
                    // authenticated probes get a beacon signed with the cluster secret.
                    if rusty_grid_core::discovery::parse_probe(&buf[..len]).is_some() {
                        let workers_count = registry.list_workers().await.len();
                        let beacon = rusty_grid_core::discovery::MasterBeacon::new(
                            &cluster_addr,
                            &web_ui_url,
                            &hostname,
                            workers_count,
                        );
                        if let Some(bytes) = rusty_grid_core::discovery::beacon_reply(
                            &buf[..len],
                            beacon,
                            cluster_secret.as_ref(),
                        ) {
                            let _ = socket.send_to(&bytes, from).await;
                        }
                    }
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
