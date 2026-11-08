//! Master TCP server listening for worker connections, conducting registration handshakes,
//! managing ephemeral port file publication, dispatching bidirectional framed messaging,
//! and orchestrating task lifecycle and failover.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::AbortHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use rusty_grid_core::blob_transfer::{BlobReply, BlobRequest, MAX_IN_FLIGHT_REQUESTS};
use rusty_grid_core::capabilities::MobileLiveStatus;
use rusty_grid_core::cluster_auth::{
    send_reject, server_handshake, ClusterSecret, REJECT_NO_SECRET, WIRE_FORMAT_AUTH,
};
use rusty_grid_core::mapreduce::{MapReduceJobSpec, MapReduceResult};
use rusty_grid_core::protocol::{
    format_incompatible_version_error, ClientMessage, ClientResponse, EndpointRole, InboundMessage,
    ProtocolError,
};
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskResult, TaskStatus};
#[cfg(feature = "p2p")]
use rusty_grid_core::transport::{BiStream, GridStream, GRID_ALPN, STREAM_CONTROL, STREAM_DATA};
use rusty_grid_core::{
    GridError, GridResult, MasterMessage, MessageTransport, WireCodec, WorkerCapabilities,
    WorkerMessage,
};

use crate::auth::DevBypass;
use crate::blob_store::{BlobStore, BlobStoreConfig};
use crate::mapreduce::{MapReduceEngine, DEFAULT_MAPREDUCE_PARALLELISM};
use crate::queue::{
    QueueStats, ResultDisposition, TaskInfo, TaskQueue, TaskState, DEFAULT_MAX_ACTIVE_BYTES,
    DEFAULT_MAX_TERMINAL_BYTES, DEFAULT_MAX_TERMINAL_TASKS,
};
#[allow(unused_imports)]
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
        config: ServerConfig,
        registry: WorkerRegistry,
        queue: TaskQueue,
        scheduler_notify: Arc<tokio::sync::Notify>,
        waiters: WaiterMap,
    ) -> GridResult<Self> {
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
            let mut ep_builder = if config.p2p_minimal_preset {
                iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            } else {
                iroh::Endpoint::builder(iroh::endpoint::presets::N0)
            };
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
        Self::spawn_with_config(config, SchedulerConfig::default(), ReaperConfig::default()).await
    }

    /// Spawns full Master stack with custom scheduler and reaper configurations.
    pub async fn spawn_with_config(
        config: ServerConfig,
        sched_config: SchedulerConfig,
        reaper_config: ReaperConfig,
    ) -> GridResult<MasterHandle> {
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

        // 1. Spawn TCP server accept loop
        let srv_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = server.run(srv_shutdown_rx).await {
                error!(error = %e, "Master server accept loop terminated with error");
            }
        });

        // 2. Spawn Background Scheduler loop
        let sched = Arc::new(
            WorkloadScheduler::new(
                sched_config,
                registry.clone(),
                queue.clone(),
                scheduler_notify.clone(),
            )
            .with_waiters(Arc::clone(&waiters)),
        );
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

            loop {
                tokio::select! {
                    _ = sched_trigger.notified() => {
                        debug!("Scheduler loop woken by event notification");
                        if sched.config().micro_batch_window > Duration::ZERO {
                            tokio::time::sleep(sched.config().micro_batch_window).await;
                        }
                        if let Ok(report) = sched.schedule_once().await {
                            let _ = &report;
                            #[cfg(feature = "dashboard")]
                            if let Some(ref b_tx) = sched_broadcast_tx {
                                if !report.assignments.is_empty() || !report.unfulfillable.is_empty() {
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
                    _ = ticker.tick() => {
                        if let Ok(report) = sched.schedule_once().await {
                            let _ = &report;
                            #[cfg(feature = "dashboard")]
                            if let Some(ref b_tx) = sched_broadcast_tx {
                                if !report.assignments.is_empty() || !report.unfulfillable.is_empty() {
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
            blob_store: blob_store.clone(),
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
            let p2p_limits = limits.clone();
            let p2p_blob_store = self.blob_store.clone();

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

        Ok(())
    }
}

/// Dashboard broadcast channel type, so signatures stay readable.
#[cfg(feature = "dashboard")]
pub(crate) type DashboardTx =
    tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>;

/// Removes waiters of `task_id` whose receiving side has gone away (timed-out or abandoned
/// waits), so they do not accumulate until the task finishes. Live waiters are kept.
pub(crate) async fn prune_closed_waiters(waiters: &WaiterMap, task_id: &TaskId) {
    let mut lock = waiters.write().await;
    if let Some(senders) = lock.get_mut(task_id) {
        senders.retain(|tx| !tx.is_closed());
        if senders.is_empty() {
            lock.remove(task_id);
        }
    }
}

/// The result to hand to clients of `task_id` if it has reached a terminal state, else `None`.
/// Falls back to a synthesised failure when a terminal task has no recorded result.
async fn terminal_result(
    queue: &TaskQueue,
    task_id: &TaskId,
    fallback_worker: Option<Uuid>,
) -> Option<TaskResult> {
    let info = queue.get_task(task_id).await?;
    if !info.state.is_terminal() {
        return None;
    }
    Some(match queue.get_result(task_id).await {
        Some(result) => result,
        None => TaskResult::failure(
            info.assigned_worker_id
                .or(fallback_worker)
                .unwrap_or_else(Uuid::nil),
            *task_id,
            if info.state == TaskState::Cancelled {
                130
            } else {
                1
            },
            "",
            "",
            0,
            info.error_message
                .clone()
                .or_else(|| Some("Task terminated".into())),
        ),
    })
}

/// Wakes every client waiting on one of `task_ids` that has reached a terminal state (for example
/// a task that ran out of failovers). Tasks that are still alive keep their waiters.
pub(crate) async fn resolve_terminal_waiters(
    queue: &TaskQueue,
    waiters: &WaiterMap,
    task_ids: &[TaskId],
    fallback_worker: Option<Uuid>,
) {
    for task_id in task_ids {
        if let Some(result) = terminal_result(queue, task_id, fallback_worker).await {
            let mut lock = waiters.write().await;
            if let Some(senders) = lock.remove(task_id) {
                for tx in senders {
                    let _ = tx.send(result.clone());
                }
            }
        }
    }
}

/// Cancels a task, tells the worker executing it to stop, and wakes everybody waiting on it with
/// the cancelled result. Shared by the client protocol and `MasterHandle::cancel_task`.
///
/// Returns `Ok(true)` if the task was cancelled now, `Ok(false)` if it had already finished and
/// was left alone, and `TaskNotFound` for an unknown id.
pub(crate) async fn cancel_task_and_notify(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    task_id: TaskId,
    reason: &str,
) -> GridResult<bool> {
    let outcome = queue
        .cancel_task_detailed(&task_id, Some(reason.to_string()))
        .await?;
    if !outcome.newly_cancelled {
        return Ok(false);
    }

    if let Some(worker_id) = outcome.worker_to_notify {
        let msg = MasterMessage::CancelTask {
            task_id,
            reason: Some(reason.to_string()),
        };
        if let Err(e) = registry.send_to_worker(&worker_id, msg).await {
            debug!(task_id = %task_id, worker_id = %worker_id, error = %e, "Could not deliver CancelTask to worker");
        }
    }

    let result = outcome.result.unwrap_or_else(|| {
        TaskResult::failure(
            outcome.worker_to_notify.unwrap_or_else(Uuid::nil),
            task_id,
            130,
            "",
            "",
            0,
            Some(reason.to_string()),
        )
    });
    let mut lock = waiters.write().await;
    if let Some(senders) = lock.remove(&task_id) {
        for tx in senders {
            let _ = tx.send(result.clone());
        }
    }
    drop(lock);

    scheduler_notify.notify_one();
    Ok(true)
}

/// The reason a request that needs the blob store fails on a master that runs without one.
const NO_BLOB_STORE: &str = "this master has no blob store, so it cannot move task input or \
     output files; start it with `oxideswarm master` (which keeps one under ~/.oxideswarm/blobs) \
     or `oxideswarm master --blob-dir DIR`";

/// How often the blob store sweeper releases the pins of finished tasks.
const BLOB_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Admits a task that moves files through the blob store: every input blob (and every file of
/// every input tree) must already be in the store, and they are pinned there until the task
/// finishes, so the bound cannot evict them before a worker fetched them. Tasks without inputs
/// or output patterns pass untouched.
async fn admit_blob_task(blob_store: Option<&Arc<BlobStore>>, task: &Task) -> Result<(), String> {
    if !task.uses_blobs() {
        return Ok(());
    }
    let Some(store) = blob_store else {
        return Err(format!(
            "Task {} has input or output files, but {NO_BLOB_STORE}",
            task.id
        ));
    };
    task.validate()?;
    let ids = store.resolve_inputs(&task.inputs).await?;
    if ids.is_empty() {
        return Ok(());
    }
    let gone = store.pin_task(task.id, ids);
    if let Some(id) = gone.first() {
        store.unpin_task(&task.id).await;
        return Err(format!(
            "input blob {id} was evicted from the master's blob store while the task was being \
             submitted; upload the inputs again (or raise `master --blob-store-mb`)"
        ));
    }
    Ok(())
}

/// Checks that a result's output files are in the store and pins them atomically if present,
/// returning either the pin guard or an error naming the first missing output.
fn pin_result_outputs(
    blob_store: Option<&Arc<BlobStore>>,
    result: &TaskResult,
) -> Result<Option<crate::blob_store::OutputPinGuard>, String> {
    if result.outputs.is_empty() {
        return Ok(None);
    }
    let Some(store) = blob_store else {
        return Err(format!(
            "the worker reported {} output files, but {NO_BLOB_STORE}",
            result.outputs.len()
        ));
    };
    let ids: Vec<_> = result.outputs.iter().map(|f| f.id).collect();
    match store.check_and_pin_outputs(&ids) {
        Ok(pin) => Ok(Some(pin)),
        Err(missing) => {
            let first = result
                .outputs
                .iter()
                .find(|f| missing.contains(&f.id))
                .expect("missing id comes from result.outputs");
            Err(format!(
                "output '{}' (blob {}) is not in the master's blob store ({} of {} outputs missing)",
                first.path,
                first.id,
                missing.len(),
                result.outputs.len()
            ))
        }
    }
}

/// Drops the unfinished uploads of one connection when the connection ends.
struct UploadOwner {
    store: Arc<BlobStore>,
    owner: u64,
}

impl UploadOwner {
    fn new(store: Arc<BlobStore>) -> Self {
        let owner = store.new_owner();
        Self { store, owner }
    }
}

impl Drop for UploadOwner {
    fn drop(&mut self) {
        self.store.abort_uploads(self.owner);
    }
}

/// Answers one worker connection's `BlobRequest`s in arrival order, off its inbound loop. A
/// worker caps its requests in flight per session at [`MAX_IN_FLIGHT_REQUESTS`], smaller than
/// this queue, so a cooperative worker never fills the queue and blocks its own heartbeats or
/// results; the bounded channel still caps buffered requests if a peer ignores that limit.
pub(crate) struct BlobService {
    requests: mpsc::Sender<(u64, BlobRequest)>,
    _task: AbortOnDrop,
}

impl BlobService {
    pub(crate) fn spawn(store: Arc<BlobStore>, replies: mpsc::Sender<MasterMessage>) -> Self {
        let (requests, mut incoming) =
            mpsc::channel::<(u64, BlobRequest)>(MAX_IN_FLIGHT_REQUESTS * 2);
        let task = tokio::spawn(async move {
            let uploads = UploadOwner::new(Arc::clone(&store));
            while let Some((request_id, request)) = incoming.recv().await {
                let reply = store.handle(uploads.owner, request).await;
                if replies
                    .send(MasterMessage::BlobReply { request_id, reply })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Self {
            requests,
            _task: AbortOnDrop(task.abort_handle()),
        }
    }
}

/// Hands a worker's `BlobRequest` to its connection's [`BlobService`], or answers it with a
/// refusal on a master without a blob store.
pub(crate) async fn serve_worker_blob_request(
    service: Option<&BlobService>,
    replies: &mpsc::Sender<MasterMessage>,
    request_id: u64,
    request: BlobRequest,
) {
    match service {
        Some(service) => {
            let _ = service.requests.send((request_id, request)).await;
        }
        None => {
            let reply = BlobReply::Failed {
                message: NO_BLOB_STORE.to_string(),
            };
            let _ = replies
                .send(MasterMessage::BlobReply { request_id, reply })
                .await;
        }
    }
}

/// The `RegisterAck` message announcing the optional features of this master.
pub(crate) fn feature_announcement(
    blob_store: Option<&Arc<BlobStore>>,
    session_resume_hold_secs: u64,
) -> String {
    use rusty_grid_core::protocol::master_features;
    let mut features = master_features::SUPPORTED.to_vec();
    if blob_store.is_some() {
        features.push(master_features::BLOBS);
    }
    let session_resume_token;
    if session_resume_hold_secs > 0 {
        session_resume_token = master_features::session_resume_feature(session_resume_hold_secs);
        features.push(&session_resume_token);
    }
    master_features::announcement(&features)
}

/// Releases the input blob pins of tasks that finished without a result that released them
/// (failover out of retries, the reaper, a cancellation), releases terminal output pins once
/// their retention period has elapsed, and drops uploads that stalled.
fn spawn_blob_store_sweeper(
    store: Arc<BlobStore>,
    queue: TaskQueue,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let sweep_interval = BLOB_SWEEP_INTERVAL
        .min(store.output_pin_retention())
        .max(Duration::from_millis(20));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(sweep_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    store.sweep_stale_uploads();
                    store.sweep_expired_output_pins().await;
                    for (task_id, pinned_at) in store.pinned_tasks() {
                        let release = match queue.get_task(&task_id).await {
                            Some(info) => info.state.is_terminal(),
                            // Pinned just before it entered the queue, or gone long ago.
                            None => pinned_at.elapsed() > BLOB_SWEEP_INTERVAL,
                        };
                        if release {
                            store.unpin_task(&task_id).await;
                        }
                    }
                }
                res = shutdown_rx.changed() => {
                    if res.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });
}

/// Handles incoming `WorkerMessage::TaskResult` messages.
///
/// The sender is taken from `result.worker_id`; the connection handlers attribute results to the
/// authenticated session instead (see `handle_session_task_message`).
pub async fn handle_task_result(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    result: TaskResult,
) {
    let worker_id = result.worker_id;
    apply_task_result(
        registry,
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        None,
        result,
        None,
        #[cfg(feature = "dashboard")]
        None,
    )
    .await;
}

/// Handles incoming `WorkerMessage::TaskResult` messages with optional dashboard telemetry broadcast.
#[cfg(feature = "dashboard")]
pub async fn handle_task_result_with_broadcast(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    result: TaskResult,
    broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
) {
    let worker_id = result.worker_id;
    apply_task_result(
        registry,
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        None,
        result,
        None,
        broadcast_tx,
    )
    .await;
}

/// Applies one worker result: records it in the queue FSM on behalf of the sending connection
/// `(worker_id, session_id)`, then does the bookkeeping that follows from what the queue decided.
///
/// A result from a worker or session that the task is not currently assigned to (reaped, failed
/// over, superseded, or a peer that never had the task) changes nothing: no FSM transition, no
/// waiter wake-up, no capacity accounting.
#[allow(clippy::too_many_arguments)]
async fn apply_task_result(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    worker_id: Uuid,
    session_id: Option<u64>,
    result: TaskResult,
    blob_store: Option<&Arc<BlobStore>>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    apply_task_result_inner(
        registry,
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        session_id,
        result,
        blob_store,
        false,
        #[cfg(feature = "dashboard")]
        broadcast_tx,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn apply_task_result_inner(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    worker_id: Uuid,
    session_id: Option<u64>,
    mut result: TaskResult,
    blob_store: Option<&Arc<BlobStore>>,
    spawn_waiter_ack: bool,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    let task_id = result.task_id;
    if result.worker_id != worker_id {
        warn!(
            worker_id = %worker_id,
            claimed = %result.worker_id,
            task_id = %task_id,
            "Result carries a worker_id that is not the sending connection; attributing it to the connection"
        );
        result.worker_id = worker_id;
    }

    info!(
        worker_id = %worker_id,
        task_id = %task_id,
        exit_code = result.exit_code,
        "Task execution result received"
    );

    // Output files a client cannot download would turn a success into a silent loss; check and
    // pin them atomically so no upload can evict them before the task outcome is recorded.
    let output_pin = match pin_result_outputs(blob_store, &result) {
        Ok(pin) => pin,
        Err(problem) => {
            warn!(task_id = %task_id, problem = %problem, "Result refers to output files the master does not hold");
            result.error = Some(match result.error.take() {
                Some(error) => format!("{error}; {problem}"),
                None => problem,
            });
            None
        }
    };

    // 1. Record outcome in TaskQueue FSM and enqueue its journal record without blocking the
    //    worker session loop on the 5 ms group-commit window.
    let (outcome, durable_ack) = match queue
        .record_result_from_enqueued(worker_id, session_id, result.clone())
        .await
    {
        Ok(pair) => pair,
        Err(e) => {
            warn!(task_id = %task_id, error = %e, "Failed to record task result in queue");
            return;
        }
    };

    if outcome.disposition == ResultDisposition::Stale {
        debug!(
            worker_id = %worker_id,
            task_id = %task_id,
            state = ?outcome.state,
            "Ignored result from a worker/session that does not own the task"
        );
        return;
    }

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        if let Some(info) = queue.get_task(&task_id).await {
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                info,
            ));
        }
        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
            queue.stats().await,
        ));
    }

    // 2. Release the worker's slot immediately and keep its circuit breaker current
    if !outcome.adopted_from_hold {
        let _ = registry.decrement_active_tasks(&worker_id).await;
    }
    if outcome.rejected {
        let _ = registry.record_rejection(&worker_id, session_id).await;
    } else if outcome.disposition != ResultDisposition::Late {
        registry.clear_rejections(&worker_id).await;
    }
    let send_ack = queue.session_resume_hold_secs() > 0;
    let ack_token = rusty_grid_core::protocol::master_features::result_ack_token(&result);

    // 3. Notify awaiting clients, but only once the task is really finished: a requeued
    //    task (rejection, execution retry) will still produce its real result and keeps its
    //    input pins. When terminal, input pins are released and output pins stay held for
    //    `output_pin_retention` so the client can download them.
    if outcome.disposition == ResultDisposition::Terminal {
        if let Some(store) = blob_store {
            store.unpin_task(&task_id).await;
        }
        if let Some(pin) = output_pin {
            pin.commit_terminal(task_id).await;
        }
        if spawn_waiter_ack {
            if let Some(ack) = durable_ack {
                let waiters = Arc::clone(waiters);
                let ack_registry = registry.clone();
                tokio::spawn(async move {
                    if let Err(err) = ack.await {
                        warn!(
                            task_id = %task_id.0,
                            error = %err,
                            "Failed to sync TaskTerminal journal record for task result"
                        );
                    }
                    if send_ack {
                        ack_registry
                            .send_result_ack(&worker_id, session_id, task_id, ack_token)
                            .await;
                    }
                    let mut lock = waiters.write().await;
                    if let Some(senders) = lock.remove(&task_id) {
                        for tx in senders {
                            let _ = tx.send(result.clone());
                        }
                    }
                });
            } else {
                if send_ack {
                    registry
                        .send_result_ack(&worker_id, session_id, task_id, ack_token)
                        .await;
                }
                let mut lock = waiters.write().await;
                if let Some(senders) = lock.remove(&task_id) {
                    for tx in senders {
                        let _ = tx.send(result.clone());
                    }
                }
            }
        } else {
            if let Some(ack) = durable_ack {
                if let Err(err) = ack.await {
                    warn!(
                        task_id = %task_id.0,
                        error = %err,
                        "Failed to sync TaskTerminal journal record for task result"
                    );
                }
            }
            if send_ack {
                registry
                    .send_result_ack(&worker_id, session_id, task_id, ack_token)
                    .await;
            }
            let mut lock = waiters.write().await;
            if let Some(senders) = lock.remove(&task_id) {
                for tx in senders {
                    let _ = tx.send(result.clone());
                }
            }
        }
    } else if send_ack {
        if let Some(ack) = durable_ack {
            if let Err(err) = ack.await {
                warn!(
                    task_id = %task_id.0,
                    error = %err,
                    "Failed to sync journal record for non-terminal task result"
                );
            }
        }
        registry
            .send_result_ack(&worker_id, session_id, task_id, ack_token)
            .await;
    }

    // 4. Wake scheduler loop to assign pending tasks
    scheduler_notify.notify_one();
}

/// Fails only the task when an outbound `MasterMessage::AssignTask` or
/// `AssignTaskWithCheckpoint` exceeds `MAX_FRAME_SIZE`, keeping the worker session alive so
/// unrelated tasks on the worker can continue.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_oversized_master_message(
    msg: MasterMessage,
    size: usize,
    max: usize,
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    worker_id: Uuid,
    session_id: u64,
    blob_store: Option<&Arc<BlobStore>>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    let task_id = match msg {
        MasterMessage::AssignTask { task }
        | MasterMessage::AssignTaskWithCheckpoint { task, .. } => task.id,
        other => {
            warn!(
                worker_id = %worker_id,
                session_id = session_id,
                variant = %other.variant_name(),
                size = size,
                max = max,
                "Outbound master message exceeds frame limit; dropping frame without closing session"
            );
            return;
        }
    };

    let reason = format!(
        "Task assignment frame ({size} bytes) exceeds maximum protocol frame limit ({max} bytes)"
    );
    warn!(
        worker_id = %worker_id,
        session_id = session_id,
        task_id = %task_id,
        size = size,
        max = max,
        "Refusing oversized task assignment without terminating worker session"
    );
    let result = TaskResult::failure(
        worker_id,
        task_id,
        1,
        rusty_grid_core::task::Bytes::new(),
        format!("[rusty_grid: {reason}]\n").into_bytes(),
        0,
        Some(reason),
    );

    let disposition = match queue
        .fail_unsendable_assignment(worker_id, Some(session_id), result.clone())
        .await
    {
        Ok(d) => d,
        Err(e) => {
            warn!(task_id = %task_id, error = %e, "Failed to record oversized assignment failure");
            return;
        }
    };

    if disposition == ResultDisposition::Stale {
        return;
    }

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        if let Some(info) = queue.get_task(&task_id).await {
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                info,
            ));
        }
        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
            queue.stats().await,
        ));
    }

    let _ = registry.decrement_active_tasks(&worker_id).await;

    if disposition == ResultDisposition::Terminal {
        if let Some(store) = blob_store {
            store.unpin_task(&task_id).await;
        }
        let mut lock = waiters.write().await;
        if let Some(senders) = lock.remove(&task_id) {
            for tx in senders {
                let _ = tx.send(result.clone());
            }
        }
    }

    scheduler_notify.notify_one();
}

/// Sends `resp` to a client, converting `ProtocolError::FrameTooLarge` into a
/// `ClientResponse::Error` frame instead of dropping the connection with no explanation.
async fn send_client_response<S: AsyncRead + AsyncWrite + Unpin + Send>(
    transport: &mut MessageTransport<S>,
    resp: &ClientResponse,
) -> Result<(), rusty_grid_core::protocol::ProtocolError> {
    match transport.send_msg(resp).await {
        Ok(()) => Ok(()),
        Err(rusty_grid_core::protocol::ProtocolError::FrameTooLarge { size, max }) => {
            let message = match resp {
                ClientResponse::TaskCompleted { task_id, .. } => format!(
                    "Task {task_id} result frame ({size} bytes) exceeds maximum protocol frame limit ({max} bytes); write large outputs to files and retrieve them with `--output`"
                ),
                _ => format!(
                    "Master response frame ({size} bytes) exceeds maximum protocol frame limit ({max} bytes)"
                ),
            };
            warn!(
                size = size,
                max = max,
                "Client response exceeds frame limit; returning ClientResponse::Error"
            );
            transport.send_msg(&ClientResponse::Error { message }).await
        }
        Err(e) => Err(e),
    }
}

/// Handles incoming `WorkerMessage::TaskProgress` messages.
pub async fn handle_task_progress(
    queue: &TaskQueue,
    worker_id: Uuid,
    task_id: TaskId,
    status: TaskStatus,
) {
    apply_task_progress(
        queue,
        worker_id,
        None,
        task_id,
        status,
        #[cfg(feature = "dashboard")]
        None,
    )
    .await;
}

/// Handles incoming `WorkerMessage::TaskProgress` messages with optional dashboard telemetry broadcast.
#[cfg(feature = "dashboard")]
pub async fn handle_task_progress_with_broadcast(
    queue: &TaskQueue,
    worker_id: Uuid,
    task_id: TaskId,
    status: TaskStatus,
    broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
) {
    apply_task_progress(queue, worker_id, None, task_id, status, broadcast_tx).await;
}

/// Applies a progress report from connection `(worker_id, session_id)`. Only the worker the task
/// is assigned to can move it to `Running`; anything else is ignored.
async fn apply_task_progress(
    queue: &TaskQueue,
    worker_id: Uuid,
    session_id: Option<u64>,
    task_id: TaskId,
    status: TaskStatus,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    debug!(worker_id = %worker_id, task_id = %task_id, ?status, "Task progress update");
    if status != TaskStatus::Running {
        return;
    }
    if let Err(e) = queue
        .mark_running_from(worker_id, session_id, &task_id)
        .await
    {
        debug!(worker_id = %worker_id, task_id = %task_id, error = %e, "Ignoring progress update");
        return;
    }
    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        if let Some(info) = queue.get_task(&task_id).await {
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                info,
            ));
        }
        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
            queue.stats().await,
        ));
    }
}

/// Applies a `WorkerMessage::MobileTelemetry` report received on the authenticated connection
/// `(worker_id, session_id)` to the registry, so battery / thermal gating uses the live value.
///
/// As with every other message, the identity comes from the connection: a report naming another
/// worker is logged and applied to the sender only. A change wakes the scheduler, because a phone
/// that just recovered (plugged in, cooled down) may now take queued heavy work.
pub(crate) async fn apply_mobile_telemetry(
    registry: &WorkerRegistry,
    scheduler_notify: &Arc<tokio::sync::Notify>,
    worker_id: Uuid,
    session_id: u64,
    claimed: Uuid,
    status: MobileLiveStatus,
) {
    if claimed != worker_id {
        warn!(worker_id = %worker_id, claimed = %claimed, "Mobile telemetry names a different worker than its connection");
    }
    let bat_pct = status.battery_pct;
    let is_chg = status.is_charging;
    let th_thr = status.thermal_throttled;
    match registry
        .update_mobile_telemetry(&worker_id, Some(session_id), status)
        .await
    {
        Ok(true) => {
            debug!(
                worker_id = %worker_id,
                battery_pct = ?bat_pct,
                is_charging = ?is_chg,
                thermal_throttled = th_thr,
                "Worker mobile telemetry updated"
            );
            scheduler_notify.notify_one();
        }
        Ok(false) => {}
        Err(e) => {
            debug!(worker_id = %worker_id, error = %e, "Mobile telemetry for an unknown worker ignored")
        }
    }
}

/// Processes a task-related message (`TaskProgress`, `TaskResult`, `Checkpoint`) received on the
/// authenticated connection `(worker_id, session_id)`.
///
/// The identity comes from the connection. A `worker_id` inside the message body is never trusted:
/// otherwise any registered worker could complete, fail or poison a task it does not own.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_session_task_message(
    msg: WorkerMessage,
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &Arc<tokio::sync::Notify>,
    waiters: &WaiterMap,
    worker_id: Uuid,
    session_id: u64,
    blob_store: Option<&Arc<BlobStore>>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    match msg {
        WorkerMessage::TaskProgress {
            worker_id: claimed,
            task_id,
            status,
        } => {
            if claimed != worker_id {
                warn!(worker_id = %worker_id, claimed = %claimed, task_id = %task_id, "Progress report names a different worker than its connection");
            }
            apply_task_progress(
                queue,
                worker_id,
                Some(session_id),
                task_id,
                status,
                #[cfg(feature = "dashboard")]
                broadcast_tx,
            )
            .await;
        }
        WorkerMessage::TaskResult {
            worker_id: claimed,
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
            if claimed != worker_id {
                warn!(worker_id = %worker_id, claimed = %claimed, task_id = %task_id, "Result names a different worker than its connection");
            }
            let result = TaskResult {
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
            };
            // Once a result starts being applied it must finish, even if this connection is
            // aborted meanwhile (reaper, or the worker re-registering): the queue transition and
            // the waiter wake-up are one logical step, and cutting them apart would leave a
            // client waiting forever on a task that is already finished.
            let registry = registry.clone();
            let queue = queue.clone();
            let scheduler_notify = Arc::clone(scheduler_notify);
            let waiters = Arc::clone(waiters);
            let blob_store = blob_store.cloned();
            #[cfg(feature = "dashboard")]
            let broadcast_tx = broadcast_tx.cloned();
            let _ = tokio::spawn(async move {
                apply_task_result_inner(
                    &registry,
                    &queue,
                    &scheduler_notify,
                    &waiters,
                    worker_id,
                    Some(session_id),
                    result,
                    blob_store.as_ref(),
                    true,
                    #[cfg(feature = "dashboard")]
                    broadcast_tx.as_ref(),
                )
                .await;
            })
            .await;
        }
        WorkerMessage::Checkpoint {
            task_id,
            sequence,
            delta_state,
        } => {
            let checkpoint = CheckpointData::new(sequence, delta_state.into_bytes());
            match queue
                .record_checkpoint_from(worker_id, Some(session_id), task_id, checkpoint)
                .await
            {
                Ok(true) => {
                    debug!(worker_id = %worker_id, task_id = %task_id, sequence = sequence, "Recorded mid-task checkpoint");
                }
                Ok(false) => {
                    debug!(worker_id = %worker_id, task_id = %task_id, sequence = sequence, "Dropped checkpoint (stale, foreign or not newer)");
                }
                Err(e) => {
                    warn!(worker_id = %worker_id, task_id = %task_id, error = %e, "Failed to record mid-task checkpoint");
                }
            }
        }
        other => {
            warn!(worker_id = %worker_id, message = %other.variant_name(), "Unexpected message routed as task message");
        }
    }
}

/// Handles fast-path worker disconnection (TCP EOF, socket error, or graceful Disconnecting message).
pub async fn handle_worker_disconnect(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    immediate_reschedule: bool,
) {
    handle_worker_disconnect_internal(
        registry,
        queue,
        scheduler_notify,
        None,
        worker_id,
        session_id,
        reason,
        immediate_reschedule,
    )
    .await;
}

/// Handles worker disconnection and notifies active waiters for any tasks that reached terminal state.
#[allow(clippy::too_many_arguments)]
pub async fn handle_worker_disconnect_with_waiters(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    immediate_reschedule: bool,
) {
    handle_worker_disconnect_internal(
        registry,
        queue,
        scheduler_notify,
        Some(waiters),
        worker_id,
        session_id,
        reason,
        immediate_reschedule,
    )
    .await;
}

/// Disconnect worker and notify broadcast subscribers
#[allow(clippy::too_many_arguments)]
pub async fn handle_worker_disconnect_with_broadcast(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: Option<&WaiterMap>,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    immediate_reschedule: bool,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
) {
    let unregistered = registry
        .unregister(worker_id, session_id)
        .await
        .unwrap_or(false);
    if !unregistered {
        debug!(worker_id = %worker_id, "Worker unregister skipped (stale session or already handled)");
        return;
    }

    warn!(
        worker_id = %worker_id,
        reason = %reason,
        immediate = immediate_reschedule,
        "Worker disconnected; executing failover sweep"
    );

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        let _ = b_tx.send(
            crate::dashboard::dto::DashboardStreamMessage::WorkerDisconnected {
                worker_id: *worker_id,
                reason: reason.to_string(),
            },
        );
    }

    failover_session_tasks(
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        session_id,
        reason,
        immediate_reschedule,
        #[cfg(feature = "dashboard")]
        broadcast_tx,
    )
    .await;
}

/// Unregisters a worker session that announced a graceful drain (`WorkerMessage::Disconnecting`),
/// returns its in-flight tasks to `Queued` without charging `retry_count` or `failover_count`
/// while under `max_evacuations` (or takes the charged failover path past that cap), resolves
/// terminal waiters if any task reached `Failed`, notifies dashboard subscribers, and pokes the
/// scheduler (`MOB-3`).
#[allow(clippy::too_many_arguments)]
pub async fn handle_worker_evacuate_with_broadcast(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: Option<&WaiterMap>,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
) {
    let unregistered = registry
        .unregister(worker_id, session_id)
        .await
        .unwrap_or(false);
    if !unregistered {
        debug!(worker_id = %worker_id, "Worker drain unregister skipped (stale session or already handled)");
        return;
    }

    info!(
        worker_id = %worker_id,
        reason = %reason,
        "Worker announced drain; evacuating in-flight tasks"
    );

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        let _ = b_tx.send(
            crate::dashboard::dto::DashboardStreamMessage::WorkerDisconnected {
                worker_id: *worker_id,
                reason: reason.to_string(),
            },
        );
    }

    let affected_tasks = queue
        .handle_session_evacuated(worker_id, session_id, reason)
        .await;
    if affected_tasks.is_empty() {
        return;
    }

    info!(
        worker_id = %worker_id,
        count = affected_tasks.len(),
        tasks = ?affected_tasks,
        "Processed evacuated tasks from draining worker"
    );

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        for task_id in &affected_tasks {
            if let Some(info) = queue.get_task(task_id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                    info,
                ));
            }
        }
        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
            queue.stats().await,
        ));
    }

    if let Some(waiters) = waiters {
        resolve_terminal_waiters(queue, waiters, &affected_tasks, Some(*worker_id)).await;
    }

    scheduler_notify.notify_one();
}

/// Fails over the in-flight tasks of one worker session: re-enqueues those with budget left,
/// fails the rest, wakes the clients of tasks that became terminal and pokes the scheduler.
/// With `session_id = Some(s)` only tasks dispatched on that session are touched.
#[allow(clippy::too_many_arguments)]
async fn failover_session_tasks(
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: Option<&WaiterMap>,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    immediate_reschedule: bool,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    let affected_tasks = queue
        .handle_session_disconnected(worker_id, session_id, reason, immediate_reschedule)
        .await;
    if affected_tasks.is_empty() {
        return;
    }

    info!(
        worker_id = %worker_id,
        count = affected_tasks.len(),
        tasks = ?affected_tasks,
        "Re-enqueued orphaned tasks for reassignment"
    );

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        for task_id in &affected_tasks {
            if let Some(info) = queue.get_task(task_id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                    info,
                ));
            }
        }
        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
            queue.stats().await,
        ));
    }

    // Resolve waiters for any tasks that reached terminal state (e.g. retries exhausted)
    if let Some(waiters) = waiters {
        resolve_terminal_waiters(queue, waiters, &affected_tasks, Some(*worker_id)).await;
    }

    scheduler_notify.notify_one();
}

/// Fails over the tasks `old_session` of `worker_id` still holds, right after the worker id
/// registered again as `new_session`.
///
/// The caller is the new session's connection handler, and the registry already holds its abort
/// handle: a third registration of the same worker id aborts it at its first contended await. The
/// sweep therefore runs in a task of its own that the caller merely awaits (as for task results),
/// so it finishes even when the handler is cancelled meanwhile. If it were cancelled before it
/// ran, the next sweep of this worker still recovers the tasks, because a sweep of session `s`
/// takes every older session along (see `TaskQueue::handle_session_disconnected`).
pub(crate) async fn failover_superseded_session(
    queue: &TaskQueue,
    scheduler_notify: &Arc<tokio::sync::Notify>,
    waiters: &WaiterMap,
    worker_id: Uuid,
    old_session: u64,
    new_session: u64,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    warn!(worker_id = %worker_id, old_session = old_session, new_session = new_session, "Worker re-registered while its previous session was live; failing over the superseded session");
    let queue = queue.clone();
    let scheduler_notify = Arc::clone(scheduler_notify);
    let waiters = Arc::clone(waiters);
    #[cfg(feature = "dashboard")]
    let broadcast_tx = broadcast_tx.cloned();
    let _ = tokio::spawn(async move {
        failover_session_tasks(
            &queue,
            &scheduler_notify,
            Some(&waiters),
            &worker_id,
            Some(old_session),
            "Superseded by a new session of the same worker",
            true,
            #[cfg(feature = "dashboard")]
            broadcast_tx.as_ref(),
        )
        .await;
    })
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_worker_disconnect_internal(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: Option<&WaiterMap>,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    immediate_reschedule: bool,
) {
    handle_worker_disconnect_with_broadcast(
        registry,
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        session_id,
        reason,
        immediate_reschedule,
        #[cfg(feature = "dashboard")]
        None,
    )
    .await;
}

/// Answers `GetTaskStatus` (and the non-terminal half of `GetTaskResult`).
async fn task_status_response(queue: &TaskQueue, task_id: TaskId) -> ClientResponse {
    match queue.get_task(&task_id).await {
        Some(info) => {
            let (status, err) = match queue.get_result(&task_id).await {
                Some(res) => {
                    if res.exit_code == 0 {
                        (TaskStatus::Completed, res.error)
                    } else if res.exit_code == 130 {
                        (TaskStatus::Cancelled, res.error)
                    } else {
                        (TaskStatus::Failed, res.error)
                    }
                }
                None => match info.state {
                    TaskState::Submitted
                    | TaskState::Queued
                    | TaskState::Scheduled
                    | TaskState::Retrying => (TaskStatus::Queued, None),
                    TaskState::Running => (TaskStatus::Running, None),
                    TaskState::Completed => (TaskStatus::Completed, None),
                    TaskState::Failed => (TaskStatus::Failed, info.error_message.clone()),
                    TaskState::Cancelled => (TaskStatus::Cancelled, info.error_message.clone()),
                },
            };
            ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name: format!("{:?}", info.state),
                assigned_worker: info.assigned_worker_id,
                error: err,
            }
        }
        None => ClientResponse::Error {
            message: format!("Task {task_id} not found"),
        },
    }
}

/// A worker as `ClusterStatus` and `ListWorkers` report it: its capabilities plus the tags only the
/// master knows, `link:<path>`, `id:<worker id>` and `state:<state>` (see
/// `rusty_grid_core::placement`). Clients get no `WorkerInfo`, so the `id:` tag is how they learn
/// the id a `require:worker=` constraint can name.
async fn listed_worker(
    w: WorkerInfo,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        &Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> WorkerCapabilities {
    #[cfg(feature = "p2p")]
    let link = match p2p_conns {
        Some(conns) => match conns.read().await.get(&w.worker_id) {
            Some(conn) => match rusty_grid_core::transport::inspect_connection_paths(conn) {
                Some(info) if info.is_relay => format!("link:Relay (DERP):{:.1}ms", info.rtt_ms),
                Some(info) => format!("link:Direct P2P (QUIC):{:.1}ms", info.rtt_ms),
                None => "link:Direct P2P (QUIC)".to_string(),
            },
            None => "link:TCP/LAN".to_string(),
        },
        None => "link:TCP/LAN".to_string(),
    };
    #[cfg(not(feature = "p2p"))]
    let link = "link:TCP/LAN".to_string();

    let mut caps = w.capabilities;
    caps.tags.push(link);
    caps.tags.push(format!(
        "{}{}",
        rusty_grid_core::placement::ID_TAG_PREFIX,
        w.worker_id
    ));
    caps.tags.push(format!(
        "{}{}",
        rusty_grid_core::placement::STATE_TAG_PREFIX,
        w.status.as_str()
    ));
    caps
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_client_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    first_msg: ClientMessage,
    mut transport: MessageTransport<S>,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    mapreduce_parallelism: usize,
    blob_store: Option<Arc<BlobStore>>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> GridResult<()> {
    let mut next_msg = Some(first_msg);
    // Uploads this connection leaves unfinished are dropped when it ends.
    let uploads = blob_store
        .as_ref()
        .map(|store| UploadOwner::new(Arc::clone(store)));
    let mapreduce = MapReduceEngine::new(queue.clone(), scheduler_notify.clone(), waiters.clone())
        .with_registry(registry.clone())
        .with_parallelism(mapreduce_parallelism);

    loop {
        let msg = match next_msg.take() {
            Some(m) => m,
            None => match transport.recv_msg::<ClientMessage>().await {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                    let message = format_incompatible_version_error("client", peer_version);
                    warn!(peer_version, error = %message, "Refusing client message with incompatible protocol version");
                    let _ = send_client_response(
                        &mut transport,
                        &ClientResponse::Error {
                            message: message.clone(),
                        },
                    )
                    .await;
                    return Err(GridError::IncompatibleVersion(message));
                }
                Err(e) => return Err(e.into()),
            },
        };

        match msg {
            ClientMessage::SubmitTask { task, wait } => {
                let task_id = task.id;
                // A task id the queue already knows is refused by `submit` below; its pins
                // belong to the original task and must stay untouched.
                let known = queue.get_task(&task_id).await.is_some();
                if !known {
                    if let Err(message) = admit_blob_task(blob_store.as_ref(), &task).await {
                        warn!(task_id = %task_id, error = %message, "SubmitTask rejected");
                        send_client_response(&mut transport, &ClientResponse::Error { message })
                            .await?;
                        continue;
                    }
                }
                let rx = if wait {
                    let (tx, rx) = oneshot::channel();
                    let mut lock = waiters.write().await;
                    lock.entry(task_id).or_default().push(tx);
                    Some(rx)
                } else {
                    None
                };

                if let Err(e) = queue.submit(task).await {
                    // Validation failure, queue full, or a task id that is already in use: tell the
                    // client why, keep the connection, and take back the waiter registered above
                    // (it must never be resolved by the result of another task with the same id).
                    drop(rx);
                    prune_closed_waiters(&waiters, &task_id).await;
                    if let (false, Some(store)) = (known, blob_store.as_ref()) {
                        store.unpin_task(&task_id).await;
                    }
                    warn!(task_id = %task_id, error = %e, "SubmitTask rejected");
                    let resp = ClientResponse::Error {
                        message: e.to_string(),
                    };
                    send_client_response(&mut transport, &resp).await?;
                    continue;
                }
                scheduler_notify.notify_one();

                #[cfg(feature = "dashboard")]
                if let Some(ref b_tx) = broadcast_tx {
                    if let Some(info) = queue.get_task(&task_id).await {
                        let _ = b_tx.send(
                            crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info),
                        );
                    }
                    let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                        queue.stats().await,
                    ));
                }

                if let Some(rx) = rx {
                    match rx.await {
                        Ok(result) => {
                            let resp = ClientResponse::TaskCompleted { task_id, result };
                            send_client_response(&mut transport, &resp).await?;
                        }
                        Err(_) => {
                            let resp = ClientResponse::Error {
                                message: "Task completion channel closed unexpectedly".into(),
                            };
                            send_client_response(&mut transport, &resp).await?;
                        }
                    }
                } else {
                    let resp = ClientResponse::TaskSubmitted { task_id };
                    send_client_response(&mut transport, &resp).await?;
                }
            }
            ClientMessage::GetTaskStatus { task_id } => {
                let resp = task_status_response(&queue, task_id).await;
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::CancelTask { task_id } => {
                // Cancels in the queue, tells the executing worker to stop and wakes `wait`ers.
                // `success` is false for unknown ids and for tasks that had already finished.
                let success = match cancel_task_and_notify(
                    &registry,
                    &queue,
                    &scheduler_notify,
                    &waiters,
                    task_id,
                    "Cancelled by client",
                )
                .await
                {
                    Ok(newly_cancelled) => newly_cancelled,
                    Err(e) => {
                        debug!(task_id = %task_id, error = %e, "CancelTask failed");
                        false
                    }
                };
                #[cfg(feature = "dashboard")]
                if success {
                    if let Some(ref b_tx) = broadcast_tx {
                        if let Some(info) = queue.get_task(&task_id).await {
                            let _ = b_tx.send(
                                crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info),
                            );
                        }
                        let _ =
                            b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                                queue.stats().await,
                            ));
                    }
                }
                let resp = ClientResponse::TaskCancelled { task_id, success };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::ClusterStatus => {
                let workers_info = registry.list_all_workers().await;
                let mut workers = Vec::with_capacity(workers_info.len());
                for w in workers_info {
                    workers.push(
                        listed_worker(
                            w,
                            #[cfg(feature = "p2p")]
                            p2p_conns.as_ref(),
                        )
                        .await,
                    );
                }
                let stats = queue.stats().await;
                let (pending, running, completed, failed) = stats.cluster_buckets();
                let resp = ClientResponse::ClusterStatus {
                    total_tasks: stats.total,
                    pending_tasks: pending,
                    running_tasks: running,
                    completed_tasks: completed,
                    failed_tasks: failed,
                    active_bytes: stats.active_bytes,
                    max_active_bytes: stats.max_active_bytes,
                    store_error: stats.store_error,
                    workers,
                };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::Hello => {
                let resp = ClientResponse::Hello {
                    min_compatible_version:
                        rusty_grid_core::protocol::PROTOCOL_VERSION_MIN_COMPATIBLE,
                };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::ListWorkers => {
                let workers_info = registry.list_all_workers().await;
                let mut workers = Vec::with_capacity(workers_info.len());
                for w in workers_info {
                    workers.push(
                        listed_worker(
                            w,
                            #[cfg(feature = "p2p")]
                            p2p_conns.as_ref(),
                        )
                        .await,
                    );
                }
                let resp = ClientResponse::WorkerList { workers };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::SubmitMapReduce { job } => {
                let job_id = job.job_id;
                let mut exec = std::pin::pin!(mapreduce.execute(job));
                let outcome = loop {
                    tokio::select! {
                        res = &mut exec => break Some(res),
                        peer = transport.recv_msg::<ClientMessage>(), if next_msg.is_none() => {
                            match peer {
                                Ok(Some(later)) => {
                                    next_msg = Some(later);
                                }
                                Ok(None) | Err(_) => {
                                    info!(
                                        job_id = %job_id,
                                        "Client disconnected while MapReduce job was in flight; cancelling job"
                                    );
                                    break None;
                                }
                            }
                        }
                    }
                };
                match outcome {
                    Some(Ok(result)) => {
                        let resp = ClientResponse::MapReduceCompleted { result };
                        send_client_response(&mut transport, &resp).await?;
                    }
                    Some(Err(e)) => {
                        let resp = ClientResponse::Error {
                            message: e.to_string(),
                        };
                        send_client_response(&mut transport, &resp).await?;
                    }
                    None => break,
                }
            }
            ClientMessage::GetTaskResult { task_id } => {
                // Non-blocking poll: the retained result of a finished task, its status while it is
                // still in flight, an error for an id the master does not know (or no longer keeps).
                let resp = match queue.get_task(&task_id).await {
                    Some(info) if info.state.is_terminal() => {
                        match queue.get_result(&task_id).await {
                            Some(result) => ClientResponse::TaskCompleted { task_id, result },
                            None => task_status_response(&queue, task_id).await,
                        }
                    }
                    _ => task_status_response(&queue, task_id).await,
                };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::BlobRequest { request } => {
                let reply = match (&blob_store, &uploads) {
                    (Some(store), Some(uploads)) => store.handle(uploads.owner, request).await,
                    _ => BlobReply::Failed {
                        message: NO_BLOB_STORE.to_string(),
                    },
                };
                send_client_response(&mut transport, &ClientResponse::BlobReply { reply }).await?;
            }
        }
    }

    Ok(())
}

/// Authenticates a freshly accepted peer and reads its first protocol message, all inside one
/// `handshake_timeout` window.
///
/// With a `cluster_secret` the OXS-AUTH handshake (`rusty_grid_core::cluster_auth`) runs first and
/// only a peer that proves knowledge of the secret gets to send a first `InboundMessage`; until
/// then frames are capped at 1 KiB. Failures are logged with the peer address and the (secret-free)
/// reason, and the connection is dropped, so no task is created and no worker registered.
///
/// Without a secret the first message is read as before, except that a peer that opens with an
/// authentication HELLO is told `REJECT("server has no secret configured")` instead of being
/// silently dropped, so a misconfigured worker gets an actionable error.
///
/// `lane` only labels the log lines ("connection" for TCP, "control stream" for QUIC).
pub(crate) async fn authenticate_and_read_first_message<S>(
    transport: &mut MessageTransport<S>,
    peer: &str,
    cluster_secret: Option<&ClusterSecret>,
    handshake_timeout: Duration,
    lane: &str,
) -> GridResult<(InboundMessage, WireCodec)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + handshake_timeout;
    let timed_out = || GridError::Timeout {
        operation: "handshake".into(),
        duration_secs: handshake_timeout.as_secs(),
    };

    if let Some(secret) = cluster_secret {
        match tokio::time::timeout_at(deadline, server_handshake(transport, secret)).await {
            Ok(Ok(session_key)) => {
                debug!(remote = %peer, lane, "Peer authenticated with the cluster secret");
                transport.enable_encryption(&session_key, EndpointRole::Server);
            }
            Ok(Err(e @ (GridError::ConnectionClosed | GridError::Io(_)))) => {
                debug!(remote = %peer, lane, error = %e, "Peer disconnected during cluster authentication");
                return Err(e);
            }
            Ok(Err(e)) => {
                warn!(remote = %peer, lane, error = %e, "Cluster authentication failed; dropping connection");
                return Err(e);
            }
            Err(_) => {
                warn!(remote = %peer, lane, timeout_secs = handshake_timeout.as_secs(), "Cluster authentication timed out; dropping connection");
                return Err(timed_out());
            }
        }
    }

    match tokio::time::timeout_at(deadline, transport.recv_msg_with_codec::<InboundMessage>()).await
    {
        Ok(Ok(Some(first))) => Ok(first),
        Ok(Ok(None)) => {
            debug!(remote = %peer, lane, "Peer closed connection before handshake");
            Err(GridError::ConnectionClosed)
        }
        // The peer opened with an authentication HELLO but we hold no secret to answer it.
        Ok(Err(ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH))) if cluster_secret.is_none() => {
            warn!(remote = %peer, lane, "Peer attempted cluster authentication but this master has no cluster secret configured; rejecting");
            let _ = tokio::time::timeout(
                REFUSAL_DRAIN_TIMEOUT,
                send_reject(transport, REJECT_NO_SECRET),
            )
            .await;
            Err(GridError::AuthFailed(
                "peer requested cluster authentication but this master has no secret configured"
                    .into(),
            ))
        }
        Ok(Err(ProtocolError::IncompatibleVersion { peer_version, .. })) => {
            let message = format_incompatible_version_error("client", peer_version);
            warn!(
                remote = %peer,
                lane,
                peer_version,
                error = %message,
                "Refusing client connection with incompatible protocol version"
            );
            if let Some(detected) = transport.last_detected_codec() {
                transport.set_outbound_codec(detected);
            }
            let err_resp = ClientResponse::Error {
                message: message.clone(),
            };
            let _ =
                tokio::time::timeout(REFUSAL_DRAIN_TIMEOUT, transport.send_msg(&err_resp)).await;
            Err(GridError::IncompatibleVersion(message))
        }
        Ok(Err(e)) => {
            warn!(remote = %peer, lane, error = %e, "Protocol error during handshake");
            Err(e.into())
        }
        Err(_) => {
            warn!(remote = %peer, lane, timeout_secs = handshake_timeout.as_secs(), "Handshake watchdog timed out");
            Err(timed_out())
        }
    }
}

/// Aborts a spawned helper task when the connection handler that owns it ends, including when the
/// handler itself is aborted by the reaper or by the worker re-registering.
pub(crate) struct AbortOnDrop(pub(crate) tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Owns the QUIC side of a registered P2P worker: closes the connection when the handler ends and
/// unlists it from the active-connection map.
///
/// Aborting the handler task (the reaper, `unregister`, a re-registration) only drops its own
/// clone of the connection; the map holds another, so without an explicit close the worker would
/// stay connected to a master that has long written it off and never get the chance to reconnect.
#[cfg(feature = "p2p")]
pub(crate) struct P2pConnGuard {
    conn: iroh::endpoint::Connection,
    conns: Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    worker_id: Uuid,
}

#[cfg(feature = "p2p")]
impl P2pConnGuard {
    pub(crate) fn new(
        conn: iroh::endpoint::Connection,
        conns: Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
        worker_id: Uuid,
    ) -> Self {
        Self {
            conn,
            conns,
            worker_id,
        }
    }

    /// Removes this guard's connection from the map, unless the same worker has since
    /// re-registered over a newer connection that took the slot.
    pub(crate) async fn unlist(&self) {
        let mut guard = self.conns.write().await;
        if guard
            .get(&self.worker_id)
            .is_some_and(|c| c.stable_id() == self.conn.stable_id())
        {
            guard.remove(&self.worker_id);
        }
    }
}

#[cfg(feature = "p2p")]
impl Drop for P2pConnGuard {
    fn drop(&mut self) {
        self.conn.close(0u32.into(), b"session ended by master");
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let conns = Arc::clone(&self.conns);
            let worker_id = self.worker_id;
            let stable_id = self.conn.stable_id();
            runtime.spawn(async move {
                let mut guard = conns.write().await;
                if guard
                    .get(&worker_id)
                    .is_some_and(|c| c.stable_id() == stable_id)
                {
                    guard.remove(&worker_id);
                }
            });
        }
    }
}

pub(crate) fn reject_master_role_claim(
    ticket: &rusty_grid_core::arbitrage::MasterLeaseTicket,
) -> MasterMessage {
    warn!(
        claimant_master_id = %ticket.master_id,
        authority_score = ticket.authority_score,
        "Refusing ClaimMasterRole: automatic master failover is not supported"
    );
    MasterMessage::RoleHandoverAck {
        accepted: false,
        reason: Some(
            "Automatic master failover is not supported; use manual role switching".into(),
        ),
        task_queue_snapshot: Vec::new(),
        connected_workers: Vec::new(),
    }
}

/// Maximum time the master spends writing a handshake refusal, finishing the control send stream,
/// and waiting for the peer to close the QUIC connection before dropping it (`BUG-17`).
const REFUSAL_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

async fn finish_and_wait_for_peer_close<T: AsyncRead + AsyncWrite + Unpin>(
    transport: &mut MessageTransport<T>,
    #[cfg(feature = "p2p")] p2p_conn: Option<&iroh::endpoint::Connection>,
) {
    let _ = tokio::io::AsyncWriteExt::shutdown(transport.get_mut()).await;
    #[cfg(feature = "p2p")]
    if let Some(conn) = p2p_conn {
        conn.closed().await;
    }
}

/// Finishes the control send stream after an authentication `REJECT` frame has been written and
/// waits up to [`REFUSAL_DRAIN_TIMEOUT`] for the peer to close the QUIC connection (`BUG-17`).
pub(crate) async fn drain_refusal_stream<T: AsyncRead + AsyncWrite + Unpin>(
    transport: &mut MessageTransport<T>,
    #[cfg(feature = "p2p")] p2p_conn: Option<&iroh::endpoint::Connection>,
) {
    let _ = tokio::time::timeout(
        REFUSAL_DRAIN_TIMEOUT,
        finish_and_wait_for_peer_close(
            transport,
            #[cfg(feature = "p2p")]
            p2p_conn,
        ),
    )
    .await;
}

/// Sends a handshake refusal message (`RegisterAck { accepted: false, .. }` or
/// `RoleHandoverAck { accepted: false, .. }`), finishes the control send stream, and waits up to
/// [`REFUSAL_DRAIN_TIMEOUT`] (`1 s`) in total for the peer to close before dropping the QUIC
/// connection handle (`BUG-17`).
pub(crate) async fn send_refusal_and_drain<T: AsyncRead + AsyncWrite + Unpin>(
    transport: &mut MessageTransport<T>,
    msg: &MasterMessage,
    #[cfg(feature = "p2p")] p2p_conn: Option<&iroh::endpoint::Connection>,
) {
    let _ = tokio::time::timeout(REFUSAL_DRAIN_TIMEOUT, async {
        if transport.send_msg(msg).await.is_ok() {
            finish_and_wait_for_peer_close(
                transport,
                #[cfg(feature = "p2p")]
                p2p_conn,
            )
            .await;
        }
    })
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    stream: S,
    remote_addr: SocketAddr,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    heartbeat_interval_secs: u64,
    handshake_timeout: Duration,
    shutdown_rx: watch::Receiver<bool>,
    abort_handle: Option<AbortHandle>,
    wire_codec: WireCodec,
    cluster_secret: Option<ClusterSecret>,
    handshake_permit: Option<OwnedSemaphorePermit>,
    mapreduce_parallelism: usize,
    blob_store: Option<Arc<BlobStore>>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    #[cfg(feature = "p2p")] p2p_conn: Option<iroh::endpoint::Connection>,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> GridResult<()> {
    let pre_auth = crate::session::PreAuthSession::new_single(
        stream,
        remote_addr,
        wire_codec,
        mapreduce_parallelism,
        #[cfg(feature = "p2p")]
        p2p_conn,
        #[cfg(feature = "p2p")]
        p2p_conns,
    );
    crate::session::WorkerSession::run_connection(
        pre_auth,
        registry,
        queue,
        scheduler_notify,
        waiters,
        heartbeat_interval_secs,
        handshake_timeout,
        shutdown_rx,
        abort_handle,
        wire_codec,
        cluster_secret,
        handshake_permit,
        blob_store,
        #[cfg(feature = "dashboard")]
        broadcast_tx,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(feature = "p2p"), allow(dead_code))]
async fn handle_multiplexed_connection<
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    D: AsyncRead + AsyncWrite + Unpin + Send + 'static,
>(
    ctrl_stream: C,
    data_stream: D,
    remote_addr: SocketAddr,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    heartbeat_interval_secs: u64,
    handshake_timeout: Duration,
    shutdown_rx: watch::Receiver<bool>,
    abort_handle: Option<AbortHandle>,
    wire_codec: WireCodec,
    cluster_secret: Option<ClusterSecret>,
    handshake_permit: Option<OwnedSemaphorePermit>,
    blob_store: Option<Arc<BlobStore>>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    #[cfg(feature = "p2p")] p2p_conn: Option<iroh::endpoint::Connection>,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> GridResult<()> {
    let pre_auth = crate::session::PreAuthSession::new_multiplexed(
        ctrl_stream,
        data_stream,
        remote_addr,
        wire_codec,
        #[cfg(feature = "p2p")]
        p2p_conn,
        #[cfg(feature = "p2p")]
        p2p_conns,
    );
    crate::session::WorkerSession::run_connection(
        pre_auth,
        registry,
        queue,
        scheduler_notify,
        waiters,
        heartbeat_interval_secs,
        handshake_timeout,
        shutdown_rx,
        abort_handle,
        wire_codec,
        cluster_secret,
        handshake_permit,
        blob_store,
        #[cfg(feature = "dashboard")]
        broadcast_tx,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn spawn_master_rendezvous_registration(
    rendezvous_url: String,
    cluster_addr: String,
    web_ui_url: String,
    hostname: String,
    p2p_ticket: Option<String>,
    registry: WorkerRegistry,
    cluster_secret: Option<ClusterSecret>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let cluster_addr = if let Ok(parsed) = cluster_addr.parse::<std::net::SocketAddr>() {
        if parsed.ip().is_unspecified() {
            let local_ip = rusty_grid_core::discovery::get_local_ip_or_loopback();
            format!("{}:{}", local_ip, parsed.port())
        } else {
            cluster_addr
        }
    } else {
        cluster_addr
    };
    let mut ticker = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let workers_count = registry.list_workers().await.len();
                let mut lease = rusty_grid_core::discovery::RendezvousLease::new(
                    &cluster_addr,
                    &web_ui_url,
                    &hostname,
                    p2p_ticket.clone(),
                    workers_count,
                    120,
                );
                if let Some(ref secret) = cluster_secret {
                    lease.sign(secret);
                }
                if let Err(e) = rusty_grid_core::discovery::register_rendezvous_lease(&rendezvous_url, &lease).await {
                    warn!(url = %rendezvous_url, error = %e, "Failed to register Master lease with rendezvous registry");
                } else {
                    debug!(url = %rendezvous_url, "Successfully renewed Master lease in rendezvous registry");
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
    blob_store: Option<Arc<BlobStore>>,
}

impl MasterHandle {
    /// The blob store of task inputs and output files, if this master runs one.
    pub fn blob_store(&self) -> Option<&Arc<BlobStore>> {
        self.blob_store.as_ref()
    }

    /// Admits a task for submission through this handle (see `admit_blob_task`); returns
    /// whether the task id was new, so a refused submission can release its pins.
    async fn admit(&self, task: &Task) -> GridResult<bool> {
        if self.queue.get_task(&task.id).await.is_some() {
            return Ok(false);
        }
        admit_blob_task(self.blob_store.as_ref(), task)
            .await
            .map_err(GridError::Config)?;
        Ok(true)
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
        let admitted = self.admit(&task).await?;
        let id = match self.queue.submit(task).await {
            Ok(id) => id,
            Err(e) => {
                self.release_admission(&task_id, admitted).await;
                return Err(e);
            }
        };
        self.scheduler_notify.notify_one();
        #[cfg(feature = "dashboard")]
        if let Some(ref b_tx) = self.broadcast_tx {
            if let Some(info) = self.queue.get_task(&id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                    info,
                ));
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                self.queue.stats().await,
            ));
        }
        Ok(id)
    }

    /// Submits a task with explicit priority.
    pub async fn submit_task_with_priority(&self, task: Task, priority: u32) -> GridResult<TaskId> {
        let task_id = task.id;
        let admitted = self.admit(&task).await?;
        let id = match self.queue.submit_with_priority(task, priority).await {
            Ok(id) => id,
            Err(e) => {
                self.release_admission(&task_id, admitted).await;
                return Err(e);
            }
        };
        self.scheduler_notify.notify_one();
        #[cfg(feature = "dashboard")]
        if let Some(ref b_tx) = self.broadcast_tx {
            if let Some(info) = self.queue.get_task(&id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                    info,
                ));
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                self.queue.stats().await,
            ));
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
            if let Some(info) = self.queue.get_task(&task_id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                    info,
                ));
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                self.queue.stats().await,
            ));
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
    let local_ip = if listen_ip.is_loopback() {
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

    let bind_addr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        discovery_port,
    );
    let socket = match tokio::net::UdpSocket::bind(bind_addr).await {
        Ok(s) => s,
        Err(e) => {
            warn!(port = discovery_port, error = %e, "Could not bind Master UDP discovery listener");
            return;
        }
    };
    let _ = socket.set_broadcast(true);

    info!(
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
                // An unsolicited broadcast cannot carry a MAC bound to a probe nonce, and clients
                // holding the secret ignore unsigned beacons, so a master with a secret does not
                // advertise itself to the whole LAN: it answers authenticated probes only.
                if cluster_secret.is_some() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::protocol::{ProtocolCapabilities, PROTOCOL_VERSION_CURRENT};
    use rusty_grid_core::task::Task;
    use rusty_grid_core::WorkerCapabilities;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_master_server_bind_ephemeral_and_port_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let port_file = temp_dir.path().join("master.port");

        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_port_file(&port_file)
            .with_handshake_timeout(2);

        let registry = WorkerRegistry::new();
        let server = MasterServer::bind(config, registry).await.unwrap();

        assert!(server.port() > 0);
        assert!(port_file.exists());

        let content = std::fs::read_to_string(&port_file).unwrap();
        assert_eq!(content.trim(), server.port().to_string());
    }

    #[tokio::test]
    async fn test_master_server_handshake_and_reject_zero_cores() {
        let server = MasterServer::bind_addr("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(async move {
            server.run(shutdown_rx).await.unwrap();
        });

        // Connect client with 0 CPU cores -> should be rejected
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut transport = MessageTransport::new(stream);

        let bad_msg = WorkerMessage::Register {
            worker_id: Uuid::new_v4(),
            capabilities: WorkerCapabilities::new("bad-worker", 0, 1024, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        };
        transport.send_msg(&bad_msg).await.unwrap();

        let resp: Option<MasterMessage> = transport.recv_msg().await.unwrap();
        match resp {
            Some(MasterMessage::RegisterAck { accepted, .. }) => {
                assert!(!accepted, "Master must reject worker with 0 cores");
            }
            other => panic!("Expected RegisterAck, got {other:?}"),
        }

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn test_server_config_web_ui_options() {
        let default_cfg = ServerConfig::default();
        assert!(!default_cfg.enable_web_ui);

        let ephemeral_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap());
        assert!(!ephemeral_cfg.enable_web_ui);

        let explicit_cfg = ServerConfig::new("127.0.0.1:8088".parse().unwrap());
        assert!(explicit_cfg.enable_web_ui);

        let custom_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(true)
            .with_web_ui_addr("127.0.0.1:0".parse().unwrap());
        assert!(custom_cfg.enable_web_ui);
        assert_eq!(custom_cfg.web_ui_addr.port(), 0);
    }

    #[tokio::test]
    async fn test_master_server_spawn_with_ephemeral_web_ui() {
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(true)
            .with_web_ui_addr("127.0.0.1:0".parse().unwrap());
        let handle = MasterServer::spawn(config).await.unwrap();
        assert!(handle.server_addr().port() > 0);
        let _ = handle.shutdown();
    }

    #[tokio::test]
    async fn test_master_server_discovery_service() {
        let disc_port = 18089;
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_discovery(true)
            .with_discovery_port(disc_port);

        let handle = MasterServer::spawn(config).await.unwrap();
        assert!(handle.server_addr().port() > 0);

        // Discovery probe using the configured discovery port
        let discovered =
            rusty_grid_core::discovery::discover_master(Duration::from_millis(1500), disc_port)
                .await;

        assert!(
            discovered.is_some(),
            "Master discovery service should respond to probe"
        );
        let beacon = discovered.unwrap();
        assert_eq!(
            beacon.service,
            rusty_grid_core::discovery::DISCOVERY_SERVICE_NAME
        );
        assert!(beacon
            .cluster_addr
            .ends_with(&format!(":{}", handle.server_addr().port())));

        let _ = handle.shutdown();
    }

    #[test]
    fn test_beacon_advertises_the_dashboard_where_it_really_listens() {
        use std::net::{IpAddr, Ipv4Addr};
        let lan: IpAddr = "192.0.2.7".parse().unwrap();
        let addr = |text: &str| Some(text.parse::<SocketAddr>().unwrap());

        // Loopback (the default) or no dashboard at all: nobody on the network can reach it.
        assert_eq!(advertised_web_ui_url(addr("127.0.0.1:8080"), lan), "");
        assert_eq!(advertised_web_ui_url(addr("[::1]:8080"), lan), "");
        assert_eq!(advertised_web_ui_url(None, lan), "");
        // A wildcard bind is reached through the machine's LAN address...
        assert_eq!(
            advertised_web_ui_url(addr("0.0.0.0:8081"), lan),
            "http://192.0.2.7:8081"
        );
        // ...unless the machine has none.
        assert_eq!(
            advertised_web_ui_url(addr("0.0.0.0:8081"), IpAddr::V4(Ipv4Addr::LOCALHOST)),
            ""
        );
        // A specific interface is advertised as bound, whatever the LAN address says.
        assert_eq!(
            advertised_web_ui_url(addr("198.51.100.2:9000"), lan),
            "http://198.51.100.2:9000"
        );
        assert_eq!(
            advertised_web_ui_url(addr("[2001:db8::1]:9000"), lan),
            "http://[2001:db8::1]:9000"
        );
    }

    #[tokio::test]
    async fn test_beacon_of_a_master_with_a_loopback_dashboard_advertises_no_dashboard() {
        let disc_port = 18091;
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(true)
            .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
            .with_discovery(true)
            .with_discovery_port(disc_port);
        let handle = MasterServer::spawn(config).await.unwrap();
        assert!(handle.dashboard_addr().unwrap().ip().is_loopback());

        let beacon =
            rusty_grid_core::discovery::discover_master(Duration::from_millis(1500), disc_port)
                .await
                .expect("the master answers the probe");
        assert!(beacon
            .cluster_addr
            .ends_with(&format!(":{}", handle.server_addr().port())));
        assert_eq!(
            beacon.web_ui_url, "",
            "the dashboard listens on loopback only: advertising a LAN URL would point nowhere"
        );
        let _ = handle.shutdown();
    }

    #[tokio::test]
    async fn test_timed_out_wait_task_does_not_leak_its_waiter() {
        let handle = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
            .await
            .unwrap();
        // No worker: the task stays queued, so the wait times out.
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::command("echo", vec!["x".into()]),
            rusty_grid_core::task::TaskRequirements::generic(1, 30),
        );
        let task_id = handle.submit_task(task).await.unwrap();
        for _ in 0..3 {
            let err = handle
                .wait_task(task_id, Some(Duration::from_millis(30)))
                .await
                .unwrap_err();
            assert!(matches!(err, GridError::Timeout { .. }), "{err}");
        }
        assert!(
            handle.waiters.read().await.get(&task_id).is_none(),
            "timed-out waits must not accumulate dead senders until the task ends"
        );

        // A live waiter is not disturbed by another waiter timing out.
        let live = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait_task(task_id, None).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = handle
            .wait_task(task_id, Some(Duration::from_millis(30)))
            .await;
        assert_eq!(
            handle.waiters.read().await.get(&task_id).map(Vec::len),
            Some(1)
        );
        handle.cancel_task(task_id).await.unwrap();
        let cancelled = live.await.unwrap().unwrap();
        assert_eq!(cancelled.exit_code, 130);
        assert!(handle.waiters.read().await.get(&task_id).is_none());
        let _ = handle.shutdown();
    }

    #[tokio::test]
    async fn test_superseded_session_sweep_survives_the_abort_of_its_caller() {
        // The caller of the sweep is the new session's handler; a third registration aborts it
        // while the sweep waits for the queue lock. The tasks must still be failed over.
        let queue = TaskQueue::new();
        let notify = Arc::new(tokio::sync::Notify::new());
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let worker_id = Uuid::new_v4();
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::command("echo", vec!["x".into()]),
            rusty_grid_core::task::TaskRequirements::generic(1, 30),
        );
        let task_id = queue.submit(task).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(task_id, worker_id, 1)])
            .await;
        queue
            .mark_running_from(worker_id, Some(1), &task_id)
            .await
            .unwrap();

        let lock = queue.hold_write_lock_for_test().await;
        let caller = {
            let (queue, notify, waiters) = (queue.clone(), notify.clone(), waiters.clone());
            tokio::spawn(async move {
                failover_superseded_session(
                    &queue,
                    &notify,
                    &waiters,
                    worker_id,
                    1,
                    2,
                    #[cfg(feature = "dashboard")]
                    None,
                )
                .await;
            })
        };
        // Let the sweep start and park on the lock, then abort its caller like a third
        // registration of the same worker id does.
        tokio::time::sleep(Duration::from_millis(100)).await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(lock);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while queue.get_state(&task_id).await != Some(TaskState::Queued) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the task is still assigned to the dead session 1"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(queue.get_task(&task_id).await.unwrap().retry_count, 1);
    }

    // --- Step B: accept-loop robustness, connection budgets, key file permissions ---

    #[test]
    fn test_accept_backoff_grows_to_a_ceiling_and_starts_over_after_a_success() {
        let mut backoff = AcceptBackoff::new();
        let pauses: Vec<u64> = (0..8)
            .map(|_| backoff.failure().as_millis() as u64)
            .collect();
        assert_eq!(pauses, vec![50, 100, 200, 400, 800, 1000, 1000, 1000]);
        backoff.reset();
        assert_eq!(backoff.failure(), Duration::from_millis(50));
    }

    #[test]
    fn test_connection_limits_keep_two_separate_budgets_and_do_not_leak_permits() {
        let limits = ConnectionLimits::new(2, 1);
        let (first_conn, first_handshake) = limits.admit().expect("first peer fits");

        // The pre-auth budget (1) is used up although a connection slot is free, and the refused
        // attempt must not keep the connection slot it briefly took.
        assert!(limits.admit().is_none());
        assert_eq!(limits.connections.available_permits(), 1);

        // The first peer authenticated: its pre-auth slot is back, its connection slot is not.
        drop(first_handshake);
        let (second_conn, second_handshake) = limits.admit().expect("second peer fits");
        drop(second_handshake);
        assert!(
            limits.admit().is_none(),
            "two open connections exhaust a budget of two"
        );

        drop(first_conn);
        assert!(
            limits.admit().is_some(),
            "a closed connection frees its slot"
        );
        drop(second_conn);
    }

    #[cfg(all(feature = "p2p", unix))]
    #[tokio::test]
    async fn test_p2p_master_key_is_created_private_and_a_loose_one_is_tightened() {
        use std::os::unix::fs::PermissionsExt;
        let mode_of = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oxideswarm-home").join("master_key.bin");
        let created = resolve_p2p_secret_key(Some(&path)).await.unwrap();
        assert_eq!(
            mode_of(&path),
            0o600,
            "a new key must be readable by its owner only"
        );
        assert_eq!(
            mode_of(path.parent().unwrap()),
            0o700,
            "so must the directory created for it"
        );

        // Reloading keeps the identity and the mode.
        let reloaded = resolve_p2p_secret_key(Some(&path)).await.unwrap();
        assert_eq!(created.to_bytes(), reloaded.to_bytes());
        assert_eq!(mode_of(&path), 0o600);

        // A key left world-readable by an older version (or by hand) is tightened on start.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let tightened = resolve_p2p_secret_key(Some(&path)).await.unwrap();
        assert_eq!(
            created.to_bytes(),
            tightened.to_bytes(),
            "tightening must not regenerate the key"
        );
        assert_eq!(mode_of(&path), 0o600);

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o664)).unwrap();
        resolve_p2p_secret_key(Some(&path)).await.unwrap();
        assert_eq!(mode_of(&path), 0o600);
    }

    #[test]
    fn test_dashboard_defaults_to_loopback() {
        let config = ServerConfig::default();
        assert!(config.web_ui_addr.ip().is_loopback());
        assert!(config.dashboard_bind_ip.is_none());
        // Binding the grid listener to every interface does not move the dashboard.
        let wide = ServerConfig::new("0.0.0.0:8088".parse().unwrap());
        assert!(wide.web_ui_addr.ip().is_loopback());
        assert!(
            !config.implicit_local_dashboard,
            "the library default stays strict"
        );
    }

    #[tokio::test]
    async fn test_master_with_larger_max_active_bytes_accepts_what_default_refuses_and_reports_bytes(
    ) {
        let make_task = |stdin_len: usize| {
            Task::new(
                rusty_grid_core::task::TaskSpec::Command {
                    program: "echo".into(),
                    args: vec!["ok".into()],
                    env: Default::default(),
                    working_dir: None,
                    stdin: Some(vec![b'x'; stdin_len]),
                },
                rusty_grid_core::task::TaskRequirements::generic(1, 30),
            )
        };

        // 1. Over TCP (`ClientMessage::SubmitTask`): a tight cap accepts the first task and
        //    refuses the second with a message naming the bytes in use, limit in bytes, and
        //    `--max-active-mb`, whereas a MasterServer with a larger `max_active_bytes` accepts both.
        let tight = MasterServer::spawn(
            ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_active_bytes(1_500),
        )
        .await
        .unwrap();
        let stream = TcpStream::connect(tight.server_addr()).await.unwrap();
        let mut client = MessageTransport::new(stream);

        client
            .send_msg(&ClientMessage::SubmitTask {
                task: make_task(100),
                wait: false,
            })
            .await
            .unwrap();
        match client.recv_msg::<ClientResponse>().await.unwrap() {
            Some(ClientResponse::TaskSubmitted { .. }) => {}
            other => panic!("expected first task to be accepted, got {other:?}"),
        }

        client
            .send_msg(&ClientMessage::SubmitTask {
                task: make_task(600),
                wait: false,
            })
            .await
            .unwrap();
        match client.recv_msg::<ClientResponse>().await.unwrap() {
            Some(ClientResponse::Error { message }) => {
                assert!(message.contains("bytes in use"), "got: {message}");
                assert!(message.contains("limit 1500 bytes"), "got: {message}");
                assert!(message.contains("--max-active-mb"), "got: {message}");
            }
            other => panic!("expected byte-limit refusal on second task, got {other:?}"),
        }
        let _ = tight.shutdown();

        let roomy = MasterServer::spawn(
            ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_active_bytes(4_000),
        )
        .await
        .unwrap();
        let stream = TcpStream::connect(roomy.server_addr()).await.unwrap();
        let mut client = MessageTransport::new(stream);
        for len in [100, 600] {
            client
                .send_msg(&ClientMessage::SubmitTask {
                    task: make_task(len),
                    wait: false,
                })
                .await
                .unwrap();
            match client.recv_msg::<ClientResponse>().await.unwrap() {
                Some(ClientResponse::TaskSubmitted { .. }) => {}
                other => panic!("expected roomy master to accept task ({len}), got {other:?}"),
            }
        }
        let _ = roomy.shutdown();

        // 2. Default `ServerConfig` (`DEFAULT_MAX_ACTIVE_BYTES`) refuses a submission that exceeds
        //    the default 256 MiB active byte cap, while a `MasterServer` started with a larger
        //    `max_active_bytes` accepts it.
        let default_master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
            .await
            .unwrap();
        let err = default_master
            .submit_task(make_task(crate::queue::DEFAULT_MAX_ACTIVE_BYTES))
            .await
            .unwrap_err();
        assert!(
            matches!(err, GridError::QueueByteLimitExceeded { .. }),
            "default master must refuse task exceeding DEFAULT_MAX_ACTIVE_BYTES, got {err:?}"
        );
        let _ = default_master.shutdown();

        let larger_master = MasterServer::spawn(
            ServerConfig::new("127.0.0.1:0".parse().unwrap())
                .with_max_active_bytes(crate::queue::DEFAULT_MAX_ACTIVE_BYTES + 8 * 1024 * 1024),
        )
        .await
        .unwrap();
        larger_master
            .submit_task(make_task(crate::queue::DEFAULT_MAX_ACTIVE_BYTES))
            .await
            .expect("master with larger max_active_bytes must accept the submission");
        let _ = larger_master.shutdown();
    }

    #[tokio::test]
    async fn test_mapreduce_cancelled_when_client_disconnects() {
        // Item 4: when a client submitting `SubmitMapReduce` disconnects while partitions are
        // running or waiting for room in the queue, `handle_client_connection` must cancel the
        // MapReduce job so running partitions are cancelled and waiting partitions never enter
        // the queue when room later opens.
        let master = MasterServer::spawn(
            ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_active_bytes(2_500),
        )
        .await
        .unwrap();

        let stream = TcpStream::connect(master.server_addr()).await.unwrap();
        let mut client = MessageTransport::new(stream);
        let job = rusty_grid_core::mapreduce::MapReduceJobSpec::new(
            "disconnect_job",
            vec!["x".repeat(1_200), "y".repeat(1_200), "z".repeat(1_200)],
            rusty_grid_core::mapreduce::MapFunctionSpec::Builtin {
                operator: "word_count".into(),
            },
            rusty_grid_core::mapreduce::ReduceFunctionSpec::Builtin {
                operator: "sum".into(),
            },
            3,
            1,
            30,
        );
        client
            .send_msg(&ClientMessage::SubmitMapReduce { job })
            .await
            .unwrap();

        // Wait until at least one partition is queued (and the remaining partitions are waiting
        // for active-byte room because 3 * ~1_400 bytes > 2_500 bytes).
        let mut saw_queued = false;
        for _ in 0..50 {
            if master.queue().stats().await.queued > 0 {
                saw_queued = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(saw_queued, "at least one map partition should be queued");

        // Disconnect the client while the MapReduce job is in flight.
        drop(client);

        // Wait briefly for `handle_client_connection` to notice EOF, drop `mapreduce.execute`,
        // and let `AbandonGuard` cancel the queued partition without queuing the waiting ones.
        let mut drained = false;
        for _ in 0..50 {
            let stats = master.queue().stats().await;
            if stats.queued == 0 && stats.active_bytes == 0 {
                drained = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let final_stats = master.queue().stats().await;
        assert!(
            drained,
            "client disconnect must cancel running/queued partitions and queue no waiting ones, got {final_stats:?}"
        );
        let _ = master.shutdown();
    }

    #[tokio::test]
    async fn test_oversized_master_frames_fail_only_the_task_and_return_client_error() {
        use rusty_grid_core::protocol::{
            serialize_message, ProtocolCapabilities, WireCodec, MAX_FRAME_SIZE,
            PROTOCOL_VERSION_CURRENT,
        };
        use rusty_grid_core::task::{Bytes, TaskRequirements, TaskSpec};
        use std::collections::HashMap;

        let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
            .await
            .unwrap();
        let addr = master.server_addr();

        // Register a mock worker with 1 CPU slot so a second task can only be dispatched if
        // `handle_oversized_master_message` releases the worker slot (`decrement_active_tasks`).
        let worker_stream = TcpStream::connect(addr).await.unwrap();
        let mut worker = MessageTransport::new(worker_stream);
        let worker_id = Uuid::new_v4();
        worker
            .send_msg(&WorkerMessage::Register {
                worker_id,
                capabilities: WorkerCapabilities {
                    name: "bug11-worker".into(),
                    cpu_cores: 1,
                    ram_mb: 8192,
                    has_gpu: false,
                    is_simulated_gpu: false,
                    gpu_device_name: None,
                    tags: vec![],
                    mobile: None,
                    has_blender: false,
                    blender_version: None,
                    vram_mb: None,
                    has_cuda: false,
                },
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        match worker.recv_msg::<MasterMessage>().await.unwrap() {
            Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
            other => panic!("expected RegisterAck, got {other:?}"),
        }

        // 1. Submit an oversized task (> 64 MiB stdin) with a registered waiter and a normal
        //    sibling task on a 1-core worker.
        //    The oversized assignment must wake its waiter with the frame-limit failure, release
        //    the worker's single slot without killing the worker session, and allow the sibling
        //    task to be dispatched and finish.
        let oversized_task = Task::new(
            TaskSpec::Command {
                program: "cat".into(),
                args: vec![],
                env: HashMap::new(),
                working_dir: None,
                stdin: Some(vec![b'x'; MAX_FRAME_SIZE + 1024]),
            },
            TaskRequirements::generic(1, 64),
        );
        let oversized_id = oversized_task.id;
        let (oversized_waiter_tx, oversized_waiter_rx) = tokio::sync::oneshot::channel();
        master
            .waiters
            .write()
            .await
            .entry(oversized_id)
            .or_default()
            .push(oversized_waiter_tx);
        master.submit_task(oversized_task).await.unwrap();

        // The registered waiter must wake with the failure result, and the worker's registry slot
        // must be released (`active_tasks == 0` and status not `Busy`).
        let oversized_res = tokio::time::timeout(Duration::from_secs(2), oversized_waiter_rx)
            .await
            .expect("oversized task waiter must be woken when assignment fails")
            .expect("oversized task waiter channel must receive TaskResult");
        assert!(!oversized_res.is_success());
        let err_msg = oversized_res.error.unwrap_or_default();
        assert!(
            err_msg.contains("frame") && err_msg.contains(&MAX_FRAME_SIZE.to_string()),
            "oversized task failure reason must mention the frame limit, got: {err_msg}"
        );
        assert_eq!(
            master.get_task_info(oversized_id).await.unwrap().state,
            crate::queue::TaskState::Failed
        );
        let worker_after_fail = master.registry.get_worker(worker_id).await.unwrap();
        assert_eq!(
            worker_after_fail.active_tasks, 0,
            "worker registry active_tasks must return to 0 after unsendable assignment"
        );
        assert_ne!(
            worker_after_fail.status,
            crate::registry::WorkerStatus::Busy,
            "worker registry status must not stay Busy after unsendable assignment"
        );

        let sibling_task = Task::new(
            TaskSpec::command("echo", vec!["sibling".into()]),
            TaskRequirements::generic(1, 64),
        );
        let sibling_id = sibling_task.id;
        master.submit_task(sibling_task).await.unwrap();

        // The 1-slot worker session must still be alive, have its slot released, and receive the
        // sibling task assignment.
        let assigned =
            tokio::time::timeout(Duration::from_secs(2), worker.recv_msg::<MasterMessage>())
                .await
                .expect("worker session must receive sibling AssignTask without timing out")
                .expect("worker transport must not error")
                .expect("worker transport must not be closed");
        match assigned {
            MasterMessage::AssignTask { task } => assert_eq!(task.id, sibling_id),
            other => panic!("expected sibling AssignTask, got {other:?}"),
        }

        // Complete the sibling task with a result whose WorkerMessage::TaskResult frame fits
        // MAX_FRAME_SIZE (by 8 bytes), so its ClientResponse::TaskCompleted frame (which carries
        // an extra 24 bytes in bincode) exceeds MAX_FRAME_SIZE by 16 bytes.
        let mut probe_msg = WorkerMessage::TaskResult {
            worker_id,
            task_id: sibling_id,
            exit_code: 0,
            stdout: Bytes::new(),
            stderr: Bytes::new(),
            execution_time_ms: 1,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: vec![],
            outputs: vec![],
        };
        let empty_size = serialize_message(&probe_msg, WireCodec::Bincode)
            .unwrap()
            .len();
        let stdout_len = MAX_FRAME_SIZE - empty_size - 8;
        if let WorkerMessage::TaskResult { ref mut stdout, .. } = probe_msg {
            *stdout = Bytes::from(vec![b'a'; stdout_len]);
        }
        let worker_frame_len = serialize_message(&probe_msg, WireCodec::Bincode)
            .unwrap()
            .len();
        assert!(
            worker_frame_len <= MAX_FRAME_SIZE,
            "worker frame must fit MAX_FRAME_SIZE: {worker_frame_len}"
        );

        worker.send_msg(&probe_msg).await.unwrap();

        // Wait for sibling task to complete on the master.
        for _ in 0..80 {
            if let Ok(info) = master.get_task_info(sibling_id).await {
                if info.state == crate::queue::TaskState::Completed {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            master.get_task_info(sibling_id).await.unwrap().state,
            crate::queue::TaskState::Completed
        );

        // 2. A client requesting the sibling task result (whose ClientResponse::TaskCompleted
        //    exceeds MAX_FRAME_SIZE) must receive ClientResponse::Error instead of EOF.
        let client_stream = TcpStream::connect(addr).await.unwrap();
        let mut client = MessageTransport::new(client_stream);
        client
            .send_msg(&ClientMessage::GetTaskResult {
                task_id: sibling_id,
            })
            .await
            .unwrap();
        match client.recv_msg::<ClientResponse>().await.unwrap() {
            Some(ClientResponse::Error { message }) => {
                assert!(
                    message.contains(&sibling_id.to_string())
                        && message.contains("frame")
                        && message.contains(&MAX_FRAME_SIZE.to_string())
                        && message.contains("--output"),
                    "expected frame limit error message naming task and --output, got: {message}"
                );
            }
            other => {
                panic!("expected ClientResponse::Error for oversized client reply, got {other:?}")
            }
        }

        // 3. A task with a 33 MiB checkpoint dispatched via AssignTaskWithCheckpoint carries the
        //    checkpoint once (`latest_checkpoint: Some(...)`, `task.latest_checkpoint: None`), so
        //    it fits inside the 64 MiB frame limit instead of doubling to 66 MiB.
        let mut cp_task = Task::new(
            TaskSpec::command("echo", vec!["cp".into()]),
            TaskRequirements::generic(1, 64),
        );
        cp_task.latest_checkpoint = Some(rusty_grid_core::task::CheckpointData::new(
            1,
            vec![b'c'; 33 * 1024 * 1024],
        ));
        let cp_task_id = cp_task.id;
        master.submit_task(cp_task).await.unwrap();

        let cp_assigned =
            tokio::time::timeout(Duration::from_secs(2), worker.recv_msg::<MasterMessage>())
                .await
                .expect("worker must receive 33 MiB AssignTaskWithCheckpoint")
                .expect("worker transport must not error")
                .expect("worker transport must not be closed");
        match cp_assigned {
            MasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            } => {
                assert_eq!(task.id, cp_task_id);
                assert!(
                    task.latest_checkpoint.is_none(),
                    "checkpoint must not be duplicated in task.latest_checkpoint"
                );
                assert_eq!(
                    latest_checkpoint.as_ref().map(|c| c.delta_state.len()),
                    Some(33 * 1024 * 1024)
                );
            }
            other => panic!("expected AssignTaskWithCheckpoint, got {other:?}"),
        }

        let _ = master.shutdown();

        // 4. Verify the multiplexed (QUIC 2-lane) data-writer path (`handle_multiplexed_connection`)
        //    also fails only the oversized task, releases the worker slot, wakes its waiter, and
        //    keeps the data lane open for the next assignment.
        let mux_registry = WorkerRegistry::new();
        let mux_queue = TaskQueue::new();
        let mux_notify = Arc::new(tokio::sync::Notify::new());
        let mux_waiters: WaiterMap = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        let (stop_tx, stop_rx) = watch::channel(false);
        let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
        let (d_master, d_worker) = tokio::io::duplex(64 * 1024);

        let mux_reg_clone = mux_registry.clone();
        let mux_queue_clone = mux_queue.clone();
        let mux_notify_clone = Arc::clone(&mux_notify);
        let mux_waiters_clone = Arc::clone(&mux_waiters);
        let mux_conn = tokio::spawn(async move {
            let _ = handle_multiplexed_connection(
                c_master,
                d_master,
                "127.0.0.1:50000".parse().unwrap(),
                mux_reg_clone,
                mux_queue_clone,
                mux_notify_clone,
                mux_waiters_clone,
                10,
                Duration::from_secs(5),
                stop_rx,
                None,
                WireCodec::Bincode,
                None,
                None,
                None,
                #[cfg(feature = "dashboard")]
                None,
                #[cfg(feature = "p2p")]
                None,
                #[cfg(feature = "p2p")]
                None,
            )
            .await;
        });

        let mut mux_ctrl = MessageTransport::new(c_worker);
        let mut mux_data = MessageTransport::new(d_worker);
        let mux_worker_id = Uuid::new_v4();
        mux_ctrl
            .send_msg(&WorkerMessage::Register {
                worker_id: mux_worker_id,
                capabilities: WorkerCapabilities {
                    name: "bug11-mux-worker".into(),
                    cpu_cores: 1,
                    ram_mb: 8192,
                    has_gpu: false,
                    is_simulated_gpu: false,
                    gpu_device_name: None,
                    tags: vec![],
                    mobile: None,
                    has_blender: false,
                    blender_version: None,
                    vram_mb: None,
                    has_cuda: false,
                },
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        match mux_ctrl.recv_msg::<MasterMessage>().await.unwrap() {
            Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
            other => panic!("expected mux RegisterAck, got {other:?}"),
        }

        let mux_oversized = Task::new(
            TaskSpec::Command {
                program: "cat".into(),
                args: vec![],
                env: HashMap::new(),
                working_dir: None,
                stdin: Some(vec![b'y'; MAX_FRAME_SIZE + 1024]),
            },
            TaskRequirements::generic(1, 64),
        );
        let mux_oversized_id = mux_oversized.id;
        let (mux_waiter_tx, mux_waiter_rx) = tokio::sync::oneshot::channel();
        mux_waiters
            .write()
            .await
            .entry(mux_oversized_id)
            .or_default()
            .push(mux_waiter_tx);
        mux_queue.submit(mux_oversized).await.unwrap();

        let mux_scheduler = crate::scheduler::WorkloadScheduler::new(
            crate::scheduler::SchedulerConfig::default(),
            mux_registry.clone(),
            mux_queue.clone(),
            Arc::clone(&mux_notify),
        );
        assert_eq!(
            mux_scheduler
                .schedule_batch(10)
                .await
                .unwrap()
                .assignments
                .len(),
            1
        );

        let mux_res = tokio::time::timeout(Duration::from_secs(2), mux_waiter_rx)
            .await
            .expect("mux waiter must be woken when data-lane assignment fails")
            .expect("mux waiter must receive TaskResult");
        assert!(!mux_res.is_success());
        let mux_worker_after_fail = mux_registry.get_worker(mux_worker_id).await.unwrap();
        assert_eq!(
            mux_worker_after_fail.active_tasks, 0,
            "mux worker registry active_tasks must return to 0 after unsendable assignment"
        );
        assert_ne!(
            mux_worker_after_fail.status,
            crate::registry::WorkerStatus::Busy,
            "mux worker registry status must not stay Busy after unsendable assignment"
        );

        // Because the 1-slot mux worker's slot was released, a second task can now be scheduled
        // and arrives on `mux_data`.
        let mux_sibling = Task::new(
            TaskSpec::command("echo", vec!["mux-ok".into()]),
            TaskRequirements::generic(1, 64),
        );
        let mux_sibling_id = mux_sibling.id;
        mux_queue.submit(mux_sibling).await.unwrap();
        assert_eq!(
            mux_scheduler
                .schedule_batch(10)
                .await
                .unwrap()
                .assignments
                .len(),
            1
        );

        let mux_assigned =
            tokio::time::timeout(Duration::from_secs(2), mux_data.recv_msg::<MasterMessage>())
                .await
                .expect("mux data lane must receive sibling AssignTask")
                .expect("mux data lane must not error")
                .expect("mux data lane must stay open");
        match mux_assigned {
            MasterMessage::AssignTask { task } => assert_eq!(task.id, mux_sibling_id),
            other => panic!("expected mux sibling AssignTask, got {other:?}"),
        }

        let _ = stop_tx.send(true);
        mux_conn.abort();
    }

    #[tokio::test]
    async fn test_claim_master_role_is_refused_with_and_without_secret_and_sends_no_tasks() {
        use rusty_grid_core::arbitrage::{MasterLeaseTicket, LEASE_VALIDITY_DURATION_MS};
        use rusty_grid_core::cluster_auth::client_handshake;
        use rusty_grid_core::task::{TaskRequirements, TaskSpec};

        for with_secret in [false, true] {
            let secret = with_secret.then(|| ClusterSecret::generate().unwrap());
            let registry = WorkerRegistry::new();
            let queue = TaskQueue::new();
            let scheduler_notify = Arc::new(tokio::sync::Notify::new());
            let waiters: WaiterMap = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
            let (stop_tx, stop_rx) = watch::channel(false);

            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let signing_secret = secret
                .clone()
                .unwrap_or_else(|| ClusterSecret::generate().unwrap());
            let high_score_ticket = MasterLeaseTicket::issue(
                Uuid::new_v4(),
                Uuid::new_v4(),
                10,
                9500,
                now_ms,
                Duration::from_millis(LEASE_VALIDITY_DURATION_MS),
                &signing_secret,
            );

            let assert_refused_ack = |ack: MasterMessage, context: &str| match ack {
                MasterMessage::RoleHandoverAck {
                    accepted,
                    reason,
                    task_queue_snapshot,
                    connected_workers,
                } => {
                    assert!(
                        !accepted,
                        "ClaimMasterRole must be refused ({context}, with_secret={with_secret})"
                    );
                    assert_eq!(
                        reason.as_deref(),
                        Some(
                            "Automatic master failover is not supported; use manual role switching"
                        ),
                        "unexpected refusal reason ({context}, with_secret={with_secret})"
                    );
                    assert!(
                        task_queue_snapshot.is_empty(),
                        "refused ClaimMasterRole must not leak task_queue_snapshot ({context}, with_secret={with_secret})"
                    );
                    assert!(
                        connected_workers.is_empty(),
                        "refused ClaimMasterRole must not leak connected_workers ({context}, with_secret={with_secret})"
                    );
                }
                other => panic!(
                    "expected RoleHandoverAck ({context}, with_secret={with_secret}), got {other:?}"
                ),
            };

            let spawn_tcp = |m_end: tokio::io::DuplexStream, port: u16| {
                let reg_clone = registry.clone();
                let queue_clone = queue.clone();
                let notify_clone = Arc::clone(&scheduler_notify);
                let waiters_clone = Arc::clone(&waiters);
                let stop_rx_clone = stop_rx.clone();
                let secret_clone = secret.clone();
                tokio::spawn(async move {
                    handle_connection(
                        m_end,
                        SocketAddr::from(([127, 0, 0, 1], port)),
                        reg_clone,
                        queue_clone,
                        notify_clone,
                        waiters_clone,
                        10,
                        Duration::from_secs(5),
                        stop_rx_clone,
                        None,
                        WireCodec::Bincode,
                        secret_clone,
                        None,
                        4,
                        None,
                        #[cfg(feature = "dashboard")]
                        None,
                        #[cfg(feature = "p2p")]
                        None,
                        #[cfg(feature = "p2p")]
                        None,
                    )
                    .await
                })
            };

            let spawn_mux = |c_master: tokio::io::DuplexStream,
                             d_master: tokio::io::DuplexStream,
                             port: u16| {
                let reg_clone = registry.clone();
                let queue_clone = queue.clone();
                let notify_clone = Arc::clone(&scheduler_notify);
                let waiters_clone = Arc::clone(&waiters);
                let stop_rx_clone = stop_rx.clone();
                let secret_clone = secret.clone();
                tokio::spawn(async move {
                    handle_multiplexed_connection(
                        c_master,
                        d_master,
                        SocketAddr::from(([127, 0, 0, 1], port)),
                        reg_clone,
                        queue_clone,
                        notify_clone,
                        waiters_clone,
                        10,
                        Duration::from_secs(5),
                        stop_rx_clone,
                        None,
                        WireCodec::Bincode,
                        secret_clone,
                        None,
                        None,
                        #[cfg(feature = "dashboard")]
                        None,
                        #[cfg(feature = "p2p")]
                        None,
                        #[cfg(feature = "p2p")]
                        None,
                    )
                    .await
                })
            };

            // 1. Before any task is queued: send ClaimMasterRole as handshake message on handle_connection.
            {
                let (m_end, w_end) = tokio::io::duplex(64 * 1024);
                let conn_task = spawn_tcp(m_end, 50001);

                let mut w_transport = MessageTransport::new(w_end);
                if let Some(ref sec) = secret {
                    let key = client_handshake(&mut w_transport, sec).await.unwrap();
                    w_transport.enable_encryption(&key, EndpointRole::Client);
                }
                w_transport
                    .send_msg(&WorkerMessage::ClaimMasterRole {
                        ticket: high_score_ticket.clone(),
                    })
                    .await
                    .unwrap();
                let ack = w_transport
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap()
                    .expect("handshake ClaimMasterRole must receive RoleHandoverAck");
                assert_refused_ack(ack, "pre-queue handshake");
                conn_task.await.unwrap().unwrap();
                assert!(registry.list_workers().await.is_empty());
            }

            // 2. Queue a task so the queue is non-empty, then test TCP handshake, multiplexed handshake,
            //    active TCP session, and active multiplexed session.
            let queued_task = Task::new(
                TaskSpec::command("echo", vec!["queued-task".into()]),
                TaskRequirements::generic(1, 64),
            );
            queue.submit(queued_task).await.unwrap();
            assert_eq!(queue.get_schedulable_tasks().await.len(), 1);

            // 2a. TCP handshake ClaimMasterRole after a task is queued.
            {
                let (m_end, w_end) = tokio::io::duplex(64 * 1024);
                let conn_task = spawn_tcp(m_end, 50002);

                let mut w_transport = MessageTransport::new(w_end);
                if let Some(ref sec) = secret {
                    let key = client_handshake(&mut w_transport, sec).await.unwrap();
                    w_transport.enable_encryption(&key, EndpointRole::Client);
                }
                w_transport
                    .send_msg(&WorkerMessage::ClaimMasterRole {
                        ticket: high_score_ticket.clone(),
                    })
                    .await
                    .unwrap();
                let ack = w_transport
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap()
                    .expect("post-queue handshake ClaimMasterRole must receive RoleHandoverAck");
                assert_refused_ack(ack, "post-queue TCP handshake");
                conn_task.await.unwrap().unwrap();
                assert!(
                    registry.list_workers().await.is_empty(),
                    "refused TCP handshake claim must not register a worker"
                );
            }

            // 2b. Multiplexed (P2P) handshake ClaimMasterRole as first control message after a task is queued.
            {
                let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
                let (d_master, _d_worker) = tokio::io::duplex(64 * 1024);
                let mux_hs_task = spawn_mux(c_master, d_master, 50003);

                let mut ctrl_transport = MessageTransport::new(c_worker);
                if let Some(ref sec) = secret {
                    let key = client_handshake(&mut ctrl_transport, sec).await.unwrap();
                    ctrl_transport.enable_encryption(&key, EndpointRole::Client);
                }
                ctrl_transport
                    .send_msg(&WorkerMessage::ClaimMasterRole {
                        ticket: high_score_ticket.clone(),
                    })
                    .await
                    .unwrap();
                let ack = ctrl_transport
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap()
                    .expect(
                        "post-queue mux handshake ClaimMasterRole must receive RoleHandoverAck",
                    );
                assert_refused_ack(ack, "post-queue multiplexed handshake");
                drop(ctrl_transport);
                mux_hs_task.await.unwrap().unwrap();
                assert!(
                    registry.list_workers().await.is_empty(),
                    "refused multiplexed handshake claim must not register a worker"
                );
            }

            // 2c. Active registered session on handle_connection.
            {
                let (m_end, w_end) = tokio::io::duplex(64 * 1024);
                let conn_task = spawn_tcp(m_end, 50004);

                let mut w_transport = MessageTransport::new(w_end);
                if let Some(ref sec) = secret {
                    let key = client_handshake(&mut w_transport, sec).await.unwrap();
                    w_transport.enable_encryption(&key, EndpointRole::Client);
                }
                let worker_id = Uuid::new_v4();
                w_transport
                    .send_msg(&WorkerMessage::Register {
                        worker_id,
                        capabilities: WorkerCapabilities::new(
                            "feat6-worker",
                            2,
                            4096,
                            false,
                            false,
                            None,
                        ),
                        protocol_version: PROTOCOL_VERSION_CURRENT,
                        features: ProtocolCapabilities::all_supported()
                            | ProtocolCapabilities::ROLE_ARBITRAGE,
                    })
                    .await
                    .unwrap();
                match w_transport.recv_msg::<MasterMessage>().await.unwrap() {
                    Some(MasterMessage::RegisterAck {
                        accepted: true,
                        features,
                        ..
                    }) => {
                        assert!(
                            !features.contains(ProtocolCapabilities::ROLE_ARBITRAGE),
                            "negotiated features must not include ROLE_ARBITRAGE"
                        );
                    }
                    other => panic!("expected RegisterAck, got {other:?}"),
                }

                w_transport
                    .send_msg(&WorkerMessage::ClaimMasterRole {
                        ticket: high_score_ticket.clone(),
                    })
                    .await
                    .unwrap();
                let ack = w_transport
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap()
                    .expect("active session ClaimMasterRole must receive RoleHandoverAck");
                assert_refused_ack(ack, "post-queue active TCP session");
                assert_eq!(
                    registry.get_worker(worker_id).await.map(|w| w.status),
                    Some(crate::registry::WorkerStatus::Connected),
                    "worker must remain Connected in registry after refused ClaimMasterRole on active TCP session"
                );

                // Session remains alive and responds to Heartbeat after refusal.
                w_transport
                    .send_msg(&WorkerMessage::Heartbeat {
                        worker_id,
                        timestamp: 42,
                        active_tasks: 0,
                        cpu_usage_pct: 10.0,
                        ram_available_mb: 2048,
                    })
                    .await
                    .unwrap();
                match w_transport.recv_msg::<MasterMessage>().await.unwrap() {
                    Some(MasterMessage::HeartbeatAck { timestamp: 42 }) => {}
                    other => {
                        panic!("expected HeartbeatAck after refused ClaimMasterRole, got {other:?}")
                    }
                }
                assert_eq!(
                    registry.get_worker(worker_id).await.map(|w| w.status),
                    Some(crate::registry::WorkerStatus::Connected)
                );
                conn_task.abort();
            }

            // 2d. Active multiplexed session on handle_multiplexed_connection.
            {
                let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
                let (d_master, _d_worker) = tokio::io::duplex(64 * 1024);
                let mux_task = spawn_mux(c_master, d_master, 50005);

                let mut ctrl_transport = MessageTransport::new(c_worker);
                if let Some(ref sec) = secret {
                    let key = client_handshake(&mut ctrl_transport, sec).await.unwrap();
                    ctrl_transport.enable_encryption(&key, EndpointRole::Client);
                }
                let mux_worker_id = Uuid::new_v4();
                ctrl_transport
                    .send_msg(&WorkerMessage::Register {
                        worker_id: mux_worker_id,
                        capabilities: WorkerCapabilities::new(
                            "feat6-mux-worker",
                            2,
                            4096,
                            false,
                            false,
                            None,
                        ),
                        protocol_version: PROTOCOL_VERSION_CURRENT,
                        features: ProtocolCapabilities::all_supported()
                            | ProtocolCapabilities::ROLE_ARBITRAGE,
                    })
                    .await
                    .unwrap();
                match ctrl_transport.recv_msg::<MasterMessage>().await.unwrap() {
                    Some(MasterMessage::RegisterAck {
                        accepted: true,
                        features,
                        ..
                    }) => {
                        assert!(
                            !features.contains(ProtocolCapabilities::ROLE_ARBITRAGE),
                            "mux negotiated features must not include ROLE_ARBITRAGE"
                        );
                    }
                    other => panic!("expected mux RegisterAck, got {other:?}"),
                }

                ctrl_transport
                    .send_msg(&WorkerMessage::ClaimMasterRole {
                        ticket: high_score_ticket.clone(),
                    })
                    .await
                    .unwrap();
                let ack = ctrl_transport
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap()
                    .expect("mux active session ClaimMasterRole must receive RoleHandoverAck");
                assert_refused_ack(ack, "post-queue active multiplexed session");
                assert_eq!(
                    registry.get_worker(mux_worker_id).await.map(|w| w.status),
                    Some(crate::registry::WorkerStatus::Connected),
                    "worker must remain Connected in registry after refused ClaimMasterRole on active multiplexed session"
                );

                // Multiplexed control lane remains alive and responds to Heartbeat after refusal.
                ctrl_transport
                    .send_msg(&WorkerMessage::Heartbeat {
                        worker_id: mux_worker_id,
                        timestamp: 84,
                        active_tasks: 0,
                        cpu_usage_pct: 12.0,
                        ram_available_mb: 2048,
                    })
                    .await
                    .unwrap();
                match ctrl_transport.recv_msg::<MasterMessage>().await.unwrap() {
                    Some(MasterMessage::HeartbeatAck { timestamp: 84 }) => {}
                    other => {
                        panic!("expected mux HeartbeatAck after refused ClaimMasterRole, got {other:?}")
                    }
                }
                assert_eq!(
                    registry.get_worker(mux_worker_id).await.map(|w| w.status),
                    Some(crate::registry::WorkerStatus::Connected)
                );

                let _ = stop_tx.send(true);
                mux_task.abort();
            }
        }
    }

    #[tokio::test]
    async fn test_master_server_bind_enforces_max_queue_size_and_json_wire_codec() {
        use rusty_grid_core::task::{TaskRequirements, TaskSpec};
        let server_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_max_queue_size(Some(1))
            .with_wire_codec(WireCodec::Json);
        let server = MasterServer::bind(server_cfg, WorkerRegistry::new())
            .await
            .unwrap();
        let addr = server.local_addr;
        let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let server_task = tokio::spawn(server.run(stop_rx));

        // Connect with a Bincode client and submit 1st task: master must reply in Json and accept.
        let stream1 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut client1 = MessageTransport::with_codec(stream1, WireCodec::Bincode);
        client1
            .send_msg(&ClientMessage::SubmitTask {
                task: Task::new(
                    TaskSpec::command("echo", vec!["first".into()]),
                    TaskRequirements::generic(1, 30),
                ),
                wait: false,
            })
            .await
            .unwrap();
        let (reply1, codec1) = client1
            .recv_msg_with_codec::<ClientResponse>()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            codec1,
            WireCodec::Json,
            "MasterServer::bind with WireCodec::Json must reply in Json even to a Bincode client"
        );
        assert!(
            matches!(reply1, ClientResponse::TaskSubmitted { .. }),
            "1st submit must succeed under max_queue_size = Some(1), got: {reply1:?}"
        );

        // Submit 2nd task while 1st task is queued: must be rejected by `max_queue_size = Some(1)`.
        let stream2 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut client2 = MessageTransport::with_codec(stream2, WireCodec::Bincode);
        client2
            .send_msg(&ClientMessage::SubmitTask {
                task: Task::new(
                    TaskSpec::command("echo", vec!["second".into()]),
                    TaskRequirements::generic(1, 30),
                ),
                wait: false,
            })
            .await
            .unwrap();
        let (reply2, codec2) = client2
            .recv_msg_with_codec::<ClientResponse>()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(codec2, WireCodec::Json);
        match reply2 {
            ClientResponse::Error { message } => {
                assert!(
                    message.contains(
                        "Task queue is full (1 queued or running tasks; wait, or raise --max-queue-size)"
                    ),
                    "expected actionable QueueFull(1) error from MasterServer::bind, got: {message}"
                );
            }
            other => panic!("expected ClientResponse::Error for 2nd submit, got {other:?}"),
        }

        server_task.abort();

        // Also verify multiplexed P2P (`handle_multiplexed_connection`) with `WireCodec::Json`:
        // a worker registering with Bincode must receive both its `RegisterAck` (control lane)
        // and `AssignTask` (data lane) in `WireCodec::Json`.
        let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
        let (d_master, d_worker) = tokio::io::duplex(64 * 1024);
        let (_mux_stop_tx, mux_stop_rx) = tokio::sync::watch::channel(false);
        let mux_reg1 = WorkerRegistry::new();
        let mux_queue1 = TaskQueue::new();
        let mux_notify1 = Arc::new(tokio::sync::Notify::new());
        let reg1_clone = mux_reg1.clone();
        let queue1_clone = mux_queue1.clone();
        let notify1_clone = Arc::clone(&mux_notify1);
        let mux_task = tokio::spawn(async move {
            handle_multiplexed_connection(
                c_master,
                d_master,
                SocketAddr::from(([127, 0, 0, 1], 50099)),
                reg1_clone,
                queue1_clone,
                notify1_clone,
                Arc::new(tokio::sync::RwLock::new(HashMap::new())),
                10,
                Duration::from_secs(5),
                mux_stop_rx,
                None,
                WireCodec::Json,
                None,
                None,
                None,
                #[cfg(feature = "dashboard")]
                None,
                #[cfg(feature = "p2p")]
                None,
                #[cfg(feature = "p2p")]
                None,
            )
            .await
        });

        let mut w_ctrl = MessageTransport::with_codec(c_worker, WireCodec::Bincode);
        let mut w_data = MessageTransport::with_codec(d_worker, WireCodec::Bincode);
        w_ctrl
            .send_msg(&WorkerMessage::Register {
                worker_id: Uuid::new_v4(),
                capabilities: WorkerCapabilities::new(
                    "mux-json-check",
                    2,
                    4096,
                    false,
                    false,
                    None,
                ),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: rusty_grid_core::protocol::ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        let (mux_reply, mux_codec) = w_ctrl
            .recv_msg_with_codec::<MasterMessage>()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            mux_codec,
            WireCodec::Json,
            "handle_multiplexed_connection with WireCodec::Json must reply in Json on ctrl_transport even to a Bincode worker"
        );
        assert!(matches!(
            mux_reply,
            MasterMessage::RegisterAck { accepted: true, .. }
        ));

        mux_queue1
            .submit(Task::new(
                TaskSpec::command("echo", vec!["mux-json-data-lane".into()]),
                TaskRequirements::generic(1, 30),
            ))
            .await
            .unwrap();
        let sched1 = WorkloadScheduler::new(
            SchedulerConfig::default(),
            mux_reg1,
            mux_queue1,
            mux_notify1,
        );
        assert_eq!(sched1.schedule_once().await.unwrap().assignments.len(), 1);
        let (json_data_msg, json_data_codec) = w_data
            .recv_msg_with_codec::<MasterMessage>()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            json_data_codec,
            WireCodec::Json,
            "handle_multiplexed_connection with WireCodec::Json must send AssignTask in Json on data_transport even to a Bincode worker"
        );
        assert!(matches!(json_data_msg, MasterMessage::AssignTask { .. }));
        mux_task.abort();

        // Conversely, when `handle_multiplexed_connection` is configured with `WireCodec::Bincode`
        // (default), a worker registering with `WireCodec::Json` must have `detected_codec` mirrored
        // onto BOTH `ctrl_transport` (`RegisterAck`) and `data_transport` (`AssignTask`).
        let (c_master2, c_worker2) = tokio::io::duplex(64 * 1024);
        let (d_master2, d_worker2) = tokio::io::duplex(64 * 1024);
        let (_mux_stop_tx2, mux_stop_rx2) = tokio::sync::watch::channel(false);
        let mux_reg2 = WorkerRegistry::new();
        let mux_queue2 = TaskQueue::new();
        let mux_notify2 = Arc::new(tokio::sync::Notify::new());
        let reg2_clone = mux_reg2.clone();
        let queue2_clone = mux_queue2.clone();
        let notify2_clone = Arc::clone(&mux_notify2);
        let mux_task2 = tokio::spawn(async move {
            handle_multiplexed_connection(
                c_master2,
                d_master2,
                SocketAddr::from(([127, 0, 0, 1], 50100)),
                reg2_clone,
                queue2_clone,
                notify2_clone,
                Arc::new(tokio::sync::RwLock::new(HashMap::new())),
                10,
                Duration::from_secs(5),
                mux_stop_rx2,
                None,
                WireCodec::Bincode,
                None,
                None,
                None,
                #[cfg(feature = "dashboard")]
                None,
                #[cfg(feature = "p2p")]
                None,
                #[cfg(feature = "p2p")]
                None,
            )
            .await
        });

        let mut w_ctrl2 = MessageTransport::with_codec(c_worker2, WireCodec::Json);
        let mut w_data2 = MessageTransport::with_codec(d_worker2, WireCodec::Json);
        w_ctrl2
            .send_msg(&WorkerMessage::Register {
                worker_id: Uuid::new_v4(),
                capabilities: WorkerCapabilities::new(
                    "mux-mirror-check",
                    2,
                    4096,
                    false,
                    false,
                    None,
                ),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: rusty_grid_core::protocol::ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        let (mux_reply2, mux_codec2) = w_ctrl2
            .recv_msg_with_codec::<MasterMessage>()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            mux_codec2,
            WireCodec::Json,
            "handle_multiplexed_connection with WireCodec::Bincode must mirror detected Json codec onto ctrl_transport"
        );
        assert!(matches!(
            mux_reply2,
            MasterMessage::RegisterAck { accepted: true, .. }
        ));

        mux_queue2
            .submit(Task::new(
                TaskSpec::command("echo", vec!["mux-data-lane".into()]),
                TaskRequirements::generic(1, 30),
            ))
            .await
            .unwrap();
        let sched2 = WorkloadScheduler::new(
            SchedulerConfig::default(),
            mux_reg2,
            mux_queue2,
            mux_notify2,
        );
        assert_eq!(sched2.schedule_once().await.unwrap().assignments.len(), 1);
        let (data_msg, data_codec) = w_data2
            .recv_msg_with_codec::<MasterMessage>()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            data_codec,
            WireCodec::Json,
            "handle_multiplexed_connection with WireCodec::Bincode must mirror detected Json codec onto data_transport"
        );
        assert!(matches!(data_msg, MasterMessage::AssignTask { .. }));
        mux_task2.abort();
    }

    #[cfg(feature = "p2p")]
    #[tokio::test]
    async fn test_p2p_handshake_refusals_delivered_over_real_iroh() {
        use iroh::endpoint::presets::Minimal;
        use iroh::endpoint::{QuicTransportConfig, RelayMode, VarInt};
        use rusty_grid_core::arbitrage::{MasterLeaseTicket, LEASE_VALIDITY_DURATION_MS};
        use rusty_grid_core::cluster_auth::{
            client_handshake, ClusterSecret, AUTH_VERSION, REJECT_NO_SECRET, WIRE_FORMAT_AUTH,
        };
        use rusty_grid_core::protocol::EndpointRole;
        use rusty_grid_core::transport::{BiStream, GRID_ALPN, STREAM_CONTROL, STREAM_DATA};

        let test_secret = ClusterSecret::generate().unwrap();
        let signing_secret = test_secret.clone();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let claim_ticket = MasterLeaseTicket::issue(
            Uuid::new_v4(),
            Uuid::new_v4(),
            10,
            9500,
            now_ms,
            Duration::from_millis(LEASE_VALIDITY_DURATION_MS),
            &signing_secret,
        );

        for secret in [None, Some(test_secret.clone())] {
            let mut cfg =
                ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_pending_handshakes(1);
            cfg.cluster_secret = secret.clone();
            let mut server = MasterServer::bind(cfg, WorkerRegistry::new())
                .await
                .unwrap();
            let master_ep = iroh::Endpoint::builder(Minimal)
                .relay_mode(RelayMode::Disabled)
                .alpns(vec![GRID_ALPN.to_vec()])
                .bind()
                .await
                .unwrap();
            let master_addr = master_ep.addr();
            server.p2p_endpoint = Some(master_ep.clone());

            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let server_task = tokio::spawn(server.run(stop_rx));

            let client_ep = iroh::Endpoint::builder(Minimal)
                .relay_mode(RelayMode::Disabled)
                .alpns(vec![GRID_ALPN.to_vec()])
                .bind()
                .await
                .unwrap();

            let open_raw_ctrl = || async {
                let conn = client_ep
                    .connect(master_addr.clone(), GRID_ALPN)
                    .await
                    .expect("connect to master over loopback iroh");
                let (mut ctrl_send, ctrl_recv) = conn.open_bi().await.expect("open ctrl bi");
                ctrl_send.write_all(&[STREAM_CONTROL]).await.unwrap();
                let (mut data_send, data_recv) = conn.open_bi().await.expect("open data bi");
                data_send.write_all(&[STREAM_DATA]).await.unwrap();
                let transport =
                    MessageTransport::new(GridStream::P2p(BiStream::new(ctrl_recv, ctrl_send)));
                (transport, (conn, data_send, data_recv))
            };

            let open_ctrl = || async {
                let (mut transport, keep) = open_raw_ctrl().await;
                if let Some(ref sec) = secret {
                    let key = client_handshake(&mut transport, sec)
                        .await
                        .expect("client handshake over iroh");
                    transport.enable_encryption(&key, EndpointRole::Client);
                }
                (transport, keep)
            };

            for half_close in [false, true] {
                for iter in 0..8 {
                    // 1. ClaimMasterRole refusal on the P2P handshake path.
                    let (mut ctrl, keep) = open_ctrl().await;
                    ctrl.send_msg(&WorkerMessage::ClaimMasterRole {
                        ticket: claim_ticket.clone(),
                    })
                    .await
                    .unwrap();
                    if half_close {
                        tokio::io::AsyncWriteExt::shutdown(ctrl.get_mut())
                            .await
                            .unwrap();
                    }
                    let claim_reply = ctrl
                        .recv_msg::<MasterMessage>()
                        .await
                        .unwrap_or_else(|e| {
                            panic!(
                                "iter {iter} (secret={}, half_close={half_close}): ClaimMasterRole refusal read failed: {e}",
                                secret.is_some()
                            )
                        })
                        .unwrap_or_else(|| {
                            panic!(
                                "iter {iter} (secret={}, half_close={half_close}): ClaimMasterRole refusal stream closed before frame arrived",
                                secret.is_some()
                            )
                        });
                    match claim_reply {
                        MasterMessage::RoleHandoverAck {
                            accepted: false,
                            ref reason,
                            ref task_queue_snapshot,
                            ref connected_workers,
                        } => {
                            assert!(reason.is_some());
                            assert!(task_queue_snapshot.is_empty());
                            assert!(connected_workers.is_empty());
                        }
                        other => panic!("expected refused RoleHandoverAck, got {other:?}"),
                    }
                    assert!(
                        ctrl.recv_msg::<MasterMessage>().await.unwrap().is_none(),
                        "master must finish the control send stream after ClaimMasterRole refusal"
                    );
                    keep.0.close(0u32.into(), b"done");

                    // 2. RegisterAck { accepted: false } refusal (cpu_cores == 0) on the P2P handshake path.
                    let (mut ctrl_reg, keep_reg) = open_ctrl().await;
                    ctrl_reg
                        .send_msg(&WorkerMessage::Register {
                            worker_id: Uuid::new_v4(),
                            capabilities: WorkerCapabilities::new(
                                "zero-core-worker",
                                0,
                                2048,
                                false,
                                false,
                                None,
                            ),
                            protocol_version: PROTOCOL_VERSION_CURRENT,
                            features: ProtocolCapabilities::all_supported(),
                        })
                        .await
                        .unwrap();
                    if half_close {
                        tokio::io::AsyncWriteExt::shutdown(ctrl_reg.get_mut())
                            .await
                            .unwrap();
                    }
                    let reg_reply = ctrl_reg
                        .recv_msg::<MasterMessage>()
                        .await
                        .unwrap_or_else(|e| {
                            panic!(
                                "iter {iter} (secret={}, half_close={half_close}): RegisterAck (cpu_cores=0) refusal read failed: {e}",
                                secret.is_some()
                            )
                        })
                        .unwrap_or_else(|| {
                            panic!(
                                "iter {iter} (secret={}, half_close={half_close}): RegisterAck (cpu_cores=0) refusal stream closed before frame arrived",
                                secret.is_some()
                            )
                        });
                    match reg_reply {
                        MasterMessage::RegisterAck {
                            accepted: false,
                            ref message,
                            ..
                        } => {
                            assert!(
                                message
                                    .as_deref()
                                    .unwrap_or_default()
                                    .contains("cpu_cores must be > 0"),
                                "unexpected refusal message: {message:?}"
                            );
                        }
                        other => panic!("expected refused RegisterAck, got {other:?}"),
                    }
                    assert!(
                        ctrl_reg
                            .recv_msg::<MasterMessage>()
                            .await
                            .unwrap()
                            .is_none(),
                        "master must finish the control send stream after cpu_cores=0 refusal"
                    );
                    keep_reg.0.close(0u32.into(), b"done");

                    // 3. RegisterAck { accepted: false } refusal (incompatible protocol_version) on P2P handshake path.
                    let (mut ctrl_ver, keep_ver) = open_ctrl().await;
                    ctrl_ver
                        .send_msg(&WorkerMessage::Register {
                            worker_id: Uuid::new_v4(),
                            capabilities: WorkerCapabilities::new(
                                "old-proto-worker",
                                4,
                                2048,
                                false,
                                false,
                                None,
                            ),
                            protocol_version: 0,
                            features: ProtocolCapabilities::all_supported(),
                        })
                        .await
                        .unwrap();
                    if half_close {
                        tokio::io::AsyncWriteExt::shutdown(ctrl_ver.get_mut())
                            .await
                            .unwrap();
                    }
                    let ver_reply = ctrl_ver
                        .recv_msg::<MasterMessage>()
                        .await
                        .unwrap_or_else(|e| {
                            panic!(
                                "iter {iter} (secret={}, half_close={half_close}): RegisterAck (protocol_version=0) refusal read failed: {e}",
                                secret.is_some()
                            )
                        })
                        .unwrap_or_else(|| {
                            panic!(
                                "iter {iter} (secret={}, half_close={half_close}): RegisterAck (protocol_version=0) refusal stream closed before frame arrived",
                                secret.is_some()
                            )
                        });
                    match ver_reply {
                        MasterMessage::RegisterAck {
                            accepted: false,
                            ref message,
                            ..
                        } => {
                            assert!(
                                message
                                    .as_deref()
                                    .unwrap_or_default()
                                    .contains("Incompatible protocol version"),
                                "unexpected refusal message: {message:?}"
                            );
                        }
                        other => panic!("expected refused RegisterAck, got {other:?}"),
                    }
                    assert!(
                        ctrl_ver.recv_msg::<MasterMessage>().await.unwrap().is_none(),
                        "master must finish the control send stream after protocol_version=0 refusal"
                    );
                    keep_ver.0.close(0u32.into(), b"done");

                    // 4. RegisterAck { accepted: false } refusal (unexpected first WorkerMessage) on P2P handshake path.
                    let (mut ctrl_other, keep_other) = open_ctrl().await;
                    ctrl_other
                        .send_msg(&WorkerMessage::Heartbeat {
                            worker_id: Uuid::new_v4(),
                            timestamp: 1,
                            active_tasks: 0,
                            cpu_usage_pct: 0.0,
                            ram_available_mb: 1024,
                        })
                        .await
                        .unwrap();
                    if half_close {
                        tokio::io::AsyncWriteExt::shutdown(ctrl_other.get_mut())
                            .await
                            .unwrap();
                    }
                    let other_reply = ctrl_other
                        .recv_msg::<MasterMessage>()
                        .await
                        .unwrap_or_else(|e| {
                            panic!(
                                "iter {iter} (secret={}, half_close={half_close}): RegisterAck (unexpected Heartbeat) refusal read failed: {e}",
                                secret.is_some()
                            )
                        })
                        .unwrap_or_else(|| {
                            panic!(
                                "iter {iter} (secret={}, half_close={half_close}): RegisterAck (unexpected Heartbeat) refusal stream closed before frame arrived",
                                secret.is_some()
                            )
                        });
                    match other_reply {
                        MasterMessage::RegisterAck {
                            accepted: false,
                            ref message,
                            ..
                        } => {
                            assert!(
                                message
                                    .as_deref()
                                    .unwrap_or_default()
                                    .contains("Expected Register message, received Heartbeat"),
                                "unexpected refusal message: {message:?}"
                            );
                        }
                        other => panic!("expected refused RegisterAck, got {other:?}"),
                    }
                    assert!(
                        ctrl_other
                            .recv_msg::<MasterMessage>()
                            .await
                            .unwrap()
                            .is_none(),
                        "master must finish the control send stream after unexpected Heartbeat refusal"
                    );
                    keep_other.0.close(0u32.into(), b"done");

                    // 5. Cluster-auth refusal on the P2P handshake path (8 of 8 in both normal and half-close modes):
                    //    - When `secret` is `None`: a client sending an auth HELLO receives `REJECT_NO_SECRET`.
                    //    - When `secret` is `Some`: an unauthenticated client sending `Register` receives
                    //      `REJECT_AUTH_REQUIRED` (`InvalidFormatTag(WIRE_FORMAT_AUTH)` -> `GridError::AuthRequired`).
                    let (mut ctrl_auth, keep_auth) = open_raw_ctrl().await;
                    if secret.is_none() {
                        if !half_close {
                            let err = client_handshake(&mut ctrl_auth, &test_secret)
                                .await
                                .expect_err("master without secret must reject client_handshake");
                            match err {
                                GridError::AuthFailed(ref msg)
                                    if msg.contains("no cluster secret configured") => {}
                                other => panic!(
                                    "iter {iter} (half_close={half_close}): expected AuthFailed(no cluster secret configured), got {other:?}"
                                ),
                            }
                        } else {
                            let mut hello_frame =
                                vec![0, 0, 0, 35, WIRE_FORMAT_AUTH, 0x01, AUTH_VERSION];
                            hello_frame.extend_from_slice(&[7u8; 32]);
                            tokio::io::AsyncWriteExt::write_all(ctrl_auth.get_mut(), &hello_frame)
                                .await
                                .unwrap();
                            tokio::io::AsyncWriteExt::shutdown(ctrl_auth.get_mut())
                                .await
                                .unwrap();
                            let reject_frame = ctrl_auth
                                .recv_raw_frame_max(1024)
                                .await
                                .unwrap()
                                .expect("REJECT_NO_SECRET frame must arrive after half-close");
                            assert_eq!(reject_frame[0], WIRE_FORMAT_AUTH);
                            assert_eq!(reject_frame[1], 0x05);
                            assert_eq!(&reject_frame[2..], REJECT_NO_SECRET.as_bytes());
                        }
                        assert!(
                            ctrl_auth.recv_raw_frame_max(1024).await.unwrap().is_none(),
                            "master must finish the control send stream after REJECT_NO_SECRET"
                        );
                    } else {
                        ctrl_auth
                            .send_msg(&WorkerMessage::Register {
                                worker_id: Uuid::new_v4(),
                                capabilities: WorkerCapabilities::new(
                                    "unauth-worker",
                                    2,
                                    2048,
                                    false,
                                    false,
                                    None,
                                ),
                                protocol_version: PROTOCOL_VERSION_CURRENT,
                                features: ProtocolCapabilities::all_supported(),
                            })
                            .await
                            .unwrap();
                        if half_close {
                            tokio::io::AsyncWriteExt::shutdown(ctrl_auth.get_mut())
                                .await
                                .unwrap();
                        }
                        let err = ctrl_auth.recv_msg::<MasterMessage>().await.expect_err(
                            "unauthenticated Register on secret master must be rejected",
                        );
                        assert!(
                            matches!(err, ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH)),
                            "iter {iter} (half_close={half_close}): expected InvalidFormatTag(WIRE_FORMAT_AUTH), got {err:?}"
                        );
                        let grid_err: GridError = err.into();
                        assert!(matches!(grid_err, GridError::AuthRequired(_)));
                        assert!(
                            ctrl_auth.recv_raw_frame_max(1024).await.unwrap().is_none(),
                            "master must finish the control send stream after REJECT_AUTH_REQUIRED"
                        );
                    }
                    keep_auth.0.close(0u32.into(), b"done");
                }
            }

            // 6. Silent peer check + `max_pending_handshakes(1)` release during auth-refusal drain:
            //    - While a first peer that failed cluster auth is still open inside
            //      `drain_refusal_stream` (holding its connection open without closing `conn`),
            //      its `handshake_permit` must already be dropped so a second peer is admitted
            //      under `max_pending_handshakes(1)`.
            //    - When a refused peer reads the refusal frame and stays silent without closing
            //      `conn`, the master keeps `conn` open for `REFUSAL_DRAIN_TIMEOUT` (`1 s`) and
            //      closes it between 0.9 s and 3.0 s.
            {
                let (mut ctrl_auth_drain, keep_auth_drain) = open_raw_ctrl().await;
                if secret.is_none() {
                    let err = client_handshake(&mut ctrl_auth_drain, &test_secret)
                        .await
                        .expect_err("master without secret must reject client_handshake");
                    assert!(matches!(err, GridError::AuthFailed(_)));
                } else {
                    ctrl_auth_drain
                        .send_msg(&WorkerMessage::Register {
                            worker_id: Uuid::new_v4(),
                            capabilities: WorkerCapabilities::new(
                                "unauth-drain-holder",
                                2,
                                2048,
                                false,
                                false,
                                None,
                            ),
                            protocol_version: PROTOCOL_VERSION_CURRENT,
                            features: ProtocolCapabilities::all_supported(),
                        })
                        .await
                        .unwrap();
                    let err = ctrl_auth_drain
                        .recv_msg::<MasterMessage>()
                        .await
                        .expect_err("unauthenticated Register must be rejected");
                    assert!(matches!(
                        err,
                        ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH)
                    ));
                }
                // `keep_auth_drain.0` is intentionally kept open so the master task is parked inside
                // `drain_refusal_stream` waiting up to 1 s for `conn.closed()`. With
                // `max_pending_handshakes(1)`, a second peer connecting right now is admitted only
                // if `drop(handshake_permit)` happened before `drain_refusal_stream`.
                let (mut ctrl_silent, keep_silent) = open_ctrl().await;
                let started = tokio::time::Instant::now();
                ctrl_silent
                    .send_msg(&WorkerMessage::ClaimMasterRole {
                        ticket: claim_ticket.clone(),
                    })
                    .await
                    .unwrap();
                let reply = ctrl_silent
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap()
                    .unwrap();
                assert!(matches!(
                    reply,
                    MasterMessage::RoleHandoverAck {
                        accepted: false,
                        ..
                    }
                ));
                keep_auth_drain.0.close(0u32.into(), b"done");
                tokio::time::timeout(Duration::from_secs(5), keep_silent.0.closed())
                    .await
                    .expect("silent refused peer must be closed before 5 s timeout");
                let elapsed = started.elapsed();
                assert!(
                    elapsed >= Duration::from_millis(900) && elapsed <= Duration::from_secs(3),
                    "silent refused peer must be closed after ~1 s (0.9..=3.0 s), took {elapsed:?}"
                );
            }

            let _ = stop_tx.send(true);
            client_ep.close().await;
            master_ep.close().await;
            let _ = server_task.await;
        }

        // 7. Zero flow-control credit peer with `max_connections = 1`:
        //    A peer whose QUIC stream receive window is 1 byte (smaller than the refusal frame)
        //    causes `transport.send_msg` on the master to block on flow-control credit. The single
        //    1 s timeout around `send_msg`, `shutdown`, and `conn.closed()` aborts the stalled
        //    refusal within 3.0 s and frees the single connection slot.
        {
            let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_connections(1);
            let mut server = MasterServer::bind(cfg, WorkerRegistry::new())
                .await
                .unwrap();
            let master_ep = iroh::Endpoint::builder(Minimal)
                .relay_mode(RelayMode::Disabled)
                .alpns(vec![GRID_ALPN.to_vec()])
                .bind()
                .await
                .unwrap();
            let master_addr = master_ep.addr();
            server.p2p_endpoint = Some(master_ep.clone());

            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let server_task = tokio::spawn(server.run(stop_rx));

            let tiny_window_quic = QuicTransportConfig::builder()
                .stream_receive_window(VarInt::from_u32(1))
                .build();
            let stalled_ep = iroh::Endpoint::builder(Minimal)
                .relay_mode(RelayMode::Disabled)
                .alpns(vec![GRID_ALPN.to_vec()])
                .transport_config(tiny_window_quic)
                .bind()
                .await
                .unwrap();

            let stalled_conn = stalled_ep
                .connect(master_addr.clone(), GRID_ALPN)
                .await
                .unwrap();
            let (mut c_send, c_recv) = stalled_conn.open_bi().await.unwrap();
            c_send.write_all(&[STREAM_CONTROL]).await.unwrap();
            let (mut d_send, _d_recv) = stalled_conn.open_bi().await.unwrap();
            d_send.write_all(&[STREAM_DATA]).await.unwrap();
            let mut stalled_ctrl =
                MessageTransport::new(GridStream::P2p(BiStream::new(c_recv, c_send)));
            let started = tokio::time::Instant::now();
            stalled_ctrl
                .send_msg(&WorkerMessage::ClaimMasterRole {
                    ticket: claim_ticket.clone(),
                })
                .await
                .unwrap();
            // Do not read from `stalled_ctrl`; its 1-byte stream receive window blocks `send_msg`.
            tokio::time::timeout(Duration::from_secs(5), stalled_conn.closed())
                .await
                .expect("peer granting no flow-control credit must be closed before 5 s timeout");
            let elapsed = started.elapsed();
            assert!(
                elapsed <= Duration::from_secs(3),
                "peer granting no flow-control credit must be closed within 3.0 s, took {elapsed:?}"
            );

            // Now `max_connections = 1` slot is free again: a normal peer must be admitted.
            let normal_ep = iroh::Endpoint::builder(Minimal)
                .relay_mode(RelayMode::Disabled)
                .alpns(vec![GRID_ALPN.to_vec()])
                .bind()
                .await
                .unwrap();
            let normal_conn = normal_ep.connect(master_addr, GRID_ALPN).await.unwrap();
            let (mut nc_send, nc_recv) = normal_conn.open_bi().await.unwrap();
            nc_send.write_all(&[STREAM_CONTROL]).await.unwrap();
            let (mut nd_send, _nd_recv) = normal_conn.open_bi().await.unwrap();
            nd_send.write_all(&[STREAM_DATA]).await.unwrap();
            let mut normal_ctrl =
                MessageTransport::new(GridStream::P2p(BiStream::new(nc_recv, nc_send)));
            normal_ctrl
                .send_msg(&WorkerMessage::ClaimMasterRole {
                    ticket: claim_ticket,
                })
                .await
                .unwrap();
            let reply = normal_ctrl
                .recv_msg::<MasterMessage>()
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                reply,
                MasterMessage::RoleHandoverAck {
                    accepted: false,
                    ..
                }
            ));
            normal_conn.close(0u32.into(), b"done");

            let _ = stop_tx.send(true);
            stalled_ep.close().await;
            normal_ep.close().await;
            master_ep.close().await;
            let _ = server_task.await;
        }
    }

    #[test]
    fn test_master_spawn_warns_when_p2p_minimal_preset_is_enabled_and_ignores_env() {
        #[derive(Default)]
        struct WarnVisitor(String);
        impl tracing::field::Visit for WarnVisitor {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "message" {
                    self.0.push_str(value);
                }
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    use std::fmt::Write;
                    let _ = write!(&mut self.0, "{value:?}");
                }
            }
        }
        struct WarnCollector {
            warnings: Arc<std::sync::Mutex<Vec<String>>>,
            thread_id: std::thread::ThreadId,
        }
        impl tracing::Subscriber for WarnCollector {
            fn register_callsite(
                &self,
                metadata: &'static tracing::Metadata<'static>,
            ) -> tracing::subscriber::Interest {
                if *metadata.level() <= tracing::Level::WARN {
                    tracing::subscriber::Interest::always()
                } else {
                    tracing::subscriber::Interest::never()
                }
            }
            fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
                *metadata.level() <= tracing::Level::WARN
            }
            fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
                Some(tracing::metadata::LevelFilter::WARN)
            }
            fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {
            }
            fn event(&self, event: &tracing::Event<'_>) {
                if *event.metadata().level() == tracing::Level::WARN
                    && std::thread::current().id() == self.thread_id
                {
                    let mut v = WarnVisitor::default();
                    event.record(&mut v);
                    self.warnings.lock().unwrap().push(v.0);
                }
            }
            fn enter(&self, _span: &tracing::span::Id) {}
            fn exit(&self, _span: &tracing::span::Id) {}
        }

        let warnings: Arc<std::sync::Mutex<Vec<String>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let thread_id = std::thread::current().id();
        let dispatch = tracing::Dispatch::new(WarnCollector {
            warnings: warnings.clone(),
            thread_id,
        });
        let _second_dispatch = tracing::Dispatch::new(WarnCollector {
            warnings: warnings.clone(),
            thread_id,
        });

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tracing::dispatcher::with_default(&dispatch, || {
            rt.block_on(async {
                tracing::callsite::rebuild_interest_cache();
                let default_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap());
                assert!(!default_cfg.p2p_minimal_preset);
                let m1 = MasterServer::spawn(default_cfg).await.expect("spawn m1");
                assert!(
                    !warnings
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|w| w.contains("P2P minimal preset is enabled (p2p_minimal_preset=true)")),
                    "without with_p2p_minimal_preset(true), MasterServer::spawn must not emit the minimal preset warning"
                );
                let _ = m1.shutdown();

                warnings.lock().unwrap().clear();
                tracing::callsite::rebuild_interest_cache();
                let p2p_off_min_cfg =
                    ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_p2p_minimal_preset(true);
                let m_off = MasterServer::spawn(p2p_off_min_cfg)
                    .await
                    .expect("spawn m_off");
                assert!(
                    !warnings
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|w| w
                            .contains("P2P minimal preset is enabled (p2p_minimal_preset=true)")),
                    "when P2P is off, MasterServer::spawn must not warn about P2P minimal preset"
                );
                let _ = m_off.shutdown();

                warnings.lock().unwrap().clear();
                tracing::callsite::rebuild_interest_cache();
                let min_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
                    .with_p2p(true)
                    .with_p2p_minimal_preset(true)
                    .with_ephemeral_key(true);
                let m2 = MasterServer::spawn(min_cfg).await.expect("spawn m2");
                assert!(
                    warnings
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|w| w.contains("P2P minimal preset is enabled (p2p_minimal_preset=true)")),
                    "with_p2p_minimal_preset(true) must emit a tracing::warn! on MasterServer::spawn, got: {:?}",
                    *warnings.lock().unwrap()
                );
                let _ = m2.shutdown();
            });
        });

        let server_source = include_str!("server.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        assert!(
            !server_source
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .any(|l| l.contains("RUSTY_GRID_P2P_MINIMAL")),
            "crates/master/src/server.rs library code must not read RUSTY_GRID_P2P_MINIMAL from the environment"
        );
    }

    #[tokio::test]
    async fn test_client_and_worker_protocol_version_mismatch_refused_on_both_sides() {
        fn stamp_frame_version<M: serde::Serialize>(
            msg: &M,
            version: u32,
            codec: WireCodec,
        ) -> Vec<u8> {
            let mut raw = rusty_grid_core::protocol::serialize_message(msg, codec)
                .unwrap()
                .to_vec();
            match codec {
                WireCodec::Bincode => {
                    let len = raw.len();
                    raw[len - 4..].copy_from_slice(&version.to_le_bytes());
                }
                WireCodec::Json => {
                    let mut val: serde_json::Value = serde_json::from_slice(&raw[1..]).unwrap();
                    val["protocol_version"] = serde_json::Value::from(version);
                    raw.truncate(1);
                    serde_json::to_writer(&mut raw, &val).unwrap();
                }
            }
            raw
        }

        let server = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("spawn master");
        let addr = server.server_addr;

        // 1. Worker with lower (2, current - 1) or higher (current + 1) protocol_version in both
        // Bincode and JSON is refused by Master with a clear RegisterAck message.
        for codec in [WireCodec::Bincode, WireCodec::Json] {
            for bad_worker_ver in [
                2u32,
                PROTOCOL_VERSION_CURRENT - 1,
                PROTOCOL_VERSION_CURRENT + 1,
            ] {
                let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
                let mut transport = MessageTransport::with_codec(stream, codec);
                transport
                    .send_msg(&WorkerMessage::Register {
                        worker_id: Uuid::new_v4(),
                        capabilities: WorkerCapabilities::new(
                            "skew-worker",
                            4,
                            4096,
                            false,
                            false,
                            None,
                        ),
                        protocol_version: bad_worker_ver,
                        features: ProtocolCapabilities::all_supported(),
                    })
                    .await
                    .unwrap();
                let ack = transport
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap()
                    .expect("RegisterAck");
                match ack {
                    MasterMessage::RegisterAck {
                        accepted,
                        protocol_version,
                        message,
                        ..
                    } => {
                        assert!(!accepted);
                        assert_eq!(protocol_version, PROTOCOL_VERSION_CURRENT);
                        let msg = message.unwrap_or_default();
                        assert!(
                            msg.contains(&format!(
                                "Incompatible protocol version {bad_worker_ver}"
                            )) && msg.contains(&PROTOCOL_VERSION_CURRENT.to_string()),
                            "expected version mismatch message for {codec:?}, got: {msg}"
                        );
                    }
                    other => panic!("expected RegisterAck, got {other:?}"),
                }
            }
        }

        // 2. Client with lower (2, current - 1) or higher (current + 1) protocol_version in both
        // Bincode and JSON receives a clear ClientResponse::Error from the Master, and a client
        // reading a mismatched ClientResponse from a fake Master also refuses with
        // GridError::IncompatibleVersion.
        for codec in [WireCodec::Bincode, WireCodec::Json] {
            for bad_client_ver in [
                2u32,
                PROTOCOL_VERSION_CURRENT - 1,
                PROTOCOL_VERSION_CURRENT + 1,
            ] {
                let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
                let mut transport = MessageTransport::with_codec(stream, codec);
                let raw_frame =
                    stamp_frame_version(&ClientMessage::ClusterStatus, bad_client_ver, codec);
                transport.send_raw_frame(raw_frame.into()).await.unwrap();

                let reply = transport
                    .recv_msg::<ClientResponse>()
                    .await
                    .expect("client must decode Master's Error response cleanly")
                    .expect("ClientResponse::Error frame");
                match reply {
                    ClientResponse::Error { message } => {
                        assert!(
                            message.contains(&format!(
                                "Incompatible protocol version {bad_client_ver}"
                            )) && message.contains(&PROTOCOL_VERSION_CURRENT.to_string()),
                            "expected clear version error from Master, got: {message}"
                        );
                    }
                    other => panic!("expected ClientResponse::Error, got {other:?}"),
                }
            }

            // Client side: receiving a non-Error ClientResponse with an incompatible protocol_version
            // fails with GridError::IncompatibleVersion naming both versions (without a
            // "Registration handshake failed:" prefix).
            for bad_master_ver in [PROTOCOL_VERSION_CURRENT - 1, PROTOCOL_VERSION_CURRENT + 2] {
                let (client_io, master_io) = tokio::io::duplex(4096);
                tokio::spawn(async move {
                    let mut fake_master = MessageTransport::with_codec(master_io, codec);
                    let _ = fake_master
                        .recv_msg::<InboundMessage>()
                        .await
                        .unwrap()
                        .unwrap();
                    let bad_resp = stamp_frame_version(
                        &ClientResponse::WorkerList {
                            workers: Vec::new(),
                        },
                        bad_master_ver,
                        codec,
                    );
                    fake_master.send_raw_frame(bad_resp.into()).await.unwrap();
                });
                let mut client_transport = MessageTransport::with_codec(client_io, codec);
                client_transport
                    .send_msg(&ClientMessage::ListWorkers)
                    .await
                    .unwrap();
                let err: GridError = client_transport
                    .recv_msg::<ClientResponse>()
                    .await
                    .expect_err("client must refuse incompatible Master ClientResponse")
                    .into();
                assert!(
                    !err.to_string().contains("Registration handshake failed:"),
                    "client refusal must not carry worker handshake prefix: {err}"
                );
                match &err {
                    GridError::IncompatibleVersion(msg) => {
                        assert!(
                            msg.contains(&format!(
                                "Incompatible protocol version {bad_master_ver}"
                            )) && msg.contains(&PROTOCOL_VERSION_CURRENT.to_string()),
                            "expected clear client-side version refusal, got: {msg}"
                        );
                    }
                    other => panic!("expected GridError::IncompatibleVersion, got {other:?}"),
                }
            }
        }

        let _ = server.shutdown();
    }

    #[tokio::test]
    async fn test_concurrent_submissions_share_group_commit_and_recover_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("state");

        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir);
        let handle = MasterServer::spawn(config).await.unwrap();

        let total_tasks = 1_000usize;
        let start = std::time::Instant::now();
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..total_tasks {
            let h = handle.clone();
            join_set.spawn(async move {
                let task = Task::new(
                    rusty_grid_core::TaskSpec::command("echo", vec![format!("task-{i}")]),
                    rusty_grid_core::TaskRequirements::generic(1, 10),
                );
                h.submit_task(task).await.unwrap()
            });
        }
        let mut submitted_ids = Vec::with_capacity(total_tasks);
        while let Some(res) = join_set.join_next().await {
            submitted_ids.push(res.unwrap());
        }
        let elapsed = start.elapsed();
        let syncs = handle.queue().store_sync_count().await.unwrap_or(0);
        assert!(
            elapsed < Duration::from_secs(2),
            "1,000 concurrent durable submissions took {:?} (expected < 2s with group commit)",
            elapsed
        );
        assert!(
            syncs > 0 && syncs < 200,
            "expected group commit to batch 1,000 submissions into < 200 syncs, got {syncs}"
        );

        handle.shutdown().unwrap();

        // Reopen MasterServer on the same state_dir and verify all 1,000 tasks are recovered.
        let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir);
        let handle2 = MasterServer::spawn(config2).await.unwrap();
        let stats = handle2.queue_stats().await.unwrap();
        assert_eq!(stats.total, total_tasks);
        assert_eq!(stats.queued, total_tasks);
        for id in submitted_ids.iter().take(16) {
            assert_eq!(handle2.queue().get_state(id).await, Some(TaskState::Queued));
        }
        handle2.shutdown().unwrap();
    }

    #[tokio::test]
    async fn test_state_dir_repins_input_and_output_blobs_across_restart() {
        use rusty_grid_core::blob::{BlobFile, BlobId, TaskInput};
        use rusty_grid_core::blob_transfer::{upload_blob, BlobChannel, BlobError};

        struct StoreConn {
            store: Arc<BlobStore>,
            owner: u64,
        }
        impl BlobChannel for StoreConn {
            async fn call(&self, request: BlobRequest) -> Result<BlobReply, BlobError> {
                Ok(self.store.handle(self.owner, request).await)
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("state");
        let blob_dir = state_dir.join("blobs");

        let input_bytes = vec![b'I'; 1024];
        let output_bytes = vec![b'O'; 1024];
        let unpinned_bytes = vec![b'U'; 512];
        let filler_bytes = vec![b'F'; 1536];
        let input_id = BlobId::of(&input_bytes);
        let output_id = BlobId::of(&output_bytes);
        let unpinned_id = BlobId::of(&unpinned_bytes);
        let filler_id = BlobId::of(&filler_bytes);

        let (queued_task_id, completed_task_id) = {
            let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
                .with_web_ui(false)
                .with_discovery(false)
                .with_state_dir(&state_dir)
                .with_blob_store(&blob_dir, 2560);
            let handle = MasterServer::spawn(config).await.unwrap();
            let bstore = handle.blob_store.as_ref().unwrap();
            let conn = StoreConn {
                store: Arc::clone(bstore),
                owner: bstore.new_owner(),
            };

            upload_blob(&conn, input_id, input_bytes.len() as u64, &input_bytes[..])
                .await
                .unwrap();
            upload_blob(
                &conn,
                output_id,
                output_bytes.len() as u64,
                &output_bytes[..],
            )
            .await
            .unwrap();
            upload_blob(
                &conn,
                unpinned_id,
                unpinned_bytes.len() as u64,
                &unpinned_bytes[..],
            )
            .await
            .unwrap();

            let mut queued_task = Task::new(
                rusty_grid_core::TaskSpec::command("echo", vec!["in".into()]),
                rusty_grid_core::TaskRequirements::generic(1, 10),
            );
            queued_task.inputs.push(TaskInput::file(
                "input.bin",
                input_id,
                input_bytes.len() as u64,
                false,
            ));
            let queued_task_id = handle.submit_task(queued_task).await.unwrap();

            let completed_task = Task::new(
                rusty_grid_core::TaskSpec::command("echo", vec!["out".into()]),
                rusty_grid_core::TaskRequirements::generic(1, 10),
            );
            let completed_task_id = handle.submit_task(completed_task).await.unwrap();
            let worker_id = Uuid::new_v4();
            handle
                .queue()
                .schedule_tasks_batch_in_sessions(&[(completed_task_id, worker_id, 1)])
                .await;
            let mut result = TaskResult::success(worker_id, completed_task_id, "ok", 5, false);
            result.outputs.push(BlobFile {
                path: "output.bin".into(),
                id: output_id,
                size: output_bytes.len() as u64,
                executable: false,
            });
            let pin = bstore.check_and_pin_outputs(&[output_id]).unwrap();
            handle
                .queue()
                .record_result_from(worker_id, Some(1), result)
                .await
                .unwrap();
            pin.commit_terminal(completed_task_id).await;

            handle.shutdown().unwrap();
            (queued_task_id, completed_task_id)
        };

        // Restart master on the same state_dir with the same 2560-byte blob cap.
        // Because `repin_recovered_blobs` re-pins `input_id` and `output_id` (2048 B total),
        // attempting to upload a 1536-byte filler blob must fail to evict either pinned blob.
        let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir)
            .with_blob_store(&blob_dir, 2560);
        let handle2 = MasterServer::spawn(config2).await.unwrap();
        let bstore2 = handle2.blob_store.as_ref().unwrap();
        let conn2 = StoreConn {
            store: Arc::clone(bstore2),
            owner: bstore2.new_owner(),
        };

        assert_eq!(bstore2.size_of(&input_id), Some(input_bytes.len() as u64));
        assert_eq!(bstore2.size_of(&output_id), Some(output_bytes.len() as u64));
        assert_eq!(
            bstore2.size_of(&unpinned_id),
            Some(unpinned_bytes.len() as u64)
        );
        assert_eq!(bstore2.stats().pinned_blobs, 2);

        upload_blob(
            &conn2,
            filler_id,
            filler_bytes.len() as u64,
            &filler_bytes[..],
        )
        .await
        .unwrap();

        assert_eq!(
            bstore2.size_of(&unpinned_id),
            None,
            "unpinned blob should be evicted first when uploading filler blob"
        );
        assert_eq!(
            bstore2.size_of(&input_id),
            Some(input_bytes.len() as u64),
            "pinned recovered input blob must survive LRU eviction"
        );
        assert_eq!(
            bstore2.size_of(&output_id),
            Some(output_bytes.len() as u64),
            "pinned recovered output blob must survive LRU eviction"
        );
        assert_eq!(
            handle2.queue().get_state(&queued_task_id).await,
            Some(TaskState::Queued)
        );
        assert_eq!(
            handle2.queue().get_state(&completed_task_id).await,
            Some(TaskState::Completed)
        );
        handle2.shutdown().unwrap();
    }

    #[tokio::test]
    async fn test_storage_failure_on_result_releases_worker_slot_unpins_blobs_and_wakes_waiters() {
        use rusty_grid_core::blob::{BlobId, TaskInput};
        use rusty_grid_core::blob_transfer::{upload_blob, BlobChannel, BlobError};

        struct StoreConn {
            store: Arc<BlobStore>,
            owner: u64,
        }
        impl BlobChannel for StoreConn {
            async fn call(&self, request: BlobRequest) -> Result<BlobReply, BlobError> {
                Ok(self.store.handle(self.owner, request).await)
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("state");
        let blob_dir = state_dir.join("blobs");
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir)
            .with_blob_store(&blob_dir, 64 * 1024);
        let handle = MasterServer::spawn(config).await.unwrap();
        let bstore = handle.blob_store.as_ref().unwrap();
        let conn = StoreConn {
            store: Arc::clone(bstore),
            owner: bstore.new_owner(),
        };

        let input_bytes = vec![b'X'; 512];
        let input_id = BlobId::of(&input_bytes);
        upload_blob(&conn, input_id, input_bytes.len() as u64, &input_bytes[..])
            .await
            .unwrap();

        let worker_id = Uuid::new_v4();
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let session_id = handle
            .registry()
            .register(
                worker_id,
                WorkerCapabilities::new("slot-worker", 4, 4096, false, false, None),
                "127.0.0.1:9999".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();

        let mut task = Task::new(
            rusty_grid_core::TaskSpec::command("echo", vec!["hi".into()]),
            rusty_grid_core::TaskRequirements::generic(1, 10),
        );
        task.inputs.push(TaskInput::file(
            "input.bin",
            input_id,
            input_bytes.len() as u64,
            false,
        ));
        let task_id = handle.submit_task(task).await.unwrap();
        assert_eq!(bstore.stats().pinned_blobs, 1);

        let (waiter_tx, waiter_rx) = tokio::sync::oneshot::channel();
        handle
            .waiters
            .write()
            .await
            .entry(task_id)
            .or_default()
            .push(waiter_tx);

        handle
            .queue()
            .schedule_tasks_batch_in_sessions(&[(task_id, worker_id, session_id)])
            .await;
        handle
            .registry()
            .increment_active_tasks(&worker_id)
            .await
            .unwrap();
        assert_eq!(
            handle
                .registry()
                .get_worker(worker_id)
                .await
                .unwrap()
                .active_tasks,
            1
        );

        // Inject storage sync failure before the result arrives.
        handle.queue().inject_store_sync_failure(1).await;

        apply_task_result(
            handle.registry(),
            handle.queue(),
            handle.scheduler_notify(),
            &handle.waiters,
            worker_id,
            Some(session_id),
            TaskResult::success(worker_id, task_id, "done", 3, false),
            handle.blob_store.as_ref(),
            #[cfg(feature = "dashboard")]
            None,
        )
        .await;

        let waited = tokio::time::timeout(Duration::from_secs(1), waiter_rx)
            .await
            .expect("waiter must be woken even when TaskStore sync fails")
            .expect("waiter channel must receive result");
        assert_eq!(waited.exit_code, 0);
        assert_eq!(
            handle
                .registry()
                .get_worker(worker_id)
                .await
                .unwrap()
                .active_tasks,
            0,
            "worker slot must be released even when TaskStore sync fails"
        );
        assert_eq!(
            bstore.stats().pinned_blobs,
            0,
            "task input blobs must be unpinned even when TaskStore sync fails"
        );
        handle.shutdown().unwrap();
    }

    #[tokio::test]
    async fn test_second_master_refused_on_locked_state_dir_leaves_blobs_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("state");
        let config1 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir);
        let handle1 = MasterServer::spawn(config1).await.unwrap();

        // Simulate an in-flight upload file inside `<state_dir>/blobs/tmp/` on the running master.
        let staged_tmp = state_dir.join("blobs").join("tmp").join("upload-1.part");
        std::fs::create_dir_all(staged_tmp.parent().unwrap()).unwrap();
        std::fs::write(&staged_tmp, b"in-flight-upload-bytes").unwrap();
        assert!(staged_tmp.exists());

        // Starting a second master on the same `--state-dir` must fail on `store.lock` BEFORE
        // `BlobStore::open` runs and purges `blobs/tmp/`.
        let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir);
        let Err(err) = MasterServer::spawn(config2).await else {
            panic!("expected second MasterServer::spawn on locked state_dir to fail");
        };
        assert!(
            err.to_string().contains("store.lock"),
            "expected store.lock refusal on locked state_dir, got {err:?}"
        );
        assert!(
            staged_tmp.exists(),
            "second master refused on locked state_dir must not purge running master's blobs/tmp/ files"
        );
        assert_eq!(
            std::fs::read(&staged_tmp).unwrap(),
            b"in-flight-upload-bytes"
        );

        handle1.shutdown().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_worker_session_50_results_batch_in_group_commit() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("state");
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir);
        let handle = MasterServer::spawn(config).await.unwrap();

        let worker_id = Uuid::new_v4();
        let (tx, _rx) = tokio::sync::mpsc::channel(128);
        let session_id = handle
            .registry()
            .register(
                worker_id,
                WorkerCapabilities::new("batch-worker", 64, 65536, false, false, None),
                "127.0.0.1:9998".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();

        let count = 50usize;
        let mut task_ids = Vec::with_capacity(count);
        let mut waiter_rxs = Vec::with_capacity(count);
        for i in 0..count {
            let t = Task::new(
                rusty_grid_core::TaskSpec::command("echo", vec![format!("r-{i}")]),
                rusty_grid_core::TaskRequirements::generic(1, 10),
            );
            let tid = handle.submit_task(t).await.unwrap();
            let (wtx, wrx) = tokio::sync::oneshot::channel();
            handle
                .waiters
                .write()
                .await
                .entry(tid)
                .or_default()
                .push(wtx);
            task_ids.push(tid);
            waiter_rxs.push(wrx);
        }

        let assign: Vec<_> = task_ids
            .iter()
            .map(|tid| (*tid, worker_id, session_id))
            .collect();
        handle
            .queue()
            .schedule_tasks_batch_in_sessions(&assign)
            .await;

        let syncs_before = handle.queue().store_sync_count().await.unwrap();
        let start = std::time::Instant::now();
        for tid in &task_ids {
            handle_session_task_message(
                WorkerMessage::TaskResult {
                    worker_id,
                    task_id: *tid,
                    exit_code: 0,
                    stdout: "ok".into(),
                    stderr: String::new().into(),
                    execution_time_ms: 1,
                    is_gpu_executed: false,
                    device_name: None,
                    error: None,
                    asset_bytes: None,
                    artifacts: Vec::new(),
                    outputs: Vec::new(),
                },
                handle.registry(),
                handle.queue(),
                handle.scheduler_notify(),
                &handle.waiters,
                worker_id,
                session_id,
                handle.blob_store.as_ref(),
                #[cfg(feature = "dashboard")]
                None,
            )
            .await;
        }

        for wrx in waiter_rxs {
            let res = tokio::time::timeout(Duration::from_secs(2), wrx)
                .await
                .expect("waiter timed out")
                .expect("waiter dropped");
            assert_eq!(res.exit_code, 0);
        }
        let elapsed = start.elapsed();
        let syncs_after = handle.queue().store_sync_count().await.unwrap();
        let sync_delta = syncs_after.saturating_sub(syncs_before);

        assert!(
            sync_delta > 0 && sync_delta < 20,
            "expected >=50 sequential session results to coalesce into < 20 syncs, got {sync_delta} (elapsed {elapsed:?})"
        );

        handle.shutdown().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_submit_after_shutdown_returns_err_and_clean_shutdown_does_not_charge_running_tasks(
    ) {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("state_part_a");
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir);
        let handle = MasterServer::spawn(config).await.unwrap();
        handle.shutdown().unwrap();

        let late_task = Task::new(
            rusty_grid_core::TaskSpec::command("echo", vec!["late".into()]),
            rusty_grid_core::TaskRequirements::generic(1, 10),
        );
        let late_id = late_task.id;
        let res = handle.submit_task(late_task).await;
        assert!(
            matches!(res, Err(GridError::Storage(_))),
            "submit_task after shutdown() must return Err(GridError::Storage), got {res:?}"
        );

        let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir);
        let handle2 = MasterServer::spawn(config2).await.unwrap();
        assert_eq!(handle2.queue().get_state(&late_id).await, None);
        handle2.shutdown().unwrap();

        // Part B: clean shutdown while a worker is executing a task must never charge failover_count.
        for round in 0..4 {
            let round_dir = dir.path().join(format!("state_round_{round}"));
            let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
                .with_web_ui(false)
                .with_discovery(false)
                .with_state_dir(&round_dir)
                .with_session_resume_hold(Duration::ZERO);
            let h = MasterServer::spawn(cfg).await.unwrap();
            let addr = h.server_addr().to_string();
            let (wtx, wrx) = tokio::sync::watch::channel(false);
            let mut w = rusty_grid_worker::client::WorkerClient::for_testing(
                addr,
                Some(format!("clean-shutdown-{round}")),
                Some(1),
                false,
            );
            let wh = tokio::spawn(async move {
                let _ = w.run(wrx).await;
            });
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while h.list_workers().await.map(|v| v.len()).unwrap_or(0) < 1
                && std::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let mut req = rusty_grid_core::TaskRequirements::generic(1, 16);
            req.max_retries = Some(2);
            let running_task = Task::new(
                rusty_grid_core::TaskSpec::BuiltinTest {
                    test_name: "long".into(),
                    iterations: 4,
                    duration_ms: 5_000,
                    should_fail: false,
                    require_gpu: false,
                },
                req,
            );
            let id = h.submit_task(running_task).await.unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while h.queue().get_state(&id).await != Some(TaskState::Running)
                && std::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            h.shutdown().unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = wtx.send(true);
            wh.abort();

            let cfg_reopen = ServerConfig::new("127.0.0.1:0".parse().unwrap())
                .with_web_ui(false)
                .with_discovery(false)
                .with_state_dir(&round_dir);
            let h2 = MasterServer::spawn(cfg_reopen).await.unwrap();
            let info = h2.queue().get_task(&id).await.unwrap();
            assert_eq!(
                info.failover_count, 0,
                "round {round}: clean master shutdown must not charge failover_count"
            );
            assert_eq!(
                info.retry_count, 0,
                "round {round}: clean master shutdown must not charge retry_count"
            );
            assert!(
                h2.queue()
                    .get_retry_history(&id)
                    .await
                    .unwrap_or_default()
                    .is_empty(),
                "round {round}: clean master shutdown must not append a RetryAttempt"
            );
            h2.shutdown().unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rvb_submit_in_flight_during_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let sd = dir.path().join("s");
        let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&sd);
        let h = MasterServer::spawn(cfg).await.unwrap();
        let mut ids = Vec::new();
        let mut js = Vec::new();
        for i in 0..200 {
            let t = Task::new(
                rusty_grid_core::TaskSpec::command("echo", vec![format!("f{i}")]),
                rusty_grid_core::TaskRequirements::generic(1, 10),
            );
            ids.push(t.id);
            let h2 = h.clone();
            js.push(tokio::spawn(async move { h2.submit_task(t).await }));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        h.shutdown().unwrap();
        let mut oks = std::collections::HashSet::new();
        for (i, j) in js.into_iter().enumerate() {
            if j.await.unwrap().is_ok() {
                oks.insert(ids[i]);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let cfg2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&sd);
        let h2 = MasterServer::spawn(cfg2).await.unwrap();
        let mut lost = 0;
        for id in &oks {
            if h2.queue().get_state(id).await.is_none() {
                lost += 1;
            }
        }
        h2.shutdown().unwrap();
        assert_eq!(lost, 0, "acknowledged submissions must survive shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rvb_hundred_results_one_session() {
        let dir = tempfile::tempdir().unwrap();
        let sd = dir.path().join("s");
        let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&sd);
        let h = MasterServer::spawn(cfg).await.unwrap();
        let w = Uuid::new_v4();
        let (tx, _rx) = tokio::sync::mpsc::channel(512);
        let sid = h
            .registry()
            .register(
                w,
                WorkerCapabilities::new("w", 128, 65536, false, false, None),
                "127.0.0.1:9997".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();
        let mut ids = Vec::new();
        let mut rxs = Vec::new();
        for i in 0..100 {
            let t = Task::new(
                rusty_grid_core::TaskSpec::command("echo", vec![format!("r{i}")]),
                rusty_grid_core::TaskRequirements::generic(1, 10),
            );
            let id = h.submit_task(t).await.unwrap();
            let (a, b) = tokio::sync::oneshot::channel();
            h.waiters.write().await.entry(id).or_default().push(a);
            ids.push(id);
            rxs.push(b);
        }
        let assign: Vec<_> = ids.iter().map(|i| (*i, w, sid)).collect();
        h.queue().schedule_tasks_batch_in_sessions(&assign).await;
        let s0 = h.queue().store_sync_count().await.unwrap();
        for id in &ids {
            handle_session_task_message(
                WorkerMessage::TaskResult {
                    worker_id: w,
                    task_id: *id,
                    exit_code: 0,
                    stdout: "ok".into(),
                    stderr: String::new().into(),
                    execution_time_ms: 1,
                    is_gpu_executed: false,
                    device_name: None,
                    error: None,
                    asset_bytes: None,
                    artifacts: Vec::new(),
                    outputs: Vec::new(),
                },
                h.registry(),
                h.queue(),
                h.scheduler_notify(),
                &h.waiters,
                w,
                sid,
                h.blob_store.as_ref(),
                #[cfg(feature = "dashboard")]
                None,
            )
            .await;
        }
        for rx in rxs {
            let res = tokio::time::timeout(Duration::from_secs(3), rx)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(res.exit_code, 0);
        }
        let s1 = h.queue().store_sync_count().await.unwrap();
        assert!(
            s1 - s0 > 0 && s1 - s0 < 20,
            "100 results should coalesce into < 20 syncs, got {}",
            s1 - s0
        );
        h.shutdown().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rvb_result_after_shutdown_detach_is_not_durable() {
        let dir = tempfile::tempdir().unwrap();
        let sd = dir.path().join("s");
        let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&sd);
        let h = MasterServer::spawn(cfg).await.unwrap();
        let t = Task::new(
            rusty_grid_core::TaskSpec::command("echo", vec!["r".into()]),
            rusty_grid_core::TaskRequirements::generic(1, 10),
        );
        let id = h.submit_task(t).await.unwrap();
        let w = Uuid::new_v4();
        h.queue()
            .schedule_tasks_batch_in_sessions(&[(id, w, 1)])
            .await;
        h.shutdown().unwrap();

        let o = h
            .queue()
            .record_result_from(
                w,
                Some(1),
                rusty_grid_core::TaskResult::success(w, id, "o", 1, false),
            )
            .await;
        assert!(
            matches!(o, Err(GridError::Storage(_))),
            "result arriving after shutdown() detaches the store must be refused, got {o:?}"
        );
        assert_eq!(
            h.queue().get_state(&id).await,
            Some(TaskState::Scheduled),
            "refused post-shutdown result must not transition in-memory state to Completed"
        );

        let cfg2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&sd);
        let h2 = MasterServer::spawn(cfg2).await.unwrap();
        assert_eq!(h2.queue().get_state(&id).await, Some(TaskState::Queued));
        h2.shutdown().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_failed_store_is_reported_in_cluster_status_and_dashboard() {
        let dir = tempfile::tempdir().unwrap();
        let sd = dir.path().join("state");
        let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(true)
            .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
            .with_auth_config(crate::auth::AuthConfig::dev())
            .with_discovery(false)
            .with_state_dir(&sd);
        let h = MasterServer::spawn(cfg).await.unwrap();
        let addr = h.server_addr();

        // Trigger a real store sync failure via fault injection + submit.
        h.queue().inject_store_sync_failure(1).await;
        let bad_task = Task::new(
            rusty_grid_core::TaskSpec::command("echo", vec!["fail-sync".into()]),
            rusty_grid_core::TaskRequirements::generic(1, 10),
        );
        let submit_res = h.submit_task(bad_task).await;
        assert!(matches!(submit_res, Err(GridError::Storage(_))));

        // 1. QueueStats and TaskQueue::store_error report the failure reason.
        let q_stats = h.queue().stats().await;
        assert!(
            q_stats
                .store_error
                .as_deref()
                .is_some_and(|s| s.contains("injected sync failure")),
            "expected store_error in QueueStats, got {:?}",
            q_stats.store_error
        );

        // 2. ClientMessage::ClusterStatus carries store_error.
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut transport = MessageTransport::new(stream);
        transport
            .send_msg(&ClientMessage::ClusterStatus)
            .await
            .unwrap();
        match transport.recv_msg::<ClientResponse>().await.unwrap() {
            Some(ClientResponse::ClusterStatus { store_error, .. }) => {
                assert!(
                    store_error
                        .as_deref()
                        .is_some_and(|s| s.contains("injected sync failure")),
                    "expected store_error in ClientResponse::ClusterStatus, got {store_error:?}"
                );
            }
            other => panic!("expected ClusterStatus, got {other:?}"),
        }

        // 3. Dashboard /api/status carries store_error on both ClusterStatusDto and its tasks QueueStats,
        // and GET / serves dashboard.html with the storeErrorBanner alert.
        let dash_addr = h
            .dashboard_addr()
            .expect("dashboard must be enabled in this test");
        let status_dto: crate::dashboard::dto::ClusterStatusDto =
            reqwest::get(format!("http://{dash_addr}/api/status"))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
        assert!(
            status_dto
                .store_error
                .as_deref()
                .is_some_and(|s| s.contains("injected sync failure")),
            "expected store_error in ClusterStatusDto, got {:?}",
            status_dto.store_error
        );
        assert_eq!(status_dto.tasks.store_error, status_dto.store_error);

        let html = reqwest::get(format!("http://{dash_addr}/"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            html.contains("id=\"storeErrorBanner\"")
                && html.contains("Task store failed: durability is off until restart"),
            "dashboard.html must contain #storeErrorBanner for store_error visibility"
        );

        h.shutdown().unwrap();
    }
}
