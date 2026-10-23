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

use rusty_grid_core::capabilities::MobileLiveStatus;
use rusty_grid_core::cluster_auth::{
    send_reject, server_handshake, ClusterSecret, REJECT_NO_SECRET, WIRE_FORMAT_AUTH,
};
use rusty_grid_core::mapreduce::{MapReduceJobSpec, MapReduceResult};
use rusty_grid_core::protocol::{
    ClientMessage, ClientResponse, EndpointRole, InboundMessage, ProtocolCapabilities,
    ProtocolError, PROTOCOL_VERSION_CURRENT, PROTOCOL_VERSION_MIN_COMPATIBLE,
};
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskResult, TaskStatus};
#[cfg(feature = "p2p")]
use rusty_grid_core::transport::{BiStream, GridStream, GRID_ALPN, STREAM_CONTROL, STREAM_DATA};
use rusty_grid_core::{
    GridError, GridResult, MasterMessage, MessageTransport, WireCodec, WorkerCapabilities,
    WorkerMessage,
};

use crate::auth::DevBypass;
use crate::liveness::{RxClock, StampedStream};
use crate::mapreduce::{MapReduceEngine, DEFAULT_MAPREDUCE_PARALLELISM};
use crate::queue::{
    QueueStats, ResultDisposition, TaskInfo, TaskQueue, TaskState, DEFAULT_MAX_TERMINAL_BYTES,
    DEFAULT_MAX_TERMINAL_TASKS,
};
#[allow(unused_imports)]
use crate::reaper::spawn_reaper_with_queue;
use crate::reaper::ReaperConfig;
use crate::registry::{HeartbeatOutcome, WorkerInfo, WorkerRegistry};
use crate::scheduler::{SchedulerConfig, WorkloadScheduler};

/// Type alias for client waiters awaiting task completion via oneshot channels.
pub type WaiterMap = Arc<RwLock<HashMap<TaskId, Vec<oneshot::Sender<TaskResult>>>>>;

/// Default cap on simultaneously open connections on the grid port (TCP and QUIC together).
pub const DEFAULT_MAX_CONNECTIONS: usize = 1024;

/// Default cap on connections that have been accepted but have not yet authenticated and sent
/// their first message. They hold a socket for up to the handshake timeout without proving
/// anything, so they get a much smaller budget than established connections.
pub const DEFAULT_MAX_PENDING_HANDSHAKES: usize = 64;

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
    /// Tasks of one Map/Reduce job submitted at once (default: [`DEFAULT_MAPREDUCE_PARALLELISM`]);
    /// keep it above the cluster's total slots so every device works.
    pub mapreduce_parallelism: usize,
    /// Enables the loopback dev bypass automatically for a master that has neither a cluster
    /// secret nor any dashboard token, when its dashboard listener is bound to a loopback address,
    /// so a bare local master still has a usable dashboard (default: false; the CLI turns it on).
    /// See [`crate::auth::resolve_dashboard_auth`].
    pub implicit_local_dashboard: bool,
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
            mapreduce_parallelism: DEFAULT_MAPREDUCE_PARALLELISM,
            implicit_local_dashboard: false,
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
}

impl MasterServer {
    /// Binds TCP listener with provided registry, allocating default task queue and notify triggers.
    pub async fn bind(config: ServerConfig, registry: WorkerRegistry) -> GridResult<Self> {
        let queue = TaskQueue::new();
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
        #[cfg(feature = "p2p")]
        let p2p_active_conns = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        #[cfg(feature = "p2p")]
        let (p2p_endpoint, p2p_ticket) = if config.enable_p2p || config.p2p_key_file.is_some() {
            let resolved_key_file = if config.ephemeral_key {
                config.p2p_key_file.clone()
            } else {
                config.p2p_key_file.clone().or_else(default_master_key_file)
            };
            let secret_key = resolve_p2p_secret_key(resolved_key_file.as_deref()).await?;
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
                .secret_key(secret_key)
                .alpns(vec![GRID_ALPN.to_vec()])
                .bind()
                .await
                .map_err(|e| GridError::Config(format!("Failed to bind iroh endpoint: {e}")))?;

            // Await home DERP relay connection and STUN discovery before serializing ticket.
            // Timeout gracefully so offline/air-gapped environments don't hang indefinitely.
            match tokio::time::timeout(Duration::from_secs(3), endpoint.online()).await {
                Ok(_) => {
                    info!(
                        "Master connected to home DERP relay and discovered public network paths"
                    );
                }
                Err(_) => {
                    warn!("Timed out awaiting DERP relay readiness; proceeding with initial discovered addresses");
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
                        let _ = tokio::fs::create_dir_all(parent).await;
                    }
                }
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
            ..Default::default()
        };
        let queue = TaskQueue::with_retention_limits(
            retry_policy,
            None,
            config.max_terminal_tasks,
            config.max_terminal_bytes,
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
                                if !report.assignments.is_empty() {
                                    for assignment in &report.assignments {
                                        if let Some(info) = sched_queue.get_task(&assignment.task_id).await {
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
                                if !report.assignments.is_empty() {
                                    for assignment in &report.assignments {
                                        if let Some(info) = sched_queue.get_task(&assignment.task_id).await {
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
        };

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

/// Creates the key file exclusively with mode 0600 (best effort on Windows, where it inherits the
/// ACL of its directory) and writes `data`, so the key is never readable by others, not even
/// between creation and a later chmod.
#[cfg(feature = "p2p")]
fn write_new_private_key(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(path)?;
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

            tokio::spawn(async move {
                let mut p2p_shutdown = p2p_shutdown_rx;
                let mut reject_log = RejectLog::default();
                loop {
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

        Ok(())
    }
}

/// Dashboard broadcast channel type, so signatures stay readable.
#[cfg(feature = "dashboard")]
type DashboardTx = tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>;

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
    mut result: TaskResult,
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

    // 1. Record outcome in TaskQueue FSM
    let outcome = match queue
        .record_result_from(worker_id, session_id, result.clone())
        .await
    {
        Ok(outcome) => outcome,
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
    let _ = registry.decrement_active_tasks(&worker_id).await;
    if outcome.rejected {
        let _ = registry.record_rejection(&worker_id, session_id).await;
    } else if outcome.disposition != ResultDisposition::Late {
        registry.clear_rejections(&worker_id).await;
    }

    // 3. Notify awaiting clients, but only once the task is really finished: a requeued
    //    task (rejection, execution retry) will still produce its real result.
    if outcome.disposition == ResultDisposition::Terminal {
        let mut lock = waiters.write().await;
        if let Some(senders) = lock.remove(&task_id) {
            for tx in senders {
                let _ = tx.send(result.clone());
            }
        }
    }

    // 4. Wake scheduler loop to assign pending tasks
    scheduler_notify.notify_one();
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
async fn apply_mobile_telemetry(
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
    match registry
        .update_mobile_telemetry(&worker_id, Some(session_id), status)
        .await
    {
        Ok(true) => {
            debug!(
                worker_id = %worker_id,
                battery_pct = ?status.battery_pct,
                is_charging = ?status.is_charging,
                thermal_throttled = status.thermal_throttled,
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
async fn handle_session_task_message(
    msg: WorkerMessage,
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &Arc<tokio::sync::Notify>,
    waiters: &WaiterMap,
    worker_id: Uuid,
    session_id: u64,
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
            };
            // Once a result starts being applied it must finish, even if this connection is
            // aborted meanwhile (reaper, or the worker re-registering): the queue transition and
            // the waiter wake-up are one logical step, and cutting them apart would leave a
            // client waiting forever on a task that is already finished.
            let registry = registry.clone();
            let queue = queue.clone();
            let scheduler_notify = Arc::clone(scheduler_notify);
            let waiters = Arc::clone(waiters);
            #[cfg(feature = "dashboard")]
            let broadcast_tx = broadcast_tx.cloned();
            let _ = tokio::spawn(async move {
                apply_task_result(
                    &registry,
                    &queue,
                    &scheduler_notify,
                    &waiters,
                    worker_id,
                    Some(session_id),
                    result,
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
async fn failover_superseded_session(
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
async fn handle_client_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    first_msg: ClientMessage,
    mut transport: MessageTransport<S>,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    mapreduce_parallelism: usize,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> GridResult<()> {
    let mut next_msg = Some(first_msg);
    let mapreduce = MapReduceEngine::new(queue.clone(), scheduler_notify.clone(), waiters.clone())
        .with_registry(registry.clone())
        .with_parallelism(mapreduce_parallelism);

    loop {
        let msg = match next_msg.take() {
            Some(m) => m,
            None => match transport.recv_msg::<ClientMessage>().await? {
                Some(m) => m,
                None => break,
            },
        };

        match msg {
            ClientMessage::SubmitTask { task, wait } => {
                let task_id = task.id;
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
                    warn!(task_id = %task_id, error = %e, "SubmitTask rejected");
                    let resp = ClientResponse::Error {
                        message: e.to_string(),
                    };
                    transport.send_msg(&resp).await?;
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
                            transport.send_msg(&resp).await?;
                        }
                        Err(_) => {
                            let resp = ClientResponse::Error {
                                message: "Task completion channel closed unexpectedly".into(),
                            };
                            transport.send_msg(&resp).await?;
                        }
                    }
                } else {
                    let resp = ClientResponse::TaskSubmitted { task_id };
                    transport.send_msg(&resp).await?;
                }
            }
            ClientMessage::GetTaskStatus { task_id } => {
                let resp = task_status_response(&queue, task_id).await;
                transport.send_msg(&resp).await?;
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
                transport.send_msg(&resp).await?;
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
                    workers,
                };
                transport.send_msg(&resp).await?;
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
                transport.send_msg(&resp).await?;
            }
            ClientMessage::SubmitMapReduce { job } => match mapreduce.execute(job).await {
                Ok(result) => {
                    let resp = ClientResponse::MapReduceCompleted { result };
                    transport.send_msg(&resp).await?;
                }
                Err(e) => {
                    let resp = ClientResponse::Error {
                        message: e.to_string(),
                    };
                    transport.send_msg(&resp).await?;
                }
            },
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
                transport.send_msg(&resp).await?;
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
async fn authenticate_and_read_first_message<S>(
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
                Duration::from_secs(1),
                send_reject(transport, REJECT_NO_SECRET),
            )
            .await;
            Err(GridError::AuthFailed(
                "peer requested cluster authentication but this master has no secret configured"
                    .into(),
            ))
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
struct AbortOnDrop(tokio::task::AbortHandle);

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
struct P2pConnGuard {
    conn: iroh::endpoint::Connection,
    conns: Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    worker_id: Uuid,
}

#[cfg(feature = "p2p")]
impl P2pConnGuard {
    fn new(
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
    async fn unlist(&self) {
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
    mut shutdown_rx: watch::Receiver<bool>,
    abort_handle: Option<AbortHandle>,
    wire_codec: WireCodec,
    cluster_secret: Option<ClusterSecret>,
    handshake_permit: Option<OwnedSemaphorePermit>,
    mapreduce_parallelism: usize,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    #[cfg(feature = "p2p")] p2p_conn: Option<iroh::endpoint::Connection>,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> GridResult<()> {
    // Every inbound byte stamps `rx_clock`: the reaper counts a connection that is mid-way through
    // a large upload as alive even though its heartbeats are stuck behind that frame.
    let rx_clock = RxClock::new();
    let mut transport =
        MessageTransport::with_codec(StampedStream::new(stream, rx_clock.clone()), wire_codec);

    // 1. Cluster authentication + handshake watchdog (Slowloris defense). Nothing is read as an
    //    `InboundMessage`, so nothing is registered or queued, until the peer has authenticated.
    let (first_msg, detected_codec) = authenticate_and_read_first_message(
        &mut transport,
        &remote_addr.to_string(),
        cluster_secret.as_ref(),
        handshake_timeout,
        "connection",
    )
    .await?;
    // Authenticated and speaking: this peer no longer counts against the pre-auth budget.
    drop(handshake_permit);

    transport.set_outbound_codec(detected_codec);

    let worker_msg = match first_msg {
        InboundMessage::Client(client_msg) => {
            return handle_client_connection(
                client_msg,
                transport,
                registry,
                queue,
                scheduler_notify,
                waiters,
                mapreduce_parallelism,
                #[cfg(feature = "dashboard")]
                broadcast_tx,
                #[cfg(feature = "p2p")]
                p2p_conns,
            )
            .await;
        }
        InboundMessage::Worker(wm) => wm,
    };

    // 2. Validate Register message
    let (worker_id, capabilities, _worker_protocol_version, worker_features) = match worker_msg {
        WorkerMessage::Register {
            worker_id,
            capabilities,
            protocol_version,
            features,
        } => {
            if protocol_version < PROTOCOL_VERSION_MIN_COMPATIBLE {
                let reject_ack = MasterMessage::RegisterAck {
                    accepted: false,
                    worker_id,
                    heartbeat_interval_secs: 0,
                    protocol_version: PROTOCOL_VERSION_CURRENT,
                    features: ProtocolCapabilities::empty(),
                    message: Some(format!(
                        "Incompatible protocol version {protocol_version} (minimum required: {PROTOCOL_VERSION_MIN_COMPATIBLE})"
                    )),
                };
                let _ = transport.send_msg(&reject_ack).await;
                return Err(GridError::RegistrationRejected(format!(
                    "Incompatible protocol version {protocol_version}"
                )));
            }
            if capabilities.cpu_cores == 0 {
                let reject_ack = MasterMessage::RegisterAck {
                    accepted: false,
                    worker_id,
                    heartbeat_interval_secs: 0,
                    protocol_version: PROTOCOL_VERSION_CURRENT,
                    features: ProtocolCapabilities::empty(),
                    message: Some("Invalid capability: cpu_cores must be > 0".into()),
                };
                let _ = transport.send_msg(&reject_ack).await;
                return Err(GridError::RegistrationRejected(
                    "cpu_cores must be > 0".into(),
                ));
            }
            (worker_id, capabilities, protocol_version, features)
        }
        other => {
            let reject_ack = MasterMessage::RegisterAck {
                accepted: false,
                worker_id: Uuid::nil(),
                heartbeat_interval_secs: 0,
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::empty(),
                message: Some(format!(
                    "Expected Register message, received {}",
                    other.variant_name()
                )),
            };
            let _ = transport.send_msg(&reject_ack).await;
            return Err(GridError::HandshakeFailed(format!(
                "Expected Register message, received {}",
                other.variant_name()
            )));
        }
    };

    // 3. Register in WorkerRegistry
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<MasterMessage>(128);
    let registration = registry
        .register_session(
            worker_id,
            capabilities,
            remote_addr,
            outbound_tx.clone(),
            abort_handle,
        )
        .await?;
    let session_id = registration.session_id;
    registry
        .set_rx_clock(&worker_id, session_id, rx_clock.clone())
        .await;

    // The previous session of this worker id was still live (no EOF seen yet) and has just been
    // aborted; its in-flight tasks lost their connection, so fail them over now, exactly once.
    if let Some(old_session) = registration.superseded_session {
        failover_superseded_session(
            &queue,
            &scheduler_notify,
            &waiters,
            worker_id,
            old_session,
            session_id,
            #[cfg(feature = "dashboard")]
            broadcast_tx.as_ref(),
        )
        .await;
    }

    #[cfg(feature = "p2p")]
    let _p2p_guard = match (&p2p_conn, &p2p_conns) {
        (Some(conn), Some(conns)) => {
            conns.write().await.insert(worker_id, conn.clone());
            let path_info = rusty_grid_core::transport::inspect_connection_paths(conn);
            info!(
                worker_id = %worker_id,
                path_info = ?path_info,
                "Tracked P2P connection path for registered worker"
            );
            Some(P2pConnGuard::new(
                conn.clone(),
                Arc::clone(conns),
                worker_id,
            ))
        }
        _ => None,
    };

    // 4. Send RegisterAck
    let negotiated_features = worker_features & ProtocolCapabilities::all_supported();
    let ack = MasterMessage::RegisterAck {
        accepted: true,
        worker_id,
        heartbeat_interval_secs,
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: negotiated_features,
        message: Some(rusty_grid_core::protocol::master_features::announcement(
            rusty_grid_core::protocol::master_features::SUPPORTED,
        )),
    };
    if let Err(e) = transport.send_msg(&ack).await {
        // Tasks may already have been assigned to this session; sweep them, not just unregister.
        handle_worker_disconnect_with_broadcast(
            &registry,
            &queue,
            &scheduler_notify,
            Some(&waiters),
            &worker_id,
            Some(session_id),
            "RegisterAck delivery failed",
            true,
            #[cfg(feature = "dashboard")]
            broadcast_tx.as_ref(),
        )
        .await;
        return Err(e.into());
    }

    #[cfg(feature = "dashboard")]
    if let Some(ref b_tx) = broadcast_tx {
        if let Some(info) = registry.get_worker(worker_id).await {
            let _ =
                b_tx.send(crate::dashboard::dto::DashboardStreamMessage::WorkerRegistered(info));
        }
    }

    info!(
        worker_id = %worker_id,
        session_id = session_id,
        remote = %remote_addr,
        "Worker successfully registered with master"
    );

    // 5. Split transport into independent writer and reader
    let (mut writer, mut reader) = transport.split();

    // 6. Spawn outbound message writer task
    let writer_task = tokio::spawn(async move {
        while let Some(msg) = outbound_rx.recv().await {
            if let Err(e) = writer.send_msg(&msg).await {
                debug!(error = %e, "Outbound message write failed; terminating writer task");
                break;
            }
        }
    });

    let _writer_guard = AbortOnDrop(writer_task.abort_handle());

    // Allow newly connected worker to complete handshake acknowledgement, split its transport,
    // and enter its inbound message loop before tasks are dispatched to it.
    let sched_notify = scheduler_notify.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        sched_notify.notify_one();
    });

    // 7. Inbound reader loop
    loop {
        tokio::select! {
            inbound_res = reader.recv_msg::<WorkerMessage>() => {
                match inbound_res {
                    Ok(Some(msg)) => {
                        match msg {
                            WorkerMessage::Heartbeat {
                                worker_id: id,
                                timestamp,
                                active_tasks,
                                cpu_usage_pct,
                                ram_available_mb,
                            } => {
                                if id == worker_id {
                                    let heartbeat = registry
                                        .record_heartbeat_session(
                                            &worker_id,
                                            active_tasks,
                                            timestamp,
                                            cpu_usage_pct,
                                            ram_available_mb,
                                            Some(session_id),
                                        )
                                        .await;
                                    if let Ok(HeartbeatOutcome::Revived) = heartbeat {
                                        scheduler_notify.notify_one();
                                        #[cfg(feature = "dashboard")]
                                        if let Some(ref b_tx) = broadcast_tx {
                                            if let Some(info) = registry.get_worker(worker_id).await {
                                                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::WorkerRegistered(info));
                                            }
                                        }
                                    }
                                    let _ = outbound_tx
                                        .send(MasterMessage::HeartbeatAck { timestamp })
                                        .await;
                                    #[cfg(feature = "dashboard")]
                                    if let Some(ref b_tx) = broadcast_tx {
                                        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::WorkerHeartbeat {
                                            worker_id: id,
                                            cpu_usage_pct,
                                            ram_available_mb,
                                            active_tasks,
                                            timestamp,
                                        });
                                    }
                                } else {
                                    warn!(expected = %worker_id, got = %id, "Heartbeat worker_id mismatch");
                                }
                            }
                            WorkerMessage::Disconnecting { worker_id: id, reason } => {
                                info!(worker_id = %id, reason = %reason, "Worker announced graceful disconnection");
                                handle_worker_disconnect_with_broadcast(
                                    &registry,
                                    &queue,
                                    &scheduler_notify,
                                    Some(&waiters),
                                    &worker_id,
                                    Some(session_id),
                                    &format!("Worker graceful disconnect: {reason}"),
                                    true,
                                    #[cfg(feature = "dashboard")]
                                    broadcast_tx.as_ref(),
                                ).await;
                                break;
                            }
                            msg @ (WorkerMessage::TaskProgress { .. }
                                | WorkerMessage::TaskResult { .. }
                                | WorkerMessage::Checkpoint { .. }) => {
                                handle_session_task_message(
                                    msg,
                                    &registry,
                                    &queue,
                                    &scheduler_notify,
                                    &waiters,
                                    worker_id,
                                    session_id,
                                    #[cfg(feature = "dashboard")]
                                    broadcast_tx.as_ref(),
                                ).await;
                            }
                            WorkerMessage::MobileTelemetry { worker_id: claimed, battery_pct, is_charging, thermal_throttled } => {
                                apply_mobile_telemetry(
                                    &registry,
                                    &scheduler_notify,
                                    worker_id,
                                    session_id,
                                    claimed,
                                    MobileLiveStatus { battery_pct, is_charging, thermal_throttled },
                                ).await;
                            }
                            WorkerMessage::Register { .. } => {
                                warn!(worker_id = %worker_id, "Ignoring unexpected Register message on active connection");
                            }
                            WorkerMessage::UnknownVariant { tag, ref raw_bytes } => {
                                warn!(worker_id = %worker_id, tag, raw_bytes_len = raw_bytes.len(), "Ignoring unrecognized WorkerMessage variant from future protocol version");
                            }

                        }
                    }
                    Ok(None) => {
                        info!(worker_id = %worker_id, "Worker connection closed by peer (EOF)");
                        handle_worker_disconnect_with_broadcast(
                            &registry,
                            &queue,
                            &scheduler_notify,
                            Some(&waiters),
                            &worker_id,
                            Some(session_id),
                            "TCP connection EOF",
                            false,
                            #[cfg(feature = "dashboard")]
                            broadcast_tx.as_ref(),
                        ).await;
                        break;
                    }
                    Err(e) => {
                        warn!(worker_id = %worker_id, error = %e, "Inbound message read error");
                        handle_worker_disconnect_with_broadcast(
                            &registry,
                            &queue,
                            &scheduler_notify,
                            Some(&waiters),
                            &worker_id,
                            Some(session_id),
                            &format!("Inbound socket error: {e}"),
                            false,
                            #[cfg(feature = "dashboard")]
                            broadcast_tx.as_ref(),
                        ).await;
                        break;
                    }
                }
            }
            res = shutdown_rx.changed() => {
                if res.is_ok() && *shutdown_rx.borrow() {
                    info!(worker_id = %worker_id, "Master shutting down; notifying worker");
                    let _ = outbound_tx.send(MasterMessage::Shutdown {
                        reason: "Master server shutting down".into(),
                        grace_period_secs: Some(5),
                    }).await;
                    break;
                } else if res.is_err() {
                    break;
                }
            }
        }
    }

    // 8. Cleanup upon disconnect
    handle_worker_disconnect_with_broadcast(
        &registry,
        &queue,
        &scheduler_notify,
        Some(&waiters),
        &worker_id,
        Some(session_id),
        "Connection terminated",
        false,
        #[cfg(feature = "dashboard")]
        broadcast_tx.as_ref(),
    )
    .await;
    #[cfg(feature = "p2p")]
    if let Some(ref guard) = _p2p_guard {
        guard.unlist().await;
    }
    writer_task.abort();
    Ok(())
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
    mut shutdown_rx: watch::Receiver<bool>,
    abort_handle: Option<AbortHandle>,
    wire_codec: WireCodec,
    cluster_secret: Option<ClusterSecret>,
    handshake_permit: Option<OwnedSemaphorePermit>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    #[cfg(feature = "p2p")] p2p_conn: Option<iroh::endpoint::Connection>,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> GridResult<()> {
    // Bytes on either lane count as proof of life for the reaper (see `handle_connection`).
    let rx_clock = RxClock::new();
    let mut ctrl_transport = MessageTransport::with_codec(
        StampedStream::new(ctrl_stream, rx_clock.clone()),
        wire_codec,
    );
    let data_transport = MessageTransport::with_codec(
        StampedStream::new(data_stream, rx_clock.clone()),
        wire_codec,
    );

    // An iroh ticket only addresses a node, it is not a credential, so QUIC peers authenticate
    // with the cluster secret exactly like TCP peers. The handshake runs on the control stream
    // (the lane that carries `Register`); the data lane is never read before registration.
    #[cfg(feature = "p2p")]
    let peer_label = match p2p_conn {
        Some(ref conn) => format!("{remote_addr} (iroh node {})", conn.remote_id()),
        None => remote_addr.to_string(),
    };
    #[cfg(not(feature = "p2p"))]
    let peer_label = remote_addr.to_string();

    // 1. Cluster authentication + handshake on the control stream (Slowloris defense)
    let (first_msg, detected_codec) = authenticate_and_read_first_message(
        &mut ctrl_transport,
        &peer_label,
        cluster_secret.as_ref(),
        handshake_timeout,
        "control stream",
    )
    .await?;
    drop(handshake_permit);

    ctrl_transport.set_outbound_codec(detected_codec);

    let worker_msg = match first_msg {
        InboundMessage::Worker(wm) => wm,
        InboundMessage::Client(_) => {
            return Err(GridError::HandshakeFailed(
                "Client connection not supported over multiplexed P2P worker transport".into(),
            ));
        }
    };

    // 2. Validate Register message
    let (worker_id, capabilities, _worker_protocol_version, worker_features) = match worker_msg {
        WorkerMessage::Register {
            worker_id,
            capabilities,
            protocol_version,
            features,
        } => {
            if protocol_version < PROTOCOL_VERSION_MIN_COMPATIBLE {
                let reject_ack = MasterMessage::RegisterAck {
                    accepted: false,
                    worker_id,
                    heartbeat_interval_secs: 0,
                    protocol_version: PROTOCOL_VERSION_CURRENT,
                    features: ProtocolCapabilities::empty(),
                    message: Some(format!(
                        "Incompatible protocol version {protocol_version} (minimum required: {PROTOCOL_VERSION_MIN_COMPATIBLE})"
                    )),
                };
                let _ = ctrl_transport.send_msg(&reject_ack).await;
                return Err(GridError::RegistrationRejected(format!(
                    "Incompatible protocol version {protocol_version}"
                )));
            }
            if capabilities.cpu_cores == 0 {
                let reject_ack = MasterMessage::RegisterAck {
                    accepted: false,
                    worker_id,
                    heartbeat_interval_secs: 0,
                    protocol_version: PROTOCOL_VERSION_CURRENT,
                    features: ProtocolCapabilities::empty(),
                    message: Some("Invalid capability: cpu_cores must be > 0".into()),
                };
                let _ = ctrl_transport.send_msg(&reject_ack).await;
                return Err(GridError::RegistrationRejected(
                    "cpu_cores must be > 0".into(),
                ));
            }
            (worker_id, capabilities, protocol_version, features)
        }
        other => {
            let reject_ack = MasterMessage::RegisterAck {
                accepted: false,
                worker_id: Uuid::nil(),
                heartbeat_interval_secs: 0,
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::empty(),
                message: Some(format!(
                    "Expected Register message, received {}",
                    other.variant_name()
                )),
            };
            let _ = ctrl_transport.send_msg(&reject_ack).await;
            return Err(GridError::HandshakeFailed(format!(
                "Expected Register message, received {}",
                other.variant_name()
            )));
        }
    };

    // 3. Register in WorkerRegistry
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<MasterMessage>(128);
    let registration = registry
        .register_session(
            worker_id,
            capabilities,
            remote_addr,
            outbound_tx.clone(),
            abort_handle,
        )
        .await?;
    let session_id = registration.session_id;
    registry
        .set_rx_clock(&worker_id, session_id, rx_clock.clone())
        .await;

    // See `handle_connection`: fail over the superseded session's tasks exactly once.
    if let Some(old_session) = registration.superseded_session {
        failover_superseded_session(
            &queue,
            &scheduler_notify,
            &waiters,
            worker_id,
            old_session,
            session_id,
            #[cfg(feature = "dashboard")]
            broadcast_tx.as_ref(),
        )
        .await;
    }

    #[cfg(feature = "p2p")]
    let _p2p_guard = match (&p2p_conn, &p2p_conns) {
        (Some(conn), Some(conns)) => {
            conns.write().await.insert(worker_id, conn.clone());
            let path_info = rusty_grid_core::transport::inspect_connection_paths(conn);
            info!(
                worker_id = %worker_id,
                path_info = ?path_info,
                "Tracked P2P connection path for registered worker"
            );
            Some(P2pConnGuard::new(
                conn.clone(),
                Arc::clone(conns),
                worker_id,
            ))
        }
        _ => None,
    };

    // 4. Send RegisterAck over control stream
    let negotiated_features = worker_features & ProtocolCapabilities::all_supported();
    let ack = MasterMessage::RegisterAck {
        accepted: true,
        worker_id,
        heartbeat_interval_secs,
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: negotiated_features,
        message: Some(rusty_grid_core::protocol::master_features::announcement(
            rusty_grid_core::protocol::master_features::SUPPORTED,
        )),
    };
    if let Err(e) = ctrl_transport.send_msg(&ack).await {
        handle_worker_disconnect_with_broadcast(
            &registry,
            &queue,
            &scheduler_notify,
            Some(&waiters),
            &worker_id,
            Some(session_id),
            "RegisterAck delivery failed",
            true,
            #[cfg(feature = "dashboard")]
            broadcast_tx.as_ref(),
        )
        .await;
        return Err(e.into());
    }

    #[cfg(feature = "dashboard")]
    if let Some(ref b_tx) = broadcast_tx {
        if let Some(info) = registry.get_worker(worker_id).await {
            let _ =
                b_tx.send(crate::dashboard::dto::DashboardStreamMessage::WorkerRegistered(info));
        }
    }

    info!(
        worker_id = %worker_id,
        session_id = session_id,
        remote = %remote_addr,
        "Worker successfully registered with master (QUIC 2-lane multiplexed)"
    );

    // 5. Split transports into independent writers and readers
    let (mut ctrl_writer, mut ctrl_reader) = ctrl_transport.split();
    let (mut data_writer, mut data_reader) = data_transport.split();

    // 6. Spawn 2-lane outbound writers: Control Lane and Data Lane
    let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<MasterMessage>(64);
    let (data_out_tx, mut data_out_rx) = mpsc::channel::<MasterMessage>(64);
    let ctrl_sender = ctrl_out_tx.clone();

    // router_task decouples data and control lane routing from outbound_rx.
    // When data_out_tx experiences backpressure, pending data frames are queued
    // locally in pending_data, while outbound_rx continues to be drained so that
    // control messages are dispatched immediately to ctrl_out_tx without head-of-line blocking.
    let router_task = tokio::spawn(async move {
        let mut pending_data: std::collections::VecDeque<MasterMessage> =
            std::collections::VecDeque::new();

        loop {
            if pending_data.is_empty() {
                match outbound_rx.recv().await {
                    Some(msg) => match msg {
                        MasterMessage::AssignTask { .. }
                        | MasterMessage::AssignTaskWithCheckpoint { .. }
                        | MasterMessage::CancelTask { .. } => match data_out_tx.try_send(msg) {
                            Ok(()) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Full(data_msg)) => {
                                pending_data.push_back(data_msg);
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                        },
                        ctrl_msg => {
                            if ctrl_out_tx.send(ctrl_msg).await.is_err() {
                                break;
                            }
                        }
                    },
                    None => break,
                }
            } else {
                tokio::select! {
                    send_permit = data_out_tx.reserve() => {
                        match send_permit {
                            Ok(permit) => {
                                if let Some(msg) = pending_data.pop_front() {
                                    permit.send(msg);
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    msg_opt = outbound_rx.recv() => {
                        match msg_opt {
                            Some(msg) => match msg {
                                MasterMessage::AssignTask { .. }
                                | MasterMessage::AssignTaskWithCheckpoint { .. }
                                | MasterMessage::CancelTask { .. } => {
                                    pending_data.push_back(msg);
                                }

                                ctrl_msg => {
                                    // Control frames bypass pending data frames immediately
                                    if ctrl_out_tx.send(ctrl_msg).await.is_err() {
                                        break;
                                    }
                                }
                            },
                            None => {
                                // outbound_rx closed; drain remaining pending data then exit
                                while let Some(msg) = pending_data.pop_front() {
                                    if data_out_tx.send(msg).await.is_err() {
                                        break;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    });

    let ctrl_writer_task = tokio::spawn(async move {
        while let Some(msg) = ctrl_out_rx.recv().await {
            if let Err(e) = ctrl_writer.send_msg(&msg).await {
                debug!(error = %e, "Control writer send error; terminating");
                break;
            }
        }
    });

    let data_writer_task = tokio::spawn(async move {
        while let Some(msg) = data_out_rx.recv().await {
            if let Err(e) = data_writer.send_msg(&msg).await {
                debug!(error = %e, "Data writer send error; terminating");
                break;
            }
        }
    });

    // These helpers must not outlive the handler: if it is aborted (reaper, re-registration) the
    // explicit aborts at the end of this function never run.
    let _router_guard = AbortOnDrop(router_task.abort_handle());
    let _ctrl_writer_guard = AbortOnDrop(ctrl_writer_task.abort_handle());
    let _data_writer_guard = AbortOnDrop(data_writer_task.abort_handle());

    let sched_notify = scheduler_notify.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        sched_notify.notify_one();
    });

    // 7. Multiplexed inbound reader loop
    loop {
        tokio::select! {
            ctrl_res = ctrl_reader.recv_msg::<WorkerMessage>() => {
                match ctrl_res {
                    Ok(Some(msg)) => {
                        match msg {
                            WorkerMessage::Heartbeat {
                                worker_id: id,
                                timestamp,
                                active_tasks,
                                cpu_usage_pct,
                                ram_available_mb,
                            } => {
                                if id == worker_id {
                                    let heartbeat = registry
                                        .record_heartbeat_session(
                                            &worker_id,
                                            active_tasks,
                                            timestamp,
                                            cpu_usage_pct,
                                            ram_available_mb,
                                            Some(session_id),
                                        )
                                        .await;
                                    if let Ok(HeartbeatOutcome::Revived) = heartbeat {
                                        scheduler_notify.notify_one();
                                        #[cfg(feature = "dashboard")]
                                        if let Some(ref b_tx) = broadcast_tx {
                                            if let Some(info) = registry.get_worker(worker_id).await {
                                                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::WorkerRegistered(info));
                                            }
                                        }
                                    }
                                    let _ = ctrl_sender
                                        .send(MasterMessage::HeartbeatAck { timestamp })
                                        .await;
                                    #[cfg(feature = "dashboard")]
                                    if let Some(ref b_tx) = broadcast_tx {
                                        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::WorkerHeartbeat {
                                            worker_id: id,
                                            cpu_usage_pct,
                                            ram_available_mb,
                                            active_tasks,
                                            timestamp,
                                        });
                                    }
                                } else {
                                    warn!(expected = %worker_id, got = %id, "Heartbeat worker_id mismatch on control stream");
                                }
                            }
                            WorkerMessage::Disconnecting { worker_id: id, reason } => {
                                info!(worker_id = %id, reason = %reason, "Worker announced graceful disconnection on control stream");
                                handle_worker_disconnect_with_broadcast(
                                    &registry,
                                    &queue,
                                    &scheduler_notify,
                                    Some(&waiters),
                                    &worker_id,
                                    Some(session_id),
                                    &format!("Worker graceful disconnect: {reason}"),
                                    true,
                                    #[cfg(feature = "dashboard")]
                                    broadcast_tx.as_ref(),
                                ).await;
                                break;
                            }
                            WorkerMessage::MobileTelemetry { worker_id: claimed, battery_pct, is_charging, thermal_throttled } => {
                                apply_mobile_telemetry(
                                    &registry,
                                    &scheduler_notify,
                                    worker_id,
                                    session_id,
                                    claimed,
                                    MobileLiveStatus { battery_pct, is_charging, thermal_throttled },
                                ).await;
                            }
                            other => {
                                warn!(worker_id = %worker_id, message = %other.variant_name(), "Unexpected message on control stream");
                            }
                        }
                    }
                    Ok(None) => {
                        info!(worker_id = %worker_id, "Worker control stream closed by peer (EOF)");
                        handle_worker_disconnect_with_broadcast(
                            &registry,
                            &queue,
                            &scheduler_notify,
                            Some(&waiters),
                            &worker_id,
                            Some(session_id),
                            "Control stream EOF",
                            false,
                            #[cfg(feature = "dashboard")]
                            broadcast_tx.as_ref(),
                        ).await;
                        break;
                    }
                    Err(e) => {
                        warn!(worker_id = %worker_id, error = %e, "Control stream read error");
                        handle_worker_disconnect_with_broadcast(
                            &registry,
                            &queue,
                            &scheduler_notify,
                            Some(&waiters),
                            &worker_id,
                            Some(session_id),
                            &format!("Control stream socket error: {e}"),
                            false,
                            #[cfg(feature = "dashboard")]
                            broadcast_tx.as_ref(),
                        ).await;
                        break;
                    }
                }
            }
            data_res = data_reader.recv_msg::<WorkerMessage>() => {
                match data_res {
                    Ok(Some(msg)) => {
                        match msg {
                            msg @ (WorkerMessage::TaskProgress { .. }
                                | WorkerMessage::TaskResult { .. }
                                | WorkerMessage::Checkpoint { .. }) => {
                                handle_session_task_message(
                                    msg,
                                    &registry,
                                    &queue,
                                    &scheduler_notify,
                                    &waiters,
                                    worker_id,
                                    session_id,
                                    #[cfg(feature = "dashboard")]
                                    broadcast_tx.as_ref(),
                                ).await;
                            }
                            other => {
                                warn!(worker_id = %worker_id, message = %other.variant_name(), "Unexpected message on data stream");
                            }

                        }
                    }
                    Ok(None) => {
                        info!(worker_id = %worker_id, "Worker data stream closed by peer (EOF)");
                        handle_worker_disconnect_with_broadcast(
                            &registry,
                            &queue,
                            &scheduler_notify,
                            Some(&waiters),
                            &worker_id,
                            Some(session_id),
                            "Data stream EOF",
                            false,
                            #[cfg(feature = "dashboard")]
                            broadcast_tx.as_ref(),
                        ).await;
                        break;
                    }
                    Err(e) => {
                        warn!(worker_id = %worker_id, error = %e, "Data stream read error");
                        handle_worker_disconnect_with_broadcast(
                            &registry,
                            &queue,
                            &scheduler_notify,
                            Some(&waiters),
                            &worker_id,
                            Some(session_id),
                            &format!("Data stream socket error: {e}"),
                            false,
                            #[cfg(feature = "dashboard")]
                            broadcast_tx.as_ref(),
                        ).await;
                        break;
                    }
                }
            }
            res = shutdown_rx.changed() => {
                if res.is_ok() && *shutdown_rx.borrow() {
                    info!(worker_id = %worker_id, "Master shutting down; notifying worker on control stream");
                    let _ = ctrl_sender.send(MasterMessage::Shutdown {
                        reason: "Master server shutting down".into(),
                        grace_period_secs: Some(5),
                    }).await;
                    break;
                } else if res.is_err() {
                    break;
                }
            }
        }
    }

    // 8. Cleanup upon disconnect
    handle_worker_disconnect_with_broadcast(
        &registry,
        &queue,
        &scheduler_notify,
        Some(&waiters),
        &worker_id,
        Some(session_id),
        "Multiplexed connection terminated",
        false,
        #[cfg(feature = "dashboard")]
        broadcast_tx.as_ref(),
    )
    .await;

    #[cfg(feature = "p2p")]
    if let Some(ref guard) = _p2p_guard {
        guard.unlist().await;
    }
    ctrl_writer_task.abort();
    data_writer_task.abort();
    router_task.abort();
    Ok(())
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
}

impl MasterHandle {
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
        let id = self.queue.submit(task).await?;
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
        let id = self.queue.submit_with_priority(task, priority).await?;
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
}
