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
use tokio::sync::{mpsc, oneshot, watch, RwLock};
use tokio::task::AbortHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use rusty_grid_core::mapreduce::{MapReduceJobSpec, MapReduceResult};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, InboundMessage};
use rusty_grid_core::task::{Task, TaskId, TaskResult, TaskStatus};
#[cfg(feature = "p2p")]
use rusty_grid_core::transport::{BiStream, GridStream, GRID_ALPN};
use rusty_grid_core::{GridError, GridResult, MasterMessage, MessageTransport, WireCodec, WorkerMessage};

use crate::mapreduce::MapReduceEngine;
use crate::queue::{QueueStats, TaskInfo, TaskQueue, TaskState};
#[allow(unused_imports)]
use crate::reaper::spawn_reaper_with_queue;
use crate::reaper::ReaperConfig;
use crate::registry::{WorkerInfo, WorkerRegistry};
use crate::scheduler::{SchedulerConfig, WorkloadScheduler};

/// Type alias for client waiters awaiting task completion via oneshot channels.
pub type WaiterMap = Arc<RwLock<HashMap<TaskId, Vec<oneshot::Sender<TaskResult>>>>>;

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
    /// Optional path to load or persist the P2P secret key for stable identity across restarts.
    pub p2p_key_file: Option<PathBuf>,
    /// Wire serialization codec (Bincode or Json, default: Bincode).
    pub wire_codec: WireCodec,
    /// Maximum execution retries for failed or disconnected tasks (default: 3).
    pub max_retries: u32,
    /// Optional TCP port for the embedded web observability dashboard (0 for ephemeral).
    pub dashboard_port: Option<u16>,
    /// Optional bind IP for the dashboard HTTP listener.
    pub dashboard_bind_ip: Option<std::net::IpAddr>,
    /// Optional filesystem path to write the dynamic bound dashboard port for zero-collision testing.
    pub dashboard_port_file: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:0".parse().expect("valid loopback default"),
            port_file: None,
            heartbeat_interval_secs: 3,
            handshake_timeout_secs: 5,
            enable_p2p: false,
            p2p_ticket_file: None,
            p2p_key_file: None,
            wire_codec: WireCodec::default(),
            max_retries: 3,
            dashboard_port: None,
            dashboard_bind_ip: None,
            dashboard_port_file: None,
        }
    }
}

impl ServerConfig {
    /// Creates a new ServerConfig binding to the given socket address.
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            ..Default::default()
        }
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
    #[cfg(feature = "dashboard")]
    pub broadcast_tx: Option<tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>>,
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
        let p2p_endpoint = if config.enable_p2p || config.p2p_key_file.is_some() {
            let secret_key = resolve_p2p_secret_key(config.p2p_key_file.as_deref()).await?;
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
                .secret_key(secret_key)
                .alpns(vec![GRID_ALPN.to_vec()])
                .bind()
                .await
                .map_err(|e| GridError::Config(format!("Failed to bind iroh endpoint: {e}")))?;
            let mut addr = endpoint.addr();
            if config.p2p_key_file.is_some() {
                // Filter ephemeral direct UDP socket addresses allocated by the OS on ephemeral bind (:0).
                // Retaining ephemeral direct socket ports would break ticket determinism across restarts
                // (Ticket 1 != Ticket 2) and point to dead ports. In iroh with presets::N0, workers
                // resolve the persistent NodeId (PublicKey) via N0 DNS/Pkarr discovery.
                addr.addrs.retain(|a| a.is_relay());
            }
            let ticket = rusty_grid_core::transport::serialize_p2p_ticket(&addr)?;
            info!(ticket = %ticket, "Master P2P endpoint established");

            if let Some(ref ticket_path) = config.p2p_ticket_file {
                if let Some(parent) = ticket_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        let _ = tokio::fs::create_dir_all(parent).await;
                    }
                }
                tokio::fs::write(ticket_path, &ticket).await.map_err(GridError::Io)?;
                info!(path = %ticket_path.display(), "Published P2P ticket to file");
            }

            Some(endpoint)
        } else {
            None
        };

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
            #[cfg(feature = "dashboard")]
            broadcast_tx: None,
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
            ..Default::default()
        };
        let queue = TaskQueue::with_config(retry_policy, None);
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let waiters = Arc::new(RwLock::new(HashMap::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let mut server = Self::bind_full(
            config.clone(),
            registry.clone(),
            queue.clone(),
            scheduler_notify.clone(),
            Arc::clone(&waiters),
        )
        .await?;
        let server_addr = server.local_addr();

        #[cfg(feature = "dashboard")]
        let (dashboard_addr, broadcast_tx) = if let Some(dash_port) = config.dashboard_port {
            let bind_ip = config.dashboard_bind_ip.unwrap_or_else(|| {
                let ip = server_addr.ip();
                if ip.is_unspecified() {
                    "127.0.0.1".parse().unwrap()
                } else {
                    ip
                }
            });
            let dash_bind_addr = SocketAddr::new(bind_ip, dash_port);
            let dash_listener = TcpListener::bind(dash_bind_addr)
                .await
                .map_err(GridError::Io)?;
            let bound_dash_addr = dash_listener.local_addr().map_err(GridError::Io)?;
            info!(addr = %bound_dash_addr, "Embedded Web Observability Dashboard listening");

            if let Some(ref dp_file) = config.dashboard_port_file {
                write_port_file(dp_file, bound_dash_addr.port()).await?;
                info!(port = bound_dash_addr.port(), path = %dp_file.display(), "Dashboard port file published");
            }

            let (b_tx, _) = tokio::sync::broadcast::channel(256);
            let state = crate::dashboard::DashboardState {
                registry: registry.clone(),
                queue: queue.clone(),
                server_addr,
                dashboard_addr: Some(bound_dash_addr),
                started_at: std::time::Instant::now(),
                broadcast_tx: b_tx.clone(),
            };

            let router = crate::dashboard::create_dashboard_router(state);
            let mut dash_shutdown_rx = shutdown_rx.clone();
            let shutdown_signal = async move {
                while !*dash_shutdown_rx.borrow_and_update() {
                    if dash_shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            };

            tokio::spawn(async move {
                if let Err(e) = axum::serve(dash_listener, router)
                    .with_graceful_shutdown(shutdown_signal)
                    .await
                {
                    warn!(error = %e, "Dashboard HTTP server terminated with error");
                }
            });

            (Some(bound_dash_addr), Some(b_tx))
        } else {
            (None, None)
        };

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
        let sched = Arc::new(WorkloadScheduler::new(
            sched_config,
            registry.clone(),
            queue.clone(),
            scheduler_notify.clone(),
        ));
        let mut sched_shutdown_rx = shutdown_rx.clone();
        #[cfg(feature = "dashboard")]
        let sched_broadcast_tx = broadcast_tx.clone();
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
                        if let Ok(report) = sched.schedule_once().await {
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

        Ok(MasterHandle {
            server_addr,
            dashboard_addr,
            registry,
            queue,
            scheduler_notify,
            waiters,
            shutdown_tx,
            #[cfg(feature = "dashboard")]
            broadcast_tx,
        })
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
}

/// Resolves the P2P node secret key from file or generates a new one.
#[cfg(feature = "p2p")]
async fn resolve_p2p_secret_key(key_path_opt: Option<&Path>) -> GridResult<iroh::SecretKey> {
    match key_path_opt {
        Some(path) => {
            if tokio::fs::try_exists(path).await.unwrap_or(false) {
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
                        tokio::fs::create_dir_all(parent).await.map_err(GridError::Io)?;
                    }
                }
                tokio::fs::write(path, secret_key.to_bytes())
                    .await
                    .map_err(GridError::Io)?;
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

impl MasterServer {

    /// Runs the master server accept loop until `shutdown_rx` signals termination.
    pub async fn run(self, mut shutdown_rx: watch::Receiver<bool>) -> GridResult<()> {
        info!(listen_addr = %self.local_addr, "Master server accept loop started");

        #[cfg(feature = "dashboard")]
        let p2p_broadcast_tx = self.broadcast_tx.clone();

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

            tokio::spawn(async move {
                let mut p2p_shutdown = p2p_shutdown_rx;
                loop {
                    tokio::select! {
                        incoming = endpoint.accept() => {
                            if let Some(incoming) = incoming {
                                match incoming.await {
                                    Ok(conn) => {
                                        match conn.accept_bi().await {
                                            Ok((send, recv)) => {
                                                let grid_stream = GridStream::P2p(BiStream::new(recv, send));
                                                let remote_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
                                                let c_reg = p2p_reg.clone();
                                                let c_q = p2p_queue.clone();
                                                let c_sched = p2p_sched.clone();
                                                let c_wait = Arc::clone(&p2p_waiters);
                                                let c_shut = p2p_shutdown.clone();
                                                let c_codec = p2p_codec;
                                                #[cfg(feature = "dashboard")]
                                                let c_bcast = p2p_broadcast_tx.clone();
                                                tokio::spawn(async move {
                                                    let _ = handle_connection(
                                                         grid_stream,
                                                         remote_addr,
                                                         c_reg,
                                                         c_q,
                                                         c_sched,
                                                         c_wait,
                                                         p2p_hb,
                                                         p2p_to,
                                                         c_shut,
                                                         None,
                                                         c_codec,
                                                         #[cfg(feature = "dashboard")]
                                                         c_bcast,
                                                    ).await;
                                                });
                                            }
                                            Err(e) => {
                                                warn!(error = %e, "Failed to accept bidirectional P2P stream");
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "P2P connection accept failed");
                                    }
                                }
                            }
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

        loop {
            tokio::select! {
                accept_res = self.listener.accept() => {
                    match accept_res {
                        Ok((stream, remote_addr)) => {
                            let registry = self.registry.clone();
                            let queue = self.queue.clone();
                            let scheduler_notify = self.scheduler_notify.clone();
                            let waiters = Arc::clone(&self.waiters);
                            let heartbeat_interval = self.config.heartbeat_interval_secs;
                            let handshake_timeout = Duration::from_secs(self.config.handshake_timeout_secs);
                            let conn_shutdown_rx = shutdown_rx.clone();
                            let conn_wire_codec = self.config.wire_codec;
                            #[cfg(feature = "dashboard")]
                            let conn_broadcast_tx = self.broadcast_tx.clone();

                            let (abort_tx, abort_rx) = oneshot::channel::<AbortHandle>();
                            let conn_handle = tokio::spawn(async move {
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
                                    #[cfg(feature = "dashboard")]
                                    conn_broadcast_tx,
                                ).await {
                                    debug!(remote_addr = %remote_addr, error = %e, "Worker connection ended");
                                }
                            });
                            let _ = abort_tx.send(conn_handle.abort_handle());
                        }
                        Err(e) => {
                            error!(error = %e, "TCP accept failed");
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

        // Clean up P2P ticket file on shutdown
        if let Some(ref ticket_path) = self.config.p2p_ticket_file {
            let _ = tokio::fs::remove_file(ticket_path).await;
        }

        Ok(())
    }
}

/// Handles incoming `WorkerMessage::TaskResult` messages.
pub async fn handle_task_result(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    result: TaskResult,
) {
    #[cfg(feature = "dashboard")]
    {
        handle_task_result_with_broadcast(registry, queue, scheduler_notify, waiters, result, None).await;
    }
    #[cfg(not(feature = "dashboard"))]
    {
        handle_task_result_internal(registry, queue, scheduler_notify, waiters, result).await;
    }
}

/// Handles incoming `WorkerMessage::TaskResult` messages with optional dashboard telemetry broadcast.
#[cfg(feature = "dashboard")]
pub async fn handle_task_result_with_broadcast(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    result: TaskResult,
    broadcast_tx: Option<&tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>>,
) {
    let task_id = result.task_id;
    let worker_id = result.worker_id;

    info!(
        worker_id = %worker_id,
        task_id = %task_id,
        exit_code = result.exit_code,
        "Task execution result received"
    );

    // 1. Record outcome in TaskQueue FSM
    if let Err(e) = queue.record_result(result.clone()).await {
        warn!(task_id = %task_id, error = %e, "Failed to record task result in queue");
    }

    if let Some(b_tx) = broadcast_tx {
        if let Some(info) = queue.get_task(&task_id).await {
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
        }
        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(queue.stats().await));
    }

    // 2. Decrement worker active tasks in registry immediately
    let _ = registry.decrement_active_tasks(&worker_id).await;

    // 3. Notify awaiting clients
    let mut lock = waiters.write().await;
    if let Some(senders) = lock.remove(&task_id) {
        for tx in senders {
            let _ = tx.send(result.clone());
        }
    }

    // 4. Wake scheduler loop to assign pending tasks
    scheduler_notify.notify_one();
}

#[cfg(not(feature = "dashboard"))]
async fn handle_task_result_internal(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    result: TaskResult,
) {
    let task_id = result.task_id;
    let worker_id = result.worker_id;

    info!(
        worker_id = %worker_id,
        task_id = %task_id,
        exit_code = result.exit_code,
        "Task execution result received"
    );

    if let Err(e) = queue.record_result(result.clone()).await {
        warn!(task_id = %task_id, error = %e, "Failed to record task result in queue");
    }

    let _ = registry.decrement_active_tasks(&worker_id).await;

    let mut lock = waiters.write().await;
    if let Some(senders) = lock.remove(&task_id) {
        for tx in senders {
            let _ = tx.send(result.clone());
        }
    }

    scheduler_notify.notify_one();
}

/// Handles incoming `WorkerMessage::TaskProgress` messages.
pub async fn handle_task_progress(
    queue: &TaskQueue,
    worker_id: Uuid,
    task_id: TaskId,
    status: TaskStatus,
) {
    #[cfg(feature = "dashboard")]
    {
        handle_task_progress_with_broadcast(queue, worker_id, task_id, status, None).await;
    }
    #[cfg(not(feature = "dashboard"))]
    {
        debug!(worker_id = %worker_id, task_id = %task_id, ?status, "Task progress update");
        if status == TaskStatus::Running {
            let _ = queue.mark_running(&task_id, worker_id).await;
        }
    }
}

/// Handles incoming `WorkerMessage::TaskProgress` messages with optional dashboard telemetry broadcast.
#[cfg(feature = "dashboard")]
pub async fn handle_task_progress_with_broadcast(
    queue: &TaskQueue,
    worker_id: Uuid,
    task_id: TaskId,
    status: TaskStatus,
    broadcast_tx: Option<&tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>>,
) {
    debug!(worker_id = %worker_id, task_id = %task_id, ?status, "Task progress update");
    if status == TaskStatus::Running {
        let _ = queue.mark_running(&task_id, worker_id).await;
        if let Some(b_tx) = broadcast_tx {
            if let Some(info) = queue.get_task(&task_id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(queue.stats().await));
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
    #[cfg(feature = "dashboard")]
    broadcast_tx: Option<&tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>>,
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
        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::WorkerDisconnected {
            worker_id: *worker_id,
            reason: reason.to_string(),
        });
    }

    // Failover all orphaned tasks assigned to this worker
    let affected_tasks = queue
        .handle_worker_disconnected(worker_id, reason, immediate_reschedule)
        .await;
    if !affected_tasks.is_empty() {
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
                    let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
                }
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(queue.stats().await));
        }

        // Resolve waiters for any tasks that reached terminal state (e.g. retries exhausted)
        if let Some(waiters) = waiters {
            for task_id in &affected_tasks {
                if let Some(info) = queue.get_task(task_id).await {
                    if info.state.is_terminal() {
                        let res = queue.get_result(task_id).await.unwrap_or_else(|| {
                            TaskResult::failure(
                                info.assigned_worker_id.unwrap_or_else(Uuid::nil),
                                *task_id,
                                if info.state == TaskState::Cancelled { 130 } else { 1 },
                                "",
                                "",
                                0,
                                info.error_message.clone().or_else(|| Some("Task terminated".into())),
                            )
                        });
                        let mut lock = waiters.write().await;
                        if let Some(senders) = lock.remove(task_id) {
                            for tx in senders {
                                let _ = tx.send(res.clone());
                            }
                        }
                    }
                }
            }
        }

        scheduler_notify.notify_one();
    }
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
    ).await;
}

async fn handle_client_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    first_msg: ClientMessage,
    mut transport: MessageTransport<S>,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    #[cfg(feature = "dashboard")]
    broadcast_tx: Option<tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>>,
) -> GridResult<()> {
    let mut next_msg = Some(first_msg);
    let mapreduce = MapReduceEngine::new(queue.clone(), scheduler_notify.clone(), waiters.clone());

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

                queue.submit(task).await?;
                scheduler_notify.notify_one();

                #[cfg(feature = "dashboard")]
                if let Some(ref b_tx) = broadcast_tx {
                    if let Some(info) = queue.get_task(&task_id).await {
                        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
                    }
                    let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(queue.stats().await));
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
                let resp = match queue.get_task(&task_id).await {
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
                                TaskState::Failed => {
                                    (TaskStatus::Failed, info.error_message.clone())
                                }
                                TaskState::Cancelled => {
                                    (TaskStatus::Cancelled, info.error_message.clone())
                                }
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
                };
                transport.send_msg(&resp).await?;
            }
            ClientMessage::CancelTask { task_id } => {
                let success = queue
                    .cancel_task(&task_id, Some("Cancelled by client".into()))
                    .await
                    .is_ok();
                #[cfg(feature = "dashboard")]
                if success {
                    if let Some(ref b_tx) = broadcast_tx {
                        if let Some(info) = queue.get_task(&task_id).await {
                            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
                        }
                        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(queue.stats().await));
                    }
                }
                let resp = ClientResponse::TaskCancelled { task_id, success };
                transport.send_msg(&resp).await?;
            }
            ClientMessage::ClusterStatus => {
                let workers_info = registry.list_all_workers().await;
                let workers = workers_info.into_iter().map(|w| w.capabilities).collect();
                let stats = queue.stats().await;
                let resp = ClientResponse::ClusterStatus {
                    total_tasks: stats.total,
                    pending_tasks: stats.queued,
                    running_tasks: stats.running,
                    completed_tasks: stats.completed,
                    failed_tasks: stats.failed,
                    workers,
                };
                transport.send_msg(&resp).await?;
            }
            ClientMessage::ListWorkers => {
                let workers_info = registry.list_all_workers().await;
                let workers = workers_info.into_iter().map(|w| w.capabilities).collect();
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
        }
    }

    Ok(())
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
    #[cfg(feature = "dashboard")]
    broadcast_tx: Option<tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>>,
) -> GridResult<()> {
    let mut transport = MessageTransport::with_codec(stream, wire_codec);

    // 1. Handshake watchdog with timeout (Slowloris defense)
    let (first_msg, detected_codec) = match tokio::time::timeout(
        handshake_timeout,
        transport.recv_msg_with_codec::<InboundMessage>(),
    )
    .await
    {
        Ok(Ok(Some((msg, codec)))) => (msg, codec),
        Ok(Ok(None)) => {
            debug!(remote = %remote_addr, "Peer closed connection before handshake");
            return Err(GridError::ConnectionClosed);
        }
        Ok(Err(e)) => {
            warn!(remote = %remote_addr, error = %e, "Protocol error during handshake");
            return Err(e.into());
        }
        Err(_) => {
            warn!(remote = %remote_addr, timeout_secs = handshake_timeout.as_secs(), "Handshake watchdog timed out");
            return Err(GridError::Timeout {
                operation: "handshake".into(),
                duration_secs: handshake_timeout.as_secs(),
            });
        }
    };

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
                #[cfg(feature = "dashboard")]
                broadcast_tx,
            )
            .await;
        }
        InboundMessage::Worker(wm) => wm,
    };

    // 2. Validate Register message
    let (worker_id, capabilities) = match worker_msg {
        WorkerMessage::Register {
            worker_id,
            capabilities,
        } => {
            if capabilities.cpu_cores == 0 {
                let reject_ack = MasterMessage::RegisterAck {
                    accepted: false,
                    worker_id,
                    heartbeat_interval_secs: 0,
                    message: Some("Invalid capability: cpu_cores must be > 0".into()),
                };
                let _ = transport.send_msg(&reject_ack).await;
                return Err(GridError::RegistrationRejected(
                    "cpu_cores must be > 0".into(),
                ));
            }
            (worker_id, capabilities)
        }
        other => {
            let reject_ack = MasterMessage::RegisterAck {
                accepted: false,
                worker_id: Uuid::nil(),
                heartbeat_interval_secs: 0,
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
    let session_id = registry
        .register(
            worker_id,
            capabilities,
            remote_addr,
            outbound_tx.clone(),
            abort_handle,
        )
        .await?;

    // 4. Send RegisterAck
    let ack = MasterMessage::RegisterAck {
        accepted: true,
        worker_id,
        heartbeat_interval_secs,
        message: None,
    };
    if let Err(e) = transport.send_msg(&ack).await {
        let _ = registry.unregister(&worker_id, Some(session_id)).await;
        return Err(e.into());
    }

    #[cfg(feature = "dashboard")]
    if let Some(ref b_tx) = broadcast_tx {
        if let Some(info) = registry.get_worker(worker_id).await {
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::WorkerRegistered(info));
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

    struct AbortOnDrop(tokio::task::AbortHandle);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
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
                                    let _ = registry
                                        .record_heartbeat(
                                            &worker_id,
                                            active_tasks,
                                            timestamp,
                                            cpu_usage_pct,
                                            ram_available_mb,
                                            Some(session_id),
                                        )
                                        .await;
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
                            WorkerMessage::TaskProgress { worker_id: id, task_id, status } => {
                                #[cfg(feature = "dashboard")]
                                handle_task_progress_with_broadcast(&queue, id, task_id, status, broadcast_tx.as_ref()).await;
                                #[cfg(not(feature = "dashboard"))]
                                handle_task_progress(&queue, id, task_id, status).await;
                            }
                            WorkerMessage::TaskResult {
                                worker_id: id,
                                task_id,
                                exit_code,
                                stdout,
                                stderr,
                                execution_time_ms,
                                is_gpu_executed,
                                error,
                            } => {
                                let result = TaskResult {
                                    worker_id: id,
                                    task_id,
                                    exit_code,
                                    stdout,
                                    stderr,
                                    execution_time_ms,
                                    is_gpu_executed,
                                    error,
                                };
                                #[cfg(feature = "dashboard")]
                                handle_task_result_with_broadcast(
                                    &registry,
                                    &queue,
                                    &scheduler_notify,
                                    &waiters,
                                    result,
                                    broadcast_tx.as_ref(),
                                ).await;
                                #[cfg(not(feature = "dashboard"))]
                                handle_task_result(
                                    &registry,
                                    &queue,
                                    &scheduler_notify,
                                    &waiters,
                                    result,
                                ).await;
                            }
                            WorkerMessage::Register { .. } => {
                                warn!(worker_id = %worker_id, "Ignoring unexpected Register message on active connection");
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
    writer_task.abort();
    Ok(())
}

/// Thread-safe client and programmatic handle to a running Master node.
#[derive(Clone)]
pub struct MasterHandle {
    server_addr: SocketAddr,
    dashboard_addr: Option<SocketAddr>,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    shutdown_tx: watch::Sender<bool>,
    #[cfg(feature = "dashboard")]
    pub broadcast_tx: Option<tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>>,
}

impl MasterHandle {
    /// Returns the bound dashboard socket address, if the dashboard is enabled.
    pub fn dashboard_addr(&self) -> Option<SocketAddr> {
        self.dashboard_addr
    }

    /// Returns the bound dashboard TCP port number, if the dashboard is enabled.
    pub fn dashboard_port(&self) -> Option<u16> {
        self.dashboard_addr.map(|a| a.port())
    }

    /// Returns a clone of the broadcast sender for telemetry events, if available.
    #[cfg(feature = "dashboard")]
    pub fn broadcast_tx(&self) -> Option<tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>> {
        self.broadcast_tx.clone()
    }

    /// Submits a task with default priority (0).
    pub async fn submit_task(&self, task: Task) -> GridResult<TaskId> {
        let id = self.queue.submit(task).await?;
        self.scheduler_notify.notify_one();
        #[cfg(feature = "dashboard")]
        if let Some(ref b_tx) = self.broadcast_tx {
            if let Some(info) = self.queue.get_task(&id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(self.queue.stats().await));
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
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(self.queue.stats().await));
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

    /// Lists all tasks currently managed by the Master.
    pub async fn list_tasks(&self) -> GridResult<Vec<TaskInfo>> {
        Ok(self.queue.list_tasks().await)
    }

    /// Cancels a queued or active task.
    pub async fn cancel_task(&self, task_id: TaskId) -> GridResult<()> {
        let worker_to_notify = self
            .queue
            .cancel_task(&task_id, Some("Cancelled by user".into()))
            .await?;

        if let Some(worker_id) = worker_to_notify {
            let msg = MasterMessage::CancelTask {
                task_id,
                reason: Some("Cancelled by user".into()),
            };
            let _ = self.registry.send_to_worker(&worker_id, msg).await;
        }

        // Notify any registered waiters with cancelled result (exit code 130)
        let cancel_result = TaskResult::failure(
            worker_to_notify.unwrap_or_else(Uuid::nil),
            task_id,
            130,
            "",
            "",
            0,
            Some("Task cancelled".into()),
        );
        let mut lock = self.waiters.write().await;
        if let Some(senders) = lock.remove(&task_id) {
            for tx in senders {
                let _ = tx.send(cancel_result.clone());
            }
        }

        self.scheduler_notify.notify_one();
        #[cfg(feature = "dashboard")]
        if let Some(ref b_tx) = self.broadcast_tx {
            if let Some(info) = self.queue.get_task(&task_id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(self.queue.stats().await));
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
        if let Some(info) = self.queue.get_task(&task_id).await {
            if info.state.is_terminal() {
                if let Some(res) = self.queue.get_result(&task_id).await {
                    return Ok(res);
                } else {
                    return Ok(TaskResult::failure(
                        info.assigned_worker_id.unwrap_or_else(Uuid::nil),
                        task_id,
                        if info.state == TaskState::Cancelled { 130 } else { 1 },
                        "",
                        "",
                        0,
                        info.error_message.clone().or_else(|| Some("Task terminated".into())),
                    ));
                }
            }
        } else {
            return Err(GridError::TaskNotFound(task_id.0));
        }

        // Slow-path: register oneshot channel
        let (tx, rx) = oneshot::channel();
        {
            let mut lock = self.waiters.write().await;
            if let Some(res) = self.queue.get_result(&task_id).await {
                return Ok(res);
            }
            if let Some(info) = self.queue.get_task(&task_id).await {
                if info.state.is_terminal() {
                    return Ok(self.queue.get_result(&task_id).await.unwrap_or_else(|| {
                        TaskResult::failure(
                            info.assigned_worker_id.unwrap_or_else(Uuid::nil),
                            task_id,
                            if info.state == TaskState::Cancelled { 130 } else { 1 },
                            "",
                            "",
                            0,
                            info.error_message.clone().or_else(|| Some("Task terminated".into())),
                        )
                    }));
                }
            }
            lock.entry(task_id).or_default().push(tx);
        }

        let wait_fut = async { rx.await.map_err(|_| GridError::ConnectionClosed) };

        if let Some(duration) = timeout {
            tokio::time::timeout(duration, wait_fut)
                .await
                .map_err(|_| GridError::Timeout {
                    operation: format!("wait_task({task_id})"),
                    duration_secs: duration.as_secs(),
                })?
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
        );
        engine.execute(spec).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
