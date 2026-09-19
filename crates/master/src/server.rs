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
use rusty_grid_core::{GridError, GridResult, MasterMessage, MessageTransport, WorkerMessage};

use crate::mapreduce::MapReduceEngine;
use crate::queue::{QueueStats, TaskInfo, TaskQueue, TaskState};
use crate::reaper::{spawn_reaper_with_queue, ReaperConfig};
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
        let listener = TcpListener::bind(config.bind_addr)
            .await
            .map_err(GridError::Io)?;
        let local_addr = listener.local_addr().map_err(GridError::Io)?;

        if let Some(ref port_path) = config.port_file {
            write_port_file(port_path, local_addr.port()).await?;
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
        let queue = TaskQueue::new();
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let waiters = Arc::new(RwLock::new(HashMap::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server = Self::bind_full(
            config,
            registry.clone(),
            queue.clone(),
            scheduler_notify.clone(),
            Arc::clone(&waiters),
        )
        .await?;
        let server_addr = server.local_addr();

        // 1. Spawn TCP server accept loop
        let srv_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let _ = server.run(srv_shutdown_rx).await;
        });

        // 2. Spawn Background Scheduler loop
        let sched = Arc::new(WorkloadScheduler::new(
            sched_config,
            registry.clone(),
            queue.clone(),
            scheduler_notify.clone(),
        ));
        let sched_shutdown_rx = shutdown_rx.clone();
        sched.spawn(sched_shutdown_rx);

        // 3. Spawn Heartbeat Reaper with Queue Failover
        let reaper_shutdown_rx = shutdown_rx.clone();
        spawn_reaper_with_queue(
            registry.clone(),
            queue.clone(),
            scheduler_notify.clone(),
            reaper_config,
            reaper_shutdown_rx,
        );

        Ok(MasterHandle {
            server_addr,
            registry,
            queue,
            scheduler_notify,
            waiters,
            shutdown_tx,
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

    /// Runs the master server accept loop until `shutdown_rx` signals termination.
    pub async fn run(self, mut shutdown_rx: watch::Receiver<bool>) -> GridResult<()> {
        info!(listen_addr = %self.local_addr, "Master server accept loop started");

        #[cfg(feature = "p2p")]
        if self.config.enable_p2p {
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
                .alpns(vec![GRID_ALPN.to_vec()])
                .bind()
                .await
                .map_err(|e| GridError::Config(format!("Failed to bind iroh endpoint: {e}")))?;
            let addr = endpoint.addr();
            let ticket = rusty_grid_core::transport::serialize_p2p_ticket(&addr)?;
            info!(ticket = %ticket, "Master P2P endpoint established");

            if let Some(ref ticket_path) = self.config.p2p_ticket_file {
                if let Some(parent) = ticket_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        let _ = tokio::fs::create_dir_all(parent).await;
                    }
                }
                tokio::fs::write(ticket_path, &ticket).await.map_err(GridError::Io)?;
                info!(path = %ticket_path.display(), "Published P2P ticket to file");
            }

            let p2p_shutdown_rx = shutdown_rx.clone();
            let p2p_reg = self.registry.clone();
            let p2p_queue = self.queue.clone();
            let p2p_sched = self.scheduler_notify.clone();
            let p2p_waiters = Arc::clone(&self.waiters);
            let p2p_hb = self.config.heartbeat_interval_secs;
            let p2p_to = Duration::from_secs(self.config.handshake_timeout_secs);

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

/// Handles incoming `WorkerMessage::TaskProgress` messages.
pub async fn handle_task_progress(
    queue: &TaskQueue,
    worker_id: Uuid,
    task_id: TaskId,
    status: TaskStatus,
) {
    debug!(worker_id = %worker_id, task_id = %task_id, ?status, "Task progress update");
    if status == TaskStatus::Running {
        let _ = queue.mark_running(&task_id, worker_id).await;
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
) {
    handle_worker_disconnect_internal(
        registry,
        queue,
        scheduler_notify,
        None,
        worker_id,
        session_id,
        reason,
    )
    .await;
}

/// Handles worker disconnection and notifies active waiters for any tasks that reached terminal state.
pub async fn handle_worker_disconnect_with_waiters(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
) {
    handle_worker_disconnect_internal(
        registry,
        queue,
        scheduler_notify,
        Some(waiters),
        worker_id,
        session_id,
        reason,
    )
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
) {
    let unregistered = registry
        .unregister(worker_id, session_id)
        .await
        .unwrap_or(false);
    if !unregistered {
        debug!(worker_id = %worker_id, "Worker unregister skipped (stale session or already handled)");
        return;
    }

    warn!(worker_id = %worker_id, reason = %reason, "Worker disconnected; executing failover sweep");

    // Failover all orphaned tasks assigned to this worker
    let affected_tasks = queue.handle_worker_disconnected(worker_id, reason).await;
    if !affected_tasks.is_empty() {
        info!(
            worker_id = %worker_id,
            count = affected_tasks.len(),
            tasks = ?affected_tasks,
            "Re-enqueued orphaned tasks for reassignment"
        );

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

async fn handle_client_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    first_msg: ClientMessage,
    mut transport: MessageTransport<S>,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
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
) -> GridResult<()> {
    let mut transport = MessageTransport::new(stream);

    // 1. Handshake watchdog with timeout (Slowloris defense)
    let first_msg = match tokio::time::timeout(
        handshake_timeout,
        transport.recv_msg::<InboundMessage>(),
    )
    .await
    {
        Ok(Ok(Some(msg))) => msg,
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

    let worker_msg = match first_msg {
        InboundMessage::Client(client_msg) => {
            return handle_client_connection(
                client_msg,
                transport,
                registry,
                queue,
                scheduler_notify,
                waiters,
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
                                } else {
                                    warn!(expected = %worker_id, got = %id, "Heartbeat worker_id mismatch");
                                }
                            }
                            WorkerMessage::Disconnecting { worker_id: id, reason } => {
                                info!(worker_id = %id, reason = %reason, "Worker announced graceful disconnection");
                                handle_worker_disconnect_with_waiters(
                                    &registry,
                                    &queue,
                                    &scheduler_notify,
                                    &waiters,
                                    &worker_id,
                                    Some(session_id),
                                    &format!("Worker graceful disconnect: {reason}"),
                                ).await;
                                break;
                            }
                            WorkerMessage::TaskProgress { worker_id: id, task_id, status } => {
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
                        handle_worker_disconnect_with_waiters(
                            &registry,
                            &queue,
                            &scheduler_notify,
                            &waiters,
                            &worker_id,
                            Some(session_id),
                            "TCP connection EOF",
                        ).await;
                        break;
                    }
                    Err(e) => {
                        warn!(worker_id = %worker_id, error = %e, "Inbound message read error");
                        handle_worker_disconnect_with_waiters(
                            &registry,
                            &queue,
                            &scheduler_notify,
                            &waiters,
                            &worker_id,
                            Some(session_id),
                            &format!("Inbound socket error: {e}"),
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
    handle_worker_disconnect_with_waiters(
        &registry,
        &queue,
        &scheduler_notify,
        &waiters,
        &worker_id,
        Some(session_id),
        "Connection terminated",
    )
    .await;
    writer_task.abort();
    Ok(())
}

/// Thread-safe client and programmatic handle to a running Master node.
#[derive(Clone)]
pub struct MasterHandle {
    server_addr: SocketAddr,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    shutdown_tx: watch::Sender<bool>,
}

impl MasterHandle {
    /// Submits a task with default priority (0).
    pub async fn submit_task(&self, task: Task) -> GridResult<TaskId> {
        let id = self.queue.submit(task).await?;
        self.scheduler_notify.notify_one();
        Ok(id)
    }

    /// Submits a task with explicit priority.
    pub async fn submit_task_with_priority(&self, task: Task, priority: u32) -> GridResult<TaskId> {
        let id = self.queue.submit_with_priority(task, priority).await?;
        self.scheduler_notify.notify_one();
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
