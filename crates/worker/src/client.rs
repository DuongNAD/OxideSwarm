//! Worker TCP client, registration handshake, and communication event loop.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};
use rusty_grid_core::task::{Task, TaskId, TaskResult, TaskStatus};

use crate::backoff::{BackoffConfig, ExponentialBackoff};
use crate::heartbeat::{current_epoch_secs, spawn_heartbeat_task, HeartbeatTracker};
use crate::runner::{RunnerConfig, TaskRunner, DEFAULT_MAX_OUTPUT_BYTES, EXIT_CODE_CANCELLED};
use crate::sandbox::SandboxConfig;

/// Execution handle for an actively running task on this worker.
pub struct TaskExecutionHandle {
    pub task_id: TaskId,
    pub cancel_tx: Option<oneshot::Sender<String>>,
    pub abort_handle: tokio::task::AbortHandle,
    pub child_pid: Arc<AtomicU32>,
    pub started_at_epoch_sec: u64,
}

/// Thread-safe registry of active tasks executing on the worker.
pub type ActiveTaskTable = Arc<RwLock<HashMap<TaskId, TaskExecutionHandle>>>;

/// RAII guard ensuring active task counter is decremented and table entry is pruned on completion or abort.
struct ActiveTaskGuard {
    task_id: TaskId,
    heartbeat_tracker: Arc<HeartbeatTracker>,
    active_task_table: ActiveTaskTable,
    _permit: OwnedSemaphorePermit,
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.heartbeat_tracker.decrement_active_tasks();
        let table = Arc::clone(&self.active_task_table);
        let task_id = self.task_id;
        tokio::spawn(async move {
            let mut lock = table.write().await;
            lock.remove(&task_id);
        });
    }
}

/// Configuration options for initializing and connecting a Worker client.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Address of the Master node to connect to (e.g. "127.0.0.1:8080").
    pub master_addr: String,
    /// Optional P2P connection ticket for connecting over iroh QUIC NAT traversal.
    pub p2p_ticket: Option<String>,
    /// Persistent Worker UUID. If `None`, generated once at client initialization and preserved across reconnects.
    pub worker_id: Option<Uuid>,
    /// Optional human-readable worker name override (CLI `--name`).
    pub name: Option<String>,
    /// Optional CPU core count override (CLI `--cores`).
    pub cores: Option<usize>,
    /// Optional total RAM MB override (CLI `--ram-mb`).
    pub ram_mb: Option<u64>,
    /// Explicit physical GPU presence toggle (CLI `--gpu`).
    pub gpu: Option<bool>,
    /// Explicitly disable all GPU capabilities (CLI `--no-gpu`).
    pub no_gpu: bool,
    /// Whether to advertise simulated GPU compute capabilities (CLI `--simulate-gpu`).
    pub simulate_gpu: bool,
    /// Custom GPU model/device name (CLI `--gpu-name`).
    pub gpu_name: Option<String>,
    /// Maximum concurrent task execution limit (CLI `--max-concurrency`).
    pub max_concurrency: Option<usize>,
    /// Fallback heartbeat interval if not specified by Master in `RegisterAck` (default: 3s).
    pub default_heartbeat_interval: Duration,
    /// Maximum duration to await Master response during registration handshake (default: 5s).
    pub handshake_timeout: Duration,
    /// Optional categorization tags for worker routing.
    pub tags: Vec<String>,
    /// Base directory for task sandbox isolation.
    pub sandbox_base_dir: Option<PathBuf>,
    /// Whether to keep sandbox directories after task completion.
    pub keep_sandboxes: bool,
    /// Maximum bytes to buffer for stdout and stderr before truncation.
    pub max_output_bytes: usize,
}

impl WorkerConfig {
    pub fn new(master_addr: impl Into<String>) -> Self {
        Self {
            master_addr: master_addr.into(),
            p2p_ticket: None,
            worker_id: None,
            name: None,
            cores: None,
            ram_mb: None,
            gpu: None,
            no_gpu: false,
            simulate_gpu: false,
            gpu_name: None,
            max_concurrency: None,
            default_heartbeat_interval: Duration::from_secs(3),
            handshake_timeout: Duration::from_secs(5),
            tags: Vec::new(),
            sandbox_base_dir: None,
            keep_sandboxes: false,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }

    pub fn with_p2p_ticket(mut self, ticket: impl Into<String>) -> Self {
        self.p2p_ticket = Some(ticket.into());
        self
    }

    pub fn with_worker_id(mut self, id: Uuid) -> Self {
        self.worker_id = Some(id);
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

    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
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
}


/// Autonomous Worker client managing connection, registration, task execution, and communication with Master.
pub struct WorkerClient {
    config: WorkerConfig,
    worker_id: Uuid,
    capabilities: WorkerCapabilities,
    active_tasks: Arc<AtomicUsize>,
    heartbeat_tracker: Arc<HeartbeatTracker>,
    backoff_config: BackoffConfig,
    runner: Arc<TaskRunner>,
    active_task_table: ActiveTaskTable,
    concurrency_semaphore: Arc<Semaphore>,
}

impl WorkerClient {
    /// Initializes a new WorkerClient, detecting host hardware and honoring CLI overrides.
    pub fn new(config: WorkerConfig) -> Self {
        let worker_id = config.worker_id.unwrap_or_else(Uuid::new_v4);
        let overrides = rusty_grid_core::capabilities::HardwareOverrides {
            name: config.name.clone(),
            cores: config.cores,
            ram_mb: config.ram_mb,
            gpu: config.gpu,
            no_gpu: config.no_gpu,
            simulate_gpu: config.simulate_gpu,
            gpu_name: config.gpu_name.clone(),
        };
        let capabilities =
            WorkerCapabilities::detect_with_overrides(overrides).with_tags(config.tags.clone());

        let active_tasks = Arc::new(AtomicUsize::new(0));
        let heartbeat_tracker = Arc::new(HeartbeatTracker::new(Arc::clone(&active_tasks)));

        let mut sandbox_cfg = SandboxConfig::default();
        if let Some(ref sb) = config.sandbox_base_dir {
            sandbox_cfg.base_dir = sb.clone();
        }
        sandbox_cfg.keep_sandboxes = config.keep_sandboxes;

        let runner_cfg =
            RunnerConfig::new(sandbox_cfg).with_max_output_bytes(config.max_output_bytes);
        let runner = Arc::new(TaskRunner::new(worker_id, capabilities.clone(), runner_cfg));
        let active_task_table = Arc::new(RwLock::new(HashMap::new()));
        let concurrency_permits = config.max_concurrency.unwrap_or(capabilities.cpu_cores).max(1);
        let concurrency_semaphore = Arc::new(Semaphore::new(concurrency_permits));

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
            active_tasks,
            heartbeat_tracker,
            backoff_config: BackoffConfig::default(),
            runner,
            active_task_table,
            concurrency_semaphore,
        }
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

    /// Primary entry point: runs the worker supervisor loop with automatic reconnection,
    /// exponential backoff, and jitter.
    pub async fn run(&mut self, mut shutdown_rx: watch::Receiver<bool>) -> GridResult<()> {
        let mut backoff = ExponentialBackoff::new(self.backoff_config.clone());

        loop {
            if *shutdown_rx.borrow() {
                info!(worker_id = %self.worker_id, "Worker shutdown requested; exiting supervisor");
                break;
            }

            match self.connect_and_run(&mut shutdown_rx).await {
                Ok(()) => {
                    info!(worker_id = %self.worker_id, "Worker connection ended cleanly");
                    break;
                }
                Err(e) => {
                    if *shutdown_rx.borrow() {
                        info!(worker_id = %self.worker_id, "Worker terminating on shutdown signal after connection notice");
                        break;
                    }

                    warn!(
                        worker_id = %self.worker_id,
                        error = %e,
                        attempt = backoff.attempt() + 1,
                        "Connection dropped or failed; applying exponential backoff before reconnect"
                    );

                    let delay = backoff.next_delay();
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
        let task_id = task.id;
        let worker_id = self.worker_id;

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

        // Mobile constraints: reject task if battery < 15% and discharging
        if let Some(ref mobile) = self.capabilities.mobile {
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

        let (cancel_tx, cancel_rx) = oneshot::channel::<String>();
        let child_pid = Arc::new(AtomicU32::new(0));

        let tracker = Arc::clone(&self.heartbeat_tracker);
        let table = Arc::clone(&self.active_task_table);
        let semaphore = Arc::clone(&self.concurrency_semaphore);
        let runner = Arc::clone(&self.runner);
        let child_pid_clone = Arc::clone(&child_pid);

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
                    table.write().await.remove(&task_id);
                    let _ = outbound_tx.send(WorkerMessage::from(res)).await;
                    return;
                }
            };

            let _guard = ActiveTaskGuard {
                task_id,
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

            let result = runner
                .execute_task(&task, Some(child_pid_clone), Some(cancel_rx))
                .await;

            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
        });

        let handle_entry = TaskExecutionHandle {
            task_id,
            cancel_tx: Some(cancel_tx),
            abort_handle: runner_handle.abort_handle(),
            child_pid,
            started_at_epoch_sec: current_epoch_secs(),
        };

        {
            let mut lock = self.active_task_table.write().await;
            lock.insert(task_id, handle_entry);
        }
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
        }
    }

    /// Establishes an underlying stream to the Master (either P2P QUIC via ticket, or direct TCP).
    async fn establish_stream(&self) -> GridResult<(rusty_grid_core::transport::GridStream, String)> {
        #[cfg(feature = "p2p")]
        if let Some(ref ticket_str) = self.config.p2p_ticket {
            let node_addr = rusty_grid_core::transport::parse_p2p_ticket(ticket_str)
                .map_err(|e| GridError::Config(format!("Invalid P2P ticket: {e}")))?;
            info!("Connecting to Master via P2P NAT Traversal (iroh QUIC)...");
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
                .alpns(vec![rusty_grid_core::transport::GRID_ALPN.to_vec()])
                .bind()
                .await
                .map_err(|e| GridError::ConnectionFailed(format!("Failed to bind iroh endpoint: {e}")))?;

            let conn = endpoint
                .connect(node_addr, rusty_grid_core::transport::GRID_ALPN)
                .await
                .map_err(|e| GridError::ConnectionFailed(format!("P2P connect failed: {e}")))?;

            let (send, recv) = conn
                .open_bi()
                .await
                .map_err(|e| GridError::ConnectionFailed(format!("Failed to open bidirectional stream: {e}")))?;

            let bi_stream = rusty_grid_core::transport::BiStream::new(recv, send);
            return Ok((
                rusty_grid_core::transport::GridStream::P2p(bi_stream),
                format!("iroh://{:?}", conn.remote_id()),
            ));
        }

        let stream = TcpStream::connect(&self.config.master_addr)
            .await
            .map_err(GridError::Io)?;
        let _ = stream.set_nodelay(true);
        let remote = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| self.config.master_addr.clone());
        Ok((rusty_grid_core::transport::GridStream::Tcp(stream), remote))
    }

    /// Conducts a single connection lifecycle: TCP/P2P connect, registration handshake,
    /// heartbeat task spawning, and bidirectional message processing.
    async fn connect_and_run(&mut self, shutdown_rx: &mut watch::Receiver<bool>) -> GridResult<()> {
        let (stream, remote_desc) = self.establish_stream().await?;
        info!(master = %remote_desc, worker_id = %self.worker_id, "Connected to Master; initiating handshake");

        let mut transport = MessageTransport::new(stream);

        // Perform atomic registration handshake
        let heartbeat_interval = self.perform_handshake(&mut transport).await?;

        // Split transport into independent reader and writer
        let (mut writer, mut reader) = transport.split();

        // Setup outbound channel and spawn heartbeat task
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<WorkerMessage>(64);
        let heartbeat_handle = spawn_heartbeat_task(
            self.worker_id,
            heartbeat_interval,
            outbound_tx.clone(),
            Arc::clone(&self.heartbeat_tracker),
        );

        let mut exit_result: GridResult<()> = Ok(());

        loop {
            tokio::select! {
                // Outbound messages to Master
                Some(worker_msg) = outbound_rx.recv() => {
                    if let Err(e) = writer.send_msg(&worker_msg).await {
                        error!(error = %e, "Failed to send message to Master");
                        exit_result = Err(GridError::from(e));
                        break;
                    }
                }

                // Inbound messages from Master
                incoming = reader.recv_msg::<MasterMessage>() => {
                    match incoming {
                        Ok(Some(msg)) => {
                            match msg {
                                MasterMessage::HeartbeatAck { timestamp } => {
                                    self.heartbeat_tracker.record_ack(timestamp, current_epoch_secs());
                                }
                                MasterMessage::Shutdown { reason, .. } => {
                                    warn!(reason = %reason, "Master ordered cluster shutdown; disconnecting");
                                    let disc_msg = WorkerMessage::Disconnecting {
                                        worker_id: self.worker_id,
                                        reason: "Master ordered shutdown".into(),
                                    };
                                    let _ = writer.send_msg(&disc_msg).await;
                                    exit_result = Ok(());
                                    break;
                                }
                                MasterMessage::AssignTask { task } => {
                                    debug!(task_id = %task.id, "AssignTask received; dispatching runner");
                                    self.handle_assign_task(task, outbound_tx.clone()).await;
                                }
                                MasterMessage::CancelTask { task_id, reason } => {
                                    debug!(task_id = %task_id, reason = ?reason, "CancelTask received; aborting task");
                                    self.handle_cancel_task(task_id, reason).await;
                                }
                                MasterMessage::RegisterAck { .. } => {
                                    warn!("Unexpected RegisterAck during active connection");
                                }
                            }
                        }
                        Ok(None) => {
                            info!("Master closed TCP connection (EOF)");
                            exit_result = Err(GridError::ConnectionClosed);
                            break;
                        }
                        Err(e) => {
                            warn!(error = %e, "Inbound message receive error");
                            exit_result = Err(e.into());
                            break;
                        }
                    }
                }

                // External shutdown signal
                res = shutdown_rx.changed() => {
                    if res.is_ok() && *shutdown_rx.borrow() {
                        info!(worker_id = %self.worker_id, "Graceful worker shutdown initiated; notifying Master");
                        let disc_msg = WorkerMessage::Disconnecting {
                            worker_id: self.worker_id,
                            reason: "Graceful worker shutdown".into(),
                        };
                        let _ = writer.send_msg(&disc_msg).await;
                        // Brief pause to allow message to flush
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        exit_result = Ok(());
                        break;
                    } else if res.is_err() {
                        break;
                    }
                }
            }
        }

        // Cleanly terminate heartbeat task
        heartbeat_handle.stop().await;
        exit_result
    }

    /// Executes the atomic registration handshake over the transport.
    pub async fn perform_handshake<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        transport: &mut MessageTransport<S>,
    ) -> GridResult<Duration> {
        let register_msg = WorkerMessage::Register {
            worker_id: self.worker_id,
            capabilities: self.capabilities.clone(),
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
        .map_err(|e| {
            GridError::HandshakeFailed(format!("Transport error awaiting RegisterAck: {e}"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::task::TaskRequirements;

    #[test]
    fn test_worker_config_builder() {
        let id = Uuid::new_v4();
        let config = WorkerConfig::new("127.0.0.1:8080")
            .with_worker_id(id)
            .with_name("test-node")
            .with_cores(16)
            .with_simulate_gpu(true)
            .with_handshake_timeout(Duration::from_secs(10))
            .with_sandbox_base_dir(PathBuf::from("/tmp/custom_sb"))
            .with_keep_sandboxes(true)
            .with_max_output_bytes(5 * 1024 * 1024)
            .with_tags(vec!["fast".into(), "gpu".into()]);

        assert_eq!(config.master_addr, "127.0.0.1:8080");
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
        let mut client = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999"));
        // Inject low battery mobile capabilities
        client.capabilities.mobile = Some(rusty_grid_core::capabilities::MobileCapabilities {
            os_version: "Android 14".into(),
            soc_model: "Snapdragon 8 Gen 3".into(),
            battery_pct: Some(10), // Under 15%
            is_charging: Some(false), // Discharging
            thermal_throttled: false,
        });

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
}
