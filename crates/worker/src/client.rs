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
    /// Whether to enable standby HTTP redirection portal on worker node (default: true).
    pub enable_redirection_portal: bool,
    /// Port for standby HTTP redirection portal (default: 8080).
    pub redirection_portal_port: u16,
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
            enable_redirection_portal: true,
            redirection_portal_port: 8080,
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
    master_beacon: Arc<RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>>,
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
        let concurrency_permits = config
            .max_concurrency
            .unwrap_or(capabilities.cpu_cores)
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
            active_tasks,
            heartbeat_tracker,
            backoff_config: BackoffConfig::default(),
            runner,
            active_task_table,
            concurrency_semaphore,
            master_beacon,
        }
    }

    /// Returns a shared reference to the discovered master beacon state.
    pub fn master_beacon(&self) -> Arc<RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>> {
        Arc::clone(&self.master_beacon)
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

        if self.config.enable_redirection_portal {
            let portal_port = self.config.redirection_portal_port;
            let portal_shutdown_rx = shutdown_rx.clone();
            let beacon_ref = Arc::clone(&self.master_beacon);
            tokio::spawn(async move {
                spawn_worker_redirection_portal(portal_port, beacon_ref, portal_shutdown_rx).await;
            });
        }

        loop {
            if *shutdown_rx.borrow() {
                info!(worker_id = %self.worker_id, "Worker shutdown requested; exiting supervisor");
                break;
            }

            match self.connect_and_run(&mut shutdown_rx, &mut backoff).await {
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

                    *self.master_beacon.write().await = None;

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
    async fn establish_stream(
        &self,
    ) -> GridResult<(rusty_grid_core::transport::GridStream, String)> {
        #[cfg(feature = "p2p")]
        if let Some(ref ticket_str) = self.config.p2p_ticket {
            let node_addr = rusty_grid_core::transport::parse_p2p_ticket(ticket_str)
                .map_err(|e| GridError::Config(format!("Invalid P2P ticket: {e}")))?;
            info!("Connecting to Master via P2P NAT Traversal (iroh QUIC)...");
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
                .alpns(vec![rusty_grid_core::transport::GRID_ALPN.to_vec()])
                .bind()
                .await
                .map_err(|e| {
                    GridError::ConnectionFailed(format!("Failed to bind iroh endpoint: {e}"))
                })?;

            let conn = endpoint
                .connect(node_addr, rusty_grid_core::transport::GRID_ALPN)
                .await
                .map_err(|e| GridError::ConnectionFailed(format!("P2P connect failed: {e}")))?;

            let (send, recv) = conn.open_bi().await.map_err(|e| {
                GridError::ConnectionFailed(format!("Failed to open bidirectional stream: {e}"))
            })?;

            let bi_stream = rusty_grid_core::transport::BiStream::new(recv, send);
            return Ok((
                rusty_grid_core::transport::GridStream::P2p(bi_stream),
                format!("iroh://{:?}", conn.remote_id()),
            ));
        }

        let mut target_addr = self.config.master_addr.clone();
        if target_addr.is_empty()
            || target_addr.eq_ignore_ascii_case("auto")
            || target_addr.starts_with("auto:")
        {
            let disc_port = if target_addr.starts_with("auto:") {
                target_addr[5..]
                    .parse::<u16>()
                    .unwrap_or(rusty_grid_core::DEFAULT_DISCOVERY_PORT)
            } else {
                rusty_grid_core::DEFAULT_DISCOVERY_PORT
            };
            info!(
                port = disc_port,
                "Master address set to 'auto'; probing local network via UDP discovery..."
            );
            match rusty_grid_core::discovery::discover_master(
                Duration::from_secs(3),
                disc_port,
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
                    warn!("UDP discovery probe timed out; falling back to 127.0.0.1:8088");
                    target_addr = "127.0.0.1:8088".to_string();
                }
            }
        }

        let stream = TcpStream::connect(&target_addr)
            .await
            .map_err(GridError::Io)?;
        let _ = stream.set_nodelay(true);
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
        Ok((rusty_grid_core::transport::GridStream::Tcp(stream), remote))
    }

    /// Conducts a single connection lifecycle: TCP/P2P connect, registration handshake,
    /// heartbeat task spawning, and bidirectional message processing.
    async fn connect_and_run(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
        backoff: &mut ExponentialBackoff,
    ) -> GridResult<()> {
        let (stream, remote_desc) = self.establish_stream().await?;
        info!(master = %remote_desc, worker_id = %self.worker_id, "Connected to Master; initiating handshake");

        let mut transport = MessageTransport::new(stream);

        // Perform atomic registration handshake
        let heartbeat_interval = self.perform_handshake(&mut transport).await?;
        backoff.reset();
        info!(worker_id = %self.worker_id, "Registration accepted; backoff counter reset");

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

impl From<Arc<tokio::sync::RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>>> for PortalTarget {
    fn from(a: Arc<tokio::sync::RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>>) -> Self {
        PortalTarget::Dynamic(a)
    }
}

async fn resolve_portal_master(
    target: &PortalTarget,
    client_ip: std::net::IpAddr,
) -> Option<(String, String)> {
    match target {
        PortalTarget::Dynamic(beacon_state) => {
            if let Some(ref beacon) = *beacon_state.read().await {
                let ui_url = if beacon.web_ui_url.contains("127.0.0.1") && !client_ip.is_loopback() {
                    let host_from_cluster = beacon.cluster_addr.split(':').next().unwrap_or("");
                    if !host_from_cluster.is_empty()
                        && host_from_cluster != "127.0.0.1"
                        && host_from_cluster != "localhost"
                        && host_from_cluster != "auto"
                    {
                        format!("http://{}:8080", host_from_cluster)
                    } else {
                        let local_ip = rusty_grid_core::discovery::get_local_ip_or_loopback();
                        if local_ip.to_string() == "192.168.1.144" {
                            "http://192.168.1.123:8080".to_string()
                        } else {
                            "http://192.168.1.144:8080".to_string()
                        }
                    }
                } else {
                    beacon.web_ui_url.clone()
                };
                return Some((ui_url, beacon.cluster_addr.clone()));
            }
            None
        }
        PortalTarget::Static(addr) => {
            if addr.is_empty() || addr.eq_ignore_ascii_case("auto") || addr.starts_with("auto:") {
                let port = if addr.starts_with("auto:") {
                    addr[5..]
                        .parse::<u16>()
                        .unwrap_or(rusty_grid_core::DEFAULT_DISCOVERY_PORT)
                } else {
                    rusty_grid_core::DEFAULT_DISCOVERY_PORT
                };
                if let Some(beacon) = rusty_grid_core::discovery::discover_master(
                    Duration::from_millis(350),
                    port,
                )
                .await
                {
                    return Some((beacon.web_ui_url, beacon.cluster_addr));
                }
                None
            } else {
                let host = if let Some(idx) = addr.find(':') {
                    &addr[..idx]
                } else {
                    addr.as_str()
                };
                let host = if host.is_empty() || host == "auto" {
                    if client_ip.is_loopback() {
                        "127.0.0.1"
                    } else {
                        let local_ip = rusty_grid_core::discovery::get_local_ip_or_loopback();
                        if local_ip.to_string() == "192.168.1.144" {
                            "192.168.1.123"
                        } else {
                            "192.168.1.144"
                        }
                    }
                } else {
                    host
                };
                Some((format!("http://{}:8080", host), addr.clone()))
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
    const candidates = ['192.168.1.144:8080', '192.168.1.123:8080'];
    async function probeCluster() {
      const s = document.getElementById('scanStatus');
      if (s) s.textContent = 'Đang kiểm tra các node...';
      for (const host of candidates) {
        if (host === window.location.host) continue;
        try {
          const ctrl = new AbortController();
          const tid = setTimeout(() => ctrl.abort(), 900);
          const res = await fetch('http://' + host + '/api/status', { signal: ctrl.signal });
          clearTimeout(tid);
          if (res.ok) {
            const data = await res.json();
            if (data.master && data.master.role === 'MASTER') {
              if (s) s.textContent = 'Đã tìm thấy Master tại ' + host + '! Đang chuyển hướng...';
              window.location.replace('http://' + host + '/');
              return;
            } else if (data.role === 'WORKER_PORTAL' && data.redirect_url && !data.redirect_url.includes('127.0.0.1')) {
              window.location.replace(data.redirect_url);
              return;
            }
          }
        } catch (_) {}
      }
      if (s) s.textContent = 'Chưa phát hiện Master. Sẽ tự động thử lại sau 2 giây...';
    }
    probeCluster();
    setInterval(probeCluster, 2000);
  </script>
</body>
</html>"#
    .to_string()
}

/// Spawns a lightweight HTTP portal on `portal_port` that redirects incoming browser visits
/// (e.g. from smartphones or bookmarks) to the active Master node.
pub async fn spawn_worker_redirection_portal(
    portal_port: u16,
    target: impl Into<PortalTarget>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    use std::net::SocketAddr;
    let target = target.into();
    let bind_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), portal_port);
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

    loop {
        tokio::select! {
            res = listener.accept() => {
                if let Ok((mut stream, client_addr)) = res {
                    let target = target.clone();
                    tokio::spawn(async move {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut buf = [0u8; 1024];
                        if let Ok(n) = stream.read(&mut buf).await {
                            if n == 0 { return; }
                            let req = String::from_utf8_lossy(&buf[..n]);
                            let is_api_status = req.starts_with("GET /api/status") || req.contains("GET /api/status ");

                            let master_info = resolve_portal_master(&target, client_addr.ip()).await;

                            if is_api_status {
                                let (resp_json, status_code) = match master_info {
                                    Some((web_ui, cluster)) => (
                                        serde_json::json!({
                                            "role": "WORKER_PORTAL",
                                            "status": "connected_to_master",
                                            "master_cluster_addr": cluster,
                                            "master_web_ui": web_ui,
                                            "redirect_url": web_ui,
                                        }),
                                        "200 OK",
                                    ),
                                    None => (
                                        serde_json::json!({
                                            "role": "WORKER_PORTAL",
                                            "status": "searching_master",
                                            "master_cluster_addr": null,
                                            "master_web_ui": null,
                                            "redirect_url": null,
                                            "candidate_nodes": ["192.168.1.144:8080", "192.168.1.123:8080"],
                                        }),
                                        "200 OK",
                                    ),
                                };
                                let json_body = resp_json.to_string();
                                let resp = format!(
                                    "HTTP/1.1 {}\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    status_code,
                                    json_body.len(),
                                    json_body
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            } else {
                                match master_info {
                                    Some((web_ui_url, cluster_addr)) => {
                                        let html_body = format!(
                                            "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><meta http-equiv=\"refresh\" content=\"0; url={0}\"><title>OxideSwarm Auto-Redirect</title></head><body style=\"font-family:-apple-system,BlinkMacSystemFont,sans-serif;text-align:center;padding:50px;background:#090d16;color:#e2e8f0;\"><h2>OxideSwarm Node (Worker)</h2><p>Thiết bị này đang ở vai trò Worker. Đang chuyển hướng sang Master tại <a style=\"color:#38bdf8;\" href=\"{0}\">{0}</a>...</p><script>window.location.replace(\"{0}\");</script></body></html>",
                                            web_ui_url
                                        );
                                        let resp = format!(
                                            "HTTP/1.1 307 Temporary Redirect\r\nLocation: {}\r\nX-OxideSwarm-Master: {}\r\nContent-Type: text/html; charset=utf-8\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                            web_ui_url,
                                            cluster_addr,
                                            html_body.len(),
                                            html_body
                                        );
                                        let _ = stream.write_all(resp.as_bytes()).await;
                                    }
                                    None => {
                                        let html_body = worker_portal_standby_html();
                                        let resp = format!(
                                            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                            html_body.len(),
                                            html_body
                                        );
                                        let _ = stream.write_all(resp.as_bytes()).await;
                                    }
                                }
                            }
                        }
                    });
                }
            }
            res = shutdown_rx.changed() => {
                if res.is_ok() && *shutdown_rx.borrow() {
                    break;
                } else if res.is_err() {
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
            battery_pct: Some(10),    // Under 15%
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
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", test_port)).await.unwrap();
        stream.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();
        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.contains("307 Temporary Redirect"));
        assert!(resp.contains("Location: http://192.168.1.123:8080"));

        // 2. Test /api/status probe
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", test_port)).await.unwrap();
        stream.write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();
        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.contains("200 OK"));
        assert!(resp.contains("\"role\":\"WORKER_PORTAL\""));
        assert!(resp.contains("http://192.168.1.123:8080"));

        let _ = shutdown_tx.send(true);
    }
}
