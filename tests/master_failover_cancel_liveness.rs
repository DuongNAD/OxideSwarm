//! End-to-end tests for the master's failover, cancel and liveness correctness, against a real
//! `MasterServer` and protocol-level fake workers (raw TCP / QUIC), so every timing and every
//! message is under the test's control.
//!
//! Validates:
//! * a worker re-registering under the same id while its old session is still live gets the old
//!   session's in-flight tasks failed over exactly once, and the old connection is torn down;
//! * results, progress and checkpoints are only accepted from the worker the task is assigned to,
//!   whatever `worker_id` the message body claims;
//! * `ClientMessage::CancelTask` reaches the executing worker and wakes `wait = true` clients;
//! * `SubmitTask` validation / duplicate-id errors are answered, keep the connection and leak no waiter;
//! * `ClientMessage::GetTaskResult` (both wire codecs, real worker);
//! * `ClusterStatus` counters cover every task state;
//! * a worker uploading a result that outlasts the reaper timeout is not reaped, a silent one still is;
//! * a worker that rejects tasks instantly does not swallow the cluster's work;
//! * QUIC workers are really disconnected when reaped or superseded.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{watch, RwLock};
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{
    master_features, serialize_message, ClientMessage, ClientResponse, MasterMessage,
    MessageTransport, ProtocolCapabilities, WireCodec, WorkerMessage, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::{
    Bytes as TaskBytes, Task, TaskId, TaskRequirements, TaskSpec, TaskStatus,
};
use rusty_grid_master::queue::{TaskQueue, TaskState};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::{WorkerRegistry, WorkerStatus};
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;

const SHORT: Duration = Duration::from_secs(5);

async fn poll_until<F, Fut>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    condition().await
}

/// Master whose reaper is effectively off (60 s), so only the behaviour under test acts.
async fn spawn_master() -> MasterHandle {
    spawn_master_with_reaper(ReaperConfig::new(
        Duration::from_secs(1),
        Duration::from_secs(60),
    ))
    .await
}

async fn spawn_master_with_reaper(reaper: ReaperConfig) -> MasterHandle {
    MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        SchedulerConfig::default(),
        reaper,
    )
    .await
    .expect("spawn master")
}

async fn state_of(master: &MasterHandle, task_id: TaskId) -> Option<TaskState> {
    master
        .get_task_info(task_id)
        .await
        .ok()
        .map(|info| info.state)
}

fn cmd_task(label: &str) -> Task {
    Task::new(
        TaskSpec::command("echo", vec![label.to_string()]),
        TaskRequirements::generic(1, 30),
    )
}

fn gpu_task() -> Task {
    Task::new(
        TaskSpec::gpu_compute("never-runs", 16),
        TaskRequirements::gpu(30),
    )
}

/// A protocol-level worker that does exactly what the test tells it to.
struct FakeWorker {
    worker_id: Uuid,
    transport: MessageTransport<TcpStream>,
    /// Heartbeat every 100 ms while waiting for a message (for tests with a short reaper timeout).
    keep_alive: bool,
}

impl FakeWorker {
    async fn connect(addr: &str, worker_id: Uuid, name: &str) -> Self {
        Self::connect_with_cores(addr, worker_id, name, 4).await
    }

    async fn connect_with_cores(addr: &str, worker_id: Uuid, name: &str, cores: usize) -> Self {
        let stream = TcpStream::connect(addr).await.expect("connect to master");
        let mut transport = MessageTransport::new(stream);
        transport
            .send_msg(&WorkerMessage::Register {
                worker_id,
                capabilities: WorkerCapabilities::new(name, cores, 8192, false, false, None),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .expect("send Register");
        match transport
            .recv_msg::<MasterMessage>()
            .await
            .expect("RegisterAck")
        {
            Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
            other => panic!("expected an accepted RegisterAck, got {other:?}"),
        }
        Self {
            worker_id,
            transport,
            keep_alive: false,
        }
    }

    fn keeping_alive(mut self) -> Self {
        self.keep_alive = true;
        self
    }

    /// Next master message other than heartbeat acks, or `None` after `within`, or on close.
    async fn next_message(&mut self, within: Duration) -> Option<MasterMessage> {
        let deadline = Instant::now() + within;
        loop {
            let left = deadline.checked_duration_since(Instant::now())?;
            let slice = if self.keep_alive {
                left.min(Duration::from_millis(100))
            } else {
                left
            };
            match tokio::time::timeout(slice, self.transport.recv_msg::<MasterMessage>()).await {
                Ok(Ok(Some(MasterMessage::HeartbeatAck { .. }))) => continue,
                Ok(Ok(Some(msg))) => return Some(msg),
                Ok(Ok(None)) | Ok(Err(_)) => return None,
                Err(_) if self.keep_alive => {
                    let worker_id = self.worker_id;
                    let _ = self
                        .transport
                        .send_msg(&WorkerMessage::Heartbeat {
                            worker_id,
                            timestamp: 1,
                            active_tasks: 1,
                            cpu_usage_pct: 1.0,
                            ram_available_mb: 4096,
                        })
                        .await;
                }
                Err(_) => return None,
            }
        }
    }

    async fn expect_assign(&mut self) -> Task {
        match self.next_message(SHORT).await {
            Some(MasterMessage::AssignTask { task }) => task,
            Some(MasterMessage::AssignTaskWithCheckpoint { task, .. }) => task,
            other => panic!("expected an AssignTask, got {other:?}"),
        }
    }

    /// True if the master closed this connection within `within`.
    async fn closed_by_master(&mut self, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        loop {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            match tokio::time::timeout(left, self.transport.recv_msg::<MasterMessage>()).await {
                Ok(Ok(Some(_))) => continue,
                Ok(Ok(None)) | Ok(Err(_)) => return true,
                Err(_) => return false,
            }
        }
    }

    async fn send(&mut self, msg: WorkerMessage) {
        self.transport.send_msg(&msg).await.expect("send to master");
    }

    async fn running(&mut self, task_id: TaskId) {
        let worker_id = self.worker_id;
        self.send(WorkerMessage::TaskProgress {
            worker_id,
            task_id,
            status: TaskStatus::Running,
        })
        .await;
    }

    async fn finish(&mut self, task_id: TaskId, exit_code: i32, stdout: &str) {
        let msg = result_msg(self.worker_id, task_id, exit_code, stdout.as_bytes());
        self.send(msg).await;
    }

    /// Sends a heartbeat and waits for its ack: everything sent before it has been processed.
    async fn sync(&mut self) {
        let worker_id = self.worker_id;
        self.send(WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 1,
            active_tasks: 0,
            cpu_usage_pct: 1.0,
            ram_available_mb: 4096,
        })
        .await;
        loop {
            match tokio::time::timeout(SHORT, self.transport.recv_msg::<MasterMessage>()).await {
                Ok(Ok(Some(MasterMessage::HeartbeatAck { .. }))) => return,
                Ok(Ok(Some(_))) => continue,
                other => panic!("no HeartbeatAck: {other:?}"),
            }
        }
    }
}

fn result_msg(worker_id: Uuid, task_id: TaskId, exit_code: i32, stdout: &[u8]) -> WorkerMessage {
    WorkerMessage::TaskResult {
        worker_id,
        task_id,
        exit_code,
        stdout: TaskBytes::copy_from_slice(stdout),
        stderr: TaskBytes::new(),
        execution_time_ms: 7,
        is_gpu_executed: false,
        device_name: None,
        error: None,
    }
}

/// A protocol-level client (what the CLI and the MCP server are).
struct Client {
    transport: MessageTransport<TcpStream>,
}

impl Client {
    async fn connect(addr: &str) -> Self {
        Self::connect_with_codec(addr, WireCodec::default()).await
    }

    async fn connect_with_codec(addr: &str, codec: WireCodec) -> Self {
        let stream = TcpStream::connect(addr).await.expect("connect client");
        Self {
            transport: MessageTransport::with_codec(stream, codec),
        }
    }

    async fn ask(&mut self, msg: ClientMessage) -> ClientResponse {
        self.transport
            .send_msg(&msg)
            .await
            .expect("send client message");
        self.read().await
    }

    async fn read(&mut self) -> ClientResponse {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.transport.recv_msg::<ClientResponse>(),
        )
        .await
        .expect("master answered in time")
        .expect("valid response")
        .expect("connection stays open")
    }
}

// ---------------------------------------------------------------------------------------------
// Same-id re-registration
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_reregistering_worker_id_fails_over_the_superseded_session_exactly_once() {
    let master = spawn_master().await;
    let addr = master.server_addr().to_string();
    let worker_id = Uuid::new_v4();

    let mut old = FakeWorker::connect(&addr, worker_id, "roaming-phone").await;
    let task_id = master.submit_task(cmd_task("roam")).await.unwrap();
    assert_eq!(old.expect_assign().await.id, task_id);
    old.running(task_id).await;
    assert!(
        poll_until(SHORT, || async {
            state_of(&master, task_id).await == Some(TaskState::Running)
        })
        .await
    );

    // Wi-Fi -> LTE: the phone reconnects before the master has seen any EOF.
    let mut new = FakeWorker::connect(&addr, worker_id, "roaming-phone").await;

    // The task lost its channel with the old session: it is re-dispatched to the new one...
    let redelivered = new.expect_assign().await;
    assert_eq!(
        redelivered.id, task_id,
        "the in-flight task must be failed over"
    );
    // ...and the superseded connection is torn down by the master.
    assert!(
        old.closed_by_master(SHORT).await,
        "the old session must be closed, not left half-alive"
    );

    new.running(task_id).await;
    new.finish(task_id, 0, "done on the new session").await;
    let result = master
        .wait_task(task_id, Some(SHORT))
        .await
        .expect("wait_task must be woken instead of hanging forever");
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.stdout_str(), "done on the new session");
    let info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(info.state, TaskState::Completed);
    assert_eq!(
        info.retry_count, 1,
        "failed over exactly once, not zero and not twice"
    );
    assert!(master
        .list_workers()
        .await
        .unwrap()
        .iter()
        .all(|w| w.status != WorkerStatus::Disconnected));

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_the_register_ack_announces_which_optional_messages_the_master_reads() {
    // Workers send WorkerMessage::MobileTelemetry only to a master that announces it; a master
    // that does not would drop the worker for the unknown variant.
    let master = spawn_master().await;
    let stream = TcpStream::connect(master.server_addr().to_string())
        .await
        .unwrap();
    let mut transport = MessageTransport::new(stream);
    transport
        .send_msg(&WorkerMessage::Register {
            worker_id: Uuid::new_v4(),
            capabilities: WorkerCapabilities::new("announce", 2, 4096, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    match transport.recv_msg::<MasterMessage>().await.unwrap() {
        Some(MasterMessage::RegisterAck {
            accepted: true,
            message,
            ..
        }) => assert!(
            master_features::announced(message.as_deref(), master_features::MOBILE_TELEMETRY),
            "{message:?}"
        ),
        other => panic!("expected an accepted RegisterAck, got {other:?}"),
    }
    let _ = master.shutdown();
}

/// Opens a connection and sends `Register` without waiting for the ack, so a test can stack
/// registrations of one worker id faster than the master processes them.
async fn register_without_waiting(
    addr: &str,
    worker_id: Uuid,
    name: &str,
) -> MessageTransport<TcpStream> {
    let stream = TcpStream::connect(addr).await.expect("connect to master");
    let mut transport = MessageTransport::new(stream);
    transport
        .send_msg(&WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities::new(name, 4, 8192, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .expect("send Register");
    transport
}

// Multi-threaded on purpose: on a current-thread runtime a queue listing never overlaps a sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_a_third_registration_of_a_worker_id_leaves_no_task_on_a_dead_session() {
    // Session 2's sweep of session 1 runs in session 2's handler, which the next registration of
    // the same worker id aborts. The tasks must not stay assigned to session 1 forever.
    let master = spawn_master().await;
    let addr = master.server_addr().to_string();

    // A backlog that makes every full listing of the queue slow, so the sweeps of the next
    // registrations really wait for the queue lock while the following registration arrives.
    for i in 0..60_000 {
        master
            .submit_task(cmd_task(&format!("backlog-{i}")))
            .await
            .unwrap();
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let listers: Vec<_> = (0..2)
        .map(|_| {
            let (master, stop) = (master.clone(), stop.clone());
            tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = master.list_tasks().await;
                    tokio::task::yield_now().await;
                }
            })
        })
        .collect();

    // The timing window is narrow and random, so try several gaps between the registrations.
    let mut live_sessions = Vec::new();
    for gap_us in [0u64, 300, 1000, 2000, 3000, 5000, 8000, 12000] {
        let worker_id = Uuid::new_v4();
        let mut first = FakeWorker::connect(&addr, worker_id, "flapping-phone").await;
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(first.expect_assign().await.id);
        }

        let _second = register_without_waiting(&addr, worker_id, "flapping-phone").await;
        tokio::time::sleep(Duration::from_micros(gap_us)).await;
        let mut third = FakeWorker::connect(&addr, worker_id, "flapping-phone").await;

        // The held tasks are the oldest in the queue, so the live session gets exactly them again.
        let mut redelivered = Vec::new();
        for _ in 0..4 {
            redelivered.push(
                third
                    .next_message(SHORT)
                    .await
                    .and_then(|msg| match msg {
                        MasterMessage::AssignTask { task } => Some(task.id),
                        _ => None,
                    })
                    .unwrap_or_else(|| {
                        panic!("gap {gap_us}us: a held task is stranded on a dead session")
                    }),
            );
        }
        redelivered.sort_by_key(|id| id.0);
        held.sort_by_key(|id| id.0);
        assert_eq!(redelivered, held, "gap {gap_us}us");
        live_sessions.push(third);
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for lister in listers {
        let _ = lister.await;
    }
    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Sender identity: only the assigned worker can report on a task
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_results_progress_and_checkpoints_from_a_non_owner_are_ignored() {
    let master = spawn_master().await;
    let addr = master.server_addr().to_string();

    let mut owner = FakeWorker::connect(&addr, Uuid::new_v4(), "owner").await;
    let task_id = master.submit_task(cmd_task("owned")).await.unwrap();
    assert_eq!(owner.expect_assign().await.id, task_id);
    owner.running(task_id).await;

    let mut intruder = FakeWorker::connect(&addr, Uuid::new_v4(), "intruder").await;
    // Everything below names the OWNER in the message body; only the connection counts.
    let spoof = owner.worker_id;
    intruder
        .send(WorkerMessage::Checkpoint {
            task_id,
            sequence: u64::MAX,
            delta_state: TaskBytes::copy_from_slice(b"poisoned"),
        })
        .await;
    intruder
        .send(WorkerMessage::TaskProgress {
            worker_id: spoof,
            task_id,
            status: TaskStatus::Running,
        })
        .await;
    intruder
        .send(result_msg(spoof, task_id, 0, b"forged output"))
        .await;
    intruder
        .send(result_msg(spoof, task_id, 1, b"forged failure"))
        .await;
    intruder.sync().await;

    let info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(
        info.state,
        TaskState::Running,
        "forged results must not finish the task"
    );
    assert_eq!(info.assigned_worker_id, Some(owner.worker_id));
    assert!(
        master.get_checkpoint(&task_id).await.is_none(),
        "a foreign checkpoint must not poison the resume state"
    );

    owner.finish(task_id, 0, "genuine output").await;
    let result = master.wait_task(task_id, Some(SHORT)).await.unwrap();
    assert_eq!(result.stdout_str(), "genuine output");
    assert_eq!(result.worker_id, owner.worker_id);

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_stale_result_from_a_reaped_worker_cannot_finish_a_requeued_task() {
    // Worker A is reaped (silent), its task is failed over to B; A then wakes up and reports.
    let master = spawn_master_with_reaper(ReaperConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(400),
    ))
    .await;
    let addr = master.server_addr().to_string();

    let mut zombie = FakeWorker::connect(&addr, Uuid::new_v4(), "zombie").await;
    let task_id = master.submit_task(cmd_task("failover")).await.unwrap();
    assert_eq!(zombie.expect_assign().await.id, task_id);
    zombie.running(task_id).await;

    // Reaped after ~400 ms of silence; the task is requeued (with backoff) and A's socket is closed.
    assert!(
        zombie.closed_by_master(SHORT).await,
        "reaping must close the connection"
    );
    let mut healthy = FakeWorker::connect(&addr, Uuid::new_v4(), "healthy")
        .await
        .keeping_alive();
    assert_eq!(healthy.expect_assign().await.id, task_id);
    healthy.running(task_id).await;

    // The zombie reconnects under a fresh connection and still believes it owns the task.
    let mut zombie_again = FakeWorker::connect(&addr, zombie.worker_id, "zombie").await;
    zombie_again.finish(task_id, 1, "zombie says failed").await;
    zombie_again.sync().await;
    assert_eq!(
        master.get_task_info(task_id).await.unwrap().state,
        TaskState::Running,
        "a stale failure must not touch the FSM of a task that now runs elsewhere"
    );

    healthy.finish(task_id, 0, "the real result").await;
    let result = master.wait_task(task_id, Some(SHORT)).await.unwrap();
    assert_eq!(result.stdout_str(), "the real result");
    assert_eq!(result.worker_id, healthy.worker_id);

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Client cancel
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_client_cancel_reaches_the_worker_and_wakes_waiting_clients() {
    let master = spawn_master().await;
    let addr = master.server_addr().to_string();
    let mut worker = FakeWorker::connect(&addr, Uuid::new_v4(), "gpu-box").await;

    // A client blocks on a long task (wait = true).
    let task = cmd_task("long");
    let task_id = task.id;
    let mut waiting = Client::connect(&addr).await;
    waiting
        .transport
        .send_msg(&ClientMessage::SubmitTask { task, wait: true })
        .await
        .unwrap();
    assert_eq!(worker.expect_assign().await.id, task_id);
    worker.running(task_id).await;

    // Another client cancels it.
    let mut operator = Client::connect(&addr).await;
    match operator.ask(ClientMessage::CancelTask { task_id }).await {
        ClientResponse::TaskCancelled {
            task_id: id,
            success,
        } => {
            assert_eq!(id, task_id);
            assert!(success);
        }
        other => panic!("unexpected response {other:?}"),
    }
    // The worker is told to stop...
    match worker.next_message(SHORT).await {
        Some(MasterMessage::CancelTask { task_id: id, .. }) => assert_eq!(id, task_id),
        other => panic!("the executing worker never got a CancelTask: {other:?}"),
    }
    // ...and the waiting client is released with a cancelled result.
    match waiting.read().await {
        ClientResponse::TaskCompleted {
            task_id: id,
            result,
        } => {
            assert_eq!(id, task_id);
            assert_eq!(result.exit_code, 130);
        }
        other => panic!("waiter got {other:?}"),
    }
    assert_eq!(
        master.get_task_info(task_id).await.unwrap().state,
        TaskState::Cancelled
    );

    // The worker's own late cancellation report is harmless and frees its slot.
    worker
        .send(WorkerMessage::TaskResult {
            worker_id: worker.worker_id,
            task_id,
            exit_code: 130,
            stdout: TaskBytes::new(),
            stderr: TaskBytes::new(),
            execution_time_ms: 1,
            is_gpu_executed: false,
            device_name: None,
            error: Some("Cancelled: by master".into()),
        })
        .await;
    worker.sync().await;
    assert_eq!(
        master.get_task_info(task_id).await.unwrap().state,
        TaskState::Cancelled
    );

    // Cancelling again, or an unknown task, reports failure instead of pretending.
    for id in [task_id, TaskId::new()] {
        match operator
            .ask(ClientMessage::CancelTask { task_id: id })
            .await
        {
            ClientResponse::TaskCancelled { success, .. } => assert!(!success),
            other => panic!("unexpected response {other:?}"),
        }
    }

    // A task cancelled while still queued wakes its waiter too (nothing was dispatched anywhere).
    let queued = gpu_task();
    let queued_id = queued.id;
    let mut queued_waiter = Client::connect(&addr).await;
    queued_waiter
        .transport
        .send_msg(&ClientMessage::SubmitTask {
            task: queued,
            wait: true,
        })
        .await
        .unwrap();
    assert!(
        poll_until(SHORT, || async {
            master.get_task_info(queued_id).await.is_ok()
        })
        .await
    );
    match operator
        .ask(ClientMessage::CancelTask { task_id: queued_id })
        .await
    {
        ClientResponse::TaskCancelled { success, .. } => assert!(success),
        other => panic!("unexpected response {other:?}"),
    }
    match queued_waiter.read().await {
        ClientResponse::TaskCompleted { result, .. } => assert_eq!(result.exit_code, 130),
        other => panic!("queued waiter got {other:?}"),
    }

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// SubmitTask errors, duplicate ids, waiter leaks
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_submit_errors_are_answered_and_duplicate_ids_never_clobber_a_live_task() {
    let registry = WorkerRegistry::new();
    let queue = TaskQueue::new();
    let notify = Arc::new(tokio::sync::Notify::new());
    let waiters: rusty_grid_master::server::WaiterMap = Arc::new(RwLock::new(HashMap::new()));
    let server = MasterServer::bind_full(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        registry,
        queue.clone(),
        notify,
        waiters.clone(),
    )
    .await
    .unwrap();
    let addr = server.local_addr().to_string();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = server.run(shutdown_rx).await;
    });

    let mut client = Client::connect(&addr).await;

    // Validation failure: an answer, not a dropped connection, and no waiter left behind.
    let invalid = Task::new(
        TaskSpec::command("echo", vec!["x".into()]),
        TaskRequirements::generic(1, 0),
    );
    match client
        .ask(ClientMessage::SubmitTask {
            task: invalid,
            wait: true,
        })
        .await
    {
        ClientResponse::Error { message } => {
            assert!(message.to_lowercase().contains("timeout"), "{message}")
        }
        other => panic!("expected an Error response, got {other:?}"),
    }
    assert!(
        waiters.read().await.is_empty(),
        "a rejected submit must not leak its waiter"
    );
    // The same connection keeps working.
    match client.ask(ClientMessage::ClusterStatus).await {
        ClientResponse::ClusterStatus { total_tasks, .. } => assert_eq!(total_tasks, 0),
        other => panic!("unexpected response {other:?}"),
    }

    // A fresh task is accepted; resubmitting the same task id is refused and changes nothing.
    let task = cmd_task("original");
    let task_id = task.id;
    match client
        .ask(ClientMessage::SubmitTask {
            task: task.clone(),
            wait: false,
        })
        .await
    {
        ClientResponse::TaskSubmitted { task_id: id } => assert_eq!(id, task_id),
        other => panic!("unexpected response {other:?}"),
    }
    match client
        .ask(ClientMessage::SubmitTask { task, wait: true })
        .await
    {
        ClientResponse::Error { message } => {
            assert!(message.contains("already exists"), "{message}")
        }
        other => panic!("expected the duplicate to be refused, got {other:?}"),
    }
    assert!(
        waiters.read().await.get(&task_id).is_none(),
        "the refused duplicate must not stay registered as a waiter for the live task"
    );
    let stats = queue.stats().await;
    assert_eq!((stats.total, stats.queued), (1, 1));
    assert_eq!(queue.get_schedulable_tasks().await.len(), 1);

    let _ = shutdown_tx.send(true);
}

// ---------------------------------------------------------------------------------------------
// GetTaskResult
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_get_task_result_end_to_end_with_a_real_worker_in_both_codecs() {
    let master = spawn_master().await;
    let addr = master.server_addr().to_string();
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
    let mut worker =
        WorkerClient::from_options(addr.clone(), Some("real-worker".into()), Some(2), false);
    tokio::spawn(async move {
        let _ = worker.run(worker_shutdown_rx).await;
    });
    assert!(
        poll_until(SHORT, || async {
            master.list_workers().await.map(|w| w.len()).ok() == Some(1)
        })
        .await
    );

    for codec in [WireCodec::Bincode, WireCodec::Json] {
        let mut client = Client::connect_with_codec(&addr, codec).await;

        // Unknown id
        match client
            .ask(ClientMessage::GetTaskResult {
                task_id: TaskId::new(),
            })
            .await
        {
            ClientResponse::Error { message } => {
                assert!(message.contains("not found"), "{message}")
            }
            other => panic!("unknown task must be an Error, got {other:?}"),
        }

        // In flight: a status, not a result
        let task = Task::new(
            TaskSpec::BuiltinTest {
                test_name: "slowish".into(),
                iterations: 10,
                duration_ms: 600,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 30),
        );
        let task_id = task.id;
        match client
            .ask(ClientMessage::SubmitTask { task, wait: false })
            .await
        {
            ClientResponse::TaskSubmitted { task_id: id } => assert_eq!(id, task_id),
            other => panic!("unexpected response {other:?}"),
        }
        match client.ask(ClientMessage::GetTaskResult { task_id }).await {
            ClientResponse::TaskStatusInfo {
                task_id: id,
                status,
                ..
            } => {
                assert_eq!(id, task_id);
                assert!(
                    matches!(
                        status,
                        TaskStatus::Queued | TaskStatus::Assigned | TaskStatus::Running
                    ),
                    "{status:?}"
                );
            }
            other => panic!("an unfinished task must report its status, got {other:?}"),
        }

        // Terminal: the retained result, repeatably
        let deadline = Instant::now() + Duration::from_secs(15);
        let result = loop {
            match client.ask(ClientMessage::GetTaskResult { task_id }).await {
                ClientResponse::TaskCompleted {
                    task_id: id,
                    result,
                } => {
                    assert_eq!(id, task_id);
                    break result;
                }
                ClientResponse::TaskStatusInfo { .. } => {
                    assert!(Instant::now() < deadline, "task never finished");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                other => panic!("unexpected response {other:?}"),
            }
        };
        assert_eq!(result.exit_code, 0, "{result:?}");
        assert_eq!(result.task_id, task_id);
        match client.ask(ClientMessage::GetTaskResult { task_id }).await {
            ClientResponse::TaskCompleted { result: again, .. } => assert_eq!(again, result),
            other => panic!("the result must stay retrievable, got {other:?}"),
        }

        // A failed task is terminal too: its (failure) result is returned, not a status.
        let failing = Task::new(
            TaskSpec::command("sh", vec!["-c".into(), "echo oops >&2; exit 3".into()]),
            TaskRequirements::generic(1, 30),
        );
        let failing_id = failing.id;
        client
            .ask(ClientMessage::SubmitTask {
                task: failing,
                wait: false,
            })
            .await;
        let failed = poll_until(Duration::from_secs(15), || async {
            state_of(&master, failing_id).await == Some(TaskState::Failed)
        })
        .await;
        assert!(failed);
        match client
            .ask(ClientMessage::GetTaskResult {
                task_id: failing_id,
            })
            .await
        {
            ClientResponse::TaskCompleted { result, .. } => {
                assert_eq!(result.exit_code, 3);
                assert!(result.stderr_str().contains("oops"));
            }
            other => panic!("unexpected response {other:?}"),
        }
    }

    let _ = worker_shutdown_tx.send(true);
    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// ClusterStatus counters
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_cluster_status_counts_scheduled_cancelled_and_every_other_state() {
    let master = spawn_master().await;
    let addr = master.server_addr().to_string();
    let mut worker = FakeWorker::connect(&addr, Uuid::new_v4(), "counter").await;

    // completed
    let done = master.submit_task(cmd_task("done")).await.unwrap();
    assert_eq!(worker.expect_assign().await.id, done);
    worker.running(done).await;
    worker.finish(done, 0, "ok").await;
    // failed
    let failed = master.submit_task(cmd_task("failed")).await.unwrap();
    assert_eq!(worker.expect_assign().await.id, failed);
    worker.finish(failed, 2, "").await;
    // running
    let running = master.submit_task(cmd_task("running")).await.unwrap();
    assert_eq!(worker.expect_assign().await.id, running);
    worker.running(running).await;
    // scheduled: assigned, no progress report yet
    let scheduled = master.submit_task(cmd_task("scheduled")).await.unwrap();
    assert_eq!(worker.expect_assign().await.id, scheduled);
    // cancelled + queued: GPU tasks nobody can run
    let cancelled = master.submit_task(gpu_task()).await.unwrap();
    let _queued = master.submit_task(gpu_task()).await.unwrap();
    master.cancel_task(cancelled).await.unwrap();
    worker.sync().await;
    assert!(
        poll_until(SHORT, || async {
            state_of(&master, done).await == Some(TaskState::Completed)
                && state_of(&master, failed).await == Some(TaskState::Failed)
        })
        .await
    );

    let mut client = Client::connect(&addr).await;
    match client.ask(ClientMessage::ClusterStatus).await {
        ClientResponse::ClusterStatus {
            total_tasks,
            pending_tasks,
            running_tasks,
            completed_tasks,
            failed_tasks,
            ..
        } => {
            assert_eq!(total_tasks, 6);
            assert_eq!(pending_tasks, 1, "the unrunnable GPU task");
            assert_eq!(running_tasks, 2, "one running + one merely scheduled");
            assert_eq!(completed_tasks, 1);
            assert_eq!(failed_tasks, 2, "one failed + one cancelled");
            assert_eq!(
                pending_tasks + running_tasks + completed_tasks + failed_tasks,
                total_tasks,
                "every task is in exactly one bucket"
            );
        }
        other => panic!("unexpected response {other:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_cluster_status_counts_a_task_that_is_waiting_out_a_failover_backoff() {
    let master = spawn_master().await;
    let addr = master.server_addr().to_string();

    // The only worker takes a task and then vanishes without a goodbye; the task is failed over
    // with a backoff (state Retrying) and nobody is left to run it.
    let mut doomed = FakeWorker::connect(&addr, Uuid::new_v4(), "vanishing").await;
    let task_id = master.submit_task(cmd_task("orphan")).await.unwrap();
    assert_eq!(doomed.expect_assign().await.id, task_id);
    doomed.running(task_id).await;
    drop(doomed);

    let mut retrying = false;
    let started = Instant::now();
    while started.elapsed() < SHORT {
        if state_of(&master, task_id).await == Some(TaskState::Retrying) {
            retrying = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        retrying,
        "the orphaned task never entered its failover backoff"
    );

    let mut client = Client::connect(&addr).await;
    match client.ask(ClientMessage::ClusterStatus).await {
        ClientResponse::ClusterStatus {
            total_tasks,
            pending_tasks,
            running_tasks,
            completed_tasks,
            failed_tasks,
            ..
        } => {
            assert_eq!(total_tasks, 1);
            assert_eq!(
                (pending_tasks, running_tasks, completed_tasks, failed_tasks),
                (1, 0, 0, 0),
                "a task waiting out its failover backoff is pending work, not missing"
            );
        }
        other => panic!("unexpected response {other:?}"),
    }

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Liveness: a long upload is not a dead worker
// ---------------------------------------------------------------------------------------------

/// The exact bytes of a framed `TaskResult` carrying `payload_len` bytes of stdout.
fn framed_result(worker_id: Uuid, task_id: TaskId, payload_len: usize) -> Vec<u8> {
    let msg = result_msg(worker_id, task_id, 0, &vec![b'x'; payload_len]);
    let payload = serialize_message(&msg, WireCodec::Bincode).expect("serialize");
    let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&payload);
    frame
}

#[tokio::test]
async fn test_a_result_upload_longer_than_the_reaper_timeout_does_not_get_the_worker_reaped() {
    // Reap after 400 ms without a heartbeat; the upload below takes ~2 s and carries no heartbeat.
    let master = spawn_master_with_reaper(ReaperConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(400),
    ))
    .await;
    let addr = master.server_addr().to_string();
    let mut worker = FakeWorker::connect(&addr, Uuid::new_v4(), "slow-uplink").await;
    let task_id = master.submit_task(cmd_task("big-output")).await.unwrap();
    assert_eq!(worker.expect_assign().await.id, task_id);
    worker.running(task_id).await;

    let frame = framed_result(worker.worker_id, task_id, 1_000_000);
    let chunks = 40;
    let chunk_len = frame.len().div_ceil(chunks);
    let started = Instant::now();
    let stream = worker.transport.get_mut();
    for chunk in frame.chunks(chunk_len) {
        stream
            .write_all(chunk)
            .await
            .expect("master closed the connection mid-upload");
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let info = master.list_workers().await.unwrap();
        assert_ne!(
            info[0].status,
            WorkerStatus::Disconnected,
            "worker reaped {:?} into its upload although bytes kept arriving",
            started.elapsed()
        );
    }
    assert!(
        started.elapsed() > Duration::from_millis(1_500),
        "the upload must outlast several reaper timeouts to prove anything"
    );

    let result = master
        .wait_task(task_id, Some(SHORT))
        .await
        .expect("the uploaded result must arrive");
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.stdout.len(), 1_000_000);
    assert_eq!(
        master.get_task_info(task_id).await.unwrap().retry_count,
        0,
        "never failed over"
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_a_completely_silent_worker_is_still_reaped_and_its_task_failed_over() {
    let master = spawn_master_with_reaper(ReaperConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(400),
    ))
    .await;
    let addr = master.server_addr().to_string();
    let mut silent = FakeWorker::connect(&addr, Uuid::new_v4(), "dead-peer").await;
    let task_id = master.submit_task(cmd_task("orphan")).await.unwrap();
    assert_eq!(silent.expect_assign().await.id, task_id);
    silent.running(task_id).await;

    assert!(
        poll_until(Duration::from_secs(5), || async {
            master
                .list_workers()
                .await
                .map(|w| w[0].status == WorkerStatus::Disconnected)
                .unwrap_or(false)
        })
        .await,
        "dead-peer detection must not be weakened"
    );
    assert!(
        poll_until(Duration::from_secs(5), || async {
            matches!(
                master.get_task_info(task_id).await.map(|i| i.state),
                Ok(TaskState::Retrying | TaskState::Queued)
            )
        })
        .await
    );

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Black-hole worker
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_a_worker_that_rejects_tasks_instantly_does_not_swallow_the_work() {
    let master = spawn_master().await;
    let addr = master.server_addr().to_string();
    // The rejecting worker has more cores, so least-loaded placement prefers it every time.
    let mut phone =
        FakeWorker::connect_with_cores(&addr, Uuid::new_v4(), "low-battery-phone", 16).await;
    let mut pc = FakeWorker::connect_with_cores(&addr, Uuid::new_v4(), "healthy-pc", 2).await;

    // A client is blocked on the task the whole time: the refusal must not be mistaken for its outcome.
    let task = cmd_task("needs-a-home");
    let task_id = task.id;
    let mut waiting = Client::connect(&addr).await;
    waiting
        .transport
        .send_msg(&ClientMessage::SubmitTask { task, wait: true })
        .await
        .unwrap();
    let assigned = phone.expect_assign().await;
    assert_eq!(assigned.id, task_id);
    phone
        .send(WorkerMessage::TaskResult {
            worker_id: phone.worker_id,
            task_id,
            exit_code: 1,
            stdout: TaskBytes::new(),
            stderr: TaskBytes::copy_from_slice(
                b"Task rejected by worker: Critical battery preservation (<15% discharging)",
            ),
            execution_time_ms: 0,
            is_gpu_executed: false,
            device_name: None,
            error: Some("Battery critically low (<15% discharging)".into()),
        })
        .await;

    // The task goes elsewhere instead of being failed for good...
    assert_eq!(pc.expect_assign().await.id, task_id);
    pc.running(task_id).await;
    pc.finish(task_id, 0, "ran on the pc").await;
    match waiting.read().await {
        ClientResponse::TaskCompleted {
            task_id: id,
            result,
        } => {
            assert_eq!(id, task_id);
            assert_eq!(
                result.exit_code, 0,
                "the waiter got the refusal instead of the real result"
            );
            assert_eq!(result.stdout_str(), "ran on the pc");
        }
        other => panic!("waiter got {other:?}"),
    }
    let info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(info.state, TaskState::Completed);
    assert_eq!(
        info.retry_count, 1,
        "a refusal is one re-dispatch, not a failed attempt"
    );

    // ...and the phone is benched, so it stops being the least-loaded sink for everything else.
    assert!(master
        .registry()
        .exclusion_remaining(&phone.worker_id)
        .await
        .is_some());
    for i in 0..3 {
        let id = master
            .submit_task(cmd_task(&format!("later-{i}")))
            .await
            .unwrap();
        assert_eq!(pc.expect_assign().await.id, id);
        pc.finish(id, 0, "ok").await;
    }
    assert!(
        phone
            .next_message(Duration::from_millis(300))
            .await
            .is_none(),
        "a cooling-down worker must not be assigned anything"
    );

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// QUIC: reaping and superseding really disconnect
// ---------------------------------------------------------------------------------------------

#[cfg(feature = "p2p")]
mod quic {
    use super::*;
    use rusty_grid_core::transport::{
        iroh, parse_p2p_ticket, BiStream, GRID_ALPN, STREAM_CONTROL, STREAM_DATA,
    };

    type ControlLane =
        MessageTransport<BiStream<iroh::endpoint::RecvStream, iroh::endpoint::SendStream>>;

    struct Lanes {
        control: ControlLane,
        _keep_alive: (
            iroh::endpoint::Connection,
            iroh::endpoint::SendStream,
            iroh::endpoint::RecvStream,
        ),
    }

    async fn open_lanes(client: &iroh::Endpoint, master_addr: iroh::EndpointAddr) -> Lanes {
        let conn = client
            .connect(master_addr, GRID_ALPN)
            .await
            .expect("connect to master over QUIC");
        let (mut ctrl_send, ctrl_recv) = conn.open_bi().await.expect("control stream");
        ctrl_send.write_all(&[STREAM_CONTROL]).await.unwrap();
        let (mut data_send, data_recv) = conn.open_bi().await.expect("data stream");
        data_send.write_all(&[STREAM_DATA]).await.unwrap();
        Lanes {
            control: MessageTransport::new(BiStream::new(ctrl_recv, ctrl_send)),
            _keep_alive: (conn, data_send, data_recv),
        }
    }

    async fn register(lanes: &mut Lanes, worker_id: Uuid, name: &str) {
        lanes
            .control
            .send_msg(&WorkerMessage::Register {
                worker_id,
                capabilities: WorkerCapabilities::new(name, 2, 4096, false, false, None),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        match lanes.control.recv_msg::<MasterMessage>().await.unwrap() {
            Some(MasterMessage::RegisterAck {
                accepted: true,
                message,
                ..
            }) => assert!(
                master_features::announced(message.as_deref(), master_features::MOBILE_TELEMETRY),
                "the QUIC handler must announce the features too: {message:?}"
            ),
            other => panic!("expected an accepted RegisterAck, got {other:?}"),
        }
    }

    /// True once the master has closed the QUIC connection (the control stream ends or errors).
    async fn closed_by_master(lanes: &mut Lanes, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        loop {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            match tokio::time::timeout(left, lanes.control.recv_msg::<MasterMessage>()).await {
                Ok(Ok(Some(_))) => continue,
                Ok(_) => return true,
                Err(_) => return false,
            }
        }
    }

    async fn spawn_p2p_master(
        reaper: ReaperConfig,
    ) -> (MasterHandle, iroh::EndpointAddr, iroh::Endpoint) {
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_p2p(true)
            .with_ephemeral_key(true);
        let master = MasterServer::spawn_with_config(config, SchedulerConfig::default(), reaper)
            .await
            .expect("spawn p2p master");
        let ticket = master.p2p_ticket().expect("p2p ticket").to_string();
        let mut master_addr = parse_p2p_ticket(&ticket).expect("parse ticket");
        master_addr.addrs.retain(|a| !a.is_relay());
        let client = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
            .relay_mode(iroh::endpoint::RelayMode::Disabled)
            .alpns(vec![GRID_ALPN.to_vec()])
            .bind()
            .await
            .expect("bind client endpoint");
        (master, master_addr, client)
    }

    #[tokio::test]
    async fn test_reaping_a_quic_worker_really_closes_its_connection_and_it_can_come_back() {
        let (master, master_addr, client) = spawn_p2p_master(ReaperConfig::new(
            Duration::from_millis(100),
            Duration::from_millis(700),
        ))
        .await;
        let worker_id = Uuid::new_v4();

        let mut lanes = open_lanes(&client, master_addr.clone()).await;
        register(&mut lanes, worker_id, "quic-phone").await;
        assert!(master
            .p2p_connection_map()
            .read()
            .await
            .contains_key(&worker_id));

        // Silent for longer than the reaper timeout: the master must hang up, not just mark it.
        assert!(
            closed_by_master(&mut lanes, Duration::from_secs(10)).await,
            "a reaped QUIC worker kept its connection and would never reconnect"
        );
        assert!(
            poll_until(SHORT, || async {
                !master
                    .p2p_connection_map()
                    .read()
                    .await
                    .contains_key(&worker_id)
            })
            .await,
            "the closed connection must be unlisted"
        );
        assert_eq!(
            master.list_workers().await.unwrap()[0].status,
            WorkerStatus::Disconnected
        );

        // It reconnects and is schedulable again.
        let mut again = open_lanes(&client, master_addr).await;
        register(&mut again, worker_id, "quic-phone").await;
        assert_eq!(
            master.list_workers().await.unwrap()[0].status,
            WorkerStatus::Connected
        );
        assert!(master
            .p2p_connection_map()
            .read()
            .await
            .contains_key(&worker_id));

        let _ = master.shutdown();
    }

    #[tokio::test]
    async fn test_a_quic_worker_registering_again_closes_its_previous_connection() {
        let (master, master_addr, client) = spawn_p2p_master(ReaperConfig::new(
            Duration::from_secs(1),
            Duration::from_secs(60),
        ))
        .await;
        let worker_id = Uuid::new_v4();

        let mut first = open_lanes(&client, master_addr.clone()).await;
        register(&mut first, worker_id, "quic-phone").await;
        let mut second = open_lanes(&client, master_addr).await;
        register(&mut second, worker_id, "quic-phone").await;

        assert!(
            closed_by_master(&mut first, Duration::from_secs(10)).await,
            "the superseded QUIC connection must be closed by the master"
        );
        // The live session is untouched and still the one listed.
        assert!(!closed_by_master(&mut second, Duration::from_millis(500)).await);
        assert!(master
            .p2p_connection_map()
            .read()
            .await
            .contains_key(&worker_id));
        assert_eq!(
            master.list_workers().await.unwrap()[0].status,
            WorkerStatus::Connected
        );

        let _ = master.shutdown();
    }
}
