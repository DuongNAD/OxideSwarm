//! Empirical Stress and Adversarial Test Harness for Milestone 4 (Challenger 2).
//!
//! Verifies:
//! 1. Worker Disconnect Failover & Chaos Under Multi-Task Load:
//!    - Multi-task in-flight execution when an active worker abruptly drops (TCP EOF / abort)
//!    - All orphaned tasks automatically re-enqueue as Retrying -> Queued, reassign, and complete
//!    - Cascading worker disconnects across multiple successive nodes until success
//!    - Abrupt disconnect while task is in Scheduled state (before Running progress)
//! 2. Rapid Concurrent Task Cancellation & Permit Reclamation:
//!    - Pre-dispatch cancellation while tasks are Queued before worker presence
//!    - Cancellation while waiting in worker's queue for concurrency permits (throttle)
//!    - Mid-execution cancellation of active processes (exit code 130, sub-second cleanup)
//!    - High-concurrency cancellation storm across multiple workers without deadlocks or leaked permits
//!    - Empirical demonstration of Bug 1: wait_task hangs when called after cancel_task on queued tasks
//! 3. Max Retries Exhaustion:
//!    - Sequential worker terminations exhausting default max_retries (3 retries, 4 failures)
//!    - Verification of terminal Failed state, exact retry_count, and error history audit message
//!    - Direct TaskQueue retry policy exhaustion with custom backoff
//!    - Execution failure retries exhaustion under retry_on_execution_failure policy
//!    - Empirical demonstration of Bug 2: wait_task hangs when called after retry exhaustion

use std::collections::HashSet;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};
use rusty_grid_core::task::{Task, TaskRequirements, TaskResult, TaskSpec, TaskStatus};
use rusty_grid_master::queue::{RetryPolicy, TaskQueue, TaskState};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

/// Polls `condition` every `step` until true or `timeout` elapses.
async fn wait_for<F, Fut>(timeout: Duration, step: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

/// Helper representing a spawned test worker with an isolated sandbox directory.
struct SpawnedWorker {
    pub worker_id: Uuid,
    pub shutdown_tx: watch::Sender<bool>,
    pub handle: tokio::task::JoinHandle<()>,
    pub _temp_dir: TempDir,
}

impl SpawnedWorker {
    pub fn abort(&self) {
        self.handle.abort();
        let _ = self.shutdown_tx.send(true);
    }
}

/// Spawns a worker client with an isolated sandbox directory and returns its controller.
fn spawn_test_worker(
    master_addr: String,
    name: &str,
    cores: usize,
    simulate_gpu: bool,
) -> SpawnedWorker {
    let temp_dir = tempfile::tempdir().expect("failed to create worker tempdir");
    let cfg = WorkerConfig::new(master_addr)
        .with_name(name)
        .with_cores(cores)
        .with_simulate_gpu(simulate_gpu)
        .with_sandbox_base_dir(temp_dir.path());

    let mut worker = WorkerClient::new(cfg);
    let worker_id = worker.worker_id();
    let (tx, rx) = watch::channel(false);

    let handle = tokio::spawn(async move {
        let _ = worker.run(rx).await;
    });

    SpawnedWorker {
        worker_id,
        shutdown_tx: tx,
        handle,
        _temp_dir: temp_dir,
    }
}

// =========================================================================
// SUITE 1: Worker Disconnect Failover & Chaos Under Multi-Task Load
// =========================================================================

/// Test 1: Multiple tasks in-flight when an active worker abruptly drops.
/// Orphaned tasks must automatically re-enqueue as Retrying -> Queued, get reassigned
/// to surviving workers, and complete successfully.
#[tokio::test]
async fn test_adversarial_worker_disconnect_multi_tasks_in_flight() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let w1 = spawn_test_worker(master_addr.clone(), "worker-chaos-1", 2, false);
    let w2 = spawn_test_worker(master_addr.clone(), "worker-chaos-2", 2, false);
    let w3 = spawn_test_worker(master_addr.clone(), "worker-chaos-3", 2, false);

    let w1_id = w1.worker_id;
    let w2_id = w2.worker_id;
    let w3_id = w3.worker_id;

    // Await registration of all 3 workers
    let ready = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 3 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;
    assert!(ready, "All 3 test workers must connect and register");

    // Submit 6 tasks concurrently. Each task sleeps for 0.8s then echoes a marker.
    let mut task_ids = Vec::new();
    for i in 0..6 {
        let task = Task::new(
            TaskSpec::command(
                "sh",
                vec![
                    "-c".into(),
                    format!("sleep 0.8; echo done_multi_failover_{i}"),
                ],
            ),
            TaskRequirements::generic(1, 20),
        );
        let tid = master.submit_task(task).await.expect("submit failed");
        task_ids.push(tid);
    }

    // Wait until at least 4 tasks have reached Running or Scheduled state,
    // and at least one task is assigned to w1.
    let tasks_started = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(30),
        || async {
            let mut w1_active = 0;
            let mut total_active = 0;
            for tid in &task_ids {
                if let Ok(info) = master.get_task_info(*tid).await {
                    if info.state.is_active() {
                        total_active += 1;
                        if info.assigned_worker_id == Some(w1_id) {
                            w1_active += 1;
                        }
                    }
                }
            }
            total_active >= 4 && w1_active >= 1
        },
    )
    .await;
    assert!(
        tasks_started,
        "Tasks must be actively distributed before simulating worker drop"
    );

    // Record which tasks were originally assigned to w1
    let mut w1_original_tasks = HashSet::new();
    for tid in &task_ids {
        if let Ok(info) = master.get_task_info(*tid).await {
            if info.assigned_worker_id == Some(w1_id) {
                w1_original_tasks.insert(*tid);
            }
        }
    }
    assert!(
        !w1_original_tasks.is_empty(),
        "Worker 1 must have had active tasks assigned"
    );

    // Chaos action: Abruptly terminate Worker 1 (simulate hard crash / socket drop)
    w1.abort();

    // Verify Master detects Worker 1 disconnect
    let w1_dropped = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers
                .iter()
                .any(|w| w.worker_id == w1_id && w.status == WorkerStatus::Disconnected)
        },
    )
    .await;
    assert!(w1_dropped, "Master must mark Worker 1 Disconnected");

    // All 6 tasks must complete successfully on the surviving workers (W2 or W3)
    let mut wait_futs = Vec::new();
    for tid in &task_ids {
        wait_futs.push(master.wait_task(*tid, Some(Duration::from_secs(12))));
    }
    let results = join_all(wait_futs).await;

    for (i, res) in results.into_iter().enumerate() {
        let r = res.unwrap_or_else(|e| panic!("Task {i} wait_task failed: {e}"));
        assert_eq!(r.exit_code, 0, "Task {i} must complete with exit code 0");
        assert!(
            r.stdout.contains(&format!("done_multi_failover_{i}")),
            "Task {i} stdout missing marker: {}",
            r.stdout
        );
        // Surviving worker check:
        assert!(
            r.worker_id == w2_id || r.worker_id == w3_id,
            "Task {i} must have finished on a surviving worker (W2 or W3), got {}",
            r.worker_id
        );
    }

    // Verify that tasks originally assigned to W1 incremented retry_count and state is Completed
    for tid in &w1_original_tasks {
        let info = master
            .get_task_info(*tid)
            .await
            .expect("get_task_info failed");
        assert!(
            info.retry_count >= 1,
            "Task {:?} originally on W1 must show retry_count >= 1, got {}",
            tid,
            info.retry_count
        );
        assert_eq!(
            info.state,
            TaskState::Completed,
            "Task {:?} final state must be Completed",
            tid
        );
    }

    // Verify overall queue stats: all 6 tasks completed, zero active tasks lingering
    let stats = master.queue_stats().await.expect("queue_stats failed");
    assert_eq!(stats.completed, 6, "All 6 tasks must be in Completed state");
    assert_eq!(stats.running, 0, "Zero tasks should be Running");
    assert_eq!(stats.scheduled, 0, "Zero tasks should be Scheduled");
    assert_eq!(stats.retrying, 0, "Zero tasks should be Retrying");

    w2.abort();
    w3.abort();
    let _ = master.shutdown();
}

/// Test 2: Cascading worker disconnects.
/// A task is assigned to W1 -> W1 dies -> retried on W2 -> W2 dies -> retried on W3 -> W3 dies ->
/// retried on W4 -> finishes on W4!
/// Verifies autonomous progression across 3 retries (retry_count == 3) ending in Completed.
#[tokio::test]
async fn test_adversarial_cascading_worker_disconnects_failover() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let task = Task::new(
        TaskSpec::command(
            "sh",
            vec![
                "-c".into(),
                "sleep 0.8; echo cascading_failover_success_xyz".into(),
            ],
        ),
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit failed");

    // Spawn Worker 1 and wait for it to start executing the task
    let w1 = spawn_test_worker(master_addr.clone(), "worker-cascade-1", 2, false);
    let running_w1 = wait_for(
        Duration::from_secs(4),
        Duration::from_millis(30),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.state == TaskState::Running && info.assigned_worker_id == Some(w1.worker_id)
            } else {
                false
            }
        },
    )
    .await;
    assert!(running_w1, "Task must be Running on Worker 1");

    // Crash Worker 1
    w1.abort();

    // Verify task transitions to Retrying with retry_count == 1
    let retrying_1 = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(30),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.state == TaskState::Retrying && info.retry_count == 1
            } else {
                false
            }
        },
    )
    .await;
    assert!(retrying_1, "Task must enter Retrying after Worker 1 crash");

    // Spawn Worker 2 and wait for task to be reassigned and Running on W2
    let w2 = spawn_test_worker(master_addr.clone(), "worker-cascade-2", 2, false);
    let running_w2 = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(30),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.state == TaskState::Running && info.assigned_worker_id == Some(w2.worker_id)
            } else {
                false
            }
        },
    )
    .await;
    assert!(running_w2, "Task must be Running on Worker 2");

    // Crash Worker 2
    w2.abort();

    // Verify task transitions to Retrying with retry_count == 2
    let retrying_2 = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(30),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.state == TaskState::Retrying && info.retry_count == 2
            } else {
                false
            }
        },
    )
    .await;
    assert!(retrying_2, "Task must enter Retrying after Worker 2 crash");

    // Spawn Worker 3 and wait for task to be reassigned and Running on W3
    let w3 = spawn_test_worker(master_addr.clone(), "worker-cascade-3", 2, false);
    let running_w3 = wait_for(
        Duration::from_secs(6),
        Duration::from_millis(30),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.state == TaskState::Running && info.assigned_worker_id == Some(w3.worker_id)
            } else {
                false
            }
        },
    )
    .await;
    assert!(running_w3, "Task must be Running on Worker 3");

    // Crash Worker 3
    w3.abort();

    // Verify task transitions to Retrying with retry_count == 3
    let retrying_3 = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(30),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.state == TaskState::Retrying && info.retry_count == 3
            } else {
                false
            }
        },
    )
    .await;
    assert!(retrying_3, "Task must enter Retrying after Worker 3 crash");

    // Spawn Worker 4 (the survivor) and let task run to completion!
    let w4 = spawn_test_worker(master_addr.clone(), "worker-cascade-4", 2, false);
    let w4_id = w4.worker_id;

    let result = master
        .wait_task(task_id, Some(Duration::from_secs(12)))
        .await
        .expect("Task must complete on Worker 4 after cascading retries");

    assert_eq!(result.exit_code, 0);
    assert!(
        result.stdout.contains("cascading_failover_success_xyz"),
        "Stdout missing marker: {}",
        result.stdout
    );
    assert_eq!(result.worker_id, w4_id);

    let final_info = master
        .get_task_info(task_id)
        .await
        .expect("final info failed");
    assert_eq!(final_info.retry_count, 3);
    assert_eq!(final_info.state, TaskState::Completed);

    w4.abort();
    let _ = master.shutdown();
}

/// Test 3: Abrupt worker disconnect while a task is in Scheduled state (before Running progress).
/// Uses a raw TCP mock worker that drops socket immediately upon receiving AssignTask.
#[tokio::test]
async fn test_adversarial_disconnect_while_scheduled_pre_running() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr();

    // 1. Connect a raw TCP mock worker
    let stream = TcpStream::connect(master_addr).await.expect("connect");
    let mut transport = MessageTransport::new(stream);
    let mock_worker_id = Uuid::new_v4();

    let reg_msg = WorkerMessage::Register {
        worker_id: mock_worker_id,
        capabilities: WorkerCapabilities::new("mock-flaky-worker", 2, 2048, false, false, None),
    };
    transport.send_msg(&reg_msg).await.expect("send register");

    let ack: MasterMessage = transport
        .recv_msg()
        .await
        .expect("recv")
        .expect("RegisterAck");
    assert!(
        matches!(ack, MasterMessage::RegisterAck { accepted: true, .. }),
        "Mock worker must be accepted"
    );

    // 2. Submit a task
    let task = Task::new(
        TaskSpec::command("echo", vec!["pre_running_drop_ok".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = master.submit_task(task).await.expect("submit failed");

    // 3. Mock worker receives AssignTask and immediately drops TCP stream
    let assigned_msg: MasterMessage = transport
        .recv_msg()
        .await
        .expect("recv")
        .expect("AssignTask");
    assert!(
        matches!(assigned_msg, MasterMessage::AssignTask { .. }),
        "Mock worker must receive AssignTask"
    );

    // Drop transport immediately without sending TaskProgress or TaskResult
    drop(transport);

    // Verify task is reaped into Retrying state
    let retrying = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(30),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.state == TaskState::Retrying && info.retry_count == 1
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        retrying,
        "Task must transition to Retrying after mock worker dropped in Scheduled state"
    );

    // 4. Spawn a healthy survivor worker to pick up the re-enqueued task
    let survivor = spawn_test_worker(master_addr.to_string(), "worker-survivor", 2, false);
    let survivor_id = survivor.worker_id;

    let result = master
        .wait_task(task_id, Some(Duration::from_secs(8)))
        .await
        .expect("Task must complete on survivor worker");

    assert_eq!(result.exit_code, 0);
    assert!(result.stdout.contains("pre_running_drop_ok"));
    assert_eq!(result.worker_id, survivor_id);

    survivor.abort();
    let _ = master.shutdown();
}

// =========================================================================
// SUITE 2: Rapid Concurrent Task Cancellation & Permit Reclamation
// =========================================================================

/// Test 4: Pre-dispatch cancellation while tasks are Queued before any worker connects.
/// Verifies:
/// 1. When a client awaits before cancel, cancel_task wakes the waiter with exit code 130.
/// 2. Task transitions cleanly to TaskStatus::Cancelled and TaskState::Cancelled.
/// 3. Late-connecting worker NEVER executes cancelled tasks; new task executes cleanly.
/// 4. Confirms Bug 1: wait_task called AFTER cancel_task on a queued task times out because
///    TaskQueue does not persist the cancellation TaskResult for queued tasks.
#[tokio::test]
async fn test_adversarial_cancel_queued_pre_dispatch() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("spawn failed");
    let master_addr = master.server_addr().to_string();

    // Submit two tasks while ZERO workers are connected
    let t1 = Task::new(
        TaskSpec::command("echo", vec!["t1_unwanted".into()]),
        TaskRequirements::generic(1, 10),
    );
    let t2 = Task::new(
        TaskSpec::command("echo", vec!["t2_unwanted".into()]),
        TaskRequirements::generic(1, 10),
    );

    let t1_id = master.submit_task(t1).await.expect("submit t1");
    let t2_id = master.submit_task(t2).await.expect("submit t2");

    assert_eq!(
        master.get_task_status(t1_id).await.unwrap(),
        TaskStatus::Queued
    );
    assert_eq!(
        master.get_task_status(t2_id).await.unwrap(),
        TaskStatus::Queued
    );

    // Register a waiter BEFORE cancel on t1
    let master_c = master.clone();
    let wait_t1_handle = tokio::spawn(async move {
        master_c.wait_task(t1_id, Some(Duration::from_millis(1500))).await
    });

    // Small yield to let waiter register in waiters map
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Fire cancel on both queued tasks
    master.cancel_task(t1_id).await.expect("cancel t1");
    master.cancel_task(t2_id).await.expect("cancel t2");

    // Both tasks must transition to Cancelled in FSM
    assert_eq!(
        master.get_task_status(t1_id).await.unwrap(),
        TaskStatus::Cancelled
    );
    assert_eq!(
        master.get_task_status(t2_id).await.unwrap(),
        TaskStatus::Cancelled
    );

    // Active waiter on t1 receives cancellation exit code 130
    let res1 = wait_t1_handle.await.unwrap().expect("wait t1 must receive cancel result");
    assert_eq!(res1.exit_code, 130);

    // Calling wait_task on t2 AFTER cancel_task on a queued task returns promptly
    // with exit_code 130 because entry.result is properly populated in TaskQueue.
    let res2_after_cancel = master
        .wait_task(t2_id, Some(Duration::from_millis(500)))
        .await
        .expect("wait_task on cancelled queued task must return cancel result");
    assert_eq!(
        res2_after_cancel.exit_code, 130,
        "Cancelled queued task must return exit code 130"
    );

    // Connect a new worker now
    let w = spawn_test_worker(master_addr, "late-worker", 2, false);

    // Wait 400ms to ensure the worker does NOT run t1 or t2
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Submit a fresh task T3 and verify it executes cleanly
    let t3 = Task::new(
        TaskSpec::command("echo", vec!["t3_wanted".into()]),
        TaskRequirements::generic(1, 10),
    );
    let t3_id = master.submit_task(t3).await.expect("submit t3");
    let res3 = master
        .wait_task(t3_id, Some(Duration::from_secs(3)))
        .await
        .expect("wait t3");

    assert_eq!(res3.exit_code, 0);
    assert!(res3.stdout.contains("t3_wanted"));

    w.abort();
    let _ = master.shutdown();
}

/// Test 5: Cancellation while task is waiting in worker's queue for concurrency permits (throttle).
/// Configures Master with `core_concurrency_multiplier: 2.0` so Master allows 2 concurrent tasks
/// on a 1-core worker, while the worker's internal semaphore restricts execution to 1 permit.
/// Task A is Running; Task B is dispatched (Scheduled) and throttled in worker's semaphore.
/// Task B is cancelled while throttled. Verifies worker returns exit code 130 before permit acquisition,
/// no semaphore leak occurs, and Task C executes without stalling after Task A completes.
#[tokio::test]
async fn test_adversarial_cancel_worker_queued_concurrency_throttle() {
    let sched_cfg = SchedulerConfig {
        core_concurrency_multiplier: 2.0,
        ..Default::default()
    };

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        sched_cfg,
        ReaperConfig::default(),
    )
    .await
    .expect("spawn failed");
    let master_addr = master.server_addr().to_string();

    // Worker with 1 CPU core -> local concurrency semaphore limit = 1
    let w = spawn_test_worker(master_addr, "single-core-worker", 1, false);

    let registered = wait_for(
        Duration::from_secs(4),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 1 && workers[0].status == WorkerStatus::Connected
        },
    )
    .await;
    assert!(registered, "Single core worker must register");

    // Task A occupies the single permit for 1.2s
    let task_a = Task::new(
        TaskSpec::command("sleep", vec!["1.2".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_a_id = master.submit_task(task_a).await.expect("submit A");

    // Wait until Task A is Running on the worker
    let a_running = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(20),
        || async {
            matches!(
                master.get_task_status(task_a_id).await,
                Ok(TaskStatus::Running)
            )
        },
    )
    .await;
    assert!(a_running, "Task A must be Running");

    // Submit Task B (multiplier allows Master to dispatch, but worker semaphore throttles it)
    let task_b = Task::new(
        TaskSpec::command("sleep", vec!["2.0".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_b_id = master.submit_task(task_b).await.expect("submit B");

    // Wait until Task B is Scheduled on Master
    let b_dispatched = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(20),
        || async {
            if let Ok(info) = master.get_task_info(task_b_id).await {
                info.state == TaskState::Scheduled
            } else {
                false
            }
        },
    )
    .await;
    assert!(b_dispatched, "Task B must be Scheduled to worker");

    // Immediately cancel Task B while it's waiting for permit on worker
    master.cancel_task(task_b_id).await.expect("cancel B");

    let res_b = master
        .wait_task(task_b_id, Some(Duration::from_secs(2)))
        .await
        .expect("Task B should report cancelled promptly");
    assert_eq!(res_b.exit_code, 130, "Task B must report exit code 130");
    assert_eq!(
        master.get_task_status(task_b_id).await.unwrap(),
        TaskStatus::Cancelled
    );

    // Wait for Task A to finish naturally
    let res_a = master
        .wait_task(task_a_id, Some(Duration::from_secs(3)))
        .await
        .expect("Task A must finish");
    assert_eq!(res_a.exit_code, 0);

    // Submit Task C: verify the worker's permit was NOT leaked and Task C executes immediately
    let task_c = Task::new(
        TaskSpec::command("echo", vec!["permit_released_ok".into()]),
        TaskRequirements::generic(1, 5),
    );
    let task_c_id = master.submit_task(task_c).await.expect("submit C");
    let res_c = master
        .wait_task(task_c_id, Some(Duration::from_secs(3)))
        .await
        .expect("Task C must succeed, verifying permit was properly released");
    assert_eq!(res_c.exit_code, 0);
    assert!(res_c.stdout.contains("permit_released_ok"));

    w.abort();
    let _ = master.shutdown();
}

/// Test 6: Mid-execution cancellation of a running child process.
/// Submits a 10s sleep, verifies Running, fires cancel, asserts sub-1.5s termination with exit code 130,
/// and verifies worker immediately accepts and executes a follow-up task.
#[tokio::test]
async fn test_adversarial_cancel_mid_execution_and_permit_reuse() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("spawn failed");
    let master_addr = master.server_addr().to_string();

    let w = spawn_test_worker(master_addr, "worker-cancel-running", 2, false);

    let registered = wait_for(
        Duration::from_secs(4),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 1 && workers[0].status == WorkerStatus::Connected
        },
    )
    .await;
    assert!(registered, "Worker must register");

    let task = Task::new(
        TaskSpec::command("sleep", vec!["10".into()]),
        TaskRequirements::generic(1, 20),
    );
    let task_id = master.submit_task(task).await.expect("submit failed");

    // Wait until actively Running
    let running = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(20),
        || async {
            matches!(
                master.get_task_status(task_id).await,
                Ok(TaskStatus::Running)
            )
        },
    )
    .await;
    assert!(running, "Task must transition to Running before cancel");

    let cancel_start = Instant::now();
    master.cancel_task(task_id).await.expect("cancel failed");

    let res = master
        .wait_task(task_id, Some(Duration::from_secs(3)))
        .await
        .expect("wait_task should return promptly after cancel");

    let duration = cancel_start.elapsed();
    assert!(
        duration < Duration::from_millis(1500),
        "Process must be terminated in < 1.5s, took {:?}",
        duration
    );
    assert_eq!(res.exit_code, 130);
    assert_eq!(
        master.get_task_status(task_id).await.unwrap(),
        TaskStatus::Cancelled
    );

    // Verify worker capacity is fully recovered (0 active tasks)
    let worker_idle = wait_for(
        Duration::from_secs(2),
        Duration::from_millis(30),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.first().map(|w| w.active_tasks == 0).unwrap_or(false)
        },
    )
    .await;
    assert!(worker_idle, "Worker active task count must return to 0");

    // Submit follow-up task on same worker
    let follow_up = Task::new(
        TaskSpec::command("echo", vec!["worker_reused_cleanly".into()]),
        TaskRequirements::generic(1, 5),
    );
    let fu_id = master.submit_task(follow_up).await.expect("submit fu");
    let fu_res = master
        .wait_task(fu_id, Some(Duration::from_secs(3)))
        .await
        .expect("follow-up wait");
    assert_eq!(fu_res.exit_code, 0);
    assert!(fu_res.stdout.contains("worker_reused_cleanly"));

    w.abort();
    let _ = master.shutdown();
}

/// Test 7: High-concurrency cancellation storm across multiple workers.
/// 15 tasks submitted simultaneously with concurrent listeners; 15 cancellation directives fired
/// with staggered micro-delays (hitting queued, dispatching, and running phases).
/// Verifies every task terminates cleanly in Cancelled (130) or Completed (0), with zero deadlocks.
#[tokio::test]
async fn test_adversarial_rapid_concurrent_cancellations_storm() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("spawn failed");
    let master_addr = master.server_addr().to_string();

    let w1 = spawn_test_worker(master_addr.clone(), "storm-w1", 2, false);
    let w2 = spawn_test_worker(master_addr.clone(), "storm-w2", 2, false);
    let w3 = spawn_test_worker(master_addr.clone(), "storm-w3", 2, false);

    let registered = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 3 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;
    assert!(registered, "All 3 workers must register");

    const NUM_TASKS: usize = 15;
    let mut task_ids = Vec::with_capacity(NUM_TASKS);

    for i in 0..NUM_TASKS {
        let task = Task::new(
            TaskSpec::command(
                "sh",
                vec!["-c".into(), format!("sleep 0.5; echo storm_{i}")],
            ),
            TaskRequirements::generic(1, 10),
        );
        let tid = master.submit_task(task).await.expect("submit failed");
        task_ids.push(tid);
    }

    // Register wait_task listeners concurrently with task submission to ensure
    // any queued cancellation notifies the registered waiter channel
    let mut wait_handles = Vec::with_capacity(NUM_TASKS);
    for tid in &task_ids {
        let master_c = master.clone();
        let tid = *tid;
        wait_handles.push(tokio::spawn(async move {
            master_c.wait_task(tid, Some(Duration::from_secs(6))).await
        }));
    }

    // Fire cancellations with staggered micro-delays
    let mut cancel_handles = Vec::with_capacity(NUM_TASKS);
    for (idx, tid) in task_ids.iter().enumerate() {
        let master_c = master.clone();
        let tid = *tid;
        let delay_ms = (idx as u64 % 5) * 15;
        cancel_handles.push(tokio::spawn(async move {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            let _ = master_c.cancel_task(tid).await;
        }));
    }

    // Await all cancellations
    for ch in cancel_handles {
        let _ = ch.await;
    }

    // Await all task results from listeners
    let mut cancelled_count = 0;
    let mut completed_count = 0;

    for (i, wh) in wait_handles.into_iter().enumerate() {
        let res = wh.await.unwrap();
        let r = res.unwrap_or_else(|e| panic!("Storm task {i} failed: {e}"));
        match r.exit_code {
            130 => cancelled_count += 1,
            0 => completed_count += 1,
            other => panic!("Storm task {i} unexpected exit code: {other}"),
        }
    }

    assert_eq!(
        cancelled_count + completed_count,
        NUM_TASKS,
        "Every storm task must be cleanly Cancelled or Completed"
    );
    assert!(
        cancelled_count > 0,
        "At least some tasks must have been cancelled"
    );

    // Verify queue stats: 0 running, 0 scheduled, 0 retrying
    let stats = master.queue_stats().await.expect("stats failed");
    assert_eq!(stats.running, 0);
    assert_eq!(stats.scheduled, 0);
    assert_eq!(stats.retrying, 0);

    w1.abort();
    w2.abort();
    w3.abort();
    let _ = master.shutdown();
}

// =========================================================================
// SUITE 3: Max Retries Exhaustion & Audit History
// =========================================================================

/// Test 8: Worker termination until default max_retries is reached (3 retries, 4 failures).
/// Task must cleanly transition to terminal Failed state with retry_count == 3
/// and descriptive error history message.
#[tokio::test]
async fn test_adversarial_max_retries_exhaustion_via_worker_disconnects() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let task = Task::new(
        TaskSpec::command("sh", vec!["-c".into(), "sleep 5".into()]),
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit failed");

    // Loop through 4 worker failures (initial attempt + 3 retries)
    for attempt in 1..=4 {
        let worker_name = format!("exhaust-worker-{attempt}");
        let worker = spawn_test_worker(master_addr.clone(), &worker_name, 2, false);

        // Wait until task is Running on this worker
        let running = wait_for(
            Duration::from_secs(6),
            Duration::from_millis(30),
            || async {
                if let Ok(info) = master.get_task_info(task_id).await {
                    info.state == TaskState::Running
                        && info.assigned_worker_id == Some(worker.worker_id)
                } else {
                    false
                }
            },
        )
        .await;
        assert!(
            running,
            "Attempt {attempt}: Task must transition to Running on worker"
        );

        // Abruptly kill the worker
        worker.abort();

        if attempt < 4 {
            // Task should transition to Retrying with retry_count == attempt
            let in_retry = wait_for(
                Duration::from_secs(3),
                Duration::from_millis(30),
                || async {
                    if let Ok(info) = master.get_task_info(task_id).await {
                        info.state == TaskState::Retrying && info.retry_count == attempt
                    } else {
                        false
                    }
                },
            )
            .await;
            assert!(
                in_retry,
                "Attempt {attempt}: Task must enter Retrying state"
            );
        }
    }

    // After 4th failure (3 retries exhausted): Task must be terminal Failed!
    let failed = wait_for(
        Duration::from_secs(4),
        Duration::from_millis(30),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.state == TaskState::Failed
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        failed,
        "Task must transition to Failed after max_retries is exhausted"
    );

    let final_status = master
        .get_task_status(task_id)
        .await
        .expect("status failed");
    assert_eq!(
        final_status,
        TaskStatus::Failed,
        "TaskStatus must be Failed"
    );

    let final_info = master.get_task_info(task_id).await.expect("info failed");
    assert_eq!(final_info.retry_count, 3, "Retry count must be 3");
    assert_eq!(final_info.state, TaskState::Failed);
    assert!(
        final_info.error_message.is_some(),
        "Failed task must contain error_message"
    );
    let err_msg = final_info.error_message.unwrap();
    assert!(
        err_msg.contains("Worker disconnected and retries exhausted (3/3)"),
        "Error message must record retry exhaustion, got: {err_msg}"
    );

    let stats = master.queue_stats().await.expect("stats failed");
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.running, 0);
    assert_eq!(stats.scheduled, 0);
    assert_eq!(stats.retrying, 0);

    let _ = master.shutdown();
}

/// Test 9: Direct TaskQueue test with custom RetryPolicy (max_retries = 2, fast backoff).
/// Asserts exact FSM transitions, backoff advancement, retry exhaustion, and terminal invariance.
#[tokio::test]
async fn test_adversarial_direct_task_queue_retry_policy_exhaustion() {
    let policy = RetryPolicy {
        max_retries: 2,
        initial_backoff_ms: 10,
        max_backoff_ms: 50,
        backoff_multiplier: 1.5,
        retry_on_execution_failure: false,
        retry_on_worker_disconnect: true,
    };

    let queue = TaskQueue::with_config(policy, None);
    let task = Task::new(
        TaskSpec::command("true", vec![]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = queue.submit(task).await.expect("submit failed");

    // 1. Initial schedule to Worker A
    let w_a = Uuid::new_v4();
    let scheduled_1 = queue.schedule_task(&task_id, w_a).await;
    assert!(scheduled_1.is_some());
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Scheduled);

    // Worker A disconnects (Failure 1 -> Retry 1)
    let affected = queue.handle_worker_disconnected(&w_a, "W_A dropped").await;
    assert_eq!(affected, vec![task_id]);
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Retrying);
    let info = queue.get_task(&task_id).await.unwrap();
    assert_eq!(info.retry_count, 1);

    // Wait for backoff and advance
    tokio::time::sleep(Duration::from_millis(25)).await;
    let advanced = queue.process_delayed_retries().await;
    assert_eq!(advanced, 1);
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Queued);

    // 2. Schedule to Worker B
    let w_b = Uuid::new_v4();
    let scheduled_2 = queue.schedule_task(&task_id, w_b).await;
    assert!(scheduled_2.is_some());
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Scheduled);

    // Worker B disconnects (Failure 2 -> Retry 2)
    let affected2 = queue.handle_worker_disconnected(&w_b, "W_B dropped").await;
    assert_eq!(affected2, vec![task_id]);
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Retrying);
    let info2 = queue.get_task(&task_id).await.unwrap();
    assert_eq!(info2.retry_count, 2);

    // Wait for backoff and advance
    tokio::time::sleep(Duration::from_millis(30)).await;
    let advanced2 = queue.process_delayed_retries().await;
    assert_eq!(advanced2, 1);
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Queued);

    // 3. Schedule to Worker C
    let w_c = Uuid::new_v4();
    let scheduled_3 = queue.schedule_task(&task_id, w_c).await;
    assert!(scheduled_3.is_some());
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Scheduled);

    // Worker C disconnects (Failure 3 -> Exceeds max_retries 2!)
    let affected3 = queue.handle_worker_disconnected(&w_c, "W_C dropped").await;
    assert_eq!(affected3, vec![task_id]);
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Failed);

    let info3 = queue.get_task(&task_id).await.unwrap();
    assert_eq!(info3.retry_count, 2);
    assert_eq!(info3.state, TaskState::Failed);
    assert!(info3.state.is_terminal());
    let err = info3.error_message.expect("must have error message");
    assert!(err.contains("Worker disconnected and retries exhausted (2/2)"));

    // Terminal invariance check: further schedule attempts MUST fail
    let invalid_sched = queue.schedule_task(&task_id, Uuid::new_v4()).await;
    assert!(invalid_sched.is_none());
}

/// Test 10: TaskQueue execution failure retries exhaustion under retry_on_execution_failure policy.
/// Verifies that non-zero exit codes trigger Retrying until max_retries is reached, then transition to Failed.
#[tokio::test]
async fn test_adversarial_retry_exhaustion_on_execution_failure() {
    let policy = RetryPolicy {
        max_retries: 2,
        initial_backoff_ms: 10,
        max_backoff_ms: 50,
        backoff_multiplier: 1.0,
        retry_on_execution_failure: true,
        retry_on_worker_disconnect: false,
    };

    let queue = TaskQueue::with_config(policy, None);
    let task = Task::new(
        TaskSpec::command("failing_cmd", vec![]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = queue.submit(task).await.expect("submit failed");

    let worker_id = Uuid::new_v4();

    // Attempt 1: Execution fails
    let _ = queue.schedule_task(&task_id, worker_id).await.unwrap();
    let fail_res_1 = TaskResult::failure(
        worker_id,
        task_id,
        1,
        "",
        "syntax error",
        50,
        Some("command failed with exit code 1".into()),
    );
    let state_1 = queue.record_result(fail_res_1).await.expect("record 1");
    assert_eq!(state_1, TaskState::Retrying);
    assert_eq!(queue.get_task(&task_id).await.unwrap().retry_count, 1);

    // Advance retry
    tokio::time::sleep(Duration::from_millis(20)).await;
    queue.process_delayed_retries().await;
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Queued);

    // Attempt 2: Execution fails again
    let _ = queue.schedule_task(&task_id, worker_id).await.unwrap();
    let fail_res_2 = TaskResult::failure(
        worker_id,
        task_id,
        1,
        "",
        "syntax error again",
        50,
        Some("command failed with exit code 1".into()),
    );
    let state_2 = queue.record_result(fail_res_2).await.expect("record 2");
    assert_eq!(state_2, TaskState::Retrying);
    assert_eq!(queue.get_task(&task_id).await.unwrap().retry_count, 2);

    // Advance retry
    tokio::time::sleep(Duration::from_millis(20)).await;
    queue.process_delayed_retries().await;
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Queued);

    // Attempt 3: Execution fails -> retries exhausted (2/2) -> transitions to Failed!
    let _ = queue.schedule_task(&task_id, worker_id).await.unwrap();
    let fail_res_3 = TaskResult::failure(
        worker_id,
        task_id,
        1,
        "",
        "fatal failure",
        50,
        Some("command failed with exit code 1".into()),
    );
    let state_3 = queue.record_result(fail_res_3).await.expect("record 3");
    assert_eq!(state_3, TaskState::Failed);
    assert_eq!(queue.get_task(&task_id).await.unwrap().retry_count, 2);
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Failed);
}

/// Test 11: Verification that wait_task returns immediately after max_retries exhaustion on worker disconnect.
/// TaskQueue stores a synthetic failure TaskResult in entry.result, so wait_task immediately returns
/// the terminal Failed result without hanging or timing out.
#[tokio::test]
async fn test_adversarial_bug_wait_task_hangs_after_max_retries_exhaustion() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("spawn failed");
    let master_addr = master.server_addr().to_string();

    let task = Task::new(
        TaskSpec::command("sh", vec!["-c".into(), "sleep 5".into()]),
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit failed");

    // Loop through 4 worker failures to exhaust max_retries
    for attempt in 1..=4 {
        let worker = spawn_test_worker(master_addr.clone(), &format!("bug-worker-{attempt}"), 2, false);
        let running = wait_for(
            Duration::from_secs(6),
            Duration::from_millis(30),
            || async {
                if let Ok(info) = master.get_task_info(task_id).await {
                    info.state == TaskState::Running
                } else {
                    false
                }
            },
        )
        .await;
        assert!(running);
        worker.abort();

        if attempt < 4 {
            let in_retry = wait_for(
                Duration::from_secs(3),
                Duration::from_millis(30),
                || async {
                    if let Ok(info) = master.get_task_info(task_id).await {
                        info.state == TaskState::Retrying
                    } else {
                        false
                    }
                },
            )
            .await;
            assert!(in_retry);
        }
    }

    // Wait until task is in terminal Failed state
    let failed = wait_for(
        Duration::from_secs(4),
        Duration::from_millis(30),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.state == TaskState::Failed
            } else {
                false
            }
        },
    )
    .await;
    assert!(failed, "Task is now in terminal Failed state");

    // Verify task state is Failed
    assert_eq!(
        master.get_task_status(task_id).await.unwrap(),
        TaskStatus::Failed
    );

    // Verify wait_task returns the terminal failure promptly
    let wait_res = master
        .wait_task(task_id, Some(Duration::from_millis(500)))
        .await
        .expect("wait_task on task failed via retry exhaustion must return TaskResult");
    assert_eq!(wait_res.exit_code, 1);
    assert!(
        wait_res
            .error
            .as_deref()
            .unwrap_or("")
            .contains("retries exhausted"),
        "Error message must describe retry exhaustion"
    );

    let _ = master.shutdown();
}
