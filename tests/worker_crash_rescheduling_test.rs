//! Integration test suite verifying Milestone 9: Fault-Tolerant Worker Lifecycle & Dynamic Task Rescheduling.
//!
//! Validates:
//! 1. Worker crash recovery: Worker running a task has its connection terminated (simulating crash);
//!    Master detects failure, re-enqueues task, schedules to surviving healthy worker, completes
//!    with exit code 0 and retry_count == 1.
//! 2. Immediate graceful disconnect: Worker sends WorkerMessage::Disconnecting;
//!    Master immediately reassigns task to surviving worker without waiting for heartbeat timeout or backoff.
//! 3. Max retries exhaustion: Consecutive worker terminations exhaust retries, cleanly transitioning
//!    task to Failed state with descriptive error message and TaskResult::failure.
//! 4. Reaper waiters resolution: Worker halts heartbeats and times out via reaper;
//!    client wait_task unblocks immediately with TaskResult::failure.

use std::time::{Duration, Instant};

use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;

/// Asynchronously polls `condition` until it returns true or `timeout` expires.
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

/// Test 1: Worker crash recovery.
/// When a worker running a task abruptly crashes (socket dropped), the master detects
/// the disconnect, re-enqueues the in-flight task, schedules it to the surviving worker,
/// and executes to completion with exit code 0 and retry_count == 1.
#[tokio::test]
async fn test_worker_crash_recovery() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let master = MasterServer::spawn(config).await.expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (_w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let (_w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);

    let mut w1 = WorkerClient::from_options(master_addr.clone(), Some("crash-w1".into()), Some(2), false);
    let w1_id = w1.worker_id();
    let mut w2 = WorkerClient::from_options(master_addr.clone(), Some("crash-w2".into()), Some(2), false);
    let w2_id = w2.worker_id();

    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });
    let _w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });

    // Wait for both workers to register
    let registered = wait_for(Duration::from_secs(5), Duration::from_millis(50), || async {
        if let Ok(workers) = master.list_workers().await {
            workers.len() == 2
        } else {
            false
        }
    })
    .await;
    assert!(registered, "Both workers must be registered");

    // Task that runs for 1.5 seconds so we can kill worker while in-flight
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "crash_recovery".into(),
            iterations: 100,
            duration_ms: 1500,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );

    let task_id = master.submit_task(task).await.expect("submit task");

    // Wait until task is running
    let is_running = wait_for(Duration::from_secs(5), Duration::from_millis(30), || async {
        if let Ok(info) = master.get_task_info(task_id).await {
            info.state == TaskState::Running
        } else {
            false
        }
    })
    .await;
    assert!(is_running, "Task must enter Running state");

    let initial_info = master.get_task_info(task_id).await.unwrap();
    let assigned_worker = initial_info.assigned_worker_id.expect("must have assigned worker");

    // Abruptly kill the worker running the task (abort tokio task, dropping TCP connection)
    if assigned_worker == w1_id {
        w1_handle.abort();
    } else {
        _w2_handle.abort();
    }

    // Await completion on survivor worker
    let completed = wait_for(Duration::from_secs(10), Duration::from_millis(50), || async {
        if let Ok(info) = master.get_task_info(task_id).await {
            info.state == TaskState::Completed
        } else {
            false
        }
    })
    .await;
    assert!(completed, "Task must complete on surviving worker after crash");

    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.retry_count, 1, "Task must have exactly 1 retry");
    assert_eq!(final_info.exit_code, Some(0));

    let survivor = if assigned_worker == w1_id { w2_id } else { w1_id };
    assert_eq!(final_info.assigned_worker_id, Some(survivor));

    let _ = master.shutdown();
}

/// Test 2: Immediate graceful disconnect.
/// When a worker announces graceful shutdown (WorkerMessage::Disconnecting), the master
/// immediately re-enqueues the task with 0ms delay, bypassing the exponential backoff
/// and reassigning to an available healthy worker in <100ms.
#[tokio::test]
async fn test_immediate_graceful_disconnect() {
    // Configure master with 10s heartbeat timeout to prove we don't wait for reaper
    let reaper_config = ReaperConfig::new(Duration::from_secs(1), Duration::from_secs(10));
    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3),
        Default::default(),
        reaper_config,
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let (_w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);

    let mut w1 = WorkerClient::from_options(master_addr.clone(), Some("graceful-w1".into()), Some(2), false);
    let w1_id = w1.worker_id();
    let mut w2 = WorkerClient::from_options(master_addr.clone(), Some("graceful-w2".into()), Some(2), false);
    let w2_id = w2.worker_id();

    tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });
    tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });

    // Wait for both workers to register
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(50), || async {
        master.list_workers().await.map(|w| w.len() == 2).unwrap_or(false)
    })
    .await);

    // Submit task with 2 second duration
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "graceful_task".into(),
            iterations: 100,
            duration_ms: 2000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit");

    // Wait until running
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(30), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
    })
    .await);

    let info = master.get_task_info(task_id).await.unwrap();
    let initial_worker = info.assigned_worker_id.expect("must be assigned");
    let (target_shutdown_tx, survivor_id) = if initial_worker == w1_id {
        (w1_shutdown_tx, w2_id)
    } else {
        (_w2_shutdown_tx, w1_id)
    };

    // Trigger graceful shutdown
    let disconnect_time = Instant::now();
    let _ = target_shutdown_tx.send(true);

    // Assert that task is reassigned to survivor in <300ms (far less than 10s heartbeat timeout and avoids 500ms backoff)
    let reassigned = wait_for(Duration::from_millis(300), Duration::from_millis(15), || async {
        if let Ok(info) = master.get_task_info(task_id).await {
            info.assigned_worker_id == Some(survivor_id)
                && (info.state == TaskState::Scheduled || info.state == TaskState::Running)
        } else {
            false
        }
    })
    .await;
    assert!(
        reassigned,
        "Graceful disconnect must trigger immediate task reassignment to survivor"
    );
    assert!(disconnect_time.elapsed() < Duration::from_millis(300));

    // Wait for completion on survivor
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(50), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Completed).unwrap_or(false)
    })
    .await);

    let _ = master.shutdown();
}

/// Test 3: Max retries exhaustion.
/// When consecutive worker failures exhaust all allowed retries, the task cleanly
/// transitions to the terminal Failed state with a descriptive failure message and
/// TaskResult::failure.
#[tokio::test]
async fn test_max_retries_exhaustion() {
    // Master with max_retries = 2
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(2),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::from_options(master_addr.clone(), Some("w1".into()), Some(1), false);
    let w1_id = w1.worker_id();
    let h1 = tokio::spawn(async move { let _ = w1.run(w1_rx).await; });

    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(50), || async {
        master.list_workers().await.map(|w| w.len() == 1).unwrap_or(false)
    })
    .await);

    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "exhaust_task".into(),
            iterations: 100,
            duration_ms: 1000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.unwrap();

    // 1. Running on w1 -> kill w1 (retry 1)
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
    })
    .await);
    h1.abort();

    assert!(wait_for(Duration::from_secs(3), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.retry_count == 1).unwrap_or(false)
    })
    .await);

    // 2. Spawn w2 -> takes task -> kill w2 (retry 2)
    let (_w2_tx, w2_rx) = watch::channel(false);
    let mut w2 = WorkerClient::from_options(master_addr.clone(), Some("w2".into()), Some(1), false);
    let h2 = tokio::spawn(async move { let _ = w2.run(w2_rx).await; });

    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running && i.assigned_worker_id != Some(w1_id)).unwrap_or(false)
    })
    .await);
    h2.abort();

    assert!(wait_for(Duration::from_secs(3), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.retry_count == 2).unwrap_or(false)
    })
    .await);

    // 3. Spawn w3 -> takes task -> kill w3 -> max_retries (2) exceeded!
    let (_w3_tx, w3_rx) = watch::channel(false);
    let mut w3 = WorkerClient::from_options(master_addr.clone(), Some("w3".into()), Some(1), false);
    let h3 = tokio::spawn(async move { let _ = w3.run(w3_rx).await; });

    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
    })
    .await);
    h3.abort();

    // Must cleanly transition to Failed
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Failed).unwrap_or(false)
    })
    .await);

    let info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(info.state, TaskState::Failed);
    assert_eq!(info.retry_count, 2);
    let err_msg = info.error_message.expect("error message must be present");
    assert!(
        err_msg.contains("maximum retry limit (2) reached"),
        "Must contain max retry limit: {err_msg}"
    );
    assert!(
        err_msg.contains("retries exhausted"),
        "Must contain retries exhausted: {err_msg}"
    );

    let _ = master.shutdown();
}

/// Test 4: Reaper client waiters resolution.
/// When a worker halts heartbeats (simulating silent partition) and is evicted by the
/// Master heartbeat reaper, any client blocked on wait_task unblocks promptly with
/// TaskResult::failure instead of hanging indefinitely.
#[tokio::test]
async fn test_reaper_waiters_resolution() {
    // Fast reaper: scans every 20ms, timeout 60ms
    let reaper_config = ReaperConfig::new(Duration::from_millis(20), Duration::from_millis(60));
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(0);
    let master = MasterServer::spawn_with_config(
        server_config,
        Default::default(),
        reaper_config,
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr();

    // Connect raw TCP worker that performs handshake then halts sending heartbeats
    let stream = tokio::net::TcpStream::connect(master_addr).await.expect("tcp connect");
    let mut transport = rusty_grid_core::protocol::MessageTransport::new(stream);

    let worker_id = Uuid::new_v4();
    let reg_msg = rusty_grid_core::protocol::WorkerMessage::Register {
        worker_id,
        capabilities: rusty_grid_core::capabilities::WorkerCapabilities::new("w-reaper", 2, 4096, false, false, None),
    };
    transport.send_msg(&reg_msg).await.expect("send register");

    // Receive RegisterAck
    let ack: Option<rusty_grid_core::protocol::MasterMessage> =
        transport.recv_msg().await.expect("recv ack");
    assert!(matches!(
        ack,
        Some(rusty_grid_core::protocol::MasterMessage::RegisterAck { .. })
    ));

    // Submit task with max_retries = 0
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "reaper_test".into(),
            iterations: 100,
            duration_ms: 5000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30).with_max_retries(0),
    );
    let task_id = master.submit_task(task).await.expect("submit task");

    // Receive AssignTask on worker
    let assign_msg: Option<rusty_grid_core::protocol::MasterMessage> =
        transport.recv_msg().await.expect("recv assign");
    assert!(matches!(
        assign_msg,
        Some(rusty_grid_core::protocol::MasterMessage::AssignTask { .. })
    ));

    // Spawn client task waiting on wait_task
    let master_clone = master.clone();
    let wait_handle = tokio::spawn(async move {
        master_clone.wait_task(task_id, Some(Duration::from_secs(5))).await
    });

    // Do NOT send any heartbeats! The worker holds TCP open but is silent (simulating partition/freeze).
    // The reaper detects inactivity >60ms and marks worker Disconnected.
    // Because max_retries == 0, task transitions to Failed and immediately resolves waiter!
    let start_wait = Instant::now();
    let result = wait_handle.await.expect("wait handle join").expect("wait_task result");

    let elapsed = start_wait.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "wait_task must unblock promptly when reaper trips (elapsed: {elapsed:?})"
    );
    assert_eq!(result.exit_code, 1);
    let err = result.error.unwrap_or_default();
    assert!(
        err.contains("maximum retry limit (0) reached") || err.contains("Heartbeat timeout"),
        "Must mention retry limit or heartbeat timeout: {err}"
    );

    let _ = master.shutdown();
}
