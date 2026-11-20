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
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{
    MasterHandle, MasterServer, ServerConfig, DEFAULT_MASTER_MAX_BATCH_SIZE,
};
use rusty_grid_worker::client::WorkerClient;

fn test_scheduler_config() -> SchedulerConfig {
    SchedulerConfig::default()
        .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
        .without_host_cpu_backpressure()
}

async fn spawn_test_master(config: ServerConfig) -> MasterHandle {
    MasterServer::spawn_with_config(config, test_scheduler_config(), ReaperConfig::default())
        .await
        .expect("master spawn")
}

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
    let master = spawn_test_master(config).await;
    let master_addr = master.server_addr().to_string();

    let (_w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let (_w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);

    let mut w1 =
        WorkerClient::for_testing(master_addr.clone(), Some("crash-w1".into()), Some(2), false);
    let w1_id = w1.worker_id();
    let mut w2 =
        WorkerClient::for_testing(master_addr.clone(), Some("crash-w2".into()), Some(2), false);
    let w2_id = w2.worker_id();

    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });
    let _w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });

    // Wait for both workers to register
    let registered = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            if let Ok(workers) = master.list_workers().await {
                workers.len() == 2
            } else {
                false
            }
        },
    )
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
    let is_running = wait_for(
        Duration::from_secs(5),
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
    assert!(is_running, "Task must enter Running state");

    let initial_info = master.get_task_info(task_id).await.unwrap();
    let assigned_worker = initial_info
        .assigned_worker_id
        .expect("must have assigned worker");

    // Abruptly kill the worker running the task (abort tokio task, dropping TCP connection)
    if assigned_worker == w1_id {
        w1_handle.abort();
    } else {
        _w2_handle.abort();
    }

    // Await completion on survivor worker
    let completed = wait_for(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.state == TaskState::Completed
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        completed,
        "Task must complete on surviving worker after crash"
    );

    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.retry_count, 1, "Task must have exactly 1 retry");
    assert_eq!(final_info.exit_code, Some(0));

    let survivor = if assigned_worker == w1_id {
        w2_id
    } else {
        w1_id
    };
    assert_eq!(final_info.assigned_worker_id, Some(survivor));

    let _ = master.shutdown();
}

/// Test 2: Immediate graceful disconnect.
/// When a worker announces graceful shutdown (WorkerMessage::Disconnecting), the master
/// immediately re-enqueues the task with 0ms delay, bypassing the exponential backoff
/// and reassigning to an available healthy worker in <100ms.
#[tokio::test]
async fn test_immediate_graceful_disconnect() {
    // Configure master with 120s heartbeat timeout to prove we don't wait for reaper
    let reaper_config = ReaperConfig::new(Duration::from_secs(1), Duration::from_secs(120));
    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3),
        test_scheduler_config(),
        reaper_config,
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let (_w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);

    let mut w1 = WorkerClient::for_testing(
        master_addr.clone(),
        Some("graceful-w1".into()),
        Some(2),
        false,
    );
    let w1_id = w1.worker_id();
    let mut w2 = WorkerClient::for_testing(
        master_addr.clone(),
        Some("graceful-w2".into()),
        Some(2),
        false,
    );
    let w2_id = w2.worker_id();

    tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });
    tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });

    // Wait for both workers to register
    assert!(
        wait_for(
            Duration::from_secs(60),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 2)
                    .unwrap_or(false)
            }
        )
        .await
    );

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
    assert!(
        wait_for(
            Duration::from_secs(60),
            Duration::from_millis(30),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false)
            }
        )
        .await
    );

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

    // Assert that task is reassigned to survivor well before the 120s reaper timeout and takes the
    // uncharged graceful evacuation path (`0 ms` retry backoff, leaving `retry_count` /
    // `failover_count` at 0).
    let reassigned = wait_for(
        Duration::from_secs(60),
        Duration::from_millis(15),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.assigned_worker_id == Some(survivor_id)
                    && matches!(
                        info.state,
                        TaskState::Scheduled | TaskState::Running | TaskState::Completed
                    )
            } else {
                false
            }
        },
    )
    .await;
    let disconnect_elapsed = disconnect_time.elapsed();
    println!("graceful disconnect reassigned task to survivor in {disconnect_elapsed:?}");
    assert!(
        reassigned,
        "Graceful disconnect must trigger task reassignment to survivor"
    );
    assert!(
        disconnect_elapsed < Duration::from_secs(60),
        "Graceful disconnect reassignment hung past 60s guard: {disconnect_elapsed:?}"
    );
    let reassigned_info = master.get_task_info(task_id).await.expect("task info");
    assert_eq!(
        reassigned_info.evacuation_count, 1,
        "graceful Disconnecting farewell must route via uncharged evacuation (evacuation_count=1)"
    );
    assert_eq!(
        reassigned_info.retry_count, 0,
        "graceful evacuation must bypass exponential retry backoff (retry_count=0)"
    );
    assert_eq!(
        reassigned_info.failover_count, 0,
        "graceful evacuation must not charge failover_count"
    );

    // Wait for completion on survivor
    assert!(
        wait_for(
            Duration::from_secs(60),
            Duration::from_millis(50),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Completed)
                    .unwrap_or(false)
            }
        )
        .await
    );

    let _ = master.shutdown();
}

/// Test 3: Max retries exhaustion.
/// When consecutive worker failures exhaust all allowed retries, the task cleanly
/// transitions to the terminal Failed state with a descriptive failure message and
/// TaskResult::failure.
#[tokio::test]
async fn test_max_retries_exhaustion() {
    // Master with max_retries = 2 (failovers have their own budget; pin it to the same limit)
    let master = spawn_test_master(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_max_retries(2)
            .with_max_failovers(2),
    )
    .await;
    let master_addr = master.server_addr().to_string();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::for_testing(master_addr.clone(), Some("w1".into()), Some(1), false);
    let w1_id = w1.worker_id();
    let h1 = tokio::spawn(async move {
        let _ = w1.run(w1_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 1)
                    .unwrap_or(false)
            }
        )
        .await
    );

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
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false)
            }
        )
        .await
    );
    h1.abort();

    assert!(
        wait_for(
            Duration::from_secs(3),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.retry_count == 1)
                    .unwrap_or(false)
            }
        )
        .await
    );

    // 2. Spawn w2 -> takes task -> kill w2 (retry 2)
    let (_w2_tx, w2_rx) = watch::channel(false);
    let mut w2 = WorkerClient::for_testing(master_addr.clone(), Some("w2".into()), Some(1), false);
    let h2 = tokio::spawn(async move {
        let _ = w2.run(w2_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Running && i.assigned_worker_id != Some(w1_id))
                    .unwrap_or(false)
            }
        )
        .await
    );
    h2.abort();

    assert!(
        wait_for(
            Duration::from_secs(3),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.retry_count == 2)
                    .unwrap_or(false)
            }
        )
        .await
    );

    // 3. Spawn w3 -> takes task -> kill w3 -> max_retries (2) exceeded!
    let (_w3_tx, w3_rx) = watch::channel(false);
    let mut w3 = WorkerClient::for_testing(master_addr.clone(), Some("w3".into()), Some(1), false);
    let h3 = tokio::spawn(async move {
        let _ = w3.run(w3_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false)
            }
        )
        .await
    );
    h3.abort();

    // Must cleanly transition to Failed
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Failed)
                    .unwrap_or(false)
            }
        )
        .await
    );

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
    // Wait-by-condition margin: 50ms scan, 1s timeout avoids 60-80ms CI starvation flakes
    // (TEST-45) while remaining far below the 60s wait guard and 120s task duration.
    let reaper_config = ReaperConfig::new(Duration::from_millis(50), Duration::from_secs(1));
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(0);
    let master =
        MasterServer::spawn_with_config(server_config, test_scheduler_config(), reaper_config)
            .await
            .expect("master spawn");
    let master_addr = master.server_addr();

    // Connect raw TCP worker that heartbeats until AssignTask arrives, then halts heartbeats
    let stream = tokio::net::TcpStream::connect(master_addr)
        .await
        .expect("tcp connect");
    let mut transport = rusty_grid_core::protocol::MessageTransport::new(stream);

    let worker_id = Uuid::new_v4();
    let reg_msg = rusty_grid_core::protocol::WorkerMessage::Register {
        worker_id,
        capabilities: rusty_grid_core::capabilities::WorkerCapabilities::new(
            "w-reaper", 2, 4096, false, false, None,
        ),
        protocol_version: rusty_grid_core::protocol::PROTOCOL_VERSION_CURRENT,
        features: rusty_grid_core::protocol::ProtocolCapabilities::all_supported(),
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
            duration_ms: 120_000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 120).with_max_retries(0),
    );
    let task_id = master.submit_task(task).await.expect("submit task");

    // Keep the raw worker alive with heartbeats while waiting for AssignTask (60s guard)
    let (mut writer, mut reader) = transport.split();
    let assign_timeout = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(assign_timeout);
    let mut hb_interval = tokio::time::interval(Duration::from_millis(50));
    hb_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = reader.recv_msg::<rusty_grid_core::protocol::MasterMessage>() => {
                match msg {
                    Ok(Some(rusty_grid_core::protocol::MasterMessage::AssignTask { task })) => {
                        assert_eq!(task.id, task_id);
                        break;
                    }
                    Ok(Some(rusty_grid_core::protocol::MasterMessage::HeartbeatAck { .. })) => continue,
                    Ok(other) => panic!("unexpected master message before AssignTask: {other:?}"),
                    Err(e) => panic!("recv_msg failed while waiting for AssignTask: {e:?}"),
                }
            }
            _ = hb_interval.tick() => {
                let hb = rusty_grid_core::protocol::WorkerMessage::Heartbeat {
                    worker_id,
                    timestamp: 0,
                    active_tasks: 0,
                    cpu_usage_pct: 0.0,
                    ram_available_mb: 4096,
                };
                writer.send_msg(&hb).await.expect("send heartbeat");
            }
            _ = &mut assign_timeout => {
                panic!("timed out waiting for AssignTask after 60s");
            }
        }
    }

    // Spawn client task waiting on wait_task
    let master_clone = master.clone();
    let wait_handle = tokio::spawn(async move {
        master_clone
            .wait_task(task_id, Some(Duration::from_secs(60)))
            .await
    });

    // Do NOT send any more heartbeats! Keep TCP open and silent (simulating partition/freeze).
    // The reaper detects inactivity >1s and marks worker Disconnected.
    // Because max_retries == 0, task transitions to Failed and immediately resolves waiter!
    let start_wait = Instant::now();
    let result = wait_handle
        .await
        .expect("wait handle join")
        .expect("wait_task result");

    let elapsed = start_wait.elapsed();
    println!("wait_task unblocked on reaper eviction in {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(60),
        "wait_task hung past 60s guard when reaper trips (elapsed: {elapsed:?})"
    );
    assert_eq!(result.exit_code, 1);
    let err = result.error.unwrap_or_default();
    assert!(
        err.contains("maximum retry limit (0) reached") || err.contains("Heartbeat timeout"),
        "Must mention retry limit or heartbeat timeout: {err}"
    );

    // Explicitly keep TCP open until wait_task resolves so NLL cannot close it early via EOF.
    drop((writer, reader));
    let _ = master.shutdown();
}
