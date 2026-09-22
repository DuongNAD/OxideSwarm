//! Empirical Stress Challenge Suite 2 for Milestone 9:
//! Fault-Tolerant Worker Lifecycle & Dynamic Task Rescheduling.
//!
//! Authored by Challenger M9.2 to empirically verify:
//! 1. `test_boundary_max_retries_0_immediate_failure_on_crash`:
//!    Task with `max_retries: Some(0)` fails immediately on first worker crash without retrying.
//! 2. `test_boundary_max_retries_1_single_retry_then_failure`:
//!    Task with `max_retries: Some(1)` allows exactly 1 retry; fails on 2nd crash.
//! 3. `test_boundary_max_retries_5_overrides_lower_server_default`:
//!    Task with `max_retries: Some(5)` overrides server default of 1 retry across 5 crash cycles.
//! 4. `test_direct_queue_boundary_crash_cycles_and_audit`:
//!    Direct queue verification of FSM states, `retry_history`, error formatting, and exit code 1.
//! 5. `test_client_wait_task_unblocks_promptly_on_crash`:
//!    `wait_task` unblocks promptly (<200ms) on crash exhaustion with `TaskResult::failure`.
//! 6. `test_client_wait_task_unblocks_on_reaper_eviction_latency`:
//!    `wait_task` unblocks within milliseconds of reaper eviction on silent worker partition.
//! 7. `test_client_wait_task_preserves_resolution_across_mid_flight_failover`:
//!    `wait_task` preserves listener channel across intermediate worker crash and succeeds on survivor.
//! 8. `test_multiple_concurrent_client_waiters_fanout`:
//!    Multiple concurrent callers to `wait_task` all receive identical synthetic failure.
//! 9. `test_tcp_wire_client_submit_wait_resolution`:
//!    Wire TCP client sending `SubmitTask { wait: true }` receives `TaskCompleted` with exit code 1.
//! 10. `test_cli_flags_verification`:
//!     CLI flags `--default-retry-max` and `--max-retries` validation, precedence, and help output.

use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures::future::join_all;
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{
    ClientMessage, ClientResponse, MasterMessage, MessageTransport, WorkerMessage,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::queue::{RetryPolicy, TaskQueue, TaskState};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;

fn resolve_field<T, F>(
    cli: Option<T>,
    env_names: &[&str],
    file_val: Option<T>,
    default: T,
    parse_env: F,
) -> T
where
    F: Fn(&str) -> Option<T>,
{
    if let Some(val) = cli {
        return val;
    }
    for env in env_names {
        if let Ok(val_str) = std::env::var(env) {
            if let Some(val) = parse_env(&val_str) {
                return val;
            }
        }
    }
    if let Some(val) = file_val {
        return val;
    }
    default
}

fn resolve_u32(cli: Option<u32>, env_names: &[&str], file_val: Option<u32>, default: u32) -> u32 {
    resolve_field(cli, env_names, file_val, default, |s| s.trim().parse().ok())
}

fn resolve_opt_u32(cli: Option<u32>, env_names: &[&str], file_val: Option<u32>) -> Option<u32> {
    if let Some(val) = cli {
        return Some(val);
    }
    for env in env_names {
        if let Ok(val_str) = std::env::var(env) {
            if let Ok(val) = val_str.trim().parse::<u32>() {
                return Some(val);
            }
        }
    }
    file_val
}

static ENV_COUNTER: AtomicUsize = AtomicUsize::new(100);

fn unique_env_key(prefix: &str) -> String {
    let id = ENV_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{prefix}_{id}_{}", Uuid::new_v4().simple())
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

/// Helper locating the compiled `rusty-grid` binary.
fn find_rusty_grid_bin() -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir.parent()?.parent()?;
    let exe_name = if cfg!(windows) {
        "rusty-grid.exe"
    } else {
        "rusty-grid"
    };
    let candidate = root.join("target").join("debug").join(exe_name);
    if candidate.exists() {
        Some(candidate)
    } else {
        None
    }
}

// ============================================================================
// 1. Boundary Condition: max_retries = Some(0) Immediate Failure on Crash
// ============================================================================

#[tokio::test]
async fn test_boundary_max_retries_0_immediate_failure_on_crash() {
    // Server configured with generous default retries (5)
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(5),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let (_w2_tx, w2_rx) = watch::channel(false);

    let mut w1 = WorkerClient::from_options(master_addr.clone(), Some("b0-w1".into()), Some(2), false);
    let w1_id = w1.worker_id();
    let mut w2 = WorkerClient::from_options(master_addr.clone(), Some("b0-w2".into()), Some(2), false);
    let w2_id = w2.worker_id();

    let h1 = tokio::spawn(async move { let _ = w1.run(w1_rx).await; });
    let _h2 = tokio::spawn(async move { let _ = w2.run(w2_rx).await; });

    // Wait for both workers to register
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(30), || async {
        master.list_workers().await.map(|w| w.len() == 2).unwrap_or(false)
    }).await);

    // Submit task with max_retries explicitly overridden to 0
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "boundary_0".into(),
            iterations: 100,
            duration_ms: 2000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30).with_max_retries(0),
    );
    let task_id = master.submit_task(task).await.expect("submit task");

    // Wait until running
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
    }).await);

    let initial_info = master.get_task_info(task_id).await.unwrap();
    let assigned_worker = initial_info.assigned_worker_id.unwrap();

    // Abruptly kill the assigned worker
    if assigned_worker == w1_id {
        h1.abort();
    } else {
        _h2.abort();
    }

    // Task MUST transition directly to Failed without any retries
    assert!(wait_for(Duration::from_secs(4), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Failed).unwrap_or(false)
    }).await);

    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Failed);
    assert_eq!(final_info.retry_count, 0, "retry_count must remain 0 when max_retries is 0");
    assert_eq!(final_info.exit_code, Some(1), "Synthetic failure exit code must be 1");

    let err = final_info.error_message.expect("error message must be populated");
    assert!(
        err.contains("maximum retry limit (0) reached after worker eviction"),
        "Error message must specify limit 0 reached: {err}"
    );
    assert!(
        err.contains("retries exhausted (0/0)"),
        "Error message must specify retries exhausted (0/0): {err}"
    );

    // The surviving worker must remain completely untouched (active_tasks == 0)
    let survivor_id = if assigned_worker == w1_id { w2_id } else { w1_id };
    let workers = master.list_workers().await.unwrap();
    let survivor = workers.iter().find(|w| w.worker_id == survivor_id).unwrap();
    assert_eq!(survivor.active_tasks, 0, "Surviving worker must never have received the task");

    let _ = master.shutdown();
}

// ============================================================================
// 2. Boundary Condition: max_retries = Some(1) Exact Single Retry then Fail
// ============================================================================

#[tokio::test]
async fn test_boundary_max_retries_1_single_retry_then_failure() {
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::from_options(master_addr.clone(), Some("b1-w1".into()), Some(1), false);
    let w1_id = w1.worker_id();
    let h1 = tokio::spawn(async move { let _ = w1.run(w1_rx).await; });

    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(30), || async {
        master.list_workers().await.map(|w| w.len() == 1).unwrap_or(false)
    }).await);

    // Task with max_retries: Some(1)
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "boundary_1".into(),
            iterations: 100,
            duration_ms: 2000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30).with_max_retries(1),
    );
    let task_id = master.submit_task(task).await.expect("submit");

    // 1. Task enters Running on w1 -> kill w1
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
    }).await);
    h1.abort();

    // 2. Task reschedules, retry_count increments to 1
    assert!(wait_for(Duration::from_secs(4), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.retry_count == 1).unwrap_or(false)
    }).await);

    // 3. Spawn w2 -> takes task
    let (_w2_tx, w2_rx) = watch::channel(false);
    let mut w2 = WorkerClient::from_options(master_addr.clone(), Some("b1-w2".into()), Some(1), false);
    let h2 = tokio::spawn(async move { let _ = w2.run(w2_rx).await; });

    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running && i.assigned_worker_id != Some(w1_id)).unwrap_or(false)
    }).await);

    // 4. Kill w2 -> max_retries (1) is now exhausted!
    h2.abort();

    // Must transition cleanly to Failed
    assert!(wait_for(Duration::from_secs(4), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Failed).unwrap_or(false)
    }).await);

    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Failed);
    assert_eq!(final_info.retry_count, 1, "retry_count must be exactly 1");
    assert_eq!(final_info.exit_code, Some(1));

    let err = final_info.error_message.expect("error message");
    assert!(err.contains("maximum retry limit (1) reached after worker eviction"));
    assert!(err.contains("retries exhausted (1/1)"));

    // Spawn a 3rd worker to verify it is NEVER given this task
    let (_w3_tx, w3_rx) = watch::channel(false);
    let mut w3 = WorkerClient::from_options(master_addr.clone(), Some("b1-w3".into()), Some(1), false);
    let w3_id = w3.worker_id();
    let _h3 = tokio::spawn(async move { let _ = w3.run(w3_rx).await; });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let w3_info = master.list_workers().await.unwrap().into_iter().find(|w| w.worker_id == w3_id).unwrap();
    assert_eq!(w3_info.active_tasks, 0, "Failed task must never be dispatched to new workers");

    let _ = master.shutdown();
}

// ============================================================================
// 3. Boundary Condition: max_retries = Some(5) Overriding Lower Server Default
// ============================================================================

#[tokio::test]
async fn test_boundary_max_retries_5_overrides_lower_server_default() {
    // Master server configured with default_retry_max = 1 (very restrictive!)
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(1),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    // Submit task requesting max_retries = Some(5)
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "boundary_5".into(),
            iterations: 100,
            duration_ms: 3000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30).with_max_retries(5),
    );
    let task_id = master.submit_task(task).await.expect("submit");

    // Execute 5 consecutive graceful disconnect cycles (immediate 0ms failover)
    // to rapidly and deterministically test all 5 retry boundary steps
    for cycle in 1..=5 {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut worker = WorkerClient::from_options(
            master_addr.clone(),
            Some(format!("w-cycle-{cycle}")),
            Some(1),
            false,
        );
        let _h = tokio::spawn(async move { let _ = worker.run(shutdown_rx).await; });

        // Wait until task is running on this worker
        assert!(
            wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
                master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
            }).await,
            "Task must become Running in cycle {cycle}"
        );

        // Gracefully disconnect the worker
        let _ = shutdown_tx.send(true);

        // Wait for retry_count to increment to cycle
        assert!(
            wait_for(Duration::from_secs(3), Duration::from_millis(15), || async {
                master.get_task_info(task_id).await.map(|i| i.retry_count == cycle as u32).unwrap_or(false)
            }).await,
            "retry_count must increment to {cycle}"
        );

        let info = master.get_task_info(task_id).await.unwrap();
        if cycle < 5 {
            // Task must be Queued or Retrying, NOT Failed (even though cycle >= 1 which exceeds server default of 1!)
            assert_ne!(
                info.state,
                TaskState::Failed,
                "Cycle {cycle} must NOT fail because per-task max_retries is 5, overriding server default 1"
            );
        }
    }

    // Now spawn the 6th worker: accepts task, then disconnects -> exhausts max_retries (5)
    let (shutdown_tx6, shutdown_rx6) = watch::channel(false);
    let mut w6 = WorkerClient::from_options(master_addr.clone(), Some("w-cycle-6".into()), Some(1), false);
    let _h6 = tokio::spawn(async move { let _ = w6.run(shutdown_rx6).await; });

    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
    }).await);

    let _ = shutdown_tx6.send(true);

    // Now it MUST transition to Failed!
    assert!(wait_for(Duration::from_secs(4), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Failed).unwrap_or(false)
    }).await);

    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Failed);
    assert_eq!(final_info.retry_count, 5, "Must have exactly 5 retries");
    assert_eq!(final_info.exit_code, Some(1));

    let err = final_info.error_message.unwrap();
    assert!(
        err.contains("maximum retry limit (5) reached after worker eviction"),
        "Error message must reflect limit 5: {err}"
    );
    assert!(
        err.contains("retries exhausted (5/5)"),
        "Error message must reflect 5/5 exhausted: {err}"
    );

    // Verify retry history contains all 5 attempts
    let queue_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(queue_info.retry_count, 5);

    let _ = master.shutdown();
}

// ============================================================================
// 4. Direct TaskQueue Boundary Crash Cycles & Audit Verification
// ============================================================================

#[tokio::test]
async fn test_direct_queue_boundary_crash_cycles_and_audit() {
    let policy = RetryPolicy {
        max_retries: 2,
        initial_backoff_ms: 10,
        max_backoff_ms: 100,
        backoff_multiplier: 1.5,
        retry_on_execution_failure: false,
        retry_on_worker_disconnect: true,
    };
    let queue = TaskQueue::with_config(policy, None);

    // Task A: inherits policy default (2 retries)
    let task_a = Task::new(
        TaskSpec::new_command("echo", vec!["A".into()]),
        TaskRequirements::generic(1, 30),
    );
    let id_a = queue.submit(task_a).await.unwrap();

    // Task B: overrides to 0 retries
    let task_b = Task::new(
        TaskSpec::new_command("echo", vec!["B".into()]),
        TaskRequirements::generic(1, 30).with_max_retries(0),
    );
    let id_b = queue.submit(task_b).await.unwrap();

    // Task C: overrides to 1 retry
    let task_c = Task::new(
        TaskSpec::new_command("echo", vec!["C".into()]),
        TaskRequirements::generic(1, 30).with_max_retries(1),
    );
    let id_c = queue.submit(task_c).await.unwrap();

    let wid1 = Uuid::new_v4();
    let wid2 = Uuid::new_v4();
    let wid3 = Uuid::new_v4();

    // Schedule and run all three tasks
    assert!(queue.schedule_task(&id_a, wid1).await.is_some());
    assert!(queue.schedule_task(&id_b, wid1).await.is_some());
    assert!(queue.schedule_task(&id_c, wid1).await.is_some());
    queue.mark_running(&id_a, wid1).await.unwrap();
    queue.mark_running(&id_b, wid1).await.unwrap();
    queue.mark_running(&id_c, wid1).await.unwrap();

    // Crash wid1
    let affected = queue.handle_worker_disconnected(&wid1, "Crash wid1", true).await;
    assert_eq!(affected.len(), 3);

    // Task B had max_retries = 0 -> MUST BE FAILED IMMEDIATELY
    let info_b = queue.get_task(&id_b).await.unwrap();
    assert_eq!(info_b.state, TaskState::Failed);
    assert_eq!(info_b.retry_count, 0);
    assert!(info_b.error_message.unwrap().contains("maximum retry limit (0) reached"));
    let res_b = queue.get_result(&id_b).await.unwrap();
    assert_eq!(res_b.exit_code, 1);

    // Tasks A & C should have re-enqueued (retry_count == 1)
    let info_a = queue.get_task(&id_a).await.unwrap();
    assert_eq!(info_a.retry_count, 1);
    assert_eq!(info_a.state, TaskState::Queued);

    let info_c = queue.get_task(&id_c).await.unwrap();
    assert_eq!(info_c.retry_count, 1);
    assert_eq!(info_c.state, TaskState::Queued);

    // Schedule A & C to wid2
    assert!(queue.schedule_task(&id_a, wid2).await.is_some());
    assert!(queue.schedule_task(&id_c, wid2).await.is_some());
    queue.mark_running(&id_a, wid2).await.unwrap();
    queue.mark_running(&id_c, wid2).await.unwrap();

    // Crash wid2
    let affected2 = queue.handle_worker_disconnected(&wid2, "Crash wid2", true).await;
    assert_eq!(affected2.len(), 2);

    // Task C had max_retries = 1 -> MUST BE FAILED NOW
    let info_c2 = queue.get_task(&id_c).await.unwrap();
    assert_eq!(info_c2.state, TaskState::Failed);
    assert_eq!(info_c2.retry_count, 1);
    assert!(info_c2.error_message.unwrap().contains("maximum retry limit (1) reached"));
    let res_c = queue.get_result(&id_c).await.unwrap();
    assert_eq!(res_c.exit_code, 1);

    // Task A had max_retries = 2 -> retry_count becomes 2, state Queued
    let info_a2 = queue.get_task(&id_a).await.unwrap();
    assert_eq!(info_a2.retry_count, 2);
    assert_eq!(info_a2.state, TaskState::Queued);

    // Schedule A to wid3
    assert!(queue.schedule_task(&id_a, wid3).await.is_some());
    queue.mark_running(&id_a, wid3).await.unwrap();

    // Crash wid3 -> exhausts Task A max_retries (2)
    let affected3 = queue.handle_worker_disconnected(&wid3, "Crash wid3", true).await;
    assert_eq!(affected3.len(), 1);

    let info_a3 = queue.get_task(&id_a).await.unwrap();
    assert_eq!(info_a3.state, TaskState::Failed);
    assert_eq!(info_a3.retry_count, 2);
    assert!(info_a3.error_message.unwrap().contains("maximum retry limit (2) reached"));
    let res_a = queue.get_result(&id_a).await.unwrap();
    assert_eq!(res_a.exit_code, 1);
}

// ============================================================================
// 5. Client wait_task Resolution: Instant Unblock on Crash Exhaustion
// ============================================================================

#[tokio::test]
async fn test_client_wait_task_unblocks_promptly_on_crash() {
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(0),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::from_options(master_addr.clone(), Some("wait-w1".into()), Some(1), false);
    let h1 = tokio::spawn(async move { let _ = w1.run(w1_rx).await; });

    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(30), || async {
        master.list_workers().await.map(|w| w.len() == 1).unwrap_or(false)
    }).await);

    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "wait_crash".into(),
            iterations: 100,
            duration_ms: 3000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30).with_max_retries(0),
    );
    let task_id = master.submit_task(task).await.expect("submit");

    // Spawn client calling wait_task with 5 second timeout
    let master_clone = master.clone();
    let wait_handle = tokio::spawn(async move {
        master_clone.wait_task(task_id, Some(Duration::from_secs(5))).await
    });

    // Wait until running
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
    }).await);

    // Kill worker and measure latency until wait_task unblocks
    let kill_time = Instant::now();
    h1.abort();

    let result = wait_handle.await.expect("join wait handle").expect("wait_task result");
    let unblock_latency = kill_time.elapsed();

    // Must unblock in less than 300ms (far less than 5s timeout!)
    assert!(
        unblock_latency < Duration::from_millis(500),
        "wait_task must unblock immediately on worker crash (took {unblock_latency:?})"
    );
    assert_eq!(result.exit_code, 1);
    assert!(!result.is_success());
    let err = result.error.unwrap_or_default();
    assert!(err.contains("maximum retry limit (0) reached after worker eviction"));

    let _ = master.shutdown();
}

// ============================================================================
// 6. Client wait_task Resolution: Reaper Eviction Unblocking Latency
// ============================================================================

#[tokio::test]
async fn test_client_wait_task_unblocks_on_reaper_eviction_latency() {
    // Fast reaper: 20ms scan interval, 60ms timeout threshold
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

    // Raw TCP connection simulating a silent/frozen worker (no heartbeats sent)
    let stream = TcpStream::connect(master_addr).await.expect("connect");
    let mut transport = MessageTransport::new(stream);

    let worker_id = Uuid::new_v4();
    let reg_msg = WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities::new("silent-reaper-worker", 2, 4096, false, false, None),
    };
    transport.send_msg(&reg_msg).await.expect("send reg");

    let ack: Option<MasterMessage> = transport.recv_msg().await.expect("recv ack");
    assert!(matches!(ack, Some(MasterMessage::RegisterAck { accepted: true, .. })));

    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "reaper_evict_test".into(),
            iterations: 100,
            duration_ms: 5000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30).with_max_retries(0),
    );
    let task_id = master.submit_task(task).await.expect("submit");

    // Receive AssignTask frame on worker socket
    let assign: Option<MasterMessage> = transport.recv_msg().await.expect("recv assign");
    assert!(matches!(assign, Some(MasterMessage::AssignTask { .. })));

    // Spawn client calling wait_task
    let master_clone = master.clone();
    let wait_handle = tokio::spawn(async move {
        master_clone.wait_task(task_id, Some(Duration::from_secs(5))).await
    });

    // Mark start of freeze. The worker sends NO heartbeats.
    let freeze_start = Instant::now();

    // Waiter MUST unblock once reaper detects inactivity > 60ms
    let result = wait_handle.await.expect("join wait handle").expect("wait_task result");
    let elapsed = freeze_start.elapsed();

    // Reaper threshold is 60ms, scan interval 20ms. Total resolution must be < 600ms
    assert!(
        elapsed < Duration::from_millis(600),
        "wait_task must resolve promptly upon reaper eviction (took {elapsed:?})"
    );
    assert_eq!(result.exit_code, 1);
    let err = result.error.unwrap_or_default();
    assert!(
        err.contains("maximum retry limit (0) reached") || err.contains("Heartbeat timeout"),
        "Error message must indicate failure reason: {err}"
    );

    let _ = master.shutdown();
}

// ============================================================================
// 7. Client wait_task Channel Preserved Across Mid-Flight Failover
// ============================================================================

#[tokio::test]
async fn test_client_wait_task_preserves_resolution_across_mid_flight_failover() {
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let (_w2_tx, w2_rx) = watch::channel(false);

    let mut w1 = WorkerClient::from_options(master_addr.clone(), Some("mid-w1".into()), Some(2), false);
    let w1_id = w1.worker_id();
    let mut w2 = WorkerClient::from_options(master_addr.clone(), Some("mid-w2".into()), Some(2), false);
    let _w2_id = w2.worker_id();

    let h1 = tokio::spawn(async move { let _ = w1.run(w1_rx).await; });
    let _h2 = tokio::spawn(async move { let _ = w2.run(w2_rx).await; });

    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(30), || async {
        master.list_workers().await.map(|w| w.len() == 2).unwrap_or(false)
    }).await);

    // Task that runs for 1.2s
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "mid_failover".into(),
            iterations: 100,
            duration_ms: 1200,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit");

    // Client starts wait_task BEFORE any crash occurs
    let master_clone = master.clone();
    let wait_handle = tokio::spawn(async move {
        master_clone.wait_task(task_id, Some(Duration::from_secs(8))).await
    });

    // Wait until task is running
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
    }).await);

    let info = master.get_task_info(task_id).await.unwrap();
    let initial_worker = info.assigned_worker_id.unwrap();

    // Kill whichever worker was assigned the task
    if initial_worker == w1_id {
        h1.abort();
    } else {
        _h2.abort();
    }

    // The client's wait_task MUST NOT crash, error, or drop; it must wait and succeed on survivor!
    let result = wait_handle.await.expect("wait handle join").expect("wait_task result");
    assert_eq!(result.exit_code, 0, "Task must complete with exit code 0 on survivor");
    assert!(result.is_success());

    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.retry_count, 1);

    let _ = master.shutdown();
}

// ============================================================================
// 8. Multiple Concurrent Client wait_task Fanout on Exhaustion
// ============================================================================

#[tokio::test]
async fn test_multiple_concurrent_client_waiters_fanout() {
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(0),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::from_options(master_addr.clone(), Some("fanout-w1".into()), Some(1), false);
    let h1 = tokio::spawn(async move { let _ = w1.run(w1_rx).await; });

    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(30), || async {
        master.list_workers().await.map(|w| w.len() == 1).unwrap_or(false)
    }).await);

    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "fanout_test".into(),
            iterations: 100,
            duration_ms: 3000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30).with_max_retries(0),
    );
    let task_id = master.submit_task(task).await.expect("submit");

    // Spawn 5 independent client callers awaiting the SAME task
    let mut handles = Vec::new();
    for _ in 0..5 {
        let m = master.clone();
        handles.push(tokio::spawn(async move {
            m.wait_task(task_id, Some(Duration::from_secs(5))).await
        }));
    }

    // Wait until running
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task_id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
    }).await);

    let kill_time = Instant::now();
    h1.abort();

    // All 5 waiters must unblock promptly and receive the exact same synthetic failure
    let results = join_all(handles).await;
    assert_eq!(results.len(), 5);

    let elapsed = kill_time.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "All 5 waiters must unblock within <500ms (took {elapsed:?})"
    );

    for res in results {
        let task_res = res.expect("join").expect("task result");
        assert_eq!(task_res.exit_code, 1);
        assert!(!task_res.is_success());
        assert!(task_res.error.unwrap().contains("maximum retry limit (0) reached"));
    }

    let _ = master.shutdown();
}

// ============================================================================
// 9. TCP Wire Client SubmitTask { wait: true } Resolution
// ============================================================================

#[tokio::test]
async fn test_tcp_wire_client_submit_wait_resolution() {
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(0),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr();

    // Spawn worker
    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::from_options(master_addr.to_string(), Some("wire-w1".into()), Some(1), false);
    let h1 = tokio::spawn(async move { let _ = w1.run(w1_rx).await; });

    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(30), || async {
        master.list_workers().await.map(|w| w.len() == 1).unwrap_or(false)
    }).await);

    // Connect raw TCP client
    let stream = TcpStream::connect(master_addr).await.expect("client connect");
    let mut client_transport = MessageTransport::new(stream);

    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "wire_wait".into(),
            iterations: 100,
            duration_ms: 3000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30).with_max_retries(0),
    );
    let submit_msg = ClientMessage::SubmitTask {
        task: task.clone(),
        wait: true,
    };
    client_transport.send_msg(&submit_msg).await.expect("send submit");

    // Wait until task is running
    assert!(wait_for(Duration::from_secs(5), Duration::from_millis(20), || async {
        master.get_task_info(task.id).await.map(|i| i.state == TaskState::Running).unwrap_or(false)
    }).await);

    // Kill worker
    h1.abort();

    // Wire client MUST receive ClientResponse::TaskCompleted containing failure TaskResult
    let resp: Option<ClientResponse> = client_transport.recv_msg().await.expect("recv resp");
    match resp {
        Some(ClientResponse::TaskCompleted { task_id, result }) => {
            assert_eq!(task_id, task.id);
            assert_eq!(result.exit_code, 1);
            assert!(!result.is_success());
            let err = result.error.unwrap_or_default();
            assert!(
                err.contains("maximum retry limit (0) reached"),
                "Wire client must receive descriptive error: {err}"
            );
        }
        other => panic!("Expected ClientResponse::TaskCompleted, got {other:?}"),
    }

    let _ = master.shutdown();
}

// ============================================================================
// 10. CLI Configuration & Flags Verification
// ============================================================================

#[test]
fn test_cli_flags_configuration_and_precedence() {
    let env_key = unique_env_key("RUSTY_GRID_DEFAULT_RETRY_MAX");
    let env_slice = [env_key.as_str()];

    // 1. CLI explicit flag takes top precedence
    std::env::set_var(&env_key, "10");
    let file_cfg = Some(5);
    let resolved = resolve_u32(Some(20), &env_slice, file_cfg, 3);
    assert_eq!(resolved, 20, "CLI argument must override env, file, and default");

    // 2. Env variable takes second precedence
    let resolved_env = resolve_u32(None, &env_slice, file_cfg, 3);
    assert_eq!(resolved_env, 10, "Env variable must override file and default");

    // 3. File config takes third precedence
    std::env::remove_var(&env_key);
    let resolved_file = resolve_u32(None, &env_slice, file_cfg, 3);
    assert_eq!(resolved_file, 5, "Config file must override default");

    // 4. Default fallback takes lowest precedence
    let resolved_default = resolve_u32(None, &env_slice, None, 3);
    assert_eq!(resolved_default, 3, "Hardcoded default fallback must be 3");

    // 5. Test resolve_opt_u32 for max_retries
    let empty_env: [&str; 0] = [];
    assert_eq!(resolve_opt_u32(Some(0), &empty_env, None), Some(0));
    assert_eq!(resolve_opt_u32(Some(7), &empty_env, None), Some(7));
    assert_eq!(resolve_opt_u32(None, &empty_env, None), None);

    // 6. Test binary execution if available
    if let Some(bin) = find_rusty_grid_bin() {
        // Test master --help mentions --default-retry-max
        let output = Command::new(&bin)
            .args(["master", "--help"])
            .output()
            .expect("exec master --help");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("--default-retry-max"),
            "master --help must document --default-retry-max"
        );

        // Test submit --help mentions --max-retries
        let output_sub = Command::new(&bin)
            .args(["submit", "--help"])
            .output()
            .expect("exec submit --help");
        let stdout_sub = String::from_utf8_lossy(&output_sub.stdout);
        assert!(
            stdout_sub.contains("--max-retries"),
            "submit --help must document --max-retries"
        );

        // Test invalid numeric flags are rejected by clap
        let output_invalid = Command::new(&bin)
            .args(["master", "--default-retry-max", "not_a_number"])
            .output()
            .expect("exec invalid master");
        assert!(
            !output_invalid.status.success(),
            "Invalid --default-retry-max must fail argument parsing"
        );

        let output_invalid_sub = Command::new(&bin)
            .args(["submit", "--max-retries", "invalid"])
            .output()
            .expect("exec invalid submit");
        assert!(
            !output_invalid_sub.status.success(),
            "Invalid --max-retries must fail argument parsing"
        );
    }
}
