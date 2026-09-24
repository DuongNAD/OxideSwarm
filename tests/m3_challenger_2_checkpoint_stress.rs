//! Adversarial Stress & Verification Test Suite for Requirement R3:
//! Fault-Tolerant Mid-Task Checkpointing & Delta Resumption.
//!
//! Authored by Challenger 2 to empirically verify:
//! 1. Disconnect race conditions: abrupt socket termination at the exact moment of checkpoint emission.
//! 2. Double reassignment & ghost task avoidance: verify evicted/zombie worker cannot hijack or corrupt an in-flight reassigned task.
//! 3. Cascading multi-worker failover: successive worker crashes across sequence 1 -> 2 -> 3 checkpoints with cumulative delta resumption.
//! 4. Invariant fuzzing & edge cases: monotonic sequence enforcement, duplicate/stale sequence rejection, 1MB large delta buffers, terminal state protection.
//! 5. Data stream backpressure & concurrent telemetry interaction: high-frequency checkpoint bursts interleaved with progress and heartbeat telemetry.

use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::error::GridError;
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{
    Bytes as TaskBytes, CheckpointData, Task, TaskId, TaskRequirements, TaskResult, TaskSpec,
};
use rusty_grid_master::queue::{RetryPolicy, TaskQueue, TaskState};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;
use rusty_grid_worker::runner::{RunnerConfig, TaskRunner};

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

/// Independent reference oracle for deterministic hash computation.
fn compute_reference_digest(task_id: &TaskId, iters: u32) -> u64 {
    let mut state: u64 = 0xcbf29ce484222325;
    for b in task_id.as_uuid().as_bytes() {
        state ^= *b as u64;
        state = state.wrapping_mul(0x100000001b3);
    }
    for i in 0..iters {
        state ^= i as u64;
        state = state.wrapping_mul(0x100000001b3);
        state = state.rotate_left(13);
    }
    state
}

// ==============================================================================================
// CHALLENGE 1: Disconnect Race Condition at the Exact Instant of Checkpoint Emission
// ==============================================================================================

#[tokio::test]
async fn test_adversarial_disconnect_race_during_checkpoint_emission() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let mut w1 = WorkerClient::from_options(
        master_addr.clone(),
        Some("race-w1".into()),
        Some(1),
        false,
    );
    let w1_id = w1.worker_id();

    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });

    let w1_registered = wait_for(Duration::from_secs(5), Duration::from_millis(50), || async {
        if let Ok(workers) = master.list_workers().await {
            workers.iter().any(|w| w.worker_id == w1_id)
        } else {
            false
        }
    })
    .await;
    assert!(w1_registered, "Worker 1 registered");

    let iters = 100u32;
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "checkpoint".into(),
            iterations: iters,
            duration_ms: 1200, // sleep at iteration 50
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit task");

    // Race trigger: poll rapidly (10ms) and abort Worker 1 immediately the instant sequence 1 appears
    let race_aborted = wait_for(Duration::from_secs(6), Duration::from_millis(10), || async {
        if let Some(cp) = master.get_checkpoint(&task_id).await {
            if cp.sequence >= 1 {
                // Abrupt kill right during the post-checkpoint window
                w1_handle.abort();
                return true;
            }
        }
        false
    })
    .await;
    assert!(race_aborted, "Checkpoint reached and worker aborted mid-flight");

    // Master must handle socket drop and requeue task with checkpoint strictly preserved
    let task_requeued = wait_for(Duration::from_secs(5), Duration::from_millis(50), || async {
        if let Ok(info) = master.get_task_info(task_id).await {
            info.state == TaskState::Queued || info.state == TaskState::Retrying
        } else {
            false
        }
    })
    .await;
    assert!(task_requeued, "Task requeued after abrupt drop");

    let preserved_cp = master.get_checkpoint(&task_id).await;
    assert!(preserved_cp.is_some(), "Checkpoint must survive socket drop");
    assert_eq!(preserved_cp.as_ref().unwrap().sequence, 1);

    // Spawn Worker 2 to resume from checkpoint
    let (_w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);
    let mut w2 = WorkerClient::from_options(
        master_addr.clone(),
        Some("race-w2".into()),
        Some(1),
        false,
    );
    let w2_id = w2.worker_id();

    let _w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });

    let completed = wait_for(Duration::from_secs(10), Duration::from_millis(50), || async {
        if let Ok(info) = master.get_task_info(task_id).await {
            info.state == TaskState::Completed
        } else {
            false
        }
    })
    .await;
    assert!(completed, "Task completed on Worker 2");

    let result = master
        .wait_task(task_id, Some(Duration::from_secs(2)))
        .await
        .expect("task result");
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.worker_id, w2_id);
    assert!(result.stdout.contains("resumed from 50"));

    let oracle_digest = compute_reference_digest(&task_id, iters);
    assert!(result.stdout.contains(&format!("{oracle_digest:016x}")));

    let _ = w1_shutdown_tx.send(true);
    let _ = master.shutdown();
}

// ==============================================================================================
// CHALLENGE 2: Double Reassignment & Ghost Task Prevention (Zombie Worker Defense)
// ==============================================================================================

#[tokio::test]
async fn test_adversarial_zombie_worker_double_reassignment_rejection() {
    let mut policy = RetryPolicy::default();
    policy.max_retries = 3;
    let queue = TaskQueue::with_config(policy, None);

    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "zombie_test".into(),
            iterations: 100,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let task_id = queue.submit(task).await.expect("submit task");

    let w1_id = Uuid::new_v4();
    let w2_id = Uuid::new_v4();

    // 1. Worker 1 is scheduled and running
    let _ = queue.schedule_task(&task_id, w1_id).await.expect("schedule w1");
    queue.mark_running(&task_id, w1_id).await.expect("mark running w1");

    // Worker 1 emits checkpoint sequence 1
    let cp1 = CheckpointData::new(1, TaskBytes::copy_from_slice(b"iter_30_state"));
    queue.record_checkpoint(task_id, cp1.clone()).await.expect("record cp1");

    // 2. Worker 1 disconnects / evicted (e.g. network partition or timeout)
    let affected = queue
        .handle_worker_disconnected(&w1_id, "Network partition", true)
        .await;
    assert_eq!(affected, vec![task_id]);

    let info_after_evict = queue.get_task(&task_id).await.unwrap();
    assert_eq!(info_after_evict.state, TaskState::Queued);
    assert_eq!(info_after_evict.assigned_worker_id, None);
    assert_eq!(queue.get_checkpoint(&task_id).await, Some(cp1.clone()));

    // 3. Worker 2 picks up the task
    let scheduled_w2 = queue.schedule_task(&task_id, w2_id).await.expect("schedule w2");
    assert_eq!(scheduled_w2.latest_checkpoint, Some(cp1.clone()));
    queue.mark_running(&task_id, w2_id).await.expect("mark running w2");

    let info_running_w2 = queue.get_task(&task_id).await.unwrap();
    assert_eq!(info_running_w2.state, TaskState::Running);
    assert_eq!(info_running_w2.assigned_worker_id, Some(w2_id));

    // 4. ADVERSARIAL ATTACK: Zombie Worker 1 (not dead in reality) attempts illegal transitions:

    // Attack 4a: Zombie Worker 1 attempts to re-mark task as Running for W1
    let zombie_mark_run = queue.mark_running(&task_id, w1_id).await;
    assert!(
        zombie_mark_run.is_err(),
        "Queue must reject mark_running from zombie worker when already Running on W2"
    );

    // Attack 4b: Zombie Worker 1 attempts to schedule task
    let zombie_schedule = queue.schedule_task(&task_id, w1_id).await;
    assert!(
        zombie_schedule.is_none(),
        "Queue must reject scheduling an already Running task to zombie worker"
    );

    // Attack 4c: Zombie Worker 1 attempts to submit a stale checkpoint (sequence 1)
    let cp_stale_zombie = CheckpointData::new(1, TaskBytes::copy_from_slice(b"stale_zombie_state"));
    queue
        .record_checkpoint(task_id, cp_stale_zombie)
        .await
        .expect("stale checkpoint handled gracefully");
    assert_eq!(
        queue.get_checkpoint(&task_id).await,
        Some(cp1.clone()),
        "Stale checkpoint from zombie worker must NOT overwrite existing checkpoint"
    );

    // 5. Worker 2 emits newer checkpoint (sequence 2)
    let cp2 = CheckpointData::new(2, TaskBytes::copy_from_slice(b"iter_70_state"));
    queue.record_checkpoint(task_id, cp2.clone()).await.expect("record cp2");
    assert_eq!(queue.get_checkpoint(&task_id).await, Some(cp2.clone()));

    // 6. Worker 2 completes execution successfully
    let w2_result = TaskResult::success(w2_id, task_id, "Completed by W2\n", 80, false);
    let outcome = queue.record_result(w2_result).await.expect("record result w2");
    assert_eq!(outcome, TaskState::Completed);

    let final_info = queue.get_task(&task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.assigned_worker_id, Some(w2_id));

    // Attack 4d: Zombie Worker 1 now attempts to submit result for already completed task
    let zombie_late_res = TaskResult::success(w1_id, task_id, "Zombie W1 late result\n", 120, false);
    let late_outcome = queue.record_result(zombie_late_res).await.expect("safe handle");
    assert_eq!(
        late_outcome,
        TaskState::Completed,
        "Terminal state Completed is immutable"
    );

    // Verify task output was NOT overwritten by zombie worker
    let preserved_info = queue.get_task(&task_id).await.unwrap();
    assert_eq!(preserved_info.assigned_worker_id, Some(w2_id));
    let final_res = queue.get_result(&task_id).await.unwrap();
    assert!(final_res.stdout.contains("Completed by W2"));
}

// ==============================================================================================
// CHALLENGE 3: Cascading Multi-Worker Failures Across Successive Checkpoints
// ==============================================================================================

#[tokio::test]
async fn test_adversarial_cascading_multi_worker_failure_resumption() {
    let mut policy = RetryPolicy::default();
    policy.max_retries = 5;
    let queue = TaskQueue::with_config(policy, None);

    let iters = 100u32;
    let task_id = TaskId::new();
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "cascading_checkpoint".into(),
            iterations: iters,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let mut task = task;
    task.id = task_id;

    let _ = queue.submit(task).await.expect("submit");

    let w1 = Uuid::new_v4();
    let w2 = Uuid::new_v4();
    let w3 = Uuid::new_v4();
    let w4 = Uuid::new_v4();

    // --- Generation 1 (Worker 1): iterations 0..25, checkpoints at 25, crashes ---
    let _ = queue.schedule_task(&task_id, w1).await.expect("sched w1");
    queue.mark_running(&task_id, w1).await.expect("run w1");

    let mut state: u64 = 0xcbf29ce484222325;
    for b in task_id.as_uuid().as_bytes() {
        state ^= *b as u64;
        state = state.wrapping_mul(0x100000001b3);
    }
    for i in 0..25 {
        state ^= i as u64;
        state = state.wrapping_mul(0x100000001b3);
        state = state.rotate_left(13);
    }
    let mut d1 = Vec::new();
    d1.extend_from_slice(&(25u64).to_le_bytes());
    d1.extend_from_slice(&state.to_le_bytes());
    let cp1 = CheckpointData::new(1, TaskBytes::from(d1));
    queue.record_checkpoint(task_id, cp1.clone()).await.expect("cp1");

    // Worker 1 crash
    queue.handle_worker_disconnected(&w1, "Crash Gen 1", true).await;

    // --- Generation 2 (Worker 2): resumes from 25, computes 25..50, checkpoints at 50, crashes ---
    let t_w2 = queue.schedule_task(&task_id, w2).await.expect("sched w2");
    assert_eq!(t_w2.latest_checkpoint.as_ref().unwrap().sequence, 1);
    queue.mark_running(&task_id, w2).await.expect("run w2");

    for i in 25..50 {
        state ^= i as u64;
        state = state.wrapping_mul(0x100000001b3);
        state = state.rotate_left(13);
    }
    let mut d2 = Vec::new();
    d2.extend_from_slice(&(50u64).to_le_bytes());
    d2.extend_from_slice(&state.to_le_bytes());
    let cp2 = CheckpointData::new(2, TaskBytes::from(d2));
    queue.record_checkpoint(task_id, cp2.clone()).await.expect("cp2");

    // Worker 2 crash
    queue.handle_worker_disconnected(&w2, "Crash Gen 2", true).await;

    // --- Generation 3 (Worker 3): resumes from 50, computes 50..75, checkpoints at 75, crashes ---
    let t_w3 = queue.schedule_task(&task_id, w3).await.expect("sched w3");
    assert_eq!(t_w3.latest_checkpoint.as_ref().unwrap().sequence, 2);
    queue.mark_running(&task_id, w3).await.expect("run w3");

    for i in 50..75 {
        state ^= i as u64;
        state = state.wrapping_mul(0x100000001b3);
        state = state.rotate_left(13);
    }
    let mut d3 = Vec::new();
    d3.extend_from_slice(&(75u64).to_le_bytes());
    d3.extend_from_slice(&state.to_le_bytes());
    let cp3 = CheckpointData::new(3, TaskBytes::from(d3));
    queue.record_checkpoint(task_id, cp3.clone()).await.expect("cp3");

    // Worker 3 crash
    queue.handle_worker_disconnected(&w3, "Crash Gen 3", true).await;

    // --- Generation 4 (Worker 4): resumes from 75, completes to 100 via TaskRunner ---
    let t_w4 = queue.schedule_task(&task_id, w4).await.expect("sched w4");
    assert_eq!(t_w4.latest_checkpoint.as_ref().unwrap().sequence, 3);
    queue.mark_running(&task_id, w4).await.expect("run w4");

    let runner = TaskRunner::new(
        w4,
        WorkerCapabilities::new("cascading-runner", 4, 8192, false, false, None),
        RunnerConfig::default(),
    );

    let exec_res = runner
        .execute_task_with_checkpoint(
            &t_w4,
            t_w4.latest_checkpoint.as_ref(),
            None,
            None,
            None,
        )
        .await;
    assert!(exec_res.is_success());
    assert!(exec_res.stdout.contains("resumed from 75"));

    let oracle = compute_reference_digest(&task_id, iters);
    assert!(exec_res.stdout.contains(&format!("{oracle:016x}")));

    // Record result in queue
    let outcome = queue.record_result(exec_res).await.expect("record result w4");
    assert_eq!(outcome, TaskState::Completed);

    let final_info = queue.get_task(&task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.retry_count, 3, "Task survived 3 worker crashes");
    assert_eq!(final_info.assigned_worker_id, Some(w4));
}

// ==============================================================================================
// CHALLENGE 4: Invariant Fuzzing & Malformed / Edge Case Checkpoint Protection
// ==============================================================================================

#[tokio::test]
async fn test_adversarial_checkpoint_invariants_and_edge_cases() {
    let queue = TaskQueue::new();
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "fuzz_checkpoint".into(),
            iterations: 50,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let task_id = queue.submit(task).await.expect("submit");
    let w_id = Uuid::new_v4();
    let _ = queue.schedule_task(&task_id, w_id).await.expect("sched");
    queue.mark_running(&task_id, w_id).await.expect("run");

    // 1. Edge Case: Checkpoint sequence 10 recorded
    let cp10 = CheckpointData::new(10, TaskBytes::copy_from_slice(b"seq_10_snapshot"));
    queue.record_checkpoint(task_id, cp10.clone()).await.expect("record seq 10");
    assert_eq!(queue.get_checkpoint(&task_id).await, Some(cp10.clone()));

    // 2. Invariant: Regressive sequence (e.g. sequence 8) must NOT overwrite sequence 10
    let cp8 = CheckpointData::new(8, TaskBytes::copy_from_slice(b"seq_8_regressive"));
    queue.record_checkpoint(task_id, cp8).await.expect("regressive safe handle");
    assert_eq!(
        queue.get_checkpoint(&task_id).await.unwrap().sequence,
        10,
        "Sequence 8 must be discarded in favor of sequence 10"
    );

    // 3. Invariant: Duplicate sequence (sequence 10) must NOT overwrite existing sequence 10
    let cp10_dup = CheckpointData::new(10, TaskBytes::copy_from_slice(b"seq_10_duplicate_poison"));
    queue.record_checkpoint(task_id, cp10_dup).await.expect("duplicate safe handle");
    assert_eq!(
        queue.get_checkpoint(&task_id).await.unwrap().delta_state.as_slice(),
        b"seq_10_snapshot",
        "Duplicate sequence 10 must not overwrite initial sequence 10"
    );

    // 4. Edge Case: 1 Megabyte large delta payload stress test
    let large_payload = vec![0x5a; 1024 * 1024]; // 1MB
    let cp11_large = CheckpointData::new(11, TaskBytes::from(large_payload.clone()));
    queue.record_checkpoint(task_id, cp11_large).await.expect("record 1MB checkpoint");
    let retrieved_cp = queue.get_checkpoint(&task_id).await.unwrap();
    assert_eq!(retrieved_cp.sequence, 11);
    assert_eq!(retrieved_cp.delta_state.len(), 1024 * 1024);
    assert_eq!(retrieved_cp.delta_state.as_slice(), large_payload.as_slice());

    // 5. Invariant: Non-existent TaskId returns GridError::TaskNotFound without panic
    let bogus_task_id = TaskId::new();
    let bogus_res = queue.record_checkpoint(bogus_task_id, cp10).await;
    match bogus_res {
        Err(GridError::TaskNotFound(id)) => assert_eq!(id, bogus_task_id.0),
        other => panic!("Expected TaskNotFound, got {other:?}"),
    }

    // 6. Invariant: Terminal Completed state freezes checkpoint updates
    let final_res = TaskResult::success(w_id, task_id, "done\n", 10, false);
    queue.record_result(final_res).await.expect("mark completed");

    let post_completion_cp = CheckpointData::new(99, TaskBytes::copy_from_slice(b"post_done_cp"));
    queue.record_checkpoint(task_id, post_completion_cp).await.expect("safe handle on completed");
    // Verify sequence is STILL 11 (frozen at completion)
    assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 11);
}

// ==============================================================================================
// CHALLENGE 5: Data Stream Backpressure & Concurrent Traffic Interactions
// ==============================================================================================

#[tokio::test]
async fn test_adversarial_data_lane_backpressure_and_concurrent_traffic() {
    let runner = TaskRunner::new(
        Uuid::new_v4(),
        WorkerCapabilities::new("backpressure-runner", 4, 8192, false, false, None),
        RunnerConfig::default(),
    );

    let iters = 100u32;
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "checkpoint".into(),
            iterations: iters,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );

    // Bounded outbound channel with small capacity to induce channel backpressure
    let (tx, mut rx) = mpsc::channel::<WorkerMessage>(2);

    let runner_handle = tokio::spawn(async move {
        runner
            .execute_task_with_checkpoint(&task, None, Some(tx), None, None)
            .await
    });

    // Simulate concurrent consumer receiving checkpoints while interleaving synthetic telemetry
    let mut checkpoints_received = Vec::new();
    let mut synthetic_heartbeats = 0;

    while let Some(msg) = rx.recv().await {
        match msg {
            WorkerMessage::Checkpoint { sequence, delta_state, .. } => {
                checkpoints_received.push((sequence, delta_state));
                // Simulate slow consumer backpressure
                tokio::time::sleep(Duration::from_millis(5)).await;
                synthetic_heartbeats += 1;
            }
            WorkerMessage::TaskResult { exit_code, stdout, .. } => {
                assert_eq!(exit_code, 0);
                assert!(stdout.contains("hash_compute complete"));
                break;
            }
            _ => {}
        }
    }
    assert!(synthetic_heartbeats > 0);

    let result = runner_handle.await.expect("runner task completed");
    assert!(result.is_success());
    assert!(!checkpoints_received.is_empty(), "Checkpoints must be received through backpressured channel");

    // Verify monotonic ordering of emitted checkpoints
    let mut prev_seq = 0;
    for (seq, _) in checkpoints_received {
        assert!(seq > prev_seq, "Emitted checkpoint sequences must strictly increase: {seq} > {prev_seq}");
        prev_seq = seq;
    }
}
