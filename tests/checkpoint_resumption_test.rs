//! Integration test suite verifying Requirement R3: Fault-Tolerant Mid-Task Checkpointing & Delta Resumption.
//!
//! Validates:
//! 1. Protocol serialization roundtrip: JSON & Bincode encoding/decoding of `WorkerMessage::Checkpoint`
//!    and `MasterMessage::AssignTaskWithCheckpoint` across both typed and wire codecs.
//! 2. Master TaskQueue checkpoint retention: Monotonic sequence enforcement and strict checkpoint
//!    retention across worker eviction, failure, and rescheduling.
//! 3. End-to-end mid-task checkpointing & resumption on worker disconnect:
//!    - Worker 1 starts an iterative compute task (100 iterations).
//!    - Worker 1 emits a checkpoint snapshot at iteration 50 with state delta.
//!    - Master receives and records the checkpoint in `TaskQueue`.
//!    - Worker 1 is abruptly terminated mid-flight (simulated crash / connection drop).
//!    - Master detects failure, re-enqueues task with checkpoint preserved.
//!    - Worker 2 connects and receives `AssignTaskWithCheckpoint`.
//!    - Worker 2 restores state from iteration 50 (does NOT restart from 0) and completes 51..100.
//!    - Result verification confirms 100% data integrity and matching cryptographic/hash digest.

use std::time::{Duration, Instant};

use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{
    deserialize_message, serialize_message, MasterMessage, WireCodec, WorkerMessage,
};
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

/// Computes reference hash digest for a given task ID and iteration count using the same formula
/// implemented by the runner.
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
// Test 1: Protocol Serialization Roundtrip
// ==============================================================================================

#[test]
fn test_checkpoint_serialization_roundtrip() {
    let task_id = TaskId::new();
    let delta = TaskBytes::copy_from_slice(b"sample_incremental_state_vector_42");

    // 1. WorkerMessage::Checkpoint roundtrip via JSON and Bincode wire codecs
    let cp_msg = WorkerMessage::Checkpoint {
        task_id,
        sequence: 42,
        delta_state: delta.clone(),
    };

    // JSON wire codec
    let json_bytes =
        serialize_message(&cp_msg, WireCodec::Json).expect("serialize cp_msg to json");
    let (deser_json, codec_json): (WorkerMessage, WireCodec) =
        deserialize_message(&json_bytes).expect("deserialize cp_msg from json");
    assert_eq!(codec_json, WireCodec::Json);
    assert_eq!(deser_json, cp_msg);

    // Bincode wire codec
    let bincode_bytes =
        serialize_message(&cp_msg, WireCodec::Bincode).expect("serialize cp_msg to bincode");
    let (deser_bincode, codec_bin): (WorkerMessage, WireCodec) =
        deserialize_message(&bincode_bytes).expect("deserialize cp_msg from bincode");
    assert_eq!(codec_bin, WireCodec::Bincode);
    assert_eq!(deser_bincode, cp_msg);

    // Direct serde_json and bincode
    let raw_json = serde_json::to_string(&cp_msg).expect("raw serde_json string");
    let raw_deser_json: WorkerMessage =
        serde_json::from_str(&raw_json).expect("raw serde_json deser");
    assert_eq!(raw_deser_json, cp_msg);

    let raw_bin = bincode::serialize(&cp_msg).expect("raw bincode serialize");
    let raw_deser_bin: WorkerMessage =
        bincode::deserialize(&raw_bin).expect("raw bincode deserialize");
    assert_eq!(raw_deser_bin, cp_msg);

    // 2. MasterMessage::AssignTaskWithCheckpoint roundtrip (with Some checkpoint)
    let cp_data = CheckpointData::new(7, delta);
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "checkpoint".into(),
            iterations: 100,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    )
    .with_checkpoint(cp_data.clone());

    let assign_msg = MasterMessage::AssignTaskWithCheckpoint {
        task: task.clone(),
        latest_checkpoint: Some(cp_data.clone()),
    };

    // JSON wire codec
    let assign_json_bytes =
        serialize_message(&assign_msg, WireCodec::Json).expect("serialize assign_msg to json");
    let (deser_assign_json, _): (MasterMessage, WireCodec) =
        deserialize_message(&assign_json_bytes).expect("deserialize assign_msg from json");
    assert_eq!(deser_assign_json, assign_msg);

    // Bincode wire codec
    let assign_bin_bytes = serialize_message(&assign_msg, WireCodec::Bincode)
        .expect("serialize assign_msg to bincode");
    let (deser_assign_bin, _): (MasterMessage, WireCodec) =
        deserialize_message(&assign_bin_bytes).expect("deserialize assign_msg from bincode");
    assert_eq!(deser_assign_bin, assign_msg);

    // 3. MasterMessage::AssignTaskWithCheckpoint roundtrip (with None checkpoint)
    let assign_none_msg = MasterMessage::AssignTaskWithCheckpoint {
        task: task.clone(),
        latest_checkpoint: None,
    };
    let assign_none_bytes = serialize_message(&assign_none_msg, WireCodec::Bincode)
        .expect("serialize assign_none to bincode");
    let (deser_assign_none, _): (MasterMessage, WireCodec) =
        deserialize_message(&assign_none_bytes).expect("deserialize assign_none from bincode");
    assert_eq!(deser_assign_none, assign_none_msg);

    // 4. CheckpointData standalone roundtrip
    let cp_json = serde_json::to_string(&cp_data).expect("cp_data to json");
    let cp_deser: CheckpointData = serde_json::from_str(&cp_json).expect("cp_data from json");
    assert_eq!(cp_deser, cp_data);
}

// ==============================================================================================
// Test 2: TaskQueue Checkpoint Retention Across Reassignment & Monotonic Ordering
// ==============================================================================================

#[tokio::test]
async fn test_task_queue_checkpoint_retention_across_reassignment() {
    let mut policy = RetryPolicy::default();
    policy.max_retries = 3;
    let queue = TaskQueue::with_config(policy, None);

    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "iterative_checkpoint".into(),
            iterations: 200,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );

    let task_id = queue.submit(task).await.expect("submit task");
    let worker_1 = Uuid::new_v4();
    let worker_2 = Uuid::new_v4();

    // 1. Initial schedule to Worker 1
    let scheduled_w1 = queue
        .schedule_task(&task_id, worker_1)
        .await
        .expect("schedule to worker 1");
    assert_eq!(scheduled_w1.latest_checkpoint, None);
    queue
        .mark_running(&task_id, worker_1)
        .await
        .expect("mark running");

    // 2. Worker 1 records Checkpoint 1 (sequence 1)
    let cp1 = CheckpointData::new(1, TaskBytes::copy_from_slice(b"delta_iter_50"));
    queue
        .record_checkpoint(task_id, cp1.clone())
        .await
        .expect("record checkpoint 1");
    assert_eq!(queue.get_checkpoint(&task_id).await, Some(cp1.clone()));

    // 3. Worker 1 records Checkpoint 2 (sequence 2)
    let cp2 = CheckpointData::new(2, TaskBytes::copy_from_slice(b"delta_iter_100"));
    queue
        .record_checkpoint(task_id, cp2.clone())
        .await
        .expect("record checkpoint 2");
    assert_eq!(queue.get_checkpoint(&task_id).await, Some(cp2.clone()));

    // 4. Monotonic enforcement: out-of-order/stale checkpoint (sequence 1) must be rejected
    let cp_stale = CheckpointData::new(1, TaskBytes::copy_from_slice(b"stale_delta_iter_50"));
    queue
        .record_checkpoint(task_id, cp_stale)
        .await
        .expect("stale checkpoint call should succeed without panic");
    // Verify latest_checkpoint remained cp2 (sequence 2)
    assert_eq!(queue.get_checkpoint(&task_id).await, Some(cp2.clone()));

    // 5. Worker 1 crashes / abruptly disconnects
    let affected = queue
        .handle_worker_disconnected(&worker_1, "Unexpected socket drop", true)
        .await;
    assert_eq!(affected, vec![task_id]);

    // Verify task state transitioned back to Queued with retry_count == 1
    let task_info = queue.get_task(&task_id).await.expect("task info exists");
    assert_eq!(task_info.state, TaskState::Queued);
    assert_eq!(task_info.retry_count, 1);
    assert_eq!(task_info.assigned_worker_id, None);

    // Verify checkpoint is strictly preserved in queue
    let preserved_cp = queue.get_checkpoint(&task_id).await;
    assert_eq!(preserved_cp, Some(cp2.clone()));

    // 6. Schedule task to Worker 2 (reassignment)
    let scheduled_w2 = queue
        .schedule_task(&task_id, worker_2)
        .await
        .expect("schedule to worker 2");
    // Worker 2 receives task with latest_checkpoint intact!
    assert_eq!(scheduled_w2.latest_checkpoint, Some(cp2));
    queue
        .mark_running(&task_id, worker_2)
        .await
        .expect("mark running w2");

    // 7. Worker 2 advances computation and records Checkpoint 3 (sequence 3)
    let cp3 = CheckpointData::new(3, TaskBytes::copy_from_slice(b"delta_iter_150"));
    queue
        .record_checkpoint(task_id, cp3.clone())
        .await
        .expect("record checkpoint 3");
    assert_eq!(queue.get_checkpoint(&task_id).await, Some(cp3.clone()));

    // 8. Worker 2 completes execution successfully
    let final_result = TaskResult::success(
        worker_2,
        task_id,
        "Iterative computation completed successfully\n",
        150,
        false,
    );
    let outcome = queue
        .record_result(final_result)
        .await
        .expect("record result");
    assert_eq!(outcome, TaskState::Completed);

    let final_info = queue.get_task(&task_id).await.expect("final task info");
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.retry_count, 1);
    assert_eq!(final_info.exit_code, Some(0));
}

// ==============================================================================================
// Test 3: Task Requeue Explicitly Preserves Latest Checkpoint
// ==============================================================================================

#[tokio::test]
async fn test_task_requeue_preserves_checkpoint() {
    let queue = TaskQueue::new();

    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "test".into(),
            iterations: 50,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );

    let task_id = queue.submit(task).await.expect("submit");
    let w1 = Uuid::new_v4();
    let _ = queue.schedule_task(&task_id, w1).await.expect("scheduled");

    let cp = CheckpointData::new(1, TaskBytes::copy_from_slice(b"requeue_checkpoint_data"));
    queue
        .record_checkpoint(task_id, cp.clone())
        .await
        .expect("record checkpoint");

    // Explicit manual requeue
    queue.requeue_task(&task_id).await.expect("requeue");

    assert_eq!(queue.get_checkpoint(&task_id).await, Some(cp.clone()));

    let w2 = Uuid::new_v4();
    let rescheduled = queue
        .schedule_task(&task_id, w2)
        .await
        .expect("rescheduled");
    assert_eq!(rescheduled.latest_checkpoint, Some(cp));
}

// ==============================================================================================
// Test 4: End-to-End Mid-Task Checkpointing and Delta Resumption on Worker Disconnect
// ==============================================================================================

#[tokio::test]
async fn test_end_to_end_mid_task_checkpoint_and_resumption_on_worker_disconnect() {
    // 1. Spawn Master server with max_retries = 3
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let master = MasterServer::spawn(config).await.expect("master spawn");
    let master_addr = master.server_addr().to_string();

    // 2. Connect Worker 1
    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let mut w1 =
        WorkerClient::from_options(master_addr.clone(), Some("checkpoint-w1".into()), Some(1), false);
    let w1_id = w1.worker_id();

    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });

    // Wait for Worker 1 to register
    let w1_registered = wait_for(Duration::from_secs(5), Duration::from_millis(50), || async {
        if let Ok(workers) = master.list_workers().await {
            workers.iter().any(|w| w.worker_id == w1_id)
        } else {
            false
        }
    })
    .await;
    assert!(w1_registered, "Worker 1 must be registered");

    // 3. Submit iterative task: 100 iterations, with duration_ms = 1500 (emits checkpoint at 50, then sleeps)
    let iters = 100u32;
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "checkpoint".into(),
            iterations: iters,
            duration_ms: 1500,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let reference_digest = compute_reference_digest(&task.id, iters);

    let task_id = master.submit_task(task).await.expect("submit task");

    // 4. Wait for Master to receive and record the mid-task checkpoint emitted at iteration 50
    let checkpoint_received = wait_for(
        Duration::from_secs(8),
        Duration::from_millis(50),
        || async {
            if let Some(cp) = master.get_checkpoint(&task_id).await {
                cp.sequence >= 1 && cp.delta_state.len() >= 16
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        checkpoint_received,
        "Master must record mid-task checkpoint emitted by Worker 1"
    );

    let cp_before_kill = master.get_checkpoint(&task_id).await.unwrap();
    assert_eq!(cp_before_kill.sequence, 1);
    assert_eq!(cp_before_kill.delta_state.len(), 16);

    // Verify delta state contains iteration 50
    let slice = cp_before_kill.delta_state.as_slice();
    let iter_bytes: [u8; 8] = slice[0..8].try_into().unwrap();
    let start_iter = u64::from_le_bytes(iter_bytes) as u32;
    assert_eq!(start_iter, 50, "Checkpoint delta state must record 50 iterations");

    // 5. Abruptly kill Worker 1 (abort tokio task, abruptly dropping the TCP connection mid-computation)
    w1_handle.abort();

    // 6. Master detects disconnect and returns task to queue with checkpoint intact
    let task_requeued = wait_for(Duration::from_secs(6), Duration::from_millis(50), || async {
        if let Ok(info) = master.get_task_info(task_id).await {
            info.state == TaskState::Queued || info.state == TaskState::Retrying
        } else {
            false
        }
    })
    .await;
    assert!(task_requeued, "Task must return to Queued/Retrying state after Worker 1 termination");

    let cp_after_kill = master.get_checkpoint(&task_id).await;
    assert_eq!(
        cp_after_kill,
        Some(cp_before_kill),
        "Latest checkpoint MUST be strictly preserved in Master queue after worker disconnect"
    );

    // 7. Spawn Worker 2 to resume execution
    let (_w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);
    let mut w2 =
        WorkerClient::from_options(master_addr.clone(), Some("checkpoint-w2".into()), Some(1), false);
    let w2_id = w2.worker_id();

    let _w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });

    // 8. Wait for task to complete on Worker 2
    let task_completed = wait_for(
        Duration::from_secs(12),
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
        task_completed,
        "Task must complete on Worker 2 via delta resumption"
    );

    // 9. Verify task execution outcome and data integrity
    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.retry_count, 1, "Task must record exactly 1 retry due to failover");
    assert_eq!(final_info.exit_code, Some(0));
    assert_eq!(final_info.assigned_worker_id, Some(w2_id));

    // Retrieve full task result
    let final_result = master
        .wait_task(task_id, Some(Duration::from_secs(2)))
        .await
        .expect("fetch task result");
    assert_eq!(final_result.exit_code, 0);
    assert_eq!(final_result.worker_id, w2_id);

    let stdout_str = final_result.stdout.as_str();
    assert!(
        stdout_str.contains("resumed from 50"),
        "Stdout must explicitly show delta resumption from checkpoint 50; got: {stdout_str}"
    );

    // Parse the digest from stdout
    let expected_digest_str = format!("{reference_digest:016x}");
    assert!(
        stdout_str.contains(&expected_digest_str),
        "Final hash digest from resumed execution must match reference oracle ({expected_digest_str}); got: {stdout_str}"
    );

    let _ = w1_shutdown_tx.send(true);
    let _ = master.shutdown();
}

// ==============================================================================================
// Test 5: Baseline Verification (Uninterrupted vs Resumed Digest Equivalence)
// ==============================================================================================

#[tokio::test]
async fn test_runner_delta_resumption_digest_equivalence() {
    let runner = TaskRunner::new(
        Uuid::new_v4(),
        WorkerCapabilities::new("test-runner", 4, 8192, false, false, None),
        RunnerConfig::default(),
    );

    let task_id = TaskId::new();
    let iters = 100u32;
    let reference_digest = compute_reference_digest(&task_id, iters);

    // 1. Run full 100 iterations without checkpoint
    let full_task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "checkpoint".into(),
            iterations: iters,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let mut full_task = full_task;
    full_task.id = task_id;

    let full_res = runner.execute_task(&full_task, None, None).await;
    assert!(full_res.is_success());
    let full_out = full_res.stdout.as_str();
    assert!(!full_out.contains("resumed"));
    assert!(full_out.contains(&format!("{reference_digest:016x}")));

    // 2. Prepare checkpoint at iteration 50 with exact state
    let mut state: u64 = 0xcbf29ce484222325;
    for b in task_id.as_uuid().as_bytes() {
        state ^= *b as u64;
        state = state.wrapping_mul(0x100000001b3);
    }
    for i in 0..50 {
        state ^= i as u64;
        state = state.wrapping_mul(0x100000001b3);
        state = state.rotate_left(13);
    }

    let mut delta = Vec::with_capacity(16);
    delta.extend_from_slice(&(50u64).to_le_bytes());
    delta.extend_from_slice(&state.to_le_bytes());

    let checkpoint = CheckpointData::new(1, TaskBytes::from(delta));
    let mut resumed_task = full_task.clone();
    resumed_task.latest_checkpoint = Some(checkpoint.clone());

    // 3. Resume from iteration 50
    let resumed_res = runner
        .execute_task_with_checkpoint(&resumed_task, Some(&checkpoint), None, None, None)
        .await;
    assert!(resumed_res.is_success());
    let resumed_out = resumed_res.stdout.as_str();
    assert!(resumed_out.contains("resumed from 50"));
    assert!(
        resumed_out.contains(&format!("{reference_digest:016x}")),
        "Resumed output must produce the EXACT same digest as the full uninterrupted run!"
    );
}
