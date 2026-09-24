//! Adversarial Stress & Verification Test Suite for Milestone M3 (Requirement R3):
//! Fault-Tolerant Mid-Task Checkpointing & Delta Resumption.
//!
//! Authored by Challenger 1 to empirically challenge and verify:
//! 1. Serialization / Deserialization stress under zero-length, multi-megabyte (1MB, 8MB, 32MB),
//!    and frame-boundary (>64MB) delta payloads.
//! 2. Malformed, corrupted, truncated JSON and Bincode frames gracefully rejected without panics.
//! 3. TaskRunner resilience against corrupted, empty, JSON-formatted, and out-of-bounds delta states.
//! 4. TaskQueue monotonic sequence enforcement: stale sequence rejection, duplicate rejection,
//!    u64::MAX boundary sequences, and high-concurrency 100-task race conditions.
//! 5. Terminal task state and nonexistent task checkpoint handling.
//! 6. Checkpoint state retention across retry exhaustion (transitioning to Failed).
//! 7. Multi-hop failover: Worker 1 -> Worker 2 -> Worker 3 across TaskQueue, TaskRunner oracle,
//!    and live MasterServer TCP cluster.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::error::GridError;
use rusty_grid_core::protocol::{
    deserialize_message, serialize_message, MasterMessage, MessageTransport, ProtocolError,
    WireCodec, WorkerMessage, MAX_FRAME_SIZE,
};
use rusty_grid_core::task::{
    Bytes as TaskBytes, CheckpointData, Task, TaskId, TaskRequirements, TaskResult, TaskSpec,
};
use rusty_grid_master::queue::{RetryPolicy, TaskQueue, TaskState};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::runner::{RunnerConfig, TaskRunner};

/// Computes reference oracle hash digest for a given task ID and iteration count.
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

/// Helper to connect a mock wire worker to MasterServer and complete handshake.
async fn spawn_mock_wire_worker(
    master_addr: SocketAddr,
    name: &str,
) -> (Uuid, MessageTransport<TcpStream>) {
    let stream = TcpStream::connect(master_addr)
        .await
        .expect("connect mock worker to master");
    let mut transport = MessageTransport::new(stream);
    let worker_id = Uuid::new_v4();
    let caps = WorkerCapabilities::new(name, 4, 8192, false, false, None);

    transport
        .send_msg(&WorkerMessage::Register {
            worker_id,
            capabilities: caps,
        })
        .await
        .expect("send Register");

    match transport.recv_msg().await.expect("recv RegisterAck") {
        Some(MasterMessage::RegisterAck { .. }) => {}
        other => panic!("Expected RegisterAck, got {other:?}"),
    }

    (worker_id, transport)
}

// ==============================================================================================
// AREA 1: Serialization / Deserialization Stress (Zero-byte, Large, Boundary, Corrupted)
// ==============================================================================================

#[test]
fn test_adversarial_checkpoint_zero_length_payload_roundtrip() {
    let task_id = TaskId::new();
    let empty_delta = TaskBytes::from(Vec::<u8>::new());
    assert_eq!(empty_delta.len(), 0);

    let cp_msg = WorkerMessage::Checkpoint {
        task_id,
        sequence: 0,
        delta_state: empty_delta.clone(),
    };

    // 1. JSON Wire Codec
    let json_bytes = serialize_message(&cp_msg, WireCodec::Json).expect("serialize empty json");
    let (deser_json, codec_json): (WorkerMessage, WireCodec) =
        deserialize_message(&json_bytes).expect("deserialize empty json");
    assert_eq!(codec_json, WireCodec::Json);
    assert_eq!(deser_json, cp_msg);

    // 2. Bincode Wire Codec
    let bin_bytes = serialize_message(&cp_msg, WireCodec::Bincode).expect("serialize empty bincode");
    let (deser_bin, codec_bin): (WorkerMessage, WireCodec) =
        deserialize_message(&bin_bytes).expect("deserialize empty bincode");
    assert_eq!(codec_bin, WireCodec::Bincode);
    assert_eq!(deser_bin, cp_msg);

    // 3. MasterMessage with zero-length checkpoint
    let cp_data = CheckpointData::new(0, empty_delta);
    let task = Task::new(
        TaskSpec::builtin_test("zero_cp", 10),
        TaskRequirements::generic(1, 10),
    );
    let assign_msg = MasterMessage::AssignTaskWithCheckpoint {
        task,
        latest_checkpoint: Some(cp_data),
    };

    let assign_bin =
        serialize_message(&assign_msg, WireCodec::Bincode).expect("serialize assign bincode");
    let (deser_assign, _): (MasterMessage, WireCodec) =
        deserialize_message(&assign_bin).expect("deserialize assign bincode");
    assert_eq!(deser_assign, assign_msg);
}

#[test]
fn test_adversarial_checkpoint_multi_megabyte_payloads() {
    let task_id = TaskId::new();
    let sizes = vec![1024 * 1024, 8 * 1024 * 1024, 32 * 1024 * 1024];

    for size in sizes {
        let mut large_vec = Vec::with_capacity(size);
        for i in 0..size {
            large_vec.push((i % 251) as u8);
        }
        let large_delta = TaskBytes::from(large_vec);
        assert_eq!(large_delta.len(), size);

        let cp_msg = WorkerMessage::Checkpoint {
            task_id,
            sequence: 100,
            delta_state: large_delta.clone(),
        };

        // Bincode roundtrip
        let bincode_bytes = serialize_message(&cp_msg, WireCodec::Bincode)
            .unwrap_or_else(|e| panic!("serialize {size} bytes bincode: {e:?}"));
        let (deser_bin, codec): (WorkerMessage, WireCodec) = deserialize_message(&bincode_bytes)
            .unwrap_or_else(|e| panic!("deserialize {size} bytes bincode: {e:?}"));
        assert_eq!(codec, WireCodec::Bincode);
        if let WorkerMessage::Checkpoint {
            delta_state: ref deser_delta,
            sequence,
            ..
        } = deser_bin
        {
            assert_eq!(sequence, 100);
            assert_eq!(deser_delta.len(), size);
            assert_eq!(deser_delta.as_slice(), large_delta.as_slice());
        } else {
            panic!("Expected Checkpoint variant");
        }

        // Direct CheckpointData serde_json roundtrip
        let cp_data = CheckpointData::new(42, large_delta.clone());
        let json_str = serde_json::to_string(&cp_data).expect("serialize large cp json");
        let deser_cp: CheckpointData =
            serde_json::from_str(&json_str).expect("deserialize large cp json");
        assert_eq!(deser_cp.delta_state.len(), size);
        assert_eq!(deser_cp.sequence, 42);
        assert_eq!(deser_cp.delta_state.as_slice(), large_delta.as_slice());
    }
}

#[test]
fn test_adversarial_checkpoint_payload_exceeding_max_frame_size() {
    let task_id = TaskId::new();
    // 65 MB (> 64 MB MAX_FRAME_SIZE)
    let oversized = vec![0xEE; MAX_FRAME_SIZE + 1024];
    let cp_msg = WorkerMessage::Checkpoint {
        task_id,
        sequence: 1,
        delta_state: TaskBytes::from(oversized),
    };

    let result = serialize_message(&cp_msg, WireCodec::Bincode);
    match result {
        Err(ProtocolError::FrameTooLarge { size, max }) => {
            assert!(size > MAX_FRAME_SIZE);
            assert_eq!(max, MAX_FRAME_SIZE);
        }
        Err(e) => panic!("Expected FrameTooLarge, got: {e:?}"),
        Ok(_) => panic!("Oversized frame must NOT serialize successfully"),
    }
}

#[test]
fn test_adversarial_checkpoint_corrupted_wire_payloads() {
    // 1. Truncated JSON
    let truncated_json = b"{\"type\":\"Checkpoint\",\"task_id\":\"00000000-0000-0000-0000-000000000000\",\"sequence\":5";
    let json_res: Result<(WorkerMessage, WireCodec), _> = deserialize_message(truncated_json);
    assert!(json_res.is_err(), "Truncated JSON must fail gracefully");

    // 2. Corrupted wire magic / non-JSON non-Bincode garbage
    let random_garbage = [0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
    let res: Result<(WorkerMessage, WireCodec), _> = deserialize_message(&random_garbage);
    assert!(res.is_err(), "Random garbage must return error without panic");

    // 3. Corrupted bincode payload (invalid length header claiming 4GB)
    let mut fake_bincode = vec![0x02]; // WIRE_FORMAT_BINCODE
    fake_bincode.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0x7F]); // huge length
    fake_bincode.extend_from_slice(&[0x00, 0x01, 0x02]);
    let bin_res: Result<(WorkerMessage, WireCodec), _> = deserialize_message(&fake_bincode);
    assert!(bin_res.is_err(), "Corrupted bincode length must return error without panic");
}

// ==============================================================================================
// AREA 2: TaskRunner Robustness Under Hostile / Anomalous Delta Payloads
// ==============================================================================================

#[tokio::test]
async fn test_adversarial_runner_corrupted_and_boundary_delta_payloads() {
    let runner = TaskRunner::new(
        Uuid::new_v4(),
        WorkerCapabilities::new("runner-adversarial", 4, 8192, false, false, None),
        RunnerConfig::default(),
    );

    let task = Task::new(
        TaskSpec::builtin_test("checkpoint", 100),
        TaskRequirements::generic(1, 30),
    );

    // Case 1: Empty delta payload (0 bytes) -> must safely default to iteration 0
    let empty_cp = CheckpointData::new(1, TaskBytes::from(Vec::<u8>::new()));
    let res1 = runner
        .execute_task_with_checkpoint(&task, Some(&empty_cp), None, None, None)
        .await;
    assert!(res1.is_success(), "Empty delta must execute without panic");
    assert!(!res1.stdout.contains("resumed"));

    // Case 2: Partial delta payload (7 bytes, neither 16-byte binary nor valid JSON)
    let partial_cp = CheckpointData::new(2, TaskBytes::copy_from_slice(b"partial"));
    let res2 = runner
        .execute_task_with_checkpoint(&task, Some(&partial_cp), None, None, None)
        .await;
    assert!(res2.is_success(), "Partial delta must execute safely");
    assert!(!res2.stdout.contains("resumed"));

    // Case 3: Malformed string delta payload (>= 16 bytes)
    // EMPIRICAL FINDING: Because runner.rs checks `if slice.len() >= 16` before JSON parsing,
    // any payload with >= 16 bytes is blindly decoded as little-endian u64 integers.
    // The first 8 bytes b"{\"iterat" are parsed as start_iter = 1953047163.
    let malformed_json_cp = CheckpointData::new(3, TaskBytes::copy_from_slice(b"{\"iteration\": \"not_a_num\"}"));
    let res3 = runner
        .execute_task_with_checkpoint(&task, Some(&malformed_json_cp), None, None, None)
        .await;
    assert!(res3.is_success(), "Malformed delta must not panic runner");
    // Verify empirical behavior: runner interprets b"{\"iterat" as u64
    assert!(
        res3.stdout.contains("resumed from 1953047163"),
        "Empirical finding: runner.rs:433 treats >=16-byte text as binary integer (start_iter: 1953047163)"
    );

    // Case 4: JSON delta payload (>= 16 bytes)
    // EMPIRICAL FINDING: The JSON parsing branch in runner.rs:438 is UNREACHABLE for any JSON payload
    // because any JSON payload with iteration/state is >= 16 bytes and gets trapped by line 433!
    let valid_json = serde_json::json!({
        "iteration": 50,
        "state": 123456789u64
    });
    let json_bytes = serde_json::to_vec(&valid_json).unwrap();
    let json_cp = CheckpointData::new(4, TaskBytes::from(json_bytes));
    let res4 = runner
        .execute_task_with_checkpoint(&task, Some(&json_cp), None, None, None)
        .await;
    assert!(res4.is_success(), "JSON delta must succeed without panic");
    println!("res4 stdout: {}", res4.stdout);
    // Demonstrates that runner fails to parse JSON and instead reads binary slice:
    let json_parsed_correctly = res4.stdout.contains("resumed from 50");
    println!("Was JSON parsed correctly as iteration 50? {}", json_parsed_correctly);
    assert!(
        !json_parsed_correctly,
        "Empirical finding: runner.rs:438 JSON deserializer is completely bypassed by line 433 slice.len() >= 16"
    );

    // Case 5: Out-of-bounds start_iter (e.g. 500 when iters is 100)
    let mut oob_vec = Vec::with_capacity(16);
    oob_vec.extend_from_slice(&(500u64).to_le_bytes());
    oob_vec.extend_from_slice(&(99999u64).to_le_bytes());
    let oob_cp = CheckpointData::new(5, TaskBytes::from(oob_vec));
    let res5 = runner
        .execute_task_with_checkpoint(&task, Some(&oob_cp), None, None, None)
        .await;
    assert!(res5.is_success(), "Out-of-bounds delta must safely handle empty loop range without panic");
    assert!(res5.stdout.contains("resumed from 500"));
}

// ==============================================================================================
// AREA 3: TaskQueue Monotonic Sequence Enforcement & Concurrency Stress
// ==============================================================================================

#[tokio::test]
async fn test_adversarial_task_queue_monotonic_sequence_enforcement() {
    let queue = TaskQueue::new();
    let task = Task::new(
        TaskSpec::builtin_test("monotonic_test", 100),
        TaskRequirements::generic(1, 30),
    );
    let task_id = queue.submit(task).await.expect("submit task");

    let worker_id = Uuid::new_v4();
    let _ = queue.schedule_task(&task_id, worker_id).await.expect("schedule");
    queue.mark_running(&task_id, worker_id).await.expect("running");

    // 1. Initial checkpoint: sequence 10
    let cp10 = CheckpointData::new(10, TaskBytes::copy_from_slice(b"seq_10"));
    queue.record_checkpoint(task_id, cp10.clone()).await.unwrap();
    assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 10);

    // 2. Stale sequence 5 -> must be ignored
    let cp5 = CheckpointData::new(5, TaskBytes::copy_from_slice(b"seq_5"));
    queue.record_checkpoint(task_id, cp5).await.unwrap();
    assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 10);

    // 3. Duplicate sequence 10 -> must be ignored
    let cp10_dup = CheckpointData::new(10, TaskBytes::copy_from_slice(b"seq_10_new_bytes"));
    queue.record_checkpoint(task_id, cp10_dup).await.unwrap();
    assert_eq!(
        queue.get_checkpoint(&task_id).await.unwrap().delta_state.as_slice(),
        b"seq_10",
        "Duplicate sequence must not overwrite existing state"
    );

    // 4. Stale sequence 0 -> must be ignored
    let cp0 = CheckpointData::new(0, TaskBytes::copy_from_slice(b"seq_0"));
    queue.record_checkpoint(task_id, cp0).await.unwrap();
    assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 10);

    // 5. Monotonic advance: sequence 11 -> accepted
    let cp11 = CheckpointData::new(11, TaskBytes::copy_from_slice(b"seq_11"));
    queue.record_checkpoint(task_id, cp11).await.unwrap();
    assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 11);

    // 6. Boundary advance: sequence u64::MAX -> accepted
    let cp_max = CheckpointData::new(u64::MAX, TaskBytes::copy_from_slice(b"seq_max"));
    queue.record_checkpoint(task_id, cp_max).await.unwrap();
    assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, u64::MAX);

    // 7. Stale sequence u64::MAX - 1 -> must be rejected
    let cp_max_minus_1 = CheckpointData::new(u64::MAX - 1, TaskBytes::copy_from_slice(b"seq_max_sub_1"));
    queue.record_checkpoint(task_id, cp_max_minus_1).await.unwrap();
    assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, u64::MAX);
}

#[tokio::test]
async fn test_adversarial_task_queue_concurrent_checkpoint_race() {
    let queue = Arc::new(TaskQueue::new());
    let task = Task::new(
        TaskSpec::builtin_test("race_test", 500),
        TaskRequirements::generic(1, 30),
    );
    let task_id = queue.submit(task).await.expect("submit task");

    let worker_id = Uuid::new_v4();
    let _ = queue.schedule_task(&task_id, worker_id).await.expect("schedule");
    queue.mark_running(&task_id, worker_id).await.expect("running");

    // Spawn 100 concurrent tasks sending sequence numbers 1..=100 in scrambled order
    let mut handles = Vec::new();
    for seq in 1..=100u64 {
        let q = Arc::clone(&queue);
        let tid = task_id;
        handles.push(tokio::spawn(async move {
            let cp = CheckpointData::new(seq, TaskBytes::from(format!("delta_seq_{seq}")));
            q.record_checkpoint(tid, cp).await
        }));
    }

    for h in handles {
        let res = h.await.expect("join handle");
        assert!(res.is_ok(), "record_checkpoint must succeed without error");
    }

    // After all 100 concurrent updates complete, the queue must hold sequence 100
    let final_cp = queue.get_checkpoint(&task_id).await.expect("checkpoint exists");
    assert_eq!(
        final_cp.sequence, 100,
        "Concurrent ingestion must converge to the highest sequence number (100)"
    );
    assert_eq!(final_cp.delta_state.as_slice(), b"delta_seq_100");
}

#[tokio::test]
async fn test_adversarial_checkpoint_on_terminal_and_nonexistent_tasks() {
    let queue = TaskQueue::new();
    let task = Task::new(
        TaskSpec::builtin_test("terminal_test", 100),
        TaskRequirements::generic(1, 30),
    );
    let task_id = queue.submit(task).await.expect("submit");
    let worker_id = Uuid::new_v4();
    let _ = queue.schedule_task(&task_id, worker_id).await.expect("schedule");
    queue.mark_running(&task_id, worker_id).await.expect("running");

    // 1. Initial checkpoint
    let cp1 = CheckpointData::new(1, TaskBytes::copy_from_slice(b"delta_1"));
    queue.record_checkpoint(task_id, cp1.clone()).await.unwrap();

    // 2. Complete the task
    let result = TaskResult::success(worker_id, task_id, "done", 10, false);
    queue.record_result(result).await.unwrap();

    // 3. Attempt to record checkpoint on terminal task -> returns Ok(()) but does NOT update
    let cp2 = CheckpointData::new(2, TaskBytes::copy_from_slice(b"delta_2"));
    queue.record_checkpoint(task_id, cp2).await.unwrap();

    // Checkpoint must remain cp1 (sequence 1)
    let cp_after = queue.get_checkpoint(&task_id).await.unwrap();
    assert_eq!(cp_after.sequence, 1);
    assert_eq!(cp_after.delta_state.as_slice(), b"delta_1");

    // 4. Attempt to record checkpoint on non-existent task ID -> returns GridError::TaskNotFound
    let non_existent_id = TaskId::new();
    let err = queue.record_checkpoint(non_existent_id, cp1).await.unwrap_err();
    match err {
        GridError::TaskNotFound(id) => assert_eq!(id, non_existent_id.0),
        other => panic!("Expected TaskNotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn test_adversarial_checkpoint_retention_on_retry_exhaustion() {
    let mut policy = RetryPolicy::default();
    policy.max_retries = 1; // Only 1 retry allowed
    let queue = TaskQueue::with_config(policy, None);

    let task = Task::new(
        TaskSpec::builtin_test("exhaustion_test", 100),
        TaskRequirements::generic(1, 30),
    );
    let task_id = queue.submit(task).await.expect("submit");

    // Worker 1 schedules & records checkpoint
    let w1 = Uuid::new_v4();
    let _ = queue.schedule_task(&task_id, w1).await.expect("schedule w1");
    queue.mark_running(&task_id, w1).await.expect("running w1");
    let cp = CheckpointData::new(1, TaskBytes::copy_from_slice(b"delta_before_crash"));
    queue.record_checkpoint(task_id, cp.clone()).await.unwrap();

    // Worker 1 crashes -> retry 1
    queue.handle_worker_disconnected(&w1, "Crash 1", true).await;
    let info1 = queue.get_task(&task_id).await.unwrap();
    assert_eq!(info1.state, TaskState::Queued);
    assert_eq!(info1.retry_count, 1);

    // Worker 2 schedules
    let w2 = Uuid::new_v4();
    let _ = queue.schedule_task(&task_id, w2).await.expect("schedule w2");
    queue.mark_running(&task_id, w2).await.expect("running w2");

    // Worker 2 crashes -> retries exhausted (1/1) -> task fails
    queue.handle_worker_disconnected(&w2, "Crash 2", true).await;
    let info2 = queue.get_task(&task_id).await.unwrap();
    assert_eq!(info2.state, TaskState::Failed);

    // Crucial check: Checkpoint must still be preserved for diagnostics / telemetry!
    let preserved = queue.get_checkpoint(&task_id).await;
    assert_eq!(preserved, Some(cp));
}

// ==============================================================================================
// AREA 4: Multi-Hop Failover Resumption (Worker 1 -> Worker 2 -> Worker 3)
// ==============================================================================================

#[tokio::test]
async fn test_adversarial_multi_hop_failover_task_queue() {
    let mut policy = RetryPolicy::default();
    policy.max_retries = 5;
    let queue = TaskQueue::with_config(policy, None);

    let task = Task::new(
        TaskSpec::builtin_test("multihop_queue", 300),
        TaskRequirements::generic(1, 30),
    );
    let task_id = queue.submit(task).await.expect("submit");

    let w1 = Uuid::new_v4();
    let w2 = Uuid::new_v4();
    let w3 = Uuid::new_v4();

    // --- HOP 1: Worker 1 ---
    let t_w1 = queue.schedule_task(&task_id, w1).await.unwrap();
    assert_eq!(t_w1.latest_checkpoint, None);
    queue.mark_running(&task_id, w1).await.unwrap();

    let cp1 = CheckpointData::new(1, TaskBytes::copy_from_slice(b"delta_hop_1"));
    queue.record_checkpoint(task_id, cp1.clone()).await.unwrap();

    // Worker 1 dies
    queue.handle_worker_disconnected(&w1, "W1 died", true).await;

    // --- HOP 2: Worker 2 ---
    let t_w2 = queue.schedule_task(&task_id, w2).await.unwrap();
    assert_eq!(t_w2.latest_checkpoint, Some(cp1));
    queue.mark_running(&task_id, w2).await.unwrap();

    let cp2 = CheckpointData::new(2, TaskBytes::copy_from_slice(b"delta_hop_2"));
    queue.record_checkpoint(task_id, cp2.clone()).await.unwrap();

    // Worker 2 dies
    queue.handle_worker_disconnected(&w2, "W2 died", true).await;

    // --- HOP 3: Worker 3 ---
    let t_w3 = queue.schedule_task(&task_id, w3).await.unwrap();
    assert_eq!(t_w3.latest_checkpoint, Some(cp2.clone()));
    queue.mark_running(&task_id, w3).await.unwrap();

    let cp3 = CheckpointData::new(3, TaskBytes::copy_from_slice(b"delta_hop_3"));
    queue.record_checkpoint(task_id, cp3.clone()).await.unwrap();

    // Worker 3 succeeds
    let res = TaskResult::success(w3, task_id, "Completed hop 3", 100, false);
    queue.record_result(res).await.unwrap();

    let final_info = queue.get_task(&task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.retry_count, 2, "Task must record exactly 2 retries across 2 failovers");
    assert_eq!(queue.get_checkpoint(&task_id).await, Some(cp3));
}

#[tokio::test]
async fn test_adversarial_multi_hop_runner_mathematical_oracle() {
    let task_id = TaskId::new();
    let iters = 300u32;
    let reference_digest = compute_reference_digest(&task_id, iters);

    let caps = WorkerCapabilities::new("oracle-runner", 4, 8192, false, false, None);

    // 1. Hop 1: Runner 1 computes 0..50
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
    let mut delta1 = Vec::with_capacity(16);
    delta1.extend_from_slice(&(50u64).to_le_bytes());
    delta1.extend_from_slice(&state.to_le_bytes());
    let _cp1 = CheckpointData::new(1, TaskBytes::from(delta1));

    // 2. Hop 2: Runner 2 resumes from 50..150
    for i in 50..150 {
        state ^= i as u64;
        state = state.wrapping_mul(0x100000001b3);
        state = state.rotate_left(13);
    }
    let mut delta2 = Vec::with_capacity(16);
    delta2.extend_from_slice(&(150u64).to_le_bytes());
    delta2.extend_from_slice(&state.to_le_bytes());
    let cp2 = CheckpointData::new(2, TaskBytes::from(delta2));

    // 3. Hop 3: Runner 3 receives cp2 and finishes 150..300
    let runner3 = TaskRunner::new(Uuid::new_v4(), caps, RunnerConfig::default());
    let task = Task::new(
        TaskSpec::builtin_test("checkpoint", iters),
        TaskRequirements::generic(1, 30),
    );
    let mut task = task;
    task.id = task_id;
    task.latest_checkpoint = Some(cp2.clone());

    let res3 = runner3
        .execute_task_with_checkpoint(&task, Some(&cp2), None, None, None)
        .await;
    assert!(res3.is_success());
    let out = res3.stdout.as_str();
    assert!(out.contains("resumed from 150"));
    let expected_str = format!("{reference_digest:016x}");
    assert!(
        out.contains(&expected_str),
        "Multi-hop resumption must produce EXACT reference digest ({expected_str}); got: {out}"
    );
}

#[tokio::test]
async fn test_adversarial_end_to_end_cluster_multi_hop_failover() {
    // Spawn MasterServer with max_retries = 5
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(5);
    let master = MasterServer::spawn(config).await.expect("master spawn");
    let master_addr = master.server_addr();

    // Submit 300-iteration task
    let task = Task::new(
        TaskSpec::builtin_test("multihop_task", 300),
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit task");

    // =========================================================================
    // HOP 1: Worker 1 connects, receives AssignTask, sends Checkpoint 1, crashes
    // =========================================================================
    let (_w1_id, mut transport1) = spawn_mock_wire_worker(master_addr, "mock-w1").await;

    // Receive AssignTask
    let assigned_w1 = match transport1.recv_msg::<MasterMessage>().await.expect("recv assign") {
        Some(MasterMessage::AssignTask { task }) => task,
        other => panic!("Expected AssignTask, got {other:?}"),
    };
    assert_eq!(assigned_w1.id, task_id);
    assert_eq!(assigned_w1.latest_checkpoint, None);

    // Send Checkpoint 1 (sequence 1, iteration 50)
    let cp1_delta = TaskBytes::copy_from_slice(b"state_iter_50_worker1");
    transport1
        .send_msg(&WorkerMessage::Checkpoint {
            task_id,
            sequence: 1,
            delta_state: cp1_delta.clone(),
        })
        .await
        .expect("send cp1");

    // Wait for Master to record Checkpoint 1
    let recorded_cp1 = wait_for(Duration::from_secs(3), Duration::from_millis(50), || async {
        if let Some(cp) = master.get_checkpoint(&task_id).await {
            cp.sequence == 1
        } else {
            false
        }
    })
    .await;
    assert!(recorded_cp1, "Master must record Checkpoint 1");

    // Abruptly terminate Worker 1 by dropping its TCP transport
    drop(transport1);

    // Wait for Master to requeue task (retry_count == 1)
    let requeued_1 = wait_for(Duration::from_secs(4), Duration::from_millis(50), || async {
        if let Ok(info) = master.get_task_info(task_id).await {
            (info.state == TaskState::Queued || info.state == TaskState::Retrying) && info.retry_count == 1
        } else {
            false
        }
    })
    .await;
    assert!(requeued_1, "Master must requeue task after Worker 1 crash with retry_count == 1");

    // Checkpoint 1 must be strictly preserved
    let cp_after_w1 = master.get_checkpoint(&task_id).await.unwrap();
    assert_eq!(cp_after_w1.sequence, 1);
    assert_eq!(cp_after_w1.delta_state, cp1_delta);

    // =========================================================================
    // HOP 2: Worker 2 connects, receives AssignTaskWithCheckpoint, sends Checkpoint 2, crashes
    // =========================================================================
    let (_w2_id, mut transport2) = spawn_mock_wire_worker(master_addr, "mock-w2").await;

    // Receive AssignTaskWithCheckpoint containing Checkpoint 1
    let (assigned_w2, latest_cp_w2) = match transport2.recv_msg::<MasterMessage>().await.expect("recv assign w2") {
        Some(MasterMessage::AssignTaskWithCheckpoint { task, latest_checkpoint }) => (task, latest_checkpoint),
        other => panic!("Expected AssignTaskWithCheckpoint on Worker 2, got {other:?}"),
    };
    assert_eq!(assigned_w2.id, task_id);
    assert_eq!(latest_cp_w2, Some(cp_after_w1));

    // Send Checkpoint 2 (sequence 2, iteration 150)
    let cp2_delta = TaskBytes::copy_from_slice(b"state_iter_150_worker2");
    transport2
        .send_msg(&WorkerMessage::Checkpoint {
            task_id,
            sequence: 2,
            delta_state: cp2_delta.clone(),
        })
        .await
        .expect("send cp2");

    // Wait for Master to record Checkpoint 2
    let recorded_cp2 = wait_for(Duration::from_secs(3), Duration::from_millis(50), || async {
        if let Some(cp) = master.get_checkpoint(&task_id).await {
            cp.sequence == 2
        } else {
            false
        }
    })
    .await;
    assert!(recorded_cp2, "Master must record Checkpoint 2");

    // Abruptly terminate Worker 2 by dropping its TCP transport
    drop(transport2);

    // Wait for Master to requeue task (retry_count == 2)
    let requeued_2 = wait_for(Duration::from_secs(4), Duration::from_millis(50), || async {
        if let Ok(info) = master.get_task_info(task_id).await {
            (info.state == TaskState::Queued || info.state == TaskState::Retrying) && info.retry_count == 2
        } else {
            false
        }
    })
    .await;
    assert!(requeued_2, "Master must requeue task after Worker 2 crash with retry_count == 2");

    // Checkpoint 2 must be strictly preserved
    let cp_after_w2 = master.get_checkpoint(&task_id).await.unwrap();
    assert_eq!(cp_after_w2.sequence, 2);
    assert_eq!(cp_after_w2.delta_state, cp2_delta);

    // =========================================================================
    // HOP 3: Worker 3 connects, receives AssignTaskWithCheckpoint (cp2), completes
    // =========================================================================
    let (w3_id, mut transport3) = spawn_mock_wire_worker(master_addr, "mock-w3").await;

    // Receive AssignTaskWithCheckpoint containing Checkpoint 2
    let (assigned_w3, latest_cp_w3) = match transport3.recv_msg::<MasterMessage>().await.expect("recv assign w3") {
        Some(MasterMessage::AssignTaskWithCheckpoint { task, latest_checkpoint }) => (task, latest_checkpoint),
        other => panic!("Expected AssignTaskWithCheckpoint on Worker 3, got {other:?}"),
    };
    assert_eq!(assigned_w3.id, task_id);
    assert_eq!(latest_cp_w3, Some(cp_after_w2));

    // Send successful TaskResult
    transport3
        .send_msg(&WorkerMessage::TaskResult {
            worker_id: w3_id,
            task_id,
            exit_code: 0,
            stdout: TaskBytes::copy_from_slice(b"Completed successfully after multi-hop failovers\n"),
            stderr: TaskBytes::from(Vec::<u8>::new()),
            execution_time_ms: 250,
            is_gpu_executed: false,
            device_name: None,
            error: None,
        })
        .await
        .expect("send final result");

    // Wait for task to be marked Completed on Master
    let completed = wait_for(Duration::from_secs(4), Duration::from_millis(50), || async {
        if let Ok(info) = master.get_task_info(task_id).await {
            info.state == TaskState::Completed
        } else {
            false
        }
    })
    .await;
    assert!(completed, "Task must transition to Completed after Worker 3 finishes");

    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.retry_count, 2, "Must record exactly 2 retries");
    assert_eq!(final_info.assigned_worker_id, Some(w3_id));

    let _ = master.shutdown();
}
