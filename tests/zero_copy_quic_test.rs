//! Comprehensive Integration Test Suite for Requirement R2:
//! High-Performance Zero-Copy I/O, QUIC Stream Multiplexing & Scheduler Micro-Batching.
//!
//! Validates:
//! 1. Zero-Copy I/O (`bytes::Bytes`):
//!    - `Bytes` wrapper ergonomics (Deref to str, Display, AsRef<[u8]>, slicing, into_bytes)
//!    - `TaskResult.stdout` and `TaskResult.stderr` zero-copy handling and `stdout_str()` / `stderr_str()` Cow conversion
//!    - Large payload (1MB+) serialization roundtrip across both Bincode and JSON formats without corruption
//! 2. 2-Lane QUIC Stream Multiplexing over Iroh P2P:
//!    - Stream header tags: `STREAM_CONTROL` (0x01) and `STREAM_DATA` (0x02)
//!    - Multiplexed bi-directional streams over an Iroh QUIC endpoint
//!    - Concurrency and Anti-Head-of-Line (HoL) Blocking: heavy data payload streaming on the Data Lane does not block Control Lane heartbeats
//! 3. Master Scheduler Micro-Batching:
//!    - `SchedulerConfig` micro-batch window (5ms debounce) and max batch size configuration
//!    - `TaskQueue::schedule_tasks_batch` atomic state transitions under a single lock acquisition
//!    - `WorkloadScheduler::schedule_batch` bulk scheduling under burst task submission

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use tokio_util::bytes::Bytes as RawBytes;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{
    deserialize_message, serialize_message, MasterMessage, MessageTransport, WireCodec, WorkerMessage,
};
use rusty_grid_core::task::{Bytes, Task, TaskId, TaskRequirements, TaskResult, TaskSpec};
use rusty_grid_core::transport::{BiStream, GridStream, STREAM_CONTROL, STREAM_DATA};
use rusty_grid_master::queue::{TaskQueue, TaskState};
use rusty_grid_master::registry::WorkerRegistry;
use rusty_grid_master::scheduler::{SchedulerConfig, WorkloadScheduler};

// =========================================================================
// PART 1: ZERO-COPY I/O AND BYTES WRAPPER TESTS
// =========================================================================

#[test]
fn test_zero_copy_bytes_wrapper_ergonomics() {
    let raw = RawBytes::from_static(b"Hello Zero-Copy World!");
    let b = Bytes::from(raw.clone());

    // 1. Length & Empty checks
    assert_eq!(b.len(), 22);
    assert!(!b.is_empty());

    // 2. Deref to str works transparently
    assert!(b.contains("Zero-Copy"));
    assert!(b.starts_with("Hello"));
    assert_eq!(&*b, "Hello Zero-Copy World!");
    assert_eq!(b, "Hello Zero-Copy World!");

    // 3. Display works transparently
    assert_eq!(format!("{}", b), "Hello Zero-Copy World!");

    // 4. Zero-copy slice points to the same underlying buffer memory
    let slice = b.slice(6..15);
    assert_eq!(&*slice, "Zero-Copy");
    assert_eq!(slice.as_ptr(), unsafe { raw.as_ptr().add(6) });

    // 5. Zero-copy conversion to and from bytes::Bytes
    let extracted: RawBytes = b.into_bytes();
    assert_eq!(extracted, raw);

    // 6. Conversions from Vec<u8>, String, and &str
    let from_vec = Bytes::from(vec![65, 66, 67]);
    assert_eq!(&*from_vec, "ABC");

    let from_str = Bytes::from("xyz");
    assert_eq!(&*from_str, "xyz");

    let from_string = Bytes::from("dynamic".to_string());
    assert_eq!(&*from_string, "dynamic");
}

#[test]
fn test_task_result_stdout_stderr_cow_methods() {
    let task_id = TaskId::new();
    let worker_id = Uuid::new_v4();

    // Case A: Valid UTF-8 returns Cow::Borrowed
    let res_utf8 = TaskResult {
        task_id,
        worker_id,
        exit_code: 0,
        stdout: Bytes::from_static(b"standard output string"),
        stderr: Bytes::from_static(b"standard error string"),
        execution_time_ms: 42,
        error: None,
        is_gpu_executed: false,
        device_name: None,
    };

    match res_utf8.stdout_str() {
        Cow::Borrowed(s) => assert_eq!(s, "standard output string"),
        Cow::Owned(_) => panic!("Expected Borrowed Cow for valid UTF-8 stdout"),
    }

    match res_utf8.stderr_str() {
        Cow::Borrowed(s) => assert_eq!(s, "standard error string"),
        Cow::Owned(_) => panic!("Expected Borrowed Cow for valid UTF-8 stderr"),
    }

    // Case B: Invalid UTF-8 returns Cow::Owned lossy without panicking
    let res_binary = TaskResult {
        task_id,
        worker_id,
        exit_code: 1,
        stdout: Bytes::from(vec![0xFF, 0xFE, 0xFD]),
        stderr: Bytes::from(vec![0x80, 0x81]),
        execution_time_ms: 10,
        error: None,
        is_gpu_executed: false,
        device_name: None,
    };

    match res_binary.stdout_str() {
        Cow::Owned(s) => assert!(s.contains('\u{FFFD}')),
        Cow::Borrowed(_) => panic!("Expected Owned Cow for invalid UTF-8 stdout"),
    }
}

#[test]
fn test_zero_copy_large_payload_serialization_roundtrip() {
    let task_id = TaskId::new();
    let worker_id = Uuid::new_v4();

    // Generate 1MB of deterministic test data for stdout and 256KB for stderr
    let stdout_len = 1024 * 1024;
    let mut stdout_vec = Vec::with_capacity(stdout_len);
    for i in 0..stdout_len {
        stdout_vec.push(((i * 7 + 13) % 256) as u8);
    }

    let stderr_len = 256 * 1024;
    let mut stderr_vec = Vec::with_capacity(stderr_len);
    for i in 0..stderr_len {
        stderr_vec.push(((i * 11 + 17) % 256) as u8);
    }

    let original = TaskResult {
        task_id,
        worker_id,
        exit_code: 0,
        stdout: Bytes::from(stdout_vec.clone()),
        stderr: Bytes::from(stderr_vec.clone()),
        execution_time_ms: 1234,
        error: None,
        is_gpu_executed: true,
        device_name: None,
    };

    // 1. Test JSON Roundtrip
    let json_encoded = serde_json::to_string(&original).expect("json serialize");
    let json_decoded: TaskResult = serde_json::from_str(&json_encoded).expect("json deserialize");

    assert_eq!(json_decoded.task_id, original.task_id);
    assert_eq!(json_decoded.worker_id, original.worker_id);
    assert_eq!(json_decoded.exit_code, original.exit_code);
    assert_eq!(json_decoded.stdout.as_bytes(), &stdout_vec[..]);
    assert_eq!(json_decoded.stderr.as_bytes(), &stderr_vec[..]);

    // 2. Test WorkerMessage::TaskResult wire variant roundtrip via Bincode WireCodec
    let worker_msg = WorkerMessage::TaskResult {
        worker_id,
        task_id,
        exit_code: 0,
        stdout: Bytes::from(stdout_vec.clone()),
        stderr: Bytes::from(stderr_vec.clone()),
        execution_time_ms: 1234,
        is_gpu_executed: true,
        device_name: None,
        error: None,
    };

    let msg_bincode = serialize_message(&worker_msg, WireCodec::Bincode).expect("bincode wire encode");
    let (msg_decoded, codec_bincode): (WorkerMessage, WireCodec) =
        deserialize_message(&msg_bincode).expect("bincode wire decode");
    assert_eq!(codec_bincode, WireCodec::Bincode);
    assert_eq!(worker_msg, msg_decoded);

    // 3. Test WorkerMessage::TaskResult wire variant roundtrip via JSON WireCodec
    let msg_json = serialize_message(&worker_msg, WireCodec::Json).expect("json wire encode");
    let (msg_json_decoded, codec_json): (WorkerMessage, WireCodec) =
        deserialize_message(&msg_json).expect("json wire decode");
    assert_eq!(codec_json, WireCodec::Json);
    assert_eq!(worker_msg, msg_json_decoded);
}

// =========================================================================
// PART 2: QUIC 2-LANE STREAM MULTIPLEXING TESTS
// =========================================================================

#[test]
fn test_quic_stream_header_constants() {
    assert_eq!(STREAM_CONTROL, 0x01, "Control stream tag must be 0x01");
    assert_eq!(STREAM_DATA, 0x02, "Data stream tag must be 0x02");
}

#[cfg(feature = "p2p")]
#[tokio::test]
async fn test_quic_stream_multiplexing_isolation_and_anti_hol_blocking() {
    use iroh::endpoint::presets::N0;
    use iroh::endpoint::RelayMode;
    use rusty_grid_core::transport::GRID_ALPN;

    // 1. Bind two independent Iroh QUIC endpoints on loopback
    let ep_server = iroh::Endpoint::builder(N0)
        .alpns(vec![GRID_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind server endpoint");

    let ep_client = iroh::Endpoint::builder(N0)
        .alpns(vec![GRID_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind client endpoint");

    let server_addr = ep_server.addr();

    // 2. Server task accepts connection and incoming bi-directional streams
    let server_handle = tokio::spawn(async move {
        let incoming = ep_server.accept().await.expect("accept").await.expect("handshake");

        // Accept first stream
        let (send_a, mut recv_a) = incoming.accept_bi().await.expect("accept stream A");
        let mut tag_a = [0u8; 1];
        recv_a.read_exact(&mut tag_a).await.expect("read tag A");

        // Accept second stream
        let (send_b, mut recv_b) = incoming.accept_bi().await.expect("accept stream B");
        let mut tag_b = [0u8; 1];
        recv_b.read_exact(&mut tag_b).await.expect("read tag B");

        // Determine which stream is Control and which is Data
        let (ctrl_send, ctrl_recv, _data_send, mut data_recv) = if tag_a[0] == STREAM_CONTROL {
            assert_eq!(tag_b[0], STREAM_DATA);
            (send_a, recv_a, send_b, recv_b)
        } else {
            assert_eq!(tag_a[0], STREAM_DATA);
            assert_eq!(tag_b[0], STREAM_CONTROL);
            (send_b, recv_b, send_a, recv_a)
        };

        // Wrap streams in MessageTransport
        let ctrl_stream = GridStream::P2p(BiStream::new(ctrl_recv, ctrl_send));
        let mut ctrl_transport = MessageTransport::new(ctrl_stream);

        // Spawn a background data sink on the server reading heavy data
        let data_sink_task = tokio::spawn(async move {
            let mut total_read = 0usize;
            let mut buf = vec![0u8; 16 * 1024];
            loop {
                match data_recv.read(&mut buf).await {
                    Ok(Some(n)) if n > 0 => total_read += n,
                    _ => break,
                }
            }
            total_read
        });

        // Server answers heartbeats on the Control Stream until Disconnecting
        while let Ok(Some(msg)) = ctrl_transport.recv_msg::<WorkerMessage>().await {
            match msg {
                WorkerMessage::Heartbeat { timestamp, .. } => {
                    let ack = MasterMessage::HeartbeatAck { timestamp };
                    ctrl_transport.send_msg(&ack).await.expect("send heartbeat ack");
                }
                WorkerMessage::Disconnecting { .. } => {
                    break;
                }
                _ => {}
            }
        }

        let total_data_read = data_sink_task.await.expect("data sink finish");
        total_data_read
    });

    // 3. Client connects to server
    let conn = ep_client.connect(server_addr, GRID_ALPN).await.expect("connect");

    // Open Lane 1: Control Stream (tag 0x01)
    let (mut ctrl_send, ctrl_recv) = conn.open_bi().await.expect("open ctrl bi");
    ctrl_send.write_all(&[STREAM_CONTROL]).await.expect("write ctrl tag");
    ctrl_send.flush().await.expect("flush ctrl tag");
    let ctrl_stream = GridStream::P2p(BiStream::new(ctrl_recv, ctrl_send));
    let mut ctrl_transport = MessageTransport::new(ctrl_stream);

    // Open Lane 2: Data Stream (tag 0x02)
    let (mut data_send, _data_recv) = conn.open_bi().await.expect("open data bi");
    data_send.write_all(&[STREAM_DATA]).await.expect("write data tag");
    data_send.flush().await.expect("flush data tag");

    // 4. Anti-Head-of-Line Blocking Simulation:
    // Concurrently stream 256KB of heavy data on the Data Lane while sending heartbeats on Control Lane
    let heavy_payload_size = 256 * 1024;
    let data_producer_task = tokio::spawn(async move {
        let chunk = vec![0xAAu8; 16 * 1024];
        let mut sent = 0;
        while sent < heavy_payload_size {
            data_send.write_all(&chunk).await.expect("write heavy chunk");
            data_send.flush().await.expect("flush heavy chunk");
            sent += chunk.len();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        data_send.shutdown().await.expect("shutdown data send");
    });

    // Measure heartbeat RTT while data is actively streaming
    let worker_id = Uuid::new_v4();
    let mut heartbeat_latencies = Vec::new();

    for i in 0..10 {
        let t0 = Instant::now();
        let hb = WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 1000 + i,
            active_tasks: 1,
            cpu_usage_pct: 50.0,
            ram_available_mb: 8192,
        };
        ctrl_transport.send_msg(&hb).await.expect("send hb");
        let ack: Option<MasterMessage> = ctrl_transport.recv_msg().await.expect("recv ack");
        let elapsed = t0.elapsed();
        heartbeat_latencies.push(elapsed);

        match ack {
            Some(MasterMessage::HeartbeatAck { timestamp }) => {
                assert_eq!(timestamp, 1000 + i);
            }
            other => panic!("Expected HeartbeatAck, got: {:?}", other),
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    // Clean disconnect on control lane
    let disc = WorkerMessage::Disconnecting {
        worker_id,
        reason: "Test complete".into(),
    };
    ctrl_transport.send_msg(&disc).await.expect("send disconnecting");

    data_producer_task.await.expect("data producer finish");
    let total_server_read = server_handle.await.expect("server finish");

    assert_eq!(total_server_read, heavy_payload_size, "Server must receive all streamed data");

    // Assert that average heartbeat latency is under 50ms despite concurrent data streaming
    let avg_latency: Duration = heartbeat_latencies.iter().sum::<Duration>() / heartbeat_latencies.len() as u32;
    println!("Control Lane Avg Heartbeat Latency under Heavy Data Stream: {:?}", avg_latency);
    assert!(
        avg_latency < Duration::from_millis(50),
        "Heartbeat latency over multiplexed control stream must remain sub-50ms (got: {:?})",
        avg_latency
    );
}

// =========================================================================
// PART 3: SCHEDULER MICRO-BATCHING TESTS
// =========================================================================

#[test]
fn test_scheduler_micro_batch_configuration() {
    let default_config = SchedulerConfig::default();
    assert_eq!(
        default_config.micro_batch_window,
        Duration::from_millis(5),
        "Default micro_batch_window must be 5ms"
    );
    assert_eq!(
        default_config.max_batch_size, 128,
        "Default max_batch_size must be 128"
    );

    let custom_config = SchedulerConfig::default()
        .with_micro_batch_window(Duration::from_millis(15))
        .with_max_batch_size(64);

    assert_eq!(custom_config.micro_batch_window, Duration::from_millis(15));
    assert_eq!(custom_config.max_batch_size, 64);
}

#[tokio::test]
async fn test_task_queue_atomic_batch_scheduling() {
    let queue = TaskQueue::new();
    let worker_1 = Uuid::new_v4();
    let worker_2 = Uuid::new_v4();

    // 1. Submit 50 tasks in Queued state
    let mut task_ids = Vec::new();
    for i in 0..50 {
        let task = Task::new(
            TaskSpec::Command {
                program: "echo".into(),
                args: vec![format!("task-{}", i)],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::default(),
        );
        let tid = task.id;
        queue.submit(task).await.expect("submit task");
        task_ids.push(tid);
    }

    assert_eq!(queue.get_schedulable_tasks().await.len(), 50);

    // 2. Prepare atomic batch assignments (alternate between worker_1 and worker_2)
    let assignments: Vec<(TaskId, Uuid)> = task_ids
        .iter()
        .enumerate()
        .map(|(idx, tid)| {
            let target_worker = if idx % 2 == 0 { worker_1 } else { worker_2 };
            (*tid, target_worker)
        })
        .collect();

    // 3. Execute atomic batch transition under a single lock acquisition
    let scheduled = queue.schedule_tasks_batch(&assignments).await;
    assert_eq!(scheduled.len(), 50, "All 50 tasks should be successfully scheduled");

    // 4. Verify all tasks are in Scheduled state and assigned correctly
    for (tid, assigned_worker, _task) in scheduled {
        let state = queue.get_state(&tid).await.expect("task state must exist");
        assert_eq!(state, TaskState::Scheduled);

        let info = queue.get_task(&tid).await.expect("task info must exist");
        assert_eq!(info.assigned_worker_id, Some(assigned_worker));
    }

    // 5. Subsequent attempt to reschedule already Scheduled tasks should yield empty
    let duplicate_scheduled = queue.schedule_tasks_batch(&assignments).await;
    assert_eq!(duplicate_scheduled.len(), 0, "Already scheduled tasks cannot be rescheduled");
}

#[tokio::test]
async fn test_workload_scheduler_micro_batch_burst_accumulation() {
    let registry = WorkerRegistry::new();
    let queue = TaskQueue::new();
    let trigger = Arc::new(Notify::new());
    let config = SchedulerConfig::default()
        .with_micro_batch_window(Duration::from_millis(5))
        .with_max_batch_size(32);

    let scheduler = WorkloadScheduler::new(config, registry.clone(), queue.clone(), trigger);

    // Register 2 healthy workers with 40 cores each (capacity for 80 concurrent tasks)
    let worker_a = Uuid::new_v4();
    let worker_b = Uuid::new_v4();

    let (tx_a, mut rx_a) = tokio::sync::mpsc::channel(128);
    tokio::spawn(async move { while rx_a.recv().await.is_some() {} });
    let caps_a = WorkerCapabilities::new("worker-a", 40, 65536, false, false, None);
    registry
        .register(worker_a, caps_a, "127.0.0.1:9001".parse().unwrap(), tx_a, None)
        .await
        .expect("register worker a");

    let (tx_b, mut rx_b) = tokio::sync::mpsc::channel(128);
    tokio::spawn(async move { while rx_b.recv().await.is_some() {} });
    let caps_b = WorkerCapabilities::new("worker-b", 40, 65536, false, false, None);
    registry
        .register(worker_b, caps_b, "127.0.0.1:9002".parse().unwrap(), tx_b, None)
        .await
        .expect("register worker b");

    // Simulate burst submission of 40 tasks into the queue
    for i in 0..40 {
        let task = Task::new(
            TaskSpec::Command {
                program: "calc".into(),
                args: vec![format!("{}", i)],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::default(),
        );
        queue.submit(task).await.expect("submit task");
    }

    assert_eq!(queue.get_schedulable_tasks().await.len(), 40);

    // Call schedule_batch with max_batch_size = 32
    let first_batch = scheduler.schedule_batch(32).await.expect("first batch");
    assert_eq!(
        first_batch.assignments.len(),
        32,
        "First batch should cap at max_batch_size (32 tasks)"
    );

    // Next schedule_batch call processes the remaining 8 tasks
    let second_batch = scheduler.schedule_batch(32).await.expect("second batch");
    assert_eq!(
        second_batch.assignments.len(),
        8,
        "Second batch should process remaining 8 tasks"
    );

    // No more queued tasks to schedule
    let empty_batch = scheduler.schedule_batch(32).await.expect("empty batch");
    assert_eq!(empty_batch.assignments.len(), 0, "No remaining tasks should be scheduled");
}
