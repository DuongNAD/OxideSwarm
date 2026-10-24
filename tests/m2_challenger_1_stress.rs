//! Empirical Challenger Stress & Adversarial Test Suite for Milestone M2
//! (Requirement R2: High-Performance Zero-Copy I/O, QUIC Stream Multiplexing & Scheduler Micro-Batching)
//!
//! Adversarial focus:
//! 1. Zero-copy buffer operations: pointer sharing, slicing, into_bytes, memory efficiency.
//! 2. Binary non-UTF8 stdout/stderr handling: Cow conversions, lossy rendering, Display/Debug safety, no panics.
//! 3. Large payloads: 5MB+ binary buffers serialized & deserialized across both Bincode and JSON codecs.
//! 4. 2-Lane QUIC Stream Multiplexing: 5MB saturated data stream concurrent with rapid control heartbeats (Anti-HoL blocking).
//! 5. Scheduler queue micro-batching: high-concurrency burst submissions, concurrent cancellations, and atomic batch state transitions.
//! 6. Pipe bounded stream safety: 5MB binary overflow draining without deadlock.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use tokio_util::bytes::Bytes as RawBytes;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{
    deserialize_message, serialize_message, MasterMessage, MessageTransport, WireCodec,
    WorkerMessage,
};
use rusty_grid_core::task::{Bytes, Task, TaskId, TaskRequirements, TaskResult, TaskSpec};
use rusty_grid_core::transport::{BiStream, GridStream, STREAM_CONTROL, STREAM_DATA};
use rusty_grid_master::queue::{TaskQueue, TaskState};
use rusty_grid_master::registry::{WorkerInfo, WorkerRegistry, WorkerStatus};
use rusty_grid_master::scheduler::{
    ScheduleSkipReason, SchedulerConfig, SchedulingPolicy, WorkloadScheduler,
};
use rusty_grid_worker::runner::read_bounded_stream;

// =========================================================================
// 1. ZERO-COPY BUFFER ERGONOMICS & POINTER IDENTITY
// =========================================================================

#[test]
fn test_zero_copy_slicing_and_pointer_invariants() {
    // Allocate 1MB buffer
    let size = 1024 * 1024;
    let mut data = Vec::with_capacity(size);
    for i in 0..size {
        data.push((i % 256) as u8);
    }
    let raw = RawBytes::from(data);
    let raw_ptr = raw.as_ptr();

    let b = Bytes::from(raw.clone());
    assert_eq!(
        b.as_bytes().as_ptr(),
        raw_ptr,
        "Bytes wrapper must share identical base pointer"
    );

    // Multiple slices across disparate offsets
    let s1 = b.slice(100..200);
    let s2 = b.slice(500_000..600_000);
    let s3 = s2.slice(10_000..20_000);

    assert_eq!(s1.len(), 100);
    assert_eq!(s2.len(), 100_000);
    assert_eq!(s3.len(), 10_000);

    // Verify pointer offsets are zero-copy directly into raw allocation
    unsafe {
        assert_eq!(s1.as_bytes().as_ptr(), raw_ptr.add(100));
        assert_eq!(s2.as_bytes().as_ptr(), raw_ptr.add(500_000));
        assert_eq!(s3.as_bytes().as_ptr(), raw_ptr.add(510_000));
    }

    // Convert into inner bytes::Bytes and verify pointer is unchanged
    let inner_bytes = b.into_bytes();
    assert_eq!(inner_bytes.as_ptr(), raw_ptr);
}

// =========================================================================
// 2. NON-UTF8 BINARY STDOUT / STDERR ADVERSARIAL HANDLING
// =========================================================================

#[test]
fn test_binary_non_utf8_stdout_stderr_slicing_and_cow() {
    let task_id = TaskId::new();
    let worker_id = Uuid::new_v4();

    // Adversarial byte pattern: invalid UTF-8 sequences (0xFF, 0xFE, lone continuation bytes, null bytes)
    let non_utf8_stdout = vec![
        0xFF, 0xFE, 0xFD, 0x00, 0x80, 0x81, 0xC0, 0xAF, b'H', b'e', b'l', b'l', b'o', 0x00, 0xF0,
        0x28, 0x8C, 0x28, 0xFF,
    ];
    let non_utf8_stderr = vec![0x80, 0xBF, 0xC2, 0x00, 0xFF];

    let b_stdout = Bytes::from(non_utf8_stdout.clone());
    let b_stderr = Bytes::from(non_utf8_stderr.clone());

    // 1. Deref to str should NOT panic, should return empty or valid slice
    let deref_str: &str = &b_stdout;
    assert_eq!(
        deref_str, "",
        "Deref on invalid UTF-8 should fallback gracefully without panicking"
    );

    // 2. Display and Debug formatting should NOT panic
    // Display is lossy (U+FFFD for bad bytes); it used to blank the whole buffer to "".
    let display_output = format!("{}", b_stdout);
    assert_eq!(display_output, String::from_utf8_lossy(&non_utf8_stdout));
    assert!(display_output.contains("Hello"));

    let debug_output = format!("{:?}", b_stdout);
    assert!(
        debug_output.contains("\\xff") || debug_output.starts_with("b\""),
        "Debug format should format inner bytes safely without panicking"
    );

    // 3. Slicing on non-UTF8 buffer
    let sub_slice = b_stdout.slice(8..13);
    assert_eq!(sub_slice.as_bytes(), b"Hello");
    // Since sub_slice is valid UTF-8, deref should work
    assert_eq!(&*sub_slice, "Hello");

    // 4. TaskResult Cow conversion methods
    let result = TaskResult {
        worker_id,
        task_id,
        exit_code: 0,
        stdout: b_stdout,
        stderr: b_stderr,
        execution_time_ms: 100,
        is_gpu_executed: false,
        device_name: None,
        error: None,
        asset_bytes: None,
    };

    let cow_stdout = result.stdout_str();
    match cow_stdout {
        Cow::Owned(ref s) => {
            assert!(
                s.contains('\u{FFFD}'),
                "Lossy UTF-8 conversion should contain replacement character"
            );
            assert!(
                s.contains("Hello"),
                "Lossy UTF-8 conversion should preserve ASCII segments"
            );
        }
        Cow::Borrowed(_) => panic!("Expected Cow::Owned for non-UTF8 stdout"),
    }

    let cow_stderr = result.stderr_str();
    match cow_stderr {
        Cow::Owned(ref s) => {
            assert!(s.contains('\u{FFFD}'));
        }
        Cow::Borrowed(_) => panic!("Expected Cow::Owned for non-UTF8 stderr"),
    }
}

// =========================================================================
// 3. LARGE PAYLOAD STRESS: 5MB+ BINARY BUFFERS (BINCODE & JSON)
// =========================================================================

#[test]
fn test_large_payload_5mb_bincode_and_json_roundtrip() {
    let task_id = TaskId::new();
    let worker_id = Uuid::new_v4();

    // 5MB of non-UTF8 binary data
    let payload_size = 5 * 1024 * 1024;
    let mut large_data = Vec::with_capacity(payload_size);
    for i in 0..payload_size {
        // High byte patterns ensuring invalid UTF-8
        large_data.push(((i * 13 + 0x80) % 256) as u8);
    }

    let original_msg = WorkerMessage::TaskResult {
        worker_id,
        task_id,
        exit_code: 0,
        stdout: Bytes::from(large_data.clone()),
        stderr: Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        execution_time_ms: 5432,
        is_gpu_executed: true,
        device_name: None,
        error: None,
        asset_bytes: None,
    };

    // --- Bincode Wire Codec Roundtrip ---
    let t0 = Instant::now();
    let bincode_bytes = serialize_message(&original_msg, WireCodec::Bincode)
        .expect("Bincode serialization of 5MB payload must succeed");
    let serialize_time = t0.elapsed();

    let t1 = Instant::now();
    let (decoded_msg, codec): (WorkerMessage, WireCodec) = deserialize_message(&bincode_bytes)
        .expect("Bincode deserialization of 5MB payload must succeed");
    let deserialize_time = t1.elapsed();

    assert_eq!(codec, WireCodec::Bincode);
    assert_eq!(original_msg, decoded_msg);

    if let WorkerMessage::TaskResult { stdout, stderr, .. } = decoded_msg {
        assert_eq!(stdout.len(), payload_size);
        assert_eq!(stdout.as_bytes(), &large_data[..]);
        assert_eq!(stderr.as_bytes(), &[0xDE, 0xAD, 0xBE, 0xEF]);
    } else {
        panic!("Decoded message variant mismatch");
    }

    println!(
        "5MB Bincode roundtrip: serialize={:?}, deserialize={:?}",
        serialize_time, deserialize_time
    );

    // --- JSON Wire Codec Roundtrip (1MB subset for JSON performance and memory sanity) ---
    let json_payload_size = 1024 * 1024;
    let json_msg = WorkerMessage::TaskResult {
        worker_id,
        task_id,
        exit_code: 0,
        stdout: Bytes::from(large_data[..json_payload_size].to_vec()),
        stderr: Bytes::from_static(b"json error check"),
        execution_time_ms: 123,
        is_gpu_executed: false,
        device_name: None,
        error: None,
        asset_bytes: None,
    };

    let json_bytes = serialize_message(&json_msg, WireCodec::Json)
        .expect("JSON serialization of binary payload must succeed");
    let (json_decoded, codec): (WorkerMessage, WireCodec) = deserialize_message(&json_bytes)
        .expect("JSON deserialization of binary payload must succeed");
    assert_eq!(codec, WireCodec::Json);
    assert_eq!(json_msg, json_decoded);
}

// =========================================================================
// 4. PIPE BOUNDED STREAM: 5MB BINARY OVERFLOW & TRUNCATION DRAIN
// =========================================================================

#[tokio::test]
async fn test_read_bounded_stream_binary_overflow_and_drain() {
    let max_capture = 64 * 1024; // 64 KB limit
    let total_stream_bytes = 2 * 1024 * 1024; // 2 MB generated

    let (reader, mut writer) = tokio::io::duplex(32 * 1024);

    // Writer task generates 2MB of raw binary stream
    let writer_task = tokio::spawn(async move {
        let chunk = vec![0xEEu8; 16 * 1024];
        let mut written = 0;
        while written < total_stream_bytes {
            writer.write_all(&chunk).await.expect("write chunk");
            written += chunk.len();
        }
        drop(writer); // EOF
    });

    // Reader task captures bounded stream
    let (captured, truncated) = read_bounded_stream(reader, max_capture).await;
    writer_task
        .await
        .expect("writer task must finish without deadlock");

    assert!(truncated, "Stream should be flagged as truncated");
    // Max captured bytes + truncation message
    assert!(captured.len() >= max_capture);
    assert!(
        captured
            .as_bytes()
            .ends_with(b"[rusty_grid: output truncated after exceeding size limit]\n"),
        "Must contain truncation footer"
    );
    // Verify initial data chunk was captured correctly
    assert_eq!(&captured.as_bytes()[0..1024], &[0xEEu8; 1024]);
}

// =========================================================================
// 5. 2-LANE QUIC STREAM MULTIPLEXING: 5MB SATURATION & HEARTBEAT ISOLATION
// =========================================================================

#[cfg(feature = "p2p")]
#[tokio::test]
async fn test_quic_stream_multiplexing_5mb_saturation_and_heartbeat_isolation() {
    use iroh::endpoint::presets::N0;
    use iroh::endpoint::RelayMode;
    use rusty_grid_core::transport::GRID_ALPN;

    // Bind server and client endpoints on loopback
    let ep_server = iroh::Endpoint::builder(N0)
        .alpns(vec![GRID_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind server");

    let ep_client = iroh::Endpoint::builder(N0)
        .alpns(vec![GRID_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind client");

    let server_addr = ep_server.addr();
    let heavy_payload_size = 5 * 1024 * 1024; // 5 MB

    // Server accepts connection and demuxes Control vs Data streams
    let server_handle = tokio::spawn(async move {
        let incoming = ep_server
            .accept()
            .await
            .expect("accept")
            .await
            .expect("handshake");

        let (send_a, mut recv_a) = incoming.accept_bi().await.expect("stream a");
        let mut tag_a = [0u8; 1];
        recv_a.read_exact(&mut tag_a).await.expect("tag a");

        let (send_b, mut recv_b) = incoming.accept_bi().await.expect("stream b");
        let mut tag_b = [0u8; 1];
        recv_b.read_exact(&mut tag_b).await.expect("tag b");

        let (ctrl_send, ctrl_recv, _data_send, mut data_recv) = if tag_a[0] == STREAM_CONTROL {
            assert_eq!(tag_b[0], STREAM_DATA);
            (send_a, recv_a, send_b, recv_b)
        } else {
            assert_eq!(tag_a[0], STREAM_DATA);
            assert_eq!(tag_b[0], STREAM_CONTROL);
            (send_b, recv_b, send_a, recv_a)
        };

        let ctrl_stream = GridStream::P2p(BiStream::new(ctrl_recv, ctrl_send));
        let mut ctrl_transport = MessageTransport::new(ctrl_stream);

        // Data consumer draining 5MB saturated stream
        let data_task = tokio::spawn(async move {
            let mut total = 0usize;
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match data_recv.read(&mut buf).await {
                    Ok(Some(n)) if n > 0 => total += n,
                    _ => break,
                }
            }
            total
        });

        // Heartbeat responder
        let mut heartbeats_answered = 0;
        while let Ok(Some(msg)) = ctrl_transport.recv_msg::<WorkerMessage>().await {
            match msg {
                WorkerMessage::Heartbeat { timestamp, .. } => {
                    heartbeats_answered += 1;
                    let ack = MasterMessage::HeartbeatAck { timestamp };
                    ctrl_transport.send_msg(&ack).await.expect("send ack");
                }
                WorkerMessage::Disconnecting { .. } => break,
                _ => {}
            }
        }

        let total_data = data_task.await.expect("data sink");
        (heartbeats_answered, total_data)
    });

    // Client connects
    let conn = ep_client
        .connect(server_addr, GRID_ALPN)
        .await
        .expect("connect");

    // Open Control Lane
    let (mut ctrl_send, ctrl_recv) = conn.open_bi().await.expect("open ctrl");
    ctrl_send
        .write_all(&[STREAM_CONTROL])
        .await
        .expect("write ctrl tag");
    ctrl_send.flush().await.expect("flush ctrl tag");
    let ctrl_stream = GridStream::P2p(BiStream::new(ctrl_recv, ctrl_send));
    let mut ctrl_transport = MessageTransport::new(ctrl_stream);

    // Open Data Lane
    let (mut data_send, _data_recv) = conn.open_bi().await.expect("open data");
    data_send
        .write_all(&[STREAM_DATA])
        .await
        .expect("write data tag");
    data_send.flush().await.expect("flush data tag");

    // Concurrently saturate Data Lane with 5MB
    let data_producer = tokio::spawn(async move {
        let chunk = vec![0xBCu8; 64 * 1024];
        let mut sent = 0;
        while sent < heavy_payload_size {
            data_send.write_all(&chunk).await.expect("write data chunk");
            data_send.flush().await.expect("flush data chunk");
            sent += chunk.len();
        }
        data_send.shutdown().await.expect("shutdown data lane");
    });

    // Send 30 heartbeats across Control Lane and measure RTT under full data saturation
    let worker_id = Uuid::new_v4();
    let num_heartbeats = 30;
    let mut latencies = Vec::new();

    for i in 0..num_heartbeats {
        let t0 = Instant::now();
        let hb = WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 5000 + i,
            active_tasks: 2,
            cpu_usage_pct: 25.0,
            ram_available_mb: 4096,
        };
        ctrl_transport.send_msg(&hb).await.expect("send heartbeat");
        let ack: Option<MasterMessage> =
            ctrl_transport.recv_msg().await.expect("recv heartbeat ack");
        let rtt = t0.elapsed();
        latencies.push(rtt);

        match ack {
            Some(MasterMessage::HeartbeatAck { timestamp }) => {
                assert_eq!(timestamp, 5000 + i);
            }
            other => panic!("Unexpected message on control lane: {:?}", other),
        }
    }

    // Disconnect
    ctrl_transport
        .send_msg(&WorkerMessage::Disconnecting {
            worker_id,
            reason: "Challenger stress test finished".into(),
        })
        .await
        .expect("disconnect");

    data_producer.await.expect("producer finished");
    let (heartbeats_answered, total_data_received) = server_handle.await.expect("server finished");

    assert_eq!(heartbeats_answered, num_heartbeats);
    assert_eq!(total_data_received, heavy_payload_size);

    let max_latency = latencies.iter().max().copied().unwrap_or_default();
    let avg_latency = latencies.iter().sum::<Duration>() / latencies.len() as u32;

    println!(
        "QUIC 5MB Saturation: Heartbeat avg_latency={:?}, max_latency={:?}",
        avg_latency, max_latency
    );

    assert!(
        avg_latency < Duration::from_millis(50),
        "Control lane average latency must stay sub-50ms under 5MB saturation (got {:?})",
        avg_latency
    );
}

// =========================================================================
// 6. SCHEDULER MICRO-BATCHING CONCURRENCY & BURST RACE STRESS
// =========================================================================

#[tokio::test]
async fn test_scheduler_micro_batch_burst_concurrency_race() {
    let queue = TaskQueue::new();
    let registry = WorkerRegistry::new();
    let trigger = Arc::new(Notify::new());
    let config = SchedulerConfig::default()
        .with_micro_batch_window(Duration::from_millis(5))
        .with_max_batch_size(64);

    let scheduler =
        WorkloadScheduler::new(config, registry.clone(), queue.clone(), trigger.clone());

    // Register 4 workers: 2 GPU workers (32 cores each) and 2 CPU-only workers (32 cores each)
    let mut workers = Vec::new();
    for i in 0..4 {
        let wid = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let is_gpu = i % 2 == 0;
        let caps = WorkerCapabilities::new(
            format!("worker-{}", i),
            64,
            32768,
            is_gpu,
            is_gpu,
            if is_gpu {
                Some("Virtual GPU".into())
            } else {
                None
            },
        );
        registry
            .register(
                wid,
                caps,
                format!("127.0.0.1:910{}", i).parse().unwrap(),
                tx,
                None,
            )
            .await
            .expect("register worker");
        workers.push(wid);
    }

    // 10 concurrent tasks submit 20 tasks each (200 total tasks submitted in burst)
    let total_tasks = 200;
    let mut submit_handles = Vec::new();
    let all_submitted_ids = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    for batch_idx in 0..10 {
        let q = queue.clone();
        let ids_collector = Arc::clone(&all_submitted_ids);
        submit_handles.push(tokio::spawn(async move {
            let mut ids = Vec::new();
            for i in 0..20 {
                let task_num = batch_idx * 20 + i;
                let is_gpu_task = task_num % 4 == 0;
                let spec = if is_gpu_task {
                    TaskSpec::new_gpu_compute("matrix_mult", 32)
                } else {
                    TaskSpec::new_command("echo", vec![format!("task-{}", task_num)])
                };
                let reqs = if is_gpu_task {
                    TaskRequirements::gpu(30)
                } else {
                    TaskRequirements::generic(1, 30)
                };
                let task = Task::new(spec, reqs);
                let tid = task.id;
                q.submit(task).await.expect("submit task");
                ids.push(tid);
            }
            let mut lock = ids_collector.lock().await;
            lock.extend(ids);
        }));
    }

    for h in submit_handles {
        h.await.expect("submit task finished");
    }

    let submitted_tids = all_submitted_ids.lock().await.clone();
    assert_eq!(submitted_tids.len(), total_tasks);

    // Concurrently cancel 30 tasks from the submitted set
    let cancel_set: HashSet<TaskId> = submitted_tids.iter().take(30).copied().collect();
    for tid in &cancel_set {
        let _ = queue
            .cancel_task(tid, Some("Concurrent cancellation stress".to_string()))
            .await;
    }

    // Concurrently run schedule_batch in multiple scheduling passes
    let mut total_scheduled = 0;
    let mut scheduled_tasks = Vec::new();

    for _ in 0..10 {
        let report = scheduler.schedule_batch(64).await.expect("schedule batch");
        total_scheduled += report.assignments.len();
        scheduled_tasks.extend(report.assignments);
        if queue.get_schedulable_tasks().await.is_empty() {
            break;
        }
    }

    // Invariants verification:
    // 1. No cancelled task was scheduled
    for assignment in &scheduled_tasks {
        assert!(
            !cancel_set.contains(&assignment.task_id),
            "Cancelled task {} must NOT be scheduled",
            assignment.task_id
        );
    }

    // 2. No duplicate task assignments
    let mut unique_scheduled = HashSet::new();
    for a in &scheduled_tasks {
        assert!(
            unique_scheduled.insert(a.task_id),
            "Duplicate task assignment detected for {}",
            a.task_id
        );
    }

    // 3. All non-cancelled tasks (170) should be scheduled
    let expected_scheduled = total_tasks - cancel_set.len();
    assert_eq!(
        total_scheduled, expected_scheduled,
        "All remaining non-cancelled tasks must be scheduled"
    );

    // 4. Verify queue state consistency
    for tid in &cancel_set {
        let state = queue.get_state(tid).await.expect("cancelled state exists");
        assert_eq!(state, TaskState::Cancelled);
    }

    for a in &scheduled_tasks {
        let state = queue
            .get_state(&a.task_id)
            .await
            .expect("scheduled state exists");
        assert_eq!(state, TaskState::Scheduled);
    }
}

// =========================================================================
// 7. SCHEDULER BATCH ATOMICITY & BOUNDARY CONDITIONS
// =========================================================================

#[tokio::test]
async fn test_scheduler_batch_atomicity_and_edge_cases() {
    let queue = TaskQueue::new();
    let worker_id = Uuid::new_v4();

    // 1. Empty slice handling
    let empty_res = queue.schedule_tasks_batch(&[]).await;
    assert!(empty_res.is_empty());

    // 2. Non-existent task IDs
    let fake_id = TaskId::new();
    let fake_res = queue.schedule_tasks_batch(&[(fake_id, worker_id)]).await;
    assert!(
        fake_res.is_empty(),
        "Non-existent task IDs must be skipped gracefully"
    );

    // 3. Batch rescheduling idempotency
    let task = Task::new(
        TaskSpec::new_command("true", vec![]),
        TaskRequirements::default(),
    );
    let tid = task.id;
    queue.submit(task).await.expect("submit");

    let first_schedule = queue.schedule_tasks_batch(&[(tid, worker_id)]).await;
    assert_eq!(first_schedule.len(), 1);

    // Immediate second schedule attempt should return empty (already scheduled)
    let second_schedule = queue.schedule_tasks_batch(&[(tid, worker_id)]).await;
    assert!(
        second_schedule.is_empty(),
        "Already scheduled task must not be rescheduled"
    );
}

// =========================================================================
// 8. ADVERSARIAL SCHEDULER PRIORITY & TOPOLOGY VERIFICATION
// =========================================================================

fn make_test_worker(
    name: &str,
    cores: usize,
    ram_mb: u64,
    has_gpu: bool,
    is_simulated_gpu: bool,
    active_tasks: usize,
) -> WorkerInfo {
    let caps = WorkerCapabilities::new(name, cores, ram_mb, has_gpu, is_simulated_gpu, None);
    WorkerInfo {
        worker_id: Uuid::new_v4(),
        session_id: 1,
        capabilities: caps,
        status: WorkerStatus::Connected,
        last_heartbeat_timestamp: 1000,
        registered_timestamp: 1000,
        active_tasks,
        remote_addr: "127.0.0.1:9000".into(),
        cpu_usage_pct: 0.0,
        ram_available_mb: ram_mb,
    }
}

/// Adversarial Topology 1:
/// 1 Real GPU (saturated), 2 Simulated GPUs (idle) -> verify simulated GPU takes task.
#[test]
fn test_adversarial_topology_1_real_saturated_2_simulated_idle() {
    let config = SchedulerConfig {
        core_concurrency_multiplier: 1.0,
        ..Default::default()
    };

    // 1 Real GPU worker: 2 cores, 8192 MB RAM, active_tasks = 2 (saturated)
    let w_real = make_test_worker("worker-real-wgpu", 2, 8192, true, false, 2);
    // 2 Simulated GPU workers: 4 cores, 8192 MB RAM each, active_tasks = 0 (idle)
    let w_sim1 = make_test_worker("worker-sim-gpu-1", 4, 8192, true, true, 0);
    let w_sim2 = make_test_worker("worker-sim-gpu-2", 4, 8192, true, true, 0);
    // 1 CPU-only worker: 8 cores, active_tasks = 0 (idle)
    let w_cpu = make_test_worker("worker-cpu-only", 8, 16384, false, false, 0);

    let workers = vec![w_real.clone(), w_sim1.clone(), w_sim2.clone(), w_cpu.clone()];

    let gpu_task = Task::new(
        TaskSpec::gpu_compute("gemm_kernel", 64),
        TaskRequirements::gpu(30),
    );

    let report = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&gpu_task), &workers);

    assert_eq!(report.assignments.len(), 1, "Exactly one task should be placed");
    let assigned_id = report.assignments[0].worker_id;

    // Simulated GPU MUST take the task because real GPU is saturated
    assert!(
        assigned_id == w_sim1.worker_id || assigned_id == w_sim2.worker_id,
        "Simulated GPU must take task when real GPU is saturated"
    );
    assert_ne!(assigned_id, w_real.worker_id, "Saturated real GPU must not receive task");
    assert_ne!(assigned_id, w_cpu.worker_id, "CPU-only worker must never receive GPU task");
}

/// Adversarial Topology 2:
/// 2 Real GPUs (1 idle, 1 busy), 2 Simulated GPUs (idle) -> verify idle real GPU takes task, not simulated GPU.
#[test]
fn test_adversarial_topology_2_real_1_idle_1_busy_2_simulated_idle() {
    // 2 Real GPUs:
    // Real GPU 1: idle (active_tasks = 0)
    let w_real_idle = make_test_worker("worker-real-idle", 4, 8192, true, false, 0);
    // Real GPU 2: busy (active_tasks = 1)
    let w_real_busy = make_test_worker("worker-real-busy", 4, 8192, true, false, 1);

    // 2 Simulated GPUs: both idle
    let w_sim1 = make_test_worker("worker-sim-1", 4, 8192, true, true, 0);
    let w_sim2 = make_test_worker("worker-sim-2", 4, 8192, true, true, 0);

    // 1 CPU worker: idle
    let w_cpu = make_test_worker("worker-cpu", 8, 16384, false, false, 0);

    let workers = vec![
        w_real_idle.clone(),
        w_real_busy.clone(),
        w_sim1.clone(),
        w_sim2.clone(),
        w_cpu.clone(),
    ];

    let gpu_task = Task::new(
        TaskSpec::gpu_compute("conv2d_layer", 128),
        TaskRequirements::gpu(30),
    );

    // 1. Verify under LeastLoaded policy: idle real GPU takes task
    let least_loaded_config = SchedulerConfig {
        policy: SchedulingPolicy::LeastLoaded,
        ..Default::default()
    };
    let report_ll = WorkloadScheduler::matchmake(
        &least_loaded_config,
        std::slice::from_ref(&gpu_task),
        &workers,
    );
    assert_eq!(report_ll.assignments.len(), 1);
    assert_eq!(
        report_ll.assignments[0].worker_id, w_real_idle.worker_id,
        "LeastLoaded must pick idle real GPU over busy real GPU and simulated GPUs"
    );

    // 2. Verify under WeightedLeastLoaded policy: idle real GPU takes task
    let weighted_config = SchedulerConfig {
        policy: SchedulingPolicy::WeightedLeastLoaded,
        ..Default::default()
    };
    let report_wll =
        WorkloadScheduler::matchmake(&weighted_config, std::slice::from_ref(&gpu_task), &workers);
    assert_eq!(report_wll.assignments.len(), 1);
    assert_eq!(
        report_wll.assignments[0].worker_id, w_real_idle.worker_id,
        "WeightedLeastLoaded must pick idle real GPU"
    );

    // 3. Verify under RoundRobin policy: real GPU takes task, simulated GPUs NEVER selected
    let rr_config = SchedulerConfig {
        policy: SchedulingPolicy::RoundRobin,
        ..Default::default()
    };
    let report_rr =
        WorkloadScheduler::matchmake(&rr_config, std::slice::from_ref(&gpu_task), &workers);
    assert_eq!(report_rr.assignments.len(), 1);
    let assigned_rr = report_rr.assignments[0].worker_id;
    assert!(
        assigned_rr == w_real_idle.worker_id || assigned_rr == w_real_busy.worker_id,
        "RoundRobin must select a real GPU, never simulated GPU"
    );
    assert_ne!(assigned_rr, w_sim1.worker_id);
    assert_ne!(assigned_rr, w_sim2.worker_id);
    assert_ne!(assigned_rr, w_cpu.worker_id);
}

/// Adversarial Concurrency Test:
/// RoundRobin behavior under heavy concurrent task submissions across real and simulated GPU workers.
#[test]
fn test_adversarial_round_robin_heavy_concurrent_submissions() {
    let config = SchedulerConfig {
        policy: SchedulingPolicy::RoundRobin,
        core_concurrency_multiplier: 1.0,
        ..Default::default()
    };

    // 2 Real GPUs (4 cores = 4 slots each => 8 total real slots)
    let w_real1 = make_test_worker("w-real-1", 4, 8192, true, false, 0);
    let w_real2 = make_test_worker("w-real-2", 4, 8192, true, false, 0);

    // 2 Simulated GPUs (4 cores = 4 slots each => 8 total sim slots)
    let w_sim1 = make_test_worker("w-sim-1", 4, 8192, true, true, 0);
    let w_sim2 = make_test_worker("w-sim-2", 4, 8192, true, true, 0);

    let workers = vec![w_real1.clone(), w_real2.clone(), w_sim1.clone(), w_sim2.clone()];

    // Generate 16 tasks (demanding 1 core each)
    let tasks: Vec<Task> = (0..16)
        .map(|i| {
            Task::new(
                TaskSpec::gpu_compute(format!("kernel_{i}"), 10),
                TaskRequirements::gpu(10),
            )
        })
        .collect();

    let report = WorkloadScheduler::matchmake(&config, &tasks, &workers);
    assert_eq!(
        report.assignments.len(),
        16,
        "All 16 available slots across real and sim GPUs must be filled"
    );

    // Count assignments per worker
    let mut real1_count = 0;
    let mut real2_count = 0;
    let mut sim1_count = 0;
    let mut sim2_count = 0;

    for a in &report.assignments {
        if a.worker_id == w_real1.worker_id {
            real1_count += 1;
        } else if a.worker_id == w_real2.worker_id {
            real2_count += 1;
        } else if a.worker_id == w_sim1.worker_id {
            sim1_count += 1;
        } else if a.worker_id == w_sim2.worker_id {
            sim2_count += 1;
        }
    }

    // Both real GPUs must be completely filled to their maximum slot capacity (4 slots each)
    assert_eq!(real1_count, 4, "Real GPU 1 must be fully utilized (4 tasks)");
    assert_eq!(real2_count, 4, "Real GPU 2 must be fully utilized (4 tasks)");
    // Remaining 8 tasks must spill over to simulated GPUs (4 each)
    assert_eq!(sim1_count, 4, "Sim GPU 1 must receive remaining 4 tasks");
    assert_eq!(sim2_count, 4, "Sim GPU 2 must receive remaining 4 tasks");

    // Verify ordering: The first 8 tasks MUST all be assigned to real GPUs before any simulated GPU
    for a in &report.assignments[0..8] {
        assert!(
            a.worker_id == w_real1.worker_id || a.worker_id == w_real2.worker_id,
            "Initial 8 tasks must all be placed on real GPUs"
        );
    }
    // Tasks 8..16 must be assigned to simulated GPUs
    for a in &report.assignments[8..16] {
        assert!(
            a.worker_id == w_sim1.worker_id || a.worker_id == w_sim2.worker_id,
            "Overflow tasks must spill over to simulated GPUs"
        );
    }
}

/// Adversarial Concurrency Test:
/// LeastLoaded behavior under heavy concurrent task submissions with saturation threshold.
#[test]
fn test_adversarial_least_loaded_heavy_concurrent_submissions() {
    let config = SchedulerConfig {
        policy: SchedulingPolicy::LeastLoaded,
        core_concurrency_multiplier: 1.0,
        ..Default::default()
    };

    // 2 Real GPUs:
    // Real 1 has 2 cores (2 max slots)
    let w_real1 = make_test_worker("w-real-1", 2, 8192, true, false, 0);
    // Real 2 has 4 cores (4 max slots)
    let w_real2 = make_test_worker("w-real-2", 4, 8192, true, false, 0);

    // 2 Simulated GPUs (4 cores each = 8 total sim slots)
    let w_sim1 = make_test_worker("w-sim-1", 4, 8192, true, true, 0);
    let w_sim2 = make_test_worker("w-sim-2", 4, 8192, true, true, 0);

    // 1 CPU worker (16 cores)
    let w_cpu = make_test_worker("w-cpu", 16, 32768, false, false, 0);

    let workers = vec![
        w_real1.clone(),
        w_real2.clone(),
        w_sim1.clone(),
        w_sim2.clone(),
        w_cpu.clone(),
    ];

    // Total GPU capacity = 2 + 4 + 4 + 4 = 14 slots.
    // Submit 30 tasks in burst to test saturation handling.
    let tasks: Vec<Task> = (0..30)
        .map(|i| {
            Task::new(
                TaskSpec::gpu_compute(format!("burst_kernel_{i}"), 10),
                TaskRequirements::gpu(10),
            )
        })
        .collect();

    let report = WorkloadScheduler::matchmake(&config, &tasks, &workers);

    // Exactly 14 tasks placed (saturating all GPU workers)
    assert_eq!(
        report.assignments.len(),
        14,
        "Exactly 14 GPU tasks must be placed to saturate cluster"
    );
    // Exactly 16 tasks skipped due to NoGpuWorkersAvailable
    assert_eq!(
        report.skipped.len(),
        16,
        "Excess 16 tasks must be skipped cleanly"
    );
    for (_, reason) in &report.skipped {
        assert_eq!(*reason, ScheduleSkipReason::NoGpuWorkersAvailable);
    }

    // Verify Real GPU capacity was completely exhausted (2 on w_real1, 4 on w_real2)
    let real1_assigned = report
        .assignments
        .iter()
        .filter(|a| a.worker_id == w_real1.worker_id)
        .count();
    let real2_assigned = report
        .assignments
        .iter()
        .filter(|a| a.worker_id == w_real2.worker_id)
        .count();
    let sim1_assigned = report
        .assignments
        .iter()
        .filter(|a| a.worker_id == w_sim1.worker_id)
        .count();
    let sim2_assigned = report
        .assignments
        .iter()
        .filter(|a| a.worker_id == w_sim2.worker_id)
        .count();
    let cpu_assigned = report
        .assignments
        .iter()
        .filter(|a| a.worker_id == w_cpu.worker_id)
        .count();

    assert_eq!(real1_assigned, 2, "Real GPU 1 saturated at 2 tasks");
    assert_eq!(real2_assigned, 4, "Real GPU 2 saturated at 4 tasks");
    assert_eq!(sim1_assigned, 4, "Sim GPU 1 saturated at 4 tasks");
    assert_eq!(sim2_assigned, 4, "Sim GPU 2 saturated at 4 tasks");
    assert_eq!(cpu_assigned, 0, "CPU worker must never receive GPU task");
}

/// Adversarial Concurrency Test with Live Async TaskQueue & Multi-threaded Task Submissions.
#[tokio::test]
async fn test_adversarial_live_queue_concurrent_gpu_submissions() {
    let queue = TaskQueue::new();
    let registry = WorkerRegistry::new();
    let trigger = Arc::new(Notify::new());
    let config = SchedulerConfig {
        policy: SchedulingPolicy::LeastLoaded,
        core_concurrency_multiplier: 1.0,
        ..Default::default()
    };

    let scheduler =
        WorkloadScheduler::new(config, registry.clone(), queue.clone(), trigger.clone());

    // Register 1 Real GPU (4 slots) and 1 Simulated GPU (4 slots)
    let w_real = Uuid::new_v4();
    let (tx_real, mut rx_real) = tokio::sync::mpsc::channel(256);
    tokio::spawn(async move { while rx_real.recv().await.is_some() {} });
    let caps_real = WorkerCapabilities::new("w-real", 4, 8192, true, false, None);
    registry
        .register(
            w_real,
            caps_real,
            "127.0.0.1:9110".parse().unwrap(),
            tx_real,
            None,
        )
        .await
        .expect("register real gpu");

    let w_sim = Uuid::new_v4();
    let (tx_sim, mut rx_sim) = tokio::sync::mpsc::channel(256);
    tokio::spawn(async move { while rx_sim.recv().await.is_some() {} });
    let caps_sim = WorkerCapabilities::new("w-sim", 4, 8192, true, true, None);
    registry
        .register(
            w_sim,
            caps_sim,
            "127.0.0.1:9111".parse().unwrap(),
            tx_sim,
            None,
        )
        .await
        .expect("register sim gpu");

    // Spawn 10 concurrent tokio tasks submitting 5 tasks each (50 total tasks)
    let mut handles = Vec::new();
    for thread_idx in 0..10 {
        let q = queue.clone();
        handles.push(tokio::spawn(async move {
            for task_idx in 0..5 {
                let task = Task::new(
                    TaskSpec::gpu_compute(format!("concurrent_{thread_idx}_{task_idx}"), 10),
                    TaskRequirements::gpu(10),
                );
                q.submit(task).await.expect("submit task");
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Run scheduler pass
    let report = scheduler.schedule_batch(64).await.expect("schedule batch");
    assert_eq!(
        report.assignments.len(),
        8,
        "Both 4-slot workers saturated at 8 tasks"
    );

    // All real slots must be used (4 tasks on real GPU), remaining 4 tasks on simulated GPU
    let real_count = report
        .assignments
        .iter()
        .filter(|a| a.worker_id == w_real)
        .count();
    let sim_count = report
        .assignments
        .iter()
        .filter(|a| a.worker_id == w_sim)
        .count();
    assert_eq!(real_count, 4, "Real GPU must receive 4 tasks");
    assert_eq!(sim_count, 4, "Simulated GPU must receive 4 tasks");
}

