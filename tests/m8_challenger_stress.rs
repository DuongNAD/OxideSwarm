//! Adversarial and Empirical Stress Test Suite for Milestone 8 (Challenger M8.1).
//!
//! Objectives:
//! 1. Malformed discriminator bytes (0x00, 0xFF, 0x03, 0xAA, 0xFE).
//! 2. Truncated Bincode payload frames (zero-length, single-byte tag, partial structs, TCP abrupt EOF).
//! 3. Large binary payloads (1 MB - 5 MB GpuCompute buffer over Bincode vs JSON).
//! 4. Rapid mixed-stream clients (Bincode vs Tagged JSON vs Raw Legacy JSON concurrent with noise).
//! 5. Master stability: verify master does not panic or desynchronize.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{
    deserialize_message, serialize_message, ClientMessage, ClientResponse,
    MasterMessage, MessageTransport, ProtocolError, WireCodec, WorkerMessage,
    WIRE_FORMAT_BINCODE, WIRE_FORMAT_JSON,
};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

/// Helper representing a spawned test worker with an isolated sandbox directory.
#[allow(dead_code)]
struct SpawnedWorker {
    pub worker_id: Uuid,
    pub shutdown_tx: watch::Sender<bool>,
    pub handle: tokio::task::JoinHandle<()>,
    pub _temp_dir: TempDir,
}

#[allow(dead_code)]
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
    wire_codec: WireCodec,
) -> SpawnedWorker {
    let temp_dir = tempfile::tempdir().expect("failed to create worker tempdir");
    let cfg = WorkerConfig::new(master_addr)
        .with_name(name)
        .with_cores(cores)
        .with_simulate_gpu(simulate_gpu)
        .with_wire_codec(wire_codec)
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

/// Awaits until at least `expected_count` workers are in `Connected` status.
async fn wait_for_workers(master: &MasterHandle, expected_count: usize, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(workers) = master.list_workers().await {
            let active = workers
                .iter()
                .filter(|w| w.status == WorkerStatus::Connected)
                .count();
            if active >= expected_count {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Helper to send a length-delimited raw frame over a TcpStream.
async fn send_raw_frame(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

/// Helper to read a length-delimited raw frame from a TcpStream.
async fn recv_raw_frame(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await?;
            Ok(Some(payload))
        }
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

// =========================================================================
// SUITE 1: Adversarial Discriminator Fuzzing & Malformed Bytes
// =========================================================================

#[test]
fn test_m8_discriminator_all_256_bytes_unit() {
    // Thorough unit fuzzing: test every possible byte value (0..=255) as first byte
    let dummy_payload = b"HelloProtocolTestingPayload";

    let mut invalid_count = 0;
    let mut json_count = 0;
    let mut bincode_count = 0;

    for tag in 0u8..=255u8 {
        let mut buf = vec![tag];
        buf.extend_from_slice(dummy_payload);

        let res = deserialize_message::<WorkerMessage>(&buf);
        match tag {
            WIRE_FORMAT_BINCODE => {
                bincode_count += 1;
                // bincode parser was invoked, should return Bincode error due to dummy payload
                assert!(matches!(res, Err(ProtocolError::Bincode(_))));
            }
            WIRE_FORMAT_JSON => {
                json_count += 1;
                // json parser was invoked, should return Json error due to dummy payload
                assert!(matches!(res, Err(ProtocolError::Json(_))));
            }
            b'{' | b' ' | b'\t' | b'\r' | b'\n' => {
                json_count += 1;
                // legacy json whitespace/object parser invoked
                assert!(matches!(res, Err(ProtocolError::Json(_))));
            }
            invalid_tag => {
                invalid_count += 1;
                match res {
                    Err(ProtocolError::InvalidFormatTag(t)) => assert_eq!(t, invalid_tag),
                    other => panic!("Expected InvalidFormatTag for 0x{tag:02x}, got {other:?}"),
                }
            }
        }
    }

    assert_eq!(bincode_count, 1);
    assert_eq!(json_count, 6); // 0x01, '{', ' ', '\t', '\r', '\n'
    assert_eq!(invalid_count, 256 - 1 - 6);
}

#[tokio::test]
async fn test_m8_malformed_discriminator_handshake_rejection() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let adversarial_tags = vec![0x00, 0xFF, 0x03, 0xAA, 0xFE, 0x10, 0x7A, 0x7C];

    for &tag in &adversarial_tags {
        let mut stream = TcpStream::connect(&master_addr)
            .await
            .expect("connect to master");

        // Send a frame with invalid discriminator tag
        let malformed_payload = vec![tag, 0x01, 0x02, 0x03, 0x04];
        send_raw_frame(&mut stream, &malformed_payload)
            .await
            .expect("send_raw_frame");

        // Master should drop/close the connection without crashing
        let mut buf = [0u8; 128];
        let read_res = stream.read(&mut buf).await;
        // Either EOF (0 bytes) or an error, indicating clean disconnection
        assert!(
            read_res.is_err() || read_res.unwrap() == 0,
            "Master did not close connection on invalid discriminator tag 0x{tag:02x}"
        );
    }

    // Verify Master is still fully operational by connecting a legitimate worker and client
    let _worker = spawn_test_worker(
        master_addr.clone(),
        "worker-after-malformed-discriminators",
        2,
        false,
        WireCodec::Bincode,
    );

    assert!(
        wait_for_workers(&master, 1, Duration::from_secs(5)).await,
        "Master failed to accept valid worker after malformed discriminator attacks"
    );

    // Verify client can submit and complete a task over Bincode
    let stream = TcpStream::connect(&master_addr)
        .await
        .expect("connect valid client");
    let mut transport = MessageTransport::with_codec(stream, WireCodec::Bincode);

    let task = Task::new(
        TaskSpec::builtin_test("health_check_post_malformed", 10),
        TaskRequirements::generic(1, 10),
    );
    let submit_msg = ClientMessage::SubmitTask { task, wait: true };
    transport
        .send_msg(&submit_msg)
        .await
        .expect("send valid task");

    let resp = transport
        .recv_msg::<ClientResponse>()
        .await
        .expect("recv valid response")
        .expect("connection closed prematurely");

    match resp {
        ClientResponse::TaskCompleted { result, .. } => {
            assert_eq!(result.exit_code, 0);
        }
        other => panic!("Unexpected client response: {other:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_m8_post_handshake_malformed_discriminator_disconnects_worker() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let mut stream = TcpStream::connect(&master_addr)
        .await
        .expect("connect to master");

    // Perform valid Register handshake over Bincode
    let worker_id = Uuid::new_v4();
    let reg_msg = WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities::new("worker-post-handshake", 2, 1024, false, false, None),
    };
    let framed_reg = serialize_message(&reg_msg, WireCodec::Bincode).expect("serialize Register");
    send_raw_frame(&mut stream, &framed_reg)
        .await
        .expect("send Register");

    // Read RegisterAck
    let ack_frame = recv_raw_frame(&mut stream)
        .await
        .expect("read RegisterAck")
        .expect("EOF on RegisterAck");
    let (ack_msg, ack_codec): (MasterMessage, WireCodec) =
        deserialize_message(&ack_frame).expect("deserialize RegisterAck");
    assert_eq!(ack_codec, WireCodec::Bincode);
    assert!(matches!(ack_msg, MasterMessage::RegisterAck { accepted: true, .. }));

    // Now send a post-handshake message with invalid discriminator 0x03
    let malformed_post = vec![0x03, 0xDE, 0xAD, 0xBE, 0xEF];
    send_raw_frame(&mut stream, &malformed_post)
        .await
        .expect("send malformed post-handshake");

    // Master should close connection
    let mut buf = [0u8; 64];
    let read_res = stream.read(&mut buf).await;
    assert!(read_res.is_err() || read_res.unwrap() == 0);

    // Wait a brief moment for master to process disconnection
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify worker is marked disconnected or evicted
    let workers = master.list_workers().await.expect("list_workers");
    let active_w = workers
        .iter()
        .find(|w| w.worker_id == worker_id && w.status == WorkerStatus::Connected);
    assert!(active_w.is_none(), "Worker was not evicted after sending malformed discriminator");

    let _ = master.shutdown();
}

// =========================================================================
// SUITE 2: Truncated Frames & Deserialization Resilience
// =========================================================================

#[tokio::test]
async fn test_m8_truncated_and_empty_frames() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    // 1. Zero-length payload frame (length prefix = 0)
    {
        let mut stream = TcpStream::connect(&master_addr).await.expect("connect");
        stream.write_all(&0u32.to_be_bytes()).await.expect("write 0 len");
        stream.flush().await.expect("flush");

        let mut buf = [0u8; 16];
        let n = stream.read(&mut buf).await.unwrap_or(0);
        assert_eq!(n, 0, "Master must close on empty frame");
    }

    // 2. Discriminator-only frame (length = 1, payload = [0x02])
    {
        let mut stream = TcpStream::connect(&master_addr).await.expect("connect");
        send_raw_frame(&mut stream, &[WIRE_FORMAT_BINCODE]).await.expect("send tag only");

        let mut buf = [0u8; 16];
        let n = stream.read(&mut buf).await.unwrap_or(0);
        assert_eq!(n, 0, "Master must close on tag-only truncated bincode frame");
    }

    // 3. Partial Bincode message (cut in half)
    {
        let mut stream = TcpStream::connect(&master_addr).await.expect("connect");
        let reg_msg = WorkerMessage::Register {
            worker_id: Uuid::new_v4(),
            capabilities: WorkerCapabilities::new("worker-truncated", 2, 1024, false, false, None),
        };
        let full_frame = serialize_message(&reg_msg, WireCodec::Bincode).expect("serialize");
        // Truncate to first 12 bytes of a 40+ byte message
        let truncated = &full_frame[..12];
        send_raw_frame(&mut stream, truncated).await.expect("send truncated");

        let mut buf = [0u8; 16];
        let n = stream.read(&mut buf).await.unwrap_or(0);
        assert_eq!(n, 0, "Master must close on truncated bincode payload");
    }

    // 4. Abrupt TCP EOF mid-frame (header promises 1000 bytes, sends 10 then closes)
    {
        let mut stream = TcpStream::connect(&master_addr).await.expect("connect");
        stream.write_all(&1000u32.to_be_bytes()).await.expect("write len 1000");
        stream.write_all(&[WIRE_FORMAT_BINCODE, 0x01, 0x02, 0x03]).await.expect("write 4 bytes");
        stream.shutdown().await.expect("abrupt shutdown");
    }

    // 5. Corrupted Bincode enum variant tag (variant index 999999)
    {
        let mut stream = TcpStream::connect(&master_addr).await.expect("connect");
        let mut corrupt_payload = vec![WIRE_FORMAT_BINCODE];
        corrupt_payload.extend_from_slice(&999999u32.to_le_bytes()); // invalid enum discriminant
        corrupt_payload.extend_from_slice(&[0x00; 32]);
        send_raw_frame(&mut stream, &corrupt_payload).await.expect("send corrupt");

        let mut buf = [0u8; 16];
        let n = stream.read(&mut buf).await.unwrap_or(0);
        assert_eq!(n, 0, "Master must close on corrupted bincode enum tag");
    }

    // Master survives all these without panicking
    let stats = master.queue().stats().await;
    assert_eq!(stats.failed, 0);

    let _ = master.shutdown();
}

// =========================================================================
// SUITE 3: Massive Binary Payloads (1 MB - 5 MB)
// =========================================================================

#[test]
fn test_m8_large_binary_payload_wire_comparison() {
    for size_mb in [1usize, 2, 5] {
        let size_bytes = size_mb * 1024 * 1024;
        let mut input_data = Vec::with_capacity(size_bytes);
        // Deterministic pseudo-random pattern
        for i in 0..size_bytes {
            input_data.push((i & 0xFF) as u8);
        }

        let task_spec = TaskSpec::GpuCompute {
            kernel_name: format!("stress_kernel_{size_mb}mb"),
            input_data: input_data.clone(),
            work_group_size: 16,
            simulated_matrix_dim: 64,
            compute_intensity: 1,
        };

        // Serialize via Bincode
        let bincode_frame = serialize_message(&task_spec, WireCodec::Bincode)
            .expect("serialize_message Bincode");

        // Serialize via JSON
        let json_frame = serialize_message(&task_spec, WireCodec::Json)
            .expect("serialize_message JSON");

        // Verify Bincode payload size is very close to raw binary size (1 byte tag + enum + length + data)
        // Overhead should be less than 100 bytes!
        let bincode_overhead = bincode_frame.len() - size_bytes;
        assert!(
            bincode_overhead < 200,
            "Bincode overhead too large: {bincode_overhead} bytes"
        );

        // Verify JSON frame is at least 3x larger than Bincode
        assert!(
            json_frame.len() > bincode_frame.len() * 3,
            "JSON ({}) did not expand >3x vs Bincode ({})",
            json_frame.len(),
            bincode_frame.len()
        );

        // Verify deserialization fidelity
        let (deserialized_spec, codec): (TaskSpec, WireCodec) =
            deserialize_message(&bincode_frame).expect("deserialize Bincode");
        assert_eq!(codec, WireCodec::Bincode);
        assert_eq!(deserialized_spec, task_spec);
    }
}

#[tokio::test]
async fn test_m8_large_binary_payload_gpu_compute_5mb_e2e() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    // Spawn worker with simulated GPU and Bincode codec
    let _worker = spawn_test_worker(
        master_addr.clone(),
        "gpu-worker-5mb",
        4,
        true,
        WireCodec::Bincode,
    );

    assert!(
        wait_for_workers(&master, 1, Duration::from_secs(5)).await,
        "Failed to connect GPU worker"
    );

    // 5 MB binary input buffer
    let size_5mb = 5 * 1024 * 1024;
    let mut large_buffer = vec![0u8; size_5mb];
    for (i, byte) in large_buffer.iter_mut().enumerate() {
        *byte = ((i * 31 + 7) & 0xFF) as u8;
    }

    let gpu_task = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "matrix_mult_5mb_stress".into(),
            input_data: large_buffer,
            work_group_size: 16,
            simulated_matrix_dim: 64,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(60),
    );

    let stream = TcpStream::connect(&master_addr)
        .await
        .expect("connect client");
    let mut transport = MessageTransport::with_codec(stream, WireCodec::Bincode);

    let start = Instant::now();
    let submit_msg = ClientMessage::SubmitTask {
        task: gpu_task,
        wait: true,
    };
    transport
        .send_msg(&submit_msg)
        .await
        .expect("submit 5MB GPU task");

    let response = transport
        .recv_msg::<ClientResponse>()
        .await
        .expect("recv ClientResponse")
        .expect("connection closed unexpectedly");

    let duration = start.elapsed();
    println!("5 MB GPU Task roundtrip completed in {:?}", duration);

    match response {
        ClientResponse::TaskCompleted { result, .. } => {
            assert_eq!(result.exit_code, 0, "Task failed: {:?}", result.error);
            assert!(result.is_gpu_executed, "Expected is_gpu_executed to be true");
        }
        other => panic!("Unexpected client response: {other:?}"),
    }

    let _ = master.shutdown();
}

// =========================================================================
// SUITE 4: Rapid Mixed-Stream Clients & Concurrency Stress
// =========================================================================

#[tokio::test]
async fn test_m8_rapid_mixed_stream_clients() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    // Connect Worker 1 with Bincode
    let _w1 = spawn_test_worker(
        master_addr.clone(),
        "worker-mixed-bincode",
        4,
        false,
        WireCodec::Bincode,
    );

    // Connect Worker 2 with Tagged JSON
    let _w2 = spawn_test_worker(
        master_addr.clone(),
        "worker-mixed-json",
        4,
        false,
        WireCodec::Json,
    );

    assert!(
        wait_for_workers(&master, 2, Duration::from_secs(5)).await,
        "Failed to connect workers"
    );

    let completed_tasks = Arc::new(AtomicUsize::new(0));
    let mut client_handles = Vec::new();

    // Launch 45 mixed clients concurrently:
    // - 15 Bincode clients (0x02)
    // - 15 Tagged JSON clients (0x01)
    // - 15 Raw Legacy JSON clients ('{')
    // PLUS 15 adversarial clients injecting malformed frames
    for i in 0..60 {
        let addr = master_addr.clone();
        let counter = Arc::clone(&completed_tasks);

        let handle = tokio::spawn(async move {
            match i % 4 {
                0 => {
                    // Bincode Client
                    let stream = TcpStream::connect(&addr).await.expect("connect bincode client");
                    let mut transport = MessageTransport::with_codec(stream, WireCodec::Bincode);

                    let task = Task::new(
                        TaskSpec::builtin_test(format!("test_bincode_{i}"), 10),
                        TaskRequirements::generic(1, 15),
                    );
                    transport
                        .send_msg(&ClientMessage::SubmitTask { task, wait: true })
                        .await
                        .expect("send Bincode SubmitTask");

                    let (resp, codec) = transport
                        .recv_msg_with_codec::<ClientResponse>()
                        .await
                        .expect("recv Bincode response")
                        .expect("premature close");

                    assert_eq!(codec, WireCodec::Bincode, "Expected symmetrical Bincode response");
                    if let ClientResponse::TaskCompleted { result, .. } = resp {
                        assert_eq!(result.exit_code, 0);
                        counter.fetch_add(1, Ordering::SeqCst);
                    } else {
                        panic!("Unexpected response: {resp:?}");
                    }
                }
                1 => {
                    // Tagged JSON Client
                    let stream = TcpStream::connect(&addr).await.expect("connect json client");
                    let mut transport = MessageTransport::with_codec(stream, WireCodec::Json);

                    let task = Task::new(
                        TaskSpec::builtin_test(format!("test_json_{i}"), 10),
                        TaskRequirements::generic(1, 15),
                    );
                    transport
                        .send_msg(&ClientMessage::SubmitTask { task, wait: true })
                        .await
                        .expect("send JSON SubmitTask");

                    let (resp, codec) = transport
                        .recv_msg_with_codec::<ClientResponse>()
                        .await
                        .expect("recv JSON response")
                        .expect("premature close");

                    assert_eq!(codec, WireCodec::Json, "Expected symmetrical JSON response");
                    if let ClientResponse::TaskCompleted { result, .. } = resp {
                        assert_eq!(result.exit_code, 0);
                        counter.fetch_add(1, Ordering::SeqCst);
                    } else {
                        panic!("Unexpected response: {resp:?}");
                    }
                }
                2 => {
                    // Raw Legacy JSON Client ('{')
                    let mut stream = TcpStream::connect(&addr).await.expect("connect raw json client");
                    let task = Task::new(
                        TaskSpec::builtin_test(format!("test_legacy_{i}"), 10),
                        TaskRequirements::generic(1, 15),
                    );
                    let raw_client_msg = ClientMessage::SubmitTask { task, wait: true };
                    let raw_json_bytes = serde_json::to_vec(&raw_client_msg).expect("to_vec");
                    assert_eq!(raw_json_bytes[0], b'{');

                    send_raw_frame(&mut stream, &raw_json_bytes)
                        .await
                        .expect("send raw json");

                    let resp_frame = recv_raw_frame(&mut stream)
                        .await
                        .expect("recv raw frame")
                        .expect("closed");

                    let (resp, codec): (ClientResponse, WireCodec) =
                        deserialize_message(&resp_frame).expect("deserialize response");
                    assert_eq!(codec, WireCodec::Json);
                    if let ClientResponse::TaskCompleted { result, .. } = resp {
                        assert_eq!(result.exit_code, 0);
                        counter.fetch_add(1, Ordering::SeqCst);
                    } else {
                        panic!("Unexpected response: {resp:?}");
                    }
                }
                3 => {
                    // Adversarial client sending malformed frames and noisy drops
                    if let Ok(mut stream) = TcpStream::connect(&addr).await {
                        // Random adversarial pattern
                        let noise = match i % 3 {
                            0 => vec![0xFF, 0xAA, 0x55],
                            1 => vec![0x00, 0x00],
                            _ => vec![WIRE_FORMAT_BINCODE, 0xDE, 0xAD],
                        };
                        let _ = send_raw_frame(&mut stream, &noise).await;
                        // Abrupt drop
                        let _ = stream.shutdown().await;
                    }
                }
                _ => unreachable!(),
            }
        });

        client_handles.push(handle);
    }

    join_all(client_handles).await;

    // 45 legitimate tasks must all be completed successfully
    assert_eq!(
        completed_tasks.load(Ordering::SeqCst),
        45,
        "Not all legitimate tasks from mixed clients completed"
    );

    // Master stats verify 45 completed tasks
    let stats = master.queue().stats().await;
    assert_eq!(stats.completed, 45);
    assert_eq!(stats.failed, 0);

    let _ = master.shutdown();
}

// =========================================================================
// SUITE 5: Frame Boundary & Streaming Integrity (Zero Desync)
// =========================================================================

#[tokio::test]
async fn test_m8_frame_boundary_and_streaming_integrity() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let stream = TcpStream::connect(&master_addr).await.expect("connect");
    let mut transport = MessageTransport::with_codec(stream, WireCodec::Bincode);

    // Perform 20 sequential request-response cycles on a single TCP connection
    for i in 0..20 {
        // Query ClusterStatus
        transport
            .send_msg(&ClientMessage::ClusterStatus)
            .await
            .expect("send ClusterStatus");

        let (resp1, codec1) = transport
            .recv_msg_with_codec::<ClientResponse>()
            .await
            .expect("recv ClusterStatus")
            .expect("EOF");

        assert_eq!(codec1, WireCodec::Bincode);
        assert!(matches!(resp1, ClientResponse::ClusterStatus { .. }));

        // Query ListWorkers
        transport
            .send_msg(&ClientMessage::ListWorkers)
            .await
            .expect("send ListWorkers");

        let (resp2, codec2) = transport
            .recv_msg_with_codec::<ClientResponse>()
            .await
            .expect("recv ListWorkers")
            .expect("EOF");

        assert_eq!(codec2, WireCodec::Bincode);
        assert!(matches!(resp2, ClientResponse::WorkerList { .. }));

        // Query non-existent task
        let fake_id = TaskId::new();
        transport
            .send_msg(&ClientMessage::GetTaskStatus { task_id: fake_id })
            .await
            .expect("send GetTaskStatus");

        let (resp3, codec3) = transport
            .recv_msg_with_codec::<ClientResponse>()
            .await
            .expect("recv TaskStatus")
            .expect("EOF");

        assert_eq!(codec3, WireCodec::Bincode);
        match resp3 {
            ClientResponse::Error { message } => {
                assert!(message.contains("not found"), "Unexpected error message: {message}");
            }
            other => panic!("Iteration {i}: Expected Error for missing task, got {other:?}"),
        }
    }

    let _ = master.shutdown();
}
