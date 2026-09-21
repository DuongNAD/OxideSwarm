//! Empirical Stress-Test and Adversarial Challenge Suite for Milestone 1 (Core Protocol).
//!
//! Covers:
//! 1. Wire framing: fuzz payloads, truncated frames, zero-length frames, 65MB frames.
//! 2. High-throughput message encoding/decoding loops (10,000 round-trip messages).
//! 3. Serialization round-trips for all enum variants with edge-case characters (unicode, empty strings, null bytes).
//! 4. Capability matching logic under hostile inputs (0 cores, u64::MAX RAM, simulated vs non-simulated GPU).
//! 5. 8-state FSM exhaustive 64-transition verification.

use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::time::Instant;
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{
    default_codec, MasterMessage, MessageReader, MessageTransport, MessageWriter, ProtocolError,
    WorkerMessage, LENGTH_FIELD_BYTES, MAX_FRAME_SIZE,
};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec, TaskStatus};

// =========================================================================
// AREA 1: Wire Framing Fuzzing & Boundary Stress Tests
// =========================================================================

#[test]
fn test_framing_fuzz_random_binary_payloads() {
    let mut codec = default_codec();

    // Pseudo-random deterministic byte generator
    let mut state: u64 = 0xDEADBEEFCAFE;
    let mut next_u8 = || -> u8 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        (state >> 33) as u8
    };

    // Test 500 varied payloads from 1 byte to 64KB
    for length in (1..=100)
        .chain((200..=2000).step_by(200))
        .chain((5000..=65536).step_by(10000))
    {
        let payload: Vec<u8> = (0..length).map(|_| next_u8()).collect();
        let bytes_payload = Bytes::from(payload);

        let mut buf = BytesMut::new();
        codec
            .encode(bytes_payload.clone(), &mut buf)
            .expect("Encode must succeed");
        assert_eq!(buf.len(), LENGTH_FIELD_BYTES + length);

        let decoded = codec
            .decode(&mut buf)
            .expect("Decode must succeed")
            .expect("Must yield frame");
        assert_eq!(decoded, bytes_payload);
        assert!(buf.is_empty());
    }
}

#[tokio::test]
async fn test_framing_fuzz_malformed_json_resilience() {
    let (client_io, server_io) = tokio::io::duplex(128 * 1024);
    let mut client_tx = MessageWriter::new(client_io);
    let mut server_rx = MessageReader::new(server_io);

    // List of corrupted, adversarial, or bizarre JSON payloads
    let garbage_payloads: &[&[u8]] = &[
        b"",                                                        // empty
        b"{",                                                       // unclosed open brace
        b"}",                                                       // standalone close brace
        b"{\"type\":",                                              // truncated key-value
        b"{\"type\": \"NonExistentVariant\"}",                      // unknown variant
        b"{\"type\": \"Register\", \"worker_id\": \"not-a-uuid\"}", // bad uuid
        b"{\"type\": \"Heartbeat\", \"timestamp\": -1}",            // negative u64
        b"{\"type\": \"Heartbeat\", \"active_tasks\": \"ten\"}",    // string for usize
        b"[1, 2, 3]",                                               // array instead of object
        b"\"just a string\"",                                       // scalar string
        b"12345678",                                                // scalar number
        b"null",                                                    // literal null
        b"{\"type\": \"\0\0\0\"}",                                  // null byte in type
        b"{\"\": \"\"}",                                            // empty keys
        b"\xFF\xFE\xFD", // completely invalid UTF-8 bytes
    ];

    for (i, garbage) in garbage_payloads.iter().enumerate() {
        // Send garbage frame
        client_tx
            .send_raw_frame(Bytes::from(*garbage))
            .await
            .expect("send_raw_frame should transmit raw bytes");

        // Server tries to deserialize into WorkerMessage -> MUST return Err(Json)
        let res: Result<Option<WorkerMessage>, ProtocolError> = server_rx.recv_msg().await;
        assert!(
            res.is_err(),
            "Garbage payload #{i} ({:?}) must trigger deserialization error",
            String::from_utf8_lossy(garbage)
        );

        // Send valid heartbeat immediately after to prove frame boundary recovery
        let valid_hb = WorkerMessage::Heartbeat {
            worker_id: Uuid::new_v4(),
            timestamp: 100 + i as u64,
            active_tasks: i,
            cpu_usage_pct: 0.0,
            ram_available_mb: 0,
        };
        client_tx
            .send_msg(&valid_hb)
            .await
            .expect("Valid send must succeed");

        // Server MUST receive the valid message cleanly without desync!
        let recovered: Option<WorkerMessage> = server_rx
            .recv_msg()
            .await
            .expect("Server must recover after corrupted frame");
        assert_eq!(
            recovered,
            Some(valid_hb),
            "Frame recovery mismatch after garbage #{i}"
        );
    }
}

#[tokio::test]
async fn test_framing_zero_length_frame() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut client_tx = MessageWriter::new(client_io);
    let mut server = MessageTransport::new(server_io);

    // Send an explicit 0-length frame (4 bytes header = 0x00000000, 0 payload bytes)
    client_tx
        .send_raw_frame(Bytes::new())
        .await
        .expect("0-length raw frame send must succeed");

    // 1. Raw frame reader should read empty Bytes
    // Wait, let's test recv_msg directly on 0-length frame:
    // It should fail with JSON parse error (empty input), but NOT panic or hang!
    let res: Result<Option<WorkerMessage>, ProtocolError> = server.recv_msg().await;
    assert!(res.is_err(), "0-length frame cannot deserialize as JSON");
    match res.unwrap_err() {
        ProtocolError::Json(e) => {
            assert!(e.is_eof(), "Expected EOF JSON error on empty frame");
        }
        other => panic!("Expected ProtocolError::Json, got {:?}", other),
    }

    // 2. Next message must still succeed cleanly
    let valid_hb = WorkerMessage::Heartbeat {
        worker_id: Uuid::new_v4(),
        timestamp: 42,
        active_tasks: 0,
        cpu_usage_pct: 0.0,
        ram_available_mb: 0,
    };
    client_tx.send_msg(&valid_hb).await.unwrap();
    let recovered: Option<WorkerMessage> = server.recv_msg().await.unwrap();
    assert_eq!(recovered, Some(valid_hb));
}

#[tokio::test]
async fn test_framing_truncated_frame_disconnect() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut server = MessageTransport::new(server_io);

    // Client manually writes a 4-byte header indicating 1024 bytes, but only writes 50 bytes and drops!
    use tokio::io::AsyncWriteExt;
    let mut raw_client = client_io;
    let frame_len: u32 = 1024;
    raw_client
        .write_all(&frame_len.to_be_bytes())
        .await
        .unwrap();
    raw_client.write_all(&[b'X'; 50]).await.unwrap();
    drop(raw_client); // Abrupt disconnect mid-frame!

    // Server reads: must fail with UnexpectedEof or Io error, NOT hang!
    let res: Result<Option<WorkerMessage>, ProtocolError> = server.recv_msg().await;
    assert!(res.is_err(), "Truncated frame must error on disconnect");
    match res.unwrap_err() {
        ProtocolError::UnexpectedEof | ProtocolError::Io(_) => {}
        other => panic!("Expected UnexpectedEof or Io, got {:?}", other),
    }
}

#[test]
fn test_framing_65mb_oversized_rejection() {
    let mut codec = default_codec();
    let mut buf = BytesMut::new();

    // 1. 65 MB frame header
    let len_65mb: u32 = 65 * 1024 * 1024;
    buf.extend_from_slice(&len_65mb.to_be_bytes());
    let res = codec.decode(&mut buf);
    assert!(res.is_err(), "65MB length header must be rejected by codec");
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::InvalidData);

    // 2. Exact 64 MB (MAX_FRAME_SIZE) header should be accepted as within bounds
    let mut fresh_codec_64 = default_codec();
    let mut buf2 = BytesMut::new();
    let len_64mb: u32 = MAX_FRAME_SIZE as u32;
    buf2.extend_from_slice(&len_64mb.to_be_bytes());
    // decode needs the payload, so it should return Ok(None) waiting for data, NOT Err!
    let res2 = fresh_codec_64.decode(&mut buf2);
    assert!(res2.is_ok());
    assert!(res2.unwrap().is_none(), "Should be waiting for 64MB data");

    // 3. Oversized frames > 64 MB must be rejected immediately upon header decode
    for mb in [65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 80, 100] {
        let mut fresh_codec = default_codec();
        let mut b = BytesMut::new();
        let l: u32 = mb * 1024 * 1024;
        b.extend_from_slice(&l.to_be_bytes());
        let res = fresh_codec.decode(&mut b);
        assert!(res.is_err(), "{mb} MB must be rejected by codec");
        assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    // u32::MAX header must be rejected
    let mut fresh_codec_max = default_codec();
    let mut buf_max = BytesMut::new();
    buf_max.extend_from_slice(&u32::MAX.to_be_bytes());
    let res_max = fresh_codec_max.decode(&mut buf_max);
    assert!(res_max.is_err(), "u32::MAX must be rejected by fresh codec");
}

#[tokio::test]
async fn test_send_raw_frame_oversized_guard() {
    let (client_io, _server_io) = tokio::io::duplex(1024);
    let mut transport = MessageTransport::new(client_io);

    // Create a 65MB Bytes object (virtual slice / zero-copy)
    // Note: Bytes::from_static or Bytes::from(vec![0; 65*1024*1024])
    let big_data = vec![0u8; 65 * 1024 * 1024];
    let res = transport.send_raw_frame(Bytes::from(big_data)).await;
    assert!(res.is_err(), "send_raw_frame must reject frames > 64MB");
    match res.unwrap_err() {
        ProtocolError::FrameTooLarge { size, max } => {
            assert_eq!(size, 65 * 1024 * 1024);
            assert_eq!(max, MAX_FRAME_SIZE);
        }
        other => panic!("Expected FrameTooLarge, got {:?}", other),
    }
}

// =========================================================================
// AREA 2: High-Throughput Message Loop (10,000 Messages)
// =========================================================================

#[tokio::test]
async fn test_high_throughput_10k_sequential() {
    let (client_io, server_io) = tokio::io::duplex(256 * 1024);
    let mut client = MessageTransport::new(client_io);
    let mut server = MessageTransport::new(server_io);

    let count = 10_000;
    let worker_id = Uuid::new_v4();

    let start = Instant::now();

    for i in 0..count {
        let msg = WorkerMessage::Heartbeat {
            worker_id,
            timestamp: i as u64,
            active_tasks: (i % 8) as usize,
            cpu_usage_pct: 0.0,
            ram_available_mb: 0,
        };

        client.send_msg(&msg).await.expect("Client send failed");
        let recv: Option<WorkerMessage> = server.recv_msg().await.expect("Server recv failed");
        assert_eq!(recv, Some(msg));

        let ack = MasterMessage::HeartbeatAck {
            timestamp: i as u64,
        };
        server.send_msg(&ack).await.expect("Server ack send failed");
        let ack_recv: Option<MasterMessage> =
            client.recv_msg().await.expect("Client ack recv failed");
        assert_eq!(ack_recv, Some(ack));
    }

    let elapsed = start.elapsed();
    let total_messages = count * 2; // worker msg + master ack
    let msgs_per_sec = total_messages as f64 / elapsed.as_secs_f64();
    println!(
        "[HIGH-THROUGHPUT SEQUENTIAL] Completed {} round-trips ({} msgs) in {:?} (~{:.0} msgs/sec)",
        count, total_messages, elapsed, msgs_per_sec
    );
    assert!(
        msgs_per_sec > 1000.0,
        "Throughput too low: {:.0} msgs/sec",
        msgs_per_sec
    );
}

#[tokio::test]
async fn test_high_throughput_10k_pipelined() {
    let (client_io, server_io) = tokio::io::duplex(512 * 1024);
    let mut client_tx = MessageWriter::new(client_io);
    let mut server_rx = MessageReader::new(server_io);

    let count = 10_000;
    let worker_id = Uuid::new_v4();

    let send_task = tokio::spawn(async move {
        for i in 0..count {
            let msg = WorkerMessage::Heartbeat {
                worker_id,
                timestamp: i as u64,
                active_tasks: (i % 4) as usize,
                cpu_usage_pct: 0.0,
                ram_available_mb: 0,
            };
            client_tx
                .send_msg(&msg)
                .await
                .expect("Pipelined send failed");
        }
    });

    let recv_task = tokio::spawn(async move {
        let mut received_count = 0;
        while let Some(msg) = server_rx
            .recv_msg::<WorkerMessage>()
            .await
            .expect("Recv failed")
        {
            match msg {
                WorkerMessage::Heartbeat { timestamp, .. } => {
                    assert_eq!(timestamp, received_count as u64);
                    received_count += 1;
                    if received_count == count {
                        break;
                    }
                }
                _ => panic!("Unexpected message variant"),
            }
        }
        received_count
    });

    let start = Instant::now();
    let (send_res, recv_res) = tokio::join!(send_task, recv_task);
    send_res.unwrap();
    let total_received = recv_res.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(total_received, count);
    let msgs_per_sec = count as f64 / elapsed.as_secs_f64();
    println!(
        "[HIGH-THROUGHPUT PIPELINED] Streamed {} messages in {:?} (~{:.0} msgs/sec)",
        count, elapsed, msgs_per_sec
    );
    assert!(
        msgs_per_sec > 5000.0,
        "Pipelined throughput too low: {:.0} msgs/sec",
        msgs_per_sec
    );
}

// =========================================================================
// AREA 3: Serialization Round-Trips with Edge Cases
// =========================================================================

#[test]
fn test_serde_worker_message_all_variants_and_edge_cases() {
    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();

    // Adversarial string corpus
    let weird_strings = vec![
        "".to_string(),                                    // empty string
        "   \t\r\n   ".to_string(),                        // whitespace only
        "Hello\0World\0\0".to_string(),                    // null bytes
        "🔥🚀💻🦀🤖⚡️".to_string(),                        // multi-byte emojis
        "北京大学 / 华为技术有限公司".to_string(),         // CJK characters
        "Тестирование распределённой системы".to_string(), // Cyrillic
        "مرحبا بك في نظام الحوسبة الموزعة".to_string(),    // RTL Arabic
        "Zero\u{200B}Width\u{200D}Space".to_string(),      // Zero-width characters
        "\"quotes\" and \\backslashes\\ and \n newlines".to_string(),
        "A".repeat(100_000), // 100KB string
    ];

    for s in &weird_strings {
        // 1. WorkerMessage::Register
        let reg = WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities {
                name: s.clone(),
                cpu_cores: usize::MAX,
                ram_mb: u64::MAX,
                has_gpu: true,
                is_simulated_gpu: true,
                gpu_device_name: Some(s.clone()),
                tags: vec![s.clone(), "tag2".into()],
                mobile: None,
            },
        };
        let json = serde_json::to_string(&reg).unwrap();
        let de: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(reg, de);

        // 2. WorkerMessage::Disconnecting
        let disc = WorkerMessage::Disconnecting {
            worker_id,
            reason: s.clone(),
        };
        let json = serde_json::to_string(&disc).unwrap();
        let de: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(disc, de);
    }

    // 3. WorkerMessage::Heartbeat boundaries
    let hb_extremes = vec![
        WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 0,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 0,
        },
        WorkerMessage::Heartbeat {
            worker_id,
            timestamp: u64::MAX,
            active_tasks: usize::MAX,
            cpu_usage_pct: 100.0,
            ram_available_mb: u64::MAX,
        },
    ];
    for hb in hb_extremes {
        let json = serde_json::to_string(&hb).unwrap();
        let de: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(hb, de);
    }

    // 4. WorkerMessage::TaskProgress for all 8 TaskStatus variants
    let all_statuses = [
        TaskStatus::Queued,
        TaskStatus::Assigned,
        TaskStatus::Running,
        TaskStatus::Completed,
        TaskStatus::Failed,
        TaskStatus::Retried,
        TaskStatus::TimedOut,
        TaskStatus::Cancelled,
    ];
    for status in all_statuses {
        let prog = WorkerMessage::TaskProgress {
            worker_id,
            task_id,
            status,
        };
        let json = serde_json::to_string(&prog).unwrap();
        let de: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(prog, de);
    }

    // 5. WorkerMessage::TaskResult edge cases
    let exit_codes = [0, 1, -1, 127, 255, i32::MIN, i32::MAX];
    for &code in &exit_codes {
        let result = WorkerMessage::TaskResult {
            worker_id,
            task_id,
            exit_code: code,
            stdout: "Output\0with\0nulls\n🔥".to_string(),
            stderr: "Error\t\r\n".to_string(),
            execution_time_ms: u64::MAX,
            is_gpu_executed: true,
            error: Some("Failed with error \0".into()),
        };
        let json = serde_json::to_string(&result).unwrap();
        let de: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(result, de);
    }
}

#[test]
fn test_serde_master_message_all_variants_and_edge_cases() {
    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();

    // 1. MasterMessage::RegisterAck edge cases
    let acks = vec![
        MasterMessage::RegisterAck {
            accepted: true,
            worker_id,
            heartbeat_interval_secs: 0,
            message: None,
        },
        MasterMessage::RegisterAck {
            accepted: false,
            worker_id,
            heartbeat_interval_secs: u64::MAX,
            message: Some("Rejected due to: \0 Unicode 🦀".into()),
        },
    ];
    for ack in acks {
        let json = serde_json::to_string(&ack).unwrap();
        let de: MasterMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(ack, de);
    }

    // 2. MasterMessage::HeartbeatAck
    let hb_ack = MasterMessage::HeartbeatAck {
        timestamp: u64::MAX,
    };
    let json = serde_json::to_string(&hb_ack).unwrap();
    let de: MasterMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(hb_ack, de);

    // 3. MasterMessage::AssignTask with ALL 5 TaskSpec variants
    let mut env = HashMap::new();
    env.insert("FOO".into(), "BAR\0BAZ".into());
    env.insert("EMPTY".into(), "".into());
    env.insert("UNICODE".into(), "🔥🦀".into());

    let mut sources = HashMap::new();
    sources.insert("Cargo.toml".into(), "[package]\nname = \"test\"".into());
    sources.insert(
        "src/main.rs".into(),
        "fn main() { println!(\"Hello\"); }".into(),
    );
    sources.insert("sub/mod.rs".into(), "".into());

    let specs = vec![
        TaskSpec::Command {
            program: "/usr/bin/env".into(),
            args: vec![
                "--option".into(),
                "arg with spaces".into(),
                "\0null\0".into(),
            ],
            env,
            working_dir: None,
            stdin: None,
        },
        TaskSpec::ShellScript {
            script: "#!/bin/bash\nset -euo pipefail\necho 'Test 123'\nexit 0\n".into(),
            interpreter: None,
            env: HashMap::new(),
        },
        TaskSpec::RustCompilation {
            crate_name: "distributed_crate_1".into(),
            source_files: sources,
            compiler_flags: vec!["build".into(), "--release".into(), "--target=x86_64".into()],
            target_dir: None,
        },
        TaskSpec::GpuCompute {
            kernel_name: "matmul_fp32".into(),
            input_data: (0..=255).cycle().take(10_000).collect(), // 10KB binary
            work_group_size: 16,
            simulated_matrix_dim: 64,
            compute_intensity: u32::MAX,
        },
        TaskSpec::BuiltinTest {
            test_name: "stress_test".into(),
            iterations: 10_000,
            duration_ms: u64::MAX,
            should_fail: true,
            require_gpu: true,
        },
    ];

    for spec in specs {
        let task = Task {
            id: task_id,
            spec,
            requirements: TaskRequirements {
                cpu_cores: 0,
                ram_mb: u64::MAX,
                gpu_required: true,
                timeout_secs: u64::MAX,
                max_retries: None,
            },
            created_at_utc: u64::MAX,
            tags: vec!["tag1".into(), "unicode_🏷️".into(), "".into()],
        };
        let assign = MasterMessage::AssignTask { task };
        let json = serde_json::to_string(&assign).unwrap();
        let de: MasterMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(assign, de);
    }

    // 4. MasterMessage::CancelTask
    let cancel = MasterMessage::CancelTask {
        task_id,
        reason: Some("User abort with \0 and emojis 🔥".into()),
    };
    let json = serde_json::to_string(&cancel).unwrap();
    let de: MasterMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(cancel, de);

    // 5. MasterMessage::Shutdown
    let shutdown = MasterMessage::Shutdown {
        reason: "Cluster maintenance \0".into(),
        grace_period_secs: Some(u64::MAX),
    };
    let json = serde_json::to_string(&shutdown).unwrap();
    let de: MasterMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(shutdown, de);
}

// =========================================================================
// AREA 4: Capability Matching Under Hostile Inputs
// =========================================================================

#[test]
fn test_capabilities_hostile_resource_boundaries() {
    // 1. Worker with zero resources
    let zero_worker = WorkerCapabilities::new("zero-worker", 0, 0, false, false, None);

    // Task requiring zero resources -> should pass
    let zero_req = TaskRequirements::new(0, 0, false, 30);
    assert!(
        zero_worker.satisfies(&zero_req),
        "Zero-resource worker should satisfy zero-resource task"
    );

    // Task requiring 1 CPU core -> MUST FAIL
    let req_1cpu = TaskRequirements::new(1, 0, false, 30);
    assert!(
        !zero_worker.satisfies(&req_1cpu),
        "Zero-core worker cannot satisfy 1-core task"
    );

    // Task requiring 1 MB RAM -> MUST FAIL
    let req_1ram = TaskRequirements::new(0, 1, false, 30);
    assert!(
        !zero_worker.satisfies(&req_1ram),
        "Zero-RAM worker cannot satisfy 1-MB-RAM task"
    );

    // Task requiring GPU -> MUST FAIL
    let req_gpu = TaskRequirements::new(0, 0, true, 30);
    assert!(
        !zero_worker.satisfies(&req_gpu),
        "Non-GPU worker cannot satisfy GPU task"
    );

    // 2. Worker with maximum resources (u64::MAX RAM, usize::MAX cores)
    let max_worker =
        WorkerCapabilities::new("max-worker", usize::MAX, u64::MAX, false, false, None);

    // Task requiring maximum resources -> MUST PASS without overflow
    let max_req = TaskRequirements::new(usize::MAX, u64::MAX, false, 60);
    assert!(
        max_worker.satisfies(&max_req),
        "Max worker must satisfy max requirements"
    );

    // Task requiring normal resources -> MUST PASS
    let normal_req = TaskRequirements::new(16, 32768, false, 60);
    assert!(max_worker.satisfies(&normal_req));

    // 3. Worker with slightly less RAM than required
    let ram_999 = WorkerCapabilities::new("ram-worker", 8, 999, false, false, None);
    let req_1000 = TaskRequirements::new(8, 1000, false, 30);
    assert!(
        !ram_999.satisfies(&req_1000),
        "999 MB worker cannot satisfy 1000 MB task"
    );

    // 4. Worker with slightly less CPU than required
    let cpu_7 = WorkerCapabilities::new("cpu-worker", 7, 8192, false, false, None);
    let req_8 = TaskRequirements::new(8, 8192, false, 30);
    assert!(
        !cpu_7.satisfies(&req_8),
        "7-core worker cannot satisfy 8-core task"
    );
}

#[test]
fn test_capabilities_gpu_matrix_exhaustive() {
    // 4 distinct GPU states
    let cpu_only = WorkerCapabilities::new("cpu-only", 4, 8192, false, false, None);
    let physical_gpu =
        WorkerCapabilities::new("phys-gpu", 4, 8192, true, false, Some("NVIDIA RTX".into()));
    let simulated_gpu =
        WorkerCapabilities::new("sim-gpu", 4, 8192, false, true, Some("Virtual GPU".into()));
    let both_gpu =
        WorkerCapabilities::new("both-gpu", 4, 8192, true, true, Some("Virtual GPU".into()));

    let generic_req = TaskRequirements::generic(2, 30);
    let gpu_req = TaskRequirements::gpu(30);

    // Generic tasks MUST be satisfied by ALL 4 workers
    assert!(cpu_only.satisfies(&generic_req));
    assert!(physical_gpu.satisfies(&generic_req));
    assert!(simulated_gpu.satisfies(&generic_req));
    assert!(both_gpu.satisfies(&generic_req));

    // GPU tasks MUST be REJECTED by cpu_only
    assert!(
        !cpu_only.satisfies(&gpu_req),
        "CRITICAL: cpu_only worker MUST NEVER satisfy GPU task"
    );

    // GPU tasks MUST be SATISFIED by physical, simulated, or both
    assert!(
        physical_gpu.satisfies(&gpu_req),
        "Physical GPU worker must satisfy GPU task"
    );
    assert!(
        simulated_gpu.satisfies(&gpu_req),
        "Simulated GPU worker must satisfy GPU task"
    );
    assert!(
        both_gpu.satisfies(&gpu_req),
        "Combined GPU worker must satisfy GPU task"
    );

    // can_execute_gpu() unit check
    assert!(!cpu_only.can_execute_gpu());
    assert!(physical_gpu.can_execute_gpu());
    assert!(simulated_gpu.can_execute_gpu());
    assert!(both_gpu.can_execute_gpu());
}

#[test]
fn test_capabilities_detect_hostile_overrides() {
    // 1. cores_override = Some(0): Must gracefully fallback to host available_parallelism >= 1
    let caps_0 = WorkerCapabilities::detect(None, Some(0), false);
    assert!(
        caps_0.cpu_cores >= 1,
        "0 cores override must fallback to >= 1"
    );

    // 2. cores_override = Some(usize::MAX): Must be respected
    let caps_max = WorkerCapabilities::detect(None, Some(usize::MAX), false);
    assert_eq!(caps_max.cpu_cores, usize::MAX);

    // 3. name_override = Some(""): Empty string must be stored
    let caps_empty_name = WorkerCapabilities::detect(Some("".into()), None, false);
    assert_eq!(caps_empty_name.name, "");

    // 4. name_override with null bytes and unicode
    let hostile_name = "worker\0\n🚀";
    let caps_hostile = WorkerCapabilities::detect(Some(hostile_name.into()), None, false);
    assert_eq!(caps_hostile.name, hostile_name);
}

// =========================================================================
// AREA 5: 8-State FSM Exhaustive 64-Transition Matrix
// =========================================================================

#[test]
fn test_fsm_exhaustive_64_transitions() {
    let all_states = [
        TaskStatus::Queued,
        TaskStatus::Assigned,
        TaskStatus::Running,
        TaskStatus::Completed,
        TaskStatus::Failed,
        TaskStatus::Retried,
        TaskStatus::TimedOut,
        TaskStatus::Cancelled,
    ];

    // Build the expected valid transitions map: (from, to) -> bool
    // Non-terminal states:
    // Queued: -> Assigned, Cancelled
    // Assigned: -> Running, Queued, Retried, Failed, TimedOut, Cancelled
    // Running: -> Completed, Failed, Retried, TimedOut, Cancelled
    // Retried: -> Queued, Failed
    // Terminal states (Completed, Failed, TimedOut, Cancelled): NONE
    let is_valid_transition = |from: TaskStatus, to: TaskStatus| -> bool {
        match from {
            TaskStatus::Queued => matches!(to, TaskStatus::Assigned | TaskStatus::Cancelled),
            TaskStatus::Assigned => matches!(
                to,
                TaskStatus::Running
                    | TaskStatus::Queued
                    | TaskStatus::Retried
                    | TaskStatus::Failed
                    | TaskStatus::TimedOut
                    | TaskStatus::Cancelled
            ),
            TaskStatus::Running => matches!(
                to,
                TaskStatus::Completed
                    | TaskStatus::Failed
                    | TaskStatus::Retried
                    | TaskStatus::TimedOut
                    | TaskStatus::Cancelled
            ),
            TaskStatus::Retried => matches!(to, TaskStatus::Queued | TaskStatus::Failed),
            TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::TimedOut
            | TaskStatus::Cancelled => false,
        }
    };

    let mut valid_count = 0;
    let mut invalid_count = 0;

    for &from in &all_states {
        for &to in &all_states {
            let actual = from.can_transition_to(to);
            let expected = is_valid_transition(from, to);

            assert_eq!(
                actual, expected,
                "FSM Transition mismatch for {:?} -> {:?}: expected {}, got {}",
                from, to, expected, actual
            );

            if actual {
                valid_count += 1;
            } else {
                invalid_count += 1;
            }
        }
    }

    // Mathematical verification:
    // Total transitions = 8 * 8 = 64
    // Valid transitions = 2 (Queued) + 6 (Assigned) + 5 (Running) + 2 (Retried) = 15
    // Invalid transitions = 64 - 15 = 49
    assert_eq!(
        valid_count, 15,
        "There must be exactly 15 valid transitions"
    );
    assert_eq!(
        invalid_count, 49,
        "There must be exactly 49 invalid transitions"
    );

    // Self-transitions check: NO state may transition to itself!
    for &state in &all_states {
        assert!(
            !state.can_transition_to(state),
            "Self-transition for {:?} must be forbidden",
            state
        );
    }

    // Terminal states check
    for &state in &all_states {
        let expected_terminal = matches!(
            state,
            TaskStatus::Completed
                | TaskStatus::Failed
                | TaskStatus::TimedOut
                | TaskStatus::Cancelled
        );
        assert_eq!(state.is_terminal(), expected_terminal);

        // Terminal states must have ZERO outgoing transitions
        if expected_terminal {
            for &next in &all_states {
                assert!(
                    !state.can_transition_to(next),
                    "Terminal state {:?} must not transition to {:?}",
                    state,
                    next
                );
            }
        }
    }

    // Active states check: Assigned and Running only
    for &state in &all_states {
        let expected_active = matches!(state, TaskStatus::Assigned | TaskStatus::Running);
        assert_eq!(state.is_active(), expected_active);
    }
}

#[test]
fn test_task_spec_deep_validation_hostile() {
    // 1. Rust compilation with sneaky empty crate_name or spaces
    let invalid_crates = ["", "   ", "\t\n"];
    for name in &invalid_crates {
        let spec = TaskSpec::RustCompilation {
            crate_name: name.to_string(),
            source_files: HashMap::from([("src/lib.rs".into(), "pub fn f() {}".into())]),
            compiler_flags: vec![],
            target_dir: None,
        };
        assert!(
            spec.validate().is_err(),
            "Blank crate_name must fail validation"
        );
    }

    // 2. Command with blank or whitespace command
    for cmd in &invalid_crates {
        let spec = TaskSpec::Command {
            program: cmd.to_string(),
            args: vec![],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        };
        assert!(
            spec.validate().is_err(),
            "Blank command must fail validation"
        );
    }

    // 3. Shell script with blank or whitespace
    for script in &invalid_crates {
        let spec = TaskSpec::ShellScript {
            script: script.to_string(),
            interpreter: None,
            env: HashMap::new(),
        };
        assert!(
            spec.validate().is_err(),
            "Blank shell script must fail validation"
        );
    }

    // 4. GPU compute with blank kernel name
    for k in &invalid_crates {
        let spec = TaskSpec::GpuCompute {
            kernel_name: k.to_string(),
            input_data: vec![1, 2, 3],
            work_group_size: 16,
            simulated_matrix_dim: 64,
            compute_intensity: 1,
        };
        assert!(
            spec.validate().is_err(),
            "Blank kernel name must fail validation"
        );
    }

    // 5. TaskRequirements with timeout = 0
    let req_timeout_0 = TaskRequirements::new(1, 1024, false, 0);
    assert!(
        req_timeout_0.validate().is_err(),
        "timeout_secs = 0 must fail validation"
    );
}
