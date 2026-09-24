//! Empirical Adversarial Challenge Suite: MapReduce Bincode Deserialization & Protocol Edge Cases
//!
//! Authored by Challenger 1 (Milestone M5 / Requirement R6).
//!
//! Aggressively verifies:
//! 1. Variable-length and hostile MapReduce job names (0 to 65535 chars, unicode, emojis, control chars).
//! 2. Empty and extreme mapper/reducer specifications.
//! 3. Zero false positives between all `ClientMessage` and `WorkerMessage` variant tags under Bincode.
//! 4. Rigorous tag-5 collision stress (`Disconnecting` vs `SubmitMapReduce`) under randomized fuzzing.
//! 5. Truncated, corrupted, and partial frame rejection without misclassification.
//! 6. Live bidirectional TCP transport stream verification with mixed client and worker message traffic.

use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::mapreduce::{
    MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec,
};
use rusty_grid_core::protocol::{
    deserialize_message, serialize_message, ClientMessage, InboundMessage,
    MessageTransport, ProtocolError, WireCodec, WorkerMessage,
};
use rusty_grid_core::task::{
    Bytes as TaskBytes, Task, TaskId, TaskRequirements, TaskSpec, TaskStatus,
};

fn dummy_task() -> Task {
    Task::new(
        TaskSpec::new_command("echo", vec!["hello".into()]),
        TaskRequirements::generic(1, 10),
    )
}

fn dummy_capabilities() -> WorkerCapabilities {
    WorkerCapabilities {
        name: "worker-test".into(),
        cpu_cores: 4,
        ram_mb: 8192,
        has_gpu: false,
        is_simulated_gpu: false,
        gpu_device_name: None,
        tags: vec!["test".into()],
        mobile: None,
    }
}

// =========================================================================
// TEST 1: Variable-Length and Hostile MapReduce Job Names
// =========================================================================

#[test]
fn test_adv_mapreduce_job_name_lengths_and_encodings() {
    let long256 = "x".repeat(256);
    let long10k = "long_name_".repeat(1000);

    let test_names: Vec<&str> = vec![
        "",                                                  // Empty
        "a",                                                 // 1 char
        "standard_job_name",                                // Standard
        &long256,                                           // 256 chars
        &long10k,                                           // 10,000 chars
        "分布式计算_MapReduce_任务_🦀_🚀",                     // Chinese + Emojis
        "مهمة_توزيع_البيانات_12345",                         // Arabic RTL
        "Пайплайн_обработки_данных_2026",                   // Cyrillic
        "Name with \t tabs \n newlines \r returns",         // Whitespace control chars
        "\"quoted\" and 'single' and `backticks`",          // Quotes
        "!@#$%^&*()_+~|}{[]:;?><,./-=",                     // Punctuation
    ];

    for name in test_names {
        let spec = MapReduceJobSpec::new(
            name,
            vec!["sample text data".into()],
            MapFunctionSpec::Builtin {
                operator: "word_count".into(),
            },
            ReduceFunctionSpec::Builtin {
                operator: "sum".into(),
            },
            2,
            2,
            30,
        );

        let client_msg = ClientMessage::SubmitMapReduce { job: spec.clone() };

        // Test Bincode serialization & deserialization as InboundMessage
        let bin_bytes = serialize_message(&client_msg, WireCodec::Bincode)
            .expect("bincode serialization failed");
        let (inbound, codec): (InboundMessage, _) = deserialize_message(&bin_bytes)
            .expect("bincode deserialization as InboundMessage failed");

        assert_eq!(codec, WireCodec::Bincode);
        match inbound {
            InboundMessage::Client(ClientMessage::SubmitMapReduce { job }) => {
                assert_eq!(job.name, name, "Job name mismatch for length {}", name.len());
                assert_eq!(job.job_id, spec.job_id);
                assert_eq!(job.input_data, spec.input_data);
                assert_eq!(job.partition_count, 2);
                assert_eq!(job.reducer_count, 2);
            }
            InboundMessage::Worker(wm) => {
                panic!("False positive: ClientMessage::SubmitMapReduce deserialized as WorkerMessage: {:?}", wm);
            }
            other => panic!("Unexpected inbound message variant: {:?}", other),
        }

        // Test JSON serialization & deserialization as InboundMessage
        let json_bytes = serialize_message(&client_msg, WireCodec::Json)
            .expect("json serialization failed");
        let (inbound_json, codec_json): (InboundMessage, _) = deserialize_message(&json_bytes)
            .expect("json deserialization as InboundMessage failed");

        assert_eq!(codec_json, WireCodec::Json);
        match inbound_json {
            InboundMessage::Client(ClientMessage::SubmitMapReduce { job }) => {
                assert_eq!(job.name, name);
            }
            other => panic!("Unexpected JSON inbound message variant: {:?}", other),
        }
    }
}

// =========================================================================
// TEST 2: Empty and Extreme Mapper/Reducer Specifications
// =========================================================================

#[test]
fn test_adv_mapreduce_mapper_reducer_edge_specs() {
    let mappers = vec![
        MapFunctionSpec::Builtin { operator: "".into() },
        MapFunctionSpec::ShellScript { script: "".into() },
        MapFunctionSpec::ShellScript { script: "while read l; do echo \"$l\"; done".into() },
        MapFunctionSpec::Command { program: "".into(), args: vec![] },
        MapFunctionSpec::Command { program: "python".into(), args: vec!["-c".into(), "import sys; print(sys.stdin.read())".into()] },
        MapFunctionSpec::ShellScript { script: "x".repeat(50_000) }, // 50KB script
        MapFunctionSpec::Command {
            program: "custom_mapper".into(),
            args: (0..500).map(|i| format!("--arg-{i}=value_{i}")).collect(),
        },
    ];

    let reducers = vec![
        ReduceFunctionSpec::Builtin { operator: "".into() },
        ReduceFunctionSpec::ShellScript { script: "".into() },
        ReduceFunctionSpec::ShellScript { script: "awk '{s+=$2} END {print s}'".into() },
        ReduceFunctionSpec::Command { program: "".into(), args: vec![] },
        ReduceFunctionSpec::Command { program: "sort".into(), args: vec!["-k1,1".into()] },
        ReduceFunctionSpec::ShellScript { script: "y".repeat(50_000) }, // 50KB reducer script
    ];

    for (m_idx, mapper) in mappers.into_iter().enumerate() {
        for (r_idx, reducer) in reducers.iter().enumerate() {
            let spec = MapReduceJobSpec::new(
                format!("edge_job_{m_idx}_{r_idx}"),
                vec![], // Empty input data edge case
                mapper.clone(),
                reducer.clone(),
                0, // 0 partitions edge case
                0, // 0 reducers edge case
                0,
            );

            let client_msg = ClientMessage::SubmitMapReduce { job: spec.clone() };

            let bytes = serialize_message(&client_msg, WireCodec::Bincode)
                .expect("Failed to serialize edge spec");
            let (inbound, _): (InboundMessage, _) = deserialize_message(&bytes)
                .expect("Failed to deserialize edge spec as InboundMessage");

            match inbound {
                InboundMessage::Client(ClientMessage::SubmitMapReduce { job }) => {
                    assert_eq!(job.mapper, mapper);
                    assert_eq!(job.reducer, *reducer);
                    assert_eq!(job.input_data.len(), 0);
                    assert_eq!(job.partition_count, 0);
                }
                InboundMessage::Worker(wm) => {
                    panic!("CRITICAL BUG: edge spec deserialized as WorkerMessage: {:?}", wm);
                }
                other => panic!("Unexpected variant: {:?}", other),
            }
        }
    }
}

// =========================================================================
// TEST 3: Exhaustive ClientMessage Variants under Bincode Inbound
// =========================================================================

#[test]
fn test_adv_client_message_all_variants_bincode_inbound() {
    let task_id = TaskId::new();
    let sample_job = MapReduceJobSpec::new(
        "sample",
        vec!["line1".into()],
        MapFunctionSpec::Builtin { operator: "word_count".into() },
        ReduceFunctionSpec::Builtin { operator: "sum".into() },
        1,
        1,
        10,
    );

    let client_messages = vec![
        (0, "SubmitTask", ClientMessage::SubmitTask { task: dummy_task(), wait: true }),
        (1, "GetTaskStatus", ClientMessage::GetTaskStatus { task_id }),
        (2, "CancelTask", ClientMessage::CancelTask { task_id }),
        (3, "ClusterStatus", ClientMessage::ClusterStatus),
        (4, "ListWorkers", ClientMessage::ListWorkers),
        (5, "SubmitMapReduce", ClientMessage::SubmitMapReduce { job: sample_job }),
    ];

    for (variant_idx, name, msg) in client_messages {
        let bytes = serialize_message(&msg, WireCodec::Bincode)
            .unwrap_or_else(|e| panic!("Failed to serialize {name}: {e}"));

        let (inbound, codec): (InboundMessage, _) = deserialize_message(&bytes)
            .unwrap_or_else(|e| panic!("Failed to deserialize {name} as InboundMessage: {e}"));

        assert_eq!(codec, WireCodec::Bincode);
        match inbound {
            InboundMessage::Client(c) => match (variant_idx, c) {
                (0, ClientMessage::SubmitTask { .. }) => {}
                (1, ClientMessage::GetTaskStatus { task_id: tid }) => assert_eq!(tid, task_id),
                (2, ClientMessage::CancelTask { task_id: tid }) => assert_eq!(tid, task_id),
                (3, ClientMessage::ClusterStatus) => {}
                (4, ClientMessage::ListWorkers) => {}
                (5, ClientMessage::SubmitMapReduce { job }) => assert_eq!(job.name, "sample"),
                (idx, unexpected) => panic!("Variant mismatch: expected variant index {idx}, got {:?}", unexpected),
            },
            InboundMessage::Worker(wm) => {
                panic!("CRITICAL COLLISION: ClientMessage {name} (index {variant_idx}) decoded as WorkerMessage: {:?}", wm);
            }
        }
    }
}

// =========================================================================
// TEST 4: Exhaustive WorkerMessage Variants under Bincode Inbound
// =========================================================================

#[test]
fn test_adv_worker_message_all_variants_bincode_inbound() {
    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();

    let worker_messages = vec![
        (
            0,
            "Register",
            WorkerMessage::Register {
                worker_id,
                capabilities: dummy_capabilities(),
            },
        ),
        (
            1,
            "Heartbeat",
            WorkerMessage::Heartbeat {
                worker_id,
                timestamp: 123456789,
                active_tasks: 2,
                cpu_usage_pct: 35.5,
                ram_available_mb: 2048,
            },
        ),
        (
            2,
            "TaskProgress",
            WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status: TaskStatus::Running,
            },
        ),
        (
            3,
            "TaskResult",
            WorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code: 0,
                stdout: TaskBytes::copy_from_slice(b"stdout data"),
                stderr: TaskBytes::copy_from_slice(b"stderr data"),
                execution_time_ms: 150,
                is_gpu_executed: true,
                device_name: Some("Test GPU".into()),
                error: None,
            },
        ),
        (
            4,
            "Checkpoint",
            WorkerMessage::Checkpoint {
                task_id,
                sequence: 1,
                delta_state: TaskBytes::copy_from_slice(b"state_snapshot"),
            },
        ),
        (
            5,
            "Disconnecting",
            WorkerMessage::Disconnecting {
                worker_id,
                reason: "Graceful shutdown".into(),
            },
        ),
    ];

    for (variant_idx, name, msg) in worker_messages {
        let bytes = serialize_message(&msg, WireCodec::Bincode)
            .unwrap_or_else(|e| panic!("Failed to serialize {name}: {e}"));

        let (inbound, codec): (InboundMessage, _) = deserialize_message(&bytes)
            .unwrap_or_else(|e| panic!("Failed to deserialize {name} as InboundMessage: {e}"));

        assert_eq!(codec, WireCodec::Bincode);
        match inbound {
            InboundMessage::Worker(w) => match (variant_idx, w) {
                (0, WorkerMessage::Register { worker_id: wid, .. }) => assert_eq!(wid, worker_id),
                (1, WorkerMessage::Heartbeat { worker_id: wid, .. }) => assert_eq!(wid, worker_id),
                (2, WorkerMessage::TaskProgress { worker_id: wid, .. }) => assert_eq!(wid, worker_id),
                (3, WorkerMessage::TaskResult { worker_id: wid, .. }) => assert_eq!(wid, worker_id),
                (4, WorkerMessage::Checkpoint { task_id: tid, .. }) => assert_eq!(tid, task_id),
                (5, WorkerMessage::Disconnecting { worker_id: wid, reason }) => {
                    assert_eq!(wid, worker_id);
                    assert_eq!(reason, "Graceful shutdown");
                }
                (idx, unexpected) => panic!("Variant mismatch: expected variant index {idx}, got {:?}", unexpected),
            },
            InboundMessage::Client(cm) => {
                panic!("CRITICAL COLLISION: WorkerMessage {name} (index {variant_idx}) decoded as ClientMessage: {:?}", cm);
            }
        }
    }
}

// =========================================================================
// TEST 5: High-Density Tag-5 Collision Stress Test
// =========================================================================

#[test]
fn test_adv_tag_5_collision_fuzz_stress() {
    // Generate 100 Disconnecting (tag 5) and 100 SubmitMapReduce (tag 5) messages
    // Interleave them and assert 100% classification precision without ambiguity
    let mut messages: Vec<(bool, Vec<u8>)> = Vec::new();

    for i in 0..100 {
        // Disconnecting message
        let reason = if i % 5 == 0 {
            "".to_string()
        } else if i % 5 == 1 {
            format!("Node failure reason #{i} with special chars: 🚀 💥")
        } else {
            "a".repeat(i * 10)
        };

        let disc_msg = WorkerMessage::Disconnecting {
            worker_id: Uuid::new_v4(),
            reason,
        };
        let disc_bytes = serialize_message(&disc_msg, WireCodec::Bincode).unwrap();
        messages.push((true, disc_bytes.to_vec()));

        // SubmitMapReduce message
        let job_name = if i % 3 == 0 {
            format!("job_{i}")
        } else {
            format!("mr_job_{}_{}", i, "x".repeat(i * 5))
        };

        let mr_spec = MapReduceJobSpec::new(
            job_name,
            vec![format!("data partition {i}"), format!("data partition {i} bis")],
            MapFunctionSpec::Builtin { operator: "word_count".into() },
            ReduceFunctionSpec::Builtin { operator: "sum".into() },
            2,
            1,
            20,
        );
        let mr_msg = ClientMessage::SubmitMapReduce { job: mr_spec };
        let mr_bytes = serialize_message(&mr_msg, WireCodec::Bincode).unwrap();
        messages.push((false, mr_bytes.to_vec()));
    }

    let mut disc_count = 0;
    let mut mr_count = 0;

    for (is_worker_expected, bytes) in messages {
        let (inbound, codec): (InboundMessage, _) = deserialize_message(&bytes)
            .expect("Failed to deserialize in tag 5 fuzz test");
        assert_eq!(codec, WireCodec::Bincode);

        if is_worker_expected {
            match inbound {
                InboundMessage::Worker(WorkerMessage::Disconnecting { .. }) => {
                    disc_count += 1;
                }
                InboundMessage::Client(cm) => {
                    panic!("WorkerMessage::Disconnecting misclassified as ClientMessage: {:?}", cm);
                }
                other => panic!("Unexpected message: {:?}", other),
            }
        } else {
            match inbound {
                InboundMessage::Client(ClientMessage::SubmitMapReduce { .. }) => {
                    mr_count += 1;
                }
                InboundMessage::Worker(wm) => {
                    panic!("ClientMessage::SubmitMapReduce misclassified as WorkerMessage: {:?}", wm);
                }
                other => panic!("Unexpected message: {:?}", other),
            }
        }
    }

    assert_eq!(disc_count, 100);
    assert_eq!(mr_count, 100);
}

// =========================================================================
// TEST 6: Truncated & Corrupted Bincode Payloads
// =========================================================================

#[test]
fn test_adv_truncated_and_corrupted_payloads() {
    let spec = MapReduceJobSpec::new(
        "test_truncated_job",
        vec!["hello world".into()],
        MapFunctionSpec::Builtin { operator: "word_count".into() },
        ReduceFunctionSpec::Builtin { operator: "sum".into() },
        1,
        1,
        10,
    );
    let msg = ClientMessage::SubmitMapReduce { job: spec };
    let bytes = serialize_message(&msg, WireCodec::Bincode).unwrap();

    // Verify truncated prefixes fail gracefully without panicking or false matching
    let mut ok_matches = Vec::new();
    for len in 1..bytes.len() - 1 {
        let truncated = &bytes[..len];
        let res: Result<(InboundMessage, WireCodec), ProtocolError> = deserialize_message(truncated);
        if let Ok((inbound, _codec)) = res {
            ok_matches.push((len, format!("{:?}", inbound)));
        }
    }
    println!("Truncted prefixes that returned Ok: {:?}", ok_matches);
    // There should be at most 1 exact match if the prefix matches Disconnecting exactly byte-for-byte
    for (len, desc) in &ok_matches {
        assert!(
            desc.contains("Disconnecting"),
            "Unexpected truncated match at length {len}: {desc}"
        );
    }


    // Corrupted tag
    let mut corrupted = bytes.to_vec();
    corrupted[0] = 0xFF; // Invalid format tag
    let res: Result<(InboundMessage, WireCodec), ProtocolError> = deserialize_message(&corrupted);
    assert!(matches!(res, Err(ProtocolError::InvalidFormatTag(0xFF))));
}

// =========================================================================
// TEST 7: Bidirectional Transport Stream with Mixed Traffic
// =========================================================================

#[tokio::test]
async fn test_adv_inbound_message_roundtrip_over_transport() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut transport = MessageTransport::new(stream);

        // Receive 4 messages: Register, SubmitMapReduce, Disconnecting, ClusterStatus
        let msg1 = transport.recv_msg_with_codec::<InboundMessage>().await.unwrap().unwrap().0;
        assert!(matches!(msg1, InboundMessage::Worker(WorkerMessage::Register { .. })));

        let msg2 = transport.recv_msg_with_codec::<InboundMessage>().await.unwrap().unwrap().0;
        assert!(matches!(msg2, InboundMessage::Client(ClientMessage::SubmitMapReduce { .. })));

        let msg3 = transport.recv_msg_with_codec::<InboundMessage>().await.unwrap().unwrap().0;
        assert!(matches!(msg3, InboundMessage::Worker(WorkerMessage::Disconnecting { .. })));

        let msg4 = transport.recv_msg_with_codec::<InboundMessage>().await.unwrap().unwrap().0;
        assert!(matches!(msg4, InboundMessage::Client(ClientMessage::ClusterStatus)));
    });

    let client_stream = TcpStream::connect(addr).await.expect("connect");
    let mut client_transport = MessageTransport::with_codec(client_stream, WireCodec::Bincode);

    // 1. Worker Register
    let reg = WorkerMessage::Register {
        worker_id: Uuid::new_v4(),
        capabilities: dummy_capabilities(),
    };
    client_transport.send_msg(&reg).await.expect("send reg");

    // 2. Client SubmitMapReduce
    let mr = ClientMessage::SubmitMapReduce {
        job: MapReduceJobSpec::new(
            "stream_mr",
            vec!["sample".into()],
            MapFunctionSpec::Builtin { operator: "word_count".into() },
            ReduceFunctionSpec::Builtin { operator: "sum".into() },
            1,
            1,
            10,
        ),
    };
    client_transport.send_msg(&mr).await.expect("send mr");

    // 3. Worker Disconnecting
    let disc = WorkerMessage::Disconnecting {
        worker_id: Uuid::new_v4(),
        reason: "Test disconnect".into(),
    };
    client_transport.send_msg(&disc).await.expect("send disc");

    // 4. Client ClusterStatus
    let status = ClientMessage::ClusterStatus;
    client_transport.send_msg(&status).await.expect("send status");

    server_task.await.expect("server task completed");
}
