use bytes::Bytes;
use std::collections::HashMap;
use uuid::Uuid;

use crate::capabilities::WorkerCapabilities;
use crate::task::{Task, TaskId, TaskRequirements, TaskSpec, TaskStatus};

use super::*;

#[test]
fn test_worker_message_roundtrip() {
    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();

    let messages = vec![
        WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities {
                name: "test-worker-1".into(),
                cpu_cores: 8,
                ram_mb: 16384,
                has_gpu: true,
                is_simulated_gpu: true,
                gpu_device_name: Some("Simulated M1 GPU".into()),
                tags: vec!["fast".into()],
                mobile: None,
                ..Default::default()
            },
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        },
        WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 1726747200,
            active_tasks: 2,
            cpu_usage_pct: 42.5,
            ram_available_mb: 8192,
        },
        WorkerMessage::TaskProgress {
            worker_id,
            task_id,
            status: TaskStatus::Running,
        },
        WorkerMessage::TaskResult {
            worker_id,
            task_id,
            exit_code: 0,
            stdout: "Success output".into(),
            stderr: "".into(),
            execution_time_ms: 120,
            is_gpu_executed: true,
            device_name: Some("Simulated M1 GPU".into()),
            error: None,
            asset_bytes: None,
            artifacts: Vec::new(),
            outputs: Vec::new(),
        },
        WorkerMessage::Checkpoint {
            task_id,
            sequence: 1,
            delta_state: TaskBytes::copy_from_slice(b"checkpoint-50"),
        },
        WorkerMessage::Disconnecting {
            worker_id,
            reason: "Normal shutdown".into(),
        },
    ];

    for msg in messages {
        let json = serde_json::to_string(&msg).expect("serialization failed");
        let deserialized: WorkerMessage =
            serde_json::from_str(&json).expect("deserialization failed");
        assert_eq!(msg, deserialized);
        if let WorkerMessage::Checkpoint { .. } = msg {
            assert_eq!(msg.worker_id(), Uuid::nil());
        } else {
            assert_eq!(msg.worker_id(), worker_id);
        }
        assert!(!msg.variant_name().is_empty());
    }
}

#[test]
fn test_master_message_roundtrip() {
    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();

    let messages = vec![
        MasterMessage::RegisterAck {
            accepted: true,
            worker_id,
            heartbeat_interval_secs: 3,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
            message: Some("Registered".into()),
        },
        MasterMessage::HeartbeatAck {
            timestamp: 1726747201,
        },
        MasterMessage::AssignTask {
            task: Task {
                id: task_id,
                spec: TaskSpec::Command {
                    program: "cargo".into(),
                    args: vec!["test".into()],
                    env: HashMap::new(),
                    working_dir: None,
                    stdin: None,
                },
                requirements: TaskRequirements {
                    cpu_cores: 4,
                    ram_mb: 8192,
                    gpu_required: false,
                    timeout_secs: 60,
                    max_retries: None,
                    vram_mb: None,
                    extensions: Vec::new(),
                },
                created_at_utc: 1726747200,
                tags: vec!["test".into()],
                latest_checkpoint: None,
                staged_files: Vec::new(),
                expected_outputs: Vec::new(),
                inputs: Vec::new(),
                output_globs: Vec::new(),
                workspace: None,
                extensions: Vec::new(),
            },
        },
        MasterMessage::AssignTaskWithCheckpoint {
            task: Task {
                id: task_id,
                spec: TaskSpec::Command {
                    program: "cargo".into(),
                    args: vec!["test".into()],
                    env: HashMap::new(),
                    working_dir: None,
                    stdin: None,
                },
                requirements: TaskRequirements {
                    cpu_cores: 4,
                    ram_mb: 8192,
                    gpu_required: false,
                    timeout_secs: 60,
                    max_retries: None,
                    vram_mb: None,
                    extensions: Vec::new(),
                },
                created_at_utc: 1726747200,
                tags: vec!["test".into()],
                latest_checkpoint: None,
                staged_files: Vec::new(),
                expected_outputs: Vec::new(),
                inputs: Vec::new(),
                output_globs: Vec::new(),
                workspace: None,
                extensions: Vec::new(),
            },
            latest_checkpoint: Some(CheckpointData::new(
                5,
                TaskBytes::copy_from_slice(b"checkpoint-state"),
            )),
        },
        MasterMessage::CancelTask {
            task_id,
            reason: Some("Canceled by test".into()),
        },
        MasterMessage::Shutdown {
            reason: "Shutdown command".into(),
            grace_period_secs: Some(10),
        },
    ];

    for msg in messages {
        let json = serde_json::to_string(&msg).expect("serialization failed");
        let deserialized: MasterMessage =
            serde_json::from_str(&json).expect("deserialization failed");
        assert_eq!(msg, deserialized);

        assert!(!msg.variant_name().is_empty());
    }
}

#[test]
fn test_codec_big_endian_wire_layout() {
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    let mut codec = default_codec();
    let mut buf = BytesMut::new();
    let payload = Bytes::from_static(b"{\"type\":\"HeartbeatAck\",\"timestamp\":123}");

    codec.encode(payload.clone(), &mut buf).unwrap();

    // Must prepend 4-byte big-endian length prefix
    assert_eq!(buf.len(), 4 + payload.len());
    let wire_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    assert_eq!(wire_len, payload.len());
    assert_eq!(&buf[4..], payload.as_ref());

    // Decode must strip prefix and yield exact payload
    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(buf.len(), 0);
}

#[tokio::test]
async fn test_message_transport_duplex() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut client = MessageTransport::new(client_io);
    let mut server = MessageTransport::new(server_io);

    let worker_id = Uuid::new_v4();
    let reg = WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities {
            name: "w1".into(),
            cpu_cores: 4,
            ram_mb: 8192,
            has_gpu: false,
            is_simulated_gpu: false,
            gpu_device_name: None,
            tags: vec![],
            mobile: None,
            ..Default::default()
        },
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: ProtocolCapabilities::all_supported(),
    };

    // Client -> Server
    client.send_msg(&reg).await.unwrap();
    let server_recv: Option<WorkerMessage> = server.recv_msg().await.unwrap();
    assert_eq!(server_recv, Some(reg));

    // Server -> Client
    let ack = MasterMessage::RegisterAck {
        accepted: true,
        worker_id,
        heartbeat_interval_secs: 3,
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: ProtocolCapabilities::all_supported(),
        message: None,
    };
    server.send_msg(&ack).await.unwrap();
    let client_recv: Option<MasterMessage> = client.recv_msg().await.unwrap();
    assert_eq!(client_recv, Some(ack));
}

#[tokio::test]
async fn test_message_transport_split() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (mut client_tx, mut client_rx) = MessageTransport::new(client_io).split();
    let (mut server_tx, mut server_rx) = MessageTransport::new(server_io).split();

    let hb = WorkerMessage::Heartbeat {
        worker_id: Uuid::new_v4(),
        timestamp: 1000,
        active_tasks: 0,
        cpu_usage_pct: 0.0,
        ram_available_mb: 0,
    };

    client_tx.send_msg(&hb).await.unwrap();
    let received: Option<WorkerMessage> = server_rx.recv_msg().await.unwrap();
    assert_eq!(received, Some(hb));

    let ack = MasterMessage::HeartbeatAck { timestamp: 1000 };
    server_tx.send_msg(&ack).await.unwrap();
    let ack_received: Option<MasterMessage> = client_rx.recv_msg().await.unwrap();
    assert_eq!(ack_received, Some(ack));
}

#[test]
fn test_partial_frame_streaming() {
    use bytes::BytesMut;
    use tokio_util::codec::Decoder;

    let mut codec = default_codec();
    let mut buf = BytesMut::new();

    // 1. Feed only 2 bytes of length prefix (< 4 bytes)
    buf.extend_from_slice(&[0, 0]);
    let res = codec.decode(&mut buf).unwrap();
    assert!(res.is_none());

    // 2. Complete the 4-byte header: length = 10
    buf.extend_from_slice(&[0, 10]);
    // And feed only 4 bytes of payload
    buf.extend_from_slice(b"abcd");
    let res = codec.decode(&mut buf).unwrap();
    assert!(res.is_none());

    // 3. Feed remaining 6 bytes of payload
    buf.extend_from_slice(b"efghij");
    let res = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(res, Bytes::from_static(b"abcdefghij"));
    assert_eq!(buf.len(), 0);
}

#[test]
fn test_pipelined_multiple_frames() {
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    let mut codec = default_codec();
    let mut buf = BytesMut::new();

    let msg1 = Bytes::from_static(b"frame 1");
    let msg2 = Bytes::from_static(b"frame 2");
    let msg3 = Bytes::from_static(b"frame 3");

    codec.encode(msg1.clone(), &mut buf).unwrap();
    codec.encode(msg2.clone(), &mut buf).unwrap();
    codec.encode(msg3.clone(), &mut buf).unwrap();

    // Verify sequential retrieval from combined buffer
    assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), msg1);
    assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), msg2);
    assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), msg3);
    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn test_oversized_frame_rejection() {
    use bytes::BytesMut;
    use tokio_util::codec::Decoder;

    let mut codec = default_codec();
    let mut buf = BytesMut::new();

    // Send a frame length of 70 MB (> 64 MB MAX_FRAME_SIZE)
    let bad_length: u32 = 70 * 1024 * 1024;
    buf.extend_from_slice(&bad_length.to_be_bytes());

    let res = codec.decode(&mut buf);
    assert!(res.is_err(), "Oversized frame must trigger error");
    let err = res.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn test_malformed_json_recovery() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut client_writer = MessageWriter::new(client_io);
    let mut server = MessageTransport::new(server_io);

    // Client sends corrupted JSON in Frame 1
    client_writer
        .send_raw_frame(Bytes::from_static(b"{\"type\": \"Corrupt\""))
        .await
        .unwrap();

    // Client sends valid JSON in Frame 2
    let valid_hb = WorkerMessage::Heartbeat {
        worker_id: Uuid::new_v4(),
        timestamp: 2000,
        active_tasks: 0,
        cpu_usage_pct: 0.0,
        ram_available_mb: 0,
    };
    client_writer.send_msg(&valid_hb).await.unwrap();

    // Server reads Frame 1 -> JSON parse error
    let res1: Result<Option<WorkerMessage>, _> = server.recv_msg().await;
    assert!(res1.is_err(), "Frame 1 must fail JSON deserialization");

    // Server reads Frame 2 -> Frame boundary preserved, succeeds!
    let res2: Option<WorkerMessage> = server.recv_msg().await.unwrap();
    assert_eq!(res2, Some(valid_hb));
}

#[tokio::test]
async fn test_eof_handling() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut server = MessageTransport::new(server_io);

    // Drop client immediately (clean EOF)
    drop(client_io);

    let msg: Option<WorkerMessage> = server.recv_msg().await.unwrap();
    assert!(msg.is_none(), "Clean disconnect must return Ok(None)");
}

#[test]
fn test_heartbeat_serde_default_backward_compat() {
    // Old JSON without cpu_usage_pct and ram_available_mb
    let json = r#"{"type":"Heartbeat","worker_id":"00000000-0000-0000-0000-000000000001","timestamp":100,"active_tasks":1}"#;
    let parsed: WorkerMessage = serde_json::from_str(json).expect("deserialize old heartbeat");
    match parsed {
        WorkerMessage::Heartbeat {
            cpu_usage_pct,
            ram_available_mb,
            ..
        } => {
            assert_eq!(cpu_usage_pct, 0.0);
            assert_eq!(ram_available_mb, 0);
        }
        _ => panic!("Expected Heartbeat"),
    }
}

#[test]
fn test_client_message_response_roundtrip() {
    let task_id = TaskId::new();
    let submit_msg = ClientMessage::GetTaskStatus { task_id };
    let json = serde_json::to_string(&submit_msg).expect("serialize ClientMessage");
    let deserialized: ClientMessage = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(submit_msg, deserialized);

    let response = ClientResponse::TaskSubmitted { task_id };
    let resp_json = serde_json::to_string(&response).expect("serialize ClientResponse");
    let resp_deserialized: ClientResponse = serde_json::from_str(&resp_json).expect("deserialize");
    assert_eq!(response, resp_deserialized);
}

#[test]
fn test_wire_codec_discriminator_serialization() {
    let wid = Uuid::new_v4();
    let msg = WorkerMessage::Heartbeat {
        worker_id: wid,
        timestamp: 12345,
        active_tasks: 2,
        cpu_usage_pct: 12.5,
        ram_available_mb: 4096,
    };

    // Bincode serialized format begins with 0x02
    let bin_bytes = serialize_message(&msg, WireCodec::Bincode).expect("serialize bincode");
    assert_eq!(bin_bytes[0], WIRE_FORMAT_BINCODE);

    // JSON serialized format begins with 0x01
    let json_bytes = serialize_message(&msg, WireCodec::Json).expect("serialize json");
    assert_eq!(json_bytes[0], WIRE_FORMAT_JSON);

    // Verify deserialization correctly recovers the message and detected codec
    let (recovered_bin, codec_bin): (WorkerMessage, WireCodec) =
        deserialize_message(&bin_bytes).expect("deserialize bincode");
    assert_eq!(recovered_bin, msg);
    assert_eq!(codec_bin, WireCodec::Bincode);

    let (recovered_json, codec_json): (WorkerMessage, WireCodec) =
        deserialize_message(&json_bytes).expect("deserialize json");
    assert_eq!(recovered_json, msg);
    assert_eq!(codec_json, WireCodec::Json);
}

#[test]
fn test_wire_codec_raw_json_fallback() {
    let wid = Uuid::new_v4();
    let raw_json = format!(
        r#"{{"type":"Heartbeat","worker_id":"{}","timestamp":999,"active_tasks":0,"cpu_usage_pct":0.0,"ram_available_mb":1024}}"#,
        wid
    );
    let raw_bytes = Bytes::copy_from_slice(raw_json.as_bytes());

    let (msg, codec): (WorkerMessage, WireCodec) =
        deserialize_message(&raw_bytes).expect("deserialize raw json");
    assert_eq!(codec, WireCodec::Json);
    match msg {
        WorkerMessage::Heartbeat {
            worker_id,
            timestamp,
            ..
        } => {
            assert_eq!(worker_id, wid);
            assert_eq!(timestamp, 999);
        }
        _ => panic!("unexpected message variant"),
    }
}

#[test]
fn test_wire_codec_invalid_discriminator() {
    let bad_payload = Bytes::from_static(&[0xFF, 0x01, 0x02, 0x03]);
    let err = deserialize_message::<WorkerMessage>(&bad_payload).unwrap_err();
    match err {
        ProtocolError::InvalidFormatTag(0xFF) => {}
        other => panic!("expected InvalidFormatTag(0xFF), got {other:?}"),
    }
}

#[tokio::test]
async fn test_transport_auto_codec_negotiation() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);

    // Client uses Json codec
    let mut client = MessageTransport::with_codec(client_io, WireCodec::Json);
    // Server defaults to Bincode
    let mut server = MessageTransport::with_codec(server_io, WireCodec::Bincode);

    let wid = Uuid::new_v4();
    let reg_msg = WorkerMessage::Register {
        worker_id: wid,
        capabilities: crate::capabilities::WorkerCapabilities::new(
            "worker-1", 4, 8192, false, false, None,
        ),
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: ProtocolCapabilities::all_supported(),
    };

    // Client sends Register message in JSON format
    client.send_msg(&reg_msg).await.expect("client send");

    // Server receives and auto-detects codec
    let (inbound, detected_codec) = server
        .recv_msg_with_codec::<InboundMessage>()
        .await
        .expect("server recv")
        .expect("some message");

    assert_eq!(detected_codec, WireCodec::Json);
    assert_eq!(inbound, InboundMessage::Worker(reg_msg));

    // Server adjusts outbound codec to match client
    server.set_outbound_codec(detected_codec);

    let ack = MasterMessage::RegisterAck {
        accepted: true,
        worker_id: wid,
        heartbeat_interval_secs: 3,
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: ProtocolCapabilities::all_supported(),
        message: None,
    };
    server.send_msg(&ack).await.expect("server send ack");

    // Client receives RegisterAck successfully in JSON
    let client_ack: MasterMessage = client
        .recv_msg()
        .await
        .expect("client recv")
        .expect("some ack");
    assert_eq!(client_ack, ack);
}

#[tokio::test]
async fn test_recv_raw_frame_max_enforces_limit_on_header_and_restores_it() {
    use tokio::io::AsyncWriteExt;
    let (mut raw, server_io) = tokio::io::duplex(1 << 20);
    let mut server = MessageTransport::new(server_io);

    // Exactly at the limit is accepted; the transport limit is back to normal afterwards.
    let mut frame = (64u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&[7u8; 64]);
    raw.write_all(&frame).await.unwrap();
    let got = server.recv_raw_frame_max(64).await.unwrap().unwrap();
    assert_eq!(got.len(), 64);

    let mut big = (4096u32).to_be_bytes().to_vec();
    big.extend_from_slice(&[9u8; 4096]);
    raw.write_all(&big).await.unwrap();
    assert_eq!(server.recv_raw_frame().await.unwrap().unwrap().len(), 4096);

    // One byte over is refused from the header alone: the payload never has to arrive.
    raw.write_all(&(65u32).to_be_bytes()).await.unwrap();
    match server.recv_raw_frame_max(64).await {
        Err(ProtocolError::FrameTooLarge { max, .. }) => assert_eq!(max, 64),
        other => panic!("expected FrameTooLarge, got {other:?}"),
    }

    // Clean EOF is reported as None, and the limit cannot be raised above the global cap.
    let (raw2, io2) = tokio::io::duplex(64);
    let mut server2 = MessageTransport::new(io2);
    drop(raw2);
    assert!(server2
        .recv_raw_frame_max(usize::MAX)
        .await
        .unwrap()
        .is_none());
}

/// Bincode is positional, so a new variant is only wire-compatible if it is appended: every
/// existing variant must keep its index and `GetTaskResult` must take the next free one.
#[test]
fn test_get_task_result_is_appended_last_and_keeps_existing_bincode_indices() {
    fn variant_index(msg: &ClientMessage) -> u32 {
        let bytes = serialize_message(msg, WireCodec::Bincode).expect("serialize");
        assert_eq!(bytes[0], WIRE_FORMAT_BINCODE);
        u32::from_le_bytes(bytes[1..5].try_into().unwrap())
    }
    let task_id = TaskId::new();
    let job = crate::mapreduce::MapReduceJobSpec::new(
        "idx",
        vec!["a".into()],
        crate::mapreduce::MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        crate::mapreduce::ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        1,
        1,
        30,
    );
    let task = Task::new(
        TaskSpec::command("echo", vec!["x".into()]),
        TaskRequirements::generic(1, 10),
    );
    assert_eq!(
        variant_index(&ClientMessage::SubmitTask { task, wait: false }),
        0
    );
    assert_eq!(variant_index(&ClientMessage::GetTaskStatus { task_id }), 1);
    assert_eq!(variant_index(&ClientMessage::CancelTask { task_id }), 2);
    assert_eq!(variant_index(&ClientMessage::ClusterStatus), 3);
    assert_eq!(variant_index(&ClientMessage::ListWorkers), 4);
    assert_eq!(variant_index(&ClientMessage::SubmitMapReduce { job }), 5);
    assert_eq!(variant_index(&ClientMessage::GetTaskResult { task_id }), 6);
}

#[test]
fn test_get_task_result_roundtrip_both_codecs() {
    let task_id = TaskId::new();
    let msg = ClientMessage::GetTaskResult { task_id };

    // Plain serde_json (human-readable mirror enum)
    let json = serde_json::to_string(&msg).expect("serialize json");
    assert!(json.contains("\"type\":\"GetTaskResult\""), "{json}");
    let back: ClientMessage = serde_json::from_str(&json).expect("deserialize json");
    assert_eq!(back, msg);

    // Both wire codecs, as a raw ClientMessage and wrapped in an InboundMessage
    for codec in [WireCodec::Bincode, WireCodec::Json] {
        let bytes = serialize_message(&msg, codec).expect("serialize");
        let (recovered, detected): (ClientMessage, WireCodec) =
            deserialize_message(&bytes).expect("deserialize ClientMessage");
        assert_eq!(recovered, msg);
        assert_eq!(detected, codec);

        // A peer that opens with the bare ClientMessage (what the CLI does) must be routed as a client.
        let (inbound, detected): (InboundMessage, WireCodec) =
            deserialize_message(&bytes).expect("deserialize InboundMessage from bare message");
        assert_eq!(inbound, InboundMessage::Client(msg.clone()));
        assert_eq!(detected, codec);

        let wrapped = InboundMessage::Client(msg.clone());
        let bytes = serialize_message(&wrapped, codec).expect("serialize inbound");
        let (recovered, _): (InboundMessage, WireCodec) =
            deserialize_message(&bytes).expect("deserialize wrapped");
        assert_eq!(recovered, wrapped);
    }
}

/// `MobileTelemetry` is appended after `Disconnecting`: every earlier `WorkerMessage` keeps its
/// positional (bincode) index and the new variant takes the next free one.
#[test]
fn test_mobile_telemetry_is_appended_last_and_keeps_existing_bincode_indices() {
    fn variant_index(msg: &WorkerMessage) -> u32 {
        let bytes = serialize_message(msg, WireCodec::Bincode).expect("serialize");
        assert_eq!(bytes[0], WIRE_FORMAT_BINCODE);
        u32::from_le_bytes(bytes[1..5].try_into().unwrap())
    }
    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();
    assert_eq!(
        variant_index(&WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities::detect(Some("idx".into()), Some(1), false),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        }),
        0
    );
    assert_eq!(
        variant_index(&WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 1,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 1,
        }),
        1
    );
    assert_eq!(
        variant_index(&WorkerMessage::TaskProgress {
            worker_id,
            task_id,
            status: TaskStatus::Running,
        }),
        2
    );
    assert_eq!(
        variant_index(&WorkerMessage::Checkpoint {
            task_id,
            sequence: 1,
            delta_state: TaskBytes::copy_from_slice(b"x"),
        }),
        4
    );
    assert_eq!(
        variant_index(&WorkerMessage::Disconnecting {
            worker_id,
            reason: "bye".into(),
        }),
        5
    );
    assert_eq!(
        variant_index(&WorkerMessage::MobileTelemetry {
            worker_id,
            battery_pct: Some(9),
            is_charging: Some(false),
            thermal_throttled: true,
            battery_temperature: None,
            network_type: None,
            battery_tier: None,
            thermal_tier: None,
        }),
        6
    );
}

#[test]
fn test_mobile_telemetry_roundtrip_both_codecs() {
    let worker_id = Uuid::new_v4();
    let cases = [
        WorkerMessage::MobileTelemetry {
            worker_id,
            battery_pct: Some(14),
            is_charging: Some(false),
            thermal_throttled: true,
            battery_temperature: Some(38.5),
            network_type: Some("cellular".into()),
            battery_tier: Some(crate::capabilities::BatteryTier::Critical),
            thermal_tier: Some(crate::capabilities::ThermalTier::Warm),
        },
        // Unknown readings are legitimate (no battery sensor reachable): `None` must survive.
        WorkerMessage::MobileTelemetry {
            worker_id,
            battery_pct: None,
            is_charging: None,
            thermal_throttled: false,
            battery_temperature: None,
            network_type: None,
            battery_tier: None,
            thermal_tier: None,
        },
    ];
    for msg in cases {
        assert_eq!(msg.variant_name(), "MobileTelemetry");
        assert_eq!(msg.worker_id(), worker_id);

        // Plain serde_json goes through the human-readable mirror.
        let json = serde_json::to_string(&msg).expect("serialize json");
        assert!(json.contains("\"type\":\"MobileTelemetry\""), "{json}");
        let back: WorkerMessage = serde_json::from_str(&json).expect("deserialize json");
        assert_eq!(back, msg);

        for codec in [WireCodec::Bincode, WireCodec::Json] {
            let bytes = serialize_message(&msg, codec).expect("serialize");
            let (recovered, detected): (WorkerMessage, WireCodec) =
                deserialize_message(&bytes).expect("deserialize");
            assert_eq!(recovered, msg);
            assert_eq!(detected, codec);
        }
    }

    // A JSON peer may omit the optional readings entirely.
    let sparse = format!(r#"{{"type":"MobileTelemetry","worker_id":"{worker_id}"}}"#);
    let parsed: WorkerMessage = serde_json::from_str(&sparse).expect("sparse json");
    assert_eq!(
        parsed,
        WorkerMessage::MobileTelemetry {
            worker_id,
            battery_pct: None,
            is_charging: None,
            thermal_throttled: false,
            battery_temperature: None,
            network_type: None,
            battery_tier: None,
            thermal_tier: None,
        }
    );
}

/// Bytes a transport read ahead of a handshake (the next frame sent right behind `RegisterAck`)
/// must be delivered by the split reader at once, not stranded until more bytes arrive.
#[tokio::test]
async fn test_split_reader_decodes_frames_that_were_read_ahead_of_the_split() {
    use tokio::io::AsyncWriteExt;
    let (mut peer, ours) = tokio::io::duplex(64 * 1024);
    let ack = MasterMessage::HeartbeatAck { timestamp: 1 };
    let next = MasterMessage::HeartbeatAck { timestamp: 2 };
    // Both frames arrive in a single write, so one socket read buffers both.
    let mut both = Vec::new();
    for msg in [&ack, &next] {
        let payload = serialize_message(msg, WireCodec::Bincode).unwrap();
        both.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        both.extend_from_slice(&payload);
    }
    peer.write_all(&both).await.unwrap();

    let mut transport = MessageTransport::new(ours);
    assert_eq!(
        transport.recv_msg::<MasterMessage>().await.unwrap(),
        Some(ack)
    );

    // The peer sends nothing more. The second frame is already in the buffer.
    let (_writer, mut reader) = transport.split();
    let got = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reader.recv_msg::<MasterMessage>(),
    )
    .await
    .expect("a frame read ahead of the split must not wait for more socket traffic")
    .unwrap();
    assert_eq!(got, Some(next));
}

#[test]
fn test_auth_frames_are_not_typed_messages() {
    // The typed decoder must refuse handshake frames, never misparse them as messages.
    let frame = [WIRE_FORMAT_AUTH, 0x01, 1, 2, 3];
    assert!(matches!(
        deserialize_message::<WorkerMessage>(&frame),
        Err(ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH))
    ));
    assert!(matches!(
        deserialize_message::<MasterMessage>(&frame),
        Err(ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH))
    ));
    assert!(matches!(
        deserialize_message::<InboundMessage>(&frame),
        Err(ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH))
    ));
}
#[test]
fn test_master_feature_announcement_round_trips_and_old_masters_announce_nothing() {
    use super::master_features::{announced, announcement, MOBILE_TELEMETRY, SUPPORTED};

    let message = announcement(SUPPORTED);
    assert!(announced(Some(&message), MOBILE_TELEMETRY));
    assert!(announced(
        Some("features=other, mobile-telemetry"),
        MOBILE_TELEMETRY
    ));
    // A master built before the announcement sends no message on an accepted ack.
    assert!(!announced(None, MOBILE_TELEMETRY));
    assert!(!announced(Some(""), MOBILE_TELEMETRY));
    // Not a feature list, or a different one: nothing is assumed.
    assert!(!announced(Some("mobile-telemetry"), MOBILE_TELEMETRY));
    assert!(!announced(
        Some("features=mobile-telemetry-v2"),
        MOBILE_TELEMETRY
    ));
    assert!(!announced(Some("features="), MOBILE_TELEMETRY));

    // The announcement is just the `message` of an accepted RegisterAck: both codecs carry it.
    let ack = MasterMessage::RegisterAck {
        accepted: true,
        worker_id: Uuid::nil(),
        heartbeat_interval_secs: 3,
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: ProtocolCapabilities::all_supported(),
        message: Some(message),
    };
    for codec in [WireCodec::Json, WireCodec::Bincode] {
        let bytes = serialize_message(&ack, codec).unwrap();
        let (back, _): (MasterMessage, _) = deserialize_message(&bytes).unwrap();
        assert_eq!(back, ack, "{codec:?}");
    }
}

#[test]
fn test_session_resume_feature_and_message_round_trip() {
    use super::master_features::{
        announced, announcement, parse_session_resume_hold_secs, session_resume_feature, BLOBS,
        MAX_SESSION_RESUME_HOLD_SECS, MOBILE_TELEMETRY, SESSION_RESUME_PREFIX,
    };

    assert_eq!(ProtocolCapabilities::SESSION_RESUME.bits(), 1 << 12);
    assert!(
        ProtocolCapabilities::all_supported().contains(ProtocolCapabilities::SESSION_RESUME),
        "SESSION_RESUME must be advertised in all_supported()"
    );
    assert_eq!(SESSION_RESUME_PREFIX, "session-resume=");
    assert_eq!(MAX_SESSION_RESUME_HOLD_SECS, 3_600);

    let token = session_resume_feature(30);
    assert_eq!(token, "session-resume=30");
    let msg = announcement(&[MOBILE_TELEMETRY, &token, BLOBS]);
    assert_eq!(msg, "features=mobile-telemetry,session-resume=30,blobs");
    assert!(announced(Some(&msg), MOBILE_TELEMETRY));
    assert!(announced(Some(&msg), BLOBS));
    assert_eq!(parse_session_resume_hold_secs(Some(&msg)), Some(30));
    assert_eq!(
        parse_session_resume_hold_secs(Some("features=session-resume=0")),
        Some(0)
    );
    assert_eq!(
        parse_session_resume_hold_secs(Some("features=mobile-telemetry, session-resume=45")),
        Some(45)
    );
    assert_eq!(
        parse_session_resume_hold_secs(Some("features=session-resume=3600")),
        Some(3_600)
    );
    assert_eq!(
        parse_session_resume_hold_secs(Some("features=session-resume=3601")),
        None
    );
    assert_eq!(
        parse_session_resume_hold_secs(Some(&format!("features=session-resume={}", u64::MAX))),
        None
    );
    assert_eq!(parse_session_resume_hold_secs(None), None);
    assert_eq!(parse_session_resume_hold_secs(Some("")), None);
    assert_eq!(
        parse_session_resume_hold_secs(Some("features=mobile-telemetry,blobs")),
        None
    );
    assert_eq!(
        parse_session_resume_hold_secs(Some("session-resume=30")),
        None
    );
    assert_eq!(
        parse_session_resume_hold_secs(Some("features=session-resume=")),
        None
    );
    assert_eq!(
        parse_session_resume_hold_secs(Some("features=session-resume=abc")),
        None
    );
    assert_eq!(
        parse_session_resume_hold_secs(Some("features=session-resume=-5")),
        None
    );

    let worker_id = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
    let t1 = TaskId(Uuid::from_u128(0xaaaa));
    let t2 = TaskId(Uuid::from_u128(0xbbbb));
    let resume = WorkerMessage::ResumeSession {
        worker_id,
        running: vec![t1, t2],
    };
    assert_eq!(resume.variant_name(), "ResumeSession");
    assert_eq!(resume.worker_id(), worker_id);

    for codec in [WireCodec::Bincode, WireCodec::Json] {
        let bytes = serialize_message(&resume, codec).unwrap();
        let (decoded, detected): (WorkerMessage, _) = deserialize_message(&bytes).unwrap();
        assert_eq!(detected, codec);
        assert_eq!(decoded, resume);

        let (inbound, _): (InboundMessage, _) = deserialize_message(&bytes).unwrap();
        assert_eq!(inbound, InboundMessage::Worker(resume.clone()));
    }
}
