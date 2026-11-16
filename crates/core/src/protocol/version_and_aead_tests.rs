use bytes::Bytes;
use uuid::Uuid;

use crate::capabilities::WorkerCapabilities;
use crate::task::{Task, TaskId, TaskRequirements, TaskSpec, TaskStatus};

use super::client_msg_mirrors::{serialize_client_message_with_version, BinaryClientMessage};
use super::client_resp_mirrors::serialize_client_response_with_version;
use super::codec::{
    bincode_exact_deserialize, AEAD_ERR_CLOSED, AEAD_ERR_INTEGRITY, AEAD_ERR_NOT_ENCRYPTED,
    AEAD_ERR_OVERFLOW, AEAD_ERR_SEQUENCE, AEAD_ERR_TOO_SHORT,
};
use super::*;

/// The text of a `ProtocolError::Violation`; the record layer reports nothing else.
fn violation_text(err: ProtocolError) -> String {
    match err {
        ProtocolError::Violation(text) => text,
        other => panic!("expected a protocol violation, got {other:?}"),
    }
}

fn server_reader(session_key: &[u8; 32]) -> AeadReaderSession {
    derive_aead_keys(session_key, EndpointRole::Server).1
}

#[test]
fn test_aead_sessions_roundtrip_in_both_directions() {
    let session_key = [42u8; 32];
    let (mut client_writer, mut client_reader) =
        derive_aead_keys(&session_key, EndpointRole::Client);
    let (mut server_writer, mut server_reader) =
        derive_aead_keys(&session_key, EndpointRole::Server);

    for seq in 0..3u64 {
        let body = format!("client frame number {seq}");
        let encrypted = client_writer.encrypt_frame(body.as_bytes()).unwrap();
        assert_eq!(encrypted[0], WIRE_FORMAT_ENCRYPTED);
        assert_eq!(&encrypted[1..9], &seq.to_be_bytes());
        assert_eq!(encrypted.len(), body.len() + AEAD_RECORD_OVERHEAD);
        assert!(!encrypted.windows(body.len()).any(|w| w == body.as_bytes()));
        let decrypted = server_reader.decrypt_frame(&encrypted).unwrap();
        assert_eq!(&decrypted[..], body.as_bytes());

        let server_reply = server_writer.encrypt_frame(b"ack payload").unwrap();
        let client_decrypted = client_reader.decrypt_frame(&server_reply).unwrap();
        assert_eq!(&client_decrypted[..], b"ack payload");
    }
    assert_eq!(client_writer.current_seq(), 3);
    assert_eq!(server_reader.expected_seq(), 3);
}

#[test]
fn test_aead_record_with_any_flipped_bit_is_refused_and_closes_the_session() {
    let session_key = [7u8; 32];
    let (mut writer, _) = derive_aead_keys(&session_key, EndpointRole::Client);
    let body = b"AssignTask: echo hello";
    let record = writer.encrypt_frame(body).unwrap();

    for index in 0..record.len() {
        for bit in [0u8, 3, 7] {
            let mut tampered = record.to_vec();
            tampered[index] ^= 1 << bit;
            let mut reader = server_reader(&session_key);
            let refused = violation_text(reader.decrypt_frame(&tampered).unwrap_err());
            // The header fails the format or sequence check, everything else Poly1305.
            let expected = match index {
                0 => AEAD_ERR_NOT_ENCRYPTED,
                1..=8 => AEAD_ERR_SEQUENCE,
                _ => AEAD_ERR_INTEGRITY,
            };
            assert_eq!(refused, expected, "byte {index} bit {bit}");
            // Fail closed: after the forgery even the genuine record is refused.
            assert_eq!(
                violation_text(reader.decrypt_frame(&record).unwrap_err()),
                AEAD_ERR_CLOSED,
                "byte {index} bit {bit}"
            );
        }
    }

    // Control: the untouched record opens.
    let mut reader = server_reader(&session_key);
    assert_eq!(&reader.decrypt_frame(&record).unwrap()[..], body);
}

#[test]
fn test_aead_replayed_reordered_and_dropped_records_are_refused() {
    let session_key = [9u8; 32];
    let (mut writer, _) = derive_aead_keys(&session_key, EndpointRole::Client);
    let records: Vec<Bytes> = (0..3u8)
        .map(|i| writer.encrypt_frame(&[i; 8]).unwrap())
        .collect();

    // Replay: the same record twice. The session is closed afterwards.
    let mut reader = server_reader(&session_key);
    reader.decrypt_frame(&records[0]).unwrap();
    assert_eq!(
        violation_text(reader.decrypt_frame(&records[0]).unwrap_err()),
        AEAD_ERR_SEQUENCE
    );
    assert_eq!(
        violation_text(reader.decrypt_frame(&records[1]).unwrap_err()),
        AEAD_ERR_CLOSED
    );

    // Reorder: record 1 before record 0.
    let mut reader = server_reader(&session_key);
    assert_eq!(
        violation_text(reader.decrypt_frame(&records[1]).unwrap_err()),
        AEAD_ERR_SEQUENCE
    );

    // Drop: record 1 never arrives.
    let mut reader = server_reader(&session_key);
    reader.decrypt_frame(&records[0]).unwrap();
    assert_eq!(
        violation_text(reader.decrypt_frame(&records[2]).unwrap_err()),
        AEAD_ERR_SEQUENCE
    );

    // Control: in order all three open.
    let mut reader = server_reader(&session_key);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(&reader.decrypt_frame(record).unwrap()[..], &[i as u8; 8]);
    }
}

#[test]
fn test_aead_reflected_and_foreign_records_are_refused() {
    let session_key = [11u8; 32];
    let (mut client_w, mut client_r) = derive_aead_keys(&session_key, EndpointRole::Client);
    let (mut server_w, mut server_r) = derive_aead_keys(&session_key, EndpointRole::Server);

    // Reflection: a record sent back to its own sender, who opens with the other direction's
    // key. The sequence number matches, so only the key can stop it.
    let to_server = client_w.encrypt_frame(b"from the client").unwrap();
    assert_eq!(
        violation_text(client_r.decrypt_frame(&to_server).unwrap_err()),
        AEAD_ERR_INTEGRITY
    );
    let to_client = server_w.encrypt_frame(b"from the server").unwrap();
    assert_eq!(
        violation_text(server_r.decrypt_frame(&to_client).unwrap_err()),
        AEAD_ERR_INTEGRITY
    );

    // A record of another connection (other session key) with the expected sequence number.
    let (mut other_w, _) = derive_aead_keys(&[12u8; 32], EndpointRole::Client);
    let foreign = other_w.encrypt_frame(b"from the client").unwrap();
    assert_eq!(
        violation_text(
            server_reader(&session_key)
                .decrypt_frame(&foreign)
                .unwrap_err()
        ),
        AEAD_ERR_INTEGRITY
    );

    // Control: delivered to the right side, the record opens.
    assert_eq!(
        &server_reader(&session_key)
            .decrypt_frame(&to_server)
            .unwrap()[..],
        b"from the client"
    );
}

#[test]
fn test_aead_sequence_counter_never_wraps() {
    let key = [13u8; 32];
    let mut writer = AeadWriterSession::starting_at(&key, u64::MAX - 1);
    let last = writer.encrypt_frame(b"last record").unwrap();
    assert_eq!(&last[1..9], &(u64::MAX - 1).to_be_bytes());
    assert_eq!(writer.current_seq(), u64::MAX);

    // `u64::MAX` is never used as a nonce and the counter does not wrap to 0: the writer
    // refuses for good.
    for _ in 0..2 {
        assert_eq!(
            violation_text(writer.encrypt_frame(b"one more").unwrap_err()),
            AEAD_ERR_OVERFLOW
        );
        assert_eq!(writer.current_seq(), u64::MAX);
    }

    let mut reader = AeadReaderSession::starting_at(&key, u64::MAX - 1);
    assert_eq!(&reader.decrypt_frame(&last).unwrap()[..], b"last record");
    assert_eq!(reader.expected_seq(), u64::MAX);
}

#[test]
fn test_aead_oversized_frame_is_refused_without_using_a_sequence_number() {
    let key = [14u8; 32];
    let mut writer = AeadWriterSession::new(&key);
    let mut reader = AeadReaderSession::new(&key);

    match writer.encrypt_frame(&vec![0u8; MAX_FRAME_SIZE - AEAD_RECORD_OVERHEAD + 1]) {
        Err(ProtocolError::FrameTooLarge { size, max }) => {
            assert_eq!((size, max), (MAX_FRAME_SIZE + 1, MAX_FRAME_SIZE));
        }
        other => panic!(
            "expected FrameTooLarge, got {:?}",
            other.map(|record| record.len())
        ),
    }
    assert_eq!(
        writer.current_seq(),
        0,
        "a refused frame must not use a nonce"
    );

    // The direction is still in sync: the next record carries sequence number 0 and opens.
    let record = writer.encrypt_frame(b"small").unwrap();
    assert_eq!(&reader.decrypt_frame(&record).unwrap()[..], b"small");
}

#[test]
fn test_aead_reader_refuses_truncated_and_unencrypted_frames() {
    let key = [15u8; 32];
    let header = [vec![WIRE_FORMAT_ENCRYPTED], 0u64.to_be_bytes().to_vec()].concat();
    let cases: Vec<(&str, Vec<u8>, &str)> = vec![
        ("empty", vec![], AEAD_ERR_TOO_SHORT),
        ("header only", header.clone(), AEAD_ERR_TOO_SHORT),
        (
            "one byte short of a tag",
            [header.clone(), vec![0u8; 15]].concat(),
            AEAD_ERR_TOO_SHORT,
        ),
        (
            "header and a zero tag",
            [header, vec![0u8; 16]].concat(),
            AEAD_ERR_INTEGRITY,
        ),
        (
            "plaintext bincode",
            [vec![WIRE_FORMAT_BINCODE], vec![7u8; 40]].concat(),
            AEAD_ERR_NOT_ENCRYPTED,
        ),
        (
            "plaintext json",
            br#"{"type":"ClusterStatus","padding":"............"}"#.to_vec(),
            AEAD_ERR_NOT_ENCRYPTED,
        ),
        (
            "handshake frame",
            [vec![WIRE_FORMAT_AUTH, 0x01, 2], vec![1u8; 32]].concat(),
            AEAD_ERR_NOT_ENCRYPTED,
        ),
    ];
    for (name, frame, expected) in cases {
        let mut reader = AeadReaderSession::new(&key);
        // Fixed texts: nothing from the frame (sequence number, bytes) is echoed.
        let text = violation_text(reader.decrypt_frame(&frame).unwrap_err());
        assert_eq!(text, expected, "{name}");
    }
}

#[tokio::test]
async fn test_encrypted_transport_duplex_and_rejection() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut client = MessageTransport::new(client_io);
    let mut server = MessageTransport::new(server_io);

    let session_key = [0x55u8; 32];
    client.enable_encryption(&session_key, EndpointRole::Client);
    server.enable_encryption(&session_key, EndpointRole::Server);

    assert!(client.is_encrypted());
    assert!(server.is_encrypted());

    let wid = Uuid::new_v4();
    let reg = WorkerMessage::register(
        wid,
        WorkerCapabilities::new("sec-worker", 4, 8192, false, false, None),
    );

    client.send_msg(&reg).await.unwrap();
    let recv_reg: WorkerMessage = server.recv_msg().await.unwrap().unwrap();
    assert_eq!(recv_reg, reg);

    let ack = MasterMessage::register_ack(
        true,
        wid,
        5,
        ProtocolCapabilities::all_supported(),
        Some("secure ok".into()),
    );
    server.send_msg(&ack).await.unwrap();
    let recv_ack: MasterMessage = client.recv_msg().await.unwrap().unwrap();
    assert_eq!(recv_ack, ack);
}

#[tokio::test]
async fn test_encrypted_transport_drops_a_tampered_frame_for_good() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut client = MessageTransport::new(client_io);
    let mut server = MessageTransport::new(server_io);
    let session_key = [0x56u8; 32];
    client.enable_encryption(&session_key, EndpointRole::Client);
    server.enable_encryption(&session_key, EndpointRole::Server);

    // What an on-path observer sees: a record, without the plaintext.
    let marker = "plaintext-marker-7f3a";
    let bye = WorkerMessage::Disconnecting {
        worker_id: Uuid::new_v4(),
        reason: marker.into(),
    };
    client.send_msg(&bye).await.unwrap();
    let on_wire = server.recv_raw_frame().await.unwrap().unwrap();
    assert_eq!(on_wire[0], WIRE_FORMAT_ENCRYPTED);
    assert!(!on_wire
        .windows(marker.len())
        .any(|w| w == marker.as_bytes()));

    // An attacker flips one bit of that record in transit: the receiver refuses it...
    let mut tampered = on_wire.to_vec();
    tampered[on_wire.len() / 2] ^= 0x01;
    client.send_raw_frame(Bytes::from(tampered)).await.unwrap();
    assert_eq!(
        violation_text(server.recv_msg::<WorkerMessage>().await.unwrap_err()),
        AEAD_ERR_INTEGRITY
    );
    // ...and nothing gets through afterwards, neither the genuine record nor a new one.
    client.send_raw_frame(on_wire).await.unwrap();
    assert_eq!(
        violation_text(server.recv_msg::<WorkerMessage>().await.unwrap_err()),
        AEAD_ERR_CLOSED
    );
    client.send_msg(&bye).await.unwrap();
    assert_eq!(
        violation_text(server.recv_msg::<WorkerMessage>().await.unwrap_err()),
        AEAD_ERR_CLOSED
    );
}

#[tokio::test]
async fn test_encrypted_split_reader_refuses_an_injected_plaintext_assignment() {
    // An on-path host injects a well-formed plaintext `AssignTask` into an encrypted
    // master -> worker connection (the worker reads through the split reader half).
    let (attacker_io, worker_io) = tokio::io::duplex(64 * 1024);
    let mut attacker = MessageTransport::new(attacker_io);
    let mut worker = MessageTransport::new(worker_io);
    worker.enable_encryption(&[0x66u8; 32], EndpointRole::Client);
    let (_worker_tx, mut worker_rx) = worker.split();
    assert!(worker_rx.is_encrypted());

    let task = Task::new(
        TaskSpec::new_shell_script("touch /tmp/injected"),
        TaskRequirements::generic(1, 30),
    );
    attacker
        .send_msg(&MasterMessage::AssignTask { task })
        .await
        .unwrap();
    assert_eq!(
        violation_text(worker_rx.recv_msg::<MasterMessage>().await.unwrap_err()),
        AEAD_ERR_NOT_ENCRYPTED
    );
}

#[test]
fn test_unknown_variant_graceful_handling() {
    // Unknown future WorkerMessage tag (e.g. tag 99) in Bincode
    let mut buf = vec![WIRE_FORMAT_BINCODE];
    buf.extend_from_slice(&99u32.to_le_bytes());
    buf.extend_from_slice(b"future-payload-bytes");

    let (msg, codec): (WorkerMessage, _) = deserialize_message(&buf).unwrap();
    assert_eq!(codec, WireCodec::Bincode);
    match msg {
        WorkerMessage::UnknownVariant { tag, raw_bytes } => {
            assert_eq!(tag, 99);
            assert_eq!(raw_bytes, b"future-payload-bytes");
        }
        other => panic!("expected UnknownVariant, got {other:?}"),
    }

    // InboundMessage wrapping unknown variant
    let (inbound, _): (InboundMessage, _) = deserialize_message(&buf).unwrap();
    match inbound {
        InboundMessage::Worker(WorkerMessage::UnknownVariant { tag, raw_bytes }) => {
            assert_eq!(tag, 99);
            assert_eq!(raw_bytes, b"future-payload-bytes");
        }
        other => panic!("expected InboundMessage::Worker(UnknownVariant), got {other:?}"),
    }

    // Unknown future MasterMessage tag in Bincode
    let mut master_buf = vec![WIRE_FORMAT_BINCODE];
    master_buf.extend_from_slice(&88u32.to_le_bytes());
    master_buf.extend_from_slice(b"future-master-payload");
    let (m_msg, _): (MasterMessage, _) = deserialize_message(&master_buf).unwrap();
    match m_msg {
        MasterMessage::UnknownVariant { tag, raw_bytes } => {
            assert_eq!(tag, 88);
            assert_eq!(raw_bytes, b"future-master-payload");
        }
        other => panic!("expected MasterMessage::UnknownVariant, got {other:?}"),
    }
}

#[test]
fn test_cluster_status_trailing_budget_fields_decode_into_legacy_bincode_client() {
    // F36-1: `active_bytes` and `max_active_bytes` must sit after `workers` in
    // `BinaryClientResponse::ClusterStatus` so a legacy bincode client (whose
    // `ClusterStatus` ends at `workers`) decodes the prefix cleanly and ignores
    // the trailing 16 bytes even when tasks are queued (`active_bytes > 0`).
    #[derive(Deserialize, Debug)]
    #[allow(dead_code)]
    enum LegacyBinaryClientResponse {
        TaskSubmitted {
            task_id: TaskId,
        },
        TaskCompleted {
            task_id: TaskId,
            result: TaskResult,
        },
        TaskStatusInfo {
            task_id: TaskId,
            status: TaskStatus,
            state_name: String,
            assigned_worker: Option<Uuid>,
            error: Option<String>,
        },
        TaskCancelled {
            task_id: TaskId,
            success: bool,
        },
        ClusterStatus {
            total_tasks: usize,
            pending_tasks: usize,
            running_tasks: usize,
            completed_tasks: usize,
            failed_tasks: usize,
            workers: Vec<WorkerCapabilities>,
        },
    }

    let worker_cap = WorkerCapabilities::new("w1", 8, 16_384, false, false, None);
    let msg = ClientResponse::ClusterStatus {
        total_tasks: 3,
        pending_tasks: 2,
        running_tasks: 1,
        completed_tasks: 0,
        failed_tasks: 0,
        workers: vec![worker_cap],
        active_bytes: 601_029,
        max_active_bytes: 1_048_576,
        store_error: None,
    };
    let encoded = serialize_message(&msg, WireCodec::Bincode).unwrap();
    assert_eq!(encoded[0], WIRE_FORMAT_BINCODE);

    // Decode with the legacy bincode schema (which uses `bincode::deserialize_from` on a slice
    // reader, just like `deserialize_message` in `protocol.rs`).
    let mut cursor = std::io::Cursor::new(&encoded[1..]);
    let legacy: LegacyBinaryClientResponse = bincode::deserialize_from(&mut cursor).unwrap();
    match legacy {
        LegacyBinaryClientResponse::ClusterStatus {
            total_tasks,
            pending_tasks,
            running_tasks,
            completed_tasks,
            failed_tasks,
            workers,
        } => {
            assert_eq!(total_tasks, 3);
            assert_eq!(pending_tasks, 2);
            assert_eq!(running_tasks, 1);
            assert_eq!(completed_tasks, 0);
            assert_eq!(failed_tasks, 0);
            assert_eq!(workers.len(), 1);
            assert_eq!(workers[0].name, "w1");
        }
        other => panic!("expected LegacyBinaryClientResponse::ClusterStatus, got {other:?}"),
    }
}

#[test]
fn test_all_supported_does_not_advertise_role_arbitrage() {
    assert!(
        !ProtocolCapabilities::all_supported().contains(ProtocolCapabilities::ROLE_ARBITRAGE),
        "ROLE_ARBITRAGE is design-only and must not be advertised in all_supported()"
    );
}

#[test]
fn test_client_message_and_response_version_skew_refused_in_both_codecs() {
    let req = ClientMessage::ClusterStatus;
    let resp = ClientResponse::TaskSubmitted {
        task_id: TaskId::new(),
    };

    for codec in [WireCodec::Bincode, WireCodec::Json] {
        for bad_ver in [
            1u32,
            2,
            PROTOCOL_VERSION_CURRENT - 1,
            PROTOCOL_VERSION_CURRENT + 1,
            99,
        ] {
            let raw_req = serialize_client_message_with_version(&req, bad_ver, codec).unwrap();
            match deserialize_message::<ClientMessage>(&raw_req) {
                Err(ProtocolError::IncompatibleVersion {
                    peer_version,
                    current_version,
                    min_compatible_version,
                }) => {
                    assert_eq!(peer_version, bad_ver);
                    assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                    assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
                }
                other => {
                    panic!("expected IncompatibleVersion({bad_ver}) for {codec:?}, got {other:?}")
                }
            }

            match deserialize_message::<InboundMessage>(&raw_req) {
                Err(ProtocolError::IncompatibleVersion {
                    peer_version,
                    current_version,
                    min_compatible_version,
                }) => {
                    assert_eq!(peer_version, bad_ver);
                    assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                    assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
                }
                other => panic!(
                    "expected InboundMessage IncompatibleVersion({bad_ver}) for {codec:?}, got {other:?}"
                ),
            }

            let raw_resp = serialize_client_response_with_version(&resp, bad_ver, codec).unwrap();
            match deserialize_message::<ClientResponse>(&raw_resp) {
                Err(ProtocolError::IncompatibleVersion {
                    peer_version,
                    current_version,
                    min_compatible_version,
                }) => {
                    assert_eq!(peer_version, bad_ver);
                    assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                    assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
                }
                other => panic!(
                    "expected ClientResponse IncompatibleVersion({bad_ver}) for {codec:?}, got {other:?}"
                ),
            }

            // ClientResponse::Error is always decoded regardless of protocol_version so a
            // rejected client can display the Master's refusal message.
            let err_resp = ClientResponse::Error {
                message: format_incompatible_version_error("client", bad_ver),
            };
            let raw_err =
                serialize_client_response_with_version(&err_resp, bad_ver, codec).unwrap();
            let (decoded_err, detected) = deserialize_message::<ClientResponse>(&raw_err).unwrap();
            assert_eq!(detected, codec);
            assert_eq!(decoded_err, err_resp);
        }
    }

    // Also verify that an unversioned legacy v2 bincode ClientMessage (4-byte ClusterStatus
    // tag only) is recognized as peer_version = 2 rather than failing with an opaque EOF error.
    let mut legacy_v2_cluster_status = vec![WIRE_FORMAT_BINCODE];
    legacy_v2_cluster_status.extend_from_slice(&3u32.to_le_bytes());
    match deserialize_message::<InboundMessage>(&legacy_v2_cluster_status) {
        Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
            assert_eq!(peer_version, 2);
        }
        other => panic!("expected IncompatibleVersion(2) for legacy v2 frame, got {other:?}"),
    }
}

#[test]
fn test_unversioned_json_client_message_and_response_refused_with_version_2() {
    let raw_json_req = br#"{"type":"ClusterStatus"}"#;
    let mut framed_json_req = vec![WIRE_FORMAT_JSON];
    framed_json_req.extend_from_slice(raw_json_req);

    for frame in [raw_json_req.as_slice(), framed_json_req.as_slice()] {
        match deserialize_message::<ClientMessage>(frame) {
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                assert_eq!(peer_version, LEGACY_UNVERSIONED_PROTOCOL_VERSION);
                assert_eq!(peer_version, 2);
            }
            other => {
                panic!("expected IncompatibleVersion(2) for unversioned JSON ClientMessage, got {other:?}")
            }
        }
        match deserialize_message::<InboundMessage>(frame) {
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                assert_eq!(peer_version, LEGACY_UNVERSIONED_PROTOCOL_VERSION);
                assert_eq!(peer_version, 2);
            }
            other => {
                panic!("expected IncompatibleVersion(2) for unversioned JSON InboundMessage, got {other:?}")
            }
        }
    }

    let raw_json_resp = br#"{"type":"ClusterStatus","total_tasks":0,"pending_tasks":0,"running_tasks":0,"completed_tasks":0,"failed_tasks":0,"workers":[]}"#;
    let mut framed_json_resp = vec![WIRE_FORMAT_JSON];
    framed_json_resp.extend_from_slice(raw_json_resp);

    for frame in [raw_json_resp.as_slice(), framed_json_resp.as_slice()] {
        match deserialize_message::<ClientResponse>(frame) {
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                assert_eq!(peer_version, LEGACY_UNVERSIONED_PROTOCOL_VERSION);
                assert_eq!(peer_version, 2);
            }
            other => {
                panic!("expected IncompatibleVersion(2) for unversioned JSON ClientResponse, got {other:?}")
            }
        }
    }

    // Unversioned JSON Register and RegisterAck also default to protocol_version = 2 and fail
    // `is_protocol_version_compatible`.
    let raw_json_register = br#"{"type":"Register","worker_id":"00000000-0000-0000-0000-000000000001","capabilities":{"name":"w","cpu_cores":2,"ram_mb":1024,"has_gpu":false,"is_simulated_gpu":false,"tags":[]}}"#;
    let (decoded_reg, _) = deserialize_message::<WorkerMessage>(raw_json_register).unwrap();
    match decoded_reg {
        WorkerMessage::Register {
            protocol_version, ..
        } => {
            assert_eq!(protocol_version, LEGACY_UNVERSIONED_PROTOCOL_VERSION);
            assert!(!is_protocol_version_compatible(protocol_version));
        }
        other => panic!("expected WorkerMessage::Register, got {other:?}"),
    }

    let raw_json_ack = br#"{"type":"RegisterAck","accepted":true,"worker_id":"00000000-0000-0000-0000-000000000001","heartbeat_interval_secs":5}"#;
    let (decoded_ack, _) = deserialize_message::<MasterMessage>(raw_json_ack).unwrap();
    match decoded_ack {
        MasterMessage::RegisterAck {
            protocol_version, ..
        } => {
            assert_eq!(protocol_version, LEGACY_UNVERSIONED_PROTOCOL_VERSION);
            assert!(!is_protocol_version_compatible(protocol_version));
        }
        other => panic!("expected MasterMessage::RegisterAck, got {other:?}"),
    }
}

// --- Protocol version 6: Task::workspace, Hello (design 0039 W1) ---------------------------

fn sample_workspace_task() -> Task {
    Task::new(
        TaskSpec::new_command("cargo", vec!["test".into()]),
        crate::task::TaskRequirements::default(),
    )
}

/// The bytes a v9 client sent for `SubmitTask`: without the two `extensions` lists (an empty list
/// is its 8-byte length). `task` must have none.
fn v9_submit_task_frame(task: &Task, version: u32, wait: bool) -> Vec<u8> {
    assert!(task.extensions.is_empty() && task.requirements.extensions.is_empty());
    let msg = ClientMessage::SubmitTask {
        task: task.clone(),
        wait,
    };
    let mut raw = serialize_client_message_with_version(&msg, version, WireCodec::Bincode)
        .unwrap()
        .to_vec();
    // Task::extensions sits just before `wait` (1 byte) and the version trailer (4 bytes).
    let len = raw.len();
    assert_eq!(&raw[len - 13..len - 5], &[0u8; 8], "empty Task::extensions");
    raw.drain(len - 13..len - 5);
    // TaskRequirements::extensions sits after `vram_mb`; with `vram_mb` None that is 1 byte on.
    let vram_tag_offset = requirements_vram_tag_offset(task);
    assert_eq!(raw[vram_tag_offset], 0, "vram_mb None tag byte");
    assert_eq!(
        &raw[vram_tag_offset + 1..vram_tag_offset + 9],
        &[0u8; 8],
        "empty TaskRequirements::extensions"
    );
    raw.drain(vram_tag_offset + 1..vram_tag_offset + 9);
    raw
}

fn requirements_vram_tag_offset(task: &Task) -> usize {
    1 + 4
        + bincode::serialized_size(&(
            task.id,
            &task.spec,
            task.requirements.cpu_cores,
            task.requirements.ram_mb,
            task.requirements.gpu_required,
            task.requirements.timeout_secs,
            task.requirements.max_retries,
        ))
        .unwrap() as usize
}

/// The bytes a pre-v9 client sent for `SubmitTask` without `TaskRequirements::vram_mb` (and
/// without the v10 `extensions` lists).
fn pre_v9_submit_task_frame(task: &Task, version: u32, wait: bool) -> Vec<u8> {
    let mut raw = v9_submit_task_frame(task, version, wait);
    let vram_tag_offset = requirements_vram_tag_offset(task);
    assert_eq!(raw[vram_tag_offset], 0, "vram_mb None tag byte");
    raw.remove(vram_tag_offset);
    raw
}

/// The bytes a v5 client sent for `SubmitTask`: without `TaskRequirements::vram_mb` and without
/// the `workspace` tag byte (`None`), which sits just before `wait` and the version trailer.
fn v5_submit_task_frame(wait: bool) -> Vec<u8> {
    let task = sample_workspace_task();
    let mut v5 = pre_v9_submit_task_frame(&task, 5, wait);
    let len = v5.len();
    assert_eq!(v5[len - 6], 0, "workspace None tag byte");
    v5.remove(len - 6);
    v5
}

#[test]
fn test_protocol_6_hello_round_trips_in_both_codecs_and_is_appended_last() {
    assert_eq!(PROTOCOL_VERSION_CURRENT, 10);
    assert_eq!(PROTOCOL_VERSION_MIN_COMPATIBLE, 10);
    for codec in [WireCodec::Bincode, WireCodec::Json] {
        let raw = serialize_message(&ClientMessage::Hello, codec).unwrap();
        let (msg, detected) = deserialize_message::<ClientMessage>(&raw).unwrap();
        assert_eq!(msg, ClientMessage::Hello);
        assert_eq!(detected, codec);
        let (inbound, _) = deserialize_message::<InboundMessage>(&raw).unwrap();
        assert!(matches!(
            inbound,
            InboundMessage::Client(ClientMessage::Hello)
        ));

        let resp = ClientResponse::Hello {
            min_compatible_version: PROTOCOL_VERSION_MIN_COMPATIBLE,
        };
        let raw = serialize_message(&resp, codec).unwrap();
        let (back, detected) = deserialize_message::<ClientResponse>(&raw).unwrap();
        assert_eq!(back, resp);
        assert_eq!(detected, codec);
    }
    // Bincode variant indices: Hello is the ninth request and the eleventh response, so every
    // earlier index is unchanged.
    let req = serialize_message(&ClientMessage::Hello, WireCodec::Bincode).unwrap();
    assert_eq!(&req[1..5], &8u32.to_le_bytes());
    let resp = serialize_message(
        &ClientResponse::Hello {
            min_compatible_version: 7,
        },
        WireCodec::Bincode,
    )
    .unwrap();
    assert_eq!(&resp[1..5], &10u32.to_le_bytes());
}

#[test]
fn test_protocol_7_list_workspaces_round_trips_in_both_codecs_and_is_appended_last() {
    for codec in [WireCodec::Bincode, WireCodec::Json] {
        let raw = serialize_message(&ClientMessage::ListWorkspaces, codec).unwrap();
        let (msg, detected) = deserialize_message::<ClientMessage>(&raw).unwrap();
        assert_eq!(msg, ClientMessage::ListWorkspaces);
        assert_eq!(detected, codec);
        let (inbound, _) = deserialize_message::<InboundMessage>(&raw).unwrap();
        assert!(matches!(
            inbound,
            InboundMessage::Client(ClientMessage::ListWorkspaces)
        ));

        let resp = ClientResponse::Workspaces {
            workspaces: vec![WorkspaceAffinity {
                name: "oxideswarm-agent.1".into(),
                worker: Uuid::from_u128(1),
                last_used_utc: 1_767_225_600,
            }],
        };
        let raw = serialize_message(&resp, codec).unwrap();
        let (back, detected) = deserialize_message::<ClientResponse>(&raw).unwrap();
        assert_eq!(back, resp);
        assert_eq!(detected, codec);
    }
    let req = serialize_message(&ClientMessage::ListWorkspaces, WireCodec::Bincode).unwrap();
    assert_eq!(&req[1..5], &9u32.to_le_bytes());
    let resp = serialize_message(
        &ClientResponse::Workspaces {
            workspaces: Vec::new(),
        },
        WireCodec::Bincode,
    )
    .unwrap();
    assert_eq!(&resp[1..5], &11u32.to_le_bytes());
}

#[test]
fn test_workspaces_bit_is_reserved_and_not_advertised() {
    assert_eq!(ProtocolCapabilities::WORKSPACES.0, 1 << 10);
    assert!(!ProtocolCapabilities::all_supported().contains(ProtocolCapabilities::WORKSPACES));
}

#[test]
fn test_task_workspace_round_trips_in_both_codecs() {
    use crate::task::{WorkspaceRef, WorkspaceSource};
    let source = WorkspaceSource::Tree {
        manifest: crate::blob::BlobId([7; 32]),
        manifest_size: 99,
    };
    for workspace in [
        None,
        Some(WorkspaceRef::new("agent-1")),
        Some(WorkspaceRef {
            name: "agent-1".into(),
            source: Some(source),
        }),
    ] {
        let mut task = sample_workspace_task();
        task.workspace = workspace;
        let msg = ClientMessage::SubmitTask {
            task: task.clone(),
            wait: true,
        };
        for codec in [WireCodec::Bincode, WireCodec::Json] {
            let raw = serialize_message(&msg, codec).unwrap();
            let (back, _) = deserialize_message::<ClientMessage>(&raw).unwrap();
            assert_eq!(back, msg, "{codec:?}");
        }
    }
}

#[test]
fn test_v5_submit_task_at_a_v6_master_is_refused_as_version_5() {
    for wait in [false, true] {
        let mut frame = v5_submit_task_frame(wait);
        // Not decodable with the v6 layout, so the opaque error would have been a bincode one.
        assert!(bincode_exact_deserialize::<BinaryClientMessage>(&frame[1..]).is_err());
        match deserialize_message::<ClientMessage>(&frame) {
            Err(ProtocolError::IncompatibleVersion {
                peer_version,
                current_version,
                min_compatible_version,
            }) => {
                assert_eq!(peer_version, 5);
                assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
            }
            other => panic!("expected IncompatibleVersion(5), got {other:?}"),
        }
        match deserialize_message::<InboundMessage>(&frame) {
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                assert_eq!(peer_version, 5);
            }
            other => panic!("expected InboundMessage IncompatibleVersion(5), got {other:?}"),
        }
        // A v4 client's frame had the same shape; its own version is reported.
        let len = frame.len();
        frame[len - 4..].copy_from_slice(&4u32.to_le_bytes());
        match deserialize_message::<ClientMessage>(&frame) {
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                assert_eq!(peer_version, 4);
            }
            other => panic!("expected IncompatibleVersion(4), got {other:?}"),
        }
    }
}

#[test]
fn test_garbage_submit_task_is_still_an_opaque_decode_error() {
    let mut frame = v5_submit_task_frame(true);
    frame.truncate(frame.len() - 7);
    assert!(matches!(
        deserialize_message::<ClientMessage>(&frame),
        Err(ProtocolError::Bincode(_))
    ));
}

#[test]
fn test_v5_non_task_requests_at_a_v6_master_are_refused_as_version_5() {
    // These have the v6 layout and are refused by the version check itself.
    for msg in [
        ClientMessage::ClusterStatus,
        ClientMessage::ListWorkers,
        ClientMessage::GetTaskStatus {
            task_id: TaskId::new(),
        },
    ] {
        for codec in [WireCodec::Bincode, WireCodec::Json] {
            let raw = serialize_client_message_with_version(&msg, 5, codec).unwrap();
            match deserialize_message::<InboundMessage>(&raw) {
                Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                    assert_eq!(peer_version, 5, "{msg:?} {codec:?}");
                }
                other => panic!("expected IncompatibleVersion(5), got {other:?}"),
            }
        }
    }
}

#[test]
fn test_v5_json_submit_task_without_workspace_is_refused_as_version_5() {
    let task = serde_json::to_value(sample_workspace_task()).unwrap();
    let mut task = task;
    task.as_object_mut().unwrap().remove("workspace");
    let frame = serde_json::json!({
        "type": "SubmitTask",
        "task": task,
        "wait": true,
        "protocol_version": 5,
    });
    let mut raw = vec![WIRE_FORMAT_JSON];
    raw.extend_from_slice(frame.to_string().as_bytes());
    match deserialize_message::<ClientMessage>(&raw) {
        Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
            assert_eq!(peer_version, 5);
        }
        other => panic!("expected IncompatibleVersion(5), got {other:?}"),
    }
}

#[test]
fn test_v5_responses_at_a_v6_client_are_refused_as_version_5() {
    // A v5 Master answers with the v6 response layout plus a version-5 trailer (Hello was
    // appended, nothing moved), so the client reads the version and refuses it. This includes
    // the ClusterStatus a v5 Master sends back to the old probe.
    let responses = [
        ClientResponse::ClusterStatus {
            total_tasks: 1,
            pending_tasks: 0,
            running_tasks: 0,
            completed_tasks: 1,
            failed_tasks: 0,
            workers: Vec::new(),
            active_bytes: 0,
            max_active_bytes: 1,
            store_error: Some("disk".into()),
        },
        ClientResponse::WorkerList {
            workers: Vec::new(),
        },
        ClientResponse::TaskSubmitted {
            task_id: TaskId::new(),
        },
    ];
    for resp in responses {
        for codec in [WireCodec::Bincode, WireCodec::Json] {
            let raw = serialize_client_response_with_version(&resp, 5, codec).unwrap();
            match deserialize_message::<ClientResponse>(&raw) {
                Err(ProtocolError::IncompatibleVersion {
                    peer_version,
                    current_version,
                    ..
                }) => {
                    assert_eq!(peer_version, 5, "{resp:?} {codec:?}");
                    assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                }
                other => panic!("expected IncompatibleVersion(5), got {other:?}"),
            }
        }
    }
    // A Hello answer stamped v5 is refused as well.
    let hello = ClientResponse::Hello {
        min_compatible_version: 5,
    };
    let raw = serialize_client_response_with_version(&hello, 5, WireCodec::Bincode).unwrap();
    assert!(matches!(
        deserialize_message::<ClientResponse>(&raw),
        Err(ProtocolError::IncompatibleVersion {
            peer_version: 5,
            ..
        })
    ));
}

#[test]
fn test_v6_requests_at_a_v7_master_are_refused_as_version_6() {
    // Every v6 ClientMessage variant (`ListWorkspaces` was appended in v7, nothing moved)
    // decodes with its v6 version trailer and is refused as IncompatibleVersion(6).
    for msg in [
        ClientMessage::Hello,
        ClientMessage::SubmitTask {
            task: sample_workspace_task(),
            wait: true,
        },
        ClientMessage::ClusterStatus,
        ClientMessage::ListWorkers,
        ClientMessage::GetTaskStatus {
            task_id: TaskId::new(),
        },
        ClientMessage::CancelTask {
            task_id: TaskId::new(),
        },
        ClientMessage::GetTaskResult {
            task_id: TaskId::new(),
        },
    ] {
        for codec in [WireCodec::Bincode, WireCodec::Json] {
            let raw = serialize_client_message_with_version(&msg, 6, codec).unwrap();
            match deserialize_message::<ClientMessage>(&raw) {
                Err(ProtocolError::IncompatibleVersion {
                    peer_version,
                    current_version,
                    min_compatible_version,
                }) => {
                    assert_eq!(peer_version, 6, "{msg:?} {codec:?}");
                    assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                    assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
                }
                other => panic!("expected ClientMessage IncompatibleVersion(6), got {other:?}"),
            }
            match deserialize_message::<InboundMessage>(&raw) {
                Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                    assert_eq!(peer_version, 6, "{msg:?} {codec:?}");
                }
                other => {
                    panic!("expected InboundMessage IncompatibleVersion(6), got {other:?}")
                }
            }
        }
    }
    let formatted = format_incompatible_version_error("client", 6);
    assert!(
        formatted.contains("Incompatible protocol version 6")
            && formatted.contains(&format!("{PROTOCOL_VERSION_CURRENT}")),
        "{formatted}"
    );
}

#[test]
fn test_v6_responses_at_a_v7_client_are_refused_as_version_6() {
    // Every v6 ClientResponse variant (`Workspaces` was appended in v7, nothing moved)
    // decodes with its v6 version trailer and is refused as IncompatibleVersion(6).
    let responses = [
        ClientResponse::Hello {
            min_compatible_version: 6,
        },
        ClientResponse::ClusterStatus {
            total_tasks: 1,
            pending_tasks: 0,
            running_tasks: 0,
            completed_tasks: 1,
            failed_tasks: 0,
            workers: Vec::new(),
            active_bytes: 0,
            max_active_bytes: 1,
            store_error: None,
        },
        ClientResponse::WorkerList {
            workers: Vec::new(),
        },
        ClientResponse::TaskSubmitted {
            task_id: TaskId::new(),
        },
    ];
    for resp in responses {
        for codec in [WireCodec::Bincode, WireCodec::Json] {
            let raw = serialize_client_response_with_version(&resp, 6, codec).unwrap();
            match deserialize_message::<ClientResponse>(&raw) {
                Err(ProtocolError::IncompatibleVersion {
                    peer_version,
                    current_version,
                    min_compatible_version,
                }) => {
                    assert_eq!(peer_version, 6, "{resp:?} {codec:?}");
                    assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                    assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
                }
                other => panic!("expected IncompatibleVersion(6), got {other:?}"),
            }
        }
    }
}

#[test]
fn test_result_ack_token_is_blake3_canonical_and_includes_worker_id() {
    let tid = TaskId(Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef));
    let wid1 = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
    let wid2 = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8889);

    let mut base = TaskResult::success(wid1, tid, "out-payload", 42, false);
    base.stderr = crate::task::Bytes::from_static(b"err-payload");
    let token1 = master_features::result_ack_token(&base);
    // Deterministic BLAKE3 LE u64 across builds and platforms:
    assert_eq!(token1, 12_561_239_036_294_283_620_u64);

    // Changing worker_id must change the token (D4 / N2):
    let mut other_worker = base.clone();
    other_worker.worker_id = wid2;
    assert_ne!(master_features::result_ack_token(&other_worker), token1);

    // Changing execution_time_ms, stdout, or outputs must change the token:
    let mut other_time = base.clone();
    other_time.execution_time_ms = 43;
    assert_ne!(master_features::result_ack_token(&other_time), token1);

    let mut other_out = base.clone();
    other_out.stdout = crate::task::Bytes::from_static(b"changed");
    assert_ne!(master_features::result_ack_token(&other_out), token1);
}

#[test]
fn test_protocol_9_task_vram_mb_round_trips_and_refuses_v8_submit() {
    let task = Task::new(
        TaskSpec::gpu_compute("vram_kernel", 64),
        crate::task::TaskRequirements::gpu(60).with_vram_mb(6_144),
    );
    let msg = ClientMessage::SubmitTask { task, wait: true };
    for codec in [WireCodec::Bincode, WireCodec::Json] {
        let raw = serialize_message(&msg, codec).unwrap();
        let (back, detected) = deserialize_message::<ClientMessage>(&raw).unwrap();
        assert_eq!(back, msg, "{codec:?}");
        assert_eq!(detected, codec);
    }

    let base_task = sample_workspace_task();
    for version in [6, 7, 8] {
        let frame = pre_v9_submit_task_frame(&base_task, version, false);
        match deserialize_message::<ClientMessage>(&frame) {
            Err(ProtocolError::IncompatibleVersion {
                peer_version,
                current_version,
                min_compatible_version,
            }) => {
                assert_eq!(peer_version, version);
                assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
            }
            other => panic!("expected IncompatibleVersion({version}), got {other:?}"),
        }
        match deserialize_message::<InboundMessage>(&frame) {
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                assert_eq!(peer_version, version);
            }
            other => {
                panic!("expected InboundMessage IncompatibleVersion({version}), got {other:?}")
            }
        }
    }
}

// --- Protocol version 10: Task::extensions and TaskRequirements::extensions (design 0082) --------

fn sample_extensions_task() -> Task {
    let mut task = sample_workspace_task();
    task.extensions = vec![
        crate::task::Extension {
            id: 0x8001,
            critical: false,
            bytes: vec![0x00, 0xff, 0x10],
        },
        crate::task::Extension {
            id: 0x8002,
            critical: true,
            bytes: vec![],
        },
    ];
    task.requirements.extensions = vec![crate::task::Extension {
        id: 0x8004,
        critical: true,
        bytes: vec![0xab; 5],
    }];
    task
}

#[test]
fn test_protocol_10_task_extensions_round_trip_in_both_codecs() {
    for task in [sample_workspace_task(), sample_extensions_task()] {
        let msgs = [ClientMessage::SubmitTask {
            task: task.clone(),
            wait: true,
        }];
        for msg in msgs {
            for codec in [WireCodec::Bincode, WireCodec::Json] {
                let raw = serialize_message(&msg, codec).unwrap();
                let (back, detected) = deserialize_message::<ClientMessage>(&raw).unwrap();
                assert_eq!(back, msg, "{codec:?}");
                assert_eq!(detected, codec);
            }
        }
        for msg in [
            MasterMessage::AssignTask { task: task.clone() },
            MasterMessage::AssignTaskWithCheckpoint {
                task: task.clone(),
                latest_checkpoint: None,
            },
        ] {
            for codec in [WireCodec::Bincode, WireCodec::Json] {
                let raw = serialize_message(&msg, codec).unwrap();
                let (back, _) = deserialize_message::<MasterMessage>(&raw).unwrap();
                assert_eq!(back, msg, "{codec:?}");
            }
        }
    }
}

#[test]
fn test_extension_json_form_is_hex_and_extensions_may_be_omitted() {
    let ext = crate::task::Extension {
        id: 7,
        critical: false,
        bytes: vec![0x00, 0xff],
    };
    let json = serde_json::to_string(&ext).unwrap();
    assert_eq!(json, r#"{"id":7,"critical":false,"bytes":"00ff"}"#);
    assert_eq!(
        serde_json::from_str::<crate::task::Extension>(&json).unwrap(),
        ext
    );
    assert!(serde_json::from_str::<crate::task::Extension>(
        r#"{"id":7,"critical":false,"bytes":"0g"}"#
    )
    .is_err());

    // A JSON peer may omit both lists.
    let task = sample_workspace_task();
    let mut value = serde_json::to_value(&task).unwrap();
    value.as_object_mut().unwrap().remove("extensions");
    value["requirements"]
        .as_object_mut()
        .unwrap()
        .remove("extensions");
    let back: Task = serde_json::from_value(value).unwrap();
    assert!(back.extensions.is_empty() && back.requirements.extensions.is_empty());
}

/// A v9 client's `SubmitTask` has no `extensions` lists: a v10 Master refuses it with the
/// version error (design 0082 D6), through both the typed and the routed decoder.
#[test]
fn legacy_v9_submit_task_is_refused_with_a_version_error() {
    let task = sample_workspace_task();
    for wait in [false, true] {
        let frame = v9_submit_task_frame(&task, 9, wait);
        assert!(bincode_exact_deserialize::<BinaryClientMessage>(&frame[1..]).is_err());
        match deserialize_message::<ClientMessage>(&frame) {
            Err(ProtocolError::IncompatibleVersion {
                peer_version,
                current_version,
                min_compatible_version,
            }) => {
                assert_eq!(peer_version, 9);
                assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
            }
            other => panic!("expected IncompatibleVersion(9), got {other:?}"),
        }
        match deserialize_message::<InboundMessage>(&frame) {
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                assert_eq!(peer_version, 9);
            }
            other => panic!("expected InboundMessage IncompatibleVersion(9), got {other:?}"),
        }
    }
}
