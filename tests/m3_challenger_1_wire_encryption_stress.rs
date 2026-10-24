//! Adversarial Stress & Verification Test Suite for Milestone 3 (Requirement R3):
//! Wire Stream Encryption, AEAD Record Layer, Anti-Replay & Protocol Capability/Version Negotiation.
//!
//! Authored by Challenger 1 to empirically challenge and stress-test:
//! 1. Ciphertext tampering: single-bit flips across ciphertext payload, Poly1305 tag corruption,
//!    truncated frames, and AAD mutation — asserting immediate connection termination and AEAD failure.
//! 2. Anti-replay enforcement: resending identical valid encrypted frames, stale sequence numbers,
//!    out-of-order sequence skips, and u64::MAX counter boundary overflows.
//! 3. Unauthenticated loopback fallback: verifying client and real worker connections to masters
//!    without secrets operate with zero encryption overhead.
//! 4. Forward compatibility: verifying unknown message tags within valid ranges (8..10_000 for Worker,
//!    7..10_000 for Master) deserialize to UnknownVariant without dropping connection, while out-of-range
//!    corrupted tags are strictly rejected with Bincode errors.
//! 5. Protocol version negotiation: verifying version mismatches (e.g. version < min_compatible)
//!    produce clean RegisterAck rejections and connection drops without panics.

use std::time::Duration;

use tokio::net::TcpStream;
use tokio_util::bytes::Bytes;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::cluster_auth::{client_handshake, ClusterSecret, WIRE_FORMAT_AUTH};
use rusty_grid_core::protocol::{
    derive_aead_keys, deserialize_message, serialize_message, AeadReaderSession, AeadWriterSession,
    ClientMessage, ClientResponse, EndpointRole, InboundMessage, MasterMessage, MessageTransport,
    ProtocolCapabilities, ProtocolError, WireCodec, WorkerMessage, PROTOCOL_VERSION_CURRENT,
    WIRE_FORMAT_BINCODE, WIRE_FORMAT_ENCRYPTED, WIRE_FORMAT_JSON,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_core::GridError;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;
use rusty_grid_worker::WorkerConfig;

fn test_secret(seed: u8) -> ClusterSecret {
    ClusterSecret::from_bytes(&[seed; 32]).unwrap()
}

async fn spawn_test_master(secret: Option<&ClusterSecret>) -> MasterHandle {
    let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_handshake_timeout(3);
    if let Some(s) = secret {
        config = config.with_cluster_secret(s.clone());
    }
    MasterServer::spawn(config)
        .await
        .expect("spawn test master")
}

fn test_register_msg(worker_id: Uuid, name: &str) -> WorkerMessage {
    WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities::new(name, 2, 4096, false, false, None),
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: ProtocolCapabilities::all_supported(),
    }
}

// =============================================================================================
// Part 1: Ciphertext Tampering & Frame Corruption Stress Tests
// =============================================================================================

#[tokio::test]
async fn test_adversarial_single_bit_flip_ciphertext() {
    let session_key = [0x42u8; 32];
    let (mut writer, _reader) = derive_aead_keys(&session_key, EndpointRole::Client);

    let plaintext = b"OxideSwarm-adversarial-wire-encryption-stress-payload-1234567890";
    let encrypted_frame = writer.encrypt_frame(plaintext).expect("seal frame");

    assert_eq!(encrypted_frame[0], WIRE_FORMAT_ENCRYPTED);
    let frame_len = encrypted_frame.len();
    assert!(
        frame_len >= 25,
        "frame must contain header, ciphertext, and tag"
    );

    // Ciphertext range: header is 9 bytes (1 byte format + 8 bytes seq), tag is last 16 bytes.
    let ciphertext_start = 9;
    let ciphertext_end = frame_len - 16;
    assert!(ciphertext_end > ciphertext_start);

    // Test flipping single bits across multiple offsets within ciphertext
    let test_offsets = [
        ciphertext_start,
        ciphertext_start + 1,
        (ciphertext_start + ciphertext_end) / 2,
        ciphertext_end - 2,
        ciphertext_end - 1,
    ];

    for &offset in &test_offsets {
        for bit in [0, 1, 3, 5, 7] {
            let mut tampered = encrypted_frame.to_vec();
            tampered[offset] ^= 1 << bit;

            // Direct AEAD Reader check: MUST fail integrity verification
            let mut fresh_reader = AeadReaderSession::new(&session_key);
            let decrypt_result = fresh_reader.decrypt_frame(&tampered);
            assert!(
                decrypt_result.is_err(),
                "Decryption must fail for bitflip at offset {offset}, bit {bit}"
            );
            match decrypt_result.unwrap_err() {
                ProtocolError::Violation(msg) => {
                    assert!(
                        msg.contains("integrity") || msg.contains("failed"),
                        "Expected integrity violation message, got: {msg}"
                    );
                }
                other => panic!("Expected ProtocolError::Violation, got: {other:?}"),
            }
        }
    }

    // Live TCP connection test: tampered ciphertext frame causes master to immediately drop connection
    let secret = test_secret(101);
    let master = spawn_test_master(Some(&secret)).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let client_key = client_handshake(&mut transport, &secret)
        .await
        .expect("handshake succeeds");
    let (mut client_writer, _) = derive_aead_keys(&client_key, EndpointRole::Client);

    let reg = test_register_msg(Uuid::new_v4(), "bitflip-worker");
    let payload = serialize_message(&reg, WireCodec::Bincode).unwrap();
    let mut tampered_frame = client_writer.encrypt_frame(&payload).unwrap().to_vec();

    // Flip single bit in ciphertext payload
    tampered_frame[12] ^= 0x01;
    transport
        .send_raw_frame(Bytes::from(tampered_frame))
        .await
        .unwrap();

    // Master MUST reject and close connection immediately
    let resp: Result<Option<MasterMessage>, _> = transport.recv_msg().await;
    match resp {
        Ok(None) => {} // Clean connection drop
        Err(_) => {}   // Socket closed / reset
        Ok(Some(msg)) => panic!("Master must not accept tampered ciphertext frame! Got: {msg:?}"),
    }

    // Verify worker was NOT registered
    let workers = master.list_workers().await.unwrap();
    assert!(
        workers.is_empty(),
        "Worker must not be registered after tampered frame"
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_adversarial_corrupt_poly1305_tag() {
    let session_key = [0x55u8; 32];
    let (mut writer, _) = derive_aead_keys(&session_key, EndpointRole::Client);

    let plaintext = b"Poly1305 authentication tag adversarial stress challenge";
    let encrypted_frame = writer.encrypt_frame(plaintext).expect("seal frame");
    let frame_len = encrypted_frame.len();

    // The Poly1305 tag occupies the last 16 bytes
    let tag_start = frame_len - 16;

    // Test vectors for tag corruption:
    // 1. Bitflip at beginning of tag
    let mut tag_corrupt_1 = encrypted_frame.to_vec();
    tag_corrupt_1[tag_start] ^= 0x01;

    // 2. Bitflip in middle of tag
    let mut tag_corrupt_2 = encrypted_frame.to_vec();
    tag_corrupt_2[tag_start + 8] ^= 0x80;

    // 3. Bitflip at end of tag
    let mut tag_corrupt_3 = encrypted_frame.to_vec();
    tag_corrupt_3[frame_len - 1] ^= 0x02;

    // 4. Zeroed tag
    let mut tag_corrupt_zero = encrypted_frame.to_vec();
    tag_corrupt_zero[tag_start..].fill(0);

    // 5. Inverted tag
    let mut tag_corrupt_inv = encrypted_frame.to_vec();
    for b in &mut tag_corrupt_inv[tag_start..] {
        *b ^= 0xFF;
    }

    let corrupt_tags = [
        ("tag_start_bitflip", tag_corrupt_1),
        ("tag_mid_bitflip", tag_corrupt_2),
        ("tag_end_bitflip", tag_corrupt_3),
        ("tag_zeroed", tag_corrupt_zero),
        ("tag_inverted", tag_corrupt_inv),
    ];

    for (name, tampered) in corrupt_tags {
        let mut reader = AeadReaderSession::new(&session_key);
        let res = reader.decrypt_frame(&tampered);
        assert!(res.is_err(), "{name} must fail decryption");
        match res.unwrap_err() {
            ProtocolError::Violation(msg) => {
                assert!(
                    msg.contains("integrity"),
                    "{name} expected integrity error, got {msg}"
                );
            }
            other => panic!("{name} expected Violation, got {other:?}"),
        }
    }

    // Live TCP test: corrupt tag causes master to terminate connection immediately
    let secret = test_secret(102);
    let master = spawn_test_master(Some(&secret)).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let client_key = client_handshake(&mut transport, &secret).await.unwrap();
    let (mut client_writer, _) = derive_aead_keys(&client_key, EndpointRole::Client);

    let reg = test_register_msg(Uuid::new_v4(), "corrupt-tag-worker");
    let payload = serialize_message(&reg, WireCodec::Bincode).unwrap();
    let mut frame = client_writer.encrypt_frame(&payload).unwrap().to_vec();

    // Corrupt tag
    let last_idx = frame.len() - 1;
    frame[last_idx] ^= 0xFF;

    transport.send_raw_frame(Bytes::from(frame)).await.unwrap();

    let resp: Result<Option<MasterMessage>, _> = transport.recv_msg().await;
    match resp {
        Ok(None) | Err(_) => {}
        Ok(Some(msg)) => panic!("Master accepted corrupt Poly1305 tag! {msg:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_adversarial_truncated_frames_rejection() {
    let session_key = [0x77u8; 32];
    let (mut writer, _) = derive_aead_keys(&session_key, EndpointRole::Client);

    let plaintext = b"Truncation attack stress test";
    let valid_frame = writer.encrypt_frame(plaintext).expect("seal frame");

    // Various truncated lengths:
    let truncations: &[(&str, &[u8])] = &[
        ("empty", &[]),
        ("discriminator_only", &[WIRE_FORMAT_ENCRYPTED]),
        ("header_partial_4b", &[WIRE_FORMAT_ENCRYPTED, 0, 0, 0, 0]),
        (
            "header_partial_8b",
            &[WIRE_FORMAT_ENCRYPTED, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            "header_only_9b",
            &[WIRE_FORMAT_ENCRYPTED, 0, 0, 0, 0, 0, 0, 0, 0],
        ),
        ("short_16b", &valid_frame[..16]),
        ("short_24b", &valid_frame[..24]), // exactly 1 byte less than MIN_ENCRYPTED_LEN (25)
        ("minus_1_byte", &valid_frame[..valid_frame.len() - 1]),
        ("minus_8_bytes", &valid_frame[..valid_frame.len() - 8]),
        ("tag_stripped_16b", &valid_frame[..valid_frame.len() - 16]),
    ];

    for (label, slice) in truncations {
        let mut reader = AeadReaderSession::new(&session_key);
        let res = reader.decrypt_frame(slice);
        assert!(res.is_err(), "Truncated frame '{label}' must be rejected");
        match res.unwrap_err() {
            ProtocolError::Violation(msg) => {
                assert!(
                    msg.contains("too short")
                        || msg.contains("integrity")
                        || msg.contains("Expected encrypted"),
                    "Unexpected message for '{label}': {msg}"
                );
            }
            other => panic!("Expected Violation for '{label}', got {other:?}"),
        }
    }

    // Live TCP test: sending truncated frame causes master to disconnect immediately
    let secret = test_secret(103);
    let master = spawn_test_master(Some(&secret)).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let _ = client_handshake(&mut transport, &secret).await.unwrap();

    // Send truncated 12-byte encrypted frame
    let truncated = vec![
        WIRE_FORMAT_ENCRYPTED,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0xAA,
        0xBB,
        0xCC,
    ];
    transport
        .send_raw_frame(Bytes::from(truncated))
        .await
        .unwrap();

    let resp: Result<Option<MasterMessage>, _> = transport.recv_msg().await;
    match resp {
        Ok(None) | Err(_) => {}
        Ok(Some(msg)) => panic!("Master accepted truncated frame! {msg:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_adversarial_aad_mutation_tampering() {
    let session_key = [0x99u8; 32];
    let (mut writer, _) = derive_aead_keys(&session_key, EndpointRole::Client);

    let plaintext = b"AAD binding verification";
    let frame = writer.encrypt_frame(plaintext).unwrap().to_vec();

    // Mutating format tag in AAD (byte 0)
    let mut bad_format = frame.clone();
    bad_format[0] = WIRE_FORMAT_BINCODE;
    let mut r1 = AeadReaderSession::new(&session_key);
    let err1 = r1.decrypt_frame(&bad_format).unwrap_err();
    assert!(matches!(err1, ProtocolError::Violation(_)));

    // Mutating sequence number in AAD (bytes 1..9)
    let mut bad_seq = frame.clone();
    bad_seq[8] ^= 0x01;
    let mut r2 = AeadReaderSession::new(&session_key);
    let err2 = r2.decrypt_frame(&bad_seq).unwrap_err();
    // Replay check fails because expected_seq is 0, but frame carries seq 1
    match err2 {
        ProtocolError::Violation(msg) => {
            assert!(
                msg.contains("Anti-replay"),
                "Expected anti-replay violation, got: {msg}"
            );
        }
        other => panic!("Expected Violation, got: {other:?}"),
    }
}

// =============================================================================================
// Part 2: Anti-Replay & Sequence Enforcement Stress Tests
// =============================================================================================

#[tokio::test]
async fn test_adversarial_replay_attack_exact_duplicate_frame() {
    let secret = test_secret(104);
    let master = spawn_test_master(Some(&secret)).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let session_key = client_handshake(&mut transport, &secret).await.unwrap();
    let (mut writer, mut reader) = derive_aead_keys(&session_key, EndpointRole::Client);

    let worker_id = Uuid::new_v4();
    let reg_msg = test_register_msg(worker_id, "replay-tester");
    let payload = serialize_message(&reg_msg, WireCodec::Bincode).unwrap();

    // Frame 0
    let frame_seq0 = writer.encrypt_frame(&payload).unwrap();
    transport.send_raw_frame(frame_seq0.clone()).await.unwrap();

    // Read RegisterAck
    let ack_raw = transport.recv_raw_frame().await.unwrap().expect("ack");
    assert_eq!(ack_raw[0], WIRE_FORMAT_ENCRYPTED);
    let ack_plain = reader.decrypt_frame(&ack_raw).unwrap();
    let (ack, _): (MasterMessage, _) = deserialize_message(&ack_plain).unwrap();
    assert!(matches!(
        ack,
        MasterMessage::RegisterAck { accepted: true, .. }
    ));

    // Replay Frame 0!
    transport.send_raw_frame(frame_seq0).await.unwrap();

    // Master MUST reject and drop connection due to anti-replay failure
    let resp: Result<Option<MasterMessage>, _> = transport.recv_msg().await;
    match resp {
        Ok(None) | Err(_) => {}
        Ok(Some(msg)) => panic!("Master accepted replayed frame 0! Got: {msg:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_adversarial_replay_stale_sequence_number_burst() {
    let secret = test_secret(105);
    let master = spawn_test_master(Some(&secret)).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let session_key = client_handshake(&mut transport, &secret).await.unwrap();
    transport.enable_encryption(&session_key, EndpointRole::Client);

    let worker_id = Uuid::new_v4();
    transport
        .send_msg(&test_register_msg(worker_id, "burst-seq-worker"))
        .await
        .unwrap();

    let ack: MasterMessage = transport.recv_msg().await.unwrap().expect("ack");
    assert!(matches!(
        ack,
        MasterMessage::RegisterAck { accepted: true, .. }
    ));

    // Send valid heartbeat at seq 1
    let hb1 = WorkerMessage::Heartbeat {
        worker_id,
        timestamp: 1000,
        active_tasks: 0,
        cpu_usage_pct: 5.0,
        ram_available_mb: 2048,
    };
    transport.send_msg(&hb1).await.unwrap();
    let _hb_ack1: MasterMessage = transport.recv_msg().await.unwrap().expect("hb ack 1");

    // Send valid heartbeat at seq 2
    let hb2 = WorkerMessage::Heartbeat {
        worker_id,
        timestamp: 2000,
        active_tasks: 0,
        cpu_usage_pct: 10.0,
        ram_available_mb: 2048,
    };
    transport.send_msg(&hb2).await.unwrap();
    let _hb_ack2: MasterMessage = transport.recv_msg().await.unwrap().expect("hb ack 2");

    // Now forge a frame using stale seq 1
    let (mut stale_writer, _) = derive_aead_keys(&session_key, EndpointRole::Client);
    // stale_writer starts at seq 0. Encrypt twice to reach seq 1.
    let _dummy = stale_writer.encrypt_frame(b"dummy").unwrap(); // seq 0
    let stale_payload = serialize_message(&hb2, WireCodec::Bincode).unwrap();
    let stale_frame_seq1 = stale_writer.encrypt_frame(&stale_payload).unwrap(); // seq 1

    // Master expects seq 3. We send seq 1.
    transport.send_raw_frame(stale_frame_seq1).await.unwrap();

    let resp: Result<Option<MasterMessage>, _> = transport.recv_msg().await;
    match resp {
        Ok(None) | Err(_) => {}
        Ok(Some(msg)) => panic!("Master accepted stale sequence number! Got: {msg:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_adversarial_out_of_order_sequence_skip() {
    let secret = test_secret(106);
    let master = spawn_test_master(Some(&secret)).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let session_key = client_handshake(&mut transport, &secret).await.unwrap();
    let (mut writer, mut reader) = derive_aead_keys(&session_key, EndpointRole::Client);

    let worker_id = Uuid::new_v4();
    let reg_msg = test_register_msg(worker_id, "skip-seq-worker");
    let payload = serialize_message(&reg_msg, WireCodec::Bincode).unwrap();

    // Send seq 0
    let frame_seq0 = writer.encrypt_frame(&payload).unwrap();
    transport.send_raw_frame(frame_seq0).await.unwrap();

    let ack_raw = transport.recv_raw_frame().await.unwrap().expect("ack");
    let ack_plain = reader.decrypt_frame(&ack_raw).unwrap();
    let (ack, _): (MasterMessage, _) = deserialize_message(&ack_plain).unwrap();
    assert!(matches!(
        ack,
        MasterMessage::RegisterAck { accepted: true, .. }
    ));

    // Burn seq 1 locally so writer now produces seq 2!
    let _burned_frame = writer.encrypt_frame(b"burned").unwrap(); // seq 1
    assert_eq!(writer.current_seq(), 2);

    // Send frame with seq 2 directly (skipping seq 1 on the wire)
    let hb = WorkerMessage::Heartbeat {
        worker_id,
        timestamp: 1000,
        active_tasks: 0,
        cpu_usage_pct: 0.0,
        ram_available_mb: 4096,
    };
    let hb_payload = serialize_message(&hb, WireCodec::Bincode).unwrap();
    let frame_seq2 = writer.encrypt_frame(&hb_payload).unwrap();

    transport.send_raw_frame(frame_seq2).await.unwrap();

    // Master expects seq 1, gets seq 2 -> anti-replay check MUST drop connection
    let resp: Result<Option<MasterMessage>, _> = transport.recv_msg().await;
    match resp {
        Ok(None) | Err(_) => {}
        Ok(Some(msg)) => {
            panic!("Master accepted out-of-order skipped sequence number! Got: {msg:?}")
        }
    }

    let _ = master.shutdown();
}

#[test]
fn test_adversarial_sequence_counter_u64_max_overflow() {
    let key = [0x11u8; 32];
    let writer = AeadWriterSession::new(&key);

    assert_eq!(writer.current_seq(), 0);

    // Verify AeadReaderSession anti-replay boundary checks
    let mut reader = AeadReaderSession::new(&key);
    assert_eq!(reader.expected_seq(), 0);

    // If frame sequence doesn't match expected_seq 0, it is immediately rejected
    let mut fake_frame = vec![WIRE_FORMAT_ENCRYPTED];
    fake_frame.extend_from_slice(&u64::MAX.to_be_bytes()); // seq = u64::MAX
    fake_frame.extend_from_slice(&[0u8; 16]); // dummy tag
    let err = reader.decrypt_frame(&fake_frame).unwrap_err();
    match err {
        ProtocolError::Violation(msg) => {
            assert!(
                msg.contains("Anti-replay"),
                "Expected anti-replay violation, got: {msg}"
            );
        }
        other => panic!("Expected Violation, got: {other:?}"),
    }
}

// =============================================================================================
// Part 3: Unauthenticated Loopback Fallback Stress Tests
// =============================================================================================

#[tokio::test]
async fn test_adversarial_unauthenticated_loopback_fallback() {
    let master = spawn_test_master(None).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    assert!(!transport.is_encrypted());

    // Send ClusterStatus request without cluster secret
    transport
        .send_msg(&ClientMessage::ClusterStatus)
        .await
        .unwrap();

    // Read raw frame from wire to confirm wire format
    let raw_frame = transport.recv_raw_frame().await.unwrap().expect("response");
    assert_ne!(
        raw_frame[0], WIRE_FORMAT_ENCRYPTED,
        "Unauthenticated loopback connection must NOT use encrypted framing (0x04)"
    );
    assert!(
        raw_frame[0] == WIRE_FORMAT_BINCODE || raw_frame[0] == WIRE_FORMAT_JSON,
        "Unauthenticated loopback must use bincode (0x02) or json (0x01), got 0x{:02x}",
        raw_frame[0]
    );

    let (resp, _): (ClientResponse, _) = deserialize_message(&raw_frame).unwrap();
    assert!(matches!(resp, ClientResponse::ClusterStatus { .. }));

    // Send 5 successive requests; ensure transport remains unencrypted with 0 crypto overhead
    for _ in 0..5 {
        transport
            .send_msg(&ClientMessage::ClusterStatus)
            .await
            .unwrap();
        let resp: ClientResponse = transport.recv_msg().await.unwrap().expect("response");
        assert!(matches!(resp, ClientResponse::ClusterStatus { .. }));
        assert!(!transport.is_encrypted());
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_adversarial_unauthenticated_worker_lifecycle_loopback() {
    let master = spawn_test_master(None).await;
    let master_addr = master.server_addr();

    let mut worker_config = WorkerConfig::new(master_addr.to_string())
        .with_handshake_timeout(Duration::from_secs(3))
        .with_redirection_portal(false);
    worker_config.default_heartbeat_interval = Duration::from_millis(100);
    assert!(worker_config.cluster_secret.is_none());

    let mut worker = WorkerClient::new(worker_config);
    let worker_id = worker.worker_id();

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });

    // Wait until worker is registered
    let mut registered = false;
    for _ in 0..40 {
        let workers = master.list_workers().await.unwrap();
        if workers.iter().any(|w| w.worker_id == worker_id) {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        registered,
        "Worker must register on unauthenticated loopback master"
    );

    // Submit task
    let task = Task::new(
        TaskSpec::new_shell_script("echo loopback_verified"),
        TaskRequirements::generic(1, 10),
    );
    let task_id = task.id;
    master.submit_task(task).await.unwrap();

    // Await completion
    let mut completed = false;
    for _ in 0..60 {
        let status = master.get_task_status(task_id).await.unwrap();
        if matches!(status, TaskStatus::Completed) {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        completed,
        "Task must execute to completion on unauthenticated loopback"
    );

    worker_handle.abort();
    let _ = master.shutdown();
}

// =============================================================================================
// Part 4: Forward Compatibility & UnknownVariant Stress Tests
// =============================================================================================

#[test]
fn test_adversarial_forward_compatible_worker_message_unknown_variants() {
    // In WorkerMessage, variant tags in 8..10_000 deserialize to UnknownVariant
    let test_tags = [8u32, 9, 42, 100, 1337, 9999];

    for tag in test_tags {
        let mut raw = Vec::new();
        raw.push(WIRE_FORMAT_BINCODE);
        raw.extend_from_slice(&tag.to_le_bytes());
        raw.extend_from_slice(b"arbitrary-future-extension-payload-bytes");

        let (msg, codec): (WorkerMessage, _) =
            deserialize_message(&raw).expect("unknown variant must deserialize cleanly");
        assert_eq!(codec, WireCodec::Bincode);

        match msg {
            WorkerMessage::UnknownVariant {
                tag: decoded_tag,
                ref raw_bytes,
            } => {
                assert_eq!(decoded_tag, tag);
                assert_eq!(raw_bytes, b"arbitrary-future-extension-payload-bytes");
                assert_eq!(msg.variant_name(), "UnknownVariant");
                assert_eq!(msg.worker_id(), Uuid::nil());
            }
            other => panic!("Expected UnknownVariant, got: {other:?}"),
        }

        // Also test InboundMessage fallback
        let (inbound, _): (InboundMessage, _) =
            deserialize_message(&raw).expect("InboundMessage unknown variant");
        match inbound {
            InboundMessage::Worker(WorkerMessage::UnknownVariant {
                tag: decoded_tag,
                ref raw_bytes,
            }) => {
                assert_eq!(decoded_tag, tag);
                assert_eq!(raw_bytes, b"arbitrary-future-extension-payload-bytes");
            }
            other => panic!("Expected InboundMessage::Worker(UnknownVariant), got: {other:?}"),
        }
    }
}

#[tokio::test]
async fn test_adversarial_forward_compatible_worker_message_no_connection_drop() {
    let master = spawn_test_master(None).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let worker_id = Uuid::new_v4();
    transport
        .send_msg(&test_register_msg(worker_id, "forward-compat-worker"))
        .await
        .unwrap();

    let ack: MasterMessage = transport.recv_msg().await.unwrap().expect("ack");
    assert!(matches!(
        ack,
        MasterMessage::RegisterAck { accepted: true, .. }
    ));

    // Now send a forward-compatible unknown WorkerMessage variant (tag = 42)
    let mut unknown_frame = Vec::new();
    unknown_frame.push(WIRE_FORMAT_BINCODE);
    unknown_frame.extend_from_slice(&42u32.to_le_bytes());
    unknown_frame.extend_from_slice(b"future-worker-feature-payload");

    transport
        .send_raw_frame(Bytes::from(unknown_frame))
        .await
        .unwrap();

    // Master must NOT drop the connection!
    // Verify connection is alive by sending a standard Heartbeat immediately after
    let hb = WorkerMessage::Heartbeat {
        worker_id,
        timestamp: 5555,
        active_tasks: 0,
        cpu_usage_pct: 1.0,
        ram_available_mb: 8192,
    };
    transport.send_msg(&hb).await.unwrap();

    // Master answers with HeartbeatAck
    let hb_ack: MasterMessage = transport.recv_msg().await.unwrap().expect("hb ack");
    match hb_ack {
        MasterMessage::HeartbeatAck { timestamp } => {
            assert_eq!(timestamp, 5555);
        }
        other => panic!("Expected HeartbeatAck, got: {other:?}"),
    }

    let _ = master.shutdown();
}

#[test]
fn test_adversarial_forward_compatible_master_message_unknown_variants() {
    // In MasterMessage, variant tags in 7..10_000 deserialize to UnknownVariant
    let test_tags = [7u32, 8, 42, 500, 1337, 9999];

    for tag in test_tags {
        let mut raw = Vec::new();
        raw.push(WIRE_FORMAT_BINCODE);
        raw.extend_from_slice(&tag.to_le_bytes());
        raw.extend_from_slice(b"future-master-command-payload");

        let (msg, codec): (MasterMessage, _) =
            deserialize_message(&raw).expect("unknown master variant must deserialize cleanly");
        assert_eq!(codec, WireCodec::Bincode);

        match msg {
            MasterMessage::UnknownVariant {
                tag: decoded_tag,
                ref raw_bytes,
            } => {
                assert_eq!(decoded_tag, tag);
                assert_eq!(raw_bytes, b"future-master-command-payload");
                assert_eq!(msg.variant_name(), "UnknownVariant");
            }
            other => panic!("Expected UnknownVariant, got: {other:?}"),
        }
    }
}

#[test]
fn test_adversarial_out_of_range_tags_and_corrupted_payloads_rejected() {
    // Tags outside 8..10_000 for WorkerMessage must return Bincode error, NOT UnknownVariant!
    let out_of_range_tags = [10_000u32, 10_001, 65_535, 1_000_000, u32::MAX];

    for tag in out_of_range_tags {
        let mut raw = Vec::new();
        raw.push(WIRE_FORMAT_BINCODE);
        raw.extend_from_slice(&tag.to_le_bytes());
        raw.extend_from_slice(b"payload-for-out-of-range-tag");

        let res: Result<(WorkerMessage, _), _> = deserialize_message(&raw);
        assert!(
            res.is_err(),
            "Tag {tag} is out of range and must be rejected"
        );
        match res.unwrap_err() {
            ProtocolError::Bincode(_) => {}
            other => panic!("Expected Bincode error for tag {tag}, got {other:?}"),
        }
    }

    // Completely random corrupted bytes
    let corrupted = [WIRE_FORMAT_BINCODE, 0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA];
    let res: Result<(WorkerMessage, _), _> = deserialize_message(&corrupted);
    assert!(res.is_err(), "Corrupted payload must return error");
}

// =============================================================================================
// Part 5: Protocol Version Mismatch Stress Tests
// =============================================================================================

#[tokio::test]
async fn test_adversarial_protocol_version_mismatch_worker_registration() {
    let master = spawn_test_master(None).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let worker_id = Uuid::new_v4();
    // Incompatible version: 0 (less than PROTOCOL_VERSION_MIN_COMPATIBLE = 1)
    let bad_version_reg = WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities::new("old-protocol-worker", 2, 4096, false, false, None),
        protocol_version: 0,
        features: ProtocolCapabilities::empty(),
    };

    transport.send_msg(&bad_version_reg).await.unwrap();

    // Master MUST respond with RegisterAck { accepted: false } and descriptive message
    let resp: MasterMessage = transport
        .recv_msg()
        .await
        .unwrap()
        .expect("RegisterAck response");

    match resp {
        MasterMessage::RegisterAck {
            accepted,
            worker_id: ack_wid,
            message,
            protocol_version,
            ..
        } => {
            assert!(!accepted, "Registration must be rejected for version 0");
            assert_eq!(ack_wid, worker_id);
            assert_eq!(protocol_version, PROTOCOL_VERSION_CURRENT);
            let reason = message.expect("rejection reason");
            assert!(
                reason.contains("Incompatible protocol version 0"),
                "Expected version rejection reason, got: {reason}"
            );
        }
        other => panic!("Expected RegisterAck, got: {other:?}"),
    }

    // Connection must be terminated by master after rejection
    let next: Option<MasterMessage> = transport.recv_msg().await.unwrap_or(None);
    assert!(next.is_none(), "Connection must be closed after rejection");

    // Worker registry must be empty
    let workers = master.list_workers().await.unwrap();
    assert!(workers.is_empty(), "Rejected worker must not be registered");

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_adversarial_protocol_version_mismatch_master_ack_rejection() {
    // When a Master returns RegisterAck with an incompatible protocol version (< MIN_COMPATIBLE),
    // the WorkerClient must reject the handshake with GridError::HandshakeFailed
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut transport = MessageTransport::new(socket);

        // Receive Register
        let msg: WorkerMessage = transport.recv_msg().await.unwrap().expect("register");
        let wid = msg.worker_id();

        // Respond with RegisterAck carrying incompatible protocol version 0
        let bad_ack = MasterMessage::RegisterAck {
            accepted: true,
            worker_id: wid,
            heartbeat_interval_secs: 10,
            message: None,
            protocol_version: 0, // Incompatible!
            features: ProtocolCapabilities::empty(),
        };
        transport.send_msg(&bad_ack).await.unwrap();
    });

    let config = WorkerConfig::new(server_addr.to_string())
        .with_handshake_timeout(Duration::from_secs(2))
        .with_redirection_portal(false);
    let worker = WorkerClient::new(config);

    let stream = TcpStream::connect(server_addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);
    let run_res = worker.perform_handshake(&mut transport).await;
    assert!(
        run_res.is_err(),
        "Worker must fail handshake when master sends version 0"
    );
    match run_res.unwrap_err() {
        GridError::HandshakeFailed(msg) => {
            assert!(
                msg.contains("Incompatible Master protocol version: 0"),
                "Expected incompatible version error, got: {msg}"
            );
        }
        other => panic!("Expected HandshakeFailed, got: {other:?}"),
    }

    let _ = server_task.await;
}

#[tokio::test]
async fn test_adversarial_secret_configuration_asymmetry_rejection() {
    // Case A: Master has secret, client connects with NO secret
    let secret = test_secret(107);
    let master = spawn_test_master(Some(&secret)).await;

    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    // Client sends Register without auth
    let reg = test_register_msg(Uuid::new_v4(), "unauth-intruder");
    let _ = transport.send_msg(&reg).await;

    let res: Result<Option<MasterMessage>, _> = transport.recv_msg().await;
    match res {
        Err(ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH)) => {
            // Master replied with REJECT frame
            let grid_err: GridError = ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH).into();
            assert!(matches!(grid_err, GridError::AuthRequired(_)));
        }
        Ok(None) | Err(_) => {}
        Ok(Some(msg)) => panic!("Unauthenticated client received valid MasterMessage: {msg:?}"),
    }

    // Case B: Master has NO secret, client attempts cluster auth handshake
    let open_master = spawn_test_master(None).await;
    let stream2 = TcpStream::connect(open_master.server_addr()).await.unwrap();
    let mut transport2 = MessageTransport::new(stream2);

    let auth_res = client_handshake(&mut transport2, &secret).await;
    assert!(
        auth_res.is_err(),
        "Auth handshake against open master must fail"
    );
    match auth_res.unwrap_err() {
        GridError::AuthFailed(msg) => {
            assert!(
                msg.contains("no cluster secret")
                    || msg.contains("no secret configured")
                    || msg.contains("rejected")
            );
        }
        other => panic!("Expected AuthFailed, got: {other:?}"),
    }

    // Case C: Master has secret A, client uses wrong secret B
    let stream3 = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport3 = MessageTransport::new(stream3);
    let wrong_secret = test_secret(108);

    let wrong_auth = client_handshake(&mut transport3, &wrong_secret).await;
    assert!(wrong_auth.is_err(), "Wrong secret must fail handshake");
    assert!(matches!(wrong_auth.unwrap_err(), GridError::AuthFailed(_)));

    let _ = master.shutdown();
    let _ = open_master.shutdown();
}
