//! Writes the seed corpora under `fuzz/corpus/<target>/`: valid encodings of what each target
//! parses. Deterministic apart from the random nonces of the handshake seeds and the random salt
//! and nonce of the default-cost `invite.oxs` seed.
//!
//!     cd fuzz && cargo run --example gen_seeds                      # every target
//!     cd fuzz && cargo run --example gen_seeds -- aead_record       # only the targets named
//!
//! The committed files are the output of this program; regenerate them only when a format
//! changes, and only for the targets whose format changed.

use std::path::Path;

use argon2::{Algorithm, Argon2, Params, Version};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
use rusty_grid_cli::join::{JoinToken, TOKEN_PREFIX};
use rusty_grid_cli::members_cmd::{InviteToken, INVITE_PREFIX};
use rusty_grid_cli::usb_kit::{encrypt_invite_oxs, INVITE_OXS_MAGIC};
use rusty_grid_core::blob::{BlobFile, BlobId, BlobSymlink, TreeManifest};
use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::cluster_auth::{client_handshake, ClusterSecret};
use rusty_grid_core::noise_auth::{
    v3_client_handshake, HandshakePurpose, MemberRole, NodeKeypair, RedactedBytes32,
};
use rusty_grid_core::protocol::{
    derive_aead_keys, serialize_message, ClientMessage, EndpointRole, MessageTransport, WireCodec,
    WorkerMessage,
};
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskRequirements, TaskResult, TaskSpec};
use rusty_grid_fuzz::{
    block_on, frame, join_chunks, FuzzIo, TEST_CLIENT_KEY, TEST_MASTER_KEY, TEST_PASSPHRASE,
    TEST_SECRET, TEST_SESSION_KEY,
};
use rusty_grid_master::store::fuzzing::{
    journal_header, journal_record_frame, snapshot_image_file,
};
use rusty_grid_master::store::{JournalRecord, PersistedTaskEntry, SnapshotImage};
use rusty_grid_master::{RetryAttempt, TaskState};
use uuid::Uuid;

fn put(dir: &Path, name: &str, bytes: &[u8]) {
    std::fs::create_dir_all(dir).expect("create corpus dir");
    std::fs::write(dir.join(name), bytes).expect("write seed");
    println!("{}/{name}: {} bytes", dir.display(), bytes.len());
}

/// `mode` byte, then `rest`.
fn with_mode(mode: u8, rest: &[u8]) -> Vec<u8> {
    let mut out = vec![mode];
    out.extend_from_slice(rest);
    out
}

fn caps() -> WorkerCapabilities {
    WorkerCapabilities {
        name: "seed-worker".into(),
        cpu_cores: 8,
        ram_mb: 16_384,
        has_gpu: false,
        is_simulated_gpu: false,
        gpu_device_name: None,
        tags: vec!["os:linux".into(), "tool:cargo".into()],
        mobile: None,
        has_blender: false,
        blender_version: None,
        vram_mb: None,
        has_cuda: false,
    }
}

fn blob_file(path: &str, content: &[u8], executable: bool) -> BlobFile {
    BlobFile {
        path: path.into(),
        id: BlobId::of(content),
        size: content.len() as u64,
        executable,
    }
}

/// A small command task with fixed id and time, so its encodings do not change between runs.
fn seed_task(n: u128) -> Task {
    let mut task = Task::new(
        TaskSpec::command("echo", vec!["seed".into()]),
        TaskRequirements::generic(1, 10),
    );
    task.id = TaskId(Uuid::from_u128(n));
    task.created_at_utc = 1_700_000_000;
    task.tags = vec!["require:os=linux".into()];
    task
}

fn inbound_frame(dir: &Path) {
    // Framed messages, in both codecs.
    let worker = WorkerMessage::register(Uuid::from_u128(1), caps());
    let task_id = TaskId(Uuid::from_u128(2));
    let clients = [
        ("hello", ClientMessage::Hello),
        ("list_workers", ClientMessage::ListWorkers),
        ("cluster_status", ClientMessage::ClusterStatus),
        ("get_task_status", ClientMessage::GetTaskStatus { task_id }),
        ("cancel_task", ClientMessage::CancelTask { task_id }),
    ];
    for (codec, tag) in [(WireCodec::Bincode, "bin"), (WireCodec::Json, "json")] {
        let bytes = serialize_message(&worker, codec).unwrap();
        put(dir, &format!("worker_register_{tag}"), &frame(&bytes));
        for (name, msg) in &clients {
            let bytes = serialize_message(msg, codec).unwrap();
            put(dir, &format!("client_{name}_{tag}"), &frame(&bytes));
        }
    }
    // Several frames in a row, and raw legacy JSON without a format byte.
    let mut two = frame(&serialize_message(&ClientMessage::Hello, WireCodec::Bincode).unwrap());
    two.extend(frame(
        &serialize_message(&ClientMessage::ListWorkers, WireCodec::Bincode).unwrap(),
    ));
    put(dir, "two_frames_bin", &two);
    put(
        dir,
        "legacy_raw_json",
        &frame(br#"{"type":"Hello","protocol_version":9}"#),
    );
}

fn server_handshake(dir: &Path) {
    // Mode byte (0 = v2, 1 = v3) + what a client sends first.
    let mut v2 = FuzzIo::new(&[]);
    {
        let mut t = MessageTransport::new(&mut v2);
        let secret = ClusterSecret::from_bytes(&TEST_SECRET).unwrap();
        let _ = block_on(client_handshake(&mut t, &secret));
    }
    let mut seed = vec![0u8];
    seed.extend_from_slice(&v2.written);
    put(dir, "v2_hello", &seed);
    // HELLO, then a PROOF-shaped frame (kind 0x03, 32 bytes of MAC): rejected, but parsed.
    let mut proof = vec![0x03, 0x03];
    proof.extend_from_slice(&[0xab; 32]);
    seed.extend(frame(&proof));
    put(dir, "v2_hello_bad_proof", &seed);
    put(dir, "v2_old_version_hello", &{
        let mut hello = vec![0x03, 0x01, 1];
        hello.extend_from_slice(&[7; 32]);
        let mut s = vec![0u8];
        s.extend(frame(&hello));
        s
    });

    let mut v3 = FuzzIo::new(&[]);
    {
        let mut t = MessageTransport::new(&mut v3);
        let client = NodeKeypair::from_private_bytes(TEST_CLIENT_KEY).unwrap();
        let master = NodeKeypair::from_private_bytes(TEST_MASTER_KEY).unwrap();
        let _ = block_on(v3_client_handshake(
            &mut t,
            &client,
            master.public_key(),
            HandshakePurpose::Member,
        ));
    }
    let mut seed = vec![1u8];
    seed.extend_from_slice(&v3.written);
    put(dir, "v3_hello_msg1", &seed);
}

fn tree_manifest(dir: &Path) {
    // The v1 and the v2 encodings.
    let v1 = TreeManifest::new(vec![
        blob_file("a.txt", b"alpha", false),
        blob_file("src/main.rs", b"fn main() {}", true),
    ])
    .unwrap();
    put(dir, "v1_two_files", &v1.encode());
    let v2 = TreeManifest::with_entries(
        vec![blob_file("bin/run.sh", b"#!/bin/sh", true)],
        vec![BlobSymlink {
            path: "link".into(),
            target: "bin/run.sh".into(),
        }],
        vec!["empty".into()],
    )
    .unwrap();
    put(dir, "v2_link_and_empty_dir", &v2.encode());
    put(
        dir,
        "v1_empty",
        &TreeManifest::new(vec![]).unwrap().encode(),
    );
}

fn aead_record(dir: &Path) {
    let messages = [
        (
            "hello_bin",
            serialize_message(&ClientMessage::Hello, WireCodec::Bincode).unwrap(),
        ),
        (
            "list_workers_json",
            serialize_message(&ClientMessage::ListWorkers, WireCodec::Json).unwrap(),
        ),
        (
            "submit_task_bin",
            serialize_message(
                &ClientMessage::SubmitTask {
                    task: seed_task(3),
                    wait: false,
                },
                WireCodec::Bincode,
            )
            .unwrap(),
        ),
        (
            "worker_register_bin",
            serialize_message(
                &WorkerMessage::register(Uuid::from_u128(1), caps()),
                WireCodec::Bincode,
            )
            .unwrap(),
        ),
    ];

    // Mode 0: records sealed by the client, as the server reads them off the wire.
    let (mut writer, _) = derive_aead_keys(&TEST_SESSION_KEY, EndpointRole::Client);
    let mut stream = Vec::new();
    for (name, plain) in &messages {
        let record = writer.encrypt_frame(plain).unwrap();
        stream.extend(frame(&record));
        put(dir, &format!("raw_upto_{name}"), &with_mode(0, &stream));
    }

    // Mode 1: `[len][op][plaintext]` chunks; op 0 keeps a record.
    let op_chunk = |op: u8, plain: &[u8]| with_mode(op, plain);
    let keep: Vec<Vec<u8>> = messages.iter().map(|(_, p)| op_chunk(0, p)).collect();
    put(
        dir,
        "sealed_keep_all",
        &with_mode(1, &join_chunks(keep.iter().map(Vec::as_slice))),
    );
    // Flip a bit in the middle of the second record (op 1, offset 16/32).
    let flip = [
        op_chunk(0, &messages[0].1),
        op_chunk(1 | (16 << 3), &messages[1].1),
    ];
    put(
        dir,
        "sealed_flip_second",
        &with_mode(1, &join_chunks(flip.iter().map(Vec::as_slice))),
    );
    // Drop the first record, replay the second, cut the third to 3/32.
    let mixed = [
        op_chunk(2, &messages[0].1),
        op_chunk(3, &messages[1].1),
        op_chunk(4 | (3 << 3), &messages[2].1),
    ];
    put(
        dir,
        "sealed_drop_replay_cut",
        &with_mode(1, &join_chunks(mixed.iter().map(Vec::as_slice))),
    );
}

fn seed_invite() -> InviteToken {
    let master = NodeKeypair::from_private_bytes(TEST_MASTER_KEY).unwrap();
    InviteToken {
        master: Some("127.0.0.1:8088".into()),
        ticket: None,
        master_key: *master.public_key(),
        invite_id: "0123456789abcdef0123456789abcdef".into(),
        invite_secret: RedactedBytes32([0x44; 32]),
        expires_at: 2_000_000_000,
        roles: vec![MemberRole::Worker, MemberRole::Client],
        require_approval: true,
    }
}

/// The base64url payload of a token or invite, decoded.
fn payload_json(token: &str, prefix: &str) -> Vec<u8> {
    let (text, _) = token[prefix.len()..].rsplit_once('.').unwrap();
    URL_SAFE_NO_PAD.decode(text).unwrap()
}

/// An `invite.oxs` with a cheap Argon2id cost (16 KiB, one pass), fixed salt and nonce, and the
/// test passphrase: the layout of `usb_kit::encrypt_invite_oxs`, which always uses the default
/// cost.
fn cheap_invite_oxs(invite: &str) -> Vec<u8> {
    let (m, t, p) = (16u32, 1u32, 1u32);
    let salt = [0x55u8; 16];
    let nonce = [0x66u8; 12];
    let mut header = Vec::new();
    header.extend_from_slice(INVITE_OXS_MAGIC);
    header.extend_from_slice(&m.to_le_bytes());
    header.extend_from_slice(&t.to_le_bytes());
    header.extend_from_slice(&p.to_le_bytes());
    header.extend_from_slice(&salt);
    header.extend_from_slice(&nonce);
    let params = Params::new(m, t, p, Some(32)).unwrap();
    let mut key = [0u8; 32];
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into(TEST_PASSPHRASE.as_bytes(), &salt, &mut key)
        .unwrap();
    let key = LessSafeKey::new(UnboundKey::new(&CHACHA20_POLY1305, &key).unwrap());
    let mut body = invite.as_bytes().to_vec();
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(&header[..]),
        &mut body,
    )
    .unwrap();
    header.extend_from_slice(&body);
    header
}

fn join_invite(dir: &Path) {
    let secret = ClusterSecret::from_bytes(&TEST_SECRET).unwrap();
    let with_master = JoinToken {
        master: Some("127.0.0.1:8088".into()),
        ticket: None,
        secret: secret.clone(),
    }
    .encode();
    let with_ticket = JoinToken {
        master: None,
        ticket: Some("nodeabc123relay".into()),
        secret,
    }
    .encode();
    let invite = seed_invite().encode();

    // Mode 0: the text piped into `oxideswarm join`.
    put(
        dir,
        "piped_join_token",
        &with_mode(0, format!("{with_master}\n").as_bytes()),
    );
    put(
        dir,
        "piped_invite",
        &with_mode(0, format!("{invite}\n").as_bytes()),
    );
    // Mode 1 and 2: the JSON payloads.
    put(
        dir,
        "join_payload_master",
        &with_mode(1, &payload_json(&with_master, TOKEN_PREFIX)),
    );
    put(
        dir,
        "join_payload_ticket",
        &with_mode(1, &payload_json(&with_ticket, TOKEN_PREFIX)),
    );
    put(
        dir,
        "invite_payload",
        &with_mode(2, &payload_json(&invite, INVITE_PREFIX)),
    );
    // Mode 3: `invite.oxs` files, at the default cost (only validated) and at a cheap cost.
    put(
        dir,
        "invite_oxs_default_cost",
        &with_mode(3, &encrypt_invite_oxs(&invite, TEST_PASSPHRASE).unwrap()),
    );
    put(
        dir,
        "invite_oxs_cheap_cost",
        &with_mode(3, &cheap_invite_oxs(&invite)),
    );
}

fn seed_records() -> Vec<JournalRecord> {
    let task = seed_task(10);
    let id = task.id;
    let worker = Uuid::from_u128(20);
    let attempt = RetryAttempt {
        attempt: 1,
        timestamp_utc: 42,
        failed_worker_id: Some(worker),
        reason: "worker lost".into(),
        exit_code: Some(137),
        error: Some("killed".into()),
    };
    let mut checkpoint = CheckpointData::new(3, vec![1u8, 2, 3]);
    checkpoint.timestamp = 1_700_000_001;
    vec![
        JournalRecord::TaskSubmitted {
            task: Box::new(task),
            priority: 9,
            sequence: 7,
            submitted_at_utc: 11,
        },
        JournalRecord::TaskDispatched {
            task_id: id,
            worker_id: worker,
            timestamp_utc: 12,
        },
        JournalRecord::TaskRunning {
            task_id: id,
            worker_id: worker,
            timestamp_utc: 13,
        },
        JournalRecord::CheckpointRecorded {
            task_id: id,
            checkpoint,
        },
        JournalRecord::TaskRequeued {
            task_id: id,
            state: TaskState::Retrying,
            retry_count: 2,
            failover_count: 1,
            evacuation_count: 3,
            rejection_count: 4,
            avoid_workers: vec![worker],
            retry_attempt: Some(attempt.clone()),
            retry_delay_ms: Some(250),
        },
        JournalRecord::TaskAdopted {
            task_id: id,
            worker_id: worker,
            timestamp_utc: 14,
        },
        JournalRecord::TaskTerminal {
            task_id: id,
            state: TaskState::Failed,
            assigned_worker_id: Some(worker),
            retry_count: 2,
            failover_count: 1,
            evacuation_count: 3,
            rejection_count: 4,
            retry_attempt: Some(attempt),
            result: Some(TaskResult::failure(
                worker,
                id,
                1,
                "out",
                "err",
                9,
                Some("boom".into()),
            )),
            error_message: Some("boom".into()),
            timestamp_utc: 15,
        },
        JournalRecord::TaskEvicted { task_id: id },
    ]
}

fn store_journal(dir: &Path) {
    // Mode 0: a whole journal of generation 0 with one record of each kind.
    let frames: Vec<Vec<u8>> = seed_records()
        .iter()
        .map(|r| journal_record_frame(r).unwrap())
        .collect();
    let mut journal = journal_header(0).to_vec();
    for f in &frames {
        journal.extend_from_slice(f);
    }
    put(dir, "journal_all_records", &with_mode(0, &journal));
    put(
        dir,
        "journal_torn_tail",
        &with_mode(0, &journal[..journal.len() - 7]),
    );
    put(
        dir,
        "journal_header_only",
        &with_mode(0, &journal_header(0)),
    );
    // Mode 4: the snapshot is one generation newer, so the journal is stale.
    put(dir, "journal_stale", &with_mode(4, &journal));
    // Mode 2: the payloads only; the target writes header, lengths and hashes.
    let payloads: Vec<&[u8]> = frames.iter().map(|f| &f[40..]).collect();
    put(
        dir,
        "journal_framed_payloads",
        &with_mode(2, &join_chunks(payloads)),
    );

    // Mode 1: whole snapshots. Mode 3: the JSON payload only.
    let task = seed_task(11);
    let image = SnapshotImage {
        next_sequence: 2,
        terminal_order: vec![task.id],
        tasks: vec![PersistedTaskEntry {
            priority: 5,
            sequence: 1,
            state: TaskState::Completed,
            assigned_worker_id: Some(Uuid::from_u128(21)),
            submitted_at_utc: 1_700_000_000,
            retry_count: 0,
            failover_count: 0,
            evacuation_count: 0,
            rejection_count: 0,
            avoid_workers: Vec::new(),
            adoptable_by_worker: None,
            fail_if_not_adopted: false,
            retry_history: Vec::new(),
            result: Some(TaskResult::success(
                Uuid::from_u128(21),
                task.id,
                "seed\n",
                5,
                false,
            )),
            error_message: None,
            cold_result: None,
            task,
        }],
    };
    let snapshot = snapshot_image_file(3, &image).unwrap();
    put(dir, "snapshot_one_task", &with_mode(1, &snapshot));
    put(
        dir,
        "snapshot_empty",
        &with_mode(
            1,
            &snapshot_image_file(0, &SnapshotImage::default()).unwrap(),
        ),
    );
    put(
        dir,
        "snapshot_framed_json",
        &with_mode(3, &snapshot[24 + 40..]),
    );
}

/// Writes the seeds of one target into the directory it is given.
type SeedWriter = fn(&Path);

fn main() {
    let wanted: Vec<String> = std::env::args().skip(1).collect();
    let root = Path::new("corpus");
    let targets: [(&str, SeedWriter); 6] = [
        ("inbound_frame", inbound_frame),
        ("server_handshake", server_handshake),
        ("tree_manifest", tree_manifest),
        ("aead_record", aead_record),
        ("join_invite", join_invite),
        ("store_journal", store_journal),
    ];
    for name in &wanted {
        assert!(
            targets.iter().any(|(t, _)| t == name),
            "unknown target {name}"
        );
    }
    for (name, write) in targets {
        if wanted.is_empty() || wanted.iter().any(|w| w == name) {
            write(&root.join(name));
        }
    }
}
