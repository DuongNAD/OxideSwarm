//! Writes the seed corpora under `fuzz/corpus/<target>/`: valid encodings of what each target
//! parses. Deterministic apart from the random nonces of the handshake seeds.
//!
//!     cd fuzz && cargo run --example gen_seeds
//!
//! The committed files are the output of this program; regenerate them only when a wire format
//! changes.

use std::path::Path;

use rusty_grid_core::blob::{BlobFile, BlobId, BlobSymlink, TreeManifest};
use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::cluster_auth::{client_handshake, ClusterSecret};
use rusty_grid_core::noise_auth::{v3_client_handshake, HandshakePurpose, NodeKeypair};
use rusty_grid_core::protocol::{
    serialize_message, ClientMessage, MessageTransport, WireCodec, WorkerMessage,
};
use rusty_grid_core::task::TaskId;
use rusty_grid_fuzz::{block_on, FuzzIo, TEST_CLIENT_KEY, TEST_MASTER_KEY, TEST_SECRET};
use uuid::Uuid;

fn put(dir: &Path, name: &str, bytes: &[u8]) {
    std::fs::create_dir_all(dir).expect("create corpus dir");
    std::fs::write(dir.join(name), bytes).expect("write seed");
    println!("{}/{name}: {} bytes", dir.display(), bytes.len());
}

/// `[len: u32 BE][payload]`, the framing of `MessageTransport`.
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(payload);
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

fn main() {
    let root = Path::new("corpus");

    // inbound_frame: framed messages, in both codecs.
    let dir = root.join("inbound_frame");
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
        put(&dir, &format!("worker_register_{tag}"), &frame(&bytes));
        for (name, msg) in &clients {
            let bytes = serialize_message(msg, codec).unwrap();
            put(&dir, &format!("client_{name}_{tag}"), &frame(&bytes));
        }
    }
    // Several frames in a row, and raw legacy JSON without a format byte.
    let mut two = frame(&serialize_message(&ClientMessage::Hello, WireCodec::Bincode).unwrap());
    two.extend(frame(
        &serialize_message(&ClientMessage::ListWorkers, WireCodec::Bincode).unwrap(),
    ));
    put(&dir, "two_frames_bin", &two);
    put(
        &dir,
        "legacy_raw_json",
        &frame(br#"{"type":"Hello","protocol_version":9}"#),
    );

    // server_handshake: mode byte (0 = v2, 1 = v3) + what a client sends first.
    let dir = root.join("server_handshake");
    let mut v2 = FuzzIo::new(&[]);
    {
        let mut t = MessageTransport::new(&mut v2);
        let secret = ClusterSecret::from_bytes(&TEST_SECRET).unwrap();
        let _ = block_on(client_handshake(&mut t, &secret));
    }
    let mut seed = vec![0u8];
    seed.extend_from_slice(&v2.written);
    put(&dir, "v2_hello", &seed);
    // HELLO, then a PROOF-shaped frame (kind 0x03, 32 bytes of MAC): rejected, but parsed.
    let mut proof = vec![0x03, 0x03];
    proof.extend_from_slice(&[0xab; 32]);
    seed.extend(frame(&proof));
    put(&dir, "v2_hello_bad_proof", &seed);
    put(&dir, "v2_old_version_hello", &{
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
            &master.public_key(),
            HandshakePurpose::Member,
        ));
    }
    let mut seed = vec![1u8];
    seed.extend_from_slice(&v3.written);
    put(&dir, "v3_hello_msg1", &seed);

    // tree_manifest: the v1 and the v2 encodings.
    let dir = root.join("tree_manifest");
    let v1 = TreeManifest::new(vec![
        blob_file("a.txt", b"alpha", false),
        blob_file("src/main.rs", b"fn main() {}", true),
    ])
    .unwrap();
    put(&dir, "v1_two_files", &v1.encode());
    let v2 = TreeManifest::with_entries(
        vec![blob_file("bin/run.sh", b"#!/bin/sh", true)],
        vec![BlobSymlink {
            path: "link".into(),
            target: "bin/run.sh".into(),
        }],
        vec!["empty".into()],
    )
    .unwrap();
    put(&dir, "v2_link_and_empty_dir", &v2.encode());
    put(
        &dir,
        "v1_empty",
        &TreeManifest::new(vec![]).unwrap().encode(),
    );
}
