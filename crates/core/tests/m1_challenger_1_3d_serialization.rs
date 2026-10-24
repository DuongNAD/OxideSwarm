//! Adversarial Stress-Testing and Serialization Boundary Verification Suite
//! Milestone 1: 3D Generation & Auto-Rigging Specs, Wire Framing, and Zero-Copy Payloads
//! Author: challenger_m1_3d_1
//!
//! Objectives:
//! 1. Fuzz-test Generative3DSpec and AutoRigSpec with extreme inputs (empty, 1MB prompt, Unicode, emojis, null bytes).
//! 2. Test AutoRigSpec with empty byte buffer vs 32MB binary payload (simulated .glb container), verifying exact byte count preservation.
//! 3. Verify zero-copy semantics: ensure asset_bytes clone is O(1) Arc refcount increment with identical memory pointer.
//! 4. Test JSON vs Bincode deserialization with corrupted tag strings or truncated binary frames.
//! 5. Verify length-delimited framing and transport over duplex streams.
//! 6. Verify concurrent multi-threaded zero-copy access and thread safety across threads.
//! 7. Verify invalid Bincode enum variant indices resilience.

use std::sync::Arc;
use std::time::Instant;
use bytes::BytesMut;
use tokio_util::codec::Decoder;
use uuid::Uuid;

use rusty_grid_core::protocol::{
    default_codec, deserialize_message, serialize_message, MessageTransport,
    ProtocolError, WireCodec, WorkerMessage, MAX_FRAME_SIZE,
    WIRE_FORMAT_BINCODE, WIRE_FORMAT_JSON,
};
use rusty_grid_core::task::{
    AutoRigSpec, Bytes as TaskBytes, CheckpointData, Generative3DSpec, Task, TaskId,
    TaskRequirements, TaskResult, TaskSpec,
};

/// Helper: creates a simulated glTF 2.0 binary (.glb) buffer of specified size.
/// Adheres to glTF 2.0 binary header format:
/// - 4-byte magic: 0x46546C67 ("glTF")
/// - 4-byte version: 2
/// - 4-byte total length
/// - Chunk 0 (JSON): 4-byte length, 0x4E4F534A ("JSON"), chunk data
/// - Chunk 1 (BIN): 4-byte length, 0x004E4942 ("BIN\0"), chunk data
fn create_simulated_glb(total_size: usize) -> Vec<u8> {
    assert!(total_size >= 64, "GLB size must be at least 64 bytes");
    let mut data = vec![0u8; total_size];

    // glTF header (12 bytes)
    data[0..4].copy_from_slice(b"glTF");
    data[4..8].copy_from_slice(&2u32.to_le_bytes());
    data[8..12].copy_from_slice(&(total_size as u32).to_le_bytes());

    // Chunk 0: JSON metadata (20 bytes)
    let json_payload = b"{\"asset\":{\"version\":\"2.0\"}}";
    let json_len = json_payload.len() as u32;
    data[12..16].copy_from_slice(&json_len.to_le_bytes());
    data[16..20].copy_from_slice(b"JSON");
    data[20..20 + json_payload.len()].copy_from_slice(json_payload);

    // Chunk 1: BIN buffer header
    let bin_header_start = 20 + json_payload.len();
    let bin_data_start = bin_header_start + 8;
    let bin_len = (total_size - bin_data_start) as u32;
    data[bin_header_start..bin_header_start + 4].copy_from_slice(&bin_len.to_le_bytes());
    data[bin_header_start + 4..bin_header_start + 8].copy_from_slice(b"BIN\0");

    // Fill remaining binary chunk with deterministic patterned pseudo-random data
    for i in bin_data_start..total_size {
        data[i] = ((i * 31 + 17) & 0xFF) as u8;
    }

    data
}

// =========================================================================
// 1. Fuzz-testing Generative3DSpec with Extreme Inputs
// =========================================================================

#[test]
fn test_adv_01_generative_3d_extreme_inputs_and_fuzzing() {
    println!("\n=== [ADV-01] Generative3DSpec Extreme Inputs & Fuzzing ===");

    // Vector A: Empty prompt with None image fails validation
    let empty_spec = Generative3DSpec {
        prompt: Some("".to_string()),
        image_data: None,
        quality: "standard".to_string(),
        engine_preference: None,
        export_texture: true,
    };
    let val_result = TaskSpec::Generative3D(empty_spec).validate();
    assert!(val_result.is_err(), "Empty prompt + None image must fail validation");
    println!("  [+] Empty prompt + None image correctly rejected: {:?}", val_result.unwrap_err());

    // Vector B: Whitespace-only prompt fails validation
    let whitespace_spec = Generative3DSpec::new_prompt("   \t\n  \r\n ");
    let ws_result = TaskSpec::Generative3D(whitespace_spec).validate();
    assert!(ws_result.is_err(), "Whitespace-only prompt must fail validation");
    println!("  [+] Whitespace-only prompt correctly rejected: {:?}", ws_result.unwrap_err());

    // Vector C: Massive 1MB prompt (1,048,576 bytes)
    let one_mb = 1024 * 1024;
    let prompt_chunk = "A photorealistic cybernetic dragon with mechanical wings, titanium scales, glowing neon emissive circuits, 8k textures, high topology mesh. ";
    let repeat_count = (one_mb / prompt_chunk.len()) + 1;
    let mut large_prompt = prompt_chunk.repeat(repeat_count);
    large_prompt.truncate(one_mb);
    assert_eq!(large_prompt.len(), one_mb);

    let large_spec = Generative3DSpec::new_prompt(large_prompt.clone())
        .with_quality("high")
        .with_engine("triposr")
        .with_export_texture(true);

    // Validation must pass
    assert!(TaskSpec::Generative3D(large_spec.clone()).validate().is_ok());

    // JSON roundtrip
    let t0 = Instant::now();
    let json_str = serde_json::to_string(&large_spec).expect("JSON serialize 1MB prompt");
    let deser_json: Generative3DSpec = serde_json::from_str(&json_str).expect("JSON deserialize 1MB prompt");
    let json_elapsed = t0.elapsed();
    assert_eq!(deser_json.prompt.as_ref().unwrap().len(), one_mb);
    assert_eq!(deser_json.prompt.as_ref().unwrap(), &large_prompt);
    assert_eq!(deser_json.quality, "high");
    assert_eq!(deser_json.engine_preference.as_deref(), Some("triposr"));
    assert!(deser_json.export_texture);
    println!("  [+] 1MB prompt JSON roundtrip succeeded in {:?} (size: {} bytes)", json_elapsed, json_str.len());

    // Bincode roundtrip
    let t1 = Instant::now();
    let bin_bytes = bincode::serialize(&large_spec).expect("Bincode serialize 1MB prompt");
    let deser_bin: Generative3DSpec = bincode::deserialize(&bin_bytes).expect("Bincode deserialize 1MB prompt");
    let bin_elapsed = t1.elapsed();
    assert_eq!(deser_bin.prompt.as_ref().unwrap().len(), one_mb);
    assert_eq!(deser_bin.prompt.as_ref().unwrap(), &large_prompt);
    println!("  [+] 1MB prompt Bincode roundtrip succeeded in {:?} (size: {} bytes)", bin_elapsed, bin_bytes.len());

    // Vector D: Adversarial Unicode, diacritics, RTL override, emojis, embedded null bytes
    let adversarial_prompts = vec![
        // Multi-byte UTF-8 & Vietnamese diacritics
        "Khám phá mô hình 3D đa chiều với hệ thống tính toán phân tán OxideSwarm: ế, ộ, ử, ỹ, đ, ạ, ắ",
        // CJK characters
        "超高精度3Dモデル生成：サイバーパンクの戦士、龍の鎧、メカニカル構造、完全テクスチャ",
        // Complex ZWJ emoji sequences & skin tone modifiers
        "👨‍👩‍👧‍👦 🧙‍♂️ 🧟‍♀️ 🦾 🤖 🦀 🏳️‍🌈 👩‍🚀 🚀 ⚙️",
        // Bidirectional text & Right-to-Left override
        "English text with \u{202E}DESREVER TXET\u{202C} and العربية and עברית mixed",
        // Embedded null bytes (\0)
        "cyberpunk\0character\0with\0titanium\0core\0and\0embedded\0null\0bytes",
        // Control characters & ANSI escape codes
        "\x01\x02\x03\x04\x1b[31;1mRedText\x1b[0m\t\r\n\x0b\x0c",
        // JSON injection and unescaped quotes
        "\"},{\"injected\":\"payload\"},\"hack\":true,\"spec\":{\"",
    ];

    for (idx, adv_prompt) in adversarial_prompts.into_iter().enumerate() {
        let spec = Generative3DSpec::new_prompt(adv_prompt)
            .with_quality("fast")
            .with_engine("sdf");

        // Validate
        assert!(TaskSpec::Generative3D(spec.clone()).validate().is_ok());

        // JSON roundtrip
        let json_enc = serde_json::to_string(&spec).expect("JSON serialize adv prompt");
        let json_dec: Generative3DSpec = serde_json::from_str(&json_enc).expect("JSON deserialize adv prompt");
        assert_eq!(json_dec.prompt.as_deref(), Some(adv_prompt), "JSON adv prompt #{} mismatch", idx);

        // Bincode roundtrip
        let bin_enc = bincode::serialize(&spec).expect("Bincode serialize adv prompt");
        let bin_dec: Generative3DSpec = bincode::deserialize(&bin_enc).expect("Bincode deserialize adv prompt");
        assert_eq!(bin_dec.prompt.as_deref(), Some(adv_prompt), "Bincode adv prompt #{} mismatch", idx);
    }
    println!("  [+] Successfully verified 7 adversarial Unicode, ZWJ emoji, RTL, null byte, and injection strings.");

    // Vector E: Quality field fuzzing
    let empty_quality_spec = Generative3DSpec {
        prompt: Some("valid prompt".to_string()),
        image_data: None,
        quality: "".to_string(),
        engine_preference: None,
        export_texture: true,
    };
    assert!(TaskSpec::Generative3D(empty_quality_spec).validate().is_err(), "Empty quality must fail validation");

    let whitespace_quality_spec = Generative3DSpec {
        prompt: Some("valid prompt".to_string()),
        image_data: None,
        quality: "   ".to_string(),
        engine_preference: None,
        export_texture: true,
    };
    assert!(TaskSpec::Generative3D(whitespace_quality_spec).validate().is_err(), "Whitespace quality must fail validation");

    // Vector F: Engine preference & GPU requirements logic
    let sdf_spec = TaskSpec::Generative3D(Generative3DSpec::new_prompt("cube").with_engine("sdf"));
    assert!(!sdf_spec.requires_gpu(), "SDF engine must NOT require GPU");

    let procedural_spec = TaskSpec::Generative3D(Generative3DSpec::new_prompt("cube").with_engine("procedural"));
    assert!(!procedural_spec.requires_gpu(), "Procedural engine must NOT require GPU");

    let triposr_spec = TaskSpec::Generative3D(Generative3DSpec::new_prompt("cube").with_engine("triposr"));
    assert!(triposr_spec.requires_gpu(), "TripoSR engine MUST require GPU");

    let auto_spec = TaskSpec::Generative3D(Generative3DSpec::new_prompt("cube"));
    assert!(auto_spec.requires_gpu(), "Default/auto engine MUST require GPU");
    println!("  [+] Verified engine preferences and GPU requirement rules.");
}

// =========================================================================
// 2. Fuzz-testing AutoRigSpec with Extreme Inputs & Collections
// =========================================================================

#[test]
fn test_adv_02_auto_rig_extreme_inputs_and_fuzzing() {
    println!("\n=== [ADV-02] AutoRigSpec Extreme Inputs & Fuzzing ===");

    // Vector A: Empty byte buffer input mesh
    let empty_rig = AutoRigSpec::new(TaskBytes::new(), "biped");
    // Serializes/deserializes without error
    let json_empty = serde_json::to_string(&empty_rig).expect("Serialize empty AutoRigSpec");
    let deser_empty: AutoRigSpec = serde_json::from_str(&json_empty).expect("Deserialize empty AutoRigSpec");
    assert!(deser_empty.input_mesh.is_empty());

    // But validation MUST fail
    let val_empty = TaskSpec::AutoRig(empty_rig).validate();
    assert!(val_empty.is_err(), "AutoRig with empty input_mesh must fail validation");
    println!("  [+] Empty input_mesh correctly rejected during validation: {:?}", val_empty.unwrap_err());

    // Vector B: Empty rig_preset validation
    let mut empty_preset_rig = AutoRigSpec::new(TaskBytes::from("valid_mesh_bytes"), "biped");
    empty_preset_rig.rig_preset = "   ".to_string();
    let val_preset = TaskSpec::AutoRig(empty_preset_rig).validate();
    assert!(val_preset.is_err(), "Whitespace rig_preset must fail validation");
    println!("  [+] Empty rig_preset correctly rejected: {:?}", val_preset.unwrap_err());

    // Vector C: Massive animation clip list (10,000 clips)
    let clip_count = 10_000;
    let clips: Vec<String> = (0..clip_count).map(|i| format!("anim_clip_{:05}_run_jump_attack", i)).collect();
    let heavy_rig = AutoRigSpec::new(TaskBytes::from("mesh_header_data"), "quadruped")
        .with_animations(clips.clone())
        .with_provider("blender");

    // JSON roundtrip
    let t0 = Instant::now();
    let json_str = serde_json::to_string(&heavy_rig).expect("Serialize 10k clips JSON");
    let deser_json: AutoRigSpec = serde_json::from_str(&json_str).expect("Deserialize 10k clips JSON");
    let json_time = t0.elapsed();
    assert_eq!(deser_json.embed_animations.len(), clip_count);
    assert_eq!(deser_json.rig_preset, "quadruped");
    assert_eq!(deser_json.preferred_provider.as_deref(), Some("blender"));
    println!("  [+] 10,000 animation clips JSON roundtrip in {:?} (size: {} bytes)", json_time, json_str.len());

    // Bincode roundtrip
    let t1 = Instant::now();
    let bin_bytes = bincode::serialize(&heavy_rig).expect("Serialize 10k clips Bincode");
    let deser_bin: AutoRigSpec = bincode::deserialize(&bin_bytes).expect("Deserialize 10k clips Bincode");
    let bin_time = t1.elapsed();
    assert_eq!(deser_bin.embed_animations.len(), clip_count);
    println!("  [+] 10,000 animation clips Bincode roundtrip in {:?} (size: {} bytes)", bin_time, bin_bytes.len());

    // Vector D: Adversarial animation names (emojis, diacritics, nulls)
    let adv_clips = vec![
        "🏃‍♂️_run_sprint_ZWJ".to_string(),
        "💃_dance_turnaround".to_string(),
        "🛑_idle_breathing\0embedded_null".to_string(),
        "Khởi_động_gait_cycle".to_string(),
        "攻撃_attack_slash_01".to_string(),
        "<script>alert(1)</script>".to_string(),
    ];
    let adv_rig = AutoRigSpec::new(TaskBytes::from("valid_glb_bytes"), "biped")
        .with_animations(adv_clips.clone());
    let bin_adv = bincode::serialize(&adv_rig).expect("Serialize adv clips");
    let deser_adv: AutoRigSpec = bincode::deserialize(&bin_adv).expect("Deserialize adv clips");
    assert_eq!(deser_adv.embed_animations, adv_clips);
    println!("  [+] Adversarial clip names with emojis and null bytes preserved exactly.");
}

// =========================================================================
// 3. 32MB Binary Payload Simulation & Length-Delimited Framing
// =========================================================================

#[test]
fn test_adv_03_auto_rig_32mb_payload_bincode_and_wire_framing() {
    println!("\n=== [ADV-03] AutoRigSpec 32MB Binary Payload (.glb) & Bincode Framing ===");

    let payload_size = 32 * 1024 * 1024; // 33,554,432 bytes
    let t_gen = Instant::now();
    let glb_bytes = create_simulated_glb(payload_size);
    let gen_time = t_gen.elapsed();
    assert_eq!(glb_bytes.len(), payload_size);
    println!("  [+] Generated 32MB simulated .glb payload in {:?}", gen_time);

    let original_task_bytes = TaskBytes::from(glb_bytes);
    let auto_rig_spec = AutoRigSpec::new(original_task_bytes.clone(), "biped")
        .with_animations(vec!["idle".to_string(), "walk".to_string(), "run".to_string()])
        .with_provider("builtin");

    // Validation must pass
    assert!(TaskSpec::AutoRig(auto_rig_spec.clone()).validate().is_ok());

    // Step A: Pure Bincode roundtrip of AutoRigSpec
    let t_ser = Instant::now();
    let bincode_buf = bincode::serialize(&auto_rig_spec).expect("Bincode serialize 32MB AutoRigSpec");
    let ser_time = t_ser.elapsed();
    println!("  [+] Serialized 32MB AutoRigSpec to Bincode in {:?} (output buffer: {} bytes)", ser_time, bincode_buf.len());

    let t_deser = Instant::now();
    let deser_spec: AutoRigSpec = bincode::deserialize(&bincode_buf).expect("Bincode deserialize 32MB AutoRigSpec");
    let deser_time = t_deser.elapsed();
    println!("  [+] Deserialized 32MB AutoRigSpec from Bincode in {:?}", deser_time);

    // Verify exact byte count preservation
    assert_eq!(deser_spec.input_mesh.len(), payload_size, "Byte count must be exactly 32MB");
    assert_eq!(deser_spec.rig_preset, "biped");
    assert_eq!(deser_spec.embed_animations.len(), 3);
    assert_eq!(deser_spec.preferred_provider.as_deref(), Some("builtin"));

    // Verify data integrity: byte-for-byte check across boundary checkpoints
    let orig_slice = original_task_bytes.as_slice();
    let deser_slice = deser_spec.input_mesh.as_slice();

    // Check header
    assert_eq!(&orig_slice[0..12], &deser_slice[0..12], "glTF header must match");
    // Check first 1KB
    assert_eq!(&orig_slice[0..1024], &deser_slice[0..1024], "First 1KB must match");
    // Check midpoint 1KB
    let mid = payload_size / 2;
    assert_eq!(&orig_slice[mid..mid + 1024], &deser_slice[mid..mid + 1024], "Midpoint 1KB must match");
    // Check last 1KB
    assert_eq!(&orig_slice[payload_size - 1024..payload_size], &deser_slice[payload_size - 1024..payload_size], "Last 1KB must match");
    println!("  [+] Verified byte-for-byte data integrity across all checkpoints (header, mid, footer).");

    // Step B: Wire message framing with WorkerMessage::TaskResult carrying 32MB asset_bytes
    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();
    let worker_msg = WorkerMessage::TaskResult {
        worker_id,
        task_id,
        exit_code: 0,
        stdout: TaskBytes::from("3D Rigging completed in 3200ms"),
        stderr: TaskBytes::new(),
        execution_time_ms: 3200,
        is_gpu_executed: false,
        device_name: Some("CPU Geodesic Solver".to_string()),
        error: None,
        asset_bytes: Some(original_task_bytes.clone()),
    };

    let t_frame = Instant::now();
    let framed_payload = serialize_message(&worker_msg, WireCodec::Bincode).expect("serialize_message with 32MB payload");
    let frame_time = t_frame.elapsed();
    assert_eq!(framed_payload[0], WIRE_FORMAT_BINCODE, "Discriminator must be 0x02");
    println!("  [+] Framed WorkerMessage with 32MB payload in {:?} (framed size: {} bytes)", frame_time, framed_payload.len());

    // Decode message
    let t_unframe = Instant::now();
    let (deser_msg, codec) = deserialize_message::<WorkerMessage>(&framed_payload).expect("deserialize_message with 32MB payload");
    let unframe_time = t_unframe.elapsed();
    assert_eq!(codec, WireCodec::Bincode);
    println!("  [+] Deserialized framed WorkerMessage in {:?}", unframe_time);

    match deser_msg {
        WorkerMessage::TaskResult {
            worker_id: wid,
            task_id: tid,
            exit_code,
            asset_bytes: Some(asset),
            ..
        } => {
            assert_eq!(wid, worker_id);
            assert_eq!(tid, task_id);
            assert_eq!(exit_code, 0);
            assert_eq!(asset.len(), payload_size, "Asset bytes must be exact 32MB");
            assert_eq!(&asset.as_slice()[0..12], b"glTF\x02\0\0\0\0\0\0\x02");
        }
        _ => panic!("Expected WorkerMessage::TaskResult with asset_bytes"),
    }
    println!("  [+] 32MB wire message transport validated with zero truncation or corruption.");
}

// =========================================================================
// 4. LengthDelimitedCodec Wire Transport over Asynchronous Duplex Stream
// =========================================================================

#[tokio::test]
async fn test_adv_04_wire_framing_transport_32mb_duplex() {
    println!("\n=== [ADV-04] Full Duplex 32MB Stream Transport with LengthDelimitedCodec ===");

    // Construct duplex channel with 64MB buffer
    let (client_io, server_io) = tokio::io::duplex(64 * 1024 * 1024);
    let mut client_transport = MessageTransport::new(client_io);
    let mut server_transport = MessageTransport::new(server_io);

    // Prepare 32MB payload
    let payload_size = 32 * 1024 * 1024;
    let glb_bytes = create_simulated_glb(payload_size);
    let asset_bytes = TaskBytes::from(glb_bytes);

    let task_id = TaskId::new();
    let worker_id = Uuid::new_v4();
    let task_result_msg = WorkerMessage::TaskResult {
        worker_id,
        task_id,
        exit_code: 0,
        stdout: TaskBytes::from("AutoRig pipeline executed successfully"),
        stderr: TaskBytes::new(),
        execution_time_ms: 1840,
        is_gpu_executed: true,
        device_name: Some("NVIDIA RTX 4090".to_string()),
        error: None,
        asset_bytes: Some(asset_bytes.clone()),
    };

    // Client sends message concurrently, server receives message
    let send_handle = tokio::spawn(async move {
        let t0 = Instant::now();
        client_transport.send_msg(&task_result_msg).await.expect("Client send 32MB message");
        t0.elapsed()
    });

    let recv_handle = tokio::spawn(async move {
        let t0 = Instant::now();
        let received: Option<WorkerMessage> = server_transport.recv_msg().await.expect("Server recv 32MB message");
        (received, t0.elapsed())
    });

    let send_elapsed = send_handle.await.expect("Send task join");
    let (received_msg, recv_elapsed) = recv_handle.await.expect("Recv task join");

    println!("  [+] Sent 32MB payload in {:?}", send_elapsed);
    println!("  [+] Received 32MB payload in {:?}", recv_elapsed);

    let received = received_msg.expect("Expected message received on server");
    match received {
        WorkerMessage::TaskResult {
            worker_id: wid,
            task_id: tid,
            exit_code,
            asset_bytes: Some(rec_asset),
            ..
        } => {
            assert_eq!(wid, worker_id);
            assert_eq!(tid, task_id);
            assert_eq!(exit_code, 0);
            assert_eq!(rec_asset.len(), payload_size);
            // Verify byte integrity at offset 0, 16MB, 32MB - 1
            assert_eq!(&rec_asset.as_slice()[0..4], b"glTF");
            assert_eq!(rec_asset.as_slice()[16 * 1024 * 1024], asset_bytes.as_slice()[16 * 1024 * 1024]);
            assert_eq!(rec_asset.as_slice()[payload_size - 1], asset_bytes.as_slice()[payload_size - 1]);
        }
        _ => panic!("Expected TaskResult variant"),
    }

    // Boundary Test: JSON serialization of 32MB binary payload
    // Serializing 32MB binary non-UTF8 data as JSON numbers ([103,108,84,...]) would expand to > 120MB,
    // which MUST trigger ProtocolError::FrameTooLarge (> 64MB).
    let json_result = serialize_message(&WorkerMessage::TaskResult {
        worker_id,
        task_id,
        exit_code: 0,
        stdout: TaskBytes::new(),
        stderr: TaskBytes::new(),
        execution_time_ms: 0,
        is_gpu_executed: false,
        device_name: None,
        error: None,
        asset_bytes: Some(asset_bytes),
    }, WireCodec::Json);

    assert!(json_result.is_err(), "32MB binary payload in JSON must exceed MAX_FRAME_SIZE (64MB)");
    match json_result.unwrap_err() {
        ProtocolError::FrameTooLarge { size, max } => {
            println!("  [+] Confirmed JSON expansion boundary: {} bytes exceeds max frame {} bytes", size, max);
            assert_eq!(max, MAX_FRAME_SIZE);
        }
        other => panic!("Expected ProtocolError::FrameTooLarge, got: {:?}", other),
    }

    // But a 512KB binary payload in JSON should comfortably fit within 64MB (< 2.5MB JSON)
    let small_asset = TaskBytes::from(create_simulated_glb(512 * 1024));
    let small_json_res = serialize_message(&WorkerMessage::TaskResult {
        worker_id,
        task_id,
        exit_code: 0,
        stdout: TaskBytes::new(),
        stderr: TaskBytes::new(),
        execution_time_ms: 0,
        is_gpu_executed: false,
        device_name: None,
        error: None,
        asset_bytes: Some(small_asset.clone()),
    }, WireCodec::Json);

    assert!(small_json_res.is_ok(), "512KB binary payload must fit in JSON wire frame");
    let (deser_small, codec) = deserialize_message::<WorkerMessage>(&small_json_res.unwrap()).expect("Deserialize small JSON");
    assert_eq!(codec, WireCodec::Json);
    if let WorkerMessage::TaskResult { asset_bytes: Some(b), .. } = deser_small {
        assert_eq!(b.len(), 512 * 1024);
    } else {
        panic!("Expected small asset bytes");
    }
    println!("  [+] Verified 512KB JSON wire frame roundtrip success.");
}

// =========================================================================
// 5. Zero-Copy Semantics & Latency Micro-Benchmark
// =========================================================================

#[test]
fn test_adv_05_zero_copy_pointer_identity_and_latency() {
    println!("\n=== [ADV-05] Zero-Copy Semantics, Pointer Identity, and Latency Benchmark ===");

    // Construct 32MB buffer
    let payload_size = 32 * 1024 * 1024;
    let glb_bytes = create_simulated_glb(payload_size);
    let original_task_bytes = TaskBytes::from(glb_bytes);

    // Initial buffer pointer in memory
    let ptr_original = original_task_bytes.inner().as_ptr();
    println!("  [+] Original 32MB buffer memory address: {:p}", ptr_original);

    // Store in TaskResult
    let result = TaskResult {
        worker_id: Uuid::new_v4(),
        task_id: TaskId::new(),
        exit_code: 0,
        stdout: TaskBytes::from("done"),
        stderr: TaskBytes::new(),
        execution_time_ms: 500,
        is_gpu_executed: true,
        device_name: Some("GPU".to_string()),
        error: None,
        asset_bytes: Some(original_task_bytes.clone()),
    };

    // Check pointer inside TaskResult
    let ptr_in_result = result.asset_bytes.as_ref().unwrap().inner().as_ptr();
    assert_eq!(ptr_original, ptr_in_result, "Cloning TaskBytes into TaskResult must not copy memory");

    // Clone entire TaskResult
    let cloned_result = result.clone();
    let ptr_in_cloned = cloned_result.asset_bytes.as_ref().unwrap().inner().as_ptr();
    assert_eq!(ptr_original, ptr_in_cloned, "Cloning TaskResult must preserve identical buffer memory address");
    println!("  [+] Verified identical buffer pointer across TaskResult clones: {:p} == {:p}", ptr_in_result, ptr_in_cloned);

    // Slicing zero-copy check
    let slice_offset = 1024 * 1024; // 1MB offset
    let slice_len = 2 * 1024 * 1024; // 2MB slice
    let sub_slice = original_task_bytes.slice(slice_offset..slice_offset + slice_len);
    let expected_sub_ptr = unsafe { ptr_original.add(slice_offset) };
    assert_eq!(sub_slice.inner().as_ptr(), expected_sub_ptr, "Slicing TaskBytes must be zero-copy pointer offset");
    assert_eq!(sub_slice.len(), slice_len);
    println!("  [+] Verified zero-copy sub-slicing pointer offset: {:p} == {:p}", sub_slice.inner().as_ptr(), expected_sub_ptr);

    // Microbenchmark: 100,000 clones of 32MB TaskResult
    // An O(1) Arc refcount clone takes ~10-30 nanoseconds per iteration.
    // If it were deep copying 32MB, 100,000 copies would copy 3.2 Terabytes and take minutes!
    let iterations = 100_000;
    let t_start = Instant::now();
    let mut sink_bytes = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        sink_bytes.push(result.clone());
    }
    let total_elapsed = t_start.elapsed();
    let ns_per_clone = total_elapsed.as_nanos() as f64 / iterations as f64;

    println!("  [+] Benchmark: 100,000 clones of 32MB TaskResult took {:?}", total_elapsed);
    println!("  [+] Average time per clone: {:.2} ns (threshold: < 500 ns)", ns_per_clone);

    assert!(
        ns_per_clone < 500.0,
        "Clone must be O(1) Arc atomic increment (< 500ns), actual was {:.2}ns",
        ns_per_clone
    );
    assert_eq!(sink_bytes.len(), iterations);
}

// =========================================================================
// 6. Adversarial Wire Tag Corruption & Truncated Frame Handling
// =========================================================================

#[test]
fn test_adv_06_corrupted_wire_tags_and_truncation() {
    println!("\n=== [ADV-06] Corrupted Wire Tags, Truncated Binary Frames, & Error Resilience ===");

    // Test A: Invalid discriminator tags
    let invalid_tags = vec![0x00, 0x03, 0x04, 0x10, 0x7F, 0xAA, 0xFE, 0xFF];
    for tag in invalid_tags {
        let corrupted_frame = vec![tag, 0x01, 0x02, 0x03];
        let result = deserialize_message::<WorkerMessage>(&corrupted_frame);
        assert!(result.is_err(), "Tag 0x{:02x} must be rejected", tag);
        match result.unwrap_err() {
            ProtocolError::InvalidFormatTag(t) => assert_eq!(t, tag),
            other => panic!("Expected ProtocolError::InvalidFormatTag, got {:?}", other),
        }
    }
    println!("  [+] All 8 corrupted wire format discriminator bytes cleanly rejected with InvalidFormatTag.");

    // Test B: Truncated Bincode binary frames
    // Take a valid serialized message
    let valid_spec = Generative3DSpec::new_prompt("robot model").with_quality("high");
    let valid_bincode = serialize_message(&valid_spec, WireCodec::Bincode).expect("Serialize valid spec");
    assert!(valid_bincode.len() > 10);

    // Truncate at various critical boundaries:
    let truncation_points = vec![
        1,  // Only discriminator tag (0x02), 0 payload bytes
        2,  // Tag + 1 byte
        5,  // Tag + partial string length
        valid_bincode.len() / 2, // Halfway through payload
        valid_bincode.len() - 1, // Cut last byte
    ];

    for trunc_len in truncation_points {
        let truncated = &valid_bincode[0..trunc_len];
        let result = deserialize_message::<Generative3DSpec>(truncated);
        assert!(result.is_err(), "Truncated frame at length {} must return error", trunc_len);
        match result.unwrap_err() {
            ProtocolError::Bincode(_) => {} // Clean Bincode error, no panic!
            other => panic!("Expected ProtocolError::Bincode, got {:?}", other),
        }
    }
    println!("  [+] Truncated binary frames safely returned ProtocolError::Bincode without crashing.");

    // Test C: Corrupted JSON strings and malicious tag strings
    let malformed_json_cases = vec![
        (r#"{"type":"NonExistentVariant","prompt":"test"}"#, "unknown variant"),
        (r#"{"type":12345,"prompt":"test"}"#, "invalid type tag"),
        (r#"{"type":null,"prompt":"test"}"#, "null type tag"),
        (r#"{"type":"Generative3D","quality":12345}"#, "type mismatch in field"),
        (r#"{"type":"Generative3D","prompt": "#, "unclosed JSON syntax"),
        (r#"{"type":"Generative3D","export_texture":"not_a_bool"}"#, "boolean mismatch"),
    ];

    for (json_case, desc) in malformed_json_cases {
        let frame = format!("{}{}", WIRE_FORMAT_JSON as char, json_case);
        let result = deserialize_message::<TaskSpec>(frame.as_bytes());
        assert!(result.is_err(), "Malformed JSON case '{}' must fail", desc);
        match result.unwrap_err() {
            ProtocolError::Json(_) => {} // Clean JSON error
            other => panic!("Expected ProtocolError::Json for '{}', got {:?}", desc, other),
        }
    }
    println!("  [+] Malformed JSON tag strings and syntax errors safely returned ProtocolError::Json.");

    // Test D: Oversized frame rejection by LengthDelimitedCodec (> 64 MB)
    let mut codec = default_codec();
    let mut oversized_buffer = BytesMut::new();
    // Encode a frame length of 65 MB (65 * 1024 * 1024 = 68,157,440 bytes)
    let oversized_len: u32 = (MAX_FRAME_SIZE + 1024 * 1024) as u32;
    oversized_buffer.extend_from_slice(&oversized_len.to_be_bytes());
    // Add dummy byte
    oversized_buffer.extend_from_slice(&[0x02]);

    let decode_result = codec.decode(&mut oversized_buffer);
    assert!(decode_result.is_err(), "LengthDelimitedCodec must reject frame > 64MB");
    let err = decode_result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "Expected InvalidData error for frame too large");
    println!("  [+] LengthDelimitedCodec strictly rejects frames > 64MB with std::io::ErrorKind::InvalidData.");
}

// =========================================================================
// 7. Full Task Spec Interchange, Requirements, & Checkpoint Data
// =========================================================================

#[test]
fn test_adv_07_full_task_spec_interchange_and_checkpoint_zero_copy() {
    println!("\n=== [ADV-07] Full Task Interchange, Requirements Validation, & Checkpoint Payloads ===");

    // Construct Generative3D Task
    let gen_spec = Generative3DSpec::new_prompt("cybernetic dragon")
        .with_quality("high")
        .with_engine("triposr");
    let reqs_gpu = TaskRequirements::gpu(300);

    let task_3d = Task::new(TaskSpec::Generative3D(gen_spec), reqs_gpu)
        .with_tags(vec!["3d".to_string(), "gen".to_string(), "gpu".to_string()]);

    // Validation must pass because TripoSR requires GPU and requirements say gpu_required = true
    assert!(task_3d.validate().is_ok());

    // Mismatched requirement validation check:
    let task_3d_mismatched = Task::new(
        TaskSpec::Generative3D(Generative3DSpec::new_prompt("dragon").with_engine("triposr")),
        TaskRequirements::generic(4, 60), // gpu_required = false
    );
    let mismatch_err = task_3d_mismatched.validate();
    assert!(mismatch_err.is_err(), "Mismatched GPU requirements must fail validation");
    println!("  [+] GPU requirement mismatch correctly caught: {:?}", mismatch_err.unwrap_err());

    // Attach 1MB CheckpointData to task
    let checkpoint_bytes = TaskBytes::from(vec![0xAA; 1024 * 1024]);
    let ptr_checkpoint_orig = checkpoint_bytes.inner().as_ptr();
    let checkpoint = CheckpointData::new(42, checkpoint_bytes);

    let task_with_checkpoint = task_3d.with_checkpoint(checkpoint);
    assert_eq!(
        task_with_checkpoint.latest_checkpoint.as_ref().unwrap().delta_state.inner().as_ptr(),
        ptr_checkpoint_orig,
        "CheckpointData delta_state must be zero-copy pointer"
    );

    // Bincode roundtrip of Task with Checkpoint
    let bin_task = bincode::serialize(&task_with_checkpoint).expect("Serialize Task with Checkpoint");
    let deser_task: Task = bincode::deserialize(&bin_task).expect("Deserialize Task with Checkpoint");

    assert_eq!(deser_task.id, task_with_checkpoint.id);
    assert_eq!(deser_task.tags, vec!["3d", "gen", "gpu"]);
    assert!(deser_task.latest_checkpoint.is_some());
    let ck = deser_task.latest_checkpoint.unwrap();
    assert_eq!(ck.sequence, 42);
    assert_eq!(ck.delta_state.len(), 1024 * 1024);
    assert_eq!(ck.delta_state.as_slice()[0], 0xAA);
    assert_eq!(ck.delta_state.as_slice()[1024 * 1024 - 1], 0xAA);

    println!("  [+] Full Task with 1MB CheckpointData successfully serialized, validated, and restored.");
}

// =========================================================================
// 8. Concurrent Multi-Threaded Zero-Copy Buffer Access
// =========================================================================

#[test]
fn test_adv_08_concurrent_multithreaded_zero_copy_access() {
    println!("\n=== [ADV-08] Concurrent Multi-Threaded Zero-Copy Buffer Access ===");

    let payload_size = 32 * 1024 * 1024;
    let glb_bytes = create_simulated_glb(payload_size);
    let original_bytes = TaskBytes::from(glb_bytes);
    let original_ptr = original_bytes.inner().as_ptr() as usize;

    let shared_result = Arc::new(TaskResult {
        worker_id: Uuid::new_v4(),
        task_id: TaskId::new(),
        exit_code: 0,
        stdout: TaskBytes::from("parallel access test"),
        stderr: TaskBytes::new(),
        execution_time_ms: 1000,
        is_gpu_executed: true,
        device_name: Some("Multi-Threaded Bus".to_string()),
        error: None,
        asset_bytes: Some(original_bytes.clone()),
    });

    let num_threads = 8;
    let mut handles = Vec::with_capacity(num_threads);

    for t_idx in 0..num_threads {
        let res_clone = Arc::clone(&shared_result);
        let orig_clone = original_bytes.clone();
        handles.push(std::thread::spawn(move || {
            let asset = res_clone.asset_bytes.as_ref().unwrap();
            let thread_ptr = asset.inner().as_ptr() as usize;
            assert_eq!(thread_ptr, original_ptr, "Thread {} must see identical pointer", t_idx);

            // Read samples concurrently from multiple offsets
            let offset_1 = t_idx * 1024 * 1024;
            let offset_2 = offset_1 + 512 * 1024;
            let b1 = asset[offset_1];
            let b2 = asset[offset_2];
            assert_eq!(b1, orig_clone[offset_1], "Thread {} offset {} mismatch", t_idx, offset_1);
            assert_eq!(b2, orig_clone[offset_2], "Thread {} offset {} mismatch", t_idx, offset_2);

            // Sub-slice check inside thread
            let slice = asset.slice(offset_1..offset_2);
            assert_eq!(slice.len(), 512 * 1024);
            assert_eq!(slice.inner().as_ptr() as usize, original_ptr + offset_1);
        }));
    }

    for h in handles {
        h.join().expect("Worker thread panicked during concurrent buffer access");
    }
    println!("  [+] Successfully validated 8 concurrent threads sharing 32MB zero-copy buffer with zero data races.");
}

// =========================================================================
// 9. Bincode Invalid Enum Discriminator Indices
// =========================================================================

#[test]
fn test_adv_09_bincode_invalid_enum_indices() {
    println!("\n=== [ADV-09] Bincode Invalid Enum Discriminator Indices ===");

    // Construct raw bytes with out-of-range u32 enum variant indices (e.g. variant 9999)
    let mut bogus_bincode = Vec::new();
    bogus_bincode.extend_from_slice(&9999u32.to_le_bytes()); // Invalid variant index for TaskSpec
    bogus_bincode.extend_from_slice(b"arbitrary trailing payload data");

    let res = bincode::deserialize::<TaskSpec>(&bogus_bincode);
    assert!(res.is_err(), "Bogus enum variant index must fail deserialization");
    println!("  [+] Invalid TaskSpec enum variant index cleanly rejected: {:?}", res.unwrap_err());

    let mut bogus_worker_msg = Vec::new();
    bogus_worker_msg.extend_from_slice(&8888u32.to_le_bytes()); // Invalid variant index for WorkerMessage
    bogus_worker_msg.extend_from_slice(b"arbitrary payload");

    let res_msg = bincode::deserialize::<WorkerMessage>(&bogus_worker_msg);
    assert!(res_msg.is_err(), "Bogus WorkerMessage variant index must fail deserialization");
    println!("  [+] Invalid WorkerMessage variant index cleanly rejected: {:?}", res_msg.unwrap_err());
}
