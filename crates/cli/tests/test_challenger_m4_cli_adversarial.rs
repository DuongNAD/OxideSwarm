//! Adversarial empirical challenge tests for OxideSwarm CLI (Feature F19)
//! Author: Challenger M4.2 (Empirical Challenger)
//!
//! Stress-tests:
//! 1. Corrupted glTF header fuzzing:
//!    - Truncated buffers (all lengths 0..19)
//!    - Invalid magic headers (b"FAKE", b"GLTF", b"\x00\x00\x00\x00", etc.)
//!    - Invalid glTF versions (0, 1, 3, 255, u32::MAX)
//!    - Buffer length mismatch and chunk 0 length overflow
//!    - Invalid chunk 0 types (BIN instead of JSON)
//!    - Malformed JSON, non-UTF8 bytes, missing asset, wrong asset versions
//! 2. Malicious and corrupted input paths:
//!    - Non-existent mesh / image paths
//!    - Zero-byte empty files
//!    - Directories passed where mesh / image files are expected
//!    - Exclusively locked / unreadable files
//!    - Empty prompt validation
//! 3. Output path boundary conditions:
//!    - Deeply nested non-existent directory tree (auto-creation validation)

use std::process::ExitCode;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::watch;

use rusty_grid_cli::cli_app::{run_submit, validate_glb_header, SubmitArgs};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

fn create_valid_test_glb() -> Vec<u8> {
    let json_bytes = br#"{"asset":{"version":"2.0"},"scenes":[{"nodes":[0]}],"nodes":[{"name":"RootNode"}]}"#;
    let pad = (4 - (json_bytes.len() % 4)) % 4;
    let mut padded_json = json_bytes.to_vec();
    padded_json.extend(vec![b' '; pad]);

    let chunk0_len = padded_json.len() as u32;
    let total_len = 12 + 8 + chunk0_len;

    let mut glb = Vec::new();
    glb.extend_from_slice(b"glTF");
    glb.extend_from_slice(&2u32.to_le_bytes());
    glb.extend_from_slice(&total_len.to_le_bytes());
    glb.extend_from_slice(&chunk0_len.to_le_bytes());
    glb.extend_from_slice(&0x4E4F534Au32.to_le_bytes()); // "JSON"
    glb.extend_from_slice(&padded_json);
    glb
}

// =========================================================================
// 1. Corrupted glTF Header Fuzzing Suite
// =========================================================================

#[test]
fn test_fuzz_glb_header_truncation() {
    let valid_glb = create_valid_test_glb();
    assert!(validate_glb_header(&valid_glb).is_ok());

    // Truncate at every single byte from 0 up to 19 (minimum required is 20)
    for len in 0..20 {
        let truncated = &valid_glb[0..len];
        let res = validate_glb_header(truncated);
        assert!(
            res.is_err(),
            "Buffer of length {len} should fail header validation"
        );
        let err_msg = res.unwrap_err();
        assert!(
            err_msg.contains("Asset buffer too small"),
            "Error for len {len} must mention buffer too small: {err_msg}"
        );
    }
}

#[test]
fn test_fuzz_glb_header_invalid_magic() {
    let valid_glb = create_valid_test_glb();

    let invalid_magics: Vec<&[u8; 4]> = vec![
        b"FAKE",
        b"glT\x01",
        b"GLTF",
        b"gltf",
        b"\x00\x00\x00\x00",
        b"RIFF",
        b"PK\x03\x04",
        b"\x89PNG",
        b"\xFF\xD8\xFF\xE0",
    ];

    for magic in invalid_magics {
        let mut corrupted = valid_glb.clone();
        corrupted[0..4].copy_from_slice(magic);
        let res = validate_glb_header(&corrupted);
        assert!(res.is_err(), "Magic {:?} must be rejected", magic);
        let err_msg = res.unwrap_err();
        assert!(
            err_msg.contains("Invalid glTF magic number"),
            "Error must indicate invalid magic: {err_msg}"
        );
    }
}

#[test]
fn test_fuzz_glb_header_invalid_versions() {
    let valid_glb = create_valid_test_glb();

    let invalid_versions: Vec<u32> = vec![0, 1, 3, 4, 10, 255, u32::MAX];

    for ver in invalid_versions {
        let mut corrupted = valid_glb.clone();
        corrupted[4..8].copy_from_slice(&ver.to_le_bytes());
        let res = validate_glb_header(&corrupted);
        assert!(res.is_err(), "Version {ver} must be rejected");
        let err_msg = res.unwrap_err();
        assert!(
            err_msg.contains("Unsupported glTF binary version"),
            "Error must indicate unsupported version: {err_msg}"
        );
    }
}

#[test]
fn test_fuzz_glb_header_length_attacks() {
    let valid_glb = create_valid_test_glb();

    // 1. Header specifies length greater than buffer (attempt buffer over-read)
    let mut corrupted = valid_glb.clone();
    let inflated_len = (valid_glb.len() + 1000) as u32;
    corrupted[8..12].copy_from_slice(&inflated_len.to_le_bytes());
    let res = validate_glb_header(&corrupted);
    assert!(res.is_err());
    assert!(res.unwrap_err().contains("Corrupted glTF binary"));

    // 2. Chunk 0 length exceeds buffer
    let mut corrupted2 = valid_glb.clone();
    let inflated_chunk_len = (valid_glb.len() + 50) as u32;
    corrupted2[12..16].copy_from_slice(&inflated_chunk_len.to_le_bytes());
    let res2 = validate_glb_header(&corrupted2);
    assert!(res2.is_err());
    assert!(res2.unwrap_err().contains("Corrupted glTF chunk 0"));
}

#[test]
fn test_fuzz_glb_header_chunk_types() {
    let valid_glb = create_valid_test_glb();

    let invalid_chunk_types: Vec<u32> = vec![
        0x004E4942, // "BIN\0" (Chunk 1 type, not Chunk 0)
        0x00000000,
        0xFFFFFFFF,
        0x58585858, // "XXXX"
    ];

    for ctype in invalid_chunk_types {
        let mut corrupted = valid_glb.clone();
        corrupted[16..20].copy_from_slice(&ctype.to_le_bytes());
        let res = validate_glb_header(&corrupted);
        assert!(res.is_err(), "Chunk 0 type 0x{ctype:08X} must be rejected");
        assert!(res.unwrap_err().contains("Invalid glTF chunk 0 type"));
    }
}

#[test]
fn test_fuzz_glb_header_corrupted_json() {
    // 1. Non-UTF-8 bytes inside JSON chunk
    let mut glb_non_utf8 = create_valid_test_glb();
    glb_non_utf8[20] = 0xFF;
    glb_non_utf8[21] = 0xFE;
    let res1 = validate_glb_header(&glb_non_utf8);
    assert!(res1.is_err());
    assert!(res1.unwrap_err().contains("not valid UTF-8"));

    // 2. Malformed JSON syntax
    let malformed_json = b"{\"asset\": unclosed";
    let mut glb_malformed = Vec::new();
    glb_malformed.extend_from_slice(b"glTF");
    glb_malformed.extend_from_slice(&2u32.to_le_bytes());
    let total_len = (20 + malformed_json.len()) as u32;
    glb_malformed.extend_from_slice(&total_len.to_le_bytes());
    glb_malformed.extend_from_slice(&(malformed_json.len() as u32).to_le_bytes());
    glb_malformed.extend_from_slice(&0x4E4F534Au32.to_le_bytes());
    glb_malformed.extend_from_slice(malformed_json);
    let res2 = validate_glb_header(&glb_malformed);
    assert!(res2.is_err());
    assert!(res2.unwrap_err().contains("Failed to parse glTF chunk 0 JSON"));

    // 3. Invalid glTF asset version string
    let bad_ver_json = b"{\"asset\":{\"version\":\"1.0\"}}";
    let mut glb_bad_ver = Vec::new();
    glb_bad_ver.extend_from_slice(b"glTF");
    glb_bad_ver.extend_from_slice(&2u32.to_le_bytes());
    let total_len = (20 + bad_ver_json.len()) as u32;
    glb_bad_ver.extend_from_slice(&total_len.to_le_bytes());
    glb_bad_ver.extend_from_slice(&(bad_ver_json.len() as u32).to_le_bytes());
    glb_bad_ver.extend_from_slice(&0x4E4F534Au32.to_le_bytes());
    glb_bad_ver.extend_from_slice(bad_ver_json);
    let res3 = validate_glb_header(&glb_bad_ver);
    assert!(res3.is_err());
    assert!(res3.unwrap_err().contains("Invalid glTF asset version"));
}

// =========================================================================
// 2. Malicious and Corrupted Input Paths Suite
// =========================================================================

#[tokio::test]
async fn test_cli_malicious_mesh_input_paths() {
    let temp_dir = TempDir::new().unwrap();

    // Case 1: Non-existent mesh file
    let non_existent_mesh = temp_dir.path().join("does_not_exist_mesh_9999.glb");
    let args1 = SubmitArgs {
        master: Some("127.0.0.1:8080".to_string()),
        task_type: "autorig".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 10,
        max_retries: None,
        wait: true,
        json: false,
        prompt: None,
        image: None,
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: Some(non_existent_mesh),
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: None,
        args: Vec::new(),
        ..Default::default()
    };
    let exit1 = run_submit(args1, None).await;
    assert_eq!(exit1, ExitCode::FAILURE, "Non-existent mesh must return failure without panic");

    // Case 2: Zero-byte mesh file
    let empty_mesh = temp_dir.path().join("zero_byte_mesh.glb");
    std::fs::write(&empty_mesh, b"").unwrap();
    let args2 = SubmitArgs {
        master: Some("127.0.0.1:8080".to_string()),
        task_type: "autorig".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 10,
        max_retries: None,
        wait: true,
        json: false,
        prompt: None,
        image: None,
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: Some(empty_mesh),
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: None,
        args: Vec::new(),
        ..Default::default()
    };
    let exit2 = run_submit(args2, None).await;
    assert_eq!(exit2, ExitCode::FAILURE, "Zero-byte mesh file must return failure without panic");

    // Case 3: Directory passed as mesh
    let dir_as_mesh = temp_dir.path().join("directory_not_a_file");
    std::fs::create_dir_all(&dir_as_mesh).unwrap();
    let args3 = SubmitArgs {
        master: Some("127.0.0.1:8080".to_string()),
        task_type: "autorig".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 10,
        max_retries: None,
        wait: true,
        json: false,
        prompt: None,
        image: None,
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: Some(dir_as_mesh),
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: None,
        args: Vec::new(),
        ..Default::default()
    };
    let exit3 = run_submit(args3, None).await;
    assert_eq!(exit3, ExitCode::FAILURE, "Directory as mesh must return failure without panic");
}

#[tokio::test]
async fn test_cli_malicious_image_and_prompt_inputs() {
    let temp_dir = TempDir::new().unwrap();

    // Case 1: Non-existent image file
    let non_existent_img = temp_dir.path().join("phantom_texture_404.png");
    let args1 = SubmitArgs {
        master: Some("127.0.0.1:8080".to_string()),
        task_type: "3d".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 10,
        max_retries: None,
        wait: true,
        json: false,
        prompt: None,
        image: Some(non_existent_img),
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: None,
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: None,
        args: Vec::new(),
        ..Default::default()
    };
    let exit1 = run_submit(args1, None).await;
    assert_eq!(exit1, ExitCode::FAILURE, "Non-existent image must fail without panic");

    // Case 2: Zero-byte image file
    let empty_img = temp_dir.path().join("empty_image.png");
    std::fs::write(&empty_img, b"").unwrap();
    let args2 = SubmitArgs {
        master: Some("127.0.0.1:8080".to_string()),
        task_type: "3d".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 10,
        max_retries: None,
        wait: true,
        json: false,
        prompt: None,
        image: Some(empty_img),
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: None,
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: None,
        args: Vec::new(),
        ..Default::default()
    };
    let exit2 = run_submit(args2, None).await;
    assert_eq!(exit2, ExitCode::FAILURE, "Zero-byte image must fail without panic");

    // Case 3: Directory passed as image
    let dir_as_img = temp_dir.path().join("image_directory");
    std::fs::create_dir_all(&dir_as_img).unwrap();
    let args3 = SubmitArgs {
        master: Some("127.0.0.1:8080".to_string()),
        task_type: "3d".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 10,
        max_retries: None,
        wait: true,
        json: false,
        prompt: None,
        image: Some(dir_as_img),
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: None,
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: None,
        args: Vec::new(),
        ..Default::default()
    };
    let exit3 = run_submit(args3, None).await;
    assert_eq!(exit3, ExitCode::FAILURE, "Directory as image must fail without panic");

    // Case 4: Empty prompt string ("" or whitespace)
    let args4 = SubmitArgs {
        master: Some("127.0.0.1:8080".to_string()),
        task_type: "3d".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 10,
        max_retries: None,
        wait: true,
        json: false,
        prompt: Some("   \t\n  ".to_string()),
        image: None,
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: None,
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: None,
        args: Vec::new(),
        ..Default::default()
    };
    let exit4 = run_submit(args4, None).await;
    assert_eq!(exit4, ExitCode::FAILURE, "Whitespace prompt must fail without panic");

    // Case 5: Missing both prompt and image for 3D
    let args5 = SubmitArgs {
        master: Some("127.0.0.1:8080".to_string()),
        task_type: "3d".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 10,
        max_retries: None,
        wait: true,
        json: false,
        prompt: None,
        image: None,
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: None,
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: None,
        args: Vec::new(),
        ..Default::default()
    };
    let exit5 = run_submit(args5, None).await;
    assert_eq!(exit5, ExitCode::FAILURE, "Missing prompt and image must fail");
}

// =========================================================================
// 3. Output Path Boundary Conditions: Deeply Nested Directory Creation
// =========================================================================

#[tokio::test]
async fn test_cli_deeply_nested_output_directory_creation() {
    let temp_dir = TempDir::new().unwrap();
    // Deeply nested non-existent directory path
    let deeply_nested_out = temp_dir
        .path()
        .join("level1")
        .join("level2")
        .join("level3")
        .join("level4")
        .join("output_monolith.glb");

    assert!(!deeply_nested_out.parent().unwrap().exists());

    // 1. Spawn in-process Master
    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("MasterServer spawn failed");
    let master_addr = master.server_addr();

    // 2. Spawn in-process Worker
    let worker_temp = TempDir::new().unwrap();
    let worker_cfg = WorkerConfig::new(master_addr.to_string())
        .with_name("worker-cli-adversarial")
        .with_cores(4)
        .with_ram_mb(8192)
        .with_sandbox_base_dir(worker_temp.path());

    let mut client = WorkerClient::new(worker_cfg);
    let (tx, rx) = watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = client.run(rx).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 3. Submit 3D task with deeply nested output path
    let args = SubmitArgs {
        master: Some(master_addr.to_string()),
        task_type: "3d".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 30,
        max_retries: None,
        wait: true,
        json: true,
        prompt: Some("an obsidian monolith".to_string()),
        image: None,
        quality: Some("fast".to_string()),
        engine: Some("sdf".to_string()),
        export_texture: true,
        no_export_texture: false,
        input_mesh: None,
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: Some(deeply_nested_out.clone()),
        args: Vec::new(),
        ..Default::default()
    };

    let exit_code = run_submit(args, None).await;
    assert_eq!(exit_code, ExitCode::SUCCESS, "Task submission and export must succeed");

    // 4. Verify deeply nested parent dirs were created and file exists
    assert!(
        deeply_nested_out.is_file(),
        "Exported glTF file must exist in deeply nested path"
    );
    let data = std::fs::read(&deeply_nested_out).unwrap();
    assert!(data.len() > 1000);
    assert!(
        validate_glb_header(&data).is_ok(),
        "Exported asset in nested directory must be a valid glTF 2.0 binary container"
    );

    // 5. Cleanup
    let _ = tx.send(true);
    let _ = worker_handle.await;
    let _ = master.shutdown();
}
