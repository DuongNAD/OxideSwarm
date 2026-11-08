//! Blender Subprocess Supervision and Process Containment Test Suite.
//!
//! Verifies Blender 3D mesh auto-rigging workloads executed through `TaskRunner`:
//! 1. Host Blender headless execution exporting rigged `.glb` assets.
//! 2. Supervised process tree termination on mid-task cancellation without orphaned processes.
//! 3. Graceful fallback to pure-Rust auto-rigging when Blender is missing or exits abnormally.

#![allow(clippy::field_reassign_with_default)]

use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot};

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{
    AutoRigSpec, Bytes, Generative3DSpec, Task, TaskId, TaskRequirements, TaskSpec,
};
use rusty_grid_worker::autorig::{
    auto_rig_mesh, auto_rig_mesh_async, blender_provider, load_mesh_from_bytes, BlenderRigProvider,
};
use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::{RunnerConfig, TaskRunner, EXIT_CODE_CANCELLED};

mod common;

use common::{
    assert_all_alive, assert_all_dead_async, blender_binary_or_skip, command_in_own_process_group,
    create_fake_blender_executable, get_python_exe, poll_pids_async, ScopedEnvVar, ENV_MUTEX,
};

/// Helper to create a standard test TaskRunner.
fn create_test_runner() -> (TaskRunner, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let sandbox_config = SandboxConfig::new(temp_dir.path().to_path_buf());
    let runner_config = RunnerConfig::new(sandbox_config);
    let mut caps = WorkerCapabilities::new("challenger_m3_2_worker", 4, 8192, false, false, None);
    caps.has_blender = true;
    caps.blender_version = Some("4.5.4 LTS".to_string());
    let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, runner_config);
    (runner, temp_dir)
}

/// Generates a test procedural humanoid GLB mesh using the Tier-0 SDF engine.
fn generate_test_glb_mesh() -> Vec<u8> {
    let gen_spec = Generative3DSpec::new_prompt("adversarial biped test mesh")
        .with_quality("fast")
        .with_engine("sdf");

    let (gen_glb, stats) = rusty_grid_worker::gen3d::generate_procedural_3d(&gen_spec)
        .expect("Procedural SDF generation should succeed");
    assert!(
        stats.vertex_count > 30,
        "SDF mesh must have sufficient vertices"
    );
    assert_eq!(
        &gen_glb[0..4],
        b"glTF",
        "SDF mesh must be a valid GLB container"
    );
    gen_glb
}

/// Validates standard glTF 2.0 Binary structure and extracts parsed JSON.
fn validate_and_parse_glb(glb_bytes: &[u8]) -> serde_json::Value {
    assert!(
        glb_bytes.len() >= 20,
        "GLB binary length ({}) too small for glTF 2.0 header",
        glb_bytes.len()
    );
    // Magic: 0x46546C67 ("glTF")
    assert_eq!(&glb_bytes[0..4], b"glTF", "Invalid glTF 2.0 magic header");
    // Version: 2
    let version = u32::from_le_bytes(glb_bytes[4..8].try_into().unwrap());
    assert_eq!(version, 2, "glTF version must be 2, got {version}");
    let total_len = u32::from_le_bytes(glb_bytes[8..12].try_into().unwrap()) as usize;
    assert_eq!(
        total_len,
        glb_bytes.len(),
        "glTF total length header mismatch"
    );

    // Chunk 0: JSON
    let json_chunk_len = u32::from_le_bytes(glb_bytes[12..16].try_into().unwrap()) as usize;
    let json_chunk_type = u32::from_le_bytes(glb_bytes[16..20].try_into().unwrap());
    assert_eq!(
        json_chunk_type, 0x4E4F534A,
        "Chunk 0 must be JSON (0x4E4F534A)"
    );

    let json_bytes = &glb_bytes[20..20 + json_chunk_len];
    let json_str = std::str::from_utf8(json_bytes).expect("glTF JSON chunk is not valid UTF-8");
    let json_val: serde_json::Value =
        serde_json::from_str(json_str).expect("glTF JSON chunk is not valid JSON");

    json_val
}

// ============================================================================
// SUITE 1: Host Blender Headless Execution Verification
// ============================================================================

#[test]
fn test_verify_host_blender_installation_and_version() {
    let _env_guard = ENV_MUTEX.blocking_lock();
    let Some(resolved) =
        blender_binary_or_skip("test_verify_host_blender_installation_and_version")
    else {
        return;
    };
    println!("Resolved Blender executable: {}", resolved.display());

    // Execute --version and assert valid Blender version output
    let output = command_in_own_process_group(&resolved)
        .arg("--version")
        .output()
        .expect("Failed to execute blender --version");

    assert!(output.status.success(), "blender --version failed");
    let version_str = String::from_utf8_lossy(&output.stdout);
    println!("Blender version output:\n{}", version_str);
    assert!(
        version_str.contains("Blender "),
        "Expected Blender version banner, got:\n{}",
        version_str
    );
}

#[tokio::test]
async fn test_verify_host_blender_headless_autorig_execution_glb() {
    let _env_guard = ENV_MUTEX.lock().await;
    let Some(blender_bin) =
        blender_binary_or_skip("test_verify_host_blender_headless_autorig_execution_glb")
    else {
        return;
    };

    let input_mesh = generate_test_glb_mesh();
    let animations = vec![
        "idle".to_string(),
        "walk".to_string(),
        "turnaround".to_string(),
    ];

    println!(
        "Executing BlenderRigProvider::execute_rigging with host Blender (input size: {} bytes)...",
        input_mesh.len()
    );

    let (runner, _dir) = create_test_runner();
    let task_id = TaskId::new();
    let t0 = Instant::now();
    let result = BlenderRigProvider::execute_rigging(
        &runner,
        task_id,
        &blender_bin,
        &input_mesh,
        "biped",
        &animations,
        60,
    )
    .await;

    let elapsed = t0.elapsed();
    println!("Blender headless auto-rig completed in {:?}", elapsed);

    assert!(
        result.is_ok(),
        "BlenderRigProvider execution failed: {:?}",
        result.err()
    );

    let rigged_glb = result.unwrap();
    println!("Generated rigged .glb size: {} bytes", rigged_glb.len());
    assert!(
        rigged_glb.len() > 2000,
        "Rigged GLB must be a substantive asset (> 2000 bytes)"
    );

    // Deep validation of the glTF 2.0 Binary container
    let gltf_json = validate_and_parse_glb(&rigged_glb);

    // 1. Verify skins
    let skins = gltf_json.get("skins").and_then(|s| s.as_array());
    assert!(
        skins.is_some() && !skins.unwrap().is_empty(),
        "glTF must contain at least one skin binding"
    );
    let skin = &skins.unwrap()[0];
    let joints = skin
        .get("joints")
        .and_then(|j| j.as_array())
        .expect("Skin must have joints array");
    assert!(
        joints.len() >= 10,
        "Rigged mesh must have an armature with at least 10 joints, found {}",
        joints.len()
    );
    assert!(
        skin.get("inverseBindMatrices").is_some(),
        "Skin must have inverseBindMatrices accessor pointer"
    );

    // 2. Verify animations
    let anims = gltf_json.get("animations").and_then(|a| a.as_array());
    assert!(
        anims.is_some(),
        "glTF must contain procedural animations array"
    );
    let anim_list = anims.unwrap();
    assert!(
        !anim_list.is_empty(),
        "glTF must contain at least one exported animation clip"
    );

    // 3. Verify mesh primitive contains JOINTS_0 and WEIGHTS_0
    let meshes = gltf_json
        .get("meshes")
        .and_then(|m| m.as_array())
        .expect("Meshes must exist");
    assert!(!meshes.is_empty());
    let primitives = meshes[0]
        .get("primitives")
        .and_then(|p| p.as_array())
        .expect("Primitives must exist");
    assert!(!primitives.is_empty());
    let attrs = primitives[0]
        .get("attributes")
        .and_then(|a| a.as_object())
        .expect("Attributes must exist");
    assert!(
        attrs.contains_key("POSITION"),
        "Primitive must have POSITION attribute"
    );
    assert!(
        attrs.contains_key("JOINTS_0"),
        "Primitive must have JOINTS_0 attribute for skinning"
    );
    assert!(
        attrs.contains_key("WEIGHTS_0"),
        "Primitive must have WEIGHTS_0 attribute for skinning"
    );
}

#[tokio::test]
async fn test_verify_autorig_async_with_preferred_provider_blender() {
    let _env_guard = ENV_MUTEX.lock().await;
    if blender_binary_or_skip("test_verify_autorig_async_with_preferred_provider_blender").is_none()
    {
        return;
    }

    let input_mesh = generate_test_glb_mesh();
    let spec = AutoRigSpec::new(Bytes::from(input_mesh), "biped")
        .with_animations(vec![
            "idle".to_string(),
            "walk".to_string(),
            "turnaround".to_string(),
        ])
        .with_provider("blender");

    let (runner, _dir) = create_test_runner();
    let t0 = Instant::now();
    let result = auto_rig_mesh_async(&runner, TaskId::new(), &spec, |_, _, _| {}).await;
    let elapsed = t0.elapsed();
    println!(
        "auto_rig_mesh_async with preferred_provider=blender took {:?}",
        elapsed
    );

    assert!(
        result.is_ok(),
        "auto_rig_mesh_async failed with preferred_provider=blender: {:?}",
        result.err()
    );

    let glb_bytes = result.unwrap();
    assert!(glb_bytes.len() > 2000);
    let gltf_json = validate_and_parse_glb(&glb_bytes);
    assert!(gltf_json.get("skins").is_some());
    assert!(gltf_json.get("animations").is_some());
}

#[tokio::test]
async fn test_verify_task_runner_autorig_execution_with_blender() {
    let _env_guard = ENV_MUTEX.lock().await;
    if blender_binary_or_skip("test_verify_task_runner_autorig_execution_with_blender").is_none() {
        return;
    }

    let (runner, _dir) = create_test_runner();
    let input_mesh = generate_test_glb_mesh();

    let spec = AutoRigSpec::new(Bytes::from(input_mesh), "biped")
        .with_animations(vec!["idle".to_string(), "walk".to_string()])
        .with_provider("blender");

    let mut requirements = TaskRequirements::default();
    requirements.timeout_secs = 60;
    let task = Task::new(TaskSpec::AutoRig(spec), requirements);

    let (tx, mut rx) = mpsc::channel(16);

    let t0 = Instant::now();
    let result = runner
        .execute_task_with_checkpoint(&task, None, Some(tx), None, None)
        .await;

    let elapsed = t0.elapsed();
    println!("TaskRunner AutoRig with Blender executed in {:?}", elapsed);

    assert!(
        result.is_success(),
        "TaskRunner execution failed: exit_code={}, error={:?}",
        result.exit_code,
        result.error
    );

    assert!(
        result.asset_bytes.is_some(),
        "asset_bytes must be populated"
    );
    let asset = result.asset_bytes.unwrap();
    assert_eq!(&asset.as_bytes()[0..4], b"glTF");

    // Collect and verify checkpoints
    let mut checkpoints = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let WorkerMessage::Checkpoint {
            sequence,
            delta_state,
            ..
        } = msg
        {
            checkpoints.push((
                sequence,
                String::from_utf8_lossy(delta_state.as_bytes()).to_string(),
            ));
        }
    }
    println!("Received checkpoints: {:?}", checkpoints);
    assert!(checkpoints
        .iter()
        .any(|(seq, msg)| *seq == 1 && msg.contains("analyzing_mesh")));
    assert!(checkpoints
        .iter()
        .any(|(seq, msg)| *seq == 2 && msg.contains("completed")));
}

#[tokio::test]
async fn test_task_runner_autorig_midway_cancellation_cleans_subprocesses() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let pids_file = temp_dir.path().join("pids_cancel.txt");

    // Custom script simulating long auto-rig execution that records leader and grandchild PIDs
    let content = format!(
        r#"
import os, sys, time, subprocess
pid = os.getpid()
with open(r"{}", "a") as f:
    f.write(f"PID:{{pid}}\n")
    f.flush()

# Spawn grandchild
gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open(r"{}", "a") as f:
    f.write(f"PID:{{gc.pid}}\n")
    f.flush()

print("[PROGRESS: 0.10] Analyzing mesh geometry...", flush=True)
time.sleep(30.0)
"#,
        pids_file.display(),
        pids_file.display()
    );

    let fake_blender = create_fake_blender_executable(temp_dir.path(), &content);
    let _env = ScopedEnvVar::set("RUSTY_GRID_BLENDER_PATH", &fake_blender).await;

    let input_mesh = generate_test_glb_mesh();
    let spec = AutoRigSpec::new(Bytes::from(input_mesh), "biped").with_provider("blender");

    let mut requirements = TaskRequirements::default();
    requirements.timeout_secs = 60;
    let task = Task::new(TaskSpec::AutoRig(spec), requirements);

    let (cancel_tx, cancel_rx) = oneshot::channel();
    let child_pid = Arc::new(AtomicU32::new(0));

    let runner = Arc::new(runner);
    let runner_clone = Arc::clone(&runner);
    let child_pid_clone = Arc::clone(&child_pid);

    let task_handle = tokio::spawn(async move {
        runner_clone
            .execute_task_with_checkpoint(&task, None, None, Some(child_pid_clone), Some(cancel_rx))
            .await
    });

    // Wait until both fake Blender and its grandchild have recorded their PIDs
    let pids = poll_pids_async(&pids_file, 2, Duration::from_secs(20)).await;
    assert_eq!(
        pids.len(),
        2,
        "Expected fake Blender leader and grandchild PIDs to be recorded"
    );
    let mut sys = sysinfo::System::new();
    assert_all_alive(&mut sys, &pids);

    // Send cancellation signal
    let _ = cancel_tx.send("Adversarial cancellation midway through auto-rig".to_string());

    let result = task_handle.await.expect("Task runner join failed");
    println!(
        "Cancellation task result: exit_code={}, error={:?}",
        result.exit_code, result.error
    );

    assert_eq!(
        result.exit_code, EXIT_CODE_CANCELLED,
        "Cancelled task must return EXIT_CODE_CANCELLED (130)"
    );
    assert!(!result.is_success());

    // Both the fake Blender process and its grandchild must be dead
    assert_all_dead_async(&pids, Duration::from_secs(4)).await;
}

// ============================================================================
// SUITE 3: Graceful Fallback When Blender is Missing or Invalid
// ============================================================================

#[tokio::test]
async fn test_graceful_fallback_when_blender_path_is_invalid_or_missing() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let missing_blender = temp_dir.path().join("blender_invalid_404.exe");

    // Set BLENDER_PATH to non-existent executable; both the worker resolver and core capability
    // detection must return None / (false, None) even if the host has Blender installed.
    let _env = ScopedEnvVar::set("BLENDER_PATH", &missing_blender).await;
    assert!(
        blender_provider::resolve_blender_binary(None).is_none(),
        "Explicit invalid BLENDER_PATH must not fall through to a host Blender installation in runner"
    );
    assert_eq!(
        rusty_grid_core::capabilities::resolve_blender_binary(None),
        None,
        "Explicit invalid BLENDER_PATH must not fall through in core resolve_blender_binary"
    );
    assert_eq!(
        rusty_grid_core::capabilities::detect_blender(None),
        (false, None),
        "Explicit invalid BLENDER_PATH must not fall through in core detect_blender"
    );

    let input_mesh = generate_test_glb_mesh();
    let spec = AutoRigSpec::new(Bytes::from(input_mesh), "biped")
        .with_animations(vec!["idle".to_string(), "walk".to_string()])
        .with_provider("blender");

    let (runner, _dir) = create_test_runner();
    let t0 = Instant::now();
    // Must NOT panic or fail, but cleanly fallback to built-in pure-Rust rigging pipeline
    let result = auto_rig_mesh_async(&runner, TaskId::new(), &spec, |_, _, _| {}).await;
    let elapsed = t0.elapsed();
    println!("Fallback execution completed in {:?}", elapsed);

    assert!(
        result.is_ok(),
        "auto_rig_mesh_async should cleanly fallback to built-in engine, but returned: {:?}",
        result.err()
    );

    let glb_bytes = result.unwrap();
    assert!(
        glb_bytes.len() > 2000,
        "Fallback rigged GLB must be valid (> 2000 bytes)"
    );

    let gltf_json = validate_and_parse_glb(&glb_bytes);
    assert!(
        gltf_json.get("skins").is_some(),
        "Fallback GLB must contain skin hierarchy"
    );
    assert!(
        gltf_json.get("animations").is_some(),
        "Fallback GLB must contain synthesized animation tracks"
    );

    // Verify it reloads cleanly into RawMesh
    let reloaded = load_mesh_from_bytes(&glb_bytes);
    assert!(
        reloaded.is_ok(),
        "Fallback GLB must reload into RawMesh cleanly"
    );
}

#[tokio::test]
async fn test_runner_and_capabilities_honor_stale_and_valid_blender_path_flag() {
    use rusty_grid_core::capabilities::HardwareOverrides;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let missing_blender = temp_dir.path().join("stale_flag_blender_404.exe");
    let missing_str = missing_blender.to_string_lossy().to_string();

    let env_marker = temp_dir.path().join("env_blender_ran.txt");
    let env_fake_blender = create_crashing_fake_blender(temp_dir.path(), &env_marker, 1);

    let flag_marker = temp_dir.path().join("flag_blender_ran.txt");
    let flag_dir = temp_dir.path().join("flag_dir");
    std::fs::create_dir_all(&flag_dir).expect("flag_dir");
    let flag_fake_blender = create_crashing_fake_blender(&flag_dir, &flag_marker, 1);
    let flag_valid_str = flag_fake_blender.to_string_lossy().to_string();

    // Point BLENDER_PATH at a runnable fake Blender so any accidental fallthrough is caught.
    let _env = ScopedEnvVar::set("BLENDER_PATH", &env_fake_blender).await;

    let input_mesh = generate_test_glb_mesh();
    let spec = AutoRigSpec::new(Bytes::from(input_mesh), "biped")
        .with_animations(vec!["idle".to_string()])
        .with_provider("blender");

    // 1. Stale `--blender-path` flag: capabilities report no Blender, and the runner must NOT
    //    fall through to BLENDER_PATH or host Blender on either `auto_rig_mesh_async` or `execute_task`.
    let stale_caps = WorkerCapabilities::detect_with_overrides(HardwareOverrides {
        blender_path: Some(missing_str.clone()),
        ..Default::default()
    });
    assert!(
        !stale_caps.has_blender && stale_caps.blender_version.is_none(),
        "stale --blender-path must report no Blender in WorkerCapabilities"
    );

    let stale_sandbox_dir = tempfile::tempdir().expect("stale sandbox");
    let stale_runner_cfg = RunnerConfig::new(SandboxConfig::new(stale_sandbox_dir.path()))
        .with_blender_path(Some(missing_str.clone()));
    let stale_runner = TaskRunner::new(uuid::Uuid::new_v4(), stale_caps, stale_runner_cfg);

    assert_eq!(
        blender_provider::resolve_blender_binary(stale_runner.config().blender_path.as_deref()),
        None,
        "stale RunnerConfig.blender_path must resolve to None"
    );

    let direct_res = auto_rig_mesh_async(&stale_runner, TaskId::new(), &spec, |_, _, _| {}).await;
    assert!(direct_res.is_ok(), "stale flag must fallback cleanly");
    assert!(
        !env_marker.exists(),
        "stale --blender-path in auto_rig_mesh_async must NOT invoke fallback BLENDER_PATH binary"
    );

    let task = Task::new(
        TaskSpec::AutoRig(spec.clone()),
        TaskRequirements::generic(1, 30),
    );
    let task_res = stale_runner.execute_task(&task, None, None).await;
    assert_eq!(task_res.exit_code, 0);
    assert!(
        !env_marker.exists(),
        "stale --blender-path in TaskRunner::execute_task must NOT invoke fallback BLENDER_PATH binary"
    );

    // 2. Valid `--blender-path` flag: capabilities report has_blender, and the runner invokes
    //    the flag's binary (not BLENDER_PATH) on both `auto_rig_mesh_async` and `execute_task`.
    let valid_caps = WorkerCapabilities::detect_with_overrides(HardwareOverrides {
        blender_path: Some(flag_valid_str.clone()),
        ..Default::default()
    });
    assert!(
        valid_caps.has_blender && valid_caps.blender_version.is_some(),
        "valid --blender-path must report has_blender in WorkerCapabilities"
    );

    let valid_sandbox_dir = tempfile::tempdir().expect("valid sandbox");
    let valid_runner_cfg = RunnerConfig::new(SandboxConfig::new(valid_sandbox_dir.path()))
        .with_blender_path(Some(flag_valid_str.clone()));
    let valid_runner = TaskRunner::new(uuid::Uuid::new_v4(), valid_caps, valid_runner_cfg);

    assert_eq!(
        blender_provider::resolve_blender_binary(valid_runner.config().blender_path.as_deref()),
        Some(flag_fake_blender.clone()),
        "valid RunnerConfig.blender_path must resolve to the flag path"
    );

    let _ = auto_rig_mesh_async(&valid_runner, TaskId::new(), &spec, |_, _, _| {}).await;
    assert!(
        flag_marker.exists(),
        "valid --blender-path in auto_rig_mesh_async must invoke the flag binary"
    );
    assert!(
        !env_marker.exists(),
        "valid --blender-path must take precedence over BLENDER_PATH"
    );

    std::fs::remove_file(&flag_marker).expect("remove flag_marker");
    let task2 = Task::new(TaskSpec::AutoRig(spec), TaskRequirements::generic(1, 30));
    let task2_res = valid_runner.execute_task(&task2, None, None).await;
    assert_eq!(task2_res.exit_code, 0);
    assert!(
        flag_marker.exists(),
        "valid --blender-path in TaskRunner::execute_task must invoke the flag binary"
    );
    assert!(
        !env_marker.exists(),
        "valid --blender-path in TaskRunner::execute_task must take precedence over BLENDER_PATH"
    );
}

fn create_crashing_fake_blender(
    dir: &std::path::Path,
    marker_path: &std::path::Path,
    exit_code: i32,
) -> std::path::PathBuf {
    let logic_path = dir.join(format!("fake_crash_{}.py", exit_code));
    let marker_str = marker_path.display().to_string().replace('\\', "/");
    let script = format!(
        "import sys\nwith open(\"{marker_str}\", \"w\") as f:\n    f.write(\"ran\")\nprint(\"[ERROR] Simulated crash\", file=sys.stderr)\nsys.exit({exit_code})\n"
    );
    std::fs::write(&logic_path, script).expect("write logic");

    let py_exe = get_python_exe();

    #[cfg(windows)]
    {
        let cmd_path = dir.join(format!("fake_blender_{}.cmd", exit_code));
        let content = format!(
            "@echo off\r\n\"{}\" \"{}\" %*\r\n",
            py_exe.display(),
            logic_path.display()
        );
        std::fs::write(&cmd_path, content).expect("write .cmd");
        cmd_path
    }

    #[cfg(not(windows))]
    {
        use std::os::unix::fs::PermissionsExt;
        let sh_path = dir.join(format!("fake_blender_{}.sh", exit_code));
        let content = format!(
            "#!/bin/sh\nexec \"{}\" \"{}\" \"$@\"\n",
            py_exe.display(),
            logic_path.display()
        );
        std::fs::write(&sh_path, content).expect("write .sh");
        let _ = std::fs::set_permissions(&sh_path, std::fs::Permissions::from_mode(0o755));
        sh_path
    }
}

#[tokio::test]
async fn test_graceful_fallback_when_blender_binary_crashes() {
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let marker_file = temp_dir.path().join("crash_marker.txt");
    let fake_blender = create_crashing_fake_blender(temp_dir.path(), &marker_file, 1);

    // Point BLENDER_PATH to the crashing binary
    let _env = ScopedEnvVar::set("BLENDER_PATH", &fake_blender).await;

    let input_mesh = generate_test_glb_mesh();
    let spec = AutoRigSpec::new(Bytes::from(input_mesh), "biped")
        .with_animations(vec!["idle".to_string()])
        .with_provider("blender");

    let (runner, _dir) = create_test_runner();
    let t0 = Instant::now();
    // Must gracefully catch failure and fallback to built-in pure-Rust engine
    let result = auto_rig_mesh_async(&runner, TaskId::new(), &spec, |_, _, _| {}).await;
    let elapsed = t0.elapsed();
    println!("Crash fallback completed in {:?}", elapsed);

    assert!(
        marker_file.exists(),
        "Fake Blender executable must have executed and written the marker file"
    );
    assert!(
        result.is_ok(),
        "auto_rig_mesh_async failed to fallback after Blender crash: {:?}",
        result.err()
    );

    let glb_bytes = result.unwrap();
    assert!(glb_bytes.len() > 2000);
    let gltf_json = validate_and_parse_glb(&glb_bytes);
    assert!(gltf_json.get("skins").is_some());
}

#[tokio::test]
async fn test_graceful_fallback_via_task_runner_on_blender_failure() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let marker_file = temp_dir.path().join("failure_marker.txt");
    let fake_blender = create_crashing_fake_blender(temp_dir.path(), &marker_file, 42);

    let _env = ScopedEnvVar::set("BLENDER_PATH", &fake_blender).await;

    let input_mesh = generate_test_glb_mesh();
    let spec = AutoRigSpec::new(Bytes::from(input_mesh), "biped")
        .with_animations(vec!["idle".to_string()])
        .with_provider("blender");

    let mut requirements = TaskRequirements::default();
    requirements.timeout_secs = 30;
    let task = Task::new(TaskSpec::AutoRig(spec), requirements);

    let result = runner
        .execute_task_with_checkpoint(&task, None, None, None, None)
        .await;

    assert!(
        marker_file.exists(),
        "Fake Blender executable must have executed and written the marker file"
    );
    assert!(
        result.is_success(),
        "TaskRunner should succeed via fallback when Blender fails: {:?}",
        result.error
    );
    assert!(result.asset_bytes.is_some());
    let glb = result.asset_bytes.unwrap();
    assert_eq!(&glb.as_bytes()[0..4], b"glTF");
}

#[test]
fn test_adversarial_corrupt_mesh_input_error_reporting() {
    // 1. Completely empty bytes
    let empty_spec = AutoRigSpec::new(Bytes::new(), "biped");
    let err_empty = auto_rig_mesh(&empty_spec);
    assert!(err_empty.is_err(), "Empty mesh must return Err");

    // 2. Truncated glTF header
    let corrupt_glb = vec![0x67, 0x6C, 0x54, 0x46, 0x02, 0x00];
    let corrupt_spec = AutoRigSpec::new(Bytes::from(corrupt_glb), "biped");
    let err_corrupt = auto_rig_mesh(&corrupt_spec);
    assert!(err_corrupt.is_err(), "Truncated GLB must return Err");

    // 3. Degenerate OBJ with zero vertices
    let bad_obj = "o BadMesh\ns off\n";
    let bad_obj_spec = AutoRigSpec::new(Bytes::from_static(bad_obj.as_bytes()), "biped");
    let err_bad_obj = auto_rig_mesh(&bad_obj_spec);
    assert!(err_bad_obj.is_err(), "OBJ without vertices must return Err");
}
