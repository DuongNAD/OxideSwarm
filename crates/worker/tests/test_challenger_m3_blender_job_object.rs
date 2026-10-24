//! Adversarial Empirical Stress & Process Containment Test Suite for Milestone 3 (F11–F16).
//!
//! Author: challenger_m3_2 (EMPIRICAL CHALLENGER)
//! Role: critic, specialist
//!
//! Mandatory Verification Objectives:
//! 1. Verify host Blender 4.5.4 LTS headless execution (at `C:\Program Files\Blender Foundation\Blender 4.5\blender.exe`)
//!    with `preferred_provider = "blender"`, executing `engines/autorig/blender_rig.py` and exporting a valid rigged `.glb`.
//! 2. Verify Win32 Job Object containment: kill tasks midway and assert strictly 0 orphan/zombie Blender or python processes remain.
//! 3. Verify graceful fallback when Blender executable is missing or invalid (clean fallback to built-in pure-Rust pipeline).

#![allow(clippy::field_reassign_with_default)]

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot};

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{
    AutoRigSpec, Bytes, Generative3DSpec, Task, TaskRequirements, TaskSpec,
};
use rusty_grid_worker::autorig::{
    auto_rig_mesh, auto_rig_mesh_async, blender_provider, load_mesh_from_bytes, BlenderRigProvider,
};
use rusty_grid_worker::job_object::JobObject;

use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::{RunnerConfig, TaskRunner, EXIT_CODE_CANCELLED};

static ENV_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Guard to safely set and restore environment variables in async tests.
struct ScopedEnvVar {
    _guard: tokio::sync::MutexGuard<'static, ()>,
    key: &'static str,
    original_val: Option<String>,
}

impl ScopedEnvVar {
    pub async fn set(key: &'static str, val: &str) -> Self {
        let guard = ENV_MUTEX.lock().await;
        let original_val = std::env::var(key).ok();
        std::env::set_var(key, val);
        Self {
            _guard: guard,
            key,
            original_val,
        }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        if let Some(ref orig) = self.original_val {
            std::env::set_var(self.key, orig);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

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

/// Polls until all given PIDs are absent from the OS process table.
fn assert_all_dead(pids: &[u32], timeout: Duration) {
    let mut sys = sysinfo::System::new();
    let start = Instant::now();
    while start.elapsed() < timeout {
        sys.refresh_processes();
        let any_alive = pids
            .iter()
            .any(|&pid| sys.process(sysinfo::Pid::from(pid as usize)).is_some());
        if !any_alive {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    sys.refresh_processes();
    let survivors: Vec<u32> = pids
        .iter()
        .copied()
        .filter(|&pid| sys.process(sysinfo::Pid::from(pid as usize)).is_some())
        .collect();

    // Kill any survivors before panicking
    for &pid in &survivors {
        if let Some(p) = sys.process(sysinfo::Pid::from(pid as usize)) {
            let _ = p.kill();
        }
    }

    panic!(
        "CRITICAL BUG: Orphaned / zombie processes survived termination! Survivors: {:?}",
        survivors
    );
}

// ============================================================================
// SUITE 1: Host Blender 4.5.4 LTS Headless Execution Verification
// ============================================================================

#[test]
fn test_verify_host_blender_installation_and_version() {
    let expected_path =
        PathBuf::from(r"C:\Program Files\Blender Foundation\Blender 4.5\blender.exe");
    assert!(
        expected_path.is_file(),
        "Host Blender 4.5.4 LTS executable must exist at {}",
        expected_path.display()
    );

    // Verify resolve_blender_binary finds this binary
    let resolved = blender_provider::resolve_blender_binary();
    assert!(
        resolved.is_some(),
        "resolve_blender_binary() failed to locate host Blender"
    );
    let resolved_path = resolved.unwrap();
    println!("Resolved Blender executable: {}", resolved_path.display());
    assert!(resolved_path.is_file());

    // Execute --version and assert Blender 4.5.4 LTS
    let output = std::process::Command::new(&resolved_path)
        .arg("--version")
        .output()
        .expect("Failed to execute blender --version");

    assert!(output.status.success(), "blender --version failed");
    let version_str = String::from_utf8_lossy(&output.stdout);
    println!("Blender version output:\n{}", version_str);
    assert!(
        version_str.contains("Blender 4.5"),
        "Expected Blender 4.5.x LTS, got:\n{}",
        version_str
    );
}

#[tokio::test]
async fn test_verify_host_blender_headless_autorig_execution_glb() {
    let blender_bin =
        blender_provider::resolve_blender_binary().expect("Host Blender 4.5 executable not found");

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

    let t0 = Instant::now();
    let result =
        BlenderRigProvider::execute_rigging(&blender_bin, &input_mesh, "biped", &animations, 60)
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
    let input_mesh = generate_test_glb_mesh();
    let spec = AutoRigSpec::new(Bytes::from(input_mesh), "biped")
        .with_animations(vec![
            "idle".to_string(),
            "walk".to_string(),
            "turnaround".to_string(),
        ])
        .with_provider("blender");

    let t0 = Instant::now();
    let result = auto_rig_mesh_async(&spec).await;
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

// ============================================================================
// SUITE 2: Win32 Job Object Containment & Process Lifecycle Stress
// ============================================================================

#[tokio::test]
async fn test_win32_job_object_blender_process_termination_on_abort() {
    let blender_bin =
        blender_provider::resolve_blender_binary().expect("Host Blender 4.5 executable not found");

    // Create Win32 Job Object with KILL_ON_JOB_CLOSE
    let mut job = JobObject::create().expect("Failed to create Win32 Job Object");

    // Spawn Blender running an intentional long sleep
    let mut cmd = tokio::process::Command::new(&blender_bin);
    cmd.arg("--background");
    cmd.arg("--factory-startup");
    cmd.arg("--python-expr");
    cmd.arg("import time, sys; print('BLENDER_ALIVE', flush=True); time.sleep(60)");
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::null());

    let mut child = cmd.spawn().expect("Failed to spawn Blender subprocess");
    let blender_pid = child.id().expect("Failed to get Blender PID");
    println!("Spawned Blender test PID: {}", blender_pid);

    job.assign_pid(blender_pid)
        .expect("Failed to assign Blender PID to Job Object");

    // Verify Blender is alive in the OS process table
    let mut sys = sysinfo::System::new();
    sys.refresh_processes();
    assert!(
        sys.process(sysinfo::Pid::from(blender_pid as usize))
            .is_some(),
        "Blender PID {blender_pid} must be alive before kill"
    );

    // Act: Forcefully terminate via JobObject::terminate
    let t0 = Instant::now();
    job.terminate(130).expect("Failed to terminate JobObject");

    // Assert: Blender PID must be strictly dead with 0 orphans
    assert_all_dead(&[blender_pid], Duration::from_secs(4));
    println!(
        "Blender PID {} verified dead in {:?}",
        blender_pid,
        t0.elapsed()
    );

    let _ = child.wait().await;
}

#[tokio::test]
async fn test_win32_job_object_implicit_drop_cleanup_blender() {
    let blender_bin =
        blender_provider::resolve_blender_binary().expect("Host Blender 4.5 executable not found");

    let mut job = JobObject::create().expect("Failed to create Win32 Job Object");

    let mut cmd = tokio::process::Command::new(&blender_bin);
    cmd.arg("--background");
    cmd.arg("--factory-startup");
    cmd.arg("--python-expr");
    cmd.arg("import time; time.sleep(60)");

    let mut child = cmd.spawn().expect("Failed to spawn Blender subprocess");
    let blender_pid = child.id().expect("Failed to get Blender PID");

    job.assign_pid(blender_pid)
        .expect("Failed to assign Blender PID to Job Object");

    let mut sys = sysinfo::System::new();
    sys.refresh_processes();
    assert!(sys
        .process(sysinfo::Pid::from(blender_pid as usize))
        .is_some());

    // Act: Drop JobObject handle WITHOUT calling terminate()!
    // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE must trigger Windows NT kernel auto-reap.
    drop(job);

    // Assert: Process must be terminated by kernel
    assert_all_dead(&[blender_pid], Duration::from_secs(4));
    println!("Blender PID {} reaped via RAII JobObject drop", blender_pid);

    let _ = child.wait().await;
}

#[tokio::test]
async fn test_win32_job_object_blender_grandchild_process_tree_containment() {
    let blender_bin =
        blender_provider::resolve_blender_binary().expect("Host Blender 4.5 executable not found");

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let pids_file = temp_dir.path().join("pids_blender_tree.txt");

    let mut job = JobObject::create().expect("Failed to create Win32 Job Object");

    let pids_path = pids_file.display().to_string().replace('\\', "/");

    // Blender script that spawns a python child, which itself spawns a grandchild
    let tree_script = temp_dir.path().join("blender_tree.py");
    let script_content = format!(
        r#"
import os, sys, time, subprocess

pids_file = "{}"
my_pid = os.getpid()
with open(pids_file, "a") as f:
    f.write(f"PID:{{my_pid}}\n")
    f.flush()

# Spawn child python process
child_code = r'''
import os, sys, time, subprocess
with open('__PIDS__', 'a') as f:
    f.write('PID:' + str(os.getpid()) + chr(10))
    f.flush()
cmd = [sys.executable, '-c', "import os, time; f = open('__PIDS__', 'a'); f.write('PID:' + str(os.getpid()) + chr(10)); f.close(); time.sleep(60)"]
gc = subprocess.Popen(cmd)
time.sleep(60)
'''.replace('__PIDS__', pids_file)

p = subprocess.Popen([sys.executable, "-c", child_code])
time.sleep(60)
"#,
        pids_path
    );

    let mut f = File::create(&tree_script).expect("Failed to write tree script");
    f.write_all(script_content.as_bytes()).expect("Write error");
    drop(f);

    let mut cmd = tokio::process::Command::new(&blender_bin);
    cmd.arg("--background");
    cmd.arg("--factory-startup");
    cmd.arg("--python");
    cmd.arg(&tree_script);

    let mut child = cmd.spawn().expect("Failed to spawn Blender subprocess");
    let root_pid = child.id().expect("Root PID");

    job.assign_pid(root_pid).expect("Failed to assign root PID");

    // Wait until root, child, and grandchild PIDs are recorded
    let start = Instant::now();
    let mut pids = Vec::new();
    while start.elapsed() < Duration::from_secs(20) {
        if let Ok(content) = std::fs::read_to_string(&pids_file) {
            pids.clear();
            for line in content.lines() {
                if let Some(pid_str) = line.strip_prefix("PID:") {
                    if let Ok(pid) = pid_str.trim().parse::<u32>() {
                        if !pids.contains(&pid) {
                            pids.push(pid);
                        }
                    }
                }
            }
            if pids.len() >= 3 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        pids.len() >= 3,
        "Expected at least 3 processes (Blender, child, grandchild), found {:?}",
        pids
    );
    println!("Spawned process tree PIDs: {:?}", pids);

    // Verify all processes alive
    let mut sys = sysinfo::System::new();
    sys.refresh_processes();
    for &pid in &pids {
        assert!(
            sys.process(sysinfo::Pid::from(pid as usize)).is_some(),
            "PID {pid} must be alive before containment termination"
        );
    }

    // Act: Drop JobObject handle to trigger kernel KILL_ON_JOB_CLOSE
    drop(job);

    // Assert: Strictly 0 survivors across all 3 generations
    assert_all_dead(&pids, Duration::from_secs(4));
    println!(
        "All {} processes across tree terminated with 0 orphans!",
        pids.len()
    );

    let _ = child.wait().await;
}

#[tokio::test]
async fn test_task_runner_autorig_midway_cancellation_cleans_subprocesses() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let pids_file = temp_dir.path().join("pids_cancel.txt");

    // Custom script simulating long auto-rig execution that records PID
    let hang_script = temp_dir.path().join("mock_hang_rig.py");
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

    let mut f = File::create(&hang_script).expect("Failed to write mock script");
    f.write_all(content.as_bytes()).expect("Write error");
    drop(f);

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

    // Wait a brief moment for execution to start
    tokio::time::sleep(Duration::from_millis(500)).await;

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
}

// ============================================================================
// SUITE 3: Graceful Fallback When Blender is Missing or Invalid
// ============================================================================

#[tokio::test]
async fn test_graceful_fallback_when_blender_path_is_invalid_or_missing() {
    // Set BLENDER_PATH to non-existent executable
    let _env = ScopedEnvVar::set(
        "BLENDER_PATH",
        r"C:\nonexistent_blender_dir\blender_invalid_404.exe",
    )
    .await;

    let input_mesh = generate_test_glb_mesh();
    let spec = AutoRigSpec::new(Bytes::from(input_mesh), "biped")
        .with_animations(vec!["idle".to_string(), "walk".to_string()])
        .with_provider("blender");

    let t0 = Instant::now();
    // Must NOT panic or fail, but cleanly fallback to built-in pure-Rust rigging pipeline
    let result = auto_rig_mesh_async(&spec).await;
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
async fn test_graceful_fallback_when_blender_binary_crashes() {
    let temp_dir = tempfile::tempdir().expect("tempdir");

    // Create a dummy batch script or executable that exits with error code 1
    let fake_blender = temp_dir.path().join("fake_blender.bat");
    let content = "@echo off\r\necho [ERROR] Simulated Blender crash >&2\r\nexit /b 1\r\n";
    let mut f = File::create(&fake_blender).expect("Failed to write fake blender");
    f.write_all(content.as_bytes()).expect("Write error");
    drop(f);

    // Point BLENDER_PATH to the crashing binary
    let _env = ScopedEnvVar::set("BLENDER_PATH", fake_blender.to_str().unwrap()).await;

    let input_mesh = generate_test_glb_mesh();
    let spec = AutoRigSpec::new(Bytes::from(input_mesh), "biped")
        .with_animations(vec!["idle".to_string()])
        .with_provider("blender");

    let t0 = Instant::now();
    // Must gracefully catch failure and fallback to built-in pure-Rust engine
    let result = auto_rig_mesh_async(&spec).await;
    let elapsed = t0.elapsed();
    println!("Crash fallback completed in {:?}", elapsed);

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

    // Dummy failing blender binary
    let fake_blender = temp_dir.path().join("fake_blender.bat");
    let content = "@echo off\r\nexit /b 42\r\n";
    let mut f = File::create(&fake_blender).expect("create fake");
    f.write_all(content.as_bytes()).expect("write fake");
    drop(f);

    let _env = ScopedEnvVar::set("BLENDER_PATH", fake_blender.to_str().unwrap()).await;

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

    // TaskRunner must succeed via fallback!
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
