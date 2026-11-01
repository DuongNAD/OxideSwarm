//! Acceptance and Stress Test Suite for SEC-5:
//! Run Python and Blender Through Hardened Process Runner.
//!
//! Objectives:
//! 1. Secret stripping: OXIDESWARM_SECRET, OXIDESWARM_SECRET_FILE, and OXIDE_MESH_TOKEN
//!    are stripped from child environment before Python and Blender workloads execute.
//! 2. Priority enforcement: Subprocesses run at task nice priority (BelowNormal by default).
//! 3. Non-blocking stderr: Voluminous stderr output (> 64 KiB) does not cause pipe deadlock.
//! 4. Process tree cleanup: Child and grandchild processes are cleanly reaped upon cancellation
//!    or future drop across Linux, macOS, and Windows.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::oneshot;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{Generative3DSpec, Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_worker::autorig::BlenderRigProvider;
use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::{
    ProcessPriority, RunnerConfig, TaskRunner, EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR,
    EXIT_CODE_TIMEOUT,
};

static ENV_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Guard to safely set and restore multiple environment variables in async tests without self-deadlock.
struct EnvLock {
    _guard: tokio::sync::MutexGuard<'static, ()>,
    vars: Vec<(&'static str, Option<String>)>,
}

impl EnvLock {
    pub async fn lock() -> Self {
        let guard = ENV_MUTEX.lock().await;
        Self {
            _guard: guard,
            vars: Vec::new(),
        }
    }

    pub fn set(&mut self, key: &'static str, val: &str) {
        let original_val = std::env::var(key).ok();
        std::env::set_var(key, val);
        self.vars.push((key, original_val));
    }
}

impl Drop for EnvLock {
    fn drop(&mut self) {
        for (key, original_val) in self.vars.iter().rev() {
            if let Some(ref orig) = original_val {
                std::env::set_var(key, orig);
            } else {
                std::env::remove_var(key);
            }
        }
    }
}

/// Resolves python executable for tests.
#[allow(dead_code)]
fn get_python_exe() -> PathBuf {
    if let Ok(custom) = std::env::var("OXIDESWARM_PYTHON") {
        let p = PathBuf::from(custom);
        if p.is_file() {
            return p;
        }
    }
    PathBuf::from("python")
}

/// Creates a standard test TaskRunner.
fn create_test_runner() -> (TaskRunner, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let sandbox_config = SandboxConfig::new(temp_dir.path().to_path_buf());
    let runner_config = RunnerConfig::new(sandbox_config);
    let mut caps = WorkerCapabilities::new("test_worker", 4, 8192, false, false, None);
    caps.has_cuda = false;
    let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, runner_config);
    (runner, temp_dir)
}

/// Polls `pids_file` until `expected_count` distinct PIDs have been recorded.
async fn poll_pids_async(pids_file: &Path, expected_count: usize, timeout: Duration) -> Vec<u32> {
    let start = Instant::now();
    let mut pids = Vec::new();
    while start.elapsed() < timeout {
        if let Ok(content) = tokio::fs::read_to_string(pids_file).await {
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
            if pids.len() >= expected_count {
                return pids;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    pids
}

/// Asserts that all given PIDs are absent from the OS process table within timeout.
async fn assert_all_dead_async(pids: &[u32], timeout: Duration) {
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sys.refresh_processes();
    let survivors: Vec<u32> = pids
        .iter()
        .copied()
        .filter(|&pid| sys.process(sysinfo::Pid::from(pid as usize)).is_some())
        .collect();

    // Clean up survivors before panic
    for &pid in &survivors {
        if let Some(p) = sys.process(sysinfo::Pid::from(pid as usize)) {
            let _ = p.kill();
        }
    }

    assert!(
        survivors.is_empty(),
        "Orphaned descendant processes survived termination: {:?}",
        survivors
    );
}

/// Creates a portable fake Blender runner script that executes a Python logic file.
fn create_fake_blender_executable(dir: &Path, python_logic: &str) -> PathBuf {
    let logic_path = dir.join("fake_blender_logic.py");
    let mut f = File::create(&logic_path).expect("Failed to create logic file");
    f.write_all(python_logic.as_bytes())
        .expect("Failed to write logic file");
    drop(f);

    #[cfg(windows)]
    {
        let cmd_path = dir.join("fake_blender.cmd");
        let content = format!(
            "@echo off\r\n\"{}\" \"{}\" %*\r\n",
            get_python_exe().display(),
            logic_path.display()
        );
        let mut f = File::create(&cmd_path).expect("Failed to create .cmd file");
        f.write_all(content.as_bytes())
            .expect("Failed to write .cmd file");
        cmd_path
    }

    #[cfg(not(windows))]
    {
        use std::os::unix::fs::PermissionsExt;
        let sh_path = dir.join("fake_blender.sh");
        let content = format!(
            "#!/bin/sh\nexec python3 \"{}\" \"$@\"\n",
            logic_path.display()
        );
        let mut f = File::create(&sh_path).expect("Failed to create .sh file");
        f.write_all(content.as_bytes())
            .expect("Failed to write .sh file");
        let _ = std::fs::set_permissions(&sh_path, std::fs::Permissions::from_mode(0o755));
        sh_path
    }
}

// ============================================================================
// Test 1: 3D Task Secret Stripping and Task Priority
// ============================================================================

#[tokio::test]
async fn test_sec5_3d_task_secret_stripping_and_nice_value() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    // Script that verifies secrets are unset, checks priority, and produces output glTF
    let mock_script = temp_dir.path().join("mock_3d_engine.py");
    let script_content = r#"
import os, sys

# Check secret environment variables
secrets = ["OXIDESWARM_SECRET", "OXIDESWARM_SECRET_FILE", "OXIDE_MESH_TOKEN"]
leaks = [s for s in secrets if s in os.environ]
if leaks:
    sys.stderr.write(f"SECRET_LEAK:{','.join(leaks)}\n")
    sys.exit(42)

# Check priority
if sys.platform == "win32":
    import ctypes
    kernel32 = ctypes.windll.kernel32
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    kernel32.GetPriorityClass.argtypes = [ctypes.c_void_p]
    pclass = kernel32.GetPriorityClass(kernel32.GetCurrentProcess())
    print(f"WIN_PRIORITY_CLASS:{pclass}", flush=True)
else:
    nice_val = os.nice(0)
    print(f"UNIX_NICE_VAL:{nice_val}", flush=True)

# Produce minimal valid glTF 2.0 binary
if "--output" in sys.argv:
    out_idx = sys.argv.index("--output")
    out_path = sys.argv[out_idx + 1]
    with open(out_path, "wb") as f:
        f.write(b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00")

sys.exit(0)
"#;
    let mut f = File::create(&mock_script).expect("Failed to create mock script");
    f.write_all(script_content.as_bytes()).expect("Write error");
    drop(f);

    // Set sentinels in parent environment to verify runner strips them
    let mut env = EnvLock::lock().await;
    env.set("OXIDESWARM_SECRET", "sentinel-cluster-secret-sec5");
    env.set("OXIDESWARM_SECRET_FILE", "sentinel-secret-file-sec5");
    env.set("OXIDE_MESH_TOKEN", "sentinel-mesh-token-sec5");
    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        mock_script.to_str().expect("path"),
    );

    let spec = Generative3DSpec::new_prompt("SEC-5 Secret Stripping Cube").with_engine("triposr");
    let requirements = TaskRequirements {
        timeout_secs: 30,
        ..Default::default()
    };
    let task = Task::new(TaskSpec::Generative3D(spec), requirements);

    let res = runner.execute_task(&task, None, None).await;

    assert!(
        res.is_success(),
        "3D task execution failed: {:?}, stderr: {:?}",
        res.error,
        res.stderr_str()
    );
    assert!(
        !res.stderr_str().contains("SECRET_LEAK"),
        "Secrets leaked to Python subprocess! stderr: {}",
        res.stderr_str()
    );

    // Verify priority class on Windows (0x4000 = 16384: BELOW_NORMAL_PRIORITY_CLASS)
    #[cfg(windows)]
    {
        assert!(
            res.stdout_str().contains("WIN_PRIORITY_CLASS:16384"),
            "Process did not run at BelowNormal priority class on Windows! stdout: {}",
            res.stdout_str()
        );
    }

    // Verify nice value on Unix (>= 10 for BelowNormal)
    #[cfg(not(windows))]
    {
        assert!(
            res.stdout_str().contains("UNIX_NICE_VAL:"),
            "No nice value reported in stdout: {}",
            res.stdout_str()
        );
        let nice_val: i32 = res
            .stdout_str()
            .lines()
            .find(|l| l.starts_with("UNIX_NICE_VAL:"))
            .and_then(|l| l.strip_prefix("UNIX_NICE_VAL:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        assert!(
            nice_val >= 10,
            "Nice value was {nice_val}, expected >= 10 for BelowNormal priority"
        );
    }

    // Verify output asset bytes collected
    assert!(res.asset_bytes.is_some());
    let bytes = res.asset_bytes.unwrap();
    assert!(bytes.as_slice().starts_with(b"glTF"));
}

// ============================================================================
// Test 2: Fake Blender Task Secret Stripping and Task Priority
// ============================================================================

#[tokio::test]
async fn test_sec5_fake_blender_task_secret_stripping_and_nice_value() {
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let fake_blender_logic = r#"
import os, sys

secrets = ["OXIDESWARM_SECRET", "OXIDESWARM_SECRET_FILE", "OXIDE_MESH_TOKEN"]
leaks = [s for s in secrets if s in os.environ]
if leaks:
    sys.stderr.write(f"SECRET_LEAK:{','.join(leaks)}\n")
    sys.exit(42)

if sys.platform == "win32":
    import ctypes
    kernel32 = ctypes.windll.kernel32
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    kernel32.GetPriorityClass.argtypes = [ctypes.c_void_p]
    pclass = kernel32.GetPriorityClass(kernel32.GetCurrentProcess())
    if pclass != 16384:
        sys.stderr.write(f"BAD_PRIORITY:{pclass}\n")
        sys.exit(43)
else:
    nice_val = os.nice(0)
    if nice_val < 10:
        sys.stderr.write(f"BAD_NICE:{nice_val}\n")
        sys.exit(43)

# Parse output file
if "--output" in sys.argv:
    out_idx = sys.argv.index("--output")
    out_path = sys.argv[out_idx + 1]
    with open(out_path, "wb") as f:
        f.write(b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00")

sys.exit(0)
"#;

    let fake_blender_path = create_fake_blender_executable(temp_dir.path(), fake_blender_logic);

    let mut env = EnvLock::lock().await;
    env.set("OXIDESWARM_SECRET", "sentinel-cluster-secret-sec5");
    env.set("OXIDESWARM_SECRET_FILE", "sentinel-secret-file-sec5");
    env.set("OXIDE_MESH_TOKEN", "sentinel-mesh-token-sec5");

    let (runner, _dir) = create_test_runner();
    let task_id = TaskId::new();
    let dummy_input = b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00";
    let res = BlenderRigProvider::execute_rigging(
        &runner,
        task_id,
        &fake_blender_path,
        dummy_input,
        "biped",
        &["walk".to_string()],
        30,
    )
    .await;

    assert!(
        res.is_ok(),
        "Fake Blender execution failed: {:?}",
        res.err()
    );
    let output_glb = res.unwrap();
    assert!(output_glb.starts_with(b"glTF"));
}

// ============================================================================
// Test 3: Blender Large Stderr Does Not Block (Pipe Deadlock Prevention)
// ============================================================================

#[tokio::test]
async fn test_sec5_blender_large_stderr_does_not_block() {
    let temp_dir = tempfile::tempdir().expect("tempdir");

    // Fake Blender logic that writes 256 KiB of output to stderr
    // Exceeds OS pipe buffers (typically 64 KiB), verifying concurrent draining.
    let logic = r#"
import os, sys

# Emit 256 KiB to stderr
chunk = "E" * 4096
for _ in range(64):
    sys.stderr.write(chunk)
    sys.stderr.flush()

if "--output" in sys.argv:
    out_idx = sys.argv.index("--output")
    out_path = sys.argv[out_idx + 1]
    with open(out_path, "wb") as f:
        f.write(b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00")

sys.exit(0)
"#;

    let fake_blender_path = create_fake_blender_executable(temp_dir.path(), logic);

    let (runner, _dir) = create_test_runner();
    let task_id = TaskId::new();
    let t0 = Instant::now();
    let res = BlenderRigProvider::execute_rigging(
        &runner,
        task_id,
        &fake_blender_path,
        b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00",
        "biped",
        &[],
        10,
    )
    .await;

    let elapsed = t0.elapsed();
    println!("Large stderr test completed in {:?}", elapsed);

    assert!(
        res.is_ok(),
        "Large stderr execution deadlocked or failed: {:?}",
        res.err()
    );
    assert!(
        elapsed < Duration::from_secs(8),
        "Execution took too long ({:?}), likely blocked on pipe",
        elapsed
    );
    let glb = res.unwrap();
    assert!(glb.starts_with(b"glTF"));
}

// ============================================================================
// Test 4: 3D Task Grandchildren Die on Abort or Cancel Cross-Platform (Review Item 4)
// ============================================================================

#[tokio::test]
async fn test_sec5_3d_task_child_and_grandchild_die_on_abort_or_cancel() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let runner = Arc::new(runner);

    let spec = Generative3DSpec::new_prompt("adversarial grandchild abort test")
        .with_quality("standard")
        .with_engine("triposr");
    let requirements = TaskRequirements {
        timeout_secs: 60,
        ..Default::default()
    };
    let task = Task::new(TaskSpec::Generative3D(spec), requirements);

    let mut env = EnvLock::lock().await;

    // ------------------------------------------------------------------------
    // Phase 1: Verify child and grandchild cleanup on explicit cancellation channel
    // ------------------------------------------------------------------------
    let pids_file_cancel = temp_dir.path().join("pids_gen3d_tree_cancel.txt");
    let pids_path_cancel = pids_file_cancel.display().to_string().replace('\\', "/");
    let script_cancel = temp_dir.path().join("gen3d_spawner_cancel.py");
    let content_cancel = format!(
        r#"
import os, sys, time, subprocess

pids_file = "{pids_path_cancel}"
my_pid = os.getpid()
with open(pids_file, "a") as f:
    f.write(f"PID:{{my_pid}}\n")
    f.flush()

# Spawn grandchild process that sleeps for 60 seconds
gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open(pids_file, "a") as f:
    f.write(f"PID:{{gc.pid}}\n")
    f.flush()

time.sleep(60.0)
"#
    );
    let mut f = File::create(&script_cancel).expect("create cancel script");
    f.write_all(content_cancel.as_bytes())
        .expect("write cancel script");
    drop(f);

    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        script_cancel.to_str().expect("path"),
    );

    let (cancel_tx, cancel_rx) = oneshot::channel();
    let child_pid = Arc::new(AtomicU32::new(0));

    let runner_clone = Arc::clone(&runner);
    let child_pid_clone = Arc::clone(&child_pid);
    let task_clone = task.clone();
    let task_handle_cancel = tokio::spawn(async move {
        runner_clone
            .execute_task_with_checkpoint(
                &task_clone,
                None,
                None,
                Some(child_pid_clone),
                Some(cancel_rx),
            )
            .await
    });

    let pids_cancel = poll_pids_async(&pids_file_cancel, 2, Duration::from_secs(20)).await;
    assert_eq!(
        pids_cancel.len(),
        2,
        "Expected 2 processes (parent and grandchild) for cancel test, found {:?}",
        pids_cancel
    );

    let mut sys = sysinfo::System::new();
    sys.refresh_processes();
    for &pid in &pids_cancel {
        assert!(
            sys.process(sysinfo::Pid::from(pid as usize)).is_some(),
            "PID {pid} must be running before cancellation"
        );
    }

    let _ = cancel_tx.send("task cancelled by operator".to_string());
    let res = task_handle_cancel.await.expect("task join failed");
    assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
    assert_all_dead_async(&pids_cancel, Duration::from_secs(4)).await;
    println!("Verified complete 3D tree cleanup on cancellation channel!");

    // ------------------------------------------------------------------------
    // Phase 2: Verify child and grandchild cleanup on real Tokio future abort (task_handle.abort())
    // ------------------------------------------------------------------------
    let pids_file_abort = temp_dir.path().join("pids_gen3d_tree_abort.txt");
    let pids_path_abort = pids_file_abort.display().to_string().replace('\\', "/");
    let script_abort = temp_dir.path().join("gen3d_spawner_abort.py");
    let content_abort = format!(
        r#"
import os, sys, time, subprocess

pids_file = "{pids_path_abort}"
my_pid = os.getpid()
with open(pids_file, "a") as f:
    f.write(f"PID:{{my_pid}}\n")
    f.flush()

# Spawn grandchild process that sleeps for 60 seconds
gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open(pids_file, "a") as f:
    f.write(f"PID:{{gc.pid}}\n")
    f.flush()

time.sleep(60.0)
"#
    );
    let mut f = File::create(&script_abort).expect("create abort script");
    f.write_all(content_abort.as_bytes())
        .expect("write abort script");
    drop(f);

    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        script_abort.to_str().expect("path"),
    );

    let runner_clone2 = Arc::clone(&runner);
    let task_clone2 = task.clone();
    let task_handle_abort =
        tokio::spawn(async move { runner_clone2.execute_task(&task_clone2, None, None).await });

    let pids_abort = poll_pids_async(&pids_file_abort, 2, Duration::from_secs(20)).await;
    assert_eq!(
        pids_abort.len(),
        2,
        "Expected 2 processes for abort test, found {:?}",
        pids_abort
    );

    let mut sys = sysinfo::System::new();
    sys.refresh_processes();
    for &pid in &pids_abort {
        assert!(
            sys.process(sysinfo::Pid::from(pid as usize)).is_some(),
            "PID {pid} must be running before future abort"
        );
    }

    // Act: Abort the Tokio future directly
    task_handle_abort.abort();
    let join_res = task_handle_abort.await;
    assert!(
        join_res.is_err() && join_res.unwrap_err().is_cancelled(),
        "task_handle must be aborted and report cancelled"
    );

    // Assert: Both child and grandchild must be terminated within 4s due to ProcessTree RAII drop
    assert_all_dead_async(&pids_abort, Duration::from_secs(4)).await;
    println!("Verified complete 3D tree cleanup on future abort!");
}

// ============================================================================
// Test 5: Blender Task Grandchildren Die on Future Drop Cross-Platform
// ============================================================================

#[tokio::test]
async fn test_sec5_blender_task_grandchildren_die_on_abort_cross_platform() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let pids_file = temp_dir.path().join("pids_blender_tree.txt");
    let pids_path = pids_file.display().to_string().replace('\\', "/");
    let logic = format!(
        r#"
import os, sys, time, subprocess

pids_file = "{pids_path}"
my_pid = os.getpid()
with open(pids_file, "a") as f:
    f.write(f"PID:{{my_pid}}\n")
    f.flush()

# Spawn grandchild process that sleeps for 60 seconds
gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open(pids_file, "a") as f:
    f.write(f"PID:{{gc.pid}}\n")
    f.flush()

time.sleep(60.0)
"#
    );

    let fake_blender_path = create_fake_blender_executable(temp_dir.path(), &logic);

    let (runner, _dir) = create_test_runner();
    let runner = Arc::new(runner);
    let r = Arc::clone(&runner);
    let task_id = TaskId::new();
    let fake_bin = fake_blender_path.clone();
    let task_handle = tokio::spawn(async move {
        BlenderRigProvider::execute_rigging(
            &r,
            task_id,
            &fake_bin,
            b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00",
            "biped",
            &[],
            60,
        )
        .await
    });

    // Wait until both processes are running
    let pids = poll_pids_async(&pids_file, 2, Duration::from_secs(20)).await;
    assert_eq!(pids.len(), 2, "Expected 2 processes, found {:?}", pids);

    let mut sys = sysinfo::System::new();
    sys.refresh_processes();
    for &pid in &pids {
        assert!(
            sys.process(sysinfo::Pid::from(pid as usize)).is_some(),
            "PID {pid} must be running before abort"
        );
    }

    // Abort the Tokio future
    task_handle.abort();
    let _ = task_handle.await;

    // Both processes must be dead within 4s due to ProcessTree RAII drop
    assert_all_dead_async(&pids, Duration::from_secs(4)).await;
    println!("Verified complete Blender tree cleanup on future abort!");
}

// ============================================================================
// Test 6: Blender Watchdog Timeout Cleans Grandchild Process Tree (Item 8)
// ============================================================================

#[tokio::test]
async fn test_sec5_blender_watchdog_timeout_cleans_grandchild_tree() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let pids_file = temp_dir.path().join("pids_timeout_tree.txt");
    let pids_path = pids_file.display().to_string().replace('\\', "/");
    let logic = format!(
        r#"
import os, sys, time, subprocess

pids_file = "{pids_path}"
my_pid = os.getpid()
with open(pids_file, "a") as f:
    f.write(f"PID:{{my_pid}}\n")
    f.flush()

# Spawn grandchild process that sleeps indefinitely
gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open(pids_file, "a") as f:
    f.write(f"PID:{{gc.pid}}\n")
    f.flush()

# Parent sleeps past the watchdog timeout
time.sleep(60.0)
"#
    );

    let fake_blender_path = create_fake_blender_executable(temp_dir.path(), &logic);
    let (runner, _dir) = create_test_runner();
    let task_id = TaskId::new();

    let t0 = Instant::now();
    let res = BlenderRigProvider::execute_rigging(
        &runner,
        task_id,
        &fake_blender_path,
        b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00",
        "biped",
        &[],
        2, // 2s timeout
    )
    .await;

    let elapsed = t0.elapsed();
    println!("Blender timeout test elapsed: {:?}", elapsed);

    // 1. Must fail with timeout error
    assert!(res.is_err(), "Expected timeout failure, got: {:?}", res);
    let err_msg = res.unwrap_err();
    assert!(
        err_msg.contains("timed out") || err_msg.contains("124"),
        "Expected timeout message, got: {err_msg}"
    );
    assert!(
        elapsed >= Duration::from_secs(2),
        "Execution must have waited for at least the 2s timeout limit"
    );

    // 2. Poll recorded PIDs
    let pids = poll_pids_async(&pids_file, 2, Duration::from_secs(10)).await;
    assert_eq!(
        pids.len(),
        2,
        "Expected parent and grandchild PIDs, found {:?}",
        pids
    );

    // 3. Assert both parent and grandchild are terminated
    assert_all_dead_async(&pids, Duration::from_secs(4)).await;
    println!("Verified complete Blender process tree termination on timeout!");
}

// ============================================================================
// Test 7: Blender Runner ProcessPriority::Idle & Task/Worker Identity (Items 2 & 8)
// ============================================================================

#[tokio::test]
async fn test_sec5_blender_runner_priority_idle_and_task_identity() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let sandbox_config = SandboxConfig::new(temp_dir.path().to_path_buf());
    let mut runner_config = RunnerConfig::new(sandbox_config);
    runner_config.process_priority = ProcessPriority::Idle;
    let worker_id = uuid::Uuid::new_v4();
    let task_id = TaskId::new();
    let caps = WorkerCapabilities::default();
    let runner = TaskRunner::new(worker_id, caps, runner_config);

    let logic = format!(
        r#"
import os, sys

# 1. Verify secrets are stripped
secrets = ["OXIDESWARM_SECRET", "OXIDESWARM_SECRET_FILE", "OXIDE_MESH_TOKEN"]
leaks = [s for s in secrets if s in os.environ]
if leaks:
    sys.stderr.write(f"SECRET_LEAK:{{','.join(leaks)}}\n")
    sys.exit(42)

# 2. Verify process priority (Idle: 64 on Windows, nice 19 on Unix)
if sys.platform == "win32":
    import ctypes
    kernel32 = ctypes.windll.kernel32
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    kernel32.GetPriorityClass.argtypes = [ctypes.c_void_p]
    pclass = kernel32.GetPriorityClass(kernel32.GetCurrentProcess())
    # IDLE_PRIORITY_CLASS is 0x00000040 = 64
    if pclass != 64:
        sys.stderr.write(f"BAD_PRIORITY:{{pclass}}\n")
        sys.exit(43)
else:
    nice_val = os.nice(0)
    if nice_val < 19:
        sys.stderr.write(f"BAD_NICE:{{nice_val}}\n")
        sys.exit(43)

# 3. Verify task ID and worker ID match runner and task
expected_task_id = "{task_id}"
expected_worker_id = "{worker_id}"
actual_task_id = os.environ.get("RUSTY_GRID_TASK_ID", "")
actual_worker_id = os.environ.get("RUSTY_GRID_WORKER_ID", "")

if actual_task_id != expected_task_id:
    sys.stderr.write(f"BAD_TASK_ID: expected {{expected_task_id}}, got {{actual_task_id}}\n")
    sys.exit(44)

if actual_worker_id != expected_worker_id:
    sys.stderr.write(f"BAD_WORKER_ID: expected {{expected_worker_id}}, got {{actual_worker_id}}\n")
    sys.exit(45)

# 4. Write valid glTF output
if "--output" in sys.argv:
    out_idx = sys.argv.index("--output")
    out_path = sys.argv[out_idx + 1]
    with open(out_path, "wb") as f:
        f.write(b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00")

sys.exit(0)
"#
    );

    let fake_blender_path = create_fake_blender_executable(temp_dir.path(), &logic);

    let mut env = EnvLock::lock().await;
    env.set("OXIDESWARM_SECRET", "sentinel-cluster-secret-sec5");
    env.set("OXIDESWARM_SECRET_FILE", "sentinel-secret-file-sec5");
    env.set("OXIDE_MESH_TOKEN", "sentinel-mesh-token-sec5");

    let dummy_input = b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00";
    let res = BlenderRigProvider::execute_rigging(
        &runner,
        task_id,
        &fake_blender_path,
        dummy_input,
        "biped",
        &["walk".to_string()],
        30,
    )
    .await;

    assert!(
        res.is_ok(),
        "Fake Blender execution failed: {:?}",
        res.err()
    );
    let output_glb = res.unwrap();
    assert!(output_glb.starts_with(b"glTF"));
}

// ============================================================================
// Test 8: Version Probes Strip Secrets (Items 6 & 8)
// ============================================================================

#[tokio::test]
async fn test_sec5_version_probes_strip_secrets() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let log_file = temp_dir.path().join("probe_env.log");
    let log_path = log_file.display().to_string().replace('\\', "/");

    let probe_logic = format!(
        r#"
import os, sys

log_file = "{log_path}"
with open(log_file, "a") as f:
    f.write("probe_invoked\n")
    for k, v in os.environ.items():
        if "OXIDE" in k:
            f.write(f"{{k}}={{v}}\n")

if any("blender" in a.lower() for a in sys.argv):
    print("Blender 4.5.4 LTS")
else:
    print("Python 3.11.0")

sys.exit(0)
"#
    );

    let script_py = temp_dir.path().join("probe_mock.py");
    std::fs::write(&script_py, &probe_logic).expect("write mock script");

    let real_py = if let Ok(custom) = std::env::var("OXIDESWARM_PYTHON") {
        PathBuf::from(custom)
    } else {
        #[cfg(windows)]
        {
            let output = std::process::Command::new("where.exe")
                .arg("python.exe")
                .output()
                .ok();
            output
                .and_then(|out| {
                    let text = String::from_utf8_lossy(&out.stdout);
                    text.lines().next().map(|line| PathBuf::from(line.trim()))
                })
                .unwrap_or_else(|| PathBuf::from("python"))
        }
        #[cfg(not(windows))]
        {
            PathBuf::from("python3")
        }
    };

    #[cfg(windows)]
    let (fake_blender, fake_python) = {
        let cmd_blender = format!(
            "@echo off\r\n\"{}\" \"{}\" blender %*\r\n",
            real_py.display(),
            script_py.display()
        );
        let cmd_python = format!(
            "@echo off\r\n\"{}\" \"{}\" python %*\r\n",
            real_py.display(),
            script_py.display()
        );
        let b_path = temp_dir.path().join("blender.cmd");
        let p_path = temp_dir.path().join("python.cmd");
        std::fs::write(&b_path, cmd_blender).expect("write blender.cmd");
        std::fs::write(&p_path, cmd_python).expect("write python.cmd");
        (b_path, p_path)
    };
    #[cfg(not(windows))]
    let (fake_blender, fake_python) = {
        use std::os::unix::fs::PermissionsExt;
        let sh_blender = format!(
            "#!/bin/sh\nexec \"{}\" \"{}\" blender \"$@\"\n",
            real_py.display(),
            script_py.display()
        );
        let sh_python = format!(
            "#!/bin/sh\nexec \"{}\" \"{}\" python \"$@\"\n",
            real_py.display(),
            script_py.display()
        );
        let b_path = temp_dir.path().join("blender.sh");
        let p_path = temp_dir.path().join("python.sh");
        std::fs::write(&b_path, sh_blender).expect("write blender");
        std::fs::write(&p_path, sh_python).expect("write python");
        let _ = std::fs::set_permissions(&b_path, std::fs::Permissions::from_mode(0o755));
        let _ = std::fs::set_permissions(&p_path, std::fs::Permissions::from_mode(0o755));
        (b_path, p_path)
    };

    let mut env = EnvLock::lock().await;
    env.set("OXIDESWARM_SECRET", "sentinel-cluster-secret-sec5");
    env.set("OXIDESWARM_SECRET_FILE", "sentinel-secret-file-sec5");
    env.set("OXIDE_MESH_TOKEN", "sentinel-mesh-token-sec5");

    // Invoke each probe directly with the full fake binary path (no PATH modification)
    assert!(
        rusty_grid_worker::autorig::blender_provider::probe_blender(&fake_blender),
        "blender_provider::probe_blender must succeed on fake binary"
    );
    assert!(
        rusty_grid_worker::gen3d::runner::probe_python(&fake_python),
        "gen3d::runner::probe_python must succeed on fake binary"
    );
    let (has_blender, _ver) = rusty_grid_core::capabilities::probe_blender_binary(&fake_blender);
    assert!(
        has_blender,
        "capabilities::probe_blender_binary must succeed on fake binary"
    );
    assert!(
        rusty_grid_core::capabilities::query_blender_version(&fake_blender).is_some(),
        "capabilities::query_blender_version must succeed on fake binary"
    );

    // Read log with expect and verify exactly 4 probe executions (1 line per probe)
    let logged = std::fs::read_to_string(&log_file).expect("probe log file was not created");
    let lines: Vec<&str> = logged.lines().collect();
    assert_eq!(
        lines.len(),
        4,
        "Expected exactly 4 probe executions (1 line per probe), got {}: {:?}",
        lines.len(),
        lines
    );

    // Verify zero leaked secret variables
    assert!(
        !logged.contains("sentinel-cluster-secret-sec5"),
        "OXIDESWARM_SECRET leaked to probe: {logged}"
    );
    assert!(
        !logged.contains("sentinel-secret-file-sec5"),
        "OXIDESWARM_SECRET_FILE leaked to probe: {logged}"
    );
    assert!(
        !logged.contains("sentinel-mesh-token-sec5"),
        "OXIDE_MESH_TOKEN leaked to probe: {logged}"
    );
}

// ============================================================================
// Test 9: glTF Header Validation Failure Fails For Every Engine (Items 1 & 8)
// ============================================================================

#[tokio::test]
async fn test_sec5_gltf_header_validation_failure_for_both_engines() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let mock_script = temp_dir.path().join("junk_glb_engine.py");
    let script_content = r#"
import sys
if "--output" in sys.argv:
    out_idx = sys.argv.index("--output")
    out_path = sys.argv[out_idx + 1]
    with open(out_path, "wb") as f:
        f.write(b"CORRUPT_NOT_A_VALID_GLTF_HEADER")
sys.exit(0)
"#;
    std::fs::write(&mock_script, script_content).expect("write mock script");

    let mut env = EnvLock::lock().await;
    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        mock_script.to_str().expect("path"),
    );

    for engine in ["triposr", "auto"] {
        let spec = Generative3DSpec::new_prompt("Bad glTF Test").with_engine(engine);
        let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
        let res = runner.execute_task(&task, None, None).await;

        assert_eq!(
            res.exit_code, EXIT_CODE_GENERAL_ERROR,
            "Failed for engine {engine}"
        );
        assert_eq!(
            res.error.as_deref(),
            Some("Generated .glb file has invalid glTF 2.0 header"),
            "Error mismatch for engine {engine}"
        );
        assert!(
            res.asset_bytes.is_none(),
            "asset_bytes must be None for engine {engine}"
        );
    }
}

// ============================================================================
// Test 10: Explicit Engine Exit Code Described When Error Empty (Items 3 & 8)
// ============================================================================

#[tokio::test]
async fn test_sec5_explicit_engine_exit_code_3_named_in_error() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let mock_script = temp_dir.path().join("exit_3_engine.py");
    let script_content = "import sys\nsys.exit(3)\n";
    std::fs::write(&mock_script, script_content).expect("write mock script");

    let mut env = EnvLock::lock().await;
    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        mock_script.to_str().expect("path"),
    );

    let spec = Generative3DSpec::new_prompt("Exit 3 Test").with_engine("triposr");
    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
    let res = runner.execute_task(&task, None, None).await;

    assert_eq!(res.exit_code, 3);
    assert_eq!(
        res.error.as_deref(),
        Some("3D engine subprocess failed with exit code 3")
    );
}

// ============================================================================
// Test 11: Exit 124 Falls Back Under Auto vs Watchdog Timeout (Items 4 & 8)
// ============================================================================

#[tokio::test]
async fn test_sec5_exit_124_falls_back_under_auto_while_watchdog_fails() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    // Engine's own exit 124
    let exit_124_script = temp_dir.path().join("exit_124_engine.py");
    std::fs::write(&exit_124_script, "import sys\nsys.exit(124)\n").expect("write");

    let mut env = EnvLock::lock().await;
    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        exit_124_script.to_str().expect("path"),
    );

    let spec = Generative3DSpec::new_prompt("Exit 124 Auto Fallback").with_engine("auto");
    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
    let res = runner.execute_task(&task, None, None).await;

    assert!(
        res.is_success(),
        "Exit 124 under auto must fall back to Tier 0 SDF"
    );
    assert!(res.asset_bytes.is_some());
    assert!(res.asset_bytes.unwrap().as_slice().starts_with(b"glTF"));

    // Real timeout via watchdog
    let hang_script = temp_dir.path().join("hang_engine.py");
    std::fs::write(&hang_script, "import time\ntime.sleep(30)\n").expect("write");
    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        hang_script.to_str().expect("path"),
    );

    let reqs = TaskRequirements {
        timeout_secs: 1,
        ..Default::default()
    };
    let spec_timeout = Generative3DSpec::new_prompt("Timeout Engine").with_engine("auto");
    let task_timeout = Task::new(TaskSpec::Generative3D(spec_timeout), reqs);
    let res_timeout = runner.execute_task(&task_timeout, None, None).await;

    assert_eq!(res_timeout.exit_code, EXIT_CODE_TIMEOUT);
    assert!(
        res_timeout
            .error
            .as_deref()
            .unwrap_or("")
            .contains("Task timed out after 1s"),
        "Expected timeout error, got: {:?}",
        res_timeout.error
    );
}

// ============================================================================
// Test 12: Cancelled Run Sends Zero Checkpoints (Items 5 & 8)
// ============================================================================

#[tokio::test]
async fn test_sec5_cancelled_dead_run_sends_zero_checkpoints() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let marker_file = temp_dir.path().join("progress_emitted.marker");
    let marker_str = marker_file.display().to_string().replace('\\', "/");

    let prog_script = temp_dir.path().join("progress_then_hang.py");
    let script_content = format!(
        r#"
import sys, time
for i in range(3):
    print(f"[PROGRESS:{{(i+1)*25}}%] Step {{i+1}}", flush=True)
    time.sleep(0.02)
with open("{marker_str}", "w") as f:
    f.write("ready")
time.sleep(30)
"#
    );
    std::fs::write(&prog_script, script_content).expect("write");

    let mut env = EnvLock::lock().await;
    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        prog_script.to_str().expect("path"),
    );

    let (ckpt_tx, mut ckpt_rx) = tokio::sync::mpsc::channel::<WorkerMessage>(32);
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
    let child_pid = Arc::new(AtomicU32::new(0));

    let spec = Generative3DSpec::new_prompt("Cancelled Progress").with_engine("triposr");
    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());

    // Trigger cancel only after the fake process writes its progress marker file
    let marker_clone = marker_file.clone();
    tokio::spawn(async move {
        let start = std::time::Instant::now();
        while !marker_clone.exists() && start.elapsed() < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = cancel_tx.send("User cancellation requested".to_string());
    });

    let res = runner
        .execute_task_with_checkpoint(&task, None, Some(ckpt_tx), Some(child_pid), Some(cancel_rx))
        .await;

    assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
    let stdout_str = String::from_utf8_lossy(res.stdout.as_slice());
    assert!(
        stdout_str.contains("[PROGRESS:"),
        "stdout must contain progress lines emitted prior to cancellation, got: {stdout_str}"
    );

    let mut checkpoints = Vec::new();
    while let Ok(ckpt) = ckpt_rx.try_recv() {
        checkpoints.push(ckpt);
    }
    assert_eq!(
        checkpoints.len(),
        0,
        "Cancelled dead run must send 0 checkpoints, received {}",
        checkpoints.len()
    );
}

// ============================================================================
// Test 13: 3D Workload Omits Output Artifact Collection (Items 7 & 8)
// ============================================================================

#[tokio::test]
async fn test_sec5_3d_workload_omits_output_artifacts() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let valid_script = temp_dir.path().join("valid_engine.py");
    let script_content = r#"
import sys
if "--output" in sys.argv:
    out_idx = sys.argv.index("--output")
    out_path = sys.argv[out_idx + 1]
    with open(out_path, "wb") as f:
        f.write(b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00")
with open("extra_artifact.txt", "w") as f:
    f.write("extra artifact")
sys.exit(0)
"#;
    std::fs::write(&valid_script, script_content).expect("write");

    let mut env = EnvLock::lock().await;
    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        valid_script.to_str().expect("path"),
    );

    let spec = Generative3DSpec::new_prompt("Output test").with_engine("triposr");
    let mut task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
    task.expected_outputs = vec!["extra_artifact.txt".to_string()];

    let res = runner.execute_task(&task, None, None).await;
    assert!(res.is_success());
    assert!(
        res.artifacts.is_empty(),
        "Artifacts must be empty for 3D path"
    );
}

// ============================================================================
// Test 14: Unreadable Output Directory Fails For Both Engines (Review Item 1)
// ============================================================================

#[tokio::test]
async fn test_sec5_unreadable_output_directory_fails_for_both_engines() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let mock_script = temp_dir.path().join("dir_output_engine.py");
    let script_content = r#"
import os
import sys
if "--output" in sys.argv:
    out_idx = sys.argv.index("--output")
    out_path = sys.argv[out_idx + 1]
    os.mkdir(out_path)
sys.exit(0)
"#;
    std::fs::write(&mock_script, script_content).expect("write mock script");

    let mut env = EnvLock::lock().await;
    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        mock_script.to_str().expect("path"),
    );

    for engine in ["triposr", "auto"] {
        let spec = Generative3DSpec::new_prompt("Dir Output Test").with_engine(engine);
        let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
        let res = runner.execute_task(&task, None, None).await;

        assert_eq!(
            res.exit_code, EXIT_CODE_GENERAL_ERROR,
            "Exit code must be 1 for engine {engine}"
        );
        let err = res.error.as_deref().unwrap_or("");
        assert!(
            err.starts_with("Failed to read generated .glb asset"),
            "Error for engine {engine} must start with 'Failed to read generated .glb asset', got: '{err}'"
        );
        assert!(
            res.asset_bytes.is_none(),
            "asset_bytes must be None for engine {engine}"
        );
    }
}

// ============================================================================
// Test 15: Missing Output File Falls Back Under Auto and Fails Under Explicit
// ============================================================================

#[tokio::test]
async fn test_sec5_missing_output_file_falls_back_under_auto_and_fails_under_explicit() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let mock_script = temp_dir.path().join("missing_output_engine.py");
    let script_content = "import sys\nsys.exit(0)\n";
    std::fs::write(&mock_script, script_content).expect("write mock script");

    let mut env = EnvLock::lock().await;
    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        mock_script.to_str().expect("path"),
    );

    // Explicit engine 'triposr' must fail with missing file error
    {
        let spec = Generative3DSpec::new_prompt("Missing Output Explicit").with_engine("triposr");
        let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
        let res = runner.execute_task(&task, None, None).await;

        assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR);
        let err = res.error.as_deref().unwrap_or("");
        assert!(
            err.starts_with("Process succeeded but expected output file"),
            "Expected missing file error, got: '{err}'"
        );
        assert!(res.asset_bytes.is_none());
    }

    // Engine 'auto' must fall back to Tier 0 SDF and succeed
    {
        let spec = Generative3DSpec::new_prompt("Missing Output Auto").with_engine("auto");
        let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
        let res = runner.execute_task(&task, None, None).await;

        assert!(
            res.is_success(),
            "Auto must fall back to Tier 0 SDF on missing output"
        );
        assert!(res.asset_bytes.is_some());
        assert!(res.asset_bytes.unwrap().as_slice().starts_with(b"glTF"));
    }
}

// ============================================================================
// Test 16: FIFO or Non-Regular Output File Fails For Both Engines (Review Item 1)
// ============================================================================

#[tokio::test]
async fn test_sec5_fifo_or_non_regular_output_fails_for_both_engines() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");

    let mock_script = temp_dir.path().join("fifo_output_engine.py");
    let script_content = r#"
import os
import sys
if "--output" in sys.argv:
    out_idx = sys.argv.index("--output")
    out_path = sys.argv[out_idx + 1]
    if hasattr(os, "mkfifo"):
        os.mkfifo(out_path)
    else:
        os.mkdir(out_path)
sys.exit(0)
"#;
    std::fs::write(&mock_script, script_content).expect("write mock script");

    let mut env = EnvLock::lock().await;
    env.set(
        "OXIDESWARM_GEN3D_SCRIPT",
        mock_script.to_str().expect("path"),
    );

    for engine in ["triposr", "auto"] {
        let spec = Generative3DSpec::new_prompt("FIFO Output Test").with_engine(engine);
        let task = Task::new(
            TaskSpec::Generative3D(spec),
            TaskRequirements {
                timeout_secs: 5,
                ..Default::default()
            },
        );
        let start = Instant::now();
        let res = runner.execute_task(&task, None, None).await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(4),
            "Task with FIFO output must fail promptly without hanging, took {elapsed:?}"
        );
        assert_eq!(
            res.exit_code, EXIT_CODE_GENERAL_ERROR,
            "Exit code must be 1 for engine {engine}"
        );
        let err = res.error.as_deref().unwrap_or("");
        assert!(
            err.starts_with("Failed to read generated .glb asset"),
            "Error for engine {engine} must start with 'Failed to read generated .glb asset', got: '{err}'"
        );
        assert!(
            res.asset_bytes.is_none(),
            "asset_bytes must be None for engine {engine}"
        );
    }
}
