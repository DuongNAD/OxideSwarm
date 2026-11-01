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
use rusty_grid_core::task::{Generative3DSpec, Task, TaskRequirements, TaskSpec};
use rusty_grid_worker::autorig::BlenderRigProvider;
use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::{RunnerConfig, TaskRunner, EXIT_CODE_CANCELLED};

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

    let dummy_input = b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00";
    let res = BlenderRigProvider::execute_rigging(
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

    let t0 = Instant::now();
    let res = BlenderRigProvider::execute_rigging(
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
// Test 4: 3D Task Grandchildren Die on Abort Cross-Platform
// ============================================================================

#[tokio::test]
async fn test_sec5_3d_task_grandchildren_die_on_abort_cross_platform() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let pids_file = temp_dir.path().join("pids_gen3d_tree.txt");

    let pids_path = pids_file.display().to_string().replace('\\', "/");
    let script = temp_dir.path().join("gen3d_spawner.py");
    let content = format!(
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
    let mut f = File::create(&script).expect("create script");
    f.write_all(content.as_bytes()).expect("write script");
    drop(f);

    let mut env = EnvLock::lock().await;
    env.set("OXIDESWARM_GEN3D_SCRIPT", script.to_str().expect("path"));

    let spec = Generative3DSpec::new_prompt("adversarial grandchild abort test")
        .with_quality("standard")
        .with_engine("triposr");
    let requirements = TaskRequirements {
        timeout_secs: 60,
        ..Default::default()
    };
    let task = Task::new(TaskSpec::Generative3D(spec), requirements);

    let (cancel_tx, cancel_rx) = oneshot::channel();
    let child_pid = Arc::new(AtomicU32::new(0));

    let runner = Arc::new(runner);
    let runner_clone = Arc::clone(&runner);
    let child_pid_clone = Arc::clone(&child_pid);
    let task_handle = tokio::spawn(async move {
        runner_clone
            .execute_task_with_checkpoint(
                &task,
                None,
                None,
                Some(child_pid_clone),
                Some(cancel_rx),
            )
            .await
    });

    // Wait until both child and grandchild processes are alive
    let pids = poll_pids_async(&pids_file, 2, Duration::from_secs(20)).await;
    assert_eq!(
        pids.len(),
        2,
        "Expected 2 processes (parent and grandchild), found {:?}",
        pids
    );

    let mut sys = sysinfo::System::new();
    sys.refresh_processes();
    for &pid in &pids {
        assert!(
            sys.process(sysinfo::Pid::from(pid as usize)).is_some(),
            "PID {pid} must be running before cancellation"
        );
    }

    // Act: Send cancellation signal
    let _ = cancel_tx.send("task cancelled by operator".to_string());
    let res = task_handle.await.expect("task join failed");

    assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);

    // Assert: Both child and grandchild must be terminated within 4s
    assert_all_dead_async(&pids, Duration::from_secs(4)).await;
    println!("Verified complete 3D tree cleanup on cancellation!");
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

    let fake_bin = fake_blender_path.clone();
    let task_handle = tokio::spawn(async move {
        BlenderRigProvider::execute_rigging(
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
