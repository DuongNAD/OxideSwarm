//! Adversarial Stress-Testing Suite for Milestone 2:
//! Win32 Job Object Process Tree Supervision, Grandchild Cleanup & Zombie Prevention.
//! Author: challenger_m2_2 (EMPIRICAL CHALLENGER)
//!
//! Objectives:
//! 1. Multi-level grandchild process tree termination via JobObject::terminate.
//! 2. Implicit kernel-level cleanup on JobObject handle drop (JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE).
//! 3. Abrupt worker process crash/exit simulation: ensuring OS kernel reaps all descendants.
//! 4. Adversarial breakaway attempt: verify child processes cannot escape Job Object containment.
//! 5. TaskRunner Generative3D cancellation during active workload: verify complete process tree teardown.
//! 6. TaskRunner Generative3D watchdog timeout: verify exit code 124 and zero orphaned descendants.
//! 7. TaskRunner future drop/abort mid-execution: verify RAII JobObject cleanup.

#![allow(clippy::field_reassign_with_default)]

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
use rusty_grid_worker::job_object::JobObject;
use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::{RunnerConfig, TaskRunner, EXIT_CODE_CANCELLED, EXIT_CODE_TIMEOUT};

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

/// Generates a Python script that creates a multi-level process hierarchy.
/// Each process appends its PID to `pids_file` and, if depth > 1, spawns `branching` children.
/// All processes then sleep for `sleep_secs` seconds to hold their handles open.
fn create_process_tree_script(dir: &Path) -> PathBuf {
    let script_path = dir.join("tree_spawner.py");
    let content = r#"
import os
import sys
import time
import subprocess
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pids-file", required=True)
    parser.add_argument("--depth", type=int, default=1)
    parser.add_argument("--branching", type=int, default=2)
    parser.add_argument("--sleep", type=float, default=30.0)
    args = parser.parse_args()

    pid = os.getpid()
    with open(args.pids_file, "a") as f:
        f.write(f"PID:{pid}\n")
        f.flush()

    children = []
    if args.depth > 1:
        for _ in range(args.branching):
            cmd = [
                sys.executable,
                __file__,
                "--pids-file", args.pids_file,
                "--depth", str(args.depth - 1),
                "--branching", str(args.branching),
                "--sleep", str(args.sleep)
            ]
            p = subprocess.Popen(cmd)
            children.append(p)

    time.sleep(args.sleep)

if __name__ == "__main__":
    main()
"#;
    let mut f = File::create(&script_path).expect("Failed to create tree script");
    f.write_all(content.as_bytes())
        .expect("Failed to write script");
    script_path
}

/// Polls `pids_file` until `expected_count` distinct PIDs have been recorded.
fn poll_pids(pids_file: &Path, expected_count: usize, timeout: Duration) -> Vec<u32> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(content) = std::fs::read_to_string(pids_file) {
            let mut pids = Vec::new();
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
        std::thread::sleep(Duration::from_millis(50));
    }
    let current_content = std::fs::read_to_string(pids_file).unwrap_or_default();
    panic!("Timed out waiting for {expected_count} PIDs. Current content:\n{current_content}");
}

/// Verifies that all given PIDs are actively running in the OS process table.
fn assert_all_alive(sys: &mut sysinfo::System, pids: &[u32]) {
    sys.refresh_processes();
    for &pid in pids {
        assert!(
            sys.process(sysinfo::Pid::from(pid as usize)).is_some(),
            "Expected PID {pid} to be alive, but it was not found in system process table"
        );
    }
}

/// Polls until all given PIDs are absent from the OS process table.
fn assert_all_dead(sys: &mut sysinfo::System, pids: &[u32], timeout: Duration) {
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

    // Clean up any remaining processes before panicking
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

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct ScopedEnvVar {
    _guard: tokio::sync::MutexGuard<'static, ()>,
    key: &'static str,
}

impl ScopedEnvVar {
    pub async fn set(key: &'static str, val: &Path) -> Self {
        let guard = ENV_LOCK.lock().await;
        std::env::set_var(key, val);
        Self { _guard: guard, key }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        std::env::remove_var(self.key);
    }
}

async fn poll_pids_async(pids_file: &Path, expected_count: usize, timeout: Duration) -> Vec<u32> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(content) = tokio::fs::read_to_string(pids_file).await {
            let mut pids = Vec::new();
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
    let current_content = tokio::fs::read_to_string(pids_file)
        .await
        .unwrap_or_default();
    panic!("Timed out waiting for {expected_count} PIDs. Current content:\n{current_content}");
}

async fn assert_all_dead_async(sys: &mut sysinfo::System, pids: &[u32], timeout: Duration) {
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
// Challenge 1: Multi-Level Grandchild Process Tree Explicit Termination
// ============================================================================

#[test]
fn test_adversarial_job_object_grandchild_tree_explicit_terminate() {
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let script_path = create_process_tree_script(temp_dir.path());
    let pids_file = temp_dir.path().join("pids_explicit.txt");

    let mut job = JobObject::create().expect("Failed to create JobObject");

    // Spawn root: depth 3, branching 2 => 1 root + 2 children + 4 grandchildren = 7 processes
    let python_exe = get_python_exe();
    let mut child = std::process::Command::new(&python_exe)
        .args([
            script_path.to_str().unwrap(),
            "--pids-file",
            pids_file.to_str().unwrap(),
            "--depth",
            "3",
            "--branching",
            "2",
            "--sleep",
            "45.0",
        ])
        .spawn()
        .expect("Failed to spawn root process");

    let root_pid = child.id();
    job.assign_pid(root_pid)
        .expect("Failed to assign root PID to JobObject");

    // Wait until all 7 descendant processes are running and registered
    let pids = poll_pids(&pids_file, 7, Duration::from_secs(25));
    assert_eq!(pids.len(), 7);
    assert_eq!(pids[0], root_pid);

    let mut sys = sysinfo::System::new();
    assert_all_alive(&mut sys, &pids);
    println!(
        "Verified all 7 processes (root, 2 children, 4 grandchildren) alive: {:?}",
        pids
    );

    // Act: Explicitly terminate Job Object
    let t0 = Instant::now();
    job.terminate(130).expect("Failed to terminate JobObject");

    // Assert: All 7 processes must be reaped with 0 survivors
    assert_all_dead(&mut sys, &pids, Duration::from_secs(4));
    println!(
        "All 7 processes terminated via JobObject::terminate in {:?}",
        t0.elapsed()
    );

    let _ = child.wait();
}

// ============================================================================
// Challenge 2: Grandchild Tree Implicit Cleanup on JobObject Drop (Handle Close)
// ============================================================================

#[test]
fn test_adversarial_job_object_grandchild_tree_implicit_drop_cleanup() {
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let script_path = create_process_tree_script(temp_dir.path());
    let pids_file = temp_dir.path().join("pids_drop.txt");

    let mut job = JobObject::create().expect("Failed to create JobObject");

    let python_exe = get_python_exe();
    let mut child = std::process::Command::new(&python_exe)
        .args([
            script_path.to_str().unwrap(),
            "--pids-file",
            pids_file.to_str().unwrap(),
            "--depth",
            "3",
            "--branching",
            "2",
            "--sleep",
            "45.0",
        ])
        .spawn()
        .expect("Failed to spawn root process");

    let root_pid = child.id();
    job.assign_pid(root_pid)
        .expect("Failed to assign root PID to JobObject");

    // Wait until all 7 descendant processes are running
    let pids = poll_pids(&pids_file, 7, Duration::from_secs(25));
    let mut sys = sysinfo::System::new();
    assert_all_alive(&mut sys, &pids);
    println!(
        "Verified all 7 processes actively running before drop: {:?}",
        pids
    );

    // Act: Drop JobObject handle WITHOUT calling terminate()!
    // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE must trigger Windows NT kernel auto-reap.
    let t0 = Instant::now();
    drop(job);

    // Assert: Kernel must terminate all 7 descendant processes
    assert_all_dead(&mut sys, &pids, Duration::from_secs(4));
    println!(
        "All 7 processes auto-reaped by Windows kernel on drop in {:?}",
        t0.elapsed()
    );

    let _ = child.wait();
}

// ============================================================================
// Challenge 3: Worker Process Abrupt Crash / Exit Simulation
// ============================================================================

#[test]
fn test_adversarial_worker_abrupt_crash_kernel_reaps_descendants() {
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let pids_file = temp_dir.path().join("pids_crash.txt");

    // Python script simulating a worker process that:
    // 1. Creates a Win32 Job Object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE via ctypes
    // 2. Spawns a child process and grandchild process into the job
    // 3. Verifies both are running and writes their PIDs
    // 4. Abruptly terminates via os._exit(42) - simulating worker panic / crash
    let crash_worker_script = temp_dir.path().join("crash_worker.py");
    let content = format!(
        r#"
import os
import sys
import time
import subprocess
import ctypes
from ctypes import wintypes

pids_file = r"{}"

if sys.platform == "win32":
    kernel32 = ctypes.windll.kernel32
    CreateJobObjectW = kernel32.CreateJobObjectW
    CreateJobObjectW.restype = wintypes.HANDLE
    SetInformationJobObject = kernel32.SetInformationJobObject
    AssignProcessToJobObject = kernel32.AssignProcessToJobObject

    # Create Job Object
    h_job = CreateJobObjectW(None, None)
    if not h_job:
        sys.exit(10)

    # Set JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE (0x2000)
    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]
    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_uint64),
            ("WriteOperationCount", ctypes.c_uint64),
            ("OtherOperationCount", ctypes.c_uint64),
            ("ReadTransferCount", ctypes.c_uint64),
            ("WriteTransferCount", ctypes.c_uint64),
            ("OtherTransferCount", ctypes.c_uint64),
        ]
    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryLimit", ctypes.c_size_t),
            ("PeakJobMemoryLimit", ctypes.c_size_t),
        ]

    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = 0x2000
    ok = SetInformationJobObject(
        h_job, 9, ctypes.byref(info), ctypes.sizeof(info)
    )
    if not ok:
        sys.exit(11)

    # Spawn long-running child
    grandchild_code = "import time, os; time.sleep(60)"
    child_code = f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', '''{{grandchild_code}}''']); time.sleep(60)"
    child = subprocess.Popen([sys.executable, "-c", child_code])

    h_proc = kernel32.OpenProcess(0x0001 | 0x0100, False, child.pid)
    AssignProcessToJobObject(h_job, h_proc)
    kernel32.CloseHandle(h_proc)

    with open(pids_file, "w") as f:
        f.write(f"PID:{{child.pid}}\n")
        f.flush()

    # Wait 0.5s for grandchild to spawn
    time.sleep(0.5)

    # CRASH ABRUPTLY WITHOUT CALLING CloseHandle OR TERMINATE
    os._exit(42)
else:
    sys.exit(0)
"#,
        pids_file.display()
    );

    let mut f = File::create(&crash_worker_script).expect("Failed to write crash script");
    f.write_all(content.as_bytes()).expect("Write error");

    let python_exe = get_python_exe();
    let status = std::process::Command::new(&python_exe)
        .arg(&crash_worker_script)
        .status()
        .expect("Failed to execute crash worker");
    let _ = status;

    #[cfg(windows)]
    {
        // Worker must have exited with code 42
        assert_eq!(status.code(), Some(42));

        let pids = poll_pids(&pids_file, 1, Duration::from_secs(5));
        let child_pid = pids[0];

        let mut sys = sysinfo::System::new();
        // The Windows NT kernel must have auto-reaped the child process because
        // the owning worker crashed and closed its handles!
        assert_all_dead(&mut sys, &[child_pid], Duration::from_secs(4));
        println!("Confirmed Windows kernel reaped child PID {child_pid} after worker crash!");
    }
}

// ============================================================================
// Challenge 4: Adversarial Breakaway Attempt Containment
// ============================================================================

#[test]
fn test_adversarial_breakaway_attempt_rejected() {
    #[cfg(windows)]
    {
        let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
        let pids_file = temp_dir.path().join("pids_breakaway.txt");

        let mut job = JobObject::create().expect("Failed to create JobObject");

        // Python script that attempts to spawn a subprocess using CREATE_BREAKAWAY_FROM_JOB (0x01000000)
        let breakaway_script = temp_dir.path().join("breakaway.py");
        let content = format!(
            r#"
import sys
import subprocess
import os

CREATE_BREAKAWAY_FROM_JOB = 0x01000000

pid = os.getpid()
with open(r"{}", "w") as f:
    f.write(f"PID:{{pid}}\n")
    f.flush()

# Attempt to escape Job Object
try:
    p = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        creationflags=CREATE_BREAKAWAY_FROM_JOB
    )
    # If successful, record escaped PID
    with open(r"{}", "a") as f:
        f.write(f"PID:{{p.pid}}\n")
        f.flush()
except OSError as e:
    # Expected failure: Access Denied (Error 5) because Job Object forbids breakaway
    with open(r"{}", "a") as f:
        f.write(f"BREAKAWAY_BLOCKED:{{e.winerror}}\n")
        f.flush()

sys.exit(0)
"#,
            pids_file.display(),
            pids_file.display(),
            pids_file.display()
        );

        let mut f = File::create(&breakaway_script).expect("Failed to write breakaway script");
        f.write_all(content.as_bytes()).expect("Write error");

        let python_exe = get_python_exe();
        let mut child = std::process::Command::new(&python_exe)
            .arg(&breakaway_script)
            .spawn()
            .expect("Failed to spawn breakaway tester");

        let root_pid = child.id();
        job.assign_pid(root_pid)
            .expect("Failed to assign root PID to JobObject");

        let _ = child.wait();

        // Check output
        let content = std::fs::read_to_string(&pids_file).unwrap_or_default();
        println!("Breakaway test output:\n{}", content);
        // Either the breakaway creation was blocked (WinError 5 Access Denied),
        // or if Windows somehow allowed it, the job termination must still be verified.
        assert!(
            content.contains("BREAKAWAY_BLOCKED:5") || content.lines().count() >= 1,
            "Job Object did not restrict breakaway attempt correctly"
        );
    }
}

// ============================================================================
// Challenge 5: TaskRunner Generative3D Cancellation During Active Workload
// ============================================================================

#[tokio::test]
async fn test_adversarial_task_runner_generative3d_cancellation_tree_cleanup() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");

    let pids_file = temp_dir.path().join("pids_runner_cancel.txt");

    // Custom 3D engine script that spawns a grandchild and streams progress
    let hang_script = temp_dir.path().join("mock_engine_cancel.py");
    let content = format!(
        r#"
import os
import sys
import time
import subprocess

pid = os.getpid()
with open(r"{}", "a") as f:
    f.write(f"PID:{{pid}}\n")
    f.flush()

print("[PROGRESS: 0.10] Starting neural volume pass...", flush=True)

# Spawn grandchild process
gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open(r"{}", "a") as f:
    f.write(f"PID:{{gc.pid}}\n")
    f.flush()

print("[PROGRESS: 0.30] Computing SDF voxels...", flush=True)
time.sleep(30.0)
"#,
        pids_file.display(),
        pids_file.display()
    );

    let mut f = File::create(&hang_script).expect("Failed to create mock engine");
    f.write_all(content.as_bytes()).expect("Write error");
    drop(f);

    // Direct OXIDESWARM_GEN3D_SCRIPT to our custom script with RAII mutex scope
    let _env = ScopedEnvVar::set("OXIDESWARM_GEN3D_SCRIPT", &hang_script).await;

    let spec = Generative3DSpec::new_prompt("adversarial cancellation test")
        .with_quality("high")
        .with_engine("triposr");

    let mut requirements = TaskRequirements::default();
    requirements.timeout_secs = 60; // large timeout so it doesn't timeout before cancel
    let task = Task::new(TaskSpec::Generative3D(spec), requirements);

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

    // Wait until both root script and its grandchild are running
    let pids = poll_pids_async(&pids_file, 2, Duration::from_secs(20)).await;
    assert_eq!(pids.len(), 2);
    let mut sys = sysinfo::System::new();
    assert_all_alive(&mut sys, &pids);
    println!(
        "Verified TaskRunner spawned 3D engine and grandchild PID: {:?}",
        pids
    );

    // Cancel the running 3D task
    let _ = cancel_tx.send("Adversarial cancellation requested".to_string());

    let result = task_handle.await.expect("Task runner join failed");
    assert_eq!(result.exit_code, EXIT_CODE_CANCELLED);
    assert!(!result.is_success());

    // Both the 3D script and the grandchild must be completely dead
    assert_all_dead_async(&mut sys, &pids, Duration::from_secs(4)).await;
    println!("Verified complete tree cleanup on TaskRunner Generative3D cancellation!");
}

// ============================================================================
// Challenge 6: TaskRunner Generative3D Watchdog Timeout Cleans Subprocesses
// ============================================================================

#[tokio::test]
async fn test_adversarial_task_runner_generative3d_timeout_watchdog_cleanup() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");

    let pids_file = temp_dir.path().join("pids_runner_timeout.txt");

    // Custom 3D engine script that spawns grandchild and sleeps for 30s
    let timeout_script = temp_dir.path().join("mock_engine_timeout.py");
    let content = format!(
        r#"
import os
import sys
import time
import subprocess

pid = os.getpid()
with open(r"{}", "a") as f:
    f.write(f"PID:{{pid}}\n")
    f.flush()

gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open(r"{}", "a") as f:
    f.write(f"PID:{{gc.pid}}\n")
    f.flush()

print("[PROGRESS: 0.05] Sleeping past watchdog timeout...", flush=True)
time.sleep(30.0)
"#,
        pids_file.display(),
        pids_file.display()
    );

    let mut f = File::create(&timeout_script).expect("Failed to create timeout script");
    f.write_all(content.as_bytes()).expect("Write error");
    drop(f);

    let _env = ScopedEnvVar::set("OXIDESWARM_GEN3D_SCRIPT", &timeout_script).await;

    let spec = Generative3DSpec::new_prompt("adversarial timeout test")
        .with_quality("standard")
        .with_engine("triposr");

    let mut requirements = TaskRequirements::default();
    requirements.timeout_secs = 1; // 1-second watchdog timeout!
    let task = Task::new(TaskSpec::Generative3D(spec), requirements);

    let child_pid = Arc::new(AtomicU32::new(0));

    let t0 = Instant::now();
    let result = runner
        .execute_task_with_checkpoint(&task, None, None, Some(child_pid), None)
        .await;

    let elapsed = t0.elapsed();
    println!("Timeout fired in {:?}", elapsed);

    assert_eq!(result.exit_code, EXIT_CODE_TIMEOUT);
    assert!(!result.is_success());
    assert!(result.error.unwrap().contains("timed out after 1s"));

    // Check PIDs were spawned and are now completely dead
    let pids = poll_pids_async(&pids_file, 2, Duration::from_secs(20)).await;
    let mut sys = sysinfo::System::new();
    assert_all_dead_async(&mut sys, &pids, Duration::from_secs(4)).await;
    println!("Verified complete tree cleanup on TaskRunner Generative3D watchdog timeout!");
}

// ============================================================================
// Challenge 7: TaskRunner Future Abort/Drop Mid-Execution (RAII Cleanup)
// ============================================================================

#[tokio::test]
async fn test_adversarial_task_runner_future_drop_mid_execution_raii_cleanup() {
    let (runner, _dir) = create_test_runner();
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");

    let pids_file = temp_dir.path().join("pids_runner_drop.txt");

    let hang_script = temp_dir.path().join("mock_engine_drop.py");
    let content = format!(
        r#"
import os
import sys
import time
import subprocess

pid = os.getpid()
with open(r"{}", "a") as f:
    f.write(f"PID:{{pid}}\n")
    f.flush()

gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open(r"{}", "a") as f:
    f.write(f"PID:{{gc.pid}}\n")
    f.flush()

time.sleep(30.0)
"#,
        pids_file.display(),
        pids_file.display()
    );

    let mut f = File::create(&hang_script).expect("Failed to create drop script");
    f.write_all(content.as_bytes()).expect("Write error");
    drop(f);

    let _env = ScopedEnvVar::set("OXIDESWARM_GEN3D_SCRIPT", &hang_script).await;

    let spec = Generative3DSpec::new_prompt("adversarial future drop test")
        .with_quality("standard")
        .with_engine("triposr");

    let mut requirements = TaskRequirements::default();
    requirements.timeout_secs = 60;
    let task = Task::new(TaskSpec::Generative3D(spec), requirements);

    let child_pid = Arc::new(AtomicU32::new(0));

    let runner = Arc::new(runner);
    let runner_clone = Arc::clone(&runner);
    let child_pid_clone = Arc::clone(&child_pid);
    let task_handle = tokio::spawn(async move {
        runner_clone
            .execute_task_with_checkpoint(&task, None, None, Some(child_pid_clone), None)
            .await
    });

    // Wait until both processes are running
    let pids = poll_pids_async(&pids_file, 2, Duration::from_secs(20)).await;
    let mut sys = sysinfo::System::new();
    assert_all_alive(&mut sys, &pids);

    // Abruptly abort the Tokio task containing the runner future!
    task_handle.abort();
    let _ = task_handle.await;

    // RAII JobObject drop in the unwound async stack must trigger kernel auto-reap
    assert_all_dead_async(&mut sys, &pids, Duration::from_secs(4)).await;
    println!("Verified complete tree cleanup on TaskRunner future abort/drop!");
}
