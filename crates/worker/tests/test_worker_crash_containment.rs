//! Integration tests for FEAT-2 (Design 0031): Containment after a worker crash.
//!
//! Verifies:
//! 1. Direct `waitpid(-1)` (`os.wait()`) inside a supervised task on Unix returns `ECHILD`
//!    immediately, proving that the double-forked lifeline supervisor is reparented to PID 1
//!    rather than remaining a child of the task leader.
//! 2. Normal task completion terminates a background child that escaped the task's process group
//!    via `setsid()` within 1 s.
//! 3. `SIGKILL` (`kill -9` on Unix, `TerminateProcess` on Windows) of a live worker subprocess
//!    terminates its running task's child, grandchild, and `setsid()` descendants within 5 s.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::{RunnerConfig, TaskRunner};

mod common;

use common::{assert_all_alive, assert_all_dead_async, get_python_exe, poll_pids_async};

const WORKER_HELPER_ENV: &str = "OXIDESWARM_CRASH_CONTAINMENT_HELPER";
const WORKER_HELPER_PIDS_ENV: &str = "OXIDESWARM_CRASH_CONTAINMENT_PIDS_FILE";
const WORKER_HELPER_SANDBOX_ENV: &str = "OXIDESWARM_CRASH_CONTAINMENT_SANDBOX_DIR";

fn create_test_runner() -> (TaskRunner, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let sandbox_config = SandboxConfig::new(temp_dir.path().to_path_buf());
    let runner_config = RunnerConfig::new(sandbox_config);
    let mut caps = WorkerCapabilities::new("test_worker", 4, 8192, false, false, None);
    caps.has_cuda = false;
    let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, runner_config);
    (runner, temp_dir)
}

/// Test 1 (Review item 1): Direct `waitpid(-1)` (`os.wait()`) inside a supervised task returns
/// `ECHILD` immediately, proving the double-forked supervisor is not a child of the task leader.
#[tokio::test]
async fn test_task_waitpid_minus_one_returns_echild_immediately() {
    let (runner, _sandbox_dir) = create_test_runner();
    let script_dir = tempfile::tempdir().expect("script tempdir");
    let script_path = script_dir.path().join("check_waitpid.py");

    let script = r#"
import errno
import os
import sys

if hasattr(os, "wait"):
    try:
        pid, status = os.wait()
        sys.stderr.write(f"UNEXPECTED_CHILD:{pid}:{status}\n")
        sys.exit(2)
    except ChildProcessError as exc:
        if exc.errno != errno.ECHILD:
            sys.stderr.write(f"UNEXPECTED_ERRNO:{exc.errno}\n")
            sys.exit(3)
    except OSError as exc:
        if exc.errno != errno.ECHILD:
            sys.stderr.write(f"UNEXPECTED_OSERROR:{exc.errno}\n")
            sys.exit(4)

print("WAITPID_ECHILD_OK", flush=True)
sys.exit(0)
"#;
    let mut f = File::create(&script_path).expect("create script");
    f.write_all(script.as_bytes()).expect("write script");
    drop(f);

    let py_exe = get_python_exe();
    let spec = TaskSpec::Command {
        program: py_exe.to_string_lossy().to_string(),
        args: vec![script_path.to_string_lossy().to_string()],
        env: Default::default(),
        working_dir: None,
        stdin: None,
    };
    let requirements = TaskRequirements {
        timeout_secs: 15,
        ..Default::default()
    };
    let task = Task::new(spec, requirements);

    let res = runner.execute_task(&task, None, None).await;
    assert!(
        res.is_success(),
        "waitpid(-1) check failed: exit_code={}, error={:?}, stderr={}",
        res.exit_code,
        res.error,
        res.stderr_str()
    );
    assert!(
        res.stdout_str().contains("WAITPID_ECHILD_OK"),
        "expected WAITPID_ECHILD_OK in stdout, got: {}",
        res.stdout_str()
    );
}

/// Test 2 (Review item 7): Normal task completion kills a background child that called `setsid()`
/// within 1 s.
#[tokio::test]
async fn test_normal_task_completion_kills_setsid_survivor_within_one_second() {
    let (runner, _sandbox_dir) = create_test_runner();
    let work_dir = tempfile::tempdir().expect("work tempdir");
    let pids_file = work_dir.path().join("setsid_survivor.pids");
    let script_path = work_dir.path().join("spawn_setsid.py");

    let script = r#"
import os
import subprocess
import sys
import time

pids_file = sys.argv[1]

child_code = """
import os, sys, time
if hasattr(os, 'setsid'):
    os.setsid()
with open(sys.argv[1], 'a', encoding='utf-8') as f:
    f.write(f'PID:{os.getpid()}\\n')
    f.flush()
    os.fsync(f.fileno())
time.sleep(120)
"""

subprocess.Popen(
    [sys.executable, "-c", child_code, pids_file],
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    close_fds=True,
)

deadline = time.time() + 10.0
while time.time() < deadline:
    if os.path.exists(pids_file):
        with open(pids_file, "r", encoding="utf-8") as f:
            if "PID:" in f.read():
                break
    time.sleep(0.02)

print("LEADER_DONE", flush=True)
sys.exit(0)
"#;
    let mut f = File::create(&script_path).expect("create script");
    f.write_all(script.as_bytes()).expect("write script");
    drop(f);

    let py_exe = get_python_exe();
    let spec = TaskSpec::Command {
        program: py_exe.to_string_lossy().to_string(),
        args: vec![
            script_path.to_string_lossy().to_string(),
            pids_file.to_string_lossy().to_string(),
        ],
        env: Default::default(),
        working_dir: None,
        stdin: None,
    };
    let requirements = TaskRequirements {
        timeout_secs: 20,
        ..Default::default()
    };
    let task = Task::new(spec, requirements);

    let res = runner.execute_task(&task, None, None).await;
    assert!(
        res.is_success(),
        "setsid task leader failed: exit_code={}, error={:?}, stderr={}",
        res.exit_code,
        res.error,
        res.stderr_str()
    );

    let pids = poll_pids_async(&pids_file, 1, Duration::from_secs(2)).await;
    assert_eq!(
        pids.len(),
        1,
        "expected setsid child PID to be recorded in {}",
        pids_file.display()
    );

    assert_all_dead_async(&pids, Duration::from_secs(1)).await;
}

/// Entrypoint invoked when the test binary spawns itself as a disposable worker process for Test 3.
#[tokio::test]
async fn worker_subprocess_helper() {
    if std::env::var(WORKER_HELPER_ENV).ok().as_deref() != Some("1") {
        return;
    }

    let pids_file = PathBuf::from(
        std::env::var(WORKER_HELPER_PIDS_ENV).expect("missing WORKER_HELPER_PIDS_ENV"),
    );
    let sandbox_dir = PathBuf::from(
        std::env::var(WORKER_HELPER_SANDBOX_ENV).expect("missing WORKER_HELPER_SANDBOX_ENV"),
    );

    let sandbox_config = SandboxConfig::new(sandbox_dir.clone());
    let runner_config = RunnerConfig::new(sandbox_config);
    let mut caps = WorkerCapabilities::new("crash_victim_worker", 4, 8192, false, false, None);
    caps.has_cuda = false;
    let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, runner_config);

    let script_path = sandbox_dir.join("victim_tree.py");
    let script = r#"
import os
import subprocess
import sys
import time

pids_file = sys.argv[1]

def record_pid(pid):
    with open(pids_file, "a", encoding="utf-8") as f:
        f.write(f"PID:{pid}\n")
        f.flush()
        os.fsync(f.fileno())

# 1. Record direct task leader (child) PID
record_pid(os.getpid())

# 2. Spawn in-group grandchild
grandchild_code = """
import os, sys, time
with open(sys.argv[1], 'a', encoding='utf-8') as f:
    f.write(f'PID:{os.getpid()}\\n')
    f.flush()
    os.fsync(f.fileno())
time.sleep(300)
"""
subprocess.Popen(
    [sys.executable, "-c", grandchild_code, pids_file],
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)

# 3. Spawn setsid escaped process
setsid_code = """
import os, sys, time
if hasattr(os, 'setsid'):
    os.setsid()
with open(sys.argv[1], 'a', encoding='utf-8') as f:
    f.write(f'PID:{os.getpid()}\\n')
    f.flush()
    os.fsync(f.fileno())
time.sleep(300)
"""
subprocess.Popen(
    [sys.executable, "-c", setsid_code, pids_file],
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    close_fds=True,
)

time.sleep(300)
"#;
    let mut f = File::create(&script_path).expect("create victim script");
    f.write_all(script.as_bytes()).expect("write victim script");
    drop(f);

    let py_exe = get_python_exe();
    let spec = TaskSpec::Command {
        program: py_exe.to_string_lossy().to_string(),
        args: vec![
            script_path.to_string_lossy().to_string(),
            pids_file.to_string_lossy().to_string(),
        ],
        env: Default::default(),
        working_dir: None,
        stdin: None,
    };
    let requirements = TaskRequirements {
        timeout_secs: 300,
        ..Default::default()
    };
    let task = Task::new(spec, requirements);

    let _ = runner.execute_task(&task, None, None).await;
}

/// Test 3 ("Done when" of FEAT-2): Starts a worker subprocess, runs a script with a child,
/// grandchild, and `setsid()` descendant, sends `SIGKILL` (`kill -9`) to the worker, and asserts
/// all descendants are dead within 5 s.
#[tokio::test]
async fn test_worker_sigkill_terminates_child_grandchild_and_setsid_descendants_within_five_seconds(
) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let pids_file = temp_dir.path().join("crash_descendants.pids");
    let sandbox_dir = temp_dir.path().join("worker_sandbox");
    std::fs::create_dir_all(&sandbox_dir).expect("create sandbox dir");

    let current_exe = std::env::current_exe().expect("current_exe");
    let mut worker_proc = tokio::process::Command::new(&current_exe)
        .args(["--exact", "worker_subprocess_helper", "--nocapture"])
        .env(WORKER_HELPER_ENV, "1")
        .env(WORKER_HELPER_PIDS_ENV, &pids_file)
        .env(WORKER_HELPER_SANDBOX_ENV, &sandbox_dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn worker subprocess helper");

    #[cfg_attr(not(unix), allow(unused_variables))]
    let worker_pid = worker_proc.id().expect("worker pid");

    let pids = poll_pids_async(&pids_file, 3, Duration::from_secs(20)).await;
    if pids.len() < 3 {
        let _ = worker_proc.kill().await;
        panic!(
            "Timed out waiting for 3 descendant PIDs (child, grandchild, setsid); got {:?}",
            pids
        );
    }

    let mut sys = sysinfo::System::new();
    assert_all_alive(&mut sys, &pids);

    // Send untrappable SIGKILL (kill -9) to the worker process.
    #[cfg(unix)]
    unsafe {
        let rc = libc::kill(worker_pid as libc::pid_t, libc::SIGKILL);
        assert_eq!(rc, 0, "kill(worker_pid, SIGKILL) failed");
    }
    #[cfg(not(unix))]
    {
        let _ = worker_proc.start_kill();
    }
    let _ = worker_proc.wait().await;

    // All 3 descendants (child, grandchild, setsid) must be terminated within 5 s.
    assert_all_dead_async(&pids, Duration::from_secs(5)).await;
}
