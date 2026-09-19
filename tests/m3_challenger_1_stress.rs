//! Adversarial stress test suite for Milestone 3 (M3: Worker Task Execution & Sandboxing).
//!
//! Authored by Challenger 1 to empirically challenge and verify:
//! 1. Path traversal attacks in `TaskSpec::RustCompilation` and `working_dir` (`../../`, `/etc/`, null bytes).
//! 2. Pipe buffer deadlock resistance: concurrent writes >1MB to both stdout and stderr simultaneously.
//! 3. Infinite stream resistance: child running `yes` or exceeding 10MB cap with continuous draining.
//! 4. Timeout watchdog precision: exact timeout enforcement, SIGKILL termination, and OS child process reaping.
//! 5. Cancellation under rapid race conditions: immediate cancellation, mid-flight cancellation, and burst races.
//! 6. Concurrency semaphore and telemetry integrity under hostile loads.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_worker::{
    sanitize_relative_path, RunnerConfig, SandboxConfig, SandboxError, TaskRunner, WorkerClient,
    WorkerConfig, EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_TIMEOUT,
};

/// Helper to check if a process is still alive in the OS process table.
#[cfg(unix)]
fn is_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    unsafe { kill(pid as i32, 0) == 0 }
}

#[cfg(not(unix))]
fn is_process_alive(_pid: u32) -> bool {
    false
}

fn create_test_capabilities(cores: usize, has_gpu: bool) -> WorkerCapabilities {
    WorkerCapabilities {
        name: format!("challenger-worker-{}", Uuid::new_v4()),
        cpu_cores: cores,
        ram_mb: 8192,
        has_gpu,
        is_simulated_gpu: has_gpu,
        gpu_device_name: if has_gpu {
            Some("Challenger Virtual GPU".into())
        } else {
            None
        },
        tags: vec!["adversarial_test".into()],
        mobile: None,
    }
}

fn create_test_runner(cores: usize, has_gpu: bool) -> (TaskRunner, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("create temp dir for runner");
    let sandbox_cfg = SandboxConfig::new(temp_dir.path());
    let runner_cfg = RunnerConfig::new(sandbox_cfg);
    let capabilities = create_test_capabilities(cores, has_gpu);
    let runner = TaskRunner::new(Uuid::new_v4(), capabilities, runner_cfg);
    (runner, temp_dir)
}

fn create_test_runner_with_cap(
    cores: usize,
    max_output_bytes: usize,
) -> (TaskRunner, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("create temp dir for runner");
    let sandbox_cfg = SandboxConfig::new(temp_dir.path());
    let runner_cfg = RunnerConfig::new(sandbox_cfg).with_max_output_bytes(max_output_bytes);
    let capabilities = create_test_capabilities(cores, false);
    let runner = TaskRunner::new(Uuid::new_v4(), capabilities, runner_cfg);
    (runner, temp_dir)
}

// =========================================================================
// 1. Path Traversal & Isolation Attack Tests
// =========================================================================

#[tokio::test]
async fn test_adversarial_path_traversal_rust_compilation_source_files() {
    let (runner, _tmp) = create_test_runner(2, false);

    // Hostile relative and absolute path attempts
    let hostile_paths = vec![
        "../../evil_escape.rs",
        "../../../etc/passwd",
        "/etc/shadow",
        "/tmp/malicious.rs",
        "src/../../../../etc/hosts",
        "sub/dir/../../../escaped.rs",
        "null\0byte_injection.rs",
        "src/nested\0/lib.rs",
    ];

    for hostile_path in hostile_paths {
        let mut sources = HashMap::new();
        sources.insert(
            hostile_path.to_string(),
            "pub fn pwned() -> bool { true }".to_string(),
        );

        let task = Task::new(
            TaskSpec::rust_compilation("pwn_crate", sources, vec![]),
            TaskRequirements::generic(1, 10),
        );

        let res = runner.execute_task(&task, None, None).await;

        assert!(
            !res.is_success(),
            "Hostile path '{}' should NOT succeed in compilation",
            hostile_path
        );
        assert_eq!(
            res.exit_code, EXIT_CODE_GENERAL_ERROR,
            "Hostile path '{}' must emit EXIT_CODE_GENERAL_ERROR",
            hostile_path
        );
        assert!(
            res.error.is_some(),
            "Hostile path '{}' must have an error message",
            hostile_path
        );

        // Verify sandbox base directory is clean of escaped files
        let leaked_file = runner.config().sandbox_config.base_dir.join(hostile_path);
        assert!(
            !leaked_file.exists(),
            "Leaked file must not exist at {}",
            leaked_file.display()
        );
    }
}

#[tokio::test]
async fn test_adversarial_path_traversal_rust_compilation_target_dir() {
    let (runner, _tmp) = create_test_runner(2, false);

    let hostile_target_dirs = vec![
        PathBuf::from("../../evil_target"),
        PathBuf::from("/tmp/unauthorized_target"),
        PathBuf::from("src/../../../../tmp/escaped_target"),
        PathBuf::from("target\0null_injection"),
    ];

    for hostile_target in hostile_target_dirs {
        let mut sources = HashMap::new();
        sources.insert(
            "src/lib.rs".to_string(),
            "pub fn safe() -> u32 { 42 }".to_string(),
        );

        let task = Task::new(
            TaskSpec::RustCompilation {
                crate_name: "target_attack_crate".to_string(),
                source_files: sources,
                compiler_flags: vec![],
                target_dir: Some(hostile_target.clone()),
            },
            TaskRequirements::generic(1, 10),
        );

        let res = runner.execute_task(&task, None, None).await;

        assert!(
            !res.is_success(),
            "Hostile target_dir '{:?}' must fail execution",
            hostile_target
        );
        assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR);
    }
}

#[tokio::test]
async fn test_adversarial_path_traversal_command_working_dir() {
    let (runner, _tmp) = create_test_runner(2, false);

    let hostile_working_dirs = vec![
        PathBuf::from("../../"),
        PathBuf::from("../../../"),
        PathBuf::from("/etc"),
        PathBuf::from("/tmp"),
        PathBuf::from("nested/../../../../"),
        PathBuf::from("foo\0bar"),
    ];

    for hostile_dir in hostile_working_dirs {
        let task = Task::new(
            TaskSpec::Command {
                program: "pwd".into(),
                args: vec![],
                env: HashMap::new(),
                working_dir: Some(hostile_dir.clone()),
                stdin: None,
            },
            TaskRequirements::generic(1, 5),
        );

        let res = runner.execute_task(&task, None, None).await;

        assert!(
            !res.is_success(),
            "Hostile working_dir '{:?}' must fail execution",
            hostile_dir
        );
        assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR);
        // Verify it did not execute in /etc or parent directories
        assert!(!res.stdout.starts_with("/etc"));
    }
}

#[tokio::test]
async fn test_sanitize_relative_path_edge_cases() {
    let base = Path::new("/sandbox/base/task-1");

    // 1. Valid subpaths
    assert!(sanitize_relative_path(base, Path::new("sub/dir/file.txt")).is_ok());
    assert!(sanitize_relative_path(base, Path::new("file.rs")).is_ok());
    assert!(sanitize_relative_path(base, Path::new("./local/file.rs")).is_ok());

    // 2. Traversal attempts
    let parent_err = sanitize_relative_path(base, Path::new("../outside.txt"));
    assert!(parent_err.is_err());
    assert!(matches!(
        parent_err.unwrap_err(),
        SandboxError::PathTraversal { .. }
    ));

    let deep_err = sanitize_relative_path(base, Path::new("a/b/../../../../outside.txt"));
    assert!(deep_err.is_err());
    assert!(matches!(
        deep_err.unwrap_err(),
        SandboxError::PathTraversal { .. }
    ));

    // 3. Absolute path attempts
    let abs_err = sanitize_relative_path(base, Path::new("/etc/passwd"));
    assert!(abs_err.is_err());
    assert!(matches!(
        abs_err.unwrap_err(),
        SandboxError::PathTraversal { .. }
    ));
}

// =========================================================================
// 2. Pipe Buffer Deadlock Resistance Tests (>1MB stdout/stderr)
// =========================================================================

#[tokio::test]
async fn test_pipe_buffer_deadlock_resistance_simultaneous_large_output() {
    let (runner, _tmp) = create_test_runner(2, false);

    // Python script that writes 2MB to stdout and 2MB to stderr simultaneously using two threads.
    // 32 chunks of 65536 bytes = 2,097,152 bytes for stdout AND 2,097,152 bytes for stderr.
    // If stdout and stderr are not drained concurrently, the 16KB/64KB OS pipe buffer
    // will fill immediately, leading to a permanent deadlock.
    let py_script = r#"
import sys, threading

def write_stdout():
    chunk = ("A" * 65536).encode("ascii")
    for _ in range(32):
        sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()

def write_stderr():
    chunk = ("B" * 65536).encode("ascii")
    for _ in range(32):
        sys.stderr.buffer.write(chunk)
    sys.stderr.buffer.flush()

t1 = threading.Thread(target=write_stdout)
t2 = threading.Thread(target=write_stderr)
t1.start()
t2.start()
t1.join()
t2.join()
"#;

    let task = Task::new(
        TaskSpec::command("python3", vec!["-c".into(), py_script.into()]),
        TaskRequirements::generic(1, 15), // 15 second timeout
    );

    // Test 1: Exit code 0 (success).
    // Verifies that writing 2MB to stderr does NOT deadlock the child process,
    // even though TaskResult::success discards stderr on exit code 0.
    let t0 = Instant::now();
    let res = runner.execute_task(&task, None, None).await;
    let elapsed = t0.elapsed();

    assert!(
        res.is_success(),
        "Concurrent pipe writing must succeed without deadlock; error: {:?}",
        res.error
    );
    assert_eq!(res.exit_code, 0);

    let expected_bytes = 32 * 65536; // 2 MB
    assert!(
        res.stdout.len() >= expected_bytes,
        "stdout captured {} bytes, expected >= {}",
        res.stdout.len(),
        expected_bytes
    );
    assert!(res.stdout.starts_with('A'));
    assert!(
        elapsed < Duration::from_secs(10),
        "Execution completed in {:?}, not hanging on pipe deadlock",
        elapsed
    );

    // Note: On exit code 0, TaskResult::success discards stderr_str (setting it to "").
    // To verify that stderr stream was genuinely captured (>2MB) without deadlock,
    // we run a child process that exits with code 42 (where TaskResult::failure retains stderr).
    let py_script_fail = r#"
import sys, threading

def write_stdout():
    chunk = ("A" * 65536).encode("ascii")
    for _ in range(32):
        sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()

def write_stderr():
    chunk = ("B" * 65536).encode("ascii")
    for _ in range(32):
        sys.stderr.buffer.write(chunk)
    sys.stderr.buffer.flush()

t1 = threading.Thread(target=write_stdout)
t2 = threading.Thread(target=write_stderr)
t1.start()
t2.start()
t1.join()
t2.join()
sys.exit(42)
"#;

    let task_fail = Task::new(
        TaskSpec::command("python3", vec!["-c".into(), py_script_fail.into()]),
        TaskRequirements::generic(1, 15),
    );

    let t1 = Instant::now();
    let res_fail = runner.execute_task(&task_fail, None, None).await;
    let elapsed_fail = t1.elapsed();

    assert_eq!(res_fail.exit_code, 42);
    assert!(
        res_fail.stdout.len() >= expected_bytes,
        "stdout on code 42 captured {} bytes",
        res_fail.stdout.len()
    );
    assert!(
        res_fail.stderr.len() >= expected_bytes,
        "stderr on code 42 captured {} bytes, expected >= {}",
        res_fail.stderr.len(),
        expected_bytes
    );
    assert!(res_fail.stdout.starts_with('A'));
    assert!(res_fail.stderr.starts_with('B'));
    assert!(
        elapsed_fail < Duration::from_secs(10),
        "Execution with code 42 completed in {:?}, not deadlocking",
        elapsed_fail
    );
}

// =========================================================================
// 3. Infinite Stream Resistance & Buffer Capping Tests
// =========================================================================

#[tokio::test]
async fn test_infinite_stream_resistance_continuous_draining_over_cap() {
    // Test with a tight buffer cap (64 KB) and child process writing 512 KB (8x cap) then exiting.
    // Verifies that:
    // 1. Memory is strictly bounded to the configured cap.
    // 2. The runner continuously drains the pipe to EOF even after the buffer is filled.
    // 3. The child process does not block or deadlock on write().
    let cap_bytes = 64 * 1024;
    let (runner, _tmp) = create_test_runner_with_cap(2, cap_bytes);

    let py_script = r#"
import sys
chunk = ("X" * 4096).encode("ascii")
for _ in range(128): # 128 * 4KB = 512 KB
    sys.stdout.buffer.write(chunk)
sys.stdout.buffer.flush()
"#;

    let task = Task::new(
        TaskSpec::command("python3", vec!["-c".into(), py_script.into()]),
        TaskRequirements::generic(1, 10),
    );

    let res = runner.execute_task(&task, None, None).await;

    assert!(
        res.is_success(),
        "Process writing beyond buffer cap must finish cleanly without deadlock"
    );
    assert_eq!(res.exit_code, 0);
    assert!(
        res.stdout
            .contains("[rusty_grid: output truncated after exceeding size limit]"),
        "Output must indicate truncation"
    );
    // Buffer length must be bounded: 64KB + length of truncation notice
    assert!(
        res.stdout.len() <= cap_bytes + 200,
        "stdout size {} exceeded expected cap {}",
        res.stdout.len(),
        cap_bytes + 200
    );
}

#[tokio::test]
async fn test_infinite_stream_resistance_yes_command_watchdog() {
    let (runner, _tmp) = create_test_runner(2, false);

    // Running `yes` continuously produces unbounded stream data at maximum pipe bandwidth.
    // Must be supervised and killed by watchdog at 1s without memory exhaustion or hanging.
    let task = Task::new(
        TaskSpec::command("yes", vec!["CHALLENGER_INFINITE_STREAM_TOKEN".into()]),
        TaskRequirements::generic(1, 1), // 1 second timeout
    );

    let child_pid = Arc::new(AtomicU32::new(0));
    let t0 = Instant::now();
    let res = runner
        .execute_task(&task, Some(Arc::clone(&child_pid)), None)
        .await;
    let elapsed = t0.elapsed();

    assert_eq!(
        res.exit_code, EXIT_CODE_TIMEOUT,
        "Runaway infinite stream must exit with timeout code 124"
    );
    assert!(!res.is_success());
    assert!(
        elapsed >= Duration::from_millis(990),
        "Timeout must wait at least 1s"
    );
    assert!(
        elapsed < Duration::from_millis(3000),
        "Timeout must trigger promptly, took {:?}",
        elapsed
    );

    // Verify child PID was reaped
    let pid = child_pid.load(Ordering::Acquire);
    assert!(pid > 0, "Child PID should have been captured");
    assert!(
        !is_process_alive(pid),
        "Child process {} must be reaped from OS after watchdog termination",
        pid
    );
}

// =========================================================================
// 4. Timeout Watchdog Precision & Process Reaping Tests
// =========================================================================

#[tokio::test]
async fn test_timeout_watchdog_precision_and_os_reaping() {
    let (runner, _tmp) = create_test_runner(2, false);

    // 1-second timeout test
    let task_1s = Task::new(
        TaskSpec::command("sleep", vec!["30".into()]),
        TaskRequirements::generic(1, 1),
    );

    let child_pid_1s = Arc::new(AtomicU32::new(0));
    let t0 = Instant::now();
    let res_1s = runner
        .execute_task(&task_1s, Some(Arc::clone(&child_pid_1s)), None)
        .await;
    let elapsed_1s = t0.elapsed();

    assert_eq!(res_1s.exit_code, EXIT_CODE_TIMEOUT);
    assert!(
        elapsed_1s >= Duration::from_millis(990) && elapsed_1s <= Duration::from_millis(2500),
        "1s timeout elapsed time {:?} out of precision bounds (1.0s - 2.5s)",
        elapsed_1s
    );

    let pid_1s = child_pid_1s.load(Ordering::Acquire);
    assert!(pid_1s > 0, "Child PID must be recorded");

    // Allow brief scheduling slice for OS kernel to update process table
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !is_process_alive(pid_1s),
        "Child process {} must be reaped from OS process table (no zombies)",
        pid_1s
    );

    // 2-second timeout test
    let task_2s = Task::new(
        TaskSpec::command("sleep", vec!["30".into()]),
        TaskRequirements::generic(1, 2),
    );

    let child_pid_2s = Arc::new(AtomicU32::new(0));
    let t1 = Instant::now();
    let res_2s = runner
        .execute_task(&task_2s, Some(Arc::clone(&child_pid_2s)), None)
        .await;
    let elapsed_2s = t1.elapsed();

    assert_eq!(res_2s.exit_code, EXIT_CODE_TIMEOUT);
    assert!(
        elapsed_2s >= Duration::from_millis(1980) && elapsed_2s <= Duration::from_millis(3500),
        "2s timeout elapsed time {:?} out of precision bounds (2.0s - 3.5s)",
        elapsed_2s
    );

    let pid_2s = child_pid_2s.load(Ordering::Acquire);
    assert!(pid_2s > 0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !is_process_alive(pid_2s),
        "Child process {} must be reaped from OS",
        pid_2s
    );
}

// =========================================================================
// 5. Cancellation Under Rapid Race Conditions
// =========================================================================

#[tokio::test]
async fn test_cancellation_immediate_race() {
    let temp_dir = tempfile::tempdir().expect("temp dir for client");
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(4)
        .with_sandbox_base_dir(temp_dir.path());
    let client = WorkerClient::new(config);
    let (tx, mut rx) = mpsc::channel(16);

    let task = Task::new(
        TaskSpec::command("sleep", vec!["30".into()]),
        TaskRequirements::generic(1, 60),
    );
    let task_id = task.id;

    // 1. Assign task
    client.handle_assign_task(task, tx).await;

    // 2. Immediately cancel without yielding or waiting
    client
        .handle_cancel_task(task_id, Some("Immediate cancel race".into()))
        .await;

    // 3. Drain messages from channel
    let mut received_cancelled_result = false;
    let timeout = Duration::from_secs(5);
    let start = Instant::now();

    while start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(WorkerMessage::TaskResult {
                task_id: rid,
                exit_code,
                error,
                ..
            })) => {
                if rid == task_id {
                    assert_eq!(exit_code, EXIT_CODE_CANCELLED);
                    assert_eq!(exit_code, 130);
                    assert!(error.unwrap().contains("Immediate cancel race"));
                    received_cancelled_result = true;
                    break;
                }
            }
            Ok(Some(WorkerMessage::TaskProgress { .. })) => {
                // Progress may or may not arrive depending on interleaving
            }
            Ok(Some(_)) | Ok(None) | Err(_) => {}
        }
    }

    assert!(
        received_cancelled_result,
        "Immediate cancel must yield TaskResult with EXIT_CODE_CANCELLED"
    );

    // 4. Verify semaphore permits and active task metrics are restored
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        client.concurrency_semaphore().available_permits(),
        4,
        "Concurrency semaphore permits must return to 4"
    );
    assert_eq!(
        client.heartbeat_tracker().active_tasks(),
        0,
        "Heartbeat active tasks counter must return to 0"
    );
    assert_eq!(
        client.active_task_table().read().await.len(),
        0,
        "Active task table must have 0 entries"
    );
}

#[tokio::test]
async fn test_cancellation_mid_flight_kills_and_reaps() {
    let temp_dir = tempfile::tempdir().expect("temp dir for client");
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(2)
        .with_sandbox_base_dir(temp_dir.path());
    let client = WorkerClient::new(config);
    let (tx, mut rx) = mpsc::channel(16);

    let task = Task::new(
        TaskSpec::command("sleep", vec!["30".into()]),
        TaskRequirements::generic(1, 60),
    );
    let task_id = task.id;

    client.handle_assign_task(task, tx).await;

    // Await Running status to ensure child process is actively executing
    let mut child_pid = 0;
    while let Some(msg) = rx.recv().await {
        if let WorkerMessage::TaskProgress {
            task_id: pid,
            status: TaskStatus::Running,
            ..
        } = msg
        {
            assert_eq!(pid, task_id);
            // Look up PID in active task table
            let table = client.active_task_table().read().await;
            if let Some(h) = table.get(&task_id) {
                child_pid = h.child_pid.load(Ordering::Acquire);
            }
            break;
        }
    }

    // Cancel while running
    client
        .handle_cancel_task(task_id, Some("Mid-flight abort".into()))
        .await;

    let res_msg = rx.recv().await.expect("result after cancel");
    match res_msg {
        WorkerMessage::TaskResult {
            task_id: rid,
            exit_code,
            error,
            ..
        } => {
            assert_eq!(rid, task_id);
            assert_eq!(exit_code, EXIT_CODE_CANCELLED);
            assert_eq!(exit_code, 130);
            assert!(error.unwrap().contains("Mid-flight abort"));
        }
        other => panic!("Expected TaskResult, got {:?}", other),
    }

    // Verify child process was reaped if PID was recorded
    if child_pid > 0 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !is_process_alive(child_pid),
            "Process {} must be reaped from OS",
            child_pid
        );
    }

    assert_eq!(client.concurrency_semaphore().available_permits(), 2);
    assert_eq!(client.heartbeat_tracker().active_tasks(), 0);
}

#[tokio::test]
async fn test_cancellation_burst_storm_integrity() {
    let temp_dir = tempfile::tempdir().expect("temp dir for client");
    let cores = 4;
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(cores)
        .with_sandbox_base_dir(temp_dir.path());
    let client = Arc::new(WorkerClient::new(config));
    let (tx, mut rx) = mpsc::channel(128);

    let num_tasks = 20;
    let mut task_ids = Vec::new();

    for i in 0..num_tasks {
        let task = Task::new(
            TaskSpec::command("sleep", vec!["10".into()]),
            TaskRequirements::generic(1, 30),
        );
        task_ids.push(task.id);
        client.handle_assign_task(task, tx.clone()).await;

        // Cancel at varied rapid timings (0ms, 5ms, 15ms, 30ms)
        let c = Arc::clone(&client);
        let tid = task_ids[i];
        let delay_ms = (i % 4) as u64 * 10;

        tokio::spawn(async move {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            c.handle_cancel_task(tid, Some(format!("Storm cancel {}", tid)))
                .await;
        });
    }

    // Collect all 20 TaskResults
    let mut completed_results = 0;
    let timeout = Duration::from_secs(10);
    let start = Instant::now();

    while completed_results < num_tasks && start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(WorkerMessage::TaskResult {
                task_id, exit_code, ..
            })) => {
                assert!(task_ids.contains(&task_id));
                assert_eq!(
                    exit_code, EXIT_CODE_CANCELLED,
                    "Task {} must result in cancellation",
                    task_id
                );
                assert_eq!(exit_code, 130);
                completed_results += 1;
            }
            Ok(Some(WorkerMessage::TaskProgress { .. })) => {
                // Ignore progress messages
            }
            Ok(Some(_)) | Ok(None) | Err(_) => {}
        }
    }

    assert_eq!(
        completed_results, num_tasks,
        "All {} tasks must emit a TaskResult",
        num_tasks
    );

    // Verify zero metric leakage after storm
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        client.concurrency_semaphore().available_permits(),
        cores,
        "All {} permits must be restored",
        cores
    );
    assert_eq!(
        client.heartbeat_tracker().active_tasks(),
        0,
        "Active tasks counter must be 0"
    );
    assert_eq!(
        client.active_task_table().read().await.len(),
        0,
        "Active task table must be completely empty"
    );
}

#[tokio::test]
async fn test_cancellation_of_queued_task_behind_saturated_worker() {
    let temp_dir = tempfile::tempdir().expect("temp dir for client");
    let cores = 1; // single core worker: max 1 active task
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(cores)
        .with_sandbox_base_dir(temp_dir.path());
    let client = Arc::new(WorkerClient::new(config));
    let (tx, mut rx) = mpsc::channel(16);

    // Task 1 occupies the single core for 1 second
    let task1 = Task::new(
        TaskSpec::command("sleep", vec!["1".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task1_id = task1.id;
    client.handle_assign_task(task1, tx.clone()).await;

    // Wait until Task 1 is running
    while let Some(msg) = rx.recv().await {
        if let WorkerMessage::TaskProgress {
            task_id,
            status: TaskStatus::Running,
            ..
        } = msg
        {
            if task_id == task1_id {
                break;
            }
        }
    }

    // Task 2 is assigned but queued behind Task 1 (waiting for the semaphore permit)
    let task2 = Task::new(
        TaskSpec::command("sleep", vec!["10".into()]),
        TaskRequirements::generic(1, 30),
    );
    let task2_id = task2.id;
    client.handle_assign_task(task2, tx.clone()).await;

    // Immediately cancel Task 2 while it is waiting in the semaphore queue
    let t_cancel_start = Instant::now();
    client
        .handle_cancel_task(task2_id, Some("Cancelled while queued".into()))
        .await;

    // Check when Task 2 cancellation result arrives
    let mut task2_cancelled_elapsed = None;
    let mut task1_finished = false;

    let timeout = Duration::from_secs(5);
    let start = Instant::now();
    while start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(WorkerMessage::TaskResult {
                task_id, exit_code, ..
            })) => {
                if task_id == task2_id {
                    assert_eq!(exit_code, EXIT_CODE_CANCELLED);
                    assert_eq!(exit_code, 130);
                    task2_cancelled_elapsed = Some(t_cancel_start.elapsed());
                } else if task_id == task1_id {
                    assert_eq!(exit_code, 0);
                    task1_finished = true;
                }
                if task2_cancelled_elapsed.is_some() && task1_finished {
                    break;
                }
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => {}
        }
    }

    assert!(
        task2_cancelled_elapsed.is_some(),
        "Task 2 must eventually receive cancelled result"
    );
    let elapsed = task2_cancelled_elapsed.unwrap();
    println!(
        "Empirical measurement: Queued task cancellation took {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "Queued task cancellation must occur immediately (<500ms) without waiting for saturated permit release, took {:?}",
        elapsed
    );
}
