//! Integration test suite for Milestone 3 (M3: Worker Task Execution & Sandboxing).
//!
//! Verifies:
//! 1. Basic command execution (echo, env injection, pwd/working directory).
//! 2. Shell script execution (multi-line scripts, exit codes).
//! 3. Distributed Rust crate compilation using `rustc` inside sandbox.
//! 4. Strict GPU gating (non-GPU rejection vs simulated GPU execution).
//! 5. Watchdog timeout enforcement (terminating runaway processes with exit code 124).
//! 6. Task cancellation (aborts process, returns cancelled status).
//! 7. Concurrency limiting via CPU core semaphore permits and telemetry.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_worker::{
    RunnerConfig, SandboxConfig, TaskRunner, WorkerClient, WorkerConfig, EXIT_CODE_CANCELLED,
    EXIT_CODE_TIMEOUT,
};

fn make_capabilities(name: &str, cores: usize, has_gpu: bool) -> WorkerCapabilities {
    WorkerCapabilities {
        name: name.into(),
        cpu_cores: cores,
        ram_mb: 8192,
        has_gpu,
        is_simulated_gpu: has_gpu,
        gpu_device_name: if has_gpu {
            Some("Simulated Virtual GPU".into())
        } else {
            None
        },
        tags: vec!["m3_test".into()],
        mobile: None,
    }
}

fn make_runner(has_gpu: bool) -> (TaskRunner, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("create temp dir for runner");
    let sandbox_cfg = SandboxConfig::new(temp_dir.path());
    let runner_cfg = RunnerConfig::new(sandbox_cfg);
    let capabilities = make_capabilities("worker-runner", 4, has_gpu);
    let runner = TaskRunner::new(Uuid::new_v4(), capabilities, runner_cfg);
    (runner, temp_dir)
}

/// 1. Test basic command execution (echo, env injection, pwd/working_dir).
#[tokio::test]
async fn test_basic_command_execution() {
    let (runner, _tmp) = make_runner(false);

    // 1a. Echo command
    let task_echo = Task::new(
        TaskSpec::command("echo", vec!["rusty_grid_exec_ok".into()]),
        TaskRequirements::generic(1, 10),
    );
    let res_echo = runner.execute_task(&task_echo, None, None).await;
    assert!(res_echo.is_success());
    assert_eq!(res_echo.exit_code, 0);
    assert!(res_echo.stdout.contains("rusty_grid_exec_ok"));
    assert!(!res_echo.is_gpu_executed);

    // 1b. Environment variable injection
    let mut env = HashMap::new();
    env.insert("CUSTOM_ENV_KEY".into(), "CUSTOM_ENV_VAL".into());
    let task_env = Task::new(
        TaskSpec::Command {
            program: "sh".into(),
            args: vec![
                "-c".into(),
                "echo KEY=$CUSTOM_ENV_KEY TID=$RUSTY_GRID_TASK_ID WID=$RUSTY_GRID_WORKER_ID".into(),
            ],
            env,
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let res_env = runner.execute_task(&task_env, None, None).await;
    assert!(res_env.is_success());
    assert!(res_env.stdout.contains("KEY=CUSTOM_ENV_VAL"));
    assert!(res_env.stdout.contains(&format!("TID={}", task_env.id)));
    assert!(res_env
        .stdout
        .contains(&format!("WID={}", runner.worker_id())));

    // 1c. Working directory resolution inside sandbox
    let task_pwd = Task::new(
        TaskSpec::Command {
            program: "pwd".into(),
            args: vec![],
            env: HashMap::new(),
            working_dir: Some(PathBuf::from("sub/workdir")),
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let res_pwd = runner.execute_task(&task_pwd, None, None).await;
    assert!(res_pwd.is_success());
    assert!(res_pwd.stdout.contains("sub/workdir"));
}

/// 2. Test shell script execution (multi-line scripts, variables, arithmetic, exit code propagation).
#[tokio::test]
async fn test_shell_script_execution() {
    let (runner, _tmp) = make_runner(false);

    // 2a. Multi-line successful script
    let script_ok = r#"
#!/bin/sh
set -e
SUM=0
for i in 1 2 3 4 5; do
    SUM=$((SUM + i))
done
echo "SUM=$SUM"
"#;
    let task_ok = Task::new(
        TaskSpec::shell_script(script_ok),
        TaskRequirements::generic(1, 10),
    );
    let res_ok = runner.execute_task(&task_ok, None, None).await;
    assert!(res_ok.is_success());
    assert_eq!(res_ok.exit_code, 0);
    assert!(res_ok.stdout.contains("SUM=15"));

    // 2b. Script returning non-zero exit code
    let script_fail = r#"
#!/bin/sh
echo "About to fail with code 37"
exit 37
"#;
    let task_fail = Task::new(
        TaskSpec::shell_script(script_fail),
        TaskRequirements::generic(1, 10),
    );
    let res_fail = runner.execute_task(&task_fail, None, None).await;
    assert!(!res_fail.is_success());
    assert_eq!(res_fail.exit_code, 37);
    assert!(res_fail.stdout.contains("About to fail with code 37"));
}

/// 3. Test Rust compilation of a sample crate using `rustc` inside the sandbox.
#[tokio::test]
async fn test_rust_compilation_workflow() {
    let (runner, _tmp) = make_runner(false);

    // 3a. Successful compilation of a library crate
    let mut sources = HashMap::new();
    sources.insert(
        "src/lib.rs".into(),
        r#"
pub fn factorial(n: u64) -> u64 {
    match n {
        0 | 1 => 1,
        _ => n * factorial(n - 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_fact() {
        assert_eq!(factorial(5), 120);
    }
}
"#
        .into(),
    );

    let task_compile = Task::new(
        TaskSpec::rust_compilation("math_utils", sources, vec!["-O".into()]),
        TaskRequirements::generic(2, 60),
    );

    let res_compile = runner.execute_task(&task_compile, None, None).await;
    assert!(
        res_compile.is_success(),
        "rustc compilation failed: stderr={}",
        res_compile.stderr
    );
    assert_eq!(res_compile.exit_code, 0);
    assert!(res_compile
        .stdout
        .contains("compilation artifacts generated"));
    assert!(res_compile.stdout.contains("libmath_utils.rlib"));

    // 3b. Rust compilation failure with syntax error
    let mut bad_sources = HashMap::new();
    bad_sources.insert(
        "src/lib.rs".into(),
        "pub fn broken() { this is completely invalid rust code }".into(),
    );

    let task_bad = Task::new(
        TaskSpec::rust_compilation("bad_crate", bad_sources, vec![]),
        TaskRequirements::generic(1, 30),
    );

    let res_bad = runner.execute_task(&task_bad, None, None).await;
    assert!(!res_bad.is_success());
    assert_ne!(res_bad.exit_code, 0);
    assert!(res_bad.stderr.contains("expected"));
}

/// 4. Test strict GPU gating:
///    - Non-GPU worker immediately rejects GpuCompute with exit code 1, is_gpu_executed = false.
///    - Simulated GPU worker executes matrix compute with is_gpu_executed = true.
#[tokio::test]
async fn test_strict_gpu_gating() {
    let (cpu_runner, _tmp1) = make_runner(false);
    let (gpu_runner, _tmp2) = make_runner(true);

    let gpu_task = Task::new(
        TaskSpec::gpu_compute("matrix_mult_fp32", 48),
        TaskRequirements::gpu(30),
    );

    // 4a. Non-GPU worker rejection
    let cpu_res = cpu_runner.execute_task(&gpu_task, None, None).await;
    assert!(!cpu_res.is_success());
    assert_eq!(cpu_res.exit_code, 1);
    assert!(!cpu_res.is_gpu_executed);
    assert_eq!(cpu_res.execution_time_ms, 0);
    assert!(cpu_res
        .error
        .as_ref()
        .unwrap()
        .contains("Worker lacks GPU capability"));
    assert!(cpu_res.stderr.contains("Worker lacks GPU capability"));

    // 4b. Simulated GPU worker execution
    let gpu_res = gpu_runner.execute_task(&gpu_task, None, None).await;
    assert!(
        gpu_res.is_success(),
        "GPU task should succeed: error={:?}",
        gpu_res.error
    );
    assert_eq!(gpu_res.exit_code, 0);
    assert!(gpu_res.is_gpu_executed);
    assert!(gpu_res.execution_time_ms >= 1);
    assert!(gpu_res.stdout.contains("[GPU COMPUTE SIMULATOR]"));
    assert!(gpu_res.stdout.contains("Status: VERIFIED_OK"));
    assert!(gpu_res.stdout.contains("Matrix Dimension: 48x48"));
    assert!(gpu_res.stdout.contains("Matrix Trace:"));
    assert!(gpu_res.stdout.contains("Frobenius Norm:"));
}

/// 5. Test timeout watchdog terminates runaway processes (e.g. sleep 60 killed at 1s, exit code 124).
#[tokio::test]
async fn test_timeout_watchdog_terminates_runaway() {
    let (runner, _tmp) = make_runner(false);

    let runaway_task = Task::new(
        TaskSpec::command("sleep", vec!["60".into()]),
        TaskRequirements::generic(1, 1), // 1 second timeout
    );

    let t0 = Instant::now();
    let res = runner.execute_task(&runaway_task, None, None).await;
    let elapsed = t0.elapsed();

    assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
    assert!(!res.is_success());
    assert!(
        elapsed >= Duration::from_secs(1),
        "Must wait for timeout duration"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "Watchdog must terminate promptly"
    );
    assert!(res
        .error
        .as_ref()
        .unwrap()
        .contains("Task timed out after 1s"));
    assert!(res.stderr.contains("Task timed out after 1s"));
}

/// 6. Test CancelTask cancels running task.
#[tokio::test]
async fn test_cancel_task_cancels_running_task() {
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

    // Assign task
    client.handle_assign_task(task, tx).await;

    // Await TaskProgress::Running
    let progress = rx.recv().await.expect("progress message");
    match progress {
        WorkerMessage::TaskProgress {
            task_id: pid,
            status,
            ..
        } => {
            assert_eq!(pid, task_id);
            assert_eq!(status, TaskStatus::Running);
        }
        other => panic!("Expected TaskProgress, got {other:?}"),
    }

    // Cancel task
    client
        .handle_cancel_task(task_id, Some("Master initiated cancellation".into()))
        .await;

    // Await TaskResult
    let result = rx.recv().await.expect("result message");
    match result {
        WorkerMessage::TaskResult {
            task_id: rid,
            exit_code,
            error,
            ..
        } => {
            assert_eq!(rid, task_id);
            assert_eq!(exit_code, EXIT_CODE_CANCELLED);
            assert_eq!(exit_code, 130);
            assert!(error
                .unwrap()
                .contains("Cancelled: Master initiated cancellation"));
        }
        other => panic!("Expected TaskResult, got {other:?}"),
    }
}

/// 7. Test concurrency semaphore limits active tasks to CPU cores.
#[tokio::test]
async fn test_concurrency_semaphore_limits_active_tasks() {
    let temp_dir = tempfile::tempdir().expect("temp dir for concurrency client");
    let cores = 2;
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(cores)
        .with_sandbox_base_dir(temp_dir.path());
    let client = WorkerClient::new(config);

    assert_eq!(
        client.concurrency_semaphore().available_permits(),
        cores,
        "Initial permits equal cores"
    );
    assert_eq!(client.heartbeat_tracker().active_tasks(), 0);

    let (tx, mut rx) = mpsc::channel(64);

    // Submit 4 short-lived tasks
    let task_count = 4;
    for _ in 0..task_count {
        let task = Task::new(
            TaskSpec::builtin_test("arithmetic_stress", 50_000),
            TaskRequirements::generic(1, 10),
        );
        client.handle_assign_task(task, tx.clone()).await;
    }

    let mut completed = 0;
    let max_active_observed = Arc::new(AtomicUsize::new(0));
    let tracker = Arc::clone(client.heartbeat_tracker());
    let observer_max = Arc::clone(&max_active_observed);

    // Spawn observer checking active_tasks concurrently
    let observer_handle = tokio::spawn(async move {
        for _ in 0..50 {
            let active = tracker.active_tasks();
            observer_max.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    while completed < task_count {
        let msg = rx.recv().await.expect("worker message");
        if let WorkerMessage::TaskResult { exit_code, .. } = msg {
            assert_eq!(exit_code, 0);
            completed += 1;
        }
    }

    let _ = observer_handle.await;

    // Observe that active_tasks never exceeded available cores
    let max_seen = max_active_observed.load(Ordering::SeqCst);
    assert!(
        max_seen <= cores,
        "Active tasks {max_seen} exceeded worker core capacity {cores}"
    );

    // Wait briefly for all RAII ActiveTaskGuard drops to run
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        client.concurrency_semaphore().available_permits(),
        cores,
        "All permits restored after task completion"
    );
    assert_eq!(
        client.heartbeat_tracker().active_tasks(),
        0,
        "Active tasks counter returned to 0"
    );
}
