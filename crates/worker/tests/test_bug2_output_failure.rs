//! Integration tests for BUG-2: Output artifact harvesting failure semantics.
//!
//! Validates:
//! 1. Missing declared output fails task with non-zero exit code naming the path.
//! 2. Single output artifact exceeding 10 MiB fails task.
//! 3. Aggregate output artifacts exceeding 32 MiB fail task.
//! 4. Symlinked output artifact fails task (security isolation).
//! 5. Process failure exit code is preserved when output is missing.

use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_worker::runner::{RunnerConfig, TaskRunner};
use rusty_grid_worker::sandbox::{Sandbox, SandboxConfig};

fn make_test_runner() -> (TaskRunner, TempDir) {
    let temp_dir = tempfile::tempdir().expect("create temp dir for runner");
    let sandbox_cfg = SandboxConfig::new(temp_dir.path());
    let runner_cfg = RunnerConfig::new(sandbox_cfg);
    let capabilities = WorkerCapabilities {
        name: "test-bug2-worker".into(),
        cpu_cores: 4,
        ram_mb: 8192,
        has_gpu: false,
        is_simulated_gpu: false,
        gpu_device_name: None,
        tags: vec!["bug2_test".into()],
        mobile: None,
        has_blender: false,
        blender_version: None,
        vram_mb: None,
        has_cuda: false,
    };
    let runner = TaskRunner::new(Uuid::new_v4(), capabilities, runner_cfg);
    (runner, temp_dir)
}

fn success_command_spec() -> TaskSpec {
    #[cfg(windows)]
    {
        TaskSpec::Command {
            program: "cmd.exe".into(),
            args: vec!["/C".into(), "echo ok".into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        }
    }
    #[cfg(not(windows))]
    {
        TaskSpec::Command {
            program: "true".into(),
            args: vec![],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        }
    }
}

fn failure_command_spec(code: i32) -> TaskSpec {
    #[cfg(windows)]
    {
        TaskSpec::Command {
            program: "cmd.exe".into(),
            args: vec!["/C".into(), format!("exit {code}")],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        }
    }
    #[cfg(not(windows))]
    {
        TaskSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), format!("exit {code}")],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        }
    }
}

#[tokio::test]
async fn test_missing_expected_output_fails_task() {
    let (runner, _tmp) = make_test_runner();
    let mut task = Task::new(success_command_spec(), TaskRequirements::generic(1, 10));
    task.expected_outputs.push("nonexistent_output.txt".into());

    let result = runner.execute_task(&task, None, None).await;

    assert!(
        !result.is_success(),
        "Task must not be marked success when declared output is missing"
    );
    assert_ne!(
        result.exit_code, 0,
        "Exit code must be non-zero when declared output is missing"
    );
    let err = result
        .error
        .as_deref()
        .expect("result.error must be populated");
    assert!(
        err.contains("nonexistent_output.txt"),
        "result.error should name the missing path, got: {err}"
    );
    let stderr_str = String::from_utf8_lossy(result.stderr.as_slice());
    assert!(
        stderr_str.contains("nonexistent_output.txt"),
        "result.stderr should name the missing path, got: {stderr_str}"
    );
}

#[tokio::test]
async fn test_missing_expected_output_fails_shell_task() {
    let (runner, _tmp) = make_test_runner();
    let spec = TaskSpec::ShellScript {
        script: "echo ok".into(),
        interpreter: None,
        env: HashMap::new(),
    };
    let mut task = Task::new(spec, TaskRequirements::generic(1, 10));
    task.expected_outputs
        .push("missing_shell_output.txt".into());

    let result = runner.execute_task(&task, None, None).await;

    assert!(
        !result.is_success(),
        "Shell task must not be marked success when declared output is missing"
    );
    assert_ne!(
        result.exit_code, 0,
        "Exit code must be non-zero when declared output is missing on shell task"
    );
    let err = result
        .error
        .as_deref()
        .expect("result.error must be populated");
    assert!(
        err.contains("missing_shell_output.txt"),
        "result.error should name the missing path, got: {err}"
    );
}

#[tokio::test]
async fn test_missing_expected_output_fails_compile_task() {
    let (runner, _tmp) = make_test_runner();
    let mut source_files = HashMap::new();
    source_files.insert(
        "src/lib.rs".to_string(),
        "pub fn hello() -> &'static str { \"hello\" }".to_string(),
    );
    let spec = TaskSpec::RustCompilation {
        crate_name: "test_crate".into(),
        source_files,
        compiler_flags: vec![],
        target_dir: None,
    };
    let mut task = Task::new(spec, TaskRequirements::generic(1, 10));
    task.expected_outputs
        .push("missing_compile_output.txt".into());

    let result = runner.execute_task(&task, None, None).await;

    assert!(
        !result.is_success(),
        "Compile task must not be marked success when declared output is missing"
    );
    assert_ne!(
        result.exit_code, 0,
        "Exit code must be non-zero when declared output is missing on compile task"
    );
    let err = result
        .error
        .as_deref()
        .expect("result.error must be populated");
    assert!(
        err.contains("missing_compile_output.txt"),
        "result.error should name the missing path, got: {err}"
    );
}

#[tokio::test]
async fn test_oversize_11mib_output_fails_task() {
    let (runner, tmp) = make_test_runner();
    let sb_cfg = SandboxConfig::new(tmp.path());
    let sandbox = Sandbox::create(&sb_cfg, TaskId::new()).await.unwrap();

    // Create an 11 MiB file (exceeds 10 MiB limit)
    let oversized_data = vec![0xAAu8; 11 * 1024 * 1024];
    let oversized_path = sandbox.path().join("big_file.bin");
    tokio::fs::write(&oversized_path, &oversized_data)
        .await
        .unwrap();

    let res = runner
        .harvest_output_artifacts(
            &sandbox,
            &["big_file.bin".to_string()],
            sandbox.task_id(),
            false,
        )
        .await;

    let err = res.expect_err("Harvesting 11 MiB file must return Err");
    assert!(
        err.contains("big_file.bin"),
        "Error must mention big_file.bin: {err}"
    );
    assert!(
        err.contains("10 MiB") || err.contains("exceeds single file limit"),
        "Error must mention 10 MiB limit: {err}"
    );
}

#[tokio::test]
async fn test_aggregate_oversize_output_fails_task() {
    let (runner, tmp) = make_test_runner();
    let sb_cfg = SandboxConfig::new(tmp.path());
    let sandbox = Sandbox::create(&sb_cfg, TaskId::new()).await.unwrap();

    // Create 4 files of 9 MiB each = 36 MiB (exceeds 32 MiB MAX_AGGREGATE_ARTIFACT_BYTES)
    let mut names = Vec::new();
    let chunk = vec![0xBBu8; 9 * 1024 * 1024];
    for i in 1..=4 {
        let name = format!("part_{i}.bin");
        let path = sandbox.path().join(&name);
        tokio::fs::write(&path, &chunk).await.unwrap();
        names.push(name);
    }

    let res = runner
        .harvest_output_artifacts(&sandbox, &names, sandbox.task_id(), false)
        .await;

    let err = res.expect_err("Aggregate outputs exceeding 32 MiB must return Err");
    assert!(
        err.contains("32 MiB") || err.contains("aggregate"),
        "Error must mention 32 MiB aggregate limit: {err}"
    );
    assert!(
        err.contains("part_4.bin"),
        "Error must name the file that crossed aggregate limit: {err}"
    );
}

#[tokio::test]
async fn test_symlink_output_fails_task() {
    let (runner, tmp) = make_test_runner();
    let sb_cfg = SandboxConfig::new(tmp.path());
    let sandbox = Sandbox::create(&sb_cfg, TaskId::new()).await.unwrap();

    let real_file = sandbox.path().join("real.txt");
    tokio::fs::write(&real_file, b"content").await.unwrap();

    let link_file = sandbox.path().join("link.txt");
    #[cfg(unix)]
    let symlink_res = tokio::fs::symlink(&real_file, &link_file).await;
    #[cfg(windows)]
    let symlink_res = std::os::windows::fs::symlink_file(&real_file, &link_file);

    if symlink_res.is_err() {
        println!("Skipping symlink test: privilege not held on this system");
        return;
    }

    let res = runner
        .harvest_output_artifacts(
            &sandbox,
            &["link.txt".to_string()],
            sandbox.task_id(),
            false,
        )
        .await;

    let err = res.expect_err("Symlink output must return Err");
    assert!(
        err.contains("symbolic link") || err.contains("symlink"),
        "Error must mention symlink: {err}"
    );
}

#[tokio::test]
async fn test_process_failure_preserves_exit_code() {
    let (runner, _tmp) = make_test_runner();
    let mut task = Task::new(failure_command_spec(42), TaskRequirements::generic(1, 10));
    task.expected_outputs.push("not_produced.txt".into());

    let result = runner.execute_task(&task, None, None).await;

    assert_eq!(
        result.exit_code, 42,
        "Process failure exit code 42 must be preserved"
    );
    assert!(!result.is_success());
}
