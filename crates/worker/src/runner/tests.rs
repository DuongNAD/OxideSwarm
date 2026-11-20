use super::*;
use rusty_grid_core::task::TaskRequirements;

fn test_capabilities(has_gpu: bool) -> WorkerCapabilities {
    WorkerCapabilities {
        name: "test-worker".into(),
        cpu_cores: 4,
        ram_mb: 8192,
        has_gpu,
        is_simulated_gpu: has_gpu,
        gpu_device_name: if has_gpu {
            Some("Test Virtual GPU".into())
        } else {
            None
        },
        tags: vec![],
        mobile: None,
        ..Default::default()
    }
}

fn test_runner(has_gpu: bool) -> TaskRunner {
    let temp_dir = std::env::temp_dir().join(format!("runner_test_{}", Uuid::new_v4()));
    let config = RunnerConfig::new(SandboxConfig::new(temp_dir));
    TaskRunner::new(Uuid::new_v4(), test_capabilities(has_gpu), config)
}

#[tokio::test]
async fn test_runner_command_echo_success() {
    let runner = test_runner(false);
    let task = Task::new(
        TaskSpec::command("echo", vec!["hello".into(), "rusty_grid".into()]),
        TaskRequirements::generic(1, 10),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(res.is_success());
    assert_eq!(res.exit_code, 0);
    assert!(res.stdout_str().contains("hello rusty_grid"));
    assert!(!res.is_gpu_executed);
}

#[tokio::test]
async fn test_runner_command_exit_code_failure() {
    let runner = test_runner(false);
    let task = Task::new(
        TaskSpec::command("sh", vec!["-c".into(), "exit 42".into()]),
        TaskRequirements::generic(1, 10),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(!res.is_success());
    assert_eq!(res.exit_code, 42);
}

#[tokio::test]
async fn test_runner_command_stdin_piping() {
    let runner = test_runner(false);
    let spec = TaskSpec::Command {
        program: "cat".into(),
        args: vec![],
        env: HashMap::new(),
        working_dir: None,
        stdin: Some(b"piped input data\n".to_vec()),
    };
    let task = Task::new(spec, TaskRequirements::generic(1, 10));

    let res = runner.execute_task(&task, None, None).await;
    assert!(res.is_success());
    assert_eq!(res.stdout_str(), "piped input data\n");
}

#[tokio::test]
async fn test_runner_command_env_injection() {
    let runner = test_runner(false);
    let mut env = HashMap::new();
    env.insert("GRID_TEST_VAR".into(), "MY_CUSTOM_VAL".into());

    let spec = TaskSpec::Command {
        program: "sh".into(),
        args: vec![
            "-c".into(),
            "echo $GRID_TEST_VAR-$RUSTY_GRID_TASK_ID".into(),
        ],
        env,
        working_dir: None,
        stdin: None,
    };
    let task = Task::new(spec, TaskRequirements::generic(1, 10));

    let res = runner.execute_task(&task, None, None).await;
    assert!(res.is_success());
    assert!(res.stdout_str().contains("MY_CUSTOM_VAL"));
    assert!(res.stdout_str().contains(&task.id.to_string()));
}

#[tokio::test]
async fn test_runner_shell_script_execution() {
    let runner = test_runner(false);
    let script = "A=5\nB=10\necho $((A + B))";
    let task = Task::new(
        TaskSpec::shell_script(script),
        TaskRequirements::generic(1, 10),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(res.is_success());
    assert_eq!(res.stdout_str().trim(), "15");
}

#[tokio::test]
async fn test_runner_timeout_watchdog_terminates_runaway() {
    let runner = test_runner(false);
    // Sleep for 60s, timeout in 1s
    let task = Task::new(
        TaskSpec::command("sleep", vec!["60".into()]),
        TaskRequirements::generic(1, 1),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
    assert!(!res.is_success());
    assert!(res.error.as_ref().unwrap().contains("timed out after 1s"));
    assert!(res.stderr_str().contains("Task timed out after 1s"));
}

#[tokio::test]
async fn test_runner_cancellation_signal() {
    let runner = test_runner(false);
    let task = Task::new(
        TaskSpec::command("sleep", vec!["60".into()]),
        TaskRequirements::generic(1, 10),
    );

    let (cancel_tx, cancel_rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = cancel_tx.send("user requested cancel".to_string());
    });

    let res = runner.execute_task(&task, None, Some(cancel_rx)).await;
    assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
    assert_eq!(res.exit_code, 130);
    assert!(!res.is_success());
    assert!(res.error.unwrap().contains("Cancelled"));
}

#[tokio::test]
async fn test_runner_gpu_strict_rejection_on_cpu_worker() {
    let runner = test_runner(false); // Non-GPU worker
    let task = Task::new(
        TaskSpec::gpu_compute("matrix_mult", 64),
        TaskRequirements::gpu(10),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert_eq!(res.exit_code, 1);
    assert!(!res.is_success());
    assert!(!res.is_gpu_executed);
    assert_eq!(res.execution_time_ms, 0);
    assert!(res.error.unwrap().contains("Worker lacks GPU capability"));
}

#[tokio::test]
async fn test_runner_gpu_matrix_compute_on_gpu_worker() {
    let runner = test_runner(true); // GPU worker
    let task = Task::new(
        TaskSpec::gpu_compute("matrix_mult", 32),
        TaskRequirements::gpu(10),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(res.is_success());
    assert_eq!(res.exit_code, 0);
    assert!(res.is_gpu_executed);
    assert!(res.execution_time_ms >= 1);
    assert!(res.stdout_str().contains("VERIFIED_OK"));
    assert!(res.stdout_str().contains("Matrix Trace:"));
    assert!(res.stdout_str().contains("Frobenius Norm:"));
}

#[tokio::test]
async fn test_runner_builtin_test_deterministic_hash() {
    let runner = test_runner(false);
    let task1 = Task::new(
        TaskSpec::builtin_test("hash_compute", 50_000),
        TaskRequirements::generic(1, 10),
    );
    let mut task2 = task1.clone();
    task2.id = task1.id; // Same task ID

    let res1 = runner.execute_task(&task1, None, None).await;
    let res2 = runner.execute_task(&task2, None, None).await;

    assert!(res1.is_success());
    assert!(res2.is_success());
    assert_eq!(
        res1.stdout, res2.stdout,
        "Identical seed & iterations must produce identical hash"
    );
}

#[tokio::test]
async fn test_runner_checkpoint_emission_and_resumption() {
    let runner = test_runner(false);
    let task = Task::new(
        TaskSpec::builtin_test("hash_compute", 100),
        TaskRequirements::generic(1, 10),
    );

    // 1. Run full 100 iterations uninterrupted
    let full_res = runner.execute_task(&task, None, None).await;
    assert!(full_res.is_success());
    let full_output = full_res.stdout_str();
    assert!(full_output.contains("100 iterations"));

    let digest_pos = full_output.find("digest = ").expect("digest must exist");
    let full_digest = &full_output[digest_pos..];

    // 2. Run with checkpoint emission channel
    let (cp_tx, mut cp_rx) = mpsc::channel(10);
    let task_clone = task.clone();
    let runner_clone = test_runner(false);
    let run_handle = tokio::spawn(async move {
        runner_clone
            .execute_task_with_checkpoint(&task_clone, None, Some(cp_tx), None, None)
            .await
    });

    // Receive emitted checkpoint at iteration 50
    let received_cp = cp_rx.recv().await.expect("must emit checkpoint");
    let (cp_seq, cp_delta) = match received_cp {
        WorkerMessage::Checkpoint {
            sequence,
            delta_state,
            ..
        } => (sequence, delta_state),
        other => panic!("Unexpected message: {:?}", other),
    };
    assert_eq!(cp_seq, 1);
    assert_eq!(cp_delta.len(), 16);

    let _ = run_handle.await;

    // 3. Resume another worker from iteration 50 checkpoint
    let cp_data = CheckpointData {
        sequence: cp_seq,
        delta_state: cp_delta,
        timestamp: 12345,
    };
    let resumed_res = runner
        .execute_task_with_checkpoint(&task, Some(&cp_data), None, None, None)
        .await;
    assert!(resumed_res.is_success());
    let resumed_output = resumed_res.stdout_str();
    assert!(resumed_output.contains("resumed from 50"));
    let resumed_digest_pos = resumed_output.find("digest = ").expect("digest must exist");
    let resumed_digest = &resumed_output[resumed_digest_pos..];

    assert_eq!(full_digest, resumed_digest);
}

#[tokio::test]
async fn test_runner_rust_compilation_workflow() {
    let runner = test_runner(false);
    let mut sources = HashMap::new();
    sources.insert(
        "src/lib.rs".into(),
        "pub fn compute_sum(a: i32, b: i32) -> i32 { a + b }".into(),
    );

    let task = Task::new(
        TaskSpec::rust_compilation("test_math_crate", sources, vec![]),
        TaskRequirements::generic(1, 30),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(res.is_success());
    assert_eq!(res.exit_code, 0);
    assert!(res.stdout_str().contains("compilation artifacts generated"));
}

#[tokio::test]
async fn test_rust_compilation_with_cargo_toml_runs_cargo_build_offline() {
    let runner = test_runner(false);

    // 1. A crate with a local path dependency builds via `cargo build --offline`
    //    and produces the compiled binary in `result.outputs`.
    let mut sources = HashMap::new();
    sources.insert(
        "Cargo.toml".to_string(),
        r#"[package]
name = "w8_app"
version = "0.1.0"
edition = "2021"

[dependencies]
w8_dep = { path = "w8_dep" }
"#
        .to_string(),
    );
    sources.insert(
        "src/main.rs".to_string(),
        "fn main() { println!(\"magic={}\", w8_dep::answer()); }\n".to_string(),
    );
    sources.insert(
        "w8_dep/Cargo.toml".to_string(),
        r#"[package]
name = "w8_dep"
version = "0.1.0"
edition = "2021"
"#
        .to_string(),
    );
    sources.insert(
        "w8_dep/src/lib.rs".to_string(),
        "pub fn answer() -> u32 { 42 }\n".to_string(),
    );

    let task = Task::new(
        TaskSpec::RustCompilation {
            crate_name: "w8_app".to_string(),
            source_files: sources,
            compiler_flags: Vec::new(),
            target_dir: None,
        },
        TaskRequirements::generic(1, 60),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(
        res.is_success(),
        "expected cargo build --offline to succeed, got: {res:?}"
    );
    assert!(
        res.outputs.iter().any(|o| o.path.contains("w8_app")),
        "expected compiled w8_app binary in result.outputs, got: {:?}",
        res.outputs
    );

    // 2. An uncached registry dependency fails immediately under `--offline`.
    let mut bad_sources = HashMap::new();
    bad_sources.insert(
        "Cargo.toml".to_string(),
        r#"[package]
name = "w8_missing_dep"
version = "0.1.0"
edition = "2021"

[dependencies]
nonexistent_oxideswarm_crate_w8 = "99.99.99"
"#
        .to_string(),
    );
    bad_sources.insert("src/main.rs".to_string(), "fn main() {}\n".to_string());

    let bad_task = Task::new(
        TaskSpec::RustCompilation {
            crate_name: "w8_missing_dep".to_string(),
            source_files: bad_sources,
            compiler_flags: Vec::new(),
            target_dir: None,
        },
        TaskRequirements::generic(1, 30),
    );

    let bad_res = runner.execute_task(&bad_task, None, None).await;
    assert!(
        !bad_res.is_success(),
        "expected uncached dependency to fail under --offline, got: {bad_res:?}"
    );

    // 3. Dangerous `compiler_flags` (`--config`, `-Z`, `--manifest-path`, `--target-dir`
    //    outside the sandbox, `--online`) are refused before spawning `cargo`.
    for bad_flags in [
        vec!["--config".to_string(), "net.offline=false".to_string()],
        vec!["--config=build.rustc-wrapper='sh'".to_string()],
        vec!["-Z".to_string(), "unstable-options".to_string()],
        vec!["-Zscript".to_string()],
        vec![
            "--manifest-path".to_string(),
            "../other/Cargo.toml".to_string(),
        ],
        vec!["--target-dir".to_string(), "../escape_target".to_string()],
        vec!["--online".to_string()],
    ] {
        let mut flag_sources = HashMap::new();
        flag_sources.insert(
            "Cargo.toml".to_string(),
            "[package]\nname = \"w8_flags\"\nversion = \"0.1.0\"\nedition = \"2021\"\n".to_string(),
        );
        flag_sources.insert("src/main.rs".to_string(), "fn main() {}\n".to_string());
        let flag_task = Task::new(
            TaskSpec::RustCompilation {
                crate_name: "w8_flags".to_string(),
                source_files: flag_sources,
                compiler_flags: bad_flags.clone(),
                target_dir: None,
            },
            TaskRequirements::generic(1, 30),
        );
        let flag_res = runner.execute_task(&flag_task, None, None).await;
        assert!(
            !flag_res.is_success(),
            "expected bad compiler_flags {bad_flags:?} to be rejected, got: {flag_res:?}"
        );
        assert!(
            flag_res
                .stderr_str()
                .contains("Invalid cargo compiler_flags"),
            "expected 'Invalid cargo compiler_flags' in stderr for {bad_flags:?}, got: {}",
            flag_res.stderr_str()
        );
    }

    // 4. A `rust-toolchain.toml` in `source_files` requesting a bogus toolchain is ignored /
    //    overridden by the worker's pinned `RUSTUP_TOOLCHAIN` so `rustup` stays offline.
    let mut tc_sources = HashMap::new();
    tc_sources.insert(
        "Cargo.toml".to_string(),
        "[package]\nname = \"w8_tc\"\nversion = \"0.1.0\"\nedition = \"2021\"\n".to_string(),
    );
    tc_sources.insert(
        "rust-toolchain.toml".to_string(),
        "[toolchain]\nchannel = \"nonexistent-oxideswarm-toolchain-99.99.99\"\n".to_string(),
    );
    tc_sources.insert("src/main.rs".to_string(), "fn main() {}\n".to_string());
    let tc_task = Task::new(
        TaskSpec::RustCompilation {
            crate_name: "w8_tc".to_string(),
            source_files: tc_sources,
            compiler_flags: vec!["--release".to_string()],
            target_dir: None,
        },
        TaskRequirements::generic(1, 60),
    );
    let tc_res = runner.execute_task(&tc_task, None, None).await;
    assert!(
        tc_res.is_success(),
        "expected rust-toolchain.toml to be neutralized offline and build with worker toolchain, got: {tc_res:?}"
    );
    assert!(
        tc_res.outputs.iter().any(|o| o.path.contains("w8_tc")),
        "expected w8_tc binary in outputs, got: {:?}",
        tc_res.outputs
    );
}

#[cfg(not(feature = "3d"))]
#[tokio::test]
async fn test_runner_3d_stubs_fail_cleanly_when_built_without_3d_feature() {
    let runner = test_runner(false);

    let gen_task = Task::new(
        TaskSpec::Generative3D(
            rusty_grid_core::task::Generative3DSpec::new_prompt("drone".to_string())
                .with_engine("sdf")
                .with_quality("fast"),
        ),
        TaskRequirements::generic(1, 30),
    );
    let gen_res = runner.execute_task(&gen_task, None, None).await;
    assert!(!gen_res.is_success());
    assert_eq!(gen_res.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(
        gen_res
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("--features 3d"),
        "unexpected Generative3D stub error: {:?}",
        gen_res.error
    );

    let rig_task = Task::new(
        TaskSpec::AutoRig(rusty_grid_core::task::AutoRigSpec::new(
            vec![1, 2, 3, 4],
            "biped",
        )),
        TaskRequirements::generic(1, 30),
    );
    let rig_res = runner.execute_task(&rig_task, None, None).await;
    assert!(!rig_res.is_success());
    assert_eq!(rig_res.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(
        rig_res
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("--features 3d"),
        "unexpected AutoRig stub error: {:?}",
        rig_res.error
    );
}

#[cfg(feature = "3d")]
#[tokio::test]
async fn test_runner_generative_3d_tier0_checkpoints() {
    let spec = rusty_grid_core::task::Generative3DSpec::new_prompt("procedural drone".to_string())
        .with_engine("sdf")
        .with_quality("fast");
    let task = Task::new(
        TaskSpec::Generative3D(spec),
        TaskRequirements::generic(1, 30),
    );

    let (cp_tx, mut cp_rx) = mpsc::channel(10);
    let task_clone = task.clone();
    let runner_clone = test_runner(false);

    let run_handle = tokio::spawn(async move {
        runner_clone
            .execute_task_with_checkpoint(&task_clone, None, Some(cp_tx), None, None)
            .await
    });

    // Collect emitted checkpoints
    let mut checkpoints = Vec::new();
    while let Some(msg) = cp_rx.recv().await {
        if let WorkerMessage::Checkpoint {
            sequence,
            delta_state,
            ..
        } = msg
        {
            checkpoints.push((sequence, delta_state));
        }
    }

    let res = run_handle.await.expect("task must join");
    assert!(res.is_success());
    assert_eq!(res.exit_code, 0);

    // Verify sequential checkpoints 1..4
    assert_eq!(checkpoints.len(), 4);
    assert_eq!(checkpoints[0].0, 1);
    assert_eq!(checkpoints[1].0, 2);
    assert_eq!(checkpoints[2].0, 3);
    assert_eq!(checkpoints[3].0, 4);

    let delta1: serde_json::Value = serde_json::from_slice(checkpoints[0].1.as_slice()).unwrap();
    assert_eq!(delta1["stage"], "sdf_geometry");
    assert_eq!(delta1["progress"], 0.25);

    let delta4: serde_json::Value = serde_json::from_slice(checkpoints[3].1.as_slice()).unwrap();
    assert_eq!(delta4["stage"], "gltf_pack");
    assert_eq!(delta4["progress"], 1.00);

    // Verify asset bytes
    assert!(res.asset_bytes.is_some());
    let asset = res.asset_bytes.unwrap();
    assert!(asset.len() >= 20);
    assert_eq!(&asset.as_slice()[0..4], b"glTF");
}

#[cfg(feature = "3d")]
#[tokio::test]
async fn test_runner_autorig_checkpoints() {
    // Generate a small mesh first
    let gen_spec = rusty_grid_core::task::Generative3DSpec::new_prompt("cylinder".to_string())
        .with_engine("sdf")
        .with_quality("fast");
    let (mesh_bytes, _) = crate::gen3d::generate_procedural_3d(&gen_spec).expect("mesh gen failed");

    let rig_spec = rusty_grid_core::task::AutoRigSpec::new(mesh_bytes, "biped")
        .with_animations(vec!["idle".to_string()]);
    let task = Task::new(
        TaskSpec::AutoRig(rig_spec),
        TaskRequirements::generic(1, 30),
    );

    let (cp_tx, mut cp_rx) = mpsc::channel(10);
    let task_clone = task.clone();
    let runner_clone = test_runner(false);

    let run_handle = tokio::spawn(async move {
        runner_clone
            .execute_task_with_checkpoint(&task_clone, None, Some(cp_tx), None, None)
            .await
    });

    // Collect emitted checkpoints
    let mut checkpoints = Vec::new();
    while let Some(msg) = cp_rx.recv().await {
        if let WorkerMessage::Checkpoint {
            sequence,
            delta_state,
            ..
        } = msg
        {
            checkpoints.push((sequence, delta_state));
        }
    }

    let res = run_handle.await.expect("task must join");
    assert!(res.is_success());
    assert_eq!(res.exit_code, 0);

    // Verify sequential checkpoints 1..4
    assert_eq!(checkpoints.len(), 4);
    assert_eq!(checkpoints[0].0, 1);
    assert_eq!(checkpoints[1].0, 2);
    assert_eq!(checkpoints[2].0, 3);
    assert_eq!(checkpoints[3].0, 4);

    let delta1: serde_json::Value = serde_json::from_slice(checkpoints[0].1.as_slice()).unwrap();
    assert_eq!(delta1["stage"], "landmark_extraction");
    assert_eq!(delta1["progress"], 0.25);

    let delta4: serde_json::Value = serde_json::from_slice(checkpoints[3].1.as_slice()).unwrap();
    assert_eq!(delta4["stage"], "animation_export");
    assert_eq!(delta4["progress"], 1.00);

    // Verify asset bytes
    assert!(res.asset_bytes.is_some());
    let asset = res.asset_bytes.unwrap();
    assert!(asset.len() >= 20);
    assert_eq!(&asset.as_slice()[0..4], b"glTF");
}

#[tokio::test]
async fn test_spawn_simulated_gpu_job_stops_when_stop_flag_raised() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let guard = super::StopOnDrop(std::sync::Arc::clone(&stop));
    drop(guard);
    assert!(
        stop.load(Ordering::Relaxed),
        "StopOnDrop must raise the stop flag on drop"
    );

    let interrupted = super::spawn_simulated_gpu_job(
        "stop_flag_test".into(),
        Vec::new(),
        Uuid::new_v4(),
        64,
        16,
        stop,
    )
    .await
    .expect("blocking task must join");
    match interrupted {
        Err(crate::gpu::GpuError::Interrupted(_)) => {}
        Err(other) => {
            panic!("expected GpuError::Interrupted when stop flag is raised, got Err({other:?})")
        }
        Ok(done) => panic!(
            "expected GpuError::Interrupted when stop flag is raised, got Ok(elapsed_ms={})",
            done.elapsed_ms
        ),
    }

    let unstopped = super::spawn_simulated_gpu_job(
        "stop_flag_control".into(),
        Vec::new(),
        Uuid::new_v4(),
        64,
        16,
        std::sync::Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("control blocking task must join");
    if let Err(err) = unstopped {
        panic!("expected Ok when stop flag stays false, got Err({err:?})");
    }
}
