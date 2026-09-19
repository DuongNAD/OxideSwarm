//! Adversarial Empirical Stress & Challenge Test Suite for Milestone 3 (Worker Task Execution & Sandboxing).
//!
//! Authored by Challenger M3-2.
//!
//! Verifies:
//! 1. Strict GPU capability gating: non-GPU worker unconditionally rejects any `GpuCompute` task
//!    with exit code 1, `is_gpu_executed: false`, execution_time_ms: 0, and descriptive error.
//! 2. Genuine matrix multiplication: on simulated GPU worker, computes $16 \times 16$, $64 \times 64$,
//!    and $128 \times 128$ matrix multiplication, asserting exact mathematical validity of Trace
//!    and Frobenius Norm against independent reference oracles, plus non-zero duration (`execution_time_ms >= 1`).
//! 3. Concurrency throttle & zero drift: floods worker with 20 parallel tasks when `cpu_cores = 2`
//!    and `cpu_cores = 4`, asserting maximum concurrent execution equals permitted cores, with zero
//!    active_tasks drift and full permit restoration (including adversarial mixed floods with failures & cancellations).
//! 4. Rust compilation edge cases: bad syntax, missing entry point, invalid compiler flags, and target_dir path traversal.
//! 5. Overall workspace and clippy cleanliness.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_worker::{
    RunnerConfig, SandboxConfig, TaskRunner, WorkerClient, WorkerConfig, EXIT_CODE_CANCELLED,
    EXIT_CODE_GENERAL_ERROR, EXIT_CODE_SUCCESS,
};

/// Helper: construct test capabilities.
fn make_test_capabilities(name: &str, cores: usize, has_gpu: bool) -> WorkerCapabilities {
    WorkerCapabilities {
        name: name.into(),
        cpu_cores: cores,
        ram_mb: 8192,
        has_gpu,
        is_simulated_gpu: has_gpu,
        gpu_device_name: if has_gpu {
            Some("Simulated Virtual GPU (Matrix Engine)".into())
        } else {
            None
        },
        tags: vec!["challenger_m3_2".into()],
        mobile: None,
    }
}

/// Helper: construct runner with temporary sandbox directory.
fn make_test_runner(has_gpu: bool) -> (TaskRunner, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("create temp dir for runner");
    let sandbox_cfg = SandboxConfig::new(temp_dir.path());
    let runner_cfg = RunnerConfig::new(sandbox_cfg);
    let capabilities = make_test_capabilities("challenger-runner", 4, has_gpu);
    let runner = TaskRunner::new(Uuid::new_v4(), capabilities, runner_cfg);
    (runner, temp_dir)
}

/// Independent mathematical reference oracle for tiled GPU matrix multiplication $C = A \times B$.
/// Computes expected (Trace, Frobenius Norm, 64-bit IEEE-754 bit hash digest).
fn reference_matrix_oracle(dim: usize, input_data: &[u8], task_uuid: &Uuid) -> (f64, f64, u64) {
    let seed = if !input_data.is_empty() {
        input_data
            .iter()
            .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64))
    } else {
        let bytes: [u8; 8] = task_uuid.as_bytes()[0..8].try_into().unwrap();
        u64::from_le_bytes(bytes)
    };

    let mut a = vec![0.0f32; dim * dim];
    let mut b = vec![0.0f32; dim * dim];
    let mut c = vec![0.0f32; dim * dim];

    for i in 0..dim {
        for j in 0..dim {
            let idx = i * dim + j;
            a[idx] = (((i as u64 * 37 + j as u64 * 17 + seed) % 1000) as f32) / 100.0;
            b[idx] = (((i as u64 * 19 + j as u64 * 43 + seed) % 1000) as f32) / 100.0;
        }
    }

    // Standard matrix multiplication C = A x B
    for i in 0..dim {
        for k in 0..dim {
            let a_val = a[i * dim + k];
            for j in 0..dim {
                c[i * dim + j] += a_val * b[k * dim + j];
            }
        }
    }

    // Invariant checksums: Trace, Frobenius Norm, IEEE-754 bit hash
    let mut trace = 0.0f64;
    let mut f_norm_sq = 0.0f64;
    let mut bit_hash: u64 = 0xcbf29ce484222325;

    for i in 0..dim {
        trace += c[i * dim + i] as f64;
    }
    for &val in &c {
        let v = val as f64;
        f_norm_sq += v * v;
        bit_hash ^= val.to_bits() as u64;
        bit_hash = bit_hash.wrapping_mul(0x100000001b3);
    }
    (trace, f_norm_sq.sqrt(), bit_hash)
}

/// Helper: parse GPU simulation stdout lines into structured metrics.
fn parse_gpu_output(stdout: &str) -> (f64, f64, u64, usize, u64) {
    let mut trace = 0.0;
    let mut f_norm = 0.0;
    let mut digest = 0u64;
    let mut dim = 0usize;
    let mut flops = 0u64;

    for line in stdout.lines() {
        if let Some(rest) = line.strip_prefix("Matrix Trace: ") {
            trace = rest.trim().parse().expect("parse trace");
        } else if let Some(rest) = line.strip_prefix("Frobenius Norm: ") {
            f_norm = rest.trim().parse().expect("parse frobenius norm");
        } else if let Some(rest) = line.strip_prefix("Verification Digest: 0x") {
            digest = u64::from_str_radix(rest.trim(), 16).expect("parse digest hex");
        } else if let Some(rest) = line.strip_prefix("Matrix Dimension: ") {
            // format: "{dim}x{dim} (FLOPs: {flops})"
            if let Some((dim_part, flops_part)) = rest.split_once("(FLOPs: ") {
                if let Some(d_str) = dim_part.split('x').next() {
                    dim = d_str.trim().parse().expect("parse dim");
                }
                if let Some(f_str) = flops_part.strip_suffix(')') {
                    flops = f_str.trim().parse().expect("parse flops");
                }
            }
        }
    }
    (trace, f_norm, digest, dim, flops)
}

// =========================================================================================
// CHALLENGE 1: Strict GPU Capability Gating
// =========================================================================================

#[tokio::test]
async fn test_strict_gpu_gating_unconditional_rejection() {
    let (runner, _tmp) = make_test_runner(false);

    // Scenario 1a: GpuCompute with explicit gpu_required = true
    let task_gpu_req = Task::new(
        TaskSpec::gpu_compute("matrix_mult_fp32", 64),
        TaskRequirements::gpu(30),
    );
    let res_1a = runner.execute_task(&task_gpu_req, None, None).await;
    assert!(!res_1a.is_success());
    assert_eq!(res_1a.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(!res_1a.is_gpu_executed);
    assert_eq!(res_1a.execution_time_ms, 0);
    assert!(res_1a.stdout.is_empty());
    assert!(
        res_1a
            .error
            .as_ref()
            .unwrap()
            .contains("Worker lacks GPU capability"),
        "Expected descriptive error, got: {:?}",
        res_1a.error
    );
    assert!(res_1a.stderr.contains("Worker lacks GPU capability"));

    // Scenario 1b: GpuCompute with gpu_required = false in requirements (runner must STILL gate unconditionally)
    let mut req_generic = TaskRequirements::generic(1, 30);
    req_generic.gpu_required = false;
    let task_gpu_unmarked = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "unmarked_kernel".into(),
            input_data: vec![1, 2, 3, 4],
            work_group_size: 16,
            simulated_matrix_dim: 32,
            compute_intensity: 2,
        },
        req_generic,
    );
    let res_1b = runner.execute_task(&task_gpu_unmarked, None, None).await;
    assert!(!res_1b.is_success());
    assert_eq!(res_1b.exit_code, 1);
    assert!(!res_1b.is_gpu_executed);
    assert_eq!(res_1b.execution_time_ms, 0);
    assert!(res_1b
        .error
        .unwrap()
        .contains("Worker lacks GPU capability"));

    // Scenario 1c: BuiltinTest requiring GPU on non-GPU worker
    let task_builtin_gpu = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "gpu_test".into(),
            iterations: 100,
            duration_ms: 10,
            should_fail: false,
            require_gpu: true,
        },
        TaskRequirements::generic(1, 10),
    );
    let res_1c = runner.execute_task(&task_builtin_gpu, None, None).await;
    assert!(!res_1c.is_success());
    assert_eq!(res_1c.exit_code, 1);
    assert!(!res_1c.is_gpu_executed);
    assert!(res_1c
        .error
        .unwrap()
        .contains("Worker lacks GPU capability"));
}

#[tokio::test]
async fn test_strict_gpu_gating_via_worker_client_assignment() {
    let temp_dir = tempfile::tempdir().expect("temp dir for client");
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(2)
        .with_simulate_gpu(false)
        .with_sandbox_base_dir(temp_dir.path());
    let client = WorkerClient::new(config);
    let (tx, mut rx) = mpsc::channel(16);

    let gpu_task = Task::new(
        TaskSpec::gpu_compute("conv2d", 32),
        TaskRequirements::gpu(30),
    );
    let task_id = gpu_task.id;

    client.handle_assign_task(gpu_task, tx).await;

    // WorkerClient must reject immediately via outbound message without running
    let msg = rx.recv().await.expect("worker result message");
    match msg {
        WorkerMessage::TaskResult {
            task_id: tid,
            exit_code,
            is_gpu_executed,
            error,
            ..
        } => {
            assert_eq!(tid, task_id);
            assert_eq!(exit_code, 1);
            assert!(!is_gpu_executed);
            assert!(error
                .unwrap()
                .contains("insufficient for task requirements"));
        }
        other => panic!("Expected TaskResult failure, got {other:?}"),
    }

    assert_eq!(client.heartbeat_tracker().active_tasks(), 0);
    assert_eq!(client.concurrency_semaphore().available_permits(), 2);
}

// =========================================================================================
// CHALLENGE 2: Genuine Matrix Multiplication on Simulated GPU Worker
// =========================================================================================

#[tokio::test]
async fn test_genuine_matrix_multiplication_oracle_dimensions() {
    let (runner, _tmp) = make_test_runner(true);

    // Test matrix dimensions 16x16, 64x64, 128x128
    let test_cases = vec![
        (16u32, 16u32, Vec::new()), // 16x16, default work group, UUID seed
        (64u32, 16u32, b"adversarial_seed_alpha".to_vec()), // 64x64, custom seed
        (128u32, 32u32, b"adversarial_seed_beta".to_vec()), // 128x128, tile=32, custom seed
    ];

    for (dim, work_group_size, seed_bytes) in test_cases {
        let task = Task::new(
            TaskSpec::GpuCompute {
                kernel_name: format!("tiled_gemm_fp32_{dim}x{dim}"),
                input_data: seed_bytes.clone(),
                work_group_size,
                simulated_matrix_dim: dim,
                compute_intensity: 1,
            },
            TaskRequirements::gpu(60),
        );

        let res = runner.execute_task(&task, None, None).await;

        assert!(
            res.is_success(),
            "GpuCompute for dim {dim} failed: {:?}",
            res.error
        );
        assert_eq!(res.exit_code, EXIT_CODE_SUCCESS);
        assert!(res.is_gpu_executed);
        assert!(
            res.execution_time_ms >= 1,
            "Execution time must be non-zero (>= 1ms), got {}",
            res.execution_time_ms
        );

        // Independent oracle calculation
        let (oracle_trace, oracle_fnorm, oracle_digest) =
            reference_matrix_oracle(dim as usize, &seed_bytes, task.id.as_uuid());

        // Parse metrics from stdout
        let (p_trace, p_fnorm, p_digest, p_dim, p_flops) = parse_gpu_output(&res.stdout);

        assert_eq!(p_dim, dim as usize, "Parsed dim matches configured dim");
        let expected_flops = 2u64 * (dim as u64) * (dim as u64) * (dim as u64);
        assert_eq!(p_flops, expected_flops, "FLOPs count matches 2 * dim^3");

        // Verify mathematical invariants
        let trace_diff = (p_trace - oracle_trace).abs();
        let fnorm_diff = (p_fnorm - oracle_fnorm).abs();

        assert!(
            trace_diff < 0.01,
            "Trace mismatch for dim {dim}: parsed={p_trace}, oracle={oracle_trace}, diff={trace_diff}"
        );
        assert!(
            fnorm_diff < 0.01,
            "Frobenius norm mismatch for dim {dim}: parsed={p_fnorm}, oracle={oracle_fnorm}, diff={fnorm_diff}"
        );
        assert_eq!(
            p_digest, oracle_digest,
            "Verification digest mismatch for dim {dim}: parsed=0x{p_digest:016x}, oracle=0x{oracle_digest:016x}"
        );

        assert!(res.stdout.contains("Status: VERIFIED_OK"));
        assert!(p_trace > 0.0, "Trace must be strictly positive");
        assert!(p_fnorm > 0.0, "Frobenius norm must be strictly positive");
    }
}

// =========================================================================================
// CHALLENGE 3: Concurrency Throttle & Zero Active Tasks Drift
// =========================================================================================

async fn run_concurrency_flood_test(cores: usize, task_count: usize) {
    let temp_dir = tempfile::tempdir().expect("temp dir for concurrency client");
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(cores)
        .with_sandbox_base_dir(temp_dir.path());
    let client = WorkerClient::new(config);

    assert_eq!(
        client.concurrency_semaphore().available_permits(),
        cores,
        "Permits initially equal cores ({cores})"
    );
    assert_eq!(client.heartbeat_tracker().active_tasks(), 0);

    let (tx, mut rx) = mpsc::channel(128);

    // Track peak concurrency and permit saturation
    let max_active = Arc::new(AtomicUsize::new(0));
    let min_permits = Arc::new(AtomicUsize::new(cores));
    let stop_observer = Arc::new(AtomicBool::new(false));

    let tracker = Arc::clone(client.heartbeat_tracker());
    let sem = Arc::clone(client.concurrency_semaphore());
    let max_active_clone = Arc::clone(&max_active);
    let min_permits_clone = Arc::clone(&min_permits);
    let stop_observer_clone = Arc::clone(&stop_observer);

    let observer_handle = tokio::spawn(async move {
        while !stop_observer_clone.load(Ordering::Relaxed) {
            let active = tracker.active_tasks();
            let permits = sem.available_permits();
            max_active_clone.fetch_max(active, Ordering::SeqCst);
            min_permits_clone.fetch_min(permits, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    // Flood 20 parallel tasks simultaneously
    for i in 0..task_count {
        let task = Task::new(
            TaskSpec::BuiltinTest {
                test_name: format!("concurrency_flood_{i}"),
                iterations: 0,
                duration_ms: 35, // 35ms sleep guarantees queue buildup across cores
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 10),
        );
        client.handle_assign_task(task, tx.clone()).await;
    }

    // Collect results
    let mut running_progress_count = 0;
    let mut completed_results_count = 0;

    while completed_results_count < task_count {
        let msg = rx.recv().await.expect("message from worker");
        match msg {
            WorkerMessage::TaskProgress { status, .. } => {
                if status == TaskStatus::Running {
                    running_progress_count += 1;
                }
            }
            WorkerMessage::TaskResult { exit_code, .. } => {
                assert_eq!(exit_code, EXIT_CODE_SUCCESS);
                completed_results_count += 1;
            }
            other => panic!("Unexpected message: {other:?}"),
        }
    }

    stop_observer.store(true, Ordering::SeqCst);
    let _ = observer_handle.await;

    assert_eq!(running_progress_count, task_count);
    assert_eq!(completed_results_count, task_count);

    let peak_active = max_active.load(Ordering::SeqCst);
    let lowest_permits = min_permits.load(Ordering::SeqCst);

    assert!(
        peak_active <= cores,
        "Peak active tasks ({peak_active}) exceeded CPU core quota ({cores})"
    );
    assert_eq!(
        peak_active, cores,
        "20-task flood must fully saturate all {cores} permits"
    );
    assert_eq!(
        lowest_permits, 0,
        "All permits must have been exhausted during peak saturation"
    );

    // Allow RAII guard drop tasks to finalize
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Zero-drift assertions
    assert_eq!(
        client.heartbeat_tracker().active_tasks(),
        0,
        "Heartbeat tracker active_tasks must have ZERO drift (expected 0)"
    );
    assert_eq!(
        client.concurrency_semaphore().available_permits(),
        cores,
        "All {cores} semaphore permits must be restored"
    );
    assert!(
        client.active_task_table().read().await.is_empty(),
        "Active task table must be completely empty after completion"
    );
}

#[tokio::test]
async fn test_concurrency_throttle_flood_2_cores() {
    run_concurrency_flood_test(2, 20).await;
}

#[tokio::test]
async fn test_concurrency_throttle_flood_4_cores() {
    run_concurrency_flood_test(4, 20).await;
}

#[tokio::test]
async fn test_concurrency_throttle_mixed_adversarial_flood() {
    // Adversarial flood: 10 success, 5 intentional failures, 5 cancellations under cores = 2
    let cores = 2;
    let total_tasks = 20;
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(cores)
        .with_sandbox_base_dir(temp_dir.path());
    let client = WorkerClient::new(config);
    let (tx, mut rx) = mpsc::channel(128);

    let mut cancel_ids = Vec::new();

    for i in 0..total_tasks {
        let task = if i < 10 {
            // Success
            Task::new(
                TaskSpec::BuiltinTest {
                    test_name: format!("ok_{i}"),
                    iterations: 0,
                    duration_ms: 25,
                    should_fail: false,
                    require_gpu: false,
                },
                TaskRequirements::generic(1, 10),
            )
        } else if i < 15 {
            // Intentional failure
            Task::new(
                TaskSpec::BuiltinTest {
                    test_name: format!("fail_{i}"),
                    iterations: 0,
                    duration_ms: 0,
                    should_fail: true,
                    require_gpu: false,
                },
                TaskRequirements::generic(1, 10),
            )
        } else {
            // Long running to be cancelled
            let t = Task::new(
                TaskSpec::BuiltinTest {
                    test_name: format!("cancel_{i}"),
                    iterations: 0,
                    duration_ms: 5000,
                    should_fail: false,
                    require_gpu: false,
                },
                TaskRequirements::generic(1, 10),
            );
            cancel_ids.push(t.id);
            t
        };

        client.handle_assign_task(task, tx.clone()).await;
    }

    // Cancel the designated tasks
    tokio::time::sleep(Duration::from_millis(30)).await;
    for tid in cancel_ids {
        client
            .handle_cancel_task(tid, Some("Adversarial cancellation test".into()))
            .await;
    }

    let mut results = 0;
    let mut ok_results = 0;
    let mut fail_results = 0;
    let mut cancel_results = 0;

    while results < total_tasks {
        let msg = rx.recv().await.expect("message from worker");
        if let WorkerMessage::TaskResult { exit_code, .. } = msg {
            results += 1;
            if exit_code == EXIT_CODE_SUCCESS {
                ok_results += 1;
            } else if exit_code == EXIT_CODE_GENERAL_ERROR {
                fail_results += 1;
            } else if exit_code == EXIT_CODE_CANCELLED {
                assert_eq!(exit_code, 130);
                cancel_results += 1;
            }
        }
    }

    assert_eq!(results, 20);
    assert_eq!(ok_results, 10);
    assert_eq!(fail_results, 5);
    assert_eq!(cancel_results, 5);

    // Verify zero drift after mixed outcomes
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        client.heartbeat_tracker().active_tasks(),
        0,
        "Active tasks must return to 0 with zero drift after mixed outcomes"
    );
    assert_eq!(
        client.concurrency_semaphore().available_permits(),
        cores,
        "All permits must be restored after mixed outcomes"
    );
    assert!(client.active_task_table().read().await.is_empty());
}

// =========================================================================================
// CHALLENGE 4: Rust Compilation Edge Cases
// =========================================================================================

#[tokio::test]
async fn test_rust_compilation_bad_syntax_rejection() {
    let (runner, _tmp) = make_test_runner(false);

    let mut bad_sources = HashMap::new();
    bad_sources.insert(
        "src/lib.rs".into(),
        r#"
pub fn calculate() -> u32 {
    let a: bool = "not a bool";
    @#$%^&* syntax error here !!!
}
"#
        .into(),
    );

    let task = Task::new(
        TaskSpec::rust_compilation("bad_syntax_crate", bad_sources, vec![]),
        TaskRequirements::generic(1, 30),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(!res.is_success());
    assert_ne!(res.exit_code, 0);
    assert!(res.stderr.contains("error"));
    assert!(!res.is_gpu_executed);
}

#[tokio::test]
async fn test_rust_compilation_missing_entry_point_cases() {
    let (runner, _tmp) = make_test_runner(false);

    // Subcase 4a: Binary crate candidate (src/main.rs) without a main() entry point
    let mut no_main_sources = HashMap::new();
    no_main_sources.insert(
        "src/main.rs".into(),
        r#"
pub fn helper_function() -> i32 {
    42
}
"#
        .into(),
    );
    let task_no_main = Task::new(
        TaskSpec::rust_compilation("missing_main_crate", no_main_sources, vec![]),
        TaskRequirements::generic(1, 30),
    );
    let res_no_main = runner.execute_task(&task_no_main, None, None).await;
    assert!(!res_no_main.is_success());
    assert_ne!(res_no_main.exit_code, 0);
    assert!(
        res_no_main.stderr.contains("main") || res_no_main.stderr.contains("E0601"),
        "stderr should mention missing main, got: {}",
        res_no_main.stderr
    );

    // Subcase 4b: No .rs source files provided (only metadata / assets)
    let mut non_rust_sources = HashMap::new();
    non_rust_sources.insert("README.md".into(), "# Non-Rust Crate".into());
    non_rust_sources.insert("assets/data.json".into(), "{}".into());

    let task_non_rust = Task::new(
        TaskSpec::rust_compilation("non_rust_crate", non_rust_sources, vec![]),
        TaskRequirements::generic(1, 30),
    );
    let res_non_rust = runner.execute_task(&task_non_rust, None, None).await;
    assert!(!res_non_rust.is_success());
    assert_ne!(res_non_rust.exit_code, 0);
    assert!(
        res_non_rust.stderr.contains("couldn't read")
            || res_non_rust.stderr.contains("No such file"),
        "stderr should report unreadable entry file, got: {}",
        res_non_rust.stderr
    );

    // Subcase 4c: Empty source files map
    let task_empty = Task::new(
        TaskSpec::rust_compilation("empty_crate", HashMap::new(), vec![]),
        TaskRequirements::generic(1, 30),
    );
    let res_empty = runner.execute_task(&task_empty, None, None).await;
    assert!(!res_empty.is_success());
    assert_ne!(res_empty.exit_code, 0);
}

#[tokio::test]
async fn test_rust_compilation_invalid_compiler_flags() {
    let (runner, _tmp) = make_test_runner(false);

    let mut valid_sources = HashMap::new();
    valid_sources.insert(
        "src/lib.rs".into(),
        "pub fn greet() -> &'static str { \"hello\" }".into(),
    );

    // Subcase 4d: Non-existent compiler flag
    let task_invalid_flag = Task::new(
        TaskSpec::rust_compilation(
            "flag_test_crate",
            valid_sources.clone(),
            vec!["--non-existent-adversarial-flag-404".into()],
        ),
        TaskRequirements::generic(1, 30),
    );
    let res_invalid_flag = runner.execute_task(&task_invalid_flag, None, None).await;
    assert!(!res_invalid_flag.is_success());
    assert_ne!(res_invalid_flag.exit_code, 0);
    assert!(
        res_invalid_flag.stderr.contains("Unrecognized option")
            || res_invalid_flag.stderr.contains("unknown"),
        "stderr should report invalid option: {}",
        res_invalid_flag.stderr
    );

    // Subcase 4e: Unknown crate type
    let task_invalid_crate_type = Task::new(
        TaskSpec::rust_compilation(
            "crate_type_test",
            valid_sources.clone(),
            vec!["--crate-type=bogus_crate_type_xyz".into()],
        ),
        TaskRequirements::generic(1, 30),
    );
    let res_crate_type = runner
        .execute_task(&task_invalid_crate_type, None, None)
        .await;
    assert!(!res_crate_type.is_success());
    assert_ne!(res_crate_type.exit_code, 0);
    assert!(
        res_crate_type.stderr.contains("unknown crate type")
            || res_crate_type.stderr.contains("error"),
        "stderr should report unknown crate type: {}",
        res_crate_type.stderr
    );
}

#[tokio::test]
async fn test_rust_compilation_path_traversal_target_dir_rejection() {
    let (runner, _tmp) = make_test_runner(false);

    let mut sources = HashMap::new();
    sources.insert("src/lib.rs".into(), "pub fn ok() {}".into());

    let task_traversal = Task::new(
        TaskSpec::RustCompilation {
            crate_name: "traversal_crate".into(),
            source_files: sources,
            compiler_flags: vec![],
            target_dir: Some(PathBuf::from("../../target_escape")),
        },
        TaskRequirements::generic(1, 30),
    );

    let res = runner.execute_task(&task_traversal, None, None).await;
    assert!(!res.is_success());
    assert_eq!(res.exit_code, 1);
    assert!(
        res.error.unwrap().contains("Path traversal"),
        "Must reject target_dir path traversal"
    );
}

#[tokio::test]
async fn test_rust_compilation_multi_file_artifact_verification() {
    let (runner, _tmp) = make_test_runner(false);

    let mut sources = HashMap::new();
    sources.insert(
        "src/lib.rs".into(),
        r#"
pub mod math;
pub fn compute_sum(a: i64, b: i64) -> i64 {
    math::add(a, b)
}
"#
        .into(),
    );
    sources.insert(
        "src/math.rs".into(),
        r#"
pub fn add(a: i64, b: i64) -> i64 {
    a + b
}
"#
        .into(),
    );

    let task = Task::new(
        TaskSpec::rust_compilation("multi_math", sources, vec!["-O".into()]),
        TaskRequirements::generic(2, 60),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(
        res.is_success(),
        "Valid multi-file compilation should succeed: {}",
        res.stderr
    );
    assert_eq!(res.exit_code, 0);
    assert!(res
        .stdout
        .contains("[rusty_grid: compilation artifacts generated in target/:"));
    assert!(res.stdout.contains("libmulti_math.rlib"));
}
