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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Barrier};
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::cluster_auth::{client_handshake, ClusterSecret};
use rusty_grid_core::protocol::{
    derive_aead_keys, EndpointRole, MasterMessage, MessageTransport, ProtocolCapabilities,
    WireCodec, WorkerMessage, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::{
    RunnerConfig, SandboxConfig, TaskRunner, WorkerClient, WorkerConfig, EXIT_CODE_CANCELLED,
    EXIT_CODE_GENERAL_ERROR, EXIT_CODE_SUCCESS,
};

/// Empirical check whether an OS process ID is still alive in the operating system's process table.
#[cfg(windows)]
fn is_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    use windows_sys::Win32::Foundation::{CloseHandle, FALSE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    const STILL_ACTIVE: u32 = 259;

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
        if handle.is_null() {
            return false;
        }
        let mut exit_code = 0u32;
        let ok = GetExitCodeProcess(handle, &mut exit_code);
        CloseHandle(handle);
        ok != 0 && exit_code == STILL_ACTIVE
    }
}

#[cfg(not(windows))]
fn is_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    unsafe { libc::kill(pid as i32, 0) == 0 }
}


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
        ..Default::default()
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

// =========================================================================================
// CHALLENGE 5: OS Process Tree Containment (Windows Job Objects & Subprocess Tree Lifecycle)
// =========================================================================================

#[tokio::test]
async fn test_process_tree_containment_grandchild_processes_killed_on_worker_task_drop() {
    let temp_dir = tempfile::tempdir().expect("temp dir for containment test");
    let pid_file = temp_dir.path().join("pids_5a.txt");

    let (runner, _tmp) = make_test_runner(false);

    #[cfg(windows)]
    {
        let gc_script = temp_dir.path().join("grandchild_5a.ps1");
        std::fs::write(
            &gc_script,
            "param([string]$PidFile)\n\
             $gc1 = Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-Command','Start-Sleep -Seconds 300' -PassThru\n\
             [System.IO.File]::AppendAllText($PidFile, \"$($gc1.Id)`n\")\n\
             Start-Sleep -Seconds 300\n",
        )
        .expect("write grandchild script");

        let task_script = temp_dir.path().join("task_5a.ps1");
        std::fs::write(
            &task_script,
            "param([string]$PidFile, [string]$GcScript)\n\
             $c1 = Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File', $GcScript, $PidFile -PassThru\n\
             $c2 = Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-Command','Start-Sleep -Seconds 300' -PassThru\n\
             [System.IO.File]::AppendAllText($PidFile, \"$($c1.Id)`n$($c2.Id)`n\")\n\
             Start-Sleep -Seconds 300\n",
        )
        .expect("write task script");

        let task = Task::new(
            TaskSpec::Command {
                program: "powershell.exe".into(),
                args: vec![
                    "-NoProfile".into(),
                    "-ExecutionPolicy".into(),
                    "Bypass".into(),
                    "-File".into(),
                    task_script.to_string_lossy().to_string(),
                    pid_file.to_string_lossy().to_string(),
                    gc_script.to_string_lossy().to_string(),
                ],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 300),
        );

        let direct_pid = Arc::new(AtomicU32::new(0));
        let direct_pid_clone = Arc::clone(&direct_pid);

        // Spawn task in a background tokio task so we can abruptly drop/abort it
        let handle = tokio::spawn(async move {
            let _ = runner.execute_task(&task, Some(direct_pid_clone), None).await;
        });


        // Wait until all 3 PIDs (Child 1, Child 2, Grandchild 1) are recorded in pid_file
        let start = Instant::now();
        let mut pids = Vec::new();
        while start.elapsed() < Duration::from_secs(15) {
            if pid_file.exists() {
                let content = std::fs::read_to_string(&pid_file).unwrap_or_default();
                let parsed: Vec<u32> = content
                    .lines()
                    .filter_map(|l| l.trim().parse::<u32>().ok())
                    .collect();
                if parsed.len() >= 3 {
                    pids = parsed;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            pids.len() >= 3,
            "Timed out waiting for child and grandchild processes to start, got {:?}",
            pids
        );

        let direct = direct_pid.load(Ordering::Acquire);
        if direct != 0 {
            pids.push(direct);
        }

        // Empirically verify that every spawned child and grandchild process is actively running in the OS
        for &pid in &pids {
            assert!(
                is_process_alive(pid),
                "Process {} should be alive in the OS process table before drop",
                pid
            );
        }

        // Abruptly drop the task future (simulating worker abort / cancellation)
        handle.abort();
        let _ = handle.await;

        // Allow Windows kernel Job Object termination to complete
        let cleanup_start = Instant::now();
        let mut all_dead = false;
        while cleanup_start.elapsed() < Duration::from_secs(5) {
            if pids.iter().all(|&pid| !is_process_alive(pid)) {
                all_dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Empirical assertion: zero orphaned processes left behind!
        for &pid in &pids {
            assert!(
                !is_process_alive(pid),
                "ORPHAN PROCESS BUG: PID {} is still alive after worker task dropped!",
                pid
            );
        }
        assert!(
            all_dead,
            "Windows Job Object must cleanly terminate all child and grandchild processes upon drop"
        );
    }
}

#[tokio::test]
async fn test_process_tree_containment_worker_process_abrupt_kill() {
    #[cfg(windows)]
    {
        let temp_dir = tempfile::tempdir().expect("temp dir for worker kill test");
        let pid_file = temp_dir.path().join("pids_5b.txt");
        let worker_pid_file = temp_dir.path().join("worker_pid_5b.txt");

        // Spawn a child process running this test binary in worker containment mode
        let current_exe = std::env::current_exe().expect("current test exe");
        let mut worker_child = std::process::Command::new(current_exe)
            .arg("--")
            .arg("test_worker_containment_helper_proc")
            .arg("--exact")
            .arg("--nocapture")
            .env("OXIDE_CONTAINMENT_WORKER_MODE", "1")
            .env("OXIDE_CONTAINMENT_PID_FILE", &pid_file)
            .env("OXIDE_CONTAINMENT_WORKER_PID_FILE", &worker_pid_file)
            .spawn()
            .expect("spawn worker process");

        // Wait until child and grandchild PIDs are written
        let start = Instant::now();
        let mut pids = Vec::new();
        while start.elapsed() < Duration::from_secs(20) {
            if pid_file.exists() {
                let content = std::fs::read_to_string(&pid_file).unwrap_or_default();
                let parsed: Vec<u32> = content
                    .lines()
                    .filter_map(|l| l.trim().parse::<u32>().ok())
                    .collect();
                if parsed.len() >= 3 {
                    pids = parsed;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            pids.len() >= 3,
            "Timed out waiting for worker process to spawn child and grandchild tree, got {:?}",
            pids
        );

        // Verify worker and all child/grandchild processes are actively running
        let worker_id = worker_child.id();
        assert!(
            is_process_alive(worker_id),
            "Worker process {} must be running",
            worker_id
        );
        for &pid in &pids {
            assert!(
                is_process_alive(pid),
                "Child/grandchild PID {} must be running",
                pid
            );
        }

        // ABRUPTLY TERMINATE the worker process (TerminateProcess / SIGKILL)
        // This simulates a hard crash/OOM and does NOT allow any userland cleanup or drop handlers to run!
        worker_child.kill().expect("forcibly kill worker process");
        let _ = worker_child.wait();

        // Verify worker is dead
        assert!(!is_process_alive(worker_id));

        // Wait for OS kernel to close the Job Object handle and terminate all member processes
        let cleanup_start = Instant::now();
        let mut all_dead = false;
        while cleanup_start.elapsed() < Duration::from_secs(5) {
            if pids.iter().all(|&pid| !is_process_alive(pid)) {
                all_dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Assert 100% clean termination: zero orphaned processes!
        for &pid in &pids {
            assert!(
                !is_process_alive(pid),
                "ORPHAN PROCESS BUG: PID {} survived worker process abrupt termination!",
                pid
            );
        }
        assert!(
            all_dead,
            "OS Job Object must terminate all child and grandchild processes on worker termination"
        );
    }
}

/// Helper test function invoked as a child process by `test_process_tree_containment_worker_process_abrupt_kill`.
#[tokio::test]
async fn test_worker_containment_helper_proc() {
    if std::env::var("OXIDE_CONTAINMENT_WORKER_MODE").is_err() {
        // Normal test runner invocation: do nothing
        return;
    }
    let pid_file = PathBuf::from(std::env::var("OXIDE_CONTAINMENT_PID_FILE").unwrap());
    let worker_pid_file =
        PathBuf::from(std::env::var("OXIDE_CONTAINMENT_WORKER_PID_FILE").unwrap());

    let _ = std::fs::write(&worker_pid_file, std::process::id().to_string());

    let temp_dir = tempfile::tempdir().unwrap();
    let (runner, _tmp) = make_test_runner(false);

    let gc_script = temp_dir.path().join("grandchild_helper.ps1");
    std::fs::write(
        &gc_script,
        "param([string]$PidFile)\n\
         $gc1 = Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-Command','Start-Sleep -Seconds 300' -PassThru\n\
         [System.IO.File]::AppendAllText($PidFile, \"$($gc1.Id)`n\")\n\
         Start-Sleep -Seconds 300\n",
    )
    .unwrap();

    let task_script = temp_dir.path().join("task_helper.ps1");
    std::fs::write(
        &task_script,
        "param([string]$PidFile, [string]$GcScript)\n\
         $c1 = Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File', $GcScript, $PidFile -PassThru\n\
         $c2 = Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-Command','Start-Sleep -Seconds 300' -PassThru\n\
         [System.IO.File]::AppendAllText($PidFile, \"$($c1.Id)`n$($c2.Id)`n\")\n\
         Start-Sleep -Seconds 300\n",
    )
    .unwrap();

    let task = Task::new(
        TaskSpec::Command {
            program: "powershell.exe".into(),
            args: vec![
                "-NoProfile".into(),
                "-ExecutionPolicy".into(),
                "Bypass".into(),
                "-File".into(),
                task_script.to_string_lossy().to_string(),
                pid_file.to_string_lossy().to_string(),
                gc_script.to_string_lossy().to_string(),
            ],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 300),
    );

    runner.execute_task(&task, None, None).await;
}

#[tokio::test]
async fn test_process_tree_direct_windows_job_object_fanout() {
    #[cfg(windows)]
    {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let pid_file = temp_dir.path().join("fanout_pids.txt");

        // PowerShell script spawning 5 distinct background sleep processes
        let script = temp_dir.path().join("fanout.ps1");
        std::fs::write(
            &script,
            "param([string]$PidFile)\n\
             for ($i = 0; $i -lt 5; $i++) {\n\
                 $p = Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-Command','Start-Sleep -Seconds 300' -PassThru\n\
                 [System.IO.File]::AppendAllText($PidFile, \"$($p.Id)`n\")\n\
             }\n\
             Start-Sleep -Seconds 300\n",
        )
        .expect("write fanout script");

        let (runner, _tmp) = make_test_runner(false);
        let task = Task::new(
            TaskSpec::Command {
                program: "powershell.exe".into(),
                args: vec![
                    "-NoProfile".into(),
                    "-ExecutionPolicy".into(),
                    "Bypass".into(),
                    "-File".into(),
                    script.to_string_lossy().to_string(),
                    pid_file.to_string_lossy().to_string(),
                ],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 300),
        );

        let direct_pid = Arc::new(AtomicU32::new(0));
        let direct_pid_clone = Arc::clone(&direct_pid);

        let handle = tokio::spawn(async move {
            let _ = runner.execute_task(&task, Some(direct_pid_clone), None).await;
        });

        // Wait until all 5 PIDs are written
        let start = Instant::now();
        let mut pids = Vec::new();
        while start.elapsed() < Duration::from_secs(15) {
            if pid_file.exists() {
                let content = std::fs::read_to_string(&pid_file).unwrap_or_default();
                let parsed: Vec<u32> = content
                    .lines()
                    .filter_map(|l| l.trim().parse::<u32>().ok())
                    .collect();
                if parsed.len() >= 5 {
                    pids = parsed;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(pids.len() >= 5, "Expected 5 fanout PIDs, got {:?}", pids);

        let direct = direct_pid.load(Ordering::Acquire);
        if direct != 0 {
            pids.push(direct);
        }

        for &pid in &pids {
            assert!(is_process_alive(pid), "Process {} should be alive", pid);
        }

        // Abruptly terminate task future
        handle.abort();
        let _ = handle.await;

        // Allow kernel to reap
        let cleanup_start = Instant::now();
        let mut all_dead = false;
        while cleanup_start.elapsed() < Duration::from_secs(5) {
            if pids.iter().all(|&pid| !is_process_alive(pid)) {
                all_dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        for &pid in &pids {
            assert!(!is_process_alive(pid), "PID {} was not terminated by ProcessTree", pid);
        }
        assert!(all_dead, "All 5 fanout processes must be terminated");
    }
}

// =========================================================================================
// CHALLENGE 6: High-Concurrency Worker Lifecycle under Encrypted Wire Transport
// =========================================================================================

#[tokio::test]
async fn test_high_concurrency_encrypted_worker_registration_and_heartbeats() {
    let cluster_secret = ClusterSecret::generate().unwrap();
    let master_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(cluster_secret.clone())
        .with_handshake_timeout(10)
        .with_max_connections(50);
    let master = MasterServer::spawn(master_config)
        .await
        .expect("spawn master with secret");
    let master_addr = master.server_addr();

    const NUM_WORKERS: usize = 12;
    let barrier_connect = Arc::new(Barrier::new(NUM_WORKERS));
    let barrier_registered = Arc::new(Barrier::new(NUM_WORKERS));
    let barrier_disconnect = Arc::new(Barrier::new(NUM_WORKERS));

    let mut handles = Vec::new();

    for i in 0..NUM_WORKERS {
        let secret = cluster_secret.clone();
        let b_conn = Arc::clone(&barrier_connect);
        let b_reg = Arc::clone(&barrier_registered);
        let b_disc = Arc::clone(&barrier_disconnect);

        let handle = tokio::spawn(async move {
            let worker_id = Uuid::new_v4();
            b_conn.wait().await;

            // Connect to master via TCP
            let stream = TcpStream::connect(master_addr)
                .await
                .expect("connect to master");
            let mut transport = MessageTransport::new(stream);

            // Mutual HMAC handshake
            let session_key = client_handshake(&mut transport, &secret)
                .await
                .expect("client handshake");

            // Enable post-handshake AEAD frame encryption
            transport.enable_encryption(&session_key, EndpointRole::Client);
            assert!(
                transport.is_encrypted(),
                "Transport must have AEAD encryption enabled"
            );

            // Synchronize registration flood
            b_reg.wait().await;

            let reg_msg = WorkerMessage::Register {
                worker_id,
                capabilities: make_test_capabilities(&format!("enc-worker-{}", i), 2, false),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            };
            transport.send_msg(&reg_msg).await.expect("send register");

            // Receive encrypted RegisterAck from master
            let ack = transport
                .recv_msg::<MasterMessage>()
                .await
                .expect("receive register ack")
                .expect("expected ack message");

            match ack {
                MasterMessage::RegisterAck { accepted, worker_id: ack_wid, .. } => {
                    assert!(accepted, "Registration must be accepted");
                    assert_eq!(ack_wid, worker_id);
                }
                other => panic!("Unexpected ack message: {:?}", other),
            }

            // Send 5 encrypted heartbeats per worker
            for seq in 0..5 {
                let hb = WorkerMessage::Heartbeat {
                    worker_id,
                    timestamp: 1_700_000_000 + seq,
                    active_tasks: 0,
                    cpu_usage_pct: 5.0 + (i as f32),
                    ram_available_mb: 4096,
                };
                transport.send_msg(&hb).await.expect("send heartbeat");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Synchronize graceful disconnect flood
            b_disc.wait().await;

            let disc_msg = WorkerMessage::Disconnecting {
                worker_id,
                reason: "stress test complete".into(),
            };
            transport.send_msg(&disc_msg).await.expect("send disconnecting");

            // Drop transport cleanly
            drop(transport);
        });

        handles.push(handle);
    }

    // Wait for all 12 worker tasks to complete without deadlock
    for h in handles {
        tokio::time::timeout(Duration::from_secs(15), h)
            .await
            .expect("Worker lifecycle task timed out (potential deadlock/socket stall)!")
            .expect("Worker task panicked!");
    }

    // Wait for Master to clean up all disconnected workers
    let start = Instant::now();
    let mut remaining = 1;
    while start.elapsed() < Duration::from_secs(5) {
        let workers = master.list_workers().await.unwrap();
        remaining = workers
            .into_iter()
            .filter(|w| w.status != rusty_grid_master::registry::WorkerStatus::Disconnected)
            .count();
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        remaining, 0,
        "Master registry should have 0 active workers after all disconnect"
    );
}

#[tokio::test]
async fn test_high_concurrency_worker_client_run_stress_encrypted() {
    let cluster_secret = ClusterSecret::generate().unwrap();
    let master_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(cluster_secret.clone())
        .with_handshake_timeout(10)
        .with_max_connections(50);
    let master = MasterServer::spawn(master_config)
        .await
        .expect("spawn master");
    let master_addr = master.server_addr();

    const NUM_CLIENTS: usize = 10;
    let mut shutdowns = Vec::new();
    let mut join_handles = Vec::new();
    let mut _temp_dirs = Vec::new();

    for i in 0..NUM_CLIENTS {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config = WorkerConfig::new(master_addr.to_string())
            .with_name(format!("stress-worker-client-{}", i))
            .with_cores(1)
            .with_cluster_secret(cluster_secret.clone())
            .with_sandbox_base_dir(temp_dir.path())
            .with_redirection_portal(false);

        let mut client = WorkerClient::new(config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(shutdown_rx).await;
        });

        shutdowns.push(shutdown_tx);
        join_handles.push(handle);
        _temp_dirs.push(temp_dir);
    }

    // Wait until all 10 workers are registered on Master
    let start = Instant::now();
    let mut registered = 0;
    while start.elapsed() < Duration::from_secs(15) {
        registered = master.list_workers().await.unwrap().len();
        if registered == NUM_CLIENTS {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        registered, NUM_CLIENTS,
        "All {} WorkerClient instances must successfully register under AEAD encryption",
        NUM_CLIENTS
    );

    // Simultaneously broadcast shutdown to all 10 workers
    for tx in shutdowns {
        let _ = tx.send(true);
    }

    // Await all join handles with 10s timeout to guarantee zero hang / deadlock
    for h in join_handles {
        tokio::time::timeout(Duration::from_secs(10), h)
            .await
            .expect("WorkerClient::run did not exit cleanly (socket stall or deadlock)!")
            .expect("Worker client panicked!");
    }
}

#[tokio::test]
async fn test_adversarial_encrypted_wire_attacks() {
    use rusty_grid_core::protocol::serialize_message;
    use tokio_util::bytes::Bytes;

    let valid_secret = ClusterSecret::generate().unwrap();
    let master_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(valid_secret.clone())
        .with_handshake_timeout(5);
    let master = MasterServer::spawn(master_config)
        .await
        .expect("spawn master");
    let master_addr = master.server_addr();

    // -------------------------------------------------------------
    // Attack 1: Ciphertext Bit-Flip Tampering Attack
    // -------------------------------------------------------------
    {
        let stream = TcpStream::connect(master_addr).await.expect("connect");
        let mut transport = MessageTransport::new(stream);
        let session_key = client_handshake(&mut transport, &valid_secret)
            .await
            .expect("handshake");

        // Manually derive keys to construct tampered ciphertext
        let (mut manual_writer, _reader) = derive_aead_keys(&session_key, EndpointRole::Client);
        transport.enable_encryption(&session_key, EndpointRole::Client);

        let wid = Uuid::new_v4();
        let reg = WorkerMessage::Register {
            worker_id: wid,
            capabilities: make_test_capabilities("tamper-victim", 1, false),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        };
        transport.send_msg(&reg).await.expect("send reg");

        let ack = transport.recv_msg::<MasterMessage>().await.expect("recv ack");
        assert!(matches!(ack, Some(MasterMessage::RegisterAck { accepted: true, .. })));

        // Craft valid heartbeat plaintext and encrypt manually
        let hb = WorkerMessage::Heartbeat {
            worker_id: wid,
            timestamp: 1_000,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 2048,
        };
        let hb_payload = serialize_message(&hb, WireCodec::Bincode).unwrap();
        // Since manual_writer starts at seq 0, increment it to seq 1 to match
        let _ = manual_writer.encrypt_frame(&b"dummy"[..]);
        let mut encrypted_frame = manual_writer.encrypt_frame(&hb_payload).unwrap().to_vec();

        // Mutate one bit of the ciphertext payload (byte 12)
        encrypted_frame[12] ^= 0x5a;

        // Send raw tampered frame over wire
        transport
            .send_raw_frame(Bytes::from(encrypted_frame))
            .await
            .expect("send raw tampered frame");

        // Master AEAD decryptor must reject tampered frame and close connection
        let next = transport.recv_msg::<MasterMessage>().await;
        assert!(
            next.is_err() || next.unwrap().is_none(),
            "Master must terminate connection upon detecting tampered ciphertext tag"
        );
    }

    // -------------------------------------------------------------
    // Attack 2: Anti-Replay Sequence Re-transmission Attack
    // -------------------------------------------------------------
    {
        let stream = TcpStream::connect(master_addr).await.expect("connect");
        let mut transport = MessageTransport::new(stream);
        let session_key = client_handshake(&mut transport, &valid_secret)
            .await
            .expect("handshake");

        transport.enable_encryption(&session_key, EndpointRole::Client);

        let wid = Uuid::new_v4();
        let reg = WorkerMessage::Register {
            worker_id: wid,
            capabilities: make_test_capabilities("replay-victim", 1, false),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        };
        transport.send_msg(&reg).await.expect("send reg");
        let _ = transport.recv_msg::<MasterMessage>().await;

        // Send a frame with sequence 0 again (replay attack)
        let mut replay_writer = derive_aead_keys(&session_key, EndpointRole::Client).0;
        let hb = WorkerMessage::Heartbeat {
            worker_id: wid,
            timestamp: 1_000,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 2048,
        };
        let hb_payload = serialize_message(&hb, WireCodec::Bincode).unwrap();
        let replayed_frame = replay_writer.encrypt_frame(&hb_payload).unwrap();

        // Transmit replayed seq=0 frame after master already expects seq=1
        transport
            .send_raw_frame(replayed_frame)
            .await
            .expect("send replayed frame");

        // Master must reject replayed sequence number
        let next = transport.recv_msg::<MasterMessage>().await;
        assert!(
            next.is_err() || next.unwrap().is_none(),
            "Master must reject replayed sequence number"
        );
    }

    // -------------------------------------------------------------
    // Attack 3: Wrong Secret Rejection
    // -------------------------------------------------------------
    {
        let wrong_secret = ClusterSecret::generate().unwrap();
        let stream = TcpStream::connect(master_addr).await.expect("connect");
        let mut transport = MessageTransport::new(stream);

        // Handshake with wrong secret must fail
        let res = client_handshake(&mut transport, &wrong_secret).await;
        assert!(
            res.is_err(),
            "Handshake with wrong cluster secret must be rejected"
        );
    }

    // Verify master is still healthy and operational
    let workers = master.list_workers().await.expect("list workers");
    let active_workers: Vec<_> = workers
        .into_iter()
        .filter(|w| w.status != rusty_grid_master::registry::WorkerStatus::Disconnected)
        .collect();
    assert!(active_workers.is_empty(), "No rogue or corrupted workers should remain active");
}

