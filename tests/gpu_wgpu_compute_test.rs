//! Comprehensive Integration Test Suite: Hardware-Accelerated Compute Shaders & CPU SIMD Simulator (Requirement R4).
//!
//! Validates:
//! 1. Mathematical equivalence between 8-wide CPU SIMD engine and triple-loop reference oracle.
//! 2. Deterministic pseudo-random matrix generation and seed derivation.
//! 3. Backward compatibility for simulated GPU execution (`simulate_gpu: true`).
//! 4. Strict negative capability gating for CPU-only workers.
//! 5. A worker that advertises a REAL GPU never fakes GPU work on the CPU: it computes on the GPU
//!    or fails the task with the reason.
//! 6. Physical WGSL shader compilation and execution on real GPU hardware (feature `gpu-wgpu`).
//! 7. End-to-end distributed cluster task scheduling to GPU-capable workers.
//! 8. Honest failure paths: parameters are honoured or rejected (never clamped), device loss and
//!    wgpu errors become failed results, the runtime is never blocked, timeouts and cancellation work.
//!
//! Tests that need a GPU skip (with a message) on a machine without one, unless
//! `RUSTY_GRID_REQUIRE_GPU=1` is set (the GPU-machine lane: `RUSTY_GRID_REQUIRE_GPU=1 cargo test
//! -p rusty_grid_worker --features gpu-wgpu`), in which case a missing GPU engine fails them.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::{MasterServer, ServerConfig, WorkerStatus};
use rusty_grid_worker::cpu_simd;
use rusty_grid_worker::runner::{
    RunnerConfig, TaskRunner, EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_TIMEOUT,
};
use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::WorkerClient;

fn make_capabilities(name: &str, cores: usize, has_gpu: bool, is_simulated: bool) -> WorkerCapabilities {
    WorkerCapabilities {
        name: name.into(),
        cpu_cores: cores,
        ram_mb: 8192,
        has_gpu,
        is_simulated_gpu: is_simulated,
        gpu_device_name: if is_simulated {
            Some("Simulated Virtual GPU (Matrix Engine)".into())
        } else if has_gpu {
            Some("Physical GPU Hardware".into())
        } else {
            None
        },
        tags: vec!["m4_test".into()],
        mobile: None,
    }
}

fn make_runner(has_gpu: bool, is_simulated: bool) -> (TaskRunner, tempfile::TempDir) {
    let tmp = tempdir().expect("Failed to create temporary directory for test sandbox");
    let caps = make_capabilities("test-worker", 4, has_gpu, is_simulated);
    let cfg = RunnerConfig::new(SandboxConfig::new(tmp.path()));
    let runner = TaskRunner::new(Uuid::new_v4(), caps, cfg);
    (runner, tmp)
}

/// A GpuCompute task with explicit parameters.
fn gpu_task(dim: u32, tile: u32, timeout_secs: u64) -> Task {
    Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "param_test_kernel".into(),
            input_data: b"param_test_payload".to_vec(),
            work_group_size: tile,
            simulated_matrix_dim: dim,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(timeout_secs),
    )
}

/// True if this build can compute on a real GPU here (feature compiled in AND an adapter starts).
fn real_gpu_engine_available() -> bool {
    #[cfg(feature = "gpu-wgpu")]
    {
        rusty_grid_worker::wgpu_engine::EngineProvider::shared().get().is_ok()
    }
    #[cfg(not(feature = "gpu-wgpu"))]
    {
        false
    }
}

/// On the GPU lane (`RUSTY_GRID_REQUIRE_GPU=1`) a missing GPU engine is a failure, not a skip.
fn require_gpu_lane_to_have_a_gpu(test: &str) {
    if std::env::var("RUSTY_GRID_REQUIRE_GPU").as_deref() == Ok("1") && !real_gpu_engine_available() {
        panic!(
            "RUSTY_GRID_REQUIRE_GPU=1 but {test} found no GPU engine (built with gpu-wgpu: {})",
            rusty_grid_worker::WGPU_COMPILED
        );
    }
}

/// The shared engine, or None (after saying so) on a machine / build without a GPU.
#[cfg(feature = "gpu-wgpu")]
fn gpu_engine_or_skip(test: &str) -> Option<Arc<rusty_grid_worker::wgpu_engine::WgpuEngine>> {
    require_gpu_lane_to_have_a_gpu(test);
    match rusty_grid_worker::wgpu_engine::EngineProvider::shared().get() {
        Ok(lease) => Some(lease.engine),
        Err(err) => {
            println!("{test}: skipped, no GPU engine on this host ({err})");
            None
        }
    }
}

async fn wait_for<F, Fut>(timeout: Duration, interval: Duration, mut predicate: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if predicate().await {
            return true;
        }
        tokio::time::sleep(interval).await;
    }
    false
}

// ============================================================================
// Scenario 1: Reference Oracle Mathematical Equivalence
// ============================================================================
#[test]
fn test_reference_oracle_mathematical_equivalence() {
    let test_dimensions = [16, 32, 48, 64, 128, 33];
    let seed = 0xdeadbeef_cafebabe_u64;

    for &dim in &test_dimensions {
        let (a, b) = cpu_simd::generate_deterministic_matrices(dim, seed);

        // Compute via 8-wide chunked unrolled SIMD engine
        let mut c_simd = vec![0.0f32; dim * dim];
        cpu_simd::matmul_simd(&a, &b, &mut c_simd, dim);

        // Compute via canonical reference triple-loop oracle
        let c_oracle = cpu_simd::reference_matmul(&a, &b, dim);

        assert_eq!(
            c_simd.len(),
            c_oracle.len(),
            "Matrix size mismatch for dimension {dim}x{dim}"
        );

        // Bitwise IEEE-754 equivalence assertion
        for idx in 0..c_simd.len() {
            let simd_bits = c_simd[idx].to_bits();
            let oracle_bits = c_oracle[idx].to_bits();
            assert_eq!(
                simd_bits, oracle_bits,
                "Float bitwise mismatch at index {idx} (row {}, col {}) for dim {dim}x{dim}: simd={}, oracle={}",
                idx / dim,
                idx % dim,
                c_simd[idx],
                c_oracle[idx]
            );
        }

        // Verify mathematical invariant metrics match exactly
        let m_simd = cpu_simd::compute_matrix_metrics(&c_simd, dim);
        let m_oracle = cpu_simd::compute_matrix_metrics(&c_oracle, dim);

        assert_eq!(
            m_simd.trace.to_bits(),
            m_oracle.trace.to_bits(),
            "Trace mismatch for {dim}x{dim}"
        );
        assert_eq!(
            m_simd.frobenius_norm.to_bits(),
            m_oracle.frobenius_norm.to_bits(),
            "Frobenius norm mismatch for {dim}x{dim}"
        );
        assert_eq!(
            m_simd.bit_hash, m_oracle.bit_hash,
            "Verification digest mismatch for {dim}x{dim}"
        );
    }
}

// ============================================================================
// Scenario 2: Deterministic Seed Derivation & Repeatability
// ============================================================================
#[test]
fn test_deterministic_seed_derivation() {
    let dummy_id_1 = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let dummy_id_2 = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();

    let input_payload = b"distributed_tensor_shard_alpha";

    // 1. When input data is provided, seed is derived purely from payload content
    let seed_payload_1 = cpu_simd::derive_seed(input_payload, dummy_id_1);
    let seed_payload_2 = cpu_simd::derive_seed(input_payload, dummy_id_2);
    assert_eq!(
        seed_payload_1, seed_payload_2,
        "Seed with payload must be identical regardless of UUID"
    );

    // 2. When input data is empty, seed is derived from task UUID bytes
    let seed_uuid_1 = cpu_simd::derive_seed(&[], dummy_id_1);
    let seed_uuid_2 = cpu_simd::derive_seed(&[], dummy_id_2);
    assert_ne!(
        seed_uuid_1, seed_uuid_2,
        "Seed without payload must depend on UUID bytes"
    );

    // 3. Repeatability: identical seed produces identical matrices
    let (a1, b1) = cpu_simd::generate_deterministic_matrices(32, seed_payload_1);
    let (a2, b2) = cpu_simd::generate_deterministic_matrices(32, seed_payload_1);
    assert_eq!(a1, a2);
    assert_eq!(b1, b2);

    // 4. Distinct seeds produce distinct matrices and digests
    let (a3, b3) = cpu_simd::generate_deterministic_matrices(32, seed_payload_1 + 1);
    assert_ne!(a1, a3);
    assert_ne!(b1, b3);

    let mut c1 = vec![0.0f32; 1024];
    let mut c3 = vec![0.0f32; 1024];
    cpu_simd::matmul_simd(&a1, &b1, &mut c1, 32);
    cpu_simd::matmul_simd(&a3, &b3, &mut c3, 32);

    let m1 = cpu_simd::compute_matrix_metrics(&c1, 32);
    let m3 = cpu_simd::compute_matrix_metrics(&c3, 32);
    assert_ne!(m1.bit_hash, m3.bit_hash);
}

// ============================================================================
// Scenario 3: Simulated GPU Backward Compatibility
// ============================================================================
#[tokio::test]
async fn test_simulated_gpu_backward_compatibility() {
    let (runner, _tmp) = make_runner(true, true);

    let task = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "backward_compat_gemm".into(),
            input_data: b"legacy_compat_payload".to_vec(),
            work_group_size: 16,
            simulated_matrix_dim: 48,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(30),
    );

    let res = runner.execute_task(&task, None, None).await;

    assert_eq!(res.exit_code, 0);
    assert!(res.is_gpu_executed);
    assert!(res.execution_time_ms >= 1);
    assert_eq!(
        res.device_name,
        Some("Simulated Virtual GPU (Matrix Engine)".to_string())
    );

    // Check all legacy string assertions expected by existing test suites
    assert!(res.stdout.contains("[GPU COMPUTE SIMULATOR]"));
    assert!(res.stdout.contains("Device: Simulated Virtual GPU (Matrix Engine)"));
    assert!(res.stdout.contains("Kernel: backward_compat_gemm"));
    assert!(res.stdout.contains("Matrix Dimension: 48x48"));
    assert!(res.stdout.contains("Status: VERIFIED_OK"));
    assert!(res.stdout.contains("Matrix Trace:"));
    assert!(res.stdout.contains("Frobenius Norm:"));
    assert!(res.stdout.contains("Verification Digest:"));
}

// ============================================================================
// Scenario 4: Negative Capability Gating
// ============================================================================
#[tokio::test]
async fn test_negative_capability_gating() {
    let (runner, _tmp) = make_runner(false, false);

    let task = Task::new(
        TaskSpec::gpu_compute("restricted_kernel", 32),
        TaskRequirements::gpu(30),
    );

    let res = runner.execute_task(&task, None, None).await;

    assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(!res.is_gpu_executed);
    assert_eq!(res.execution_time_ms, 0);
    assert_eq!(res.device_name, None);
    assert!(res.error.is_some());
    assert!(res.error.unwrap().contains("Worker lacks GPU capability"));
    assert!(res.stderr.contains("Worker lacks GPU capability"));
}

// ============================================================================
// Scenario 5: A Real-GPU Worker Never Fakes GPU Work On The CPU
// ============================================================================
#[test]
fn test_cpu_simd_report_format() {
    // Direct validation of the CPU SIMD engine's report (the engine behind the simulator).
    let task_id = Uuid::new_v4();
    let (stdout, elapsed) = cpu_simd::execute_simd_compute_with_device(
        "fallback_kernel",
        b"weights",
        task_id,
        32,
        16,
        "CPU SIMD Fallback",
        "[GPU COMPUTE SIMULATOR]",
    );

    assert!(elapsed >= 1);
    assert!(stdout.contains("[GPU COMPUTE SIMULATOR]"));
    assert!(stdout.contains("Device: CPU SIMD Fallback"));
    assert!(stdout.contains("Backend: CPU_SIMD_FALLBACK"));
    assert!(stdout.contains("Kernel: fallback_kernel"));
    assert!(stdout.contains("Matrix Dimension: 32x32"));
    // VERIFIED_OK is earned: 1024 of 1024 entries were recomputed with an independent dot product.
    assert!(stdout.contains("Verification: 1024 of 1024 entries"), "{stdout}");
    assert!(stdout.contains("Status: VERIFIED_OK"));
}

#[tokio::test]
async fn test_real_gpu_worker_computes_on_the_gpu_or_fails_it_never_fakes_it() {
    // has_gpu = true, is_simulated = false: what a worker advertises when it really has a GPU.
    let (runner, _tmp) = make_runner(true, false);
    let task = Task::new(
        TaskSpec::gpu_compute("fallback_runner_test", 32),
        TaskRequirements::gpu(30),
    );

    let res = runner.execute_task(&task, None, None).await;

    if real_gpu_engine_available() {
        assert_eq!(res.exit_code, 0, "{:?}", res.error);
        assert!(res.is_gpu_executed);
        assert!(res.stdout.contains("[GPU COMPUTE WGPU]"), "{}", res.stdout);
        assert!(!res.stdout.contains("SIMULATOR"));
        assert!(!res.stdout.contains("CPU_SIMD"));
        assert!(res.stdout.contains("Status: VERIFIED_OK"));
        assert_ne!(res.device_name.as_deref(), Some("CPU SIMD Fallback"));
    } else {
        require_gpu_lane_to_have_a_gpu("test_real_gpu_worker_computes_on_the_gpu_or_fails_it_never_fakes_it");
        // No engine: the task FAILS with the reason. It must not succeed on the CPU while
        // claiming GPU execution (the old behaviour: exit 0, is_gpu_executed, VERIFIED_OK).
        assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR);
        assert!(!res.is_success());
        assert!(!res.is_gpu_executed);
        assert!(res.device_name.is_none());
        assert!(res.stdout.is_empty(), "no report for work that did not run: {}", res.stdout);
        let error = res.error.expect("failure must carry a message");
        if rusty_grid_worker::WGPU_COMPILED {
            assert!(error.contains("GPU unavailable"), "{error}");
        } else {
            assert!(error.contains("gpu-wgpu"), "{error}");
            assert!(error.contains("--features gpu-wgpu"), "{error}");
        }
        assert!(res.stderr.contains("Execution Error"));
    }
}

// ============================================================================
// Scenario 6: Physical WGPU Execution When Hardware Adapter Available
// ============================================================================
#[test]
fn test_physical_wgpu_execution_when_available() {
    #[cfg(feature = "gpu-wgpu")]
    {
        let Some(engine) = gpu_engine_or_skip("test_physical_wgpu_execution_when_available") else {
            return;
        };
        let adapter_info = &engine.adapter_info;
        println!(
            "Discovered physical GPU adapter: '{}' (backend: {:?}, type: {:?})",
            adapter_info.name, adapter_info.backend, adapter_info.device_type
        );

        let dim = 32;
        let input_data = b"hardware_wgpu_test_tensor";
        let task_id = Uuid::new_v4();
        let seed = cpu_simd::derive_seed(input_data, task_id);
        let (a, b) = cpu_simd::generate_deterministic_matrices(dim, seed);
        let mut c_expected = vec![0.0f32; dim * dim];
        cpu_simd::matmul_simd(&a, &b, &mut c_expected, dim);

        let outcome = engine
            .execute_gemm("wgsl_gemm_hardware_test", input_data, task_id, dim, 16)
            .expect("execute_gemm failed on physical GPU");

        println!("Physical GPU execution elapsed: {} ms", outcome.elapsed_ms);
        assert!(!outcome.device_name.is_empty());
        assert_eq!(outcome.device_name, adapter_info.name);
        assert!(outcome.stdout.contains("[GPU COMPUTE WGPU]"));
        assert!(outcome.verification.passed());
        assert!(outcome.stdout.contains("Status: VERIFIED_OK"));
        assert_eq!(outcome.matrix_c.len(), dim * dim);

        // Verify numerical convergence between GPU shader and CPU reference oracle
        for i in 0..dim * dim {
            let diff = (outcome.matrix_c[i] - c_expected[i]).abs();
            assert!(
                diff < 1e-2,
                "GPU vs CPU mismatch at index {i}: gpu={}, cpu={}, diff={}",
                outcome.matrix_c[i],
                c_expected[i],
                diff
            );
        }
        println!("Physical GPU GEMM mathematical validation passed successfully!");
    }

    #[cfg(not(feature = "gpu-wgpu"))]
    {
        // Without the feature there is no GPU engine: say so, refuse on the GPU lane.
        require_gpu_lane_to_have_a_gpu("test_physical_wgpu_execution_when_available");
        println!("Feature 'gpu-wgpu' not enabled in current build; physical shader execution skipped.");
    }
}

// ============================================================================
// Scenario 7: End-to-End Cluster GPU Task Distribution
// ============================================================================
#[tokio::test]
async fn test_e2e_cluster_gpu_task_distribution() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let (w1_tx, w1_rx) = watch::channel(false);
    let (w2_tx, w2_rx) = watch::channel(false);

    // Worker 1: CPU-only worker (2 cores, no GPU)
    let mut w1 = WorkerClient::from_options(
        master_addr.clone(),
        Some("worker-cpu-only".into()),
        Some(2),
        false,
    );
    let w1_id = w1.worker_id();

    // Worker 2: GPU-capable worker (4 cores, simulated GPU)
    let mut w2 = WorkerClient::from_options(
        master_addr.clone(),
        Some("worker-gpu-accelerated".into()),
        Some(4),
        true,
    );
    let w2_gpu_id = w2.worker_id();

    tokio::spawn(async move {
        let _ = w1.run(w1_rx).await;
    });
    tokio::spawn(async move {
        let _ = w2.run(w2_rx).await;
    });

    // Wait for both workers to register
    let ready = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 2 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;

    assert!(ready, "Cluster workers failed to register within timeout");

    // Submit a GPU-demanding computation
    let gpu_task = Task::new(
        TaskSpec::gpu_compute("e2e_cluster_gemm", 32),
        TaskRequirements::gpu(15),
    );

    let task_id = master
        .submit_task(gpu_task)
        .await
        .expect("Failed to submit GPU task");

    let result = master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .expect("GPU task execution timed out");

    assert_eq!(result.exit_code, 0, "GPU compute task must exit with 0");
    assert_eq!(
        result.worker_id, w2_gpu_id,
        "GPU compute task must be scheduled exclusively to GPU-capable worker"
    );
    assert_ne!(
        result.worker_id, w1_id,
        "GPU compute task must never be scheduled to CPU-only worker"
    );
    assert!(
        result.is_gpu_executed,
        "TaskResult must indicate GPU execution"
    );
    assert!(
        result.device_name.is_some(),
        "TaskResult must include device telemetry"
    );
    assert!(
        result.stdout.contains("Status: VERIFIED_OK"),
        "Stdout must contain VERIFIED_OK"
    );

    // Clean teardown
    let _ = w1_tx.send(true);
    let _ = w2_tx.send(true);
    let _ = master.shutdown();
}

// ============================================================================
// Scenario 8: High-Concurrency GPU Compute on a Single Worker
// ============================================================================
#[tokio::test]
async fn test_adversarial_concurrent_gpu_compute_invocations() {
    #[cfg(feature = "gpu-wgpu")]
    {
        if let Some(engine) = gpu_engine_or_skip("test_adversarial_concurrent_gpu_compute_invocations") {
            // 1. Direct concurrent execute_gemm invocations across 12 threads
            const NUM_TASKS: usize = 12;
            let mut handles = Vec::with_capacity(NUM_TASKS);

            for i in 0..NUM_TASKS {
                let eng = Arc::clone(&engine);
                handles.push(tokio::task::spawn_blocking(move || {
                    let task_id = Uuid::new_v4();
                    let payload = format!("concurrent_tensor_batch_{i}").into_bytes();
                    let dim = 32;
                    let outcome = eng
                        .execute_gemm(&format!("concurrent_gemm_{i}"), &payload, task_id, dim, 16)
                        .expect("Concurrent execute_gemm must not fail");
                    (i, outcome)
                }));
            }

            for h in handles {
                let (idx, outcome) = h.await.expect("Task panicked");
                assert!(
                    outcome.stdout.contains("Status: VERIFIED_OK"),
                    "Task {idx} must verify OK"
                );
                assert_eq!(outcome.matrix_c.len(), 32 * 32);
            }
        }
    }

    // 2. High-concurrency execution via TaskRunner on a worker that advertises a real GPU:
    // every task runs on the GPU (when there is one) or fails with a message, none panics.
    let (runner, _tmp) = make_runner(true, false);
    let runner = Arc::new(runner);

    const RUNNER_TASKS: usize = 10;
    let mut task_handles = Vec::with_capacity(RUNNER_TASKS);

    for i in 0..RUNNER_TASKS {
        let r = Arc::clone(&runner);
        task_handles.push(tokio::spawn(async move {
            let task = Task::new(
                TaskSpec::gpu_compute(format!("runner_concurrent_{i}"), 32),
                TaskRequirements::gpu(30),
            );
            r.execute_task(&task, None, None).await
        }));
    }

    for (idx, h) in task_handles.into_iter().enumerate() {
        let res = h.await.expect("Runner task panicked");
        if real_gpu_engine_available() {
            assert_eq!(res.exit_code, 0, "Task {idx} must succeed: {:?}", res.error);
            assert!(res.is_gpu_executed, "Task {idx} must be GPU executed");
            assert!(res.device_name.is_some(), "Task {idx} must report device name");
            assert!(
                res.stdout.contains("Status: VERIFIED_OK"),
                "Task {idx} must contain VERIFIED_OK in stdout"
            );
        } else {
            require_gpu_lane_to_have_a_gpu("test_adversarial_concurrent_gpu_compute_invocations");
            assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR, "Task {idx}");
            assert!(!res.is_gpu_executed, "Task {idx} must not claim GPU execution");
            assert!(res.error.is_some(), "Task {idx} must say why it failed");
        }
    }
}

// ============================================================================
// Scenario 9: Parameters Are Honoured Or Rejected, Never Silently Clamped
// ============================================================================
#[tokio::test]
async fn test_gpu_task_parameters_are_honoured_or_rejected_never_clamped() {
    // The simulated GPU claims 1 <= dim <= 2048 and work groups up to 32x32.
    let (runner, _tmp) = make_runner(true, true);

    // Out of range: a clear error naming the parameter and the limit, and no report at all.
    for (dim, tile, needle) in [
        (0, 16, "simulated_matrix_dim must be at least 1"),
        (16, 0, "work_group_size must be at least 1"),
        (100_000, 16, "simulated_matrix_dim 100000 is above what this device accepts (at most 2048"),
        (2049, 16, "at most 2048"),
        (64, 1000, "work_group_size 1000 is above"),
        (64, 33, "work_group_size 33 is above"),
    ] {
        let res = runner.execute_task(&gpu_task(dim, tile, 10), None, None).await;
        assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR, "dim {dim} tile {tile}");
        assert!(!res.is_gpu_executed, "dim {dim} tile {tile}");
        assert!(res.stdout.is_empty(), "dim {dim} tile {tile}: {}", res.stdout);
        let error = res.error.expect("error message");
        assert!(error.contains(needle), "dim {dim} tile {tile}: {error}");
    }

    // In range: used exactly as asked. 513 is one above the old silent clamp of 512, 4 below its
    // old minimum work group edge.
    let res = runner.execute_task(&gpu_task(513, 4, 120), None, None).await;
    assert_eq!(res.exit_code, 0, "{:?}", res.error);
    assert!(res.stdout.contains("Matrix Dimension: 513x513"), "{}", res.stdout);
    assert!(res.stdout.contains("Work Group Size: 4x4"), "{}", res.stdout);
    let res = runner.execute_task(&gpu_task(5, 1, 10), None, None).await;
    assert_eq!(res.exit_code, 0, "{:?}", res.error);
    assert!(res.stdout.contains("Matrix Dimension: 5x5"), "{}", res.stdout);
    assert!(res.stdout.contains("Status: VERIFIED_OK"));
}

#[cfg(feature = "gpu-wgpu")]
#[test]
fn test_wgpu_engine_honours_dim_and_work_group_or_rejects_with_a_clear_error() {
    use rusty_grid_worker::gpu::GpuError;
    let Some(engine) = gpu_engine_or_skip("test_wgpu_engine_honours_dim_and_work_group") else {
        return;
    };
    let limits = engine.device_limits();
    println!("device limits: {limits:?}");

    // Irregular dimensions and several work group edges produce correct products, and the report
    // prints the edge that was really dispatched.
    for (dim, tile) in [(1usize, 1usize), (7, 4), (17, 8), (33, 16), (100, 12), (129, 16)] {
        let outcome = engine
            .execute_gemm("tile_test", b"tile_payload", Uuid::new_v4(), dim, tile)
            .unwrap_or_else(|e| panic!("dim {dim} tile {tile}: {e}"));
        assert!(outcome.verification.passed(), "dim {dim} tile {tile}");
        assert!(
            outcome.stdout.contains(&format!("Work Group Size: {tile}x{tile}\n")),
            "{}",
            outcome.stdout
        );
        assert!(outcome.stdout.contains(&format!("Matrix Dimension: {dim}x{dim} ")));
    }

    // A work group of 32x32 = 1024 threads only on devices that allow it.
    let big_tile = engine.execute_gemm("tile_test", b"", Uuid::new_v4(), 64, 32);
    if limits.max_workgroup_invocations >= 1024 && limits.max_workgroup_edge >= 32 {
        assert!(big_tile.expect("32x32 allowed on this device").verification.passed());
    } else {
        assert!(matches!(big_tile, Err(GpuError::InvalidRequest(_))), "{big_tile:?}");
    }

    // Out of range: InvalidRequest before anything touches the device, naming the limit.
    for (dim, tile) in [(0usize, 16usize), (16, 0)] {
        let err = engine.execute_gemm("k", b"", Uuid::new_v4(), dim, tile).unwrap_err();
        assert!(matches!(err, GpuError::InvalidRequest(_)), "{err:?}");
    }
    let too_big = limits.max_supported_dim() as usize + 1;
    let err = engine.execute_gemm("k", b"", Uuid::new_v4(), too_big, 16).unwrap_err();
    let GpuError::InvalidRequest(message) = &err else {
        panic!("{err:?}");
    };
    assert!(message.contains(&format!("simulated_matrix_dim {too_big} is above")), "{message}");
    // The same holds for a size that does not even fit 32 bits.
    let err = engine.execute_gemm("k", b"", Uuid::new_v4(), usize::MAX, 16).unwrap_err();
    assert!(matches!(err, GpuError::InvalidRequest(_)), "{err:?}");
    // None of that harmed the engine.
    assert!(!engine.is_lost());
    assert!(engine.execute_gemm("k", b"", Uuid::new_v4(), 16, 16).is_ok());
}

#[cfg(feature = "gpu-wgpu")]
#[test]
fn gpu_output_matches_cpu_oracle_for_dims_1_15_16_17_255_512() {
    let Some(engine) = gpu_engine_or_skip("gpu_output_matches_cpu_oracle") else {
        return;
    };
    for dim in [1usize, 15, 16, 17, 255, 512] {
        let input = format!("oracle_dim_{dim}").into_bytes();
        let task_id = Uuid::new_v4();
        let seed = cpu_simd::derive_seed(&input, task_id);
        let (a, b) = cpu_simd::generate_deterministic_matrices(dim, seed);
        let mut expected = vec![0.0f32; dim * dim];
        cpu_simd::matmul_simd(&a, &b, &mut expected, dim);

        let outcome = engine
            .execute_gemm("oracle_kernel", &input, task_id, dim, 16)
            .unwrap_or_else(|e| panic!("dim {dim}: {e}"));

        assert_eq!(outcome.matrix_c.len(), dim * dim);
        // Every element within the rigorous f32 dot-product bound of the CPU result.
        let bound = dim as f32 * f32::EPSILON;
        for (i, (&gpu, &cpu)) in outcome.matrix_c.iter().zip(&expected).enumerate() {
            let tolerance = bound * cpu.abs().max(1e-3) * 2.0;
            assert!((gpu - cpu).abs() <= tolerance, "dim {dim} index {i}: gpu={gpu} cpu={cpu}");
        }
        // The two engines' traces agree to far better than a unit in the last place of a sum.
        let cpu_metrics = cpu_simd::compute_matrix_metrics(&expected, dim);
        let relative = (outcome.metrics.trace - cpu_metrics.trace).abs() / cpu_metrics.trace.abs().max(1e-9);
        assert!(relative < 1e-4, "dim {dim}: trace {} vs {}", outcome.metrics.trace, cpu_metrics.trace);
    }
}

// ============================================================================
// Scenario 10: GPU Device Loss / Errors Become Failed Results, Never Panics Or Hangs
// ============================================================================
#[cfg(feature = "gpu-wgpu")]
#[test]
fn device_loss_returns_failure_result_not_panic() {
    use rusty_grid_worker::gpu::GpuError;
    // An engine of our own: destroying the shared one would hurt the other tests.
    require_gpu_lane_to_have_a_gpu("device_loss_returns_failure_result_not_panic");
    let engine = match rusty_grid_worker::wgpu_engine::WgpuEngine::try_init() {
        Ok(Some(engine)) => engine,
        other => {
            println!("device_loss_returns_failure_result_not_panic: skipped, no GPU engine ({other:?})");
            return;
        }
    };
    assert!(engine.execute_gemm("before", b"x", Uuid::new_v4(), 32, 16).is_ok());

    // Destroy the underlying wgpu Device to simulate physical device loss / driver reset.
    engine.device.destroy();

    // wgpu's default handler panics here ("uncaptured error handler is not set"); the engine
    // must turn that into an Err, and keep turning it into one.
    for attempt in 0..3 {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            engine.execute_gemm("post_device_loss_kernel", b"lost_device_tensor", Uuid::new_v4(), 32, 16)
        }));
        let outcome = result.unwrap_or_else(|_| panic!("attempt {attempt}: execute_gemm panicked on a lost device"));
        let err = outcome.expect_err("a destroyed device cannot compute");
        assert!(matches!(err, GpuError::Device(_)), "attempt {attempt}: {err:?}");
        assert!(!err.to_string().is_empty());
        println!("attempt {attempt}: {err}");
    }
    assert!(engine.is_lost(), "an engine that failed on the device is marked lost");
}

#[cfg(feature = "gpu-wgpu")]
#[tokio::test]
async fn lost_engine_is_rebuilt_and_the_next_task_runs() {
    use rusty_grid_worker::wgpu_engine::EngineProvider;
    require_gpu_lane_to_have_a_gpu("lost_engine_is_rebuilt_and_the_next_task_runs");
    let provider = Arc::new(EngineProvider::new());
    let first = match provider.get() {
        Ok(lease) => lease,
        Err(err) => {
            println!("lost_engine_is_rebuilt_and_the_next_task_runs: skipped, no GPU engine ({err})");
            return;
        }
    };
    assert!(first.init_ms.is_some(), "the first lease built the engine");
    let again = provider.get().expect("engine still there");
    assert!(Arc::ptr_eq(&first.engine, &again.engine));
    assert!(again.init_ms.is_none(), "a running engine is handed out without a rebuild");

    let tmp = tempdir().unwrap();
    let runner = TaskRunner::new(
        Uuid::new_v4(),
        make_capabilities("rebuild-worker", 4, true, false),
        RunnerConfig::new(SandboxConfig::new(tmp.path())),
    )
    .with_gpu_engine_provider(Arc::clone(&provider));

    let ok = runner.execute_task(&gpu_task(32, 16, 30), None, None).await;
    assert_eq!(ok.exit_code, 0, "{:?}", ok.error);

    // The GPU dies under the worker: that task fails with a message (it does not hang or kill
    // the runner task) ...
    first.engine.device.destroy();
    let failed = runner.execute_task(&gpu_task(32, 16, 30), None, None).await;
    assert_eq!(failed.exit_code, EXIT_CODE_GENERAL_ERROR, "{:?}", failed.stdout);
    assert!(!failed.is_gpu_executed);
    assert!(failed.error.as_deref().is_some_and(|e| e.contains("GPU device error")), "{:?}", failed.error);

    // ... and the next task gets a fresh engine instead of the dead one cached for ever.
    let recovered = runner.execute_task(&gpu_task(32, 16, 30), None, None).await;
    assert_eq!(recovered.exit_code, 0, "{:?}", recovered.error);
    assert!(recovered.is_gpu_executed);
    assert!(recovered.stdout.contains("Engine Init Time:"), "{}", recovered.stdout);
    let rebuilt = provider.get().expect("engine");
    assert!(!Arc::ptr_eq(&first.engine, &rebuilt.engine), "a new engine replaced the lost one");
}

#[cfg(feature = "gpu-wgpu")]
#[test]
fn cancelled_or_expired_gemm_returns_interrupted_without_touching_the_gpu() {
    use rusty_grid_worker::gpu::GpuError;
    use rusty_grid_worker::wgpu_engine::GemmControl;
    use std::sync::atomic::AtomicBool;
    let Some(engine) = gpu_engine_or_skip("cancelled_or_expired_gemm") else {
        return;
    };
    let cancel = Arc::new(AtomicBool::new(true));
    let control = GemmControl {
        cancel: Some(cancel),
        ..Default::default()
    };
    let err = engine
        .execute_gemm_controlled("k", b"", Uuid::new_v4(), 64, 16, &control)
        .unwrap_err();
    assert!(matches!(err, GpuError::Interrupted(_)), "{err:?}");

    let expired = GemmControl {
        deadline: Some(Instant::now()),
        ..Default::default()
    };
    let err = engine
        .execute_gemm_controlled("k", b"", Uuid::new_v4(), 64, 16, &expired)
        .unwrap_err();
    assert!(matches!(err, GpuError::Interrupted(_)), "{err:?}");
    assert!(!engine.is_lost(), "an interruption is not a device failure");
}

// ============================================================================
// Scenario 10b: The Runtime Is Never Blocked; Timeout And Cancel Work On GPU-Class Tasks
// ============================================================================
#[tokio::test(flavor = "current_thread")]
async fn gpu_class_compute_does_not_block_the_async_runtime() {
    // One runtime thread, a CPU multiplication that takes a while in a debug build, and a ticker
    // that stands for the heartbeat. With the multiplication run inline the ticker stalls for the
    // whole multiplication; on a blocking thread it keeps ticking.
    let (runner, _tmp) = make_runner(true, true);
    let ticker = tokio::spawn(async {
        let mut worst = Duration::ZERO;
        let mut last = Instant::now();
        for _ in 0..600 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let now = Instant::now();
            worst = worst.max(now - last);
            last = now;
        }
        worst
    });

    let started = Instant::now();
    let res = runner.execute_task(&gpu_task(1000, 16, 120), None, None).await;
    let compute_wall = started.elapsed();
    assert_eq!(res.exit_code, 0, "{:?}", res.error);

    let worst_gap = ticker.await.unwrap();
    println!("multiplication took {compute_wall:?}, worst ticker gap {worst_gap:?}");
    assert!(
        worst_gap < Duration::from_millis(400),
        "the runtime stalled for {worst_gap:?} while a GPU-class task ran ({compute_wall:?})"
    );
}

#[test]
fn gpu_class_timeout_fires_and_stops_the_multiplication() {
    // A manual runtime, because dropping a runtime waits for its blocking threads: if the
    // multiplication were not told to stop, the drop would sit through the rest of it.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (runner, _tmp) = make_runner(true, true);
    let started = Instant::now();
    // 1500^3 * 2 = 6.75 GFLOP: many seconds in a debug build, so only the timeout can end this.
    let res = runtime.block_on(runner.execute_task(&gpu_task(1500, 16, 1), None, None));
    assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT, "{:?}", res.error);
    assert!(res.error.unwrap().contains("timed out"));
    assert!(started.elapsed() < Duration::from_secs(10), "{:?}", started.elapsed());

    let shutdown_started = Instant::now();
    drop(runtime);
    assert!(
        shutdown_started.elapsed() < Duration::from_millis(2500),
        "the timed-out multiplication kept running on its blocking thread for {:?}",
        shutdown_started.elapsed()
    );
}

#[tokio::test]
async fn gpu_class_cancel_is_honoured_promptly() {
    let (runner, _tmp) = make_runner(true, true);
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = cancel_tx.send("user requested cancel".to_string());
    });
    let started = Instant::now();
    let res = runner
        .execute_task(&gpu_task(1500, 16, 120), None, Some(cancel_rx))
        .await;
    assert_eq!(res.exit_code, EXIT_CODE_CANCELLED, "{:?}", res.error);
    assert!(started.elapsed() < Duration::from_secs(10), "{:?}", started.elapsed());
}

// ============================================================================
// Scenario 11: Mixed-Cluster Strict Negative Capability Routing Stress
// ============================================================================
#[tokio::test]
async fn test_adversarial_scheduling_isolation_mixed_cluster() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let (w1_tx, w1_rx) = watch::channel(false);
    let (w2_tx, w2_rx) = watch::channel(false);
    let (w3_tx, w3_rx) = watch::channel(false);

    // Worker 1: 4 CPU cores, CPU-only
    let mut w1 = WorkerClient::from_options(
        master_addr.clone(),
        Some("cluster-cpu-1".into()),
        Some(4),
        false,
    );
    let w1_id = w1.worker_id();

    // Worker 2: 4 CPU cores, CPU-only
    let mut w2 = WorkerClient::from_options(
        master_addr.clone(),
        Some("cluster-cpu-2".into()),
        Some(4),
        false,
    );
    let w2_id = w2.worker_id();

    // Worker 3: 2 CPU cores, Simulated GPU enabled
    let mut w3 = WorkerClient::from_options(
        master_addr.clone(),
        Some("cluster-gpu-3".into()),
        Some(2),
        true,
    );
    let w3_gpu_id = w3.worker_id();

    tokio::spawn(async move { let _ = w1.run(w1_rx).await; });
    tokio::spawn(async move { let _ = w2.run(w2_rx).await; });
    tokio::spawn(async move { let _ = w3.run(w3_rx).await; });

    let ready = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 3 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;
    assert!(ready, "All 3 workers must register");

    // Submit 12 interleaved tasks: 6 CPU tasks and 6 GPU tasks
    let mut cpu_task_ids = Vec::new();
    let mut gpu_task_ids = Vec::new();

    for i in 0..6 {
        let cpu_t = Task::new(
            TaskSpec::command("echo", vec![format!("mixed_cpu_{i}")]),
            TaskRequirements::generic(1, 10),
        );
        let cid = master.submit_task(cpu_t).await.expect("submit CPU task");
        cpu_task_ids.push(cid);

        let gpu_t = Task::new(
            TaskSpec::gpu_compute(format!("mixed_gpu_{i}"), 32),
            TaskRequirements::gpu(15),
        );
        let gid = master.submit_task(gpu_t).await.expect("submit GPU task");
        gpu_task_ids.push(gid);
    }

    // Await all 12 tasks
    let mut all_wait_futs = Vec::new();
    for &id in &cpu_task_ids {
        all_wait_futs.push(master.wait_task(id, Some(Duration::from_secs(8))));
    }
    for &id in &gpu_task_ids {
        all_wait_futs.push(master.wait_task(id, Some(Duration::from_secs(8))));
    }

    let mut results = Vec::new();
    for fut in all_wait_futs {
        results.push(fut.await);
    }

    // 1. Verify CPU tasks
    for (i, res) in results[0..6].iter().enumerate() {
        let r = res.as_ref().unwrap_or_else(|e| panic!("CPU task {i} failed: {e}"));
        assert_eq!(r.exit_code, 0);
        assert!(!r.is_gpu_executed);
    }

    // 2. Strict negative capability assertion:
    // 100% of GPU tasks MUST have been executed by Worker 3 (GPU), and 0% by Worker 1 or 2!
    for (i, res) in results[6..12].iter().enumerate() {
        let r = res.as_ref().unwrap_or_else(|e| panic!("GPU task {i} failed: {e}"));
        assert_eq!(r.exit_code, 0);
        assert!(r.is_gpu_executed);
        assert_eq!(
            r.worker_id, w3_gpu_id,
            "GPU task {i} MUST be executed by Worker 3 (GPU-capable); assigned to {}",
            r.worker_id
        );
        assert_ne!(
            r.worker_id, w1_id,
            "CRITICAL BUG: GPU task {i} assigned to non-GPU Worker 1!"
        );
        assert_ne!(
            r.worker_id, w2_id,
            "CRITICAL BUG: GPU task {i} assigned to non-GPU Worker 2!"
        );
    }

    // 3. Negative routing when GPU worker is disconnected:
    // Disconnect Worker 3
    let _ = w3_tx.send(true);

    let w3_disconnected = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.iter().find(|w| w.worker_id == w3_gpu_id).map(|w| w.status)
                == Some(WorkerStatus::Disconnected)
        },
    )
    .await;
    assert!(w3_disconnected, "Worker 3 must transition to Disconnected");

    // Submit a GPU task while ONLY CPU workers are active
    let stranded_gpu = Task::new(
        TaskSpec::gpu_compute("stranded_gpu_task", 32),
        TaskRequirements::gpu(15),
    );
    let stranded_id = master.submit_task(stranded_gpu).await.expect("submit stranded");

    // Wait 500ms: Task MUST remain in Queued state and NEVER spill over to CPU workers
    tokio::time::sleep(Duration::from_millis(500)).await;
    let status = master.get_task_status(stranded_id).await.expect("status check");
    assert_eq!(
        status,
        rusty_grid_core::task::TaskStatus::Queued,
        "Stranded GPU task MUST remain Queued; never assign to CPU workers"
    );

    // Verify CPU workers are still healthy and executing generic tasks
    let quick_cpu = Task::new(
        TaskSpec::command("echo", vec!["cpu_alive".into()]),
        TaskRequirements::generic(1, 5),
    );
    let quick_id = master.submit_task(quick_cpu).await.expect("quick submit");
    let quick_res = master.wait_task(quick_id, Some(Duration::from_secs(3))).await.expect("quick wait");
    assert_eq!(quick_res.exit_code, 0);
    assert!(quick_res.worker_id == w1_id || quick_res.worker_id == w2_id);

    // Teardown
    let _ = w1_tx.send(true);
    let _ = w2_tx.send(true);
    let _ = master.shutdown();
}

