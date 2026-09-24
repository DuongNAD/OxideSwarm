//! Comprehensive Integration Test Suite: Hardware-Accelerated Compute Shaders & CPU SIMD Fallback (Requirement R4).
//!
//! Validates:
//! 1. Mathematical equivalence between 8-wide CPU SIMD engine and triple-loop reference oracle.
//! 2. Deterministic pseudo-random matrix generation and seed derivation.
//! 3. Backward compatibility for simulated GPU execution (`simulate_gpu: true`).
//! 4. Strict negative capability gating for CPU-only workers.
//! 5. Graceful CPU fallback when physical GPU execution is requested without hardware adapters.
//! 6. Physical WGSL shader compilation and execution on real GPU hardware (NVIDIA RTX 5060 Ti).
//! 7. End-to-end distributed cluster task scheduling to GPU-capable workers.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::{MasterServer, ServerConfig, WorkerStatus};
use rusty_grid_worker::cpu_simd;
use rusty_grid_worker::runner::{RunnerConfig, TaskRunner, EXIT_CODE_GENERAL_ERROR};
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
// Scenario 5: Graceful CPU Fallback on Hardware Absence
// ============================================================================
#[tokio::test]
async fn test_graceful_cpu_fallback_on_simulated_absence() {
    // 1. Direct validation of CPU SIMD fallback formatting
    let task_id = Uuid::new_v4();
    let (stdout, elapsed) = cpu_simd::execute_simd_compute_with_device(
        "fallback_kernel",
        b"weights",
        task_id,
        32,
        16,
        Instant::now(),
        "CPU SIMD Fallback",
        "[GPU COMPUTE SIMULATOR]",
    );

    assert!(elapsed >= 1);
    assert!(stdout.contains("[GPU COMPUTE SIMULATOR]"));
    assert!(stdout.contains("Device: CPU SIMD Fallback"));
    assert!(stdout.contains("Backend: CPU_SIMD_FALLBACK"));
    assert!(stdout.contains("Kernel: fallback_kernel"));
    assert!(stdout.contains("Matrix Dimension: 32x32"));
    assert!(stdout.contains("Status: VERIFIED_OK"));

    // 2. Validate runner execution for hardware-configured worker falling back gracefully
    let (runner, _tmp) = make_runner(true, false);
    let task = Task::new(
        TaskSpec::gpu_compute("fallback_runner_test", 32),
        TaskRequirements::gpu(30),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert_eq!(res.exit_code, 0);
    assert!(res.is_gpu_executed);
    assert!(res.stdout.contains("Status: VERIFIED_OK"));
    assert!(res.device_name.is_some());
}

// ============================================================================
// Scenario 6: Physical WGPU Execution When Hardware Adapter Available
// ============================================================================
#[test]
fn test_physical_wgpu_execution_when_available() {
    #[cfg(feature = "gpu-wgpu")]
    {
        if let Some(adapter_info) = rusty_grid_worker::wgpu_engine::probe_physical_adapter() {
            println!(
                "Discovered physical GPU adapter: '{}' (backend: {:?}, type: {:?})",
                adapter_info.name, adapter_info.backend, adapter_info.device_type
            );

            let engine = rusty_grid_worker::wgpu_engine::WgpuEngine::try_init()
                .expect("WgpuEngine::try_init returned error")
                .expect("Physical adapter probed but initialization returned None");

            let dim = 32;
            let input_data = b"hardware_wgpu_test_tensor";
            let task_id = Uuid::new_v4();
            let seed = cpu_simd::derive_seed(input_data, task_id);
            let (a, b) = cpu_simd::generate_deterministic_matrices(dim, seed);
            let mut c_expected = vec![0.0f32; dim * dim];
            cpu_simd::matmul_simd(&a, &b, &mut c_expected, dim);

            let outcome = engine
                .execute_gemm(
                    "wgsl_gemm_hardware_test",
                    input_data,
                    task_id,
                    dim,
                    16,
                    Instant::now(),
                )
                .expect("execute_gemm failed on physical GPU");

            println!("Physical GPU execution elapsed: {} ms", outcome.elapsed_ms);
            assert!(!outcome.device_name.is_empty());
            assert_eq!(outcome.device_name, adapter_info.name);
            assert!(outcome.stdout.contains("[GPU COMPUTE WGPU]"));
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
        } else {
            println!("No physical GPU adapter available on host; hardware execution gracefully skipped.");
        }
    }

    #[cfg(not(feature = "gpu-wgpu"))]
    {
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
        if let Some(_) = rusty_grid_worker::wgpu_engine::probe_physical_adapter() {
            let engine = Arc::new(
                rusty_grid_worker::wgpu_engine::WgpuEngine::try_init()
                    .expect("try_init failed")
                    .expect("No physical engine"),
            );

            // 1. Direct concurrent execute_gemm invocations across 12 threads
            const NUM_TASKS: usize = 12;
            let mut handles = Vec::with_capacity(NUM_TASKS);

            for i in 0..NUM_TASKS {
                let eng = Arc::clone(&engine);
                handles.push(tokio::spawn(async move {
                    let task_id = Uuid::new_v4();
                    let payload = format!("concurrent_tensor_batch_{i}").into_bytes();
                    let dim = 32;
                    let outcome = eng
                        .execute_gemm(
                            &format!("concurrent_gemm_{i}"),
                            &payload,
                            task_id,
                            dim,
                            16,
                            Instant::now(),
                        )
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

    // 2. High-concurrency execution via TaskRunner (works for physical or simulated)
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
        assert_eq!(res.exit_code, 0, "Task {idx} must succeed with exit code 0");
        assert!(res.is_gpu_executed, "Task {idx} must be GPU executed");
        assert!(res.device_name.is_some(), "Task {idx} must report device name");
        assert!(
            res.stdout.contains("Status: VERIFIED_OK"),
            "Task {idx} must contain VERIFIED_OK in stdout"
        );
    }
}

// ============================================================================
// Scenario 9: Memory Bounds, Clamping, and Irregular Matrix Dimensions
// ============================================================================
#[test]
fn test_adversarial_gpu_memory_bounds_and_matrix_dimensions() {
    #[cfg(feature = "gpu-wgpu")]
    {
        if let Some(_) = rusty_grid_worker::wgpu_engine::probe_physical_adapter() {
            let engine = rusty_grid_worker::wgpu_engine::WgpuEngine::try_init()
                .expect("try_init failed")
                .expect("physical adapter unavailable");

            // Test irregular non-power-of-two dimensions and workgroup edge conditions
            let test_dims = [16, 17, 33, 48, 65, 128, 256];

            for &dim in &test_dims {
                let task_id = Uuid::new_v4();
                let input = format!("dim_test_{dim}").into_bytes();
                let seed = cpu_simd::derive_seed(&input, task_id);
                let (a, b) = cpu_simd::generate_deterministic_matrices(dim, seed);
                let mut c_expected = vec![0.0f32; dim * dim];
                cpu_simd::matmul_simd(&a, &b, &mut c_expected, dim);

                let outcome = engine
                    .execute_gemm("irregular_dim_kernel", &input, task_id, dim, 16, Instant::now())
                    .unwrap_or_else(|e| panic!("execute_gemm failed for dim {dim}x{dim}: {e}"));

                assert_eq!(outcome.matrix_c.len(), dim * dim);
                assert!(outcome.stdout.contains("Status: VERIFIED_OK"));

                // Verify numerical convergence for every dimension
                for i in 0..dim * dim {
                    let diff = (outcome.matrix_c[i] - c_expected[i]).abs();
                    assert!(
                        diff < 1e-2,
                        "Mismatch for dim {dim} at index {i}: gpu={}, cpu={}, diff={}",
                        outcome.matrix_c[i],
                        c_expected[i],
                        diff
                    );
                }
            }
        }
    }

    // Test extreme boundary clamping at the TaskRunner level
    let (runner, _tmp) = make_runner(true, true);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        // Under-bounds: dim = 0, work_group_size = 0 (should clamp to 16 and 4)
        let under_task = Task::new(
            TaskSpec::GpuCompute {
                kernel_name: "under_bounds_kernel".into(),
                input_data: vec![],
                work_group_size: 0,
                simulated_matrix_dim: 0,
                compute_intensity: 1,
            },
            TaskRequirements::gpu(10),
        );
        let res_under = runner.execute_task(&under_task, None, None).await;
        assert_eq!(res_under.exit_code, 0);
        assert!(res_under.stdout.contains("Matrix Dimension: 16x16"));
        assert!(res_under.stdout.contains("Work Group Size: 4x4"));

        // Over-bounds: dim = 100_000, work_group_size = 1000 (should clamp to 512 and 32)
        let over_task = Task::new(
            TaskSpec::GpuCompute {
                kernel_name: "over_bounds_kernel".into(),
                input_data: vec![],
                work_group_size: 1000,
                simulated_matrix_dim: 100_000,
                compute_intensity: 1,
            },
            TaskRequirements::gpu(10),
        );
        let res_over = runner.execute_task(&over_task, None, None).await;
        assert_eq!(res_over.exit_code, 0);
        assert!(res_over.stdout.contains("Matrix Dimension: 512x512"));
        assert!(res_over.stdout.contains("Work Group Size: 32x32"));
    });
}

// ============================================================================
// Scenario 10: GPU Device Loss / Corruption & Graceful Fallback
// ============================================================================
#[tokio::test]
async fn test_adversarial_gpu_device_loss_recovery() {
    // 1. Validate that TaskRunner with physical GPU configuration (has_gpu = true, is_simulated = false)
    // cleanly falls back to CPU SIMD if physical GPU execution fails or is unavailable.
    let (runner, _tmp) = make_runner(true, false);

    let task = Task::new(
        TaskSpec::gpu_compute("device_loss_test_kernel", 32),
        TaskRequirements::gpu(15),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert_eq!(res.exit_code, 0);
    assert!(res.is_gpu_executed);
    assert!(res.stdout.contains("Status: VERIFIED_OK"));
    assert!(res.device_name.is_some());

    #[cfg(feature = "gpu-wgpu")]
    {
        if let Some(engine) = rusty_grid_worker::wgpu_engine::WgpuEngine::try_init().ok().flatten() {
            // Explicitly destroy the underlying wgpu Device to simulate physical device loss / driver reset
            engine.device.destroy();

            // Calling execute_gemm directly: does it panic or return Err?
            let panic_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                engine.execute_gemm(
                    "post_device_loss_kernel",
                    b"lost_device_tensor",
                    Uuid::new_v4(),
                    32,
                    16,
                    Instant::now(),
                )
            }));

            println!("Device loss empirical test: execute_gemm panicked = {}", panic_res.is_err());
            assert!(
                panic_res.is_err(),
                "Expected wgpu to panic on invalid device in create_buffer because uncaptured error handler is not set"
            );
        }
    }
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

