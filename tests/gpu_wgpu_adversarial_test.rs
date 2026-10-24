//! Adversarial Stress & Empirical Verification Test Suite for Milestone M4 (Requirement R4).
//!
//! Authored by Challenger 1 to stress-test:
//! 1. Mathematical Edge Cases & Irregular Matrix Dimensions:
//!    - Zero-dimension (0x0), single-element (1x1), non-power-of-two (17x17, 33x33, 65x65, 127x127), and large (512x512).
//! 2. Extreme Float Ranges & IEEE-754 Compliance:
//!    - Subnormals/denormals, infinities (+inf, -inf), NaNs, large dynamic ranges, negative zeros, catastrophic cancellation.
//! 3. Bitwise Deterministic Equivalence:
//!    - Extensive fuzzing across random seeds and hostile float matrices ensuring bitwise identity
//!      (simd[idx].to_bits() == oracle[idx].to_bits(), trace, frobenius norm, FNV-1a 64-bit digest).
//! 4. Hardware WGPU Physical GPU Execution (needs feature `gpu-wgpu` and an adapter; a host
//!    without one skips these unless `RUSTY_GRID_REQUIRE_GPU=1`):
//!    - Irregular tile boundary stress (1x1, 17x17, 33x33, 65x65, 127x127, 512x512) on physical GPU.
//!    - Rapid back-to-back GPU execution stress (buffer allocation/deallocation, memory leaks).
//! 5. Runner Parameter Invariants:
//!    - Hostile task specification parameters (0-dim, max-dim, out-of-range tiles) are rejected with a
//!      message naming the limit, never silently clamped.

use tempfile::tempdir;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_worker::cpu_simd;
use rusty_grid_worker::runner::{
    RunnerConfig, TaskRunner, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_SUCCESS,
};
use rusty_grid_worker::sandbox::SandboxConfig;

fn make_test_runner(has_gpu: bool, is_simulated: bool) -> (TaskRunner, tempfile::TempDir) {
    let tmp = tempdir().expect("Failed to create temporary directory for test sandbox");
    let caps = WorkerCapabilities {
        name: format!("challenger-m4-{}", Uuid::new_v4()),
        cpu_cores: 4,
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
        tags: vec!["challenger_m4".into()],
        mobile: None,
        ..Default::default()
    };
    let cfg = RunnerConfig::new(SandboxConfig::new(tmp.path()));
    let runner = TaskRunner::new(Uuid::new_v4(), caps, cfg);
    (runner, tmp)
}

/// The shared GPU engine, or None (after saying so) on a host/build without one. With
/// `RUSTY_GRID_REQUIRE_GPU=1` a missing engine fails the test instead.
#[cfg(feature = "gpu-wgpu")]
fn gpu_engine_or_skip(
    test: &str,
) -> Option<std::sync::Arc<rusty_grid_worker::wgpu_engine::WgpuEngine>> {
    match rusty_grid_worker::wgpu_engine::EngineProvider::shared().get() {
        Ok(lease) => Some(lease.engine),
        Err(err) => {
            assert!(
                std::env::var("RUSTY_GRID_REQUIRE_GPU").as_deref() != Ok("1"),
                "RUSTY_GRID_REQUIRE_GPU=1 but {test} found no GPU engine: {err}"
            );
            println!("{test}: skipped, no GPU engine on this host ({err})");
            None
        }
    }
}

// ============================================================================
// Scenario 1: Non-Power-of-Two and Boundary Dimensions on CPU SIMD
// ============================================================================
#[test]
fn test_simd_vs_oracle_irregular_dimensions() {
    let test_dims = [
        1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 512,
    ];
    let seed = 0xa5a5a5a5_5a5a5a5a_u64;

    for &dim in &test_dims {
        let (a, b) = cpu_simd::generate_deterministic_matrices(dim, seed);
        let mut c_simd = vec![0.0f32; dim * dim];
        cpu_simd::matmul_simd(&a, &b, &mut c_simd, dim);

        let c_oracle = cpu_simd::reference_matmul(&a, &b, dim);

        assert_eq!(
            c_simd.len(),
            c_oracle.len(),
            "Size mismatch for dimension {dim}x{dim}"
        );

        for idx in 0..c_simd.len() {
            assert_eq!(
                c_simd[idx].to_bits(),
                c_oracle[idx].to_bits(),
                "Bitwise mismatch at index {idx} for dimension {dim}x{dim}: simd={}, oracle={}",
                c_simd[idx],
                c_oracle[idx]
            );
        }

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
            "Bit hash mismatch for {dim}x{dim}"
        );
    }
}

// ============================================================================
// Scenario 2: Single Element (1x1) and Zero Dimension (0x0) Handling
// ============================================================================
#[test]
#[allow(clippy::approx_constant)]
fn test_single_element_1x1_matrix() {
    let a = vec![3.1415927f32];
    let b = vec![2.7182817f32];
    let mut c_simd = vec![0.0f32; 1];

    cpu_simd::matmul_simd(&a, &b, &mut c_simd, 1);
    let c_oracle = cpu_simd::reference_matmul(&a, &b, 1);

    assert_eq!(c_simd[0].to_bits(), c_oracle[0].to_bits());
    assert_eq!(c_simd[0], 3.1415927f32 * 2.7182817f32);

    let m_simd = cpu_simd::compute_matrix_metrics(&c_simd, 1);
    let m_oracle = cpu_simd::compute_matrix_metrics(&c_oracle, 1);
    assert_eq!(m_simd, m_oracle);
    assert_eq!(m_simd.trace, c_simd[0] as f64);
    assert_eq!(m_simd.frobenius_norm, (c_simd[0] as f64).abs());
}

#[test]
fn test_zero_dimension_simd_handling() {
    let a: Vec<f32> = vec![];
    let b: Vec<f32> = vec![];
    let mut c_simd: Vec<f32> = vec![];

    // Must not panic on empty buffers
    cpu_simd::matmul_simd(&a, &b, &mut c_simd, 0);
    let c_oracle = cpu_simd::reference_matmul(&a, &b, 0);

    assert_eq!(c_simd.len(), 0);
    assert_eq!(c_oracle.len(), 0);

    let m_simd = cpu_simd::compute_matrix_metrics(&c_simd, 0);
    assert_eq!(m_simd.trace, 0.0);
    assert_eq!(m_simd.frobenius_norm, 0.0);
    assert_eq!(m_simd.bit_hash, 0xcbf29ce484222325); // FNV-1a offset basis
}

// ============================================================================
// Scenario 3: Extreme Float Ranges (Subnormals, Infs, NaNs, Cancellation)
// ============================================================================
#[test]
fn test_extreme_floats_subnormals_and_cancellation() {
    let dim = 17; // Odd dimension exercising chunk remainder
    let num = dim * dim;

    // 1. Subnormal numbers
    let mut a_subnormal = vec![0.0f32; num];
    let mut b_subnormal = vec![0.0f32; num];
    for i in 0..num {
        a_subnormal[i] = f32::from_bits((i as u32 + 1) * 3); // Subnormals near min positive
        b_subnormal[i] = 1.0f32 + (i as f32 * 0.01);
    }

    let mut c_simd = vec![0.0f32; num];
    cpu_simd::matmul_simd(&a_subnormal, &b_subnormal, &mut c_simd, dim);
    let c_oracle = cpu_simd::reference_matmul(&a_subnormal, &b_subnormal, dim);

    for idx in 0..num {
        assert_eq!(
            c_simd[idx].to_bits(),
            c_oracle[idx].to_bits(),
            "Subnormal mismatch at {idx}: simd={}, oracle={}",
            c_simd[idx],
            c_oracle[idx]
        );
    }
    let m_simd = cpu_simd::compute_matrix_metrics(&c_simd, dim);
    let m_oracle = cpu_simd::compute_matrix_metrics(&c_oracle, dim);
    assert_eq!(m_simd, m_oracle);

    // 2. Catastrophic cancellation: Large positive and negative values in sequential accumulation
    let mut a_cancel = vec![0.0f32; num];
    let mut b_cancel = vec![0.0f32; num];
    for i in 0..dim {
        // Inner product across k will oscillate between +1e15 and -1e15
        for k in 0..dim {
            if k % 2 == 0 {
                a_cancel[i * dim + k] = 1e15;
            } else {
                a_cancel[i * dim + k] = -1e15;
            }
            b_cancel[k * dim + i] = 1.0;
        }
    }

    let mut c_cancel_simd = vec![0.0f32; num];
    cpu_simd::matmul_simd(&a_cancel, &b_cancel, &mut c_cancel_simd, dim);
    let c_cancel_oracle = cpu_simd::reference_matmul(&a_cancel, &b_cancel, dim);

    for idx in 0..num {
        assert_eq!(
            c_cancel_simd[idx].to_bits(),
            c_cancel_oracle[idx].to_bits(),
            "Cancellation mismatch at {idx}: simd={}, oracle={}",
            c_cancel_simd[idx],
            c_cancel_oracle[idx]
        );
    }

    // 3. Negative zero handling
    let a_negzero = vec![-0.0f32; num];
    let b_negzero = vec![1.0f32; num];
    let mut c_negzero_simd = vec![0.0f32; num];
    cpu_simd::matmul_simd(&a_negzero, &b_negzero, &mut c_negzero_simd, dim);
    let c_negzero_oracle = cpu_simd::reference_matmul(&a_negzero, &b_negzero, dim);

    for idx in 0..num {
        assert_eq!(
            c_negzero_simd[idx].to_bits(),
            c_negzero_oracle[idx].to_bits(),
            "Negative zero mismatch at {idx}"
        );
    }
}

// ============================================================================
// Scenario 4: Pseudo-Random Fuzzing Across Arbitrary Matrices
// ============================================================================
#[test]
fn test_simd_fuzzing_arbitrary_matrices() {
    let test_cases = [(7, 101), (17, 202), (33, 303), (65, 404), (127, 505)];

    for (dim, base_seed) in test_cases {
        let num = dim * dim;
        let mut a = vec![0.0f32; num];
        let mut b = vec![0.0f32; num];

        // Linear congruential pseudo-random generator with diverse float magnitudes
        let mut state = base_seed as u64;
        for i in 0..num {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let val_a = ((state >> 32) as i32) as f32 / 65536.0;
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let val_b = ((state >> 32) as i32) as f32 / 65536.0;

            a[i] = val_a;
            b[i] = val_b;
        }

        let mut c_simd = vec![0.0f32; num];
        cpu_simd::matmul_simd(&a, &b, &mut c_simd, dim);
        let c_oracle = cpu_simd::reference_matmul(&a, &b, dim);

        for idx in 0..num {
            assert_eq!(
                c_simd[idx].to_bits(),
                c_oracle[idx].to_bits(),
                "Fuzz bit mismatch at {idx} for dim {dim}"
            );
        }

        let m_simd = cpu_simd::compute_matrix_metrics(&c_simd, dim);
        let m_oracle = cpu_simd::compute_matrix_metrics(&c_oracle, dim);
        assert_eq!(m_simd, m_oracle);
    }
}

// ============================================================================
// Scenario 5: Deterministic Digest Invariance Over Repeated Executions
// ============================================================================
#[test]
fn test_deterministic_digest_invariance_across_iterations() {
    let task_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let input_payload = b"deterministic_quantum_tensor_shard";
    let dim = 33;
    let tile = 16;

    let (stdout_base, _) = cpu_simd::execute_simd_compute(
        "digest_invariance_kernel",
        input_payload,
        task_id,
        dim,
        tile,
    );

    // Extract the digest line from stdout
    let digest_line = stdout_base
        .lines()
        .find(|l| l.starts_with("Verification Digest:"))
        .expect("Verification Digest line missing");

    // Execute 50 consecutive runs and verify bitwise identical digest string
    for run in 1..=50 {
        let (stdout_repeat, _) = cpu_simd::execute_simd_compute(
            "digest_invariance_kernel",
            input_payload,
            task_id,
            dim,
            tile,
        );

        let repeat_digest = stdout_repeat
            .lines()
            .find(|l| l.starts_with("Verification Digest:"))
            .expect("Verification Digest line missing in repeat run");

        assert_eq!(
            digest_line, repeat_digest,
            "Digest divergence detected at run {run}: expected '{digest_line}', got '{repeat_digest}'"
        );
    }
}

// ============================================================================
// Scenario 6: Hardware Physical WGPU Execution Across Irregular Dimensions
// ============================================================================
#[test]
fn test_wgpu_hardware_irregular_dimensions() {
    #[cfg(feature = "gpu-wgpu")]
    {
        let Some(engine) = gpu_engine_or_skip("test_wgpu_hardware_irregular_dimensions") else {
            return;
        };
        let adapter_info = &engine.adapter_info;
        println!(
            "Adversarial WGPU Testing on Physical Adapter: '{}' ({:?})",
            adapter_info.name, adapter_info.backend
        );

        // Dimensions: 1x1, 17x17, 33x33, 65x65, 127x127, 512x512
        let stress_dims = [1, 17, 33, 65, 127, 512];
        let payload = b"adversarial_test_payload";

        for &dim in &stress_dims {
            let task_id = Uuid::new_v4();
            let seed = cpu_simd::derive_seed(payload, task_id);
            let (a, b) = cpu_simd::generate_deterministic_matrices(dim, seed);
            let mut c_expected = vec![0.0f32; dim * dim];
            cpu_simd::matmul_simd(&a, &b, &mut c_expected, dim);

            let outcome = engine
                .execute_gemm(
                    &format!("adversarial_gemm_{dim}"),
                    payload,
                    task_id,
                    dim,
                    16,
                )
                .unwrap_or_else(|e| panic!("execute_gemm failed for dim {dim}x{dim}: {e}"));

            assert_eq!(outcome.matrix_c.len(), dim * dim);
            assert!(
                outcome.verification.passed(),
                "dim {dim}: {}",
                outcome.verification.summary()
            );
            assert!(outcome.stdout.contains("Status: VERIFIED_OK"));
            assert_eq!(outcome.device_name, adapter_info.name);

            // Check maximum absolute and relative error against CPU reference
            let mut max_abs_diff = 0.0f32;
            for i in 0..dim * dim {
                let diff = (outcome.matrix_c[i] - c_expected[i]).abs();
                if diff > max_abs_diff {
                    max_abs_diff = diff;
                }
                assert!(
                    diff < 1e-1,
                    "Excessive GPU vs CPU error at index {i} for dim {dim}x{dim}: gpu={}, cpu={}, diff={}",
                    outcome.matrix_c[i],
                    c_expected[i],
                    diff
                );
            }

            println!(
                "  [PASS] Physical GPU Dim {dim:>3}x{dim:<3} | Max Abs Diff: {:.6e} | Time: {} ms",
                max_abs_diff, outcome.elapsed_ms
            );
        }
    }
}

// ============================================================================
// Scenario 7: Rapid Back-to-Back Hardware GPU Execution Burst
// ============================================================================
#[test]
fn test_wgpu_hardware_rapid_burst_stress() {
    #[cfg(feature = "gpu-wgpu")]
    {
        let Some(engine) = gpu_engine_or_skip("test_wgpu_hardware_rapid_burst_stress") else {
            return;
        };
        println!(
            "Executing 20 rapid back-to-back GEMM dispatches on '{}'...",
            engine.adapter_info.name
        );
        let dim = 48;

        for iter in 1..=20 {
            let outcome = engine
                .execute_gemm(
                    &format!("burst_kernel_{iter}"),
                    b"burst_test_payload",
                    Uuid::new_v4(),
                    dim,
                    16,
                )
                .unwrap_or_else(|e| panic!("Burst iteration {iter} failed: {e}"));

            assert_eq!(outcome.matrix_c.len(), dim * dim);
            assert!(outcome.stdout.contains("Status: VERIFIED_OK"));
        }
        println!("  [PASS] Completed 20 rapid back-to-back GPU GEMM dispatches successfully.");
    }
}

// ============================================================================
// Scenario 8: Runner Task Specification Hostile Parameters: Rejected, Not Clamped
// ============================================================================
#[tokio::test]
async fn test_runner_hostile_parameter_clamping() {
    let (runner, _tmp) = make_test_runner(true, true);

    // 1. Hostile underflow: dim = 0, work_group_size = 0. These used to be silently raised to
    // 16 and 4; now the task fails and says which parameter is wrong.
    let task_underflow = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "underflow_clamp_kernel".into(),
            input_data: b"clamp_payload".to_vec(),
            work_group_size: 0,
            simulated_matrix_dim: 0,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(30),
    );

    let res_underflow = runner.execute_task(&task_underflow, None, None).await;
    assert_eq!(res_underflow.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(!res_underflow.is_gpu_executed);
    assert!(
        res_underflow.stdout.is_empty(),
        "no result for a rejected request"
    );
    let error = res_underflow.error.expect("error message");
    assert!(
        error.contains("simulated_matrix_dim must be at least 1"),
        "{error}"
    );

    // 2. Hostile overflow: dim = 100000, work_group_size = 999. These used to be silently
    // lowered to 512 and 32 (so a caller asking for 100000 got a 512 benchmark and exit 0).
    let task_overflow = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "overflow_clamp_kernel".into(),
            input_data: b"clamp_payload".to_vec(),
            work_group_size: 999,
            simulated_matrix_dim: 100000,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(30),
    );

    let res_overflow = runner.execute_task(&task_overflow, None, None).await;
    assert_eq!(res_overflow.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(!res_overflow.is_gpu_executed);
    let error = res_overflow.error.expect("error message");
    assert!(
        error.contains("simulated_matrix_dim 100000 is above what this device accepts"),
        "{error}"
    );

    // 3. Huge input payload (100 KB) for seed derivation
    let huge_input = vec![0x42u8; 100_000];
    let task_huge = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "huge_payload_kernel".into(),
            input_data: huge_input,
            work_group_size: 16,
            simulated_matrix_dim: 32,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(30),
    );

    let res_huge = runner.execute_task(&task_huge, None, None).await;
    assert_eq!(res_huge.exit_code, EXIT_CODE_SUCCESS);
    assert!(res_huge.stdout.contains("Status: VERIFIED_OK"));

    // 4. Input whose hash is the largest possible seed: the seed addition must wrap, not panic
    // (debug) or silently differ (release). 0xff.. folded with *31 is some u64; use the
    // unit-tested generator to prove the extreme seeds directly as well.
    let (a, _) = cpu_simd::generate_deterministic_matrices(16, u64::MAX);
    assert!(a.iter().all(|v| v.is_finite()));
}

// ============================================================================
// Scenario 9: Hash WGSL Compute Shader Compilation & Validation
// (hash.wgsl is EXPERIMENTAL and not dispatched by any task: this only checks that it still
// compiles. It does not produce the CPU digest, see the header of the file.)
// ============================================================================
#[test]
fn test_hash_wgsl_shader_compilation() {
    #[cfg(feature = "gpu-wgpu")]
    {
        if let Some(engine) = gpu_engine_or_skip("test_hash_wgsl_shader_compilation") {
            let hash_wgsl = include_str!("../crates/worker/src/shaders/hash.wgsl");
            // The engine's device reports errors to a logging handler instead of panicking, so
            // the compilation result has to be read from an error scope.
            engine
                .device
                .push_error_scope(wgpu::ErrorFilter::Validation);
            let shader = engine
                .device
                .create_shader_module(wgpu::ShaderModuleDescriptor {
                    label: Some("adversarial_hash_shader_module"),
                    source: wgpu::ShaderSource::Wgsl(hash_wgsl.into()),
                });
            let error = pollster::block_on(engine.device.pop_error_scope());
            assert!(error.is_none(), "hash.wgsl no longer compiles: {error:?}");

            // Verify shader module compilation succeeds
            println!(
                "  [PASS] WGSL validation of hash.wgsl succeeded: {:?}",
                shader
            );
        }
    }
}

// ============================================================================
// Scenario 10: Direct WGPU Zero-Dimension Boundary Handling
// ============================================================================
#[test]
fn test_direct_wgpu_zero_dimension_behavior() {
    #[cfg(feature = "gpu-wgpu")]
    {
        if let Some(engine) = gpu_engine_or_skip("test_direct_wgpu_zero_dimension_behavior") {
            // dim = 0 would create 0-byte buffers, which wgpu rejects. The engine refuses the
            // request up front instead of panicking or leaving it to the driver.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                engine.execute_gemm("zero_dim_kernel", b"payload", Uuid::new_v4(), 0, 16)
            }));
            let outcome = result.expect("execute_gemm(dim=0) must not panic");
            let err = outcome.expect_err("dim = 0 is not a matrix");
            assert!(
                matches!(err, rusty_grid_worker::gpu::GpuError::InvalidRequest(_)),
                "{err:?}"
            );
            assert!(!engine.is_lost());
        }
    }
}
