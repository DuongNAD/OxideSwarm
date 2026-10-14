//! Deterministic CPU SIMD Engine (Requirement R4).
//!
//! Provides high-performance, 8-wide chunked unrolled general matrix multiplication (GEMM)
//! with cache-friendly loop permutation and sequential accumulation order matching canonical
//! floating-point arithmetic. Computes deterministic invariant checksums (Trace, Frobenius Norm,
//! and 64-bit FNV-1a verification digest) for grid verification.
//!
//! This is the engine behind the *simulated* GPU (`--simulate-gpu`, a labelled test mode) and the
//! CPU oracle the real GPU result is checked against ([`verify_gemm`]). It is never used to
//! stand in for a real GPU: a worker that advertises a real GPU and cannot drive it fails the
//! task instead (see `runner::TaskRunner`).

use std::time::Instant;
use uuid::Uuid;

use crate::gpu::GpuError;

/// Mathematical invariants computed over a row-major matrix buffer.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixMetrics {
    /// Sum of diagonal entries: \sum_{i=0}^{\min(M, N)-1} C[i, i]
    pub trace: f64,
    /// Square root of sum of squared entries: \sqrt{\sum_{i, j} C[i, j]^2}
    pub frobenius_norm: f64,
    /// 64-bit FNV-1a verification digest over IEEE-754 bit representations
    pub bit_hash: u64,
}

/// Derives a deterministic 64-bit seed from task input data or task UUID.
#[inline]
pub fn derive_seed(input_data: &[u8], task_id: Uuid) -> u64 {
    if !input_data.is_empty() {
        input_data
            .iter()
            .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64))
    } else {
        let bytes: [u8; 8] = task_id.as_bytes()[0..8].try_into().unwrap_or([0u8; 8]);
        u64::from_le_bytes(bytes)
    }
}

/// Generates deterministic single-precision floating point matrices A and B
/// using linear congruential arithmetic parameterized by seed.
///
/// `seed` is any `u64` (a hash of caller-controlled input), so it is added with wrapping
/// arithmetic: a plain `+` would panic in a debug build where a release build silently wraps, and
/// the two workers would then disagree about the matrix.
pub fn generate_deterministic_matrices(dim: usize, seed: u64) -> (Vec<f32>, Vec<f32>) {
    let mut a = vec![0.0f32; dim * dim];
    let mut b = vec![0.0f32; dim * dim];

    for i in 0..dim {
        for j in 0..dim {
            let idx = i * dim + j;
            a[idx] = ((seed.wrapping_add(i as u64 * 37 + j as u64 * 17) % 1000) as f32) / 100.0;
            b[idx] = ((seed.wrapping_add(i as u64 * 19 + j as u64 * 43) % 1000) as f32) / 100.0;
        }
    }
    (a, b)
}

/// High-performance CPU matrix multiplication (C = A x B) utilizing
/// cache-friendly (i, k, j) loop traversal and 8-wide chunk unrolling.
///
/// Accumulates inner product terms in strictly sequential order across k
/// to guarantee bitwise mathematical equivalence with canonical row-by-column
/// evaluation.
pub fn matmul_simd(a: &[f32], b: &[f32], c: &mut [f32], dim: usize) {
    let _ = matmul_simd_cancellable(a, b, c, dim, &|| false);
}

/// [`matmul_simd`] that asks `should_stop` before every output row and gives up (returning
/// `false`, `c` partially filled) as soon as it says so. A long CPU multiplication running on a
/// blocking thread can only be cancelled or timed out cooperatively; this is the hook.
pub fn matmul_simd_cancellable(
    a: &[f32],
    b: &[f32],
    c: &mut [f32],
    dim: usize,
    should_stop: &dyn Fn() -> bool,
) -> bool {
    assert_eq!(a.len(), dim * dim, "Matrix A size mismatch");
    assert_eq!(b.len(), dim * dim, "Matrix B size mismatch");
    assert_eq!(c.len(), dim * dim, "Matrix C size mismatch");

    c.fill(0.0);

    for i in 0..dim {
        if should_stop() {
            return false;
        }
        let c_row = i * dim;
        for k in 0..dim {
            let a_val = a[i * dim + k];
            let b_row = k * dim;

            let mut j = 0;
            // 8-wide chunk unrolling (256-bit AVX2 / dual NEON register size)
            while j + 8 <= dim {
                let ci = c_row + j;
                let bi = b_row + j;

                c[ci + 0] += a_val * b[bi + 0];
                c[ci + 1] += a_val * b[bi + 1];
                c[ci + 2] += a_val * b[bi + 2];
                c[ci + 3] += a_val * b[bi + 3];
                c[ci + 4] += a_val * b[bi + 4];
                c[ci + 5] += a_val * b[bi + 5];
                c[ci + 6] += a_val * b[bi + 6];
                c[ci + 7] += a_val * b[bi + 7];

                j += 8;
            }

            // Remainder handling for non-multiples of 8
            while j < dim {
                c[c_row + j] += a_val * b[b_row + j];
                j += 1;
            }
        }
    }
    true
}

/// Independent naive triple-loop matrix multiplication reference oracle.
/// Used to verify bitwise accuracy of optimized implementations.
pub fn reference_matmul(a: &[f32], b: &[f32], dim: usize) -> Vec<f32> {
    let mut c = vec![0.0f32; dim * dim];
    for i in 0..dim {
        for j in 0..dim {
            let mut sum = 0.0f32;
            for k in 0..dim {
                sum += a[i * dim + k] * b[k * dim + j];
            }
            c[i * dim + j] = sum;
        }
    }
    c
}

/// Computes invariant checksums (Trace, Frobenius Norm, 64-bit FNV-1a digest).
pub fn compute_matrix_metrics(c: &[f32], dim: usize) -> MatrixMetrics {
    let mut trace = 0.0f64;
    let mut f_norm_sq = 0.0f64;
    let mut bit_hash: u64 = 0xcbf29ce484222325;

    for i in 0..dim {
        trace += c[i * dim + i] as f64;
    }

    for &val in c {
        let v = val as f64;
        f_norm_sq += v * v;
        bit_hash ^= val.to_bits() as u64;
        bit_hash = bit_hash.wrapping_mul(0x100000001b3);
    }

    MatrixMetrics {
        trace,
        frobenius_norm: f_norm_sq.sqrt(),
        bit_hash,
    }
}

/// Matrices up to this many entries are checked entry by entry; bigger ones on this many
/// deterministically scattered entries (so the check costs `O(entries * dim)`, not `O(dim^3)`).
const VERIFY_SAMPLE_ENTRIES: usize = 4096;

/// What [`verify_gemm`] found when it recomputed entries of `C = A x B` with an independent
/// `f64` dot product.
#[derive(Debug, Clone, PartialEq)]
pub struct Verification {
    /// Entries recomputed and compared.
    pub checked_entries: usize,
    /// Entries in the whole matrix.
    pub total_entries: usize,
    /// Non-finite (NaN / infinite) entries anywhere in `C`; scanned over the whole matrix.
    pub non_finite_entries: usize,
    /// Largest `|C - exact| / sum(|a*b|)` over the checked entries.
    pub max_relative_error: f64,
    /// Relative tolerance every checked entry had to stay within: `dim * f32::EPSILON`, the
    /// classical worst-case bound for an `f32` dot product accumulated in any order (and with or
    /// without fused multiply-add), so a correct GPU result never trips it.
    pub tolerance: f64,
    /// First entry outside the tolerance: `(row, column, got, exact)`.
    pub first_mismatch: Option<(usize, usize, f32, f64)>,
}

impl Verification {
    /// True only if something was actually compared and nothing was wrong.
    pub fn passed(&self) -> bool {
        self.checked_entries > 0 && self.non_finite_entries == 0 && self.first_mismatch.is_none()
    }

    /// The value of the `Status:` line.
    pub fn status(&self) -> &'static str {
        if self.passed() {
            "VERIFIED_OK"
        } else {
            "VERIFICATION_FAILED"
        }
    }

    /// One line for the `Verification:` report line (and for error messages).
    pub fn summary(&self) -> String {
        let mut text = format!(
            "{} of {} entries recomputed with an f64 CPU dot product, max relative error {:.3e} (tolerance {:.3e}), {} non-finite",
            self.checked_entries,
            self.total_entries,
            self.max_relative_error,
            self.tolerance,
            self.non_finite_entries,
        );
        if let Some((row, col, got, exact)) = self.first_mismatch {
            text.push_str(&format!(
                "; first mismatch at ({row}, {col}): got {got}, expected {exact}"
            ));
        }
        text
    }
}

/// Entries of a `dim x dim` matrix to recompute: all of them when there are few, otherwise the
/// four corners (where tile-edge bugs live) plus pseudo-random positions from a fixed generator,
/// so the same matrix is always checked at the same places.
fn verification_positions(dim: usize) -> Vec<(usize, usize)> {
    if dim == 0 {
        return Vec::new();
    }
    if dim * dim <= VERIFY_SAMPLE_ENTRIES {
        return (0..dim)
            .flat_map(|i| (0..dim).map(move |j| (i, j)))
            .collect();
    }
    let last = dim - 1;
    let mut positions = vec![(0, 0), (0, last), (last, 0), (last, last)];
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    while positions.len() < VERIFY_SAMPLE_ENTRIES {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let i = ((state >> 33) as usize) % dim;
        let j = ((state >> 8) as usize) % dim;
        positions.push((i, j));
    }
    positions
}

/// Checks a `dim x dim` product `c` of `a x b` (row-major `f32`) against an independent `f64`
/// recomputation of [`verification_positions`], and scans the whole of `c` for NaN / infinities.
///
/// This is what lets a result say `VERIFIED_OK`: an all-zero, NaN-filled, half-written or
/// mis-indexed output fails it. The comparison is tolerance based on purpose: a GPU may fuse
/// multiply-adds or reorder the sum, so its bits legitimately differ from the CPU's.
pub fn verify_gemm(a: &[f32], b: &[f32], c: &[f32], dim: usize) -> Verification {
    let total_entries = dim * dim;
    let tolerance = dim as f64 * f32::EPSILON as f64;
    let mut verification = Verification {
        checked_entries: 0,
        total_entries,
        non_finite_entries: c.iter().filter(|v| !v.is_finite()).count(),
        max_relative_error: 0.0,
        tolerance,
        first_mismatch: None,
    };
    if a.len() != total_entries || b.len() != total_entries || c.len() != total_entries {
        // Nothing can be compared; `passed()` is false because `checked_entries` stays 0.
        return verification;
    }

    for (i, j) in verification_positions(dim) {
        let mut exact = 0.0f64;
        let mut magnitude = 0.0f64;
        for k in 0..dim {
            // An f32 x f32 product is exact in f64, so this is the true dot product up to f64
            // accumulation noise, which is far below the tolerance.
            let product = a[i * dim + k] as f64 * b[k * dim + j] as f64;
            exact += product;
            magnitude += product.abs();
        }
        let got = c[i * dim + j];
        let error = (got as f64 - exact).abs();
        let relative = if magnitude > 0.0 {
            error / magnitude
        } else if error == 0.0 {
            0.0
        } else {
            f64::INFINITY
        };
        verification.checked_entries += 1;
        if relative > verification.max_relative_error {
            verification.max_relative_error = relative;
        }
        // Not `error > tolerance * magnitude`: a NaN entry must count as a mismatch too.
        let within_tolerance = error <= tolerance * magnitude;
        if !within_tolerance && verification.first_mismatch.is_none() {
            verification.first_mismatch = Some((i, j, got, exact));
        }
    }
    verification
}

/// Everything that goes into the human-readable result of a GPU-class task.
pub struct ComputeReport<'a> {
    /// First line, e.g. `[GPU COMPUTE WGPU]` or `[GPU COMPUTE SIMULATOR]`.
    pub banner: &'a str,
    pub device: &'a str,
    pub backend: &'a str,
    pub kernel_name: &'a str,
    pub dim: usize,
    pub tile: usize,
    /// Appended to the work group line (e.g. to say the CPU engine does not tile).
    pub work_group_note: &'a str,
    /// Time spent computing (upload, multiply and read-back for a GPU), not including matrix
    /// generation, engine start-up or verification.
    pub execution_ms: u64,
    /// Extra `Name: value` lines placed after `Execution Time` (e.g. engine start-up time).
    pub extra_lines: &'a [String],
    pub metrics: &'a MatrixMetrics,
    /// Who the digest is comparable with.
    pub digest_scope: &'a str,
    pub verification: &'a Verification,
}

/// Renders a [`ComputeReport`] in the line format the CLI, the dashboard and the agent bridge read.
pub fn format_report(report: &ComputeReport<'_>) -> String {
    let flops = 2u64 * (report.dim as u64) * (report.dim as u64) * (report.dim as u64);
    let mut out = format!(
        "{}\nDevice: {}\nBackend: {}\nKernel: {}\nMatrix Dimension: {dim}x{dim} (FLOPs: {flops})\nWork Group Size: {tile}x{tile}{note}\nExecution Time: {ms} ms\n",
        report.banner,
        report.device,
        report.backend,
        report.kernel_name,
        dim = report.dim,
        tile = report.tile,
        note = report.work_group_note,
        ms = report.execution_ms,
    );
    for line in report.extra_lines {
        out.push_str(line);
        out.push('\n');
    }
    out.push_str(&format!(
        "Matrix Trace: {:.4}\nFrobenius Norm: {:.4}\nVerification Digest: 0x{:016x}\nDigest Scope: {}\nVerification: {}\nStatus: {}\n",
        report.metrics.trace,
        report.metrics.frobenius_norm,
        report.metrics.bit_hash,
        report.digest_scope,
        report.verification.summary(),
        report.verification.status(),
    ));
    out
}

/// The result of a completed [`run_simd_compute`].
#[derive(Debug, Clone)]
pub struct SimdOutcome {
    pub stdout: String,
    /// Time spent in the multiplication, in milliseconds (at least 1).
    pub elapsed_ms: u64,
    pub metrics: MatrixMetrics,
    pub verification: Verification,
}

/// Parameters of one CPU SIMD run.
pub struct SimdRequest<'a> {
    pub kernel_name: &'a str,
    pub input_data: &'a [u8],
    pub task_id: Uuid,
    pub dim: usize,
    pub tile: usize,
    pub device_name: &'a str,
    pub banner: &'a str,
}

/// Runs the CPU multiplication for `request`, verifies it ([`verify_gemm`]) and formats the
/// report. `should_stop` is asked before each output row; when it says so the run ends with
/// [`GpuError::Interrupted`]. Blocking: call it from `spawn_blocking` (the runner does).
///
/// The `Status:` line is `VERIFIED_OK` only when the verification passed; a failed one is
/// returned as `Ok` with `VERIFICATION_FAILED` in the report so the caller decides what to do.
pub fn run_simd_compute(
    request: &SimdRequest<'_>,
    should_stop: &dyn Fn() -> bool,
) -> Result<SimdOutcome, GpuError> {
    let dim = request.dim;
    let seed = derive_seed(request.input_data, request.task_id);
    let (a, b) = generate_deterministic_matrices(dim, seed);
    let mut c = vec![0.0f32; dim * dim];

    let started = Instant::now();
    if !matmul_simd_cancellable(&a, &b, &mut c, dim, should_stop) {
        return Err(GpuError::Interrupted("the CPU multiplication was stopped"));
    }
    let elapsed_ms = std::cmp::max(1, started.elapsed().as_millis() as u64);

    let metrics = compute_matrix_metrics(&c, dim);
    let verification = verify_gemm(&a, &b, &c, dim);
    let stdout = format_report(&ComputeReport {
        banner: request.banner,
        device: request.device_name,
        backend: "CPU_SIMD_FALLBACK",
        kernel_name: request.kernel_name,
        dim,
        tile: request.tile,
        work_group_note: " (nominal: the CPU engine does not tile)",
        execution_ms: elapsed_ms,
        extra_lines: &[],
        metrics: &metrics,
        digest_scope: "bit-exact across CPU SIMD workers; a GPU's digest differs (fused multiply-add, summation order)",
        verification: &verification,
    });

    Ok(SimdOutcome {
        stdout,
        elapsed_ms,
        metrics,
        verification,
    })
}

/// Executes deterministic CPU SIMD compute for the simulated GPU with standardized output.
pub fn execute_simd_compute(
    kernel_name: &str,
    input_data: &[u8],
    task_id: Uuid,
    dim: usize,
    tile: usize,
) -> (String, u64) {
    execute_simd_compute_with_device(
        kernel_name,
        input_data,
        task_id,
        dim,
        tile,
        "Simulated Virtual GPU (Matrix Engine)",
        "[GPU COMPUTE SIMULATOR]",
    )
}

/// Executes deterministic CPU SIMD compute with explicit device name and banner (uninterruptible
/// convenience over [`run_simd_compute`]).
pub fn execute_simd_compute_with_device(
    kernel_name: &str,
    input_data: &[u8],
    task_id: Uuid,
    dim: usize,
    tile: usize,
    device_name: &str,
    banner: &str,
) -> (String, u64) {
    let outcome = run_simd_compute(
        &SimdRequest {
            kernel_name,
            input_data,
            task_id,
            dim,
            tile,
            device_name,
            banner,
        },
        &|| false,
    )
    .expect("a run that is never asked to stop cannot be interrupted");
    (outcome.stdout, outcome.elapsed_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_vs_reference_oracle_exact_match() {
        for dim in [16, 32, 48, 64, 33] {
            let seed = 42;
            let (a, b) = generate_deterministic_matrices(dim, seed);

            let mut c_simd = vec![0.0f32; dim * dim];
            matmul_simd(&a, &b, &mut c_simd, dim);

            let c_oracle = reference_matmul(&a, &b, dim);

            assert_eq!(c_simd.len(), c_oracle.len());
            for idx in 0..c_simd.len() {
                assert_eq!(
                    c_simd[idx].to_bits(),
                    c_oracle[idx].to_bits(),
                    "Mismatch at idx {idx} for dim {dim}"
                );
            }

            let m_simd = compute_matrix_metrics(&c_simd, dim);
            let m_oracle = compute_matrix_metrics(&c_oracle, dim);
            assert_eq!(m_simd, m_oracle);
        }
    }

    /// `derive_seed` can return any u64 and the old `+ seed` overflowed (a panic in the debug
    /// profile, silent wrap-around in release) for the top of that range.
    #[test]
    fn seed_near_u64_max_does_not_overflow_and_matches_wrapping_arithmetic() {
        for seed in [u64::MAX, u64::MAX - 1, u64::MAX - 26_000, 0, 1] {
            for dim in [1usize, 16, 33, 64] {
                let (a, b) = generate_deterministic_matrices(dim, seed);
                for i in 0..dim {
                    for j in 0..dim {
                        let expect_a =
                            ((seed.wrapping_add((i * 37 + j * 17) as u64) % 1000) as f32) / 100.0;
                        let expect_b =
                            ((seed.wrapping_add((i * 19 + j * 43) as u64) % 1000) as f32) / 100.0;
                        assert_eq!(a[i * dim + j], expect_a, "A({i},{j}) seed {seed} dim {dim}");
                        assert_eq!(b[i * dim + j], expect_b, "B({i},{j}) seed {seed} dim {dim}");
                    }
                }
            }
        }
    }

    /// Seeds that never overflowed must keep producing the very same matrices (and so digests).
    #[test]
    fn seed_arithmetic_is_unchanged_for_seeds_that_did_not_overflow() {
        let (a, b) = generate_deterministic_matrices(3, 5);
        // (i*37 + j*17 + 5) % 1000 / 100 and (i*19 + j*43 + 5) % 1000 / 100
        assert_eq!(a[0], 0.05);
        assert_eq!(a[1], 0.22);
        assert_eq!(a[3], 0.42);
        assert_eq!(b[1], 0.48);
        assert_eq!(b[3], 0.24);
    }

    fn matrices(dim: usize) -> (Vec<f32>, Vec<f32>, Vec<f32>) {
        let (a, b) = generate_deterministic_matrices(dim, 7);
        let mut c = vec![0.0f32; dim * dim];
        matmul_simd(&a, &b, &mut c, dim);
        (a, b, c)
    }

    #[test]
    fn verification_accepts_a_correct_product_small_and_sampled() {
        for dim in [1usize, 2, 15, 16, 17, 64, 65, 200] {
            let (a, b, c) = matrices(dim);
            let v = verify_gemm(&a, &b, &c, dim);
            assert!(v.passed(), "dim {dim}: {}", v.summary());
            assert_eq!(v.status(), "VERIFIED_OK");
            assert!(v.checked_entries > 0);
            assert!(v.checked_entries <= VERIFY_SAMPLE_ENTRIES.max(dim * dim));
        }
    }

    #[test]
    fn verification_accepts_fused_and_reordered_accumulation() {
        // A GPU may use fused multiply-add and a different summation order: still correct.
        let dim = 96;
        let (a, b, _) = matrices(dim);
        let mut c = vec![0.0f32; dim * dim];
        for i in 0..dim {
            for j in 0..dim {
                let mut acc = 0.0f32;
                for k in (0..dim).rev() {
                    acc = a[i * dim + k].mul_add(b[k * dim + j], acc);
                }
                c[i * dim + j] = acc;
            }
        }
        let v = verify_gemm(&a, &b, &c, dim);
        assert!(v.passed(), "{}", v.summary());
        assert!(
            v.max_relative_error > 0.0,
            "the reordered sum should differ in the last bits"
        );
    }

    #[test]
    fn verification_rejects_zero_nan_and_garbage_output() {
        for dim in [4usize, 16, 100] {
            let (a, b, good) = matrices(dim);

            let zeros = vec![0.0f32; dim * dim];
            assert!(
                !verify_gemm(&a, &b, &zeros, dim).passed(),
                "all zero, dim {dim}"
            );

            let mut nan = good.clone();
            nan[dim * dim - 1] = f32::NAN;
            let v = verify_gemm(&a, &b, &nan, dim);
            assert!(!v.passed(), "one NaN, dim {dim}");
            assert_eq!(v.non_finite_entries, 1);
            assert_eq!(v.status(), "VERIFICATION_FAILED");

            let mut inf = good.clone();
            inf[0] = f32::INFINITY;
            assert!(
                !verify_gemm(&a, &b, &inf, dim).passed(),
                "one inf, dim {dim}"
            );

            let mut off = good.clone();
            off[dim + 1] *= 1.01; // 1 % off at a position every size checks
            let v = verify_gemm(&a, &b, &off, dim);
            // dim 100 samples; the corrupted entry may be unsampled there, so only demand it
            // where the whole matrix is checked.
            if dim * dim <= VERIFY_SAMPLE_ENTRIES {
                assert!(!v.passed(), "1% error, dim {dim}: {}", v.summary());
                assert_eq!(v.first_mismatch.map(|m| (m.0, m.1)), Some((1, 1)));
            }

            // a transposed product is the classic indexing bug
            let mut transposed = vec![0.0f32; dim * dim];
            for i in 0..dim {
                for j in 0..dim {
                    transposed[j * dim + i] = good[i * dim + j];
                }
            }
            assert!(
                !verify_gemm(&a, &b, &transposed, dim).passed(),
                "transposed, dim {dim}"
            );
        }
    }

    #[test]
    fn verification_with_nothing_to_compare_does_not_pass() {
        let v = verify_gemm(&[], &[], &[], 0);
        assert!(!v.passed());
        assert_eq!(v.status(), "VERIFICATION_FAILED");
        // wrong buffer sizes are not verified either
        let v = verify_gemm(&[1.0; 4], &[1.0; 4], &[1.0; 3], 2);
        assert!(!v.passed());
    }

    #[test]
    fn verification_positions_are_deterministic_and_cover_the_corners() {
        let dim = 1000;
        let first = verification_positions(dim);
        let second = verification_positions(dim);
        assert_eq!(first, second);
        assert_eq!(first.len(), VERIFY_SAMPLE_ENTRIES);
        for corner in [(0, 0), (0, dim - 1), (dim - 1, 0), (dim - 1, dim - 1)] {
            assert!(first.contains(&corner), "{corner:?}");
        }
        assert!(first.iter().all(|&(i, j)| i < dim && j < dim));
        assert_eq!(verification_positions(8).len(), 64);
    }

    #[test]
    fn simd_report_is_verified_and_honest_about_what_it_is() {
        let (stdout, ms) = execute_simd_compute("k", b"x", Uuid::nil(), 40, 16);
        assert!(ms >= 1);
        assert!(stdout.contains("Status: VERIFIED_OK"), "{stdout}");
        assert!(
            stdout.contains("Verification: 1600 of 1600 entries"),
            "{stdout}"
        );
        assert!(stdout.contains("Backend: CPU_SIMD_FALLBACK"));
        assert!(stdout.contains("Work Group Size: 16x16 (nominal: the CPU engine does not tile)"));
        assert!(stdout.contains("Digest Scope:"));
    }

    #[test]
    fn cancellable_matmul_stops_between_rows() {
        let dim = 64;
        let (a, b) = generate_deterministic_matrices(dim, 1);
        let mut c = vec![0.0f32; dim * dim];
        assert!(!matmul_simd_cancellable(&a, &b, &mut c, dim, &|| true));
        let calls = std::cell::Cell::new(0);
        let stop_on_third = || {
            calls.set(calls.get() + 1);
            calls.get() >= 3
        };
        assert!(!matmul_simd_cancellable(
            &a,
            &b,
            &mut c,
            dim,
            &stop_on_third
        ));
        assert_eq!(calls.get(), 3);
        assert!(matmul_simd_cancellable(&a, &b, &mut c, dim, &|| false));
    }

    #[test]
    fn run_simd_compute_reports_interruption() {
        let request = SimdRequest {
            kernel_name: "k",
            input_data: b"",
            task_id: Uuid::nil(),
            dim: 32,
            tile: 16,
            device_name: "d",
            banner: "[b]",
        };
        let err = run_simd_compute(&request, &|| true).unwrap_err();
        assert!(matches!(err, GpuError::Interrupted(_)), "{err:?}");
    }
}
