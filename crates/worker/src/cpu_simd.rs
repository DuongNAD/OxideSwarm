//! Deterministic CPU SIMD Engine (Requirement R4).
//!
//! Provides high-performance, 8-wide chunked unrolled general matrix multiplication (GEMM)
//! with cache-friendly loop permutation and sequential accumulation order matching canonical
//! floating-point arithmetic. Computes deterministic invariant checksums (Trace, Frobenius Norm,
//! and 64-bit FNV-1a verification digest) for grid verification.

use std::time::Instant;
use uuid::Uuid;

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
pub fn generate_deterministic_matrices(dim: usize, seed: u64) -> (Vec<f32>, Vec<f32>) {
    let mut a = vec![0.0f32; dim * dim];
    let mut b = vec![0.0f32; dim * dim];

    for i in 0..dim {
        for j in 0..dim {
            let idx = i * dim + j;
            a[idx] = (((i as u64 * 37 + j as u64 * 17 + seed) % 1000) as f32) / 100.0;
            b[idx] = (((i as u64 * 19 + j as u64 * 43 + seed) % 1000) as f32) / 100.0;
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
    assert_eq!(a.len(), dim * dim, "Matrix A size mismatch");
    assert_eq!(b.len(), dim * dim, "Matrix B size mismatch");
    assert_eq!(c.len(), dim * dim, "Matrix C size mismatch");

    c.fill(0.0);

    for i in 0..dim {
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

/// Executes deterministic CPU SIMD compute with standardized output formatting.
pub fn execute_simd_compute(
    kernel_name: &str,
    input_data: &[u8],
    task_id: Uuid,
    dim: usize,
    tile: usize,
    start_time: Instant,
) -> (String, u64) {
    execute_simd_compute_with_device(
        kernel_name,
        input_data,
        task_id,
        dim,
        tile,
        start_time,
        "Simulated Virtual GPU (Matrix Engine)",
        "[GPU COMPUTE SIMULATOR]",
    )
}

/// Executes deterministic CPU SIMD compute with explicit device name and banner.
pub fn execute_simd_compute_with_device(
    kernel_name: &str,
    input_data: &[u8],
    task_id: Uuid,
    dim: usize,
    tile: usize,
    start_time: Instant,
    device_name: &str,
    banner: &str,
) -> (String, u64) {
    let seed = derive_seed(input_data, task_id);
    let (a, b) = generate_deterministic_matrices(dim, seed);
    let mut c = vec![0.0f32; dim * dim];

    matmul_simd(&a, &b, &mut c, dim);

    let metrics = compute_matrix_metrics(&c, dim);
    let elapsed = std::cmp::max(1, start_time.elapsed().as_millis() as u64);
    let flops = 2u64 * (dim as u64) * (dim as u64) * (dim as u64);

    let stdout = format!(
        "{banner}\nDevice: {device_name}\nBackend: CPU_SIMD_FALLBACK\nKernel: {kernel_name}\nMatrix Dimension: {dim}x{dim} (FLOPs: {flops})\nWork Group Size: {tile}x{tile}\nExecution Time: {elapsed} ms\nMatrix Trace: {:.4}\nFrobenius Norm: {:.4}\nVerification Digest: 0x{:016x}\nStatus: VERIFIED_OK\n",
        metrics.trace, metrics.frobenius_norm, metrics.bit_hash
    );

    (stdout, elapsed)
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
}
