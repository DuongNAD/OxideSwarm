//! Times one `dim x dim` matrix multiplication on this machine's GPU and on its CPU, and shows
//! which adapter the GPU engine picked:
//!
//! ```text
//! cargo run --release -p rusty_grid_worker --features gpu-wgpu --example gemm_bench -- 1024
//! ```
//!
//! GPU time is upload + multiplication + read-back of the engine's own measurement (engine
//! start-up and the CPU verification of the result are not part of it); CPU time is the 8-wide
//! SIMD multiplication of `cpu_simd` on one thread. Use a release build: a debug build says
//! nothing about the CPU.

use std::time::Instant;
use uuid::Uuid;

use rusty_grid_worker::cpu_simd;
use rusty_grid_worker::wgpu_engine::EngineProvider;

fn main() {
    let dim: usize = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(1024);
    let lease = match EngineProvider::shared().get() {
        Ok(lease) => lease,
        Err(err) => {
            eprintln!("no GPU engine here: {err}");
            std::process::exit(2);
        }
    };
    let engine = lease.engine;
    let info = &engine.adapter_info;
    println!(
        "GPU: {} ({:?}, {:?}); engine start-up {} ms",
        info.name,
        info.backend,
        info.device_type,
        lease.init_ms.unwrap_or(0)
    );
    println!(
        "Matrix: {dim}x{dim} f32, {:.2} GFLOP",
        2.0 * (dim as f64).powi(3) / 1e9
    );

    // The first dispatch pays for driver-side shader warm-up; do not count it.
    let task_id = Uuid::new_v4();
    if let Err(err) = engine.execute_gemm("warm_up", b"bench", task_id, dim, 16) {
        eprintln!("GPU run failed: {err}");
        std::process::exit(1);
    }
    let mut gpu_ms: Vec<u64> = (0..5)
        .map(|_| {
            engine
                .execute_gemm("bench", b"bench", task_id, dim, 16)
                .expect("GPU run")
                .elapsed_ms
        })
        .collect();
    gpu_ms.sort_unstable();
    println!(
        "GPU compute time: best {} ms, median {} ms (5 runs, millisecond resolution)",
        gpu_ms[0], gpu_ms[2]
    );

    let seed = cpu_simd::derive_seed(b"bench", task_id);
    let (a, b) = cpu_simd::generate_deterministic_matrices(dim, seed);
    let mut c = vec![0.0f32; dim * dim];
    let started = Instant::now();
    cpu_simd::matmul_simd(&a, &b, &mut c, dim);
    let cpu_ms = started.elapsed().as_millis();
    println!("CPU SIMD time (1 thread): {cpu_ms} ms");
    println!(
        "GPU speed-up over the CPU SIMD engine: {:.1}x (median)",
        cpu_ms as f64 / gpu_ms[2].max(1) as f64
    );
}
