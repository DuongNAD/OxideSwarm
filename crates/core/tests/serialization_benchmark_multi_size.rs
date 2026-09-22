//! Programmatic serialization performance benchmark comparing Bincode vs JSON across multiple payload sizes.
//!
//! Validates Requirement R1 and R4:
//! - Payload sizes: 1 KB (1024), 16 KB (16384), 64 KB (65536), 1 MB (1048576)
//! - Measures JSON vs Bincode payload sizes & size reduction %
//! - Measures serialization & deserialization throughput / speedup
//! - Verifies bit-for-bit lossless roundtrip equivalence for both formats
//! - Verifies framed protocol wire message discriminator bytes (0x02 vs 0x01)

use std::time::Instant;
use uuid::Uuid;

use rusty_grid_core::protocol::{
    deserialize_message, serialize_message, MasterMessage, WireCodec,
};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskResult, TaskSpec};

#[allow(dead_code)]
struct BenchmarkResult {
    payload_name: &'static str,
    target_size_bytes: usize,
    json_bytes: usize,
    bincode_bytes: usize,
    size_reduction_pct: f64,
    json_ser_micros: f64,
    bincode_ser_micros: f64,
    ser_speedup: f64,
    json_de_micros: f64,
    bincode_de_micros: f64,
    de_speedup: f64,
    roundtrip_ok: bool,
}

fn bench_gpu_compute(size: usize, iters: usize) -> BenchmarkResult {
    let input_data: Vec<u8> = (0..size).map(|i| ((i * 31 + 128) % 256) as u8).collect();
    let spec = TaskSpec::GpuCompute {
        kernel_name: format!("kernel_{}_bytes", size),
        input_data,
        work_group_size: 64,
        simulated_matrix_dim: 128,
        compute_intensity: 100,
    };

    // Serialize once for sizing and roundtrip verification
    let json_bytes = serde_json::to_vec(&spec).expect("JSON serialize");
    let bincode_bytes = bincode::serialize(&spec).expect("Bincode serialize");

    let json_deser: TaskSpec = serde_json::from_slice(&json_bytes).expect("JSON deserialize");
    let bincode_deser: TaskSpec = bincode::deserialize(&bincode_bytes).expect("Bincode deserialize");

    let roundtrip_ok = (json_deser == spec) && (bincode_deser == spec);
    assert!(roundtrip_ok, "Lossless roundtrip must match original spec");

    let size_reduction_pct = 100.0 * (1.0 - (bincode_bytes.len() as f64 / json_bytes.len() as f64));

    // Warm-up
    for _ in 0..iters.min(50) {
        let _ = serde_json::to_vec(&spec).unwrap();
        let _ = bincode::serialize(&spec).unwrap();
    }

    // Benchmark Bincode serialization
    let start = Instant::now();
    for _ in 0..iters {
        let _ = bincode::serialize(&spec).unwrap();
    }
    let bincode_ser_total = start.elapsed();
    let bincode_ser_micros = bincode_ser_total.as_secs_f64() * 1_000_000.0 / (iters as f64);

    // Benchmark JSON serialization
    let start = Instant::now();
    for _ in 0..iters {
        let _ = serde_json::to_vec(&spec).unwrap();
    }
    let json_ser_total = start.elapsed();
    let json_ser_micros = json_ser_total.as_secs_f64() * 1_000_000.0 / (iters as f64);

    let ser_speedup = json_ser_micros / bincode_ser_micros.max(0.001);

    // Benchmark Bincode deserialization
    let start = Instant::now();
    for _ in 0..iters {
        let _: TaskSpec = bincode::deserialize(&bincode_bytes).unwrap();
    }
    let bincode_de_total = start.elapsed();
    let bincode_de_micros = bincode_de_total.as_secs_f64() * 1_000_000.0 / (iters as f64);

    // Benchmark JSON deserialization
    let start = Instant::now();
    for _ in 0..iters {
        let _: TaskSpec = serde_json::from_slice(&json_bytes).unwrap();
    }
    let json_de_total = start.elapsed();
    let json_de_micros = json_de_total.as_secs_f64() * 1_000_000.0 / (iters as f64);

    let de_speedup = json_de_micros / bincode_de_micros.max(0.001);

    BenchmarkResult {
        payload_name: "TaskSpec::GpuCompute",
        target_size_bytes: size,
        json_bytes: json_bytes.len(),
        bincode_bytes: bincode_bytes.len(),
        size_reduction_pct,
        json_ser_micros,
        bincode_ser_micros,
        ser_speedup,
        json_de_micros,
        bincode_de_micros,
        de_speedup,
        roundtrip_ok,
    }
}

fn bench_task_result(size: usize, iters: usize) -> BenchmarkResult {
    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();
    let stdout = "A".repeat(size);

    let result = TaskResult::success(worker_id, task_id, stdout, 100, true);

    let json_bytes = serde_json::to_vec(&result).expect("JSON serialize");
    let bincode_bytes = bincode::serialize(&result).expect("Bincode serialize");

    let json_deser: TaskResult = serde_json::from_slice(&json_bytes).expect("JSON deserialize");
    let bincode_deser: TaskResult = bincode::deserialize(&bincode_bytes).expect("Bincode deserialize");

    let roundtrip_ok = (json_deser == result) && (bincode_deser == result);
    assert!(roundtrip_ok, "Lossless roundtrip must match original result");

    let size_reduction_pct = 100.0 * (1.0 - (bincode_bytes.len() as f64 / json_bytes.len() as f64));

    // Warm-up
    for _ in 0..iters.min(50) {
        let _ = serde_json::to_vec(&result).unwrap();
        let _ = bincode::serialize(&result).unwrap();
    }

    // Benchmark Bincode serialization
    let start = Instant::now();
    for _ in 0..iters {
        let _ = bincode::serialize(&result).unwrap();
    }
    let bincode_ser_total = start.elapsed();
    let bincode_ser_micros = bincode_ser_total.as_secs_f64() * 1_000_000.0 / (iters as f64);

    // Benchmark JSON serialization
    let start = Instant::now();
    for _ in 0..iters {
        let _ = serde_json::to_vec(&result).unwrap();
    }
    let json_ser_total = start.elapsed();
    let json_ser_micros = json_ser_total.as_secs_f64() * 1_000_000.0 / (iters as f64);

    let ser_speedup = json_ser_micros / bincode_ser_micros.max(0.001);

    // Benchmark Bincode deserialization
    let start = Instant::now();
    for _ in 0..iters {
        let _: TaskResult = bincode::deserialize(&bincode_bytes).unwrap();
    }
    let bincode_de_total = start.elapsed();
    let bincode_de_micros = bincode_de_total.as_secs_f64() * 1_000_000.0 / (iters as f64);

    // Benchmark JSON deserialization
    let start = Instant::now();
    for _ in 0..iters {
        let _: TaskResult = serde_json::from_slice(&json_bytes).unwrap();
    }
    let json_de_total = start.elapsed();
    let json_de_micros = json_de_total.as_secs_f64() * 1_000_000.0 / (iters as f64);

    let de_speedup = json_de_micros / bincode_de_micros.max(0.001);

    BenchmarkResult {
        payload_name: "TaskResult",
        target_size_bytes: size,
        json_bytes: json_bytes.len(),
        bincode_bytes: bincode_bytes.len(),
        size_reduction_pct,
        json_ser_micros,
        bincode_ser_micros,
        ser_speedup,
        json_de_micros,
        bincode_de_micros,
        de_speedup,
        roundtrip_ok,
    }
}

#[test]
fn test_multi_payload_size_benchmark_matrix() {
    let test_sizes = [
        ("1 KB", 1024, 2000),
        ("16 KB", 16384, 1000),
        ("64 KB", 65536, 500),
        ("1 MB", 1048576, 50),
    ];

    println!("\n==========================================================================================");
    println!("EMPIRICAL SERIALIZATION BENCHMARK MATRIX: JSON VS BINCODE ACROSS MULTIPLE PAYLOAD SIZES");
    println!("==========================================================================================");

    println!("\n--- Part 1: TaskSpec::GpuCompute (Raw Binary Payload) ---");
    println!(
        "{:<8} | {:<10} | {:<10} | {:<10} | {:<12} | {:<12} | {:<10} | {:<12} | {:<12} | {:<10}",
        "Size", "JSON (B)", "Bin (B)", "Reduction", "JSON Ser (us)", "Bin Ser (us)", "Ser Speed", "JSON De (us)", "Bin De (us)", "De Speed"
    );
    println!("{}", "-".repeat(118));

    for (label, size_bytes, iters) in &test_sizes {
        let res = bench_gpu_compute(*size_bytes, *iters);
        println!(
            "{:<8} | {:<10} | {:<10} | {:>9.2}% | {:>12.2} | {:>12.2} | {:>9.2}x | {:>12.2} | {:>12.2} | {:>9.2}x",
            label,
            res.json_bytes,
            res.bincode_bytes,
            res.size_reduction_pct,
            res.json_ser_micros,
            res.bincode_ser_micros,
            res.ser_speedup,
            res.json_de_micros,
            res.bincode_de_micros,
            res.de_speedup
        );

        // Verification assertions
        assert!(res.roundtrip_ok, "Roundtrip must be lossless");
        assert!(
            res.bincode_bytes < res.json_bytes,
            "Bincode size must be strictly smaller than JSON for binary payloads"
        );
        assert!(
            res.size_reduction_pct > 65.0,
            "Expected >65% reduction on binary buffer across all sizes, got {:.2}% for {}",
            res.size_reduction_pct,
            label
        );
        assert!(
            res.ser_speedup >= 1.0,
            "Bincode serialization must be faster than JSON: got {:.2}x for {}",
            res.ser_speedup,
            label
        );
    }

    println!("\n--- Part 2: TaskResult (String Log Output Payload) ---");
    println!(
        "{:<8} | {:<10} | {:<10} | {:<10} | {:<12} | {:<12} | {:<10} | {:<12} | {:<12} | {:<10}",
        "Size", "JSON (B)", "Bin (B)", "Reduction", "JSON Ser (us)", "Bin Ser (us)", "Ser Speed", "JSON De (us)", "Bin De (us)", "De Speed"
    );
    println!("{}", "-".repeat(118));

    for (label, size_bytes, iters) in &test_sizes {
        let res = bench_task_result(*size_bytes, *iters);
        println!(
            "{:<8} | {:<10} | {:<10} | {:>9.2}% | {:>12.2} | {:>12.2} | {:>9.2}x | {:>12.2} | {:>12.2} | {:>9.2}x",
            label,
            res.json_bytes,
            res.bincode_bytes,
            res.size_reduction_pct,
            res.json_ser_micros,
            res.bincode_ser_micros,
            res.ser_speedup,
            res.json_de_micros,
            res.bincode_de_micros,
            res.de_speedup
        );

        assert!(res.roundtrip_ok, "Roundtrip must be lossless");
        assert!(
            res.bincode_bytes <= res.json_bytes,
            "Bincode size must be <= JSON size for string logs"
        );
        assert!(
            res.ser_speedup >= 1.0,
            "Bincode serialization must be faster than JSON: got {:.2}x for {}",
            res.ser_speedup,
            label
        );
    }

    println!("\n--- Part 3: Framed Message Wire Protocol Discriminator Across Sizes ---");
    for (label, size_bytes, _) in &test_sizes {
        let input_data: Vec<u8> = (0..*size_bytes).map(|i| (i % 256) as u8).collect();
        let spec = TaskSpec::GpuCompute {
            kernel_name: format!("framed_{}", label),
            input_data,
            work_group_size: 32,
            simulated_matrix_dim: 64,
            compute_intensity: 10,
        };
        let task = Task::new(
            spec,
            TaskRequirements {
                cpu_cores: 2,
                ram_mb: 2048,
                gpu_required: true,
                timeout_secs: 30,
                max_retries: None,
            },
        );
        let msg = MasterMessage::AssignTask { task };

        let bin_wire = serialize_message(&msg, WireCodec::Bincode).expect("bincode wire serialize");
        let json_wire = serialize_message(&msg, WireCodec::Json).expect("json wire serialize");

        assert_eq!(bin_wire[0], 0x02, "Bincode wire discriminator must be 0x02");
        assert_eq!(json_wire[0], 0x01, "JSON wire discriminator must be 0x01");

        let (decoded_bin, codec_bin): (MasterMessage, WireCodec) =
            deserialize_message(&bin_wire).expect("bincode wire deserialize");
        let (decoded_json, codec_json): (MasterMessage, WireCodec) =
            deserialize_message(&json_wire).expect("json wire deserialize");

        assert_eq!(codec_bin, WireCodec::Bincode);
        assert_eq!(codec_json, WireCodec::Json);
        assert_eq!(decoded_bin, msg);
        assert_eq!(decoded_json, msg);

        let savings = 100.0 * (1.0 - (bin_wire.len() as f64 / json_wire.len() as f64));
        println!(
            "{:<8} Framed: JSON = {:>10} B, Bincode = {:>10} B, Savings = {:>6.2}%, Codec Detection OK",
            label, json_wire.len(), bin_wire.len(), savings
        );
        assert!(
            savings > 60.0,
            "Framed wire savings on GPU task must exceed 60%, got {:.2}%",
            savings
        );
    }
    println!("\nMulti-size benchmark matrix validation PASSED with 100% rigor!\n");
}
