//! Programmatic serialization performance benchmark comparing Bincode vs JSON.
//!
//! Validates Requirement R1 and R4:
//! - Measurable reduction in payload size (e.g. ~66% to ~78% on binary data buffers)
//! - Measurable improvement in serialization and deserialization throughput
//! - Bit-for-bit lossless roundtrip deserialization equivalence

use std::time::Instant;
use uuid::Uuid;

use rusty_grid_core::protocol::{deserialize_message, serialize_message, MasterMessage, WireCodec};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskResult, TaskSpec};

const BENCH_ITERATIONS: usize = 5_000;

#[test]
fn test_task_spec_and_result_benchmark() {
    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();

    // 1. GPU TaskSpec with 16 KB raw binary input buffer (simulated float/weight buffer)
    let gpu_spec = TaskSpec::GpuCompute {
        kernel_name: "gemm_fp32".into(),
        input_data: (0..16384).map(|i| ((i * 31 + 128) % 256) as u8).collect(),
        work_group_size: 64,
        simulated_matrix_dim: 128,
        compute_intensity: 100,
    };

    // 2. TaskResult with 32 KB stdout logs
    let task_result = TaskResult::success(worker_id, task_id, "x".repeat(32768), 150, true);

    // =========================================================================
    // Part 1: TaskSpec::GpuCompute (Binary Buffer) Benchmark
    // =========================================================================
    let json_gpu_bytes = serde_json::to_vec(&gpu_spec).expect("JSON serialize gpu_spec");
    let bincode_gpu_bytes = bincode::serialize(&gpu_spec).expect("Bincode serialize gpu_spec");

    // Lossless roundtrip verification
    let json_deser: TaskSpec = serde_json::from_slice(&json_gpu_bytes).expect("JSON deserialize");
    let bincode_deser: TaskSpec =
        bincode::deserialize(&bincode_gpu_bytes).expect("Bincode deserialize");
    assert_eq!(json_deser, gpu_spec);
    assert_eq!(bincode_deser, gpu_spec);

    let size_savings_pct =
        100.0 * (1.0 - (bincode_gpu_bytes.len() as f64 / json_gpu_bytes.len() as f64));
    println!("\n=== Benchmark: TaskSpec::GpuCompute (16 KB buffer) ===");
    println!("JSON Size:        {} bytes", json_gpu_bytes.len());
    println!("Bincode Size:     {} bytes", bincode_gpu_bytes.len());
    println!("Payload Savings:  {:.2}%", size_savings_pct);

    // Bincode should be substantially smaller than JSON (JSON prints [128,159,...])
    assert!(
        bincode_gpu_bytes.len() < json_gpu_bytes.len(),
        "Bincode payload must be strictly smaller than JSON payload"
    );
    assert!(
        size_savings_pct > 60.0,
        "Expected >60% payload size savings on raw binary buffer, got {:.2}%",
        size_savings_pct
    );

    // Warm-up
    for _ in 0..500 {
        let _ = bincode::serialize(&gpu_spec).unwrap();
        let _ = serde_json::to_vec(&gpu_spec).unwrap();
    }

    // Benchmark Bincode serialization
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        let _ = bincode::serialize(&gpu_spec).unwrap();
    }
    let bincode_ser_time = start.elapsed();

    // Benchmark JSON serialization
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        let _ = serde_json::to_vec(&gpu_spec).unwrap();
    }
    let json_ser_time = start.elapsed();

    let ser_speedup = json_ser_time.as_nanos() as f64 / bincode_ser_time.as_nanos().max(1) as f64;
    println!(
        "Bincode Ser Time: {:?} ({} iters)",
        bincode_ser_time, BENCH_ITERATIONS
    );
    println!(
        "JSON Ser Time:    {:?} ({} iters)",
        json_ser_time, BENCH_ITERATIONS
    );
    println!("Ser Speedup:      {:.2}x", ser_speedup);
    assert!(
        bincode_ser_time <= json_ser_time,
        "Bincode serialization must be faster than or equal to JSON"
    );

    // Benchmark Deserialization
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        let _: TaskSpec = bincode::deserialize(&bincode_gpu_bytes).unwrap();
    }
    let bincode_de_time = start.elapsed();

    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        let _: TaskSpec = serde_json::from_slice(&json_gpu_bytes).unwrap();
    }
    let json_de_time = start.elapsed();

    let de_speedup = json_de_time.as_nanos() as f64 / bincode_de_time.as_nanos().max(1) as f64;
    println!(
        "Bincode De Time:  {:?} ({} iters)",
        bincode_de_time, BENCH_ITERATIONS
    );
    println!(
        "JSON De Time:     {:?} ({} iters)",
        json_de_time, BENCH_ITERATIONS
    );
    println!("De Speedup:       {:.2}x", de_speedup);

    // =========================================================================
    // Part 2: TaskResult (Heavy Log Output) Benchmark
    // =========================================================================
    let json_res_bytes = serde_json::to_vec(&task_result).expect("JSON serialize task_result");
    let bincode_res_bytes =
        bincode::serialize(&task_result).expect("Bincode serialize task_result");

    let json_res_deser: TaskResult =
        serde_json::from_slice(&json_res_bytes).expect("JSON deserialize");
    let bincode_res_deser: TaskResult =
        bincode::deserialize(&bincode_res_bytes).expect("Bincode deserialize");
    assert_eq!(json_res_deser, task_result);
    assert_eq!(bincode_res_deser, task_result);

    println!("\n=== Benchmark: TaskResult (32 KB stdout) ===");
    println!("JSON Size:        {} bytes", json_res_bytes.len());
    println!("Bincode Size:     {} bytes", bincode_res_bytes.len());
    assert!(bincode_res_bytes.len() <= json_res_bytes.len());

    // Warm-up
    for _ in 0..500 {
        let _ = bincode::serialize(&task_result).unwrap();
        let _ = serde_json::to_vec(&task_result).unwrap();
    }

    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        let _ = bincode::serialize(&task_result).unwrap();
    }
    let bincode_res_ser_time = start.elapsed();

    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        let _ = serde_json::to_vec(&task_result).unwrap();
    }
    let json_res_ser_time = start.elapsed();

    let res_ser_speedup =
        json_res_ser_time.as_nanos() as f64 / bincode_res_ser_time.as_nanos().max(1) as f64;
    println!("Bincode Res Ser:  {:?}", bincode_res_ser_time);
    println!("JSON Res Ser:     {:?}", json_res_ser_time);
    println!("Res Ser Speedup:  {:.2}x", res_ser_speedup);

    // =========================================================================
    // Part 3: Framed Message Wire Protocol Benchmark (serialize_message / deserialize_message)
    // =========================================================================
    let task = Task::new(
        gpu_spec,
        TaskRequirements {
            cpu_cores: 4,
            ram_mb: 8192,
            gpu_required: true,
            timeout_secs: 60,
            max_retries: None,
        },
    );
    let assign_msg = MasterMessage::AssignTask { task };

    let framed_bincode =
        serialize_message(&assign_msg, WireCodec::Bincode).expect("serialize bincode");
    let framed_json = serialize_message(&assign_msg, WireCodec::Json).expect("serialize json");

    // Format discriminator assertions
    assert_eq!(
        framed_bincode[0], 0x02,
        "Bincode frame must start with 0x02"
    );
    assert_eq!(framed_json[0], 0x01, "JSON frame must start with 0x01");

    let (decoded_bin_msg, detected_bin_codec): (MasterMessage, WireCodec) =
        deserialize_message(&framed_bincode).expect("deserialize bincode frame");
    let (decoded_json_msg, detected_json_codec): (MasterMessage, WireCodec) =
        deserialize_message(&framed_json).expect("deserialize json frame");

    assert_eq!(detected_bin_codec, WireCodec::Bincode);
    assert_eq!(detected_json_codec, WireCodec::Json);
    assert_eq!(decoded_bin_msg, assign_msg);
    assert_eq!(decoded_json_msg, assign_msg);

    println!("\n=== Framed MasterMessage::AssignTask Wire Protocol ===");
    println!("Framed Bincode Size: {} bytes", framed_bincode.len());
    println!("Framed JSON Size:    {} bytes", framed_json.len());
    println!(
        "Payload Savings:     {:.2}%",
        100.0 * (1.0 - (framed_bincode.len() as f64 / framed_json.len() as f64))
    );
    println!("All programmatic serialization comparisons passed successfully!\n");
}
