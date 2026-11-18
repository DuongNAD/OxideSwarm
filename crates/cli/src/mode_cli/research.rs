//! `ox-mode research` workflow engine and interactive menu, split out of `mode_cli.rs`
//! (design 0080, MAINT-2).

use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use chrono::Utc;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

use rusty_grid_core::capabilities::{MobileCapabilities, WorkerCapabilities};
use rusty_grid_core::mode::{ModeExecutionResult, WorkflowMode};
use rusty_grid_core::protocol::{deserialize_message, serialize_message, MasterMessage, WireCodec};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};

use super::{execute_single_action, render_execution_result, CLR_BOLD, CLR_RESET};

/// Captures real hardware resource profile and host telemetry.
pub fn run_hardware_profile() -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Research, "profile");

    let mut sys = System::new_with_specifics(
        RefreshKind::new()
            .with_cpu(CpuRefreshKind::new().with_cpu_usage())
            .with_memory(MemoryRefreshKind::new().with_ram()),
    );
    std::thread::sleep(Duration::from_millis(150));
    sys.refresh_cpu_specifics(CpuRefreshKind::new().with_cpu_usage());
    sys.refresh_memory();

    let total_ram_mb = sys.total_memory() / (1024 * 1024);
    let avail_bytes = if sys.available_memory() > 0 {
        sys.available_memory()
    } else if sys.free_memory() > 0 {
        sys.free_memory()
    } else {
        sys.total_memory().saturating_sub(sys.used_memory())
    };
    let avail_ram_mb = (avail_bytes / (1024 * 1024)).max(1);
    let used_ram_mb = total_ram_mb.saturating_sub(avail_ram_mb);
    let ram_usage_pct = if total_ram_mb > 0 {
        (used_ram_mb as f32 / total_ram_mb as f32) * 100.0
    } else {
        0.0
    };

    let cpu_count = sys.cpus().len();
    let cpu_brand = sys
        .cpus()
        .first()
        .map(|c| c.brand().trim().to_string())
        .unwrap_or_else(|| "Unknown CPU".to_string());
    let cpu_usage_pct = sys.global_cpu_info().cpu_usage();

    let os_name = System::name().unwrap_or_else(|| "Unknown OS".to_string());
    let os_version = System::os_version().unwrap_or_default();
    let host_name = System::host_name().unwrap_or_else(|| "localhost".to_string());

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = true;
    res.exit_code = Some(0);

    let stdout = format!(
        "=== OxideSwarm Hardware & Resource Profile ===\nHost: {} ({}, {})\nCPU Architecture: {} ({} logical cores)\nCPU Global Load: {:.1}%\nTotal Physical RAM: {} MB (~{:.1} GB)\nAvailable RAM: {} MB ({:.1}% in use)\n",
        host_name,
        os_name,
        os_version,
        cpu_brand,
        cpu_count,
        cpu_usage_pct,
        total_ram_mb,
        (total_ram_mb as f64) / 1024.0,
        avail_ram_mb,
        ram_usage_pct
    );

    res.stdout = stdout;
    res = res.with_metric("host_name", host_name);
    res = res.with_metric("os_name", os_name);
    res = res.with_metric("os_version", os_version);
    res = res.with_metric("cpu_brand", cpu_brand);
    res = res.with_metric("cpu_cores", cpu_count);
    res = res.with_metric("cpu_usage_pct", cpu_usage_pct);
    res = res.with_metric("ram_total_mb", total_ram_mb);
    res = res.with_metric("ram_available_mb", avail_ram_mb);
    res = res.with_metric("ram_usage_pct", ram_usage_pct);

    res
}

/// Executes in-memory micro-benchmarks for `Task` JSON serde and Bincode wire-protocol codec roundtrips.
pub fn run_micro_benchmarks() -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Research, "bench");

    let task_count = 10_000usize;
    let dummy_task = Task::new(
        TaskSpec::new_command("echo", vec!["benchmark_payload".to_string()]),
        TaskRequirements {
            cpu_cores: 2,
            ram_mb: 512,
            gpu_required: false,
            timeout_secs: 30,
            ..Default::default()
        },
    );

    // Benchmark 1: Task JSON serialization & deserialization roundtrip
    let t1_start = Instant::now();
    let mut json_bytes_total = 0usize;
    for _ in 0..task_count {
        let serialized = serde_json::to_string(&dummy_task).expect("Task JSON serialize");
        json_bytes_total += serialized.len();
        let _deserialized: Task = serde_json::from_str(&serialized).expect("Task JSON deserialize");
    }
    let t1_dur = t1_start.elapsed();
    let t1_secs = t1_dur.as_secs_f64().max(1e-9);
    let serde_ops_per_sec = (task_count as f64 / t1_secs) as u64;
    let serde_mb_per_sec = (json_bytes_total as f64 / (1024.0 * 1024.0)) / t1_secs;

    // Benchmark 2: Real Bincode wire-protocol serialize_message + deserialize_message roundtrip
    let codec_iters = 10_000usize;
    let expected_task_id = dummy_task.id.to_string();
    let mut wire_task = dummy_task;
    let t2_start = Instant::now();
    let mut wire_bytes_total = 0usize;
    let mut codec_frame_bytes = 0usize;
    let mut codec_wire_discriminator = 0u8;
    let mut codec_decoded_task_id = String::new();
    let mut codec_verified_iters = 0usize;
    for i in 0..codec_iters {
        let expected_timeout = (i as u64) + 1;
        wire_task.requirements.timeout_secs = expected_timeout;
        let wire_msg = MasterMessage::AssignTask {
            task: wire_task.clone(),
        };
        let encoded =
            serialize_message(&wire_msg, WireCodec::Bincode).expect("Bincode wire serialize");
        wire_bytes_total += encoded.len();
        let (decoded, codec): (MasterMessage, WireCodec) =
            deserialize_message(&encoded).expect("Bincode wire deserialize");
        let disc = encoded.first().copied().unwrap_or(0);
        if codec == WireCodec::Bincode && disc == 0x02 {
            if let MasterMessage::AssignTask { task } = decoded {
                if task.requirements.timeout_secs == expected_timeout {
                    codec_verified_iters += 1;
                    if i == 0 {
                        codec_frame_bytes = encoded.len();
                        codec_wire_discriminator = disc;
                        codec_decoded_task_id = task.id.to_string();
                    }
                }
            }
        }
    }
    let t2_dur = t2_start.elapsed();
    let t2_secs = t2_dur.as_secs_f64().max(1e-9);
    let codec_ops_per_sec = (codec_iters as f64 / t2_secs) as u64;
    let codec_mb_per_sec = (wire_bytes_total as f64 / (1024.0 * 1024.0)) / t2_secs;

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = codec_frame_bytes > 5
        && codec_wire_discriminator == 0x02
        && codec_decoded_task_id == expected_task_id
        && codec_verified_iters == codec_iters;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let stdout = format!(
        "=== OxideSwarm Micro-Benchmarks ===\n1. Task JSON Serde Roundtrip: {} tasks in {:.3}s -> {} ops/sec ({:.2} MB/s)\n2. Bincode Wire Codec Roundtrip (MasterMessage::AssignTask): {} iterations in {:.3}s -> {} ops/sec ({:.2} MB/s)\nTotal Duration: {} ms\n",
        task_count,
        t1_secs,
        serde_ops_per_sec,
        serde_mb_per_sec,
        codec_iters,
        t2_secs,
        codec_ops_per_sec,
        codec_mb_per_sec,
        res.duration_ms
    );

    res.stdout = stdout;
    res = res.with_metric("serde_tasks_count", task_count);
    res = res.with_metric("serde_ops_per_sec", serde_ops_per_sec);
    res = res.with_metric("serde_mb_per_sec", serde_mb_per_sec);
    res = res.with_metric("codec_ops_per_sec", codec_ops_per_sec);
    res = res.with_metric("codec_mb_per_sec", codec_mb_per_sec);
    res = res.with_metric("codec_frame_bytes", codec_frame_bytes);
    res = res.with_metric("codec_wire_discriminator", codec_wire_discriminator);
    res = res.with_metric("codec_decoded_task_id", codec_decoded_task_id);
    res = res.with_metric("expected_task_id", expected_task_id);
    res = res.with_metric("codec_verified_iters", codec_verified_iters);

    res
}

/// Synthetic worker profile for in-memory scheduling simulation.
#[derive(Debug, Clone)]
struct SimWorker {
    pub caps: WorkerCapabilities,
    pub active_tasks: usize,
    pub current_cpu_load: f32,
}

/// Executes an in-memory synthetic scheduling simulation and computes decision latency percentiles.
///
/// Explicitly labeled as an in-memory simulation (not a live cluster) with synthetic worker profiles.
pub fn run_distributed_benchmark(_workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Research, "distributed");

    let mut workers = [
        SimWorker {
            caps: WorkerCapabilities::new("sim-cpu-node-a", 16, 32768, false, false, None),
            active_tasks: 0,
            current_cpu_load: 15.0,
        },
        SimWorker {
            caps: WorkerCapabilities::new("sim-cpu-node-b", 8, 16384, false, false, None),
            active_tasks: 0,
            current_cpu_load: 25.0,
        },
        SimWorker {
            caps: WorkerCapabilities::new("sim-gpu-node", 12, 24576, false, true, None),
            active_tasks: 0,
            current_cpu_load: 10.0,
        },
        SimWorker {
            caps: WorkerCapabilities::new("sim-mobile-node", 8, 8192, false, false, None)
                .with_mobile(Some(MobileCapabilities {
                    os_version: "synthetic-mobile".to_string(),
                    soc_model: "synthetic-soc".to_string(),
                    battery_pct: Some(78),
                    is_charging: Some(false),
                    thermal_throttled: false,
                })),
            active_tasks: 0,
            current_cpu_load: 12.0,
        },
    ];

    let task_count = 1000usize;
    let mut tasks = Vec::with_capacity(task_count);
    for i in 0..task_count {
        let (cpu, ram, gpu) = match i % 10 {
            0..=5 => (1, 256, false),
            6..=8 => (4, 2048, false),
            _ => (2, 1024, true),
        };
        tasks.push(TaskRequirements {
            cpu_cores: cpu,
            ram_mb: ram,
            gpu_required: gpu,
            timeout_secs: 60,
            ..Default::default()
        });
    }

    let sim_start = Instant::now();
    let mut latencies_us = Vec::with_capacity(task_count);
    let mut worker_allocations: HashMap<String, usize> = HashMap::new();
    let mut scheduled_count = 0usize;

    for task_req in &tasks {
        let t_start = Instant::now();
        let mut best_idx = None;
        let mut best_score = f32::MAX;

        for (idx, w) in workers.iter().enumerate() {
            if !w.caps.satisfies(task_req) {
                continue;
            }

            if let Some(ref m) = w.caps.mobile {
                if m.thermal_throttled || m.battery_pct.unwrap_or(100) < 15 {
                    continue;
                }
            }

            let gpu_penalty =
                if !task_req.gpu_required && (w.caps.has_gpu || w.caps.is_simulated_gpu) {
                    100.0
                } else {
                    0.0
                };

            let score = (w.active_tasks as f32) * 10.0 + w.current_cpu_load + gpu_penalty;
            if score < best_score {
                best_score = score;
                best_idx = Some(idx);
            }
        }

        let elapsed_us = t_start.elapsed().as_nanos() as f64 / 1000.0;
        latencies_us.push(elapsed_us);

        if let Some(idx) = best_idx {
            scheduled_count += 1;
            let w = &mut workers[idx];
            w.active_tasks += 1;
            *worker_allocations.entry(w.caps.name.clone()).or_insert(0) += 1;
        }
    }

    let sim_dur = sim_start.elapsed();
    let sim_secs = sim_dur.as_secs_f64();
    let throughput_ops_sec = if sim_secs > 0.0 {
        (scheduled_count as f64 / sim_secs) as u64
    } else {
        scheduled_count as u64
    };

    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50_us = latencies_us
        .get(latencies_us.len() / 2)
        .copied()
        .unwrap_or(0.0);
    let p90_us = latencies_us
        .get((latencies_us.len() * 90) / 100)
        .copied()
        .unwrap_or(0.0);
    let p95_us = latencies_us
        .get((latencies_us.len() * 95) / 100)
        .copied()
        .unwrap_or(0.0);
    let p99_us = latencies_us
        .get((latencies_us.len() * 99) / 100)
        .copied()
        .unwrap_or(0.0);
    let min_us = latencies_us.first().copied().unwrap_or(0.0);
    let max_us = latencies_us.last().copied().unwrap_or(0.0);

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = scheduled_count == task_count;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let mut stdout = format!(
        "=== In-Memory Synthetic Scheduling Simulation (not a live cluster) ===\nStatus: {}\nTotal Synthetic Tasks: {}\nScheduled: {} / {}\nSimulated Decision Throughput: {} tasks/sec (Simulation time: {:.3}s)\n\nLatency Percentiles (In-memory decision time per task):\n  ● Min     : {:.2} µs\n  ● P50     : {:.2} µs\n  ● P90     : {:.2} µs\n  ● P95     : {:.2} µs\n  ● P99     : {:.2} µs\n  ● Max     : {:.2} µs\n\nSynthetic Worker Allocation Breakdown:\n",
        if res.success {
            "[COMPLETED - SYNTHETIC SIMULATION]"
        } else {
            "[INCOMPLETE]"
        },
        task_count,
        scheduled_count,
        task_count,
        throughput_ops_sec,
        sim_secs,
        min_us,
        p50_us,
        p90_us,
        p95_us,
        p99_us,
        max_us
    );

    let mut sorted_workers: Vec<_> = worker_allocations.iter().collect();
    sorted_workers.sort_by_key(|(name, _)| (*name).clone());
    for (worker_name, count) in sorted_workers {
        let pct = (*count as f64 / scheduled_count.max(1) as f64) * 100.0;
        stdout.push_str(&format!(
            "  ● {:<22} -> {:>4} tasks ({:>5.1}%)\n",
            worker_name, count, pct
        ));
    }

    res.stdout = stdout;
    res = res.with_metric("simulation_only", true);
    res = res.with_metric("total_tasks", task_count);
    res = res.with_metric("scheduled_tasks", scheduled_count);
    res = res.with_metric("throughput_tasks_per_sec", throughput_ops_sec);
    res = res.with_metric("latency_p50_us", p50_us);
    res = res.with_metric("latency_p90_us", p90_us);
    res = res.with_metric("latency_p95_us", p95_us);
    res = res.with_metric("latency_p99_us", p99_us);
    res = res.with_metric("latency_min_us", min_us);
    res = res.with_metric("latency_max_us", max_us);
    res = res.with_metric("worker_allocations", worker_allocations);
    res = res.with_metric("all_tasks_scheduled", scheduled_count == task_count);

    res
}

/// Generates a research report combining host resource telemetry, micro-benchmarks, and the synthetic simulation.
pub fn generate_research_report(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Research, "report");

    let profile = run_hardware_profile();
    let bench = run_micro_benchmarks();
    let dist = run_distributed_benchmark(workspace_root);

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = profile.success && bench.success && dist.success;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let report_data = serde_json::json!({
        "timestamp": Utc::now().to_rfc3339(),
        "hardware_profile": profile.metrics,
        "micro_benchmarks": bench.metrics,
        "distributed_simulation": dist.metrics,
        "summary": {
            "status": if res.success { "completed" } else { "failed" },
            "simulation_only": true,
            "total_benchmark_duration_ms": res.duration_ms
        }
    });

    let report_file = workspace_root.join("target/research_report.json");
    let write_res =
        fs::create_dir_all(report_file.parent().unwrap_or(workspace_root)).and_then(|_| {
            let json_str = serde_json::to_string_pretty(&report_data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            fs::write(&report_file, json_str)
        });
    if let Err(e) = write_res {
        res.success = false;
        res.exit_code = Some(1);
        res.stderr = format!(
            "Failed to write research report to {}: {}",
            report_file.display(),
            e
        );
        return res;
    }

    let stdout = format!(
        "=== OxideSwarm Research & Performance Benchmark Report ===\nReport Generated: {}\nSaved Path: {}\n\n{}\n{}\n{}\n",
        Utc::now().to_rfc3339(),
        report_file.display(),
        profile.stdout,
        bench.stdout,
        dist.stdout
    );

    res.stdout = stdout;
    res = res.with_metric("report_file", report_file.to_string_lossy().to_string());
    res = res.with_metric("hardware", &profile.metrics);
    res = res.with_metric("hardware_profile", &profile.metrics);
    res = res.with_metric("micro_benchmarks", &bench.metrics);
    res = res.with_metric("distributed_simulation", &dist.metrics);
    res = res.with_metric("report_data", report_data);

    res
}

pub(super) async fn run_research_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== 🔬 RESEARCH & PERFORMANCE BENCHMARKING MODE ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Hardware Resource Profiler (CPU/RAM)");
    println!("  [2] Micro-Benchmarks (Task JSON Serde & Bincode Wire Codec)");
    println!("  [3] Synthetic Scheduling Simulation Benchmark");
    println!("  [4] Export Complete Research JSON Report");
    println!("  [0] Back to Main Menu");
    print!("Select [0-4]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            let res =
                execute_single_action(WorkflowMode::Research, "profile", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            let res =
                execute_single_action(WorkflowMode::Research, "bench", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            let res =
                execute_single_action(WorkflowMode::Research, "distributed", 5, workspace_root)
                    .await;
            render_execution_result(&res);
        }
        "4" => {
            let res =
                execute_single_action(WorkflowMode::Research, "report", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}
