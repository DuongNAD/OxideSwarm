//! Adversarial Stress-Test Suite for Milestone 1: 3D Core Protocol & Capability Telemetry.
//!
//! Objectives:
//! 1. Conflicting override combinations:
//!    - `--no-gpu` alongside `--cuda` or `--vram-mb 16000` (strict domination forces GPU/VRAM/CUDA to None/false).
//!    - `--no-blender` alongside `--blender-path "C:\Blender"` (strict domination forces has_blender: false and blender_version: None).
//! 2. Legacy JSON payloads:
//!    - Simulating nodes running older OxideSwarm versions lacking has_blender, vram_mb, has_cuda, asset_bytes.
//!    - Verify 100% seamless deserialization into default values.
//! 3. Rapid repeated capability detection:
//!    - 200+ sequential and concurrent detection calls to ensure zero subprocess leaks, no hanging processes, and zero file handle leaks.
//! 4. End-to-End CLI binary wire verification:
//!    - Launch oxideswarm binary with conflicting flags, receive Register message over wire, verify strict domination.

use std::collections::HashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

use rusty_grid_core::capabilities::{HardwareOverrides, WorkerCapabilities};
use rusty_grid_core::protocol::{MessageTransport, WireCodec, WorkerMessage};
use rusty_grid_core::task::{
    TaskId, TaskRequirements, TaskResult, TaskSpec,
};

// =========================================================================
// Win32 Process Handle Query Helper
// =========================================================================

#[cfg(windows)]
fn get_current_process_handle_count() -> u32 {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> *mut std::ffi::c_void;
        fn GetProcessHandleCount(hProcess: *mut std::ffi::c_void, pdwHandleCount: *mut u32) -> i32;
    }
    let mut count: u32 = 0;
    unsafe {
        let h = GetCurrentProcess();
        let res = GetProcessHandleCount(h, &mut count);
        if res != 0 {
            count
        } else {
            0
        }
    }
}

#[cfg(not(windows))]
fn get_current_process_handle_count() -> u32 {
    // Unix fallback: count open file descriptors in /proc/self/fd
    if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
        entries.count() as u32
    } else {
        0
    }
}

// =========================================================================
// PART 1: Conflicting Overrides — GPU, VRAM, and CUDA Strict Domination
// =========================================================================

#[test]
fn test_adversarial_overrides_no_gpu_dominates_cuda() {
    let overrides = HardwareOverrides {
        name: Some("node-no-gpu-cuda".into()),
        no_gpu: true,
        cuda: Some(true),
        ..Default::default()
    };
    let caps = WorkerCapabilities::detect_with_overrides(overrides);

    assert_eq!(caps.name, "node-no-gpu-cuda");
    assert!(!caps.has_gpu, "no_gpu MUST force has_gpu to false");
    assert!(!caps.is_simulated_gpu, "no_gpu MUST force is_simulated_gpu to false");
    assert!(caps.gpu_device_name.is_none(), "no_gpu MUST strip gpu_device_name");
    assert!(!caps.has_cuda, "no_gpu MUST dominate cuda and force has_cuda to false");
    assert!(caps.vram_mb.is_none(), "no_gpu MUST force vram_mb to None");
    assert!(!caps.can_execute_gpu(), "can_execute_gpu() must return false");
    assert!(
        !caps.satisfies(&TaskRequirements::gpu(60)),
        "Worker with no_gpu must NEVER satisfy GPU tasks"
    );
}

#[test]
fn test_adversarial_overrides_no_gpu_dominates_vram_mb() {
    let overrides = HardwareOverrides {
        name: Some("node-no-gpu-vram".into()),
        no_gpu: true,
        vram_mb: Some(16000),
        ..Default::default()
    };
    let caps = WorkerCapabilities::detect_with_overrides(overrides);

    assert!(!caps.has_gpu, "no_gpu MUST force has_gpu to false");
    assert!(
        caps.vram_mb.is_none(),
        "no_gpu MUST strictly dominate vram_mb (expected None, got {:?})",
        caps.vram_mb
    );
    assert!(!caps.has_cuda, "no_gpu MUST force has_cuda to false");
    assert!(!caps.can_execute_gpu());
}

#[test]
fn test_adversarial_overrides_no_gpu_dominates_all_gpu_flags_combined() {
    // Extreme conflict: user provides every single positive GPU flag alongside no_gpu: true
    let overrides = HardwareOverrides {
        name: Some("node-hostile-gpu-conflict".into()),
        no_gpu: true,
        gpu: Some(true),
        simulate_gpu: true,
        gpu_name: Some("Hostile Supercomputer GPU 9000".into()),
        vram_mb: Some(49152),
        cuda: Some(true),
        ..Default::default()
    };
    let caps = WorkerCapabilities::detect_with_overrides(overrides);

    assert!(!caps.has_gpu, "no_gpu must strictly dominate gpu: Some(true)");
    assert!(!caps.is_simulated_gpu, "no_gpu must strictly dominate simulate_gpu: true");
    assert!(caps.gpu_device_name.is_none(), "no_gpu must strip custom gpu_device_name");
    assert!(caps.vram_mb.is_none(), "no_gpu must strip vram_mb");
    assert!(!caps.has_cuda, "no_gpu must strip has_cuda");
    assert!(!caps.can_execute_gpu());
    assert!(!caps.satisfies(&TaskRequirements::gpu(30)));
}

#[test]
fn test_adversarial_overrides_no_gpu_dominates_environment_variables() {
    // Set host environment variables for CUDA and VRAM
    std::env::set_var("RUSTY_GRID_CUDA", "1");
    std::env::set_var("OXIDE_SWARM_CUDA", "true");
    std::env::set_var("RUSTY_GRID_VRAM_MB", "16000");
    std::env::set_var("OXIDE_SWARM_VRAM_MB", "16000");

    let overrides = HardwareOverrides {
        no_gpu: true,
        ..Default::default()
    };
    let caps = WorkerCapabilities::detect_with_overrides(overrides);

    // Clean up env vars immediately
    std::env::remove_var("RUSTY_GRID_CUDA");
    std::env::remove_var("OXIDE_SWARM_CUDA");
    std::env::remove_var("RUSTY_GRID_VRAM_MB");
    std::env::remove_var("OXIDE_SWARM_VRAM_MB");

    assert!(!caps.has_gpu);
    assert!(caps.vram_mb.is_none(), "no_gpu must override RUSTY_GRID_VRAM_MB");
    assert!(!caps.has_cuda, "no_gpu must override RUSTY_GRID_CUDA");
}

#[test]
fn test_adversarial_overrides_no_cuda_dominates_cuda() {
    let overrides = HardwareOverrides {
        gpu: Some(true),
        vram_mb: Some(8192),
        cuda: Some(true),
        no_cuda: true,
        ..Default::default()
    };
    let caps = WorkerCapabilities::detect_with_overrides(overrides);

    assert!(caps.has_gpu);
    assert_eq!(caps.vram_mb, Some(8192));
    assert!(!caps.has_cuda, "no_cuda MUST strictly dominate cuda: Some(true)");
}

// =========================================================================
// PART 2: Conflicting Overrides — Blender Strict Domination
// =========================================================================

#[test]
fn test_adversarial_overrides_no_blender_dominates_blender_path() {
    let overrides = HardwareOverrides {
        name: Some("node-no-blender-path".into()),
        no_blender: true,
        blender_path: Some(r"C:\Program Files\Blender Foundation\Blender 4.5\blender.exe".into()),
        ..Default::default()
    };
    let caps = WorkerCapabilities::detect_with_overrides(overrides);

    assert!(
        !caps.has_blender,
        "no_blender MUST strictly force has_blender to false even with blender_path set"
    );
    assert!(
        caps.blender_version.is_none(),
        "no_blender MUST strictly force blender_version to None (got {:?})",
        caps.blender_version
    );
}

#[test]
fn test_adversarial_overrides_no_blender_dominates_all_blender_flags_combined() {
    let overrides = HardwareOverrides {
        name: Some("node-hostile-blender-conflict".into()),
        no_blender: true,
        blender: Some(true),
        blender_path: Some(r"C:\Blender\blender.exe".into()),
        blender_version: Some("Blender 4.5.4 LTS".into()),
        ..Default::default()
    };
    let caps = WorkerCapabilities::detect_with_overrides(overrides);

    assert!(!caps.has_blender, "no_blender must dominate blender: Some(true)");
    assert!(
        caps.blender_version.is_none(),
        "no_blender must strip custom blender_version"
    );
}

#[test]
fn test_adversarial_overrides_no_blender_dominates_environment_variables() {
    std::env::set_var("RUSTY_GRID_BLENDER_PATH", r"C:\Blender\blender.exe");
    std::env::set_var("OXIDE_SWARM_BLENDER_PATH", r"C:\Blender\blender.exe");
    std::env::set_var("BLENDER_PATH", r"C:\Blender\blender.exe");

    let overrides = HardwareOverrides {
        no_blender: true,
        ..Default::default()
    };
    let caps = WorkerCapabilities::detect_with_overrides(overrides);

    std::env::remove_var("RUSTY_GRID_BLENDER_PATH");
    std::env::remove_var("OXIDE_SWARM_BLENDER_PATH");
    std::env::remove_var("BLENDER_PATH");

    assert!(!caps.has_blender);
    assert!(caps.blender_version.is_none());
}

#[test]
fn test_adversarial_overrides_blender_explicit_false() {
    let overrides = HardwareOverrides {
        blender: Some(false),
        blender_path: Some(r"C:\Blender\blender.exe".into()),
        blender_version: Some("Blender 4.5".into()),
        ..Default::default()
    };
    let caps = WorkerCapabilities::detect_with_overrides(overrides);

    assert!(!caps.has_blender);
    assert!(caps.blender_version.is_none());
}

#[test]
fn test_adversarial_overrides_blender_nonexistent_path_fails_gracefully() {
    let overrides = HardwareOverrides {
        no_blender: false,
        blender: None,
        blender_path: Some(r"Z:\nonexistent_blender_directory\blender.exe".into()),
        ..Default::default()
    };
    let caps = WorkerCapabilities::detect_with_overrides(overrides);

    // If Blender is not on host, non-existent path must safely resolve to (false, None)
    // or detect system blender if installed, but never crash or panic
    if !caps.has_blender {
        assert!(caps.blender_version.is_none());
    }
}

// =========================================================================
// PART 3: Legacy JSON Payloads — 100% Backward Compatibility Deserialization
// =========================================================================

#[test]
fn test_legacy_worker_capabilities_json_seamless_deserialization() {
    // Simulates a v0.1.0 worker running an older OxideSwarm binary before Milestone 1.
    // The payload lacks has_blender, blender_version, vram_mb, has_cuda, and mobile.
    let legacy_worker_json = r#"{
        "name": "legacy-worker-v0.1.0",
        "cpu_cores": 16,
        "ram_mb": 65536,
        "has_gpu": true,
        "is_simulated_gpu": false,
        "gpu_device_name": "NVIDIA GeForce RTX 4090",
        "tags": ["production", "us-east-1"]
    }"#;

    let caps: WorkerCapabilities =
        serde_json::from_str(legacy_worker_json).expect("Legacy WorkerCapabilities JSON MUST deserialize seamlessly");

    assert_eq!(caps.name, "legacy-worker-v0.1.0");
    assert_eq!(caps.cpu_cores, 16);
    assert_eq!(caps.ram_mb, 65536);
    assert!(caps.has_gpu);
    assert!(!caps.is_simulated_gpu);
    assert_eq!(caps.gpu_device_name.as_deref(), Some("NVIDIA GeForce RTX 4090"));
    assert_eq!(caps.tags, vec!["production", "us-east-1"]);
    assert!(caps.mobile.is_none());

    // Milestone 1 default fields:
    assert!(!caps.has_blender, "Legacy JSON missing has_blender must default to false");
    assert!(caps.blender_version.is_none(), "Legacy JSON missing blender_version must default to None");
    assert!(caps.vram_mb.is_none(), "Legacy JSON missing vram_mb must default to None");
    assert!(!caps.has_cuda, "Legacy JSON missing has_cuda must default to false");
}

#[test]
fn test_legacy_minimal_worker_capabilities_json() {
    // Extreme legacy minimal payload with nulls
    let minimal_json = r#"{
        "name": "bare-worker",
        "cpu_cores": 2,
        "ram_mb": 2048,
        "has_gpu": false,
        "is_simulated_gpu": false,
        "gpu_device_name": null
    }"#;

    let caps: WorkerCapabilities =
        serde_json::from_str(minimal_json).expect("Minimal legacy JSON MUST deserialize cleanly");

    assert_eq!(caps.name, "bare-worker");
    assert_eq!(caps.cpu_cores, 2);
    assert_eq!(caps.ram_mb, 2048);
    assert!(!caps.has_gpu);
    assert!(caps.tags.is_empty());
    assert!(!caps.has_blender);
    assert!(caps.blender_version.is_none());
    assert!(caps.vram_mb.is_none());
    assert!(!caps.has_cuda);
}

#[test]
fn test_legacy_task_result_json_seamless_deserialization() {
    // Simulates an older worker reporting a completed TaskResult.
    // Lacks asset_bytes, device_name, and error fields.
    let wid = Uuid::new_v4();
    let tid = TaskId::new();
    let legacy_result_json = format!(
        r#"{{
            "worker_id": "{wid}",
            "task_id": "{tid}",
            "exit_code": 0,
            "stdout": "Build succeeded: 42 packages compiled\n",
            "stderr": "",
            "execution_time_ms": 1420,
            "is_gpu_executed": false
        }}"#
    );

    let result: TaskResult =
        serde_json::from_str(&legacy_result_json).expect("Legacy TaskResult JSON MUST deserialize seamlessly");

    assert_eq!(result.worker_id, wid);
    assert_eq!(result.task_id, tid);
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.stdout, "Build succeeded: 42 packages compiled\n");
    assert!(result.stderr.is_empty());
    assert_eq!(result.execution_time_ms, 1420);
    assert!(!result.is_gpu_executed);
    assert!(result.device_name.is_none());
    assert!(result.error.is_none());

    // Milestone 1 zero-copy field:
    assert!(
        result.asset_bytes.is_none(),
        "Legacy TaskResult missing asset_bytes must default to None"
    );
}

#[test]
fn test_legacy_worker_message_task_result_wire_protocol_deserialization() {
    // Simulates wire message WorkerMessage::TaskResult from an older worker node
    let wid = Uuid::new_v4();
    let tid = TaskId::new();
    let legacy_wire_json = format!(
        r#"{{
            "type": "TaskResult",
            "worker_id": "{wid}",
            "task_id": "{tid}",
            "exit_code": 0,
            "stdout": "Execution completed",
            "stderr": "",
            "execution_time_ms": 500,
            "is_gpu_executed": false
        }}"#
    );

    let msg: WorkerMessage =
        serde_json::from_str(&legacy_wire_json).expect("Legacy WorkerMessage::TaskResult MUST deserialize seamlessly");

    match msg {
        WorkerMessage::TaskResult {
            worker_id,
            task_id,
            exit_code,
            asset_bytes,
            ..
        } => {
            assert_eq!(worker_id, wid);
            assert_eq!(task_id, tid);
            assert_eq!(exit_code, 0);
            assert!(
                asset_bytes.is_none(),
                "asset_bytes must default to None for older wire message payloads"
            );
        }
        other => panic!("Expected WorkerMessage::TaskResult, got {other:?}"),
    }
}

#[test]
fn test_legacy_worker_message_register_wire_protocol_deserialization() {
    let wid = Uuid::new_v4();
    let legacy_register_json = format!(
        r#"{{
            "type": "Register",
            "worker_id": "{wid}",
            "capabilities": {{
                "name": "legacy-worker-1",
                "cpu_cores": 8,
                "ram_mb": 16384,
                "has_gpu": false,
                "is_simulated_gpu": false,
                "gpu_device_name": null,
                "tags": []
            }}
        }}"#
    );

    let msg: WorkerMessage =
        serde_json::from_str(&legacy_register_json).expect("Legacy Register message MUST deserialize seamlessly");

    match msg {
        WorkerMessage::Register {
            worker_id,
            capabilities,
        } => {
            assert_eq!(worker_id, wid);
            assert_eq!(capabilities.name, "legacy-worker-1");
            assert_eq!(capabilities.cpu_cores, 8);
            assert_eq!(capabilities.ram_mb, 16384);
            assert!(!capabilities.has_blender);
            assert!(capabilities.blender_version.is_none());
            assert!(capabilities.vram_mb.is_none());
            assert!(!capabilities.has_cuda);
        }
        other => panic!("Expected WorkerMessage::Register, got {other:?}"),
    }
}

#[test]
fn test_legacy_task_spec_variants_unaffected() {
    // Existing task variants from before Milestone 1 MUST continue to serialize and deserialize without issue
    let cmd = TaskSpec::Command {
        program: "cargo".into(),
        args: vec!["build".into(), "--release".into()],
        env: HashMap::from([("RUST_LOG".into(), "info".into())]),
        working_dir: None,
        stdin: None,
    };
    let json = serde_json::to_string(&cmd).expect("TaskSpec::Command JSON serialize failed");
    let deser: TaskSpec = serde_json::from_str(&json).expect("TaskSpec::Command JSON deserialize failed");
    assert_eq!(cmd, deser);

    let shell = TaskSpec::ShellScript {
        script: "echo Hello World".into(),
        interpreter: Some("bash".into()),
        env: HashMap::new(),
    };
    let json_shell = serde_json::to_string(&shell).unwrap();
    let deser_shell: TaskSpec = serde_json::from_str(&json_shell).unwrap();
    assert_eq!(shell, deser_shell);
}

#[test]
fn test_new_3d_task_spec_serde_aliases() {
    // Human JSON with tag alias "generative_3d"
    let json_3d = r#"{"type": "generative_3d", "prompt": "A medieval fortress", "quality": "fast"}"#;
    let task_spec: TaskSpec = serde_json::from_str(json_3d).expect("Deserializing generative_3d alias failed");
    match task_spec {
        TaskSpec::Generative3D(spec) => {
            assert_eq!(spec.prompt.as_deref(), Some("A medieval fortress"));
            assert_eq!(spec.quality, "fast");
            assert_eq!(spec.export_texture, true, "export_texture must default to true when omitted in JSON");
        }
        other => panic!("Expected TaskSpec::Generative3D, got {other:?}"),
    }

    // Explicit export_texture: true
    let json_3d_explicit = r#"{"type": "generative_3d", "prompt": "A medieval fortress", "export_texture": true}"#;
    let spec_explicit: TaskSpec = serde_json::from_str(json_3d_explicit).expect("Deserializing explicit export_texture failed");
    match spec_explicit {
        TaskSpec::Generative3D(s) => {
            assert!(s.export_texture, "Explicit export_texture must be true");
        }
        other => panic!("Expected TaskSpec::Generative3D, got {other:?}"),
    }

    // Human JSON with tag alias "3d"
    let json_3d_short = r#"{"type": "3d", "prompt": "low-poly car"}"#;
    let spec_short: TaskSpec = serde_json::from_str(json_3d_short).expect("Deserializing 3d alias failed");
    match spec_short {
        TaskSpec::Generative3D(s) => {
            assert_eq!(s.prompt.as_deref(), Some("low-poly car"));
            assert_eq!(s.quality, "standard");
        }
        other => panic!("Expected TaskSpec::Generative3D, got {other:?}"),
    }

    // Human JSON with tag alias "auto_rig" using byte array
    let json_rig = r#"{"type": "auto_rig", "input_mesh": [1, 2, 3, 4], "rig_preset": "quadruped"}"#;
    let spec_rig: TaskSpec = serde_json::from_str(json_rig).expect("Deserializing auto_rig alias failed");
    match spec_rig {
        TaskSpec::AutoRig(s) => {
            assert_eq!(s.input_mesh.as_slice(), &[1, 2, 3, 4]);
            assert_eq!(s.rig_preset, "quadruped");
            assert_eq!(s.embed_animations, vec!["idle", "walk", "turnaround"]);
        }
        other => panic!("Expected TaskSpec::AutoRig, got {other:?}"),
    }

    // Human JSON with tag alias "rig" using string/path
    let json_rig_str = r#"{"type": "rig", "input_mesh": "character.glb"}"#;
    let spec_rig_str: TaskSpec = serde_json::from_str(json_rig_str).expect("Deserializing rig alias failed");
    match spec_rig_str {
        TaskSpec::AutoRig(s) => {
            assert_eq!(s.input_mesh.as_slice(), b"character.glb");
            assert_eq!(s.rig_preset, "biped");
        }
        other => panic!("Expected TaskSpec::AutoRig, got {other:?}"),
    }
}

// =========================================================================
// PART 4: Rapid Repeated Capability Detection — Subprocess & Handle Leak Stress
// =========================================================================

#[test]
fn test_rapid_repeated_detection_sequential_leak_and_stability() {
    const ITERATIONS: usize = 200;
    println!("\n=== Starting Rapid Repeated Capability Detection Stress ({ITERATIONS} iterations) ===");

    let initial_handles = get_current_process_handle_count();
    let mut latencies: Vec<Duration> = Vec::with_capacity(ITERATIONS);

    let start_overall = Instant::now();

    for i in 0..ITERATIONS {
        let t0 = Instant::now();
        // Alternate between default auto-detection and custom override permutations
        let caps = if i % 4 == 0 {
            WorkerCapabilities::detect(None, None, false)
        } else if i % 4 == 1 {
            WorkerCapabilities::detect_with_overrides(HardwareOverrides {
                no_gpu: true,
                cuda: Some(true),
                vram_mb: Some(16000),
                ..Default::default()
            })
        } else if i % 4 == 2 {
            WorkerCapabilities::detect_with_overrides(HardwareOverrides {
                no_blender: true,
                blender_path: Some(r"C:\Blender\blender.exe".into()),
                ..Default::default()
            })
        } else {
            WorkerCapabilities::detect_with_overrides(HardwareOverrides {
                gpu: Some(true),
                vram_mb: Some(8192),
                cuda: Some(true),
                blender: Some(true),
                blender_version: Some("Blender 4.5.4 LTS".into()),
                ..Default::default()
            })
        };

        let elapsed = t0.elapsed();
        latencies.push(elapsed);

        // Sanity assertions per iteration
        assert!(caps.cpu_cores >= 1);
        assert!(caps.ram_mb > 0);
        if i % 4 == 1 {
            assert!(!caps.has_gpu);
            assert!(caps.vram_mb.is_none());
            assert!(!caps.has_cuda);
        }
        if i % 4 == 2 {
            assert!(!caps.has_blender);
            assert!(caps.blender_version.is_none());
        }

        // Subprocess hanging guard: detection MUST NOT hang
        assert!(
            elapsed < Duration::from_millis(1500),
            "Detection call #{i} exceeded 1500ms safety threshold (took {:?})",
            elapsed
        );
    }

    let total_duration = start_overall.elapsed();
    let final_handles = get_current_process_handle_count();

    // Compute empirical stats
    let total_millis: f64 = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).sum();
    let mean_ms = total_millis / ITERATIONS as f64;
    let min_ms = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).fold(f64::INFINITY, f64::min);
    let max_ms = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).fold(0.0, f64::max);

    latencies.sort();
    let p50_ms = latencies[ITERATIONS * 50 / 100].as_secs_f64() * 1000.0;
    let p95_ms = latencies[ITERATIONS * 95 / 100].as_secs_f64() * 1000.0;
    let p99_ms = latencies[ITERATIONS * 99 / 100].as_secs_f64() * 1000.0;

    println!("Total Time: {total_duration:?}");
    println!("Mean Latency: {mean_ms:.2} ms");
    println!("Min: {min_ms:.2} ms | Max: {max_ms:.2} ms");
    println!("p50: {p50_ms:.2} ms | p95: {p95_ms:.2} ms | p99: {p99_ms:.2} ms");
    println!("Initial Process Handles: {initial_handles} | Final Process Handles: {final_handles}");

    // File / Process Handle Leak Invariant:
    // Any handle count increase across 200 iterations MUST be bounded (<= 5).
    // An unbounded leak would result in delta ~= 200+ handles.
    let handle_growth = final_handles.saturating_sub(initial_handles);
    assert!(
        handle_growth <= 5,
        "File handle leak detected! Handle count grew by {handle_growth} (initial: {initial_handles}, final: {final_handles})"
    );
}

#[tokio::test]
async fn test_rapid_repeated_detection_concurrent_stress() {
    const CONCURRENT_WORKERS: usize = 20;
    const CALLS_PER_WORKER: usize = 10;
    let total_calls = CONCURRENT_WORKERS * CALLS_PER_WORKER;

    println!("\n=== Starting Concurrent Capability Detection Stress ({total_calls} calls across {CONCURRENT_WORKERS} threads) ===");

    let initial_handles = get_current_process_handle_count();
    let t0 = Instant::now();

    let mut join_handles = Vec::with_capacity(CONCURRENT_WORKERS);
    for w in 0..CONCURRENT_WORKERS {
        let handle = tokio::spawn(async move {
            for c in 0..CALLS_PER_WORKER {
                let caps = WorkerCapabilities::detect(
                    Some(format!("concurrent-worker-{w}-{c}")),
                    Some(8),
                    false,
                );
                assert_eq!(caps.name, format!("concurrent-worker-{w}-{c}"));
                assert_eq!(caps.cpu_cores, 8);
                assert!(caps.ram_mb > 0);
            }
        });
        join_handles.push(handle);
    }

    for h in join_handles {
        h.await.expect("Concurrent worker task panicked or failed");
    }

    let elapsed = t0.elapsed();
    let final_handles = get_current_process_handle_count();
    let handle_growth = final_handles.saturating_sub(initial_handles);

    println!("Completed {total_calls} concurrent detections in {elapsed:?}");
    println!("Initial Handles: {initial_handles} | Final Handles: {final_handles} | Growth: {handle_growth}");

    assert!(
        handle_growth <= 15,
        "Concurrent file handle leak detected! Grew by {handle_growth}"
    );
}

// =========================================================================
// PART 5: End-to-End CLI Binary Wire Protocol Verification
// =========================================================================

#[tokio::test]
async fn test_cli_e2e_conflicting_flags_no_gpu_wire_domination() {
    // 1. Bind an ephemeral TCP listener representing MasterServer
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral master listener");
    let master_addr = listener.local_addr().expect("Failed to get local addr");

    // 2. Locate target/debug/oxideswarm.exe binary
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let target_dir = manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target")
        .join("debug");

    #[cfg(windows)]
    let bin_path = target_dir.join("oxideswarm.exe");
    #[cfg(not(windows))]
    let bin_path = target_dir.join("oxideswarm");

    if !bin_path.is_file() {
        eprintln!("Binary not found at {:?}, skipping e2e CLI test", bin_path);
        return;
    }

    // 3. Spawn oxideswarm worker with conflicting flags: --no-gpu alongside --cuda and --vram-mb 16000
    let mut child = std::process::Command::new(&bin_path)
        .arg("worker")
        .arg("--master")
        .arg(master_addr.to_string())
        .arg("--name")
        .arg("worker-cli-no-gpu")
        .arg("--no-gpu")
        .arg("--cuda")
        .arg("--vram-mb")
        .arg("16000")
        .arg("--wire-codec")
        .arg("json")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn oxideswarm worker");

    // 4. Accept the incoming worker connection within 10 seconds
    let accept_res = tokio::time::timeout(Duration::from_secs(10), listener.accept()).await;
    let (socket, _peer_addr) = match accept_res {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => panic!("Listener accept error: {e}"),
        Err(_) => {
            let _ = child.kill();
            panic!("Timed out waiting for oxideswarm worker to connect to master");
        }
    };

    let mut transport = MessageTransport::with_codec(socket, WireCodec::Json);

    // 5. Read the WorkerMessage::Register message emitted by WorkerClient
    let recv_res = tokio::time::timeout(Duration::from_secs(5), transport.recv_msg()).await;
    let register_msg = match recv_res {
        Ok(Ok(Some(msg))) => msg,
        Ok(Ok(None)) => panic!("Connection closed prematurely before Register message"),
        Ok(Err(e)) => panic!("Error reading Register message: {e}"),
        Err(_) => panic!("Timed out waiting for Register message"),
    };

    // Kill child process now that we have the wire message
    let _ = child.kill();
    let _ = child.wait();

    // 6. Verify that on the wire, no_gpu STRICTLY dominated --cuda and --vram-mb
    match register_msg {
        WorkerMessage::Register {
            worker_id,
            capabilities,
        } => {
            println!("Received Register from Worker {worker_id} (name: {})", capabilities.name);
            println!("Advertised Capabilities: {:?}", capabilities);

            assert_eq!(capabilities.name, "worker-cli-no-gpu");
            assert!(
                !capabilities.has_gpu,
                "CLI --no-gpu MUST force has_gpu: false on the wire"
            );
            assert!(
                !capabilities.is_simulated_gpu,
                "CLI --no-gpu MUST force is_simulated_gpu: false on the wire"
            );
            assert!(
                capabilities.gpu_device_name.is_none(),
                "CLI --no-gpu MUST force gpu_device_name: None on the wire"
            );
            assert!(
                capabilities.vram_mb.is_none(),
                "CLI --no-gpu MUST dominate --vram-mb 16000, forcing vram_mb: None on the wire (got {:?})",
                capabilities.vram_mb
            );
            assert!(
                !capabilities.has_cuda,
                "CLI --no-gpu MUST dominate --cuda, forcing has_cuda: false on the wire"
            );
        }
        other => panic!("Expected WorkerMessage::Register, got {other:?}"),
    }
}

#[tokio::test]
async fn test_cli_e2e_conflicting_flags_no_blender_wire_domination() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral master listener");
    let master_addr = listener.local_addr().expect("Failed to get local addr");

    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let target_dir = manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target")
        .join("debug");

    #[cfg(windows)]
    let bin_path = target_dir.join("oxideswarm.exe");
    #[cfg(not(windows))]
    let bin_path = target_dir.join("oxideswarm");

    if !bin_path.is_file() {
        eprintln!("Binary not found at {:?}, skipping e2e CLI test", bin_path);
        return;
    }

    // Spawn with --no-blender alongside --blender-path and --blender-version
    let mut child = std::process::Command::new(&bin_path)
        .arg("worker")
        .arg("--master")
        .arg(master_addr.to_string())
        .arg("--name")
        .arg("worker-cli-no-blender")
        .arg("--no-blender")
        .arg("--blender-path")
        .arg(r"C:\Program Files\Blender Foundation\Blender 4.5\blender.exe")
        .arg("--blender-version")
        .arg("Blender 4.5.4 LTS")
        .arg("--wire-codec")
        .arg("json")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn oxideswarm worker");

    let accept_res = tokio::time::timeout(Duration::from_secs(10), listener.accept()).await;
    let (socket, _peer_addr) = match accept_res {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => panic!("Listener accept error: {e}"),
        Err(_) => {
            let _ = child.kill();
            panic!("Timed out waiting for oxideswarm worker to connect to master");
        }
    };

    let mut transport = MessageTransport::with_codec(socket, WireCodec::Json);

    let recv_res = tokio::time::timeout(Duration::from_secs(5), transport.recv_msg()).await;
    let register_msg = match recv_res {
        Ok(Ok(Some(msg))) => msg,
        Ok(Ok(None)) => panic!("Connection closed prematurely before Register message"),
        Ok(Err(e)) => panic!("Error reading Register message: {e}"),
        Err(_) => panic!("Timed out waiting for Register message"),
    };

    let _ = child.kill();
    let _ = child.wait();

    match register_msg {
        WorkerMessage::Register {
            worker_id,
            capabilities,
        } => {
            println!("Received Register from Worker {worker_id} (name: {})", capabilities.name);
            println!("Advertised Capabilities: {:?}", capabilities);

            assert_eq!(capabilities.name, "worker-cli-no-blender");
            assert!(
                !capabilities.has_blender,
                "CLI --no-blender MUST dominate --blender-path, forcing has_blender: false on wire"
            );
            assert!(
                capabilities.blender_version.is_none(),
                "CLI --no-blender MUST dominate --blender-version, forcing blender_version: None on wire"
            );
        }
        other => panic!("Expected WorkerMessage::Register, got {other:?}"),
    }
}
