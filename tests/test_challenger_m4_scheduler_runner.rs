//! Adversarial Empirical Stress Test Suite for Milestone 4 (Features F17 & F18)
//!
//! Evaluates:
//! 1. Feature F17: Master Hardware-Aware 3D Matchmaking (`WorkloadScheduler`)
//!    - Extreme VRAM boundary conditions (0 MB, 6144 MB, ties, simulated vs physical GPU)
//!    - Negative battery and thermal throttling stress (0% battery discharging, thermal_throttled tag)
//!    - Capability mismatch stress (Blender required vs missing, diagnostic error propagation)
//!    - High-throughput matchmaking burst (100 concurrent 3D tasks against heterogeneous pool)
//! 2. Feature F18: TaskRunner 3D Execution & Incremental Checkpoints (`TaskRunner`)
//!    - Checkpoint integrity (strict ascending sequence 1..4 without drop or disorder)
//!    - glTF 2.0 binary asset container compliance (magic, chunk offsets, 4-byte buffer alignment)
//!    - Malformed and corrupted mesh input handling (graceful WorkerError::AutoRig, no panic/zombies)

use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

use rusty_grid_core::capabilities::{MobileCapabilities, WorkerCapabilities};
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{
    AutoRigSpec, Generative3DSpec, Task, TaskRequirements, TaskSpec,
};
use rusty_grid_master::registry::{WorkerInfo, WorkerStatus};
use rusty_grid_master::scheduler::{
    ScheduleSkipReason, SchedulerConfig, SchedulingPolicy, WorkloadScheduler,
};
use rusty_grid_worker::runner::{RunnerConfig, TaskRunner, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_SUCCESS};

/// Helper to construct test worker metadata with explicit capabilities.
fn make_test_worker(
    name: &str,
    cores: usize,
    ram_mb: u64,
    gpu: bool,
    simulated_gpu: bool,
    vram_mb: Option<u64>,
    has_blender: bool,
    tags: Vec<String>,
    mobile: Option<MobileCapabilities>,
) -> WorkerInfo {
    let mut caps = WorkerCapabilities::new(name, cores, ram_mb, gpu, simulated_gpu, None);
    caps.vram_mb = vram_mb;
    caps.has_blender = has_blender;
    caps.tags = tags;
    caps.mobile = mobile;

    WorkerInfo {
        worker_id: Uuid::new_v4(),
        session_id: 1,
        capabilities: caps,
        status: WorkerStatus::Connected,
        last_heartbeat_timestamp: 1000,
        registered_timestamp: 1000,
        active_tasks: 0,
        remote_addr: "127.0.0.1:9000".into(),
        cpu_usage_pct: 0.0,
        ram_available_mb: ram_mb,
    }
}

/// Helper to create a local in-memory TaskRunner for testing execution.
fn make_test_runner(gpu: bool) -> TaskRunner {
    let caps = WorkerCapabilities::new("challenger-worker-1", 4, 8192, gpu, false, None);
    TaskRunner::new(Uuid::new_v4(), caps, RunnerConfig::default())
}

// =========================================================================================
// 1. SCHEDULER ADVERSARIAL STRESS TESTS (FEATURE F17)
// =========================================================================================

#[test]
fn test_stress_vram_extreme_boundaries() {
    println!("\n=== [STRESS 1.1] Extreme VRAM Boundary Conditions ===");

    let config = SchedulerConfig::default();

    let w_zero_vram = make_test_worker("w-zero-vram", 8, 16384, true, false, Some(0), false, vec![], None);
    let w_2gb_vram = make_test_worker("w-2gb-vram", 8, 16384, true, false, Some(2048), false, vec![], None);
    let w_4gb_vram = make_test_worker("w-4gb-vram", 8, 16384, true, false, Some(4096), false, vec![], None);
    let w_boundary_minus_1 = make_test_worker("w-6143mb", 8, 16384, true, false, Some(6143), false, vec![], None);
    let w_exact_boundary = make_test_worker("w-6144mb", 8, 16384, true, false, Some(6144), false, vec![], None);
    let w_large_vram = make_test_worker("w-8192mb", 8, 16384, true, false, Some(8192), false, vec![], None);

    let workers = vec![
        w_zero_vram.clone(),
        w_2gb_vram.clone(),
        w_4gb_vram.clone(),
        w_boundary_minus_1.clone(),
        w_exact_boundary.clone(),
        w_large_vram.clone(),
    ];

    // High quality 3D neural generation requires 6144 MB VRAM
    let high_vram_task = Task::new(
        TaskSpec::Generative3D(
            Generative3DSpec::new_prompt("high-fidelity 3d asset".to_string())
                .with_quality("high")
                .with_engine("triposr"),
        ),
        TaskRequirements::generic(2, 60),
    );

    let report = WorkloadScheduler::matchmake(&config, &[high_vram_task.clone()], &workers);

    assert_eq!(report.assignments.len(), 1, "Must assign to an eligible high-VRAM node");
    let assigned_id = report.assignments[0].worker_id;

    // Strict boundary assertion: Worker with 0 VRAM, 2048 MB, 4096 MB, or 6143 MB MUST NEVER be assigned
    assert_ne!(assigned_id, w_zero_vram.worker_id, "Worker with 0 VRAM must be strictly rejected");
    assert_ne!(assigned_id, w_2gb_vram.worker_id, "Worker with 2048 MB VRAM must be rejected for 6144 MB task");
    assert_ne!(assigned_id, w_4gb_vram.worker_id, "Worker with 4096 MB VRAM must be rejected for 6144 MB task");
    assert_ne!(assigned_id, w_boundary_minus_1.worker_id, "Worker with 6143 MB VRAM must be rejected for 6144 MB task (off-by-one check)");

    // Worker with exact 6144 MB or 8192 MB is valid
    assert!(
        assigned_id == w_exact_boundary.worker_id || assigned_id == w_large_vram.worker_id,
        "Assignment must land on worker with >= 6144 MB VRAM"
    );

    // Test Simulated GPU vs Physical GPU:
    // Simulated GPU has vram_mb: None, but is_simulated_gpu: true
    let w_simulated = make_test_worker("w-simulated", 8, 32768, true, true, None, false, vec![], None);
    let sim_pool = vec![w_zero_vram.clone(), w_simulated.clone()];
    let sim_report = WorkloadScheduler::matchmake(&config, &[high_vram_task.clone()], &sim_pool);

    assert_eq!(sim_report.assignments.len(), 1);
    assert_eq!(
        sim_report.assignments[0].worker_id, w_simulated.worker_id,
        "Simulated GPU with fallback RAM must be eligible when no physical GPU meets VRAM requirement"
    );

    // Test fail_unfulfillable_tasks when highest VRAM in cluster is 4096 MB
    let mut fail_config = config.clone();
    fail_config.fail_unfulfillable_tasks = true;
    let underpowered_pool = vec![w_zero_vram, w_2gb_vram, w_4gb_vram];

    let fail_report = WorkloadScheduler::matchmake(&fail_config, &[high_vram_task], &underpowered_pool);
    assert_eq!(fail_report.assignments.len(), 0, "No assignments when unfulfillable");
    assert_eq!(fail_report.unfulfillable.len(), 1, "Must be tagged as unfulfillable");
    assert!(
        fail_report.unfulfillable[0].1.contains("Cluster lacks workers with required VRAM (6144 MB)"),
        "Diagnostic message must accurately cite the required 6144 MB threshold"
    );

    println!("-> [PASS] Extreme VRAM boundary conditions and simulated GPU gating verified.");
}

#[test]
fn test_stress_vram_ties_and_determinism() {
    println!("\n=== [STRESS 1.2] VRAM Ties and Deterministic Tie-Breaking ===");

    let config = SchedulerConfig {
        policy: SchedulingPolicy::LeastLoaded,
        ..Default::default()
    };

    // 4 workers with identical specs and identical 8192 MB VRAM
    let mut workers = Vec::new();
    for i in 1..=4 {
        workers.push(make_test_worker(
            &format!("worker-tie-{i}"),
            8,
            16384,
            true,
            false,
            Some(8192),
            false,
            vec![],
            None,
        ));
    }

    // Sort workers by ID so order is fixed
    workers.sort_by_key(|w| w.worker_id);

    // 8 identical standard 3D tasks (requires 4096 MB VRAM)
    let tasks: Vec<Task> = (0..8)
        .map(|i| {
            Task::new(
                TaskSpec::Generative3D(
                    Generative3DSpec::new_prompt(format!("asset_{i}"))
                        .with_quality("standard")
                        .with_engine("triposr"),
                ),
                TaskRequirements::generic(2, 30),
            )
        })
        .collect();

    // Run matchmaking 10 consecutive times to verify determinism and absence of race conditions
    let first_report = WorkloadScheduler::matchmake(&config, &tasks, &workers);
    assert_eq!(first_report.assignments.len(), 8);

    // Verify perfect 2-task distribution across all 4 equal workers
    let mut load_counts: HashMap<Uuid, usize> = HashMap::new();
    for a in &first_report.assignments {
        *load_counts.entry(a.worker_id).or_insert(0) += 1;
    }
    assert_eq!(load_counts.len(), 4, "Tasks must be spread across all 4 workers");
    for (_, count) in &load_counts {
        assert_eq!(*count, 2, "Each worker must receive exactly 2 tasks");
    }

    for iteration in 2..=10 {
        let repeat_report = WorkloadScheduler::matchmake(&config, &tasks, &workers);
        assert_eq!(
            first_report.assignments, repeat_report.assignments,
            "Iteration {iteration} assignment mismatch: matchmaking must be 100% deterministic"
        );
    }

    println!("-> [PASS] VRAM ties broken deterministically; 100% repeatable distribution verified.");
}

#[test]
fn test_stress_negative_battery_and_thermal_throttling() {
    println!("\n=== [STRESS 1.3] Negative Battery & Thermal Throttling Stress ===");

    let config = SchedulerConfig::default();

    // Worker 1: 0% battery while discharging
    let w_empty_battery = make_test_worker(
        "mobile-empty-battery",
        4,
        4096,
        true,
        false,
        Some(4096),
        false,
        vec![],
        Some(MobileCapabilities {
            os_version: "Android 14".to_string(),
            soc_model: "Snapdragon 8 Gen 2".to_string(),
            battery_pct: Some(0),
            is_charging: Some(false),
            thermal_throttled: false,
        }),
    );

    // Worker 2: 5% battery while discharging
    let w_low_battery = make_test_worker(
        "mobile-low-battery",
        4,
        4096,
        true,
        false,
        Some(4096),
        false,
        vec![],
        Some(MobileCapabilities {
            os_version: "Android 14".to_string(),
            soc_model: "Snapdragon 8 Gen 2".to_string(),
            battery_pct: Some(5),
            is_charging: Some(false),
            thermal_throttled: false,
        }),
    );

    // Worker 3: Tagged with "thermal_throttled"
    let w_tagged_thermal = make_test_worker(
        "mobile-tagged-thermal",
        8,
        8192,
        true,
        false,
        Some(4096),
        false,
        vec!["thermal_throttled".to_string()],
        None,
    );

    // Worker 4: Mobile struct reporting thermal_throttled: true
    let w_struct_thermal = make_test_worker(
        "mobile-struct-thermal",
        8,
        8192,
        true,
        false,
        Some(4096),
        false,
        vec![],
        Some(MobileCapabilities {
            os_version: "Android 14".to_string(),
            soc_model: "Dimensity 9300".to_string(),
            battery_pct: Some(95),
            is_charging: Some(true),
            thermal_throttled: true,
        }),
    );

    // Worker 5: Healthy desktop node
    let w_healthy_desktop = make_test_worker(
        "desktop-healthy",
        16,
        32768,
        true,
        false,
        Some(8192),
        true,
        vec![],
        None,
    );

    let all_workers = vec![
        w_empty_battery.clone(),
        w_low_battery.clone(),
        w_tagged_thermal.clone(),
        w_struct_thermal.clone(),
        w_healthy_desktop.clone(),
    ];

    // Submit a burst of 10 Generative3D and 10 AutoRig tasks
    let mut tasks = Vec::new();
    for i in 0..10 {
        tasks.push(Task::new(
            TaskSpec::Generative3D(
                Generative3DSpec::new_prompt(format!("3d_item_{i}"))
                    .with_quality("fast")
                    .with_engine("sdf"),
            ),
            TaskRequirements::generic(1, 30),
        ));
        tasks.push(Task::new(
            TaskSpec::AutoRig(AutoRigSpec::new(b"dummy_mesh_bytes".to_vec(), "biped")),
            TaskRequirements::generic(1, 30),
        ));
    }
    assert_eq!(tasks.len(), 20);

    let report = WorkloadScheduler::matchmake(&config, &tasks, &all_workers);

    // 16-core healthy worker saturates at 16 tasks with core_concurrency_multiplier=1.0
    assert_eq!(report.assignments.len(), 16, "Healthy worker accepts tasks up to its 16-core saturation limit");
    assert_eq!(report.skipped.len(), 4, "Remaining 4 tasks must be skipped rather than spilled to compromised nodes");

    for (_, reason) in &report.skipped {
        assert_eq!(*reason, ScheduleSkipReason::AllEligibleWorkersSaturated);
    }

    let mut false_assignment_count = 0;
    for a in &report.assignments {
        if a.worker_id == w_empty_battery.worker_id
            || a.worker_id == w_low_battery.worker_id
            || a.worker_id == w_tagged_thermal.worker_id
            || a.worker_id == w_struct_thermal.worker_id
        {
            false_assignment_count += 1;
        }
        assert_eq!(
            a.worker_id, w_healthy_desktop.worker_id,
            "Task {:?} was erroneously assigned to compromised worker {:?}",
            a.task_id, a.worker_id
        );
    }

    assert_eq!(
        false_assignment_count, 0,
        "CRITICAL: Found {false_assignment_count} false assignments of 3D workloads to throttled/low-battery workers! Expected 0."
    );

    // Now test with core_concurrency_multiplier = 2.0 to absorb all 20 tasks
    let config_high_cap = SchedulerConfig {
        core_concurrency_multiplier: 2.0,
        ..Default::default()
    };
    let report_full = WorkloadScheduler::matchmake(&config_high_cap, &tasks, &all_workers);
    assert_eq!(report_full.assignments.len(), 20, "All 20 tasks assigned under high capacity");
    for a in &report_full.assignments {
        assert_eq!(a.worker_id, w_healthy_desktop.worker_id, "All 20 tasks must land on healthy desktop");
    }

    // Adversarial verification: when healthy desktop is absent, ALL 20 3D tasks must be skipped
    let throttled_only_pool = vec![
        w_empty_battery,
        w_low_battery,
        w_tagged_thermal,
        w_struct_thermal,
    ];
    let throttled_report = WorkloadScheduler::matchmake(&config, &tasks, &throttled_only_pool);
    assert_eq!(
        throttled_report.assignments.len(),
        0,
        "Zero 3D tasks must be assigned when only throttled/battery-starved workers exist"
    );
    assert_eq!(throttled_report.skipped.len(), 20);

    println!("-> [PASS] 0% false assignments to thermal-throttled or low-battery workers verified.");
}

#[test]
fn test_stress_capability_mismatch_and_unfulfillable_tasks() {
    println!("\n=== [STRESS 1.4] Capability Mismatch & Blender Provider Stress ===");

    let mut config = SchedulerConfig::default();
    config.fail_unfulfillable_tasks = true;

    let w_builtin_only_1 = make_test_worker("w-pure-rust-1", 4, 8192, false, false, None, false, vec![], None);
    let w_builtin_only_2 = make_test_worker("w-pure-rust-2", 4, 8192, false, false, None, false, vec![], None);
    let pool_no_blender = vec![w_builtin_only_1.clone(), w_builtin_only_2.clone()];

    // AutoRig task demanding preferred_provider = "blender"
    let blender_rig_task = Task::new(
        TaskSpec::AutoRig(
            AutoRigSpec::new(b"mesh_data".to_vec(), "biped")
                .with_provider("blender"),
        ),
        TaskRequirements::generic(1, 30),
    );

    let report = WorkloadScheduler::matchmake(&config, &[blender_rig_task.clone()], &pool_no_blender);

    assert_eq!(report.assignments.len(), 0, "Blender task must not be assigned to non-Blender workers");
    assert_eq!(report.unfulfillable.len(), 1, "Must be flagged as unfulfillable");
    assert_eq!(report.unfulfillable[0].0, blender_rig_task.id);
    assert_eq!(
        report.unfulfillable[0].1,
        "Cluster lacks workers with Blender capability",
        "Diagnostic message must explicitly identify missing Blender capability"
    );

    // Verify graceful skipping when fail_unfulfillable_tasks is disabled
    let mut soft_config = config.clone();
    soft_config.fail_unfulfillable_tasks = false;
    let soft_report = WorkloadScheduler::matchmake(&soft_config, &[blender_rig_task], &pool_no_blender);
    assert_eq!(soft_report.assignments.len(), 0);
    assert_eq!(soft_report.unfulfillable.len(), 0);
    assert_eq!(soft_report.skipped.len(), 1);

    // Now introduce 1 Blender worker into the cluster
    let mut w_blender = make_test_worker("w-blender", 8, 16384, false, false, None, true, vec![], None);
    w_blender.capabilities.has_blender = true;

    let pool_with_blender = vec![w_builtin_only_1.clone(), w_builtin_only_2.clone(), w_blender.clone()];

    let blender_task_2 = Task::new(
        TaskSpec::AutoRig(
            AutoRigSpec::new(b"mesh_data_2".to_vec(), "biped")
                .with_provider("blender"),
        ),
        TaskRequirements::generic(1, 30),
    );

    let assigned_report = WorkloadScheduler::matchmake(&config, &[blender_task_2], &pool_with_blender);
    assert_eq!(assigned_report.assignments.len(), 1);
    assert_eq!(
        assigned_report.assignments[0].worker_id, w_blender.worker_id,
        "Blender task must route directly to the Blender-capable node"
    );

    // Builtin auto-rig task must preserve the Blender worker when equivalent non-Blender workers exist
    let builtin_task = Task::new(
        TaskSpec::AutoRig(
            AutoRigSpec::new(b"mesh_data_builtin".to_vec(), "biped")
                .with_provider("builtin"),
        ),
        TaskRequirements::generic(1, 30),
    );

    let preserve_report = WorkloadScheduler::matchmake(&config, &[builtin_task], &pool_with_blender);
    assert_eq!(preserve_report.assignments.len(), 1);
    assert_ne!(
        preserve_report.assignments[0].worker_id, w_blender.worker_id,
        "Builtin task must prefer non-Blender nodes to preserve specialized Blender worker"
    );

    println!("-> [PASS] Capability mismatch diagnostic and Blender worker preservation verified.");
}

#[test]
fn test_stress_high_throughput_matchmaking_burst_100_tasks() {
    println!("\n=== [STRESS 1.5] High-Throughput Matchmaking Burst: 100 3D Tasks ===");

    let config = SchedulerConfig {
        policy: SchedulingPolicy::WeightedLeastLoaded,
        core_concurrency_multiplier: 4.0, // High concurrency for burst
        ..Default::default()
    };

    // Heterogeneous 10-node cluster pool
    let w1_gpu_high = make_test_worker("w1-gpu-high", 16, 32768, true, false, Some(16384), false, vec![], None);
    let w2_gpu_mid = make_test_worker("w2-gpu-mid", 8, 16384, true, false, Some(8192), false, vec![], None);
    let w3_gpu_budget = make_test_worker("w3-gpu-budget", 4, 8192, true, false, Some(4096), false, vec![], None);
    let w4_gpu_entry = make_test_worker("w4-gpu-entry", 4, 8192, true, false, Some(2048), false, vec![], None);
    let w5_simulated = make_test_worker("w5-simulated", 8, 16384, true, true, None, false, vec![], None);
    let w6_blender = make_test_worker("w6-blender", 8, 16384, true, false, Some(8192), true, vec![], None);
    let w7_cpu_builder = make_test_worker("w7-cpu-only", 32, 65536, false, false, None, false, vec![], None);
    let w8_mobile_healthy = make_test_worker("w8-mobile-healthy", 8, 8192, true, false, Some(4096), false, vec![], Some(MobileCapabilities {
        os_version: "Android 14".to_string(),
        soc_model: "Snapdragon 8 Gen 3".to_string(),
        battery_pct: Some(80),
        is_charging: Some(true),
        thermal_throttled: false,
    }));
    let w9_mobile_dead_bat = make_test_worker("w9-mobile-dead-bat", 8, 8192, true, false, Some(4096), false, vec![], Some(MobileCapabilities {
        os_version: "Android 14".to_string(),
        soc_model: "Exynos 2400".to_string(),
        battery_pct: Some(3),
        is_charging: Some(false),
        thermal_throttled: false,
    }));
    let w10_mobile_throttled = make_test_worker("w10-mobile-throttled", 8, 8192, true, false, Some(4096), false, vec!["thermal_throttled".to_string()], None);

    let cluster = vec![
        w1_gpu_high.clone(),
        w2_gpu_mid.clone(),
        w3_gpu_budget.clone(),
        w4_gpu_entry.clone(),
        w5_simulated.clone(),
        w6_blender.clone(),
        w7_cpu_builder.clone(),
        w8_mobile_healthy.clone(),
        w9_mobile_dead_bat.clone(),
        w10_mobile_throttled.clone(),
    ];

    // Assemble 100 heterogeneous 3D tasks
    let mut tasks = Vec::with_capacity(100);
    // 25 High-VRAM 3D neural generation (requires 6144 MB)
    for i in 0..25 {
        tasks.push(Task::new(
            TaskSpec::Generative3D(
                Generative3DSpec::new_prompt(format!("high_hero_{i}"))
                    .with_quality("high")
                    .with_engine("triposr"),
            ),
            TaskRequirements::generic(2, 60),
        ));
    }
    // 25 Standard-VRAM 3D neural generation (requires 4096 MB)
    for i in 0..25 {
        tasks.push(Task::new(
            TaskSpec::Generative3D(
                Generative3DSpec::new_prompt(format!("standard_prop_{i}"))
                    .with_quality("standard")
                    .with_engine("triposr"),
            ),
            TaskRequirements::generic(1, 45),
        ));
    }
    // 25 Fast-VRAM Procedural SDF generation (0 VRAM required)
    for i in 0..25 {
        tasks.push(Task::new(
            TaskSpec::Generative3D(
                Generative3DSpec::new_prompt(format!("sdf_item_{i}"))
                    .with_quality("fast")
                    .with_engine("sdf"),
            ),
            TaskRequirements::generic(1, 15),
        ));
    }
    // 15 Blender-demanding AutoRig tasks
    for _i in 0..15 {
        tasks.push(Task::new(
            TaskSpec::AutoRig(
                AutoRigSpec::new(b"blender_mesh_binary".to_vec(), "biped")
                    .with_provider("blender"),
            ),
            TaskRequirements::generic(1, 30),
        ));
    }
    // 10 Builtin pure-Rust AutoRig tasks
    for _i in 0..10 {
        tasks.push(Task::new(
            TaskSpec::AutoRig(
                AutoRigSpec::new(b"builtin_mesh_binary".to_vec(), "biped")
                    .with_provider("builtin"),
            ),
            TaskRequirements::generic(1, 30),
        ));
    }
    assert_eq!(tasks.len(), 100);

    let start_t = Instant::now();
    let report = WorkloadScheduler::matchmake(&config, &tasks, &cluster);
    let duration = start_t.elapsed();

    println!("   Matchmaking 100 tasks against 10 heterogeneous nodes took: {:?}", duration);
    assert!(duration < Duration::from_millis(50), "Matchmaking 100 tasks must take < 50ms (took {:?})", duration);

    // Verify constraint invariants across all assignments:
    let mut high_vram_assigned = 0;
    let mut blender_rig_assigned = 0;
    let total_assigned = report.assignments.len();

    let cluster_map: HashMap<Uuid, &WorkerInfo> = cluster.iter().map(|w| (w.worker_id, w)).collect();

    for a in &report.assignments {
        let worker = cluster_map.get(&a.worker_id).expect("Assigned worker must exist");

        // 1. Invariant: Dead battery and thermal throttled nodes MUST NEVER receive 3D tasks
        assert_ne!(worker.worker_id, w9_mobile_dead_bat.worker_id, "Dead battery worker received a task!");
        assert_ne!(worker.worker_id, w10_mobile_throttled.worker_id, "Thermal throttled worker received a task!");

        // 2. Invariant: High VRAM tasks (6144 MB) must only go to workers with >= 6144 MB VRAM or simulated
        if let TaskSpec::Generative3D(ref spec) = a.task.spec {
            if spec.quality == "high" && spec.engine_preference.as_deref() != Some("sdf") {
                let vram = worker.capabilities.vram_mb.unwrap_or(0);
                assert!(
                    vram >= 6144 || worker.capabilities.is_simulated_gpu,
                    "High VRAM task assigned to node with only {vram} MB VRAM"
                );
                high_vram_assigned += 1;
            }
        }

        // 3. Invariant: Blender AutoRig tasks must only go to Blender-capable nodes
        if let TaskSpec::AutoRig(ref spec) = a.task.spec {
            if spec.preferred_provider.as_deref() == Some("blender") {
                assert!(worker.capabilities.has_blender, "Blender task assigned to non-Blender node!");
                blender_rig_assigned += 1;
            }
        }
    }

    // Verify determinism: Run matchmake again with identical input
    let repeat_report = WorkloadScheduler::matchmake(&config, &tasks, &cluster);
    assert_eq!(
        report.assignments, repeat_report.assignments,
        "High-throughput burst matchmaking was not 100% deterministic between runs!"
    );

    println!(
        "   Assignments summary: total={}, high_vram={}, blender_rig={}, skipped={}",
        total_assigned,
        high_vram_assigned,
        blender_rig_assigned,
        report.skipped.len()
    );
    println!("-> [PASS] 100-task burst completed with zero constraint violations and 100% determinism.");
}

// =========================================================================================
// 2. TASKRUNNER 3D EXECUTION & CHECKPOINT INTEGRITY TESTS (FEATURE F18)
// =========================================================================================

#[tokio::test]
async fn test_stress_taskrunner_checkpoint_sequence_integrity() {
    println!("\n=== [STRESS 2.1] TaskRunner Checkpoint Sequence Integrity ===");

    // 1. Generative 3D (Tier 0 Procedural SDF) Checkpoint Integrity
    let sdf_spec = Generative3DSpec::new_prompt("adversarial_drone".to_string())
        .with_quality("fast")
        .with_engine("sdf");
    let sdf_task = Task::new(TaskSpec::Generative3D(sdf_spec), TaskRequirements::generic(1, 30));

    let (tx_sdf, mut rx_sdf) = mpsc::channel(16);
    let runner_clone = make_test_runner(false);
    let sdf_task_clone = sdf_task.clone();

    let handle_sdf = tokio::spawn(async move {
        runner_clone
            .execute_task_with_checkpoint(&sdf_task_clone, None, Some(tx_sdf), None, None)
            .await
    });

    let mut sdf_checkpoints = Vec::new();
    while let Some(msg) = rx_sdf.recv().await {
        if let WorkerMessage::Checkpoint { sequence, delta_state, .. } = msg {
            sdf_checkpoints.push((sequence, delta_state));
        }
    }

    let sdf_result = handle_sdf.await.expect("join handle");
    assert!(sdf_result.is_success(), "SDF task must succeed: {:?}", sdf_result.error);
    assert_eq!(sdf_result.exit_code, EXIT_CODE_SUCCESS);

    // Verify strict ascending sequence 1, 2, 3, 4 without drop or disorder
    assert_eq!(sdf_checkpoints.len(), 4, "Must emit exactly 4 checkpoints");
    let seq_numbers: Vec<u64> = sdf_checkpoints.iter().map(|(s, _)| *s).collect();
    assert_eq!(seq_numbers, vec![1, 2, 3, 4], "SDF checkpoints must be strictly sequential 1, 2, 3, 4");

    let expected_sdf_stages = ["sdf_geometry", "manifold_uv", "texture_baking", "gltf_pack"];
    let expected_sdf_progress = [0.25, 0.50, 0.75, 1.00];

    for (i, (seq, delta_bytes)) in sdf_checkpoints.iter().enumerate() {
        assert_eq!(*seq, (i + 1) as u64);
        let val: serde_json::Value = serde_json::from_slice(delta_bytes.as_slice())
            .expect("Checkpoint delta must be valid JSON");
        assert_eq!(val["stage"], expected_sdf_stages[i]);
        assert_eq!(val["progress"], expected_sdf_progress[i]);
        assert!(val["message"].is_string());
    }

    // 2. AutoRig Pure-Rust Checkpoint Integrity
    // Use the generated GLB from the SDF task as input to AutoRig
    let input_mesh = sdf_result.asset_bytes.expect("SDF must produce asset_bytes");

    let rig_spec = AutoRigSpec::new(input_mesh.as_slice().to_vec(), "biped")
        .with_animations(vec!["idle".to_string()]);
    let rig_task = Task::new(TaskSpec::AutoRig(rig_spec), TaskRequirements::generic(1, 30));

    let (tx_rig, mut rx_rig) = mpsc::channel(16);
    let runner_clone_rig = make_test_runner(false);
    let rig_task_clone = rig_task.clone();

    let handle_rig = tokio::spawn(async move {
        runner_clone_rig
            .execute_task_with_checkpoint(&rig_task_clone, None, Some(tx_rig), None, None)
            .await
    });

    let mut rig_checkpoints = Vec::new();
    while let Some(msg) = rx_rig.recv().await {
        if let WorkerMessage::Checkpoint { sequence, delta_state, .. } = msg {
            rig_checkpoints.push((sequence, delta_state));
        }
    }

    let rig_result = handle_rig.await.expect("join handle");
    assert!(rig_result.is_success(), "AutoRig task must succeed: {:?}", rig_result.error);
    assert_eq!(rig_result.exit_code, EXIT_CODE_SUCCESS);

    assert_eq!(rig_checkpoints.len(), 4, "AutoRig must emit exactly 4 checkpoints");
    let rig_seq_numbers: Vec<u64> = rig_checkpoints.iter().map(|(s, _)| *s).collect();
    assert_eq!(rig_seq_numbers, vec![1, 2, 3, 4], "AutoRig checkpoints must be strictly sequential 1, 2, 3, 4");

    let expected_rig_stages = ["landmark_extraction", "armature_ibm", "geodesic_skinning", "animation_export"];
    let expected_rig_progress = [0.25, 0.50, 0.75, 1.00];

    for (i, (seq, delta_bytes)) in rig_checkpoints.iter().enumerate() {
        assert_eq!(*seq, (i + 1) as u64);
        let val: serde_json::Value = serde_json::from_slice(delta_bytes.as_slice())
            .expect("Checkpoint delta must be valid JSON");
        assert_eq!(val["stage"], expected_rig_stages[i]);
        assert_eq!(val["progress"], expected_rig_progress[i]);
        assert!(val["message"].is_string());
    }

    println!("-> [PASS] Both Generative3D and AutoRig emit strictly monotonic checkpoints (1, 2, 3, 4).");
}

#[tokio::test]
async fn test_stress_gltf_binary_payload_and_alignment() {
    println!("\n=== [STRESS 2.2] Asset Binary Payload & glTF 2.0 Alignment ===");

    let runner = make_test_runner(false);

    // 1. Generate mesh via Tier 0 SDF
    let sdf_spec = Generative3DSpec::new_prompt("torus_mesh".to_string())
        .with_quality("fast")
        .with_engine("sdf");
    let sdf_task = Task::new(TaskSpec::Generative3D(sdf_spec), TaskRequirements::generic(1, 30));
    let sdf_res = runner.execute_task(&sdf_task, None, None).await;

    assert!(sdf_res.is_success());
    let sdf_glb = sdf_res.asset_bytes.expect("Generative3D must produce asset_bytes");

    // 2. Generate rigged mesh via AutoRig
    let rig_spec = AutoRigSpec::new(sdf_glb.as_slice().to_vec(), "biped")
        .with_animations(vec!["idle".to_string(), "walk".to_string(), "turnaround".to_string()]);
    let rig_task = Task::new(TaskSpec::AutoRig(rig_spec), TaskRequirements::generic(1, 30));
    let rig_res = runner.execute_task(&rig_task, None, None).await;

    assert!(rig_res.is_success());
    let rig_glb = rig_res.asset_bytes.expect("AutoRig must produce asset_bytes");

    // Verify binary specs for both GLBs
    for (name, glb_bytes) in [("Generative3D_SDF", &sdf_glb), ("AutoRig_Rigged", &rig_glb)] {
        let glb = glb_bytes.as_slice();
        println!("   Inspecting binary glTF container for: {name} ({} bytes)", glb.len());

        assert!(glb.len() >= 20, "GLB must be at least 20 bytes");

        // Magic: 0x46546C67 ("glTF")
        let magic = u32::from_le_bytes([glb[0], glb[1], glb[2], glb[3]]);
        assert_eq!(magic, 0x46546C67, "{name}: Magic must be 0x46546C67 (glTF)");

        // Version: 2
        let version = u32::from_le_bytes([glb[4], glb[5], glb[6], glb[7]]);
        assert_eq!(version, 2, "{name}: Version must be 2");

        // Total Length in header must match actual length
        let header_len = u32::from_le_bytes([glb[8], glb[9], glb[10], glb[11]]) as usize;
        assert_eq!(header_len, glb.len(), "{name}: Header length must equal exact file length");

        // Chunk 0: JSON
        let chunk0_len = u32::from_le_bytes([glb[12], glb[13], glb[14], glb[15]]) as usize;
        let chunk0_type = u32::from_le_bytes([glb[16], glb[17], glb[18], glb[19]]);
        assert_eq!(chunk0_type, 0x4E4F534A, "{name}: Chunk 0 type must be 0x4E4F534A (JSON)");
        assert_eq!(chunk0_len % 4, 0, "{name}: Chunk 0 length must be aligned to 4 bytes");

        let json_start = 20;
        let json_end = json_start + chunk0_len;
        assert!(json_end <= glb.len(), "{name}: Chunk 0 exceeds container bounds");

        let json_slice = &glb[json_start..json_end];
        let gltf: serde_json::Value = serde_json::from_slice(json_slice)
            .expect("{name}: Chunk 0 must parse into valid glTF 2.0 JSON metadata");

        assert_eq!(gltf["asset"]["version"], "2.0", "{name}: glTF version must be '2.0'");

        // Validate buffer alignment in accessors and bufferViews
        if let Some(buffer_views) = gltf.get("bufferViews").and_then(|v| v.as_array()) {
            for (bv_idx, bv) in buffer_views.iter().enumerate() {
                let byte_offset = bv.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0);
                assert_eq!(
                    byte_offset % 4,
                    0,
                    "{name}: BufferView {bv_idx} byteOffset ({byte_offset}) is not 4-byte aligned!"
                );
            }
        }

        // Chunk 1: BIN
        if json_end < glb.len() {
            let chunk1_offset = json_end;
            assert!(chunk1_offset + 8 <= glb.len(), "{name}: Truncated Chunk 1 header");

            let chunk1_len = u32::from_le_bytes([
                glb[chunk1_offset],
                glb[chunk1_offset + 1],
                glb[chunk1_offset + 2],
                glb[chunk1_offset + 3],
            ]) as usize;
            let chunk1_type = u32::from_le_bytes([
                glb[chunk1_offset + 4],
                glb[chunk1_offset + 5],
                glb[chunk1_offset + 6],
                glb[chunk1_offset + 7],
            ]);

            assert_eq!(chunk1_type, 0x004E4942, "{name}: Chunk 1 type must be 0x004E4942 (BIN)");
            assert_eq!(chunk1_len % 4, 0, "{name}: Chunk 1 length must be aligned to 4 bytes");
            assert_eq!(
                12 + 8 + chunk0_len + 8 + chunk1_len,
                glb.len(),
                "{name}: Total container length must equal sum of header + chunk0 + chunk1"
            );
        }
    }

    println!("-> [PASS] glTF 2.0 headers, chunk offsets, and 4-byte buffer alignment verified.");
}

#[tokio::test]
async fn test_stress_taskrunner_malformed_corrupted_mesh() {
    println!("\n=== [STRESS 2.3] Malformed & Corrupted Mesh Input Handling ===");

    let runner = make_test_runner(false);

    // Subcase 2.3a: Empty mesh bytes
    println!("   Testing Subcase 2.3a: Empty input mesh bytes");
    let empty_spec = AutoRigSpec::new(vec![], "biped");
    let empty_task = Task::new(TaskSpec::AutoRig(empty_spec), TaskRequirements::generic(1, 30));
    let empty_res = runner.execute_task(&empty_task, None, None).await;

    assert!(!empty_res.is_success(), "Empty mesh must fail");
    assert_eq!(empty_res.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(empty_res.asset_bytes.is_none());
    assert!(
        empty_res.error.as_deref().unwrap_or("").contains("Auto-rig error")
            || empty_res.error.as_deref().unwrap_or("").contains("too small"),
        "Error message must provide diagnostic context: {:?}",
        empty_res.error
    );

    // Subcase 2.3b: Arbitrary random noise bytes
    println!("   Testing Subcase 2.3b: Random noise bytes");
    let random_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xAA, 0x55, 0x12, 0x34, 0x89, 0xAB, 0xCD, 0xEF];
    let noise_spec = AutoRigSpec::new(random_bytes, "biped");
    let noise_task = Task::new(TaskSpec::AutoRig(noise_spec), TaskRequirements::generic(1, 30));
    let noise_res = runner.execute_task(&noise_task, None, None).await;

    assert!(!noise_res.is_success(), "Noise bytes must fail gracefully");
    assert_eq!(noise_res.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(noise_res.asset_bytes.is_none());
    assert!(noise_res.error.is_some());

    // Subcase 2.3c: Truncated GLB header (valid magic 'glTF', version 2, but container ends at byte 16)
    println!("   Testing Subcase 2.3c: Truncated GLB header");
    let mut truncated_glb = Vec::new();
    truncated_glb.extend_from_slice(b"glTF");
    truncated_glb.extend_from_slice(&2u32.to_le_bytes()); // version 2
    truncated_glb.extend_from_slice(&100u32.to_le_bytes()); // claims length 100
    truncated_glb.extend_from_slice(&20u32.to_le_bytes()); // chunk 0 len

    let trunc_spec = AutoRigSpec::new(truncated_glb, "biped");
    let trunc_task = Task::new(TaskSpec::AutoRig(trunc_spec), TaskRequirements::generic(1, 30));
    let trunc_res = runner.execute_task(&trunc_task, None, None).await;

    assert!(!trunc_res.is_success(), "Truncated GLB must fail gracefully");
    assert_eq!(trunc_res.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(trunc_res.asset_bytes.is_none());
    assert!(trunc_res.error.is_some());

    // Subcase 2.3d: Corrupted JSON chunk within valid GLB framing
    println!("   Testing Subcase 2.3d: Corrupted JSON chunk inside GLB");
    let bad_json_content = b"{ this is definitely not valid json 1234567890 }";
    let mut bad_json_bytes = bad_json_content.to_vec();
    while bad_json_bytes.len() % 4 != 0 {
        bad_json_bytes.push(b' ');
    }
    let total_len = 12 + 8 + bad_json_bytes.len();

    let mut bad_json_glb = Vec::new();
    bad_json_glb.extend_from_slice(b"glTF");
    bad_json_glb.extend_from_slice(&2u32.to_le_bytes());
    bad_json_glb.extend_from_slice(&(total_len as u32).to_le_bytes());
    bad_json_glb.extend_from_slice(&(bad_json_bytes.len() as u32).to_le_bytes());
    bad_json_glb.extend_from_slice(&0x4E4F534Au32.to_le_bytes()); // "JSON"
    bad_json_glb.extend_from_slice(&bad_json_bytes);

    let bad_json_spec = AutoRigSpec::new(bad_json_glb, "biped");
    let bad_json_task = Task::new(TaskSpec::AutoRig(bad_json_spec), TaskRequirements::generic(1, 30));
    let bad_json_res = runner.execute_task(&bad_json_task, None, None).await;

    assert!(!bad_json_res.is_success(), "Corrupted JSON chunk GLB must fail gracefully");
    assert_eq!(bad_json_res.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(bad_json_res.asset_bytes.is_none());
    assert!(bad_json_res.error.is_some());

    // Subcase 2.3e: Valid glTF JSON but missing meshes/primitives
    println!("   Testing Subcase 2.3e: Valid glTF container with empty meshes");
    let empty_mesh_json = serde_json::json!({
        "asset": { "version": "2.0" },
        "meshes": []
    });
    let mut empty_json_bytes = serde_json::to_vec(&empty_mesh_json).unwrap();
    while empty_json_bytes.len() % 4 != 0 {
        empty_json_bytes.push(b' ');
    }
    let empty_total_len = 12 + 8 + empty_json_bytes.len();

    let mut empty_mesh_glb = Vec::new();
    empty_mesh_glb.extend_from_slice(b"glTF");
    empty_mesh_glb.extend_from_slice(&2u32.to_le_bytes());
    empty_mesh_glb.extend_from_slice(&(empty_total_len as u32).to_le_bytes());
    empty_mesh_glb.extend_from_slice(&(empty_json_bytes.len() as u32).to_le_bytes());
    empty_mesh_glb.extend_from_slice(&0x4E4F534Au32.to_le_bytes());
    empty_mesh_glb.extend_from_slice(&empty_json_bytes);

    let empty_mesh_spec = AutoRigSpec::new(empty_mesh_glb, "biped");
    let empty_mesh_task = Task::new(TaskSpec::AutoRig(empty_mesh_spec), TaskRequirements::generic(1, 30));
    let empty_mesh_res = runner.execute_task(&empty_mesh_task, None, None).await;

    assert!(!empty_mesh_res.is_success(), "glTF with empty meshes must fail gracefully");
    assert_eq!(empty_mesh_res.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(empty_mesh_res.asset_bytes.is_none());
    assert!(empty_mesh_res.error.is_some());

    println!("-> [PASS] All 5 malformed and corrupted mesh conditions handled gracefully without panics.");
}
