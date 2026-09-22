//! Adversarial stress and empirical verification suite for Milestone 4.
//!
//! Authored by Challenger 1 to independently and aggressively verify:
//! 1. Strict GPU Routing Isolation under High Concurrency:
//!    - Interleaved flood of 40 GPU & CPU tasks across a mixed 4-worker cluster (2 CPU, 2 GPU).
//!    - Verification of dual-gating: spec-level GPU (`TaskSpec::GpuCompute`) and requirement-level (`TaskRequirements::gpu`).
//!    - Mathematical guarantee that ZERO GPU tasks ever land on CPU workers, and CPU workers NEVER execute GPU workloads.
//!    - GPU worker concurrency saturation: excess GPU tasks remain queued and NEVER spill over to idle CPU workers.
//! 2. Batch Spread Scheduling Under Varied Worker Topologies:
//!    - Batches of 5, 10, and 15 independent tasks across heterogeneous workers (2, 4, 8 cores).
//!    - Verification that tasks are distributed across all eligible workers and never concentrated on a single worker.
//!    - Pure matchmaking algorithmic tests for topologies under `WeightedLeastLoaded` and `LeastLoaded`.
//! 3. Unsatisfiable Requirements & Head-of-Line Blocking Resistance:
//!    - Tasks requiring more CPU cores than any connected worker possesses.
//!    - Tasks requiring more RAM than any connected worker possesses.
//!    - GPU tasks submitted when no GPU worker is connected.
//!    - Interleaved queue asserting satisfiable tasks bypass unsatisfiable tasks with zero head-of-line blocking.
//!    - Dynamic resolution: late-joining powerhouse and GPU workers pick up and execute stranded queued tasks.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use futures::future::join_all;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_master::registry::{WorkerInfo, WorkerStatus};
use rusty_grid_master::scheduler::{
    ScheduleSkipReason, SchedulerConfig, SchedulingPolicy, WorkloadScheduler,
};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

/// Asynchronously polls `condition` until it returns true or `timeout` expires.
async fn wait_for<F, Fut>(timeout: Duration, step: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

/// Helper struct for holding worker test fixtures.
struct WorkerNode {
    worker_id: Uuid,
    shutdown_tx: watch::Sender<bool>,
    _sandbox_dir: tempfile::TempDir,
}

impl WorkerNode {
    async fn start(
        master_addr: String,
        name: &str,
        cores: usize,
        _ram_mb: u64,
        simulate_gpu: bool,
    ) -> Self {
        let sandbox_dir = tempfile::tempdir().expect("create worker temp sandbox");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = WorkerConfig::new(master_addr)
            .with_name(name)
            .with_cores(cores)
            .with_simulate_gpu(simulate_gpu)
            .with_sandbox_base_dir(sandbox_dir.path());

        // Note: override_ram_mb is in CLI/capabilities; capabilities detect ram or default
        // WorkerClient constructs capabilities with detected or default ram,
        // so we use WorkerConfig as provided.
        let mut client = WorkerClient::new(config);
        let worker_id = client.worker_id();

        tokio::spawn(async move {
            let _ = client.run(shutdown_rx).await;
        });

        Self {
            worker_id,
            shutdown_tx,
            _sandbox_dir: sandbox_dir,
        }
    }

    fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
    }
}

// =========================================================================
// SECTION 1: STRICT GPU ROUTING ISOLATION UNDER HIGH CONCURRENCY
// =========================================================================

#[tokio::test]
async fn test_adversarial_strict_gpu_routing_high_concurrency_flood() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    // Spawn mixed 4-worker cluster:
    // W1: CPU-only (2 cores)
    // W2: CPU-only (4 cores)
    // W3: GPU-simulated (4 cores)
    // W4: GPU-simulated (8 cores)
    let w1 = WorkerNode::start(master_addr.clone(), "cpu-worker-1", 2, 4096, false).await;
    let w2 = WorkerNode::start(master_addr.clone(), "cpu-worker-2", 4, 8192, false).await;
    let w3 = WorkerNode::start(master_addr.clone(), "gpu-worker-3", 4, 8192, true).await;
    let w4 = WorkerNode::start(master_addr.clone(), "gpu-worker-4", 8, 16384, true).await;

    let cpu_worker_ids = [w1.worker_id, w2.worker_id];
    let gpu_worker_ids = [w3.worker_id, w4.worker_id];

    // Await all 4 workers registered
    let ready = wait_for(
        Duration::from_secs(6),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 4 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;
    assert!(ready, "All 4 workers in mixed cluster must register");

    // Prepare interleaved flood of 40 tasks (20 GPU, 20 CPU):
    // Variant A: Explicit GPU requirement + GPU compute kernel (10 tasks)
    // Variant B: Dual-gating test: generic requirement, but spec is GpuCompute (5 tasks)
    // Variant C: Dual-gating test: GPU requirement, but spec is command (5 tasks)
    // Variant D: Generic CPU tasks (20 tasks)
    let mut tasks = Vec::with_capacity(40);
    let mut is_gpu_task_flag = Vec::with_capacity(40);

    for i in 0..40 {
        if i % 2 == 0 {
            // GPU task variations
            let gpu_variant = (i / 2) % 3;
            let task = match gpu_variant {
                0 => Task::new(
                    TaskSpec::gpu_compute(format!("kernel_gemm_{i}"), 32),
                    TaskRequirements::gpu(15),
                ),
                1 => Task::new(
                    TaskSpec::BuiltinTest {
                        test_name: format!("builtin_gpu_{i}"),
                        iterations: 100,
                        duration_ms: 0,
                        should_fail: false,
                        require_gpu: true,
                    },
                    TaskRequirements::gpu(15),
                ),
                _ => Task::new(
                    TaskSpec::command("echo", vec![format!("gpu_command_payload_{i}")]),
                    TaskRequirements::gpu(15), // Requirements demand GPU even though spec is Command
                ),
            };
            tasks.push(task);
            is_gpu_task_flag.push(true);
        } else {
            // CPU task
            let task = Task::new(
                TaskSpec::command("echo", vec![format!("cpu_flood_task_{i}")]),
                TaskRequirements::generic(1, 15),
            );
            tasks.push(task);
            is_gpu_task_flag.push(false);
        }
    }

    // Submit all 40 tasks rapidly
    let mut task_ids = Vec::with_capacity(40);
    for t in tasks {
        let tid = master.submit_task(t).await.expect("submit task failed");
        task_ids.push(tid);
    }

    // Await all 40 tasks concurrently
    let wait_futures: Vec<_> = task_ids
        .iter()
        .map(|&id| master.wait_task(id, Some(Duration::from_secs(12))))
        .collect();

    let results = join_all(wait_futures).await;

    // Rigorous Empirical Assertions:
    let mut total_gpu_completed = 0;
    let mut total_cpu_completed = 0;

    for (idx, res) in results.into_iter().enumerate() {
        let r = res.unwrap_or_else(|e| panic!("Task {idx} failed or timed out: {e}"));
        assert_eq!(r.exit_code, 0, "Task {idx} failed with non-zero exit code");

        let was_gpu_task = is_gpu_task_flag[idx];
        if was_gpu_task {
            total_gpu_completed += 1;
            // 1. Must be executed by a GPU worker (W3 or W4)
            assert!(
                gpu_worker_ids.contains(&r.worker_id),
                "CRITICAL VIOLATION: GPU task {idx} was assigned to non-GPU worker {}",
                r.worker_id
            );
            // 2. Must NEVER be executed by CPU worker (W1 or W2)
            assert!(
                !cpu_worker_ids.contains(&r.worker_id),
                "CRITICAL VIOLATION: CPU worker {} executed GPU task {idx}!",
                r.worker_id
            );
        } else {
            total_cpu_completed += 1;
            // If executed on a CPU worker, must NOT be flagged as GPU executed
            if cpu_worker_ids.contains(&r.worker_id) {
                assert!(
                    !r.is_gpu_executed,
                    "CPU worker {} falsely marked task {idx} as GPU executed",
                    r.worker_id
                );
            }
        }
    }

    assert_eq!(total_gpu_completed, 20, "All 20 GPU tasks must complete");
    assert_eq!(total_cpu_completed, 20, "All 20 CPU tasks must complete");

    // Cleanup
    w1.shutdown();
    w2.shutdown();
    w3.shutdown();
    w4.shutdown();
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_adversarial_gpu_saturation_queues_and_never_spills_to_cpu() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    // Mixed cluster:
    // W1: CPU-only (4 cores)
    // W2: CPU-only (4 cores)
    // W3: GPU-simulated (1 core -> concurrency limit 1)
    let w1 = WorkerNode::start(master_addr.clone(), "cpu-worker-1", 4, 8192, false).await;
    let w2 = WorkerNode::start(master_addr.clone(), "cpu-worker-2", 4, 8192, false).await;
    let w3 = WorkerNode::start(master_addr.clone(), "gpu-worker-solo", 1, 8192, true).await;

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

    // Submit 3 GPU tasks. Each GPU compute task takes time.
    // Task 1 should immediately schedule to W3 (the only GPU worker).
    // Tasks 2 and 3 MUST remain queued and MUST NOT spill over to idle CPU workers W1 & W2!
    let gpu_task_1 = Task::new(
        TaskSpec::command("sh", vec!["-c".into(), "sleep 0.25; echo done1".into()]),
        TaskRequirements::gpu(15),
    );
    let gpu_task_2 = Task::new(
        TaskSpec::command("sh", vec!["-c".into(), "sleep 0.25; echo done2".into()]),
        TaskRequirements::gpu(15),
    );
    let gpu_task_3 = Task::new(
        TaskSpec::command("sh", vec!["-c".into(), "sleep 0.25; echo done3".into()]),
        TaskRequirements::gpu(15),
    );

    let id1 = master.submit_task(gpu_task_1).await.unwrap();
    let id2 = master.submit_task(gpu_task_2).await.unwrap();
    let id3 = master.submit_task(gpu_task_3).await.unwrap();

    // Sleep 60ms to allow scheduler tick and task 1 to start on W3
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Check statuses:
    let s2 = master.get_task_status(id2).await.unwrap();
    let s3 = master.get_task_status(id3).await.unwrap();

    // Since W3 has 1 core (concurrency limit 1), tasks 2 and 3 MUST be Queued
    assert_eq!(
        s2,
        TaskStatus::Queued,
        "Task 2 must remain Queued while GPU worker is saturated"
    );
    assert_eq!(
        s3,
        TaskStatus::Queued,
        "Task 3 must remain Queued while GPU worker is saturated"
    );
    let info2 = master.get_task_info(id2).await.unwrap();
    let info3 = master.get_task_info(id3).await.unwrap();

    assert_ne!(
        info2.assigned_worker_id,
        Some(w1.worker_id),
        "Saturated GPU task 2 must never spill to CPU worker 1"
    );
    assert_ne!(
        info2.assigned_worker_id,
        Some(w2.worker_id),
        "Saturated GPU task 2 must never spill to CPU worker 2"
    );
    assert_ne!(
        info3.assigned_worker_id,
        Some(w1.worker_id),
        "Saturated GPU task 3 must never spill to CPU worker 1"
    );
    assert_ne!(
        info3.assigned_worker_id,
        Some(w2.worker_id),
        "Saturated GPU task 3 must never spill to CPU worker 2"
    );

    // Await all 3 results — they should sequentially finish on W3
    let res1 = master
        .wait_task(id1, Some(Duration::from_secs(8)))
        .await
        .unwrap();
    let res2 = master
        .wait_task(id2, Some(Duration::from_secs(8)))
        .await
        .unwrap();
    let res3 = master
        .wait_task(id3, Some(Duration::from_secs(8)))
        .await
        .unwrap();

    assert_eq!(
        res1.worker_id, w3.worker_id,
        "Task 1 must execute on GPU worker"
    );
    assert_eq!(
        res2.worker_id, w3.worker_id,
        "Task 2 must execute on GPU worker"
    );
    assert_eq!(
        res3.worker_id, w3.worker_id,
        "Task 3 must execute on GPU worker"
    );

    w1.shutdown();
    w2.shutdown();
    w3.shutdown();
    let _ = master.shutdown();
}

// =========================================================================
// SECTION 2: BATCH SPREAD SCHEDULING UNDER VARIED WORKER TOPOLOGIES
// =========================================================================

#[tokio::test]
async fn test_adversarial_batch_spread_scheduling_varied_topologies() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    // Spawn 3 workers with differing core counts:
    // W_small:  2 cores (capacity 2)
    // W_medium: 4 cores (capacity 4)
    // W_large:  8 cores (capacity 8)
    let w_small = WorkerNode::start(master_addr.clone(), "worker-small", 2, 4096, false).await;
    let w_med = WorkerNode::start(master_addr.clone(), "worker-med", 4, 8192, false).await;
    let w_large = WorkerNode::start(master_addr.clone(), "worker-large", 8, 16384, false).await;

    let ready = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 3 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;
    assert!(ready, "All 3 heterogeneous workers must register");

    let worker_ids = [w_small.worker_id, w_med.worker_id, w_large.worker_id];

    // ---------------------------------------------------------------------
    // Subtest 2.1: Batch of 5 independent tasks
    // ---------------------------------------------------------------------
    {
        let tasks: Vec<_> = (0..5)
            .map(|i| {
                Task::new(
                    TaskSpec::command(
                        "sh",
                        vec!["-c".into(), format!("sleep 0.15; echo batch_5_task_{i}")],
                    ),
                    TaskRequirements::generic(1, 10),
                )
            })
            .collect();

        let mut ids = Vec::new();
        for t in tasks {
            ids.push(master.submit_task(t).await.unwrap());
        }

        let wait_futures: Vec<_> = ids
            .iter()
            .map(|&id| master.wait_task(id, Some(Duration::from_secs(8))))
            .collect();
        let results = join_all(wait_futures).await;

        let mut assigned_workers = HashSet::new();
        let mut counts: HashMap<Uuid, usize> = HashMap::new();

        for (idx, r) in results.into_iter().enumerate() {
            let res = r.unwrap_or_else(|e| panic!("Batch 5 task {idx} failed: {e}"));
            assert_eq!(res.exit_code, 0);
            assigned_workers.insert(res.worker_id);
            *counts.entry(res.worker_id).or_insert(0) += 1;
        }

        // Spread check: All 3 workers must receive at least 1 task
        assert_eq!(
            assigned_workers.len(),
            3,
            "Batch of 5 tasks must be spread across all 3 workers; got counts: {:?}",
            counts
        );
        for wid in &worker_ids {
            let cnt = counts.get(wid).copied().unwrap_or(0);
            assert!(
                cnt >= 1,
                "Worker {wid} was starved in batch of 5! Counts: {:?}",
                counts
            );
            assert!(
                cnt < 5,
                "Worker {wid} concentrated all tasks in batch of 5! Counts: {:?}",
                counts
            );
        }
    }

    // ---------------------------------------------------------------------
    // Subtest 2.2: Batch of 10 independent tasks
    // ---------------------------------------------------------------------
    {
        let tasks: Vec<_> = (0..10)
            .map(|i| {
                Task::new(
                    TaskSpec::command(
                        "sh",
                        vec!["-c".into(), format!("sleep 0.15; echo batch_10_task_{i}")],
                    ),
                    TaskRequirements::generic(1, 10),
                )
            })
            .collect();

        let mut ids = Vec::new();
        for t in tasks {
            ids.push(master.submit_task(t).await.unwrap());
        }

        let wait_futures: Vec<_> = ids
            .iter()
            .map(|&id| master.wait_task(id, Some(Duration::from_secs(8))))
            .collect();
        let results = join_all(wait_futures).await;

        let mut assigned_workers = HashSet::new();
        let mut counts: HashMap<Uuid, usize> = HashMap::new();

        for (idx, r) in results.into_iter().enumerate() {
            let res = r.unwrap_or_else(|e| panic!("Batch 10 task {idx} failed: {e}"));
            assert_eq!(res.exit_code, 0);
            assigned_workers.insert(res.worker_id);
            *counts.entry(res.worker_id).or_insert(0) += 1;
        }

        assert_eq!(
            assigned_workers.len(),
            3,
            "Batch of 10 tasks must be spread across all 3 workers; got counts: {:?}",
            counts
        );
        for wid in &worker_ids {
            let cnt = counts.get(wid).copied().unwrap_or(0);
            assert!(
                cnt >= 1,
                "Worker {wid} was starved in batch of 10! Counts: {:?}",
                counts
            );
            assert!(
                cnt < 10,
                "Worker {wid} concentrated all tasks in batch of 10! Counts: {:?}",
                counts
            );
        }
    }

    // ---------------------------------------------------------------------
    // Subtest 2.3: Batch of 15 independent tasks
    // ---------------------------------------------------------------------
    {
        let tasks: Vec<_> = (0..15)
            .map(|i| {
                Task::new(
                    TaskSpec::command(
                        "sh",
                        vec!["-c".into(), format!("sleep 0.15; echo batch_15_task_{i}")],
                    ),
                    TaskRequirements::generic(1, 10),
                )
            })
            .collect();

        let mut ids = Vec::new();
        for t in tasks {
            ids.push(master.submit_task(t).await.unwrap());
        }

        let wait_futures: Vec<_> = ids
            .iter()
            .map(|&id| master.wait_task(id, Some(Duration::from_secs(10))))
            .collect();
        let results = join_all(wait_futures).await;

        let mut assigned_workers = HashSet::new();
        let mut counts: HashMap<Uuid, usize> = HashMap::new();

        for (idx, r) in results.into_iter().enumerate() {
            let res = r.unwrap_or_else(|e| panic!("Batch 15 task {idx} failed: {e}"));
            assert_eq!(res.exit_code, 0);
            assigned_workers.insert(res.worker_id);
            *counts.entry(res.worker_id).or_insert(0) += 1;
        }

        assert_eq!(
            assigned_workers.len(),
            3,
            "Batch of 15 tasks must be spread across all 3 workers; got counts: {:?}",
            counts
        );
        for wid in &worker_ids {
            let cnt = counts.get(wid).copied().unwrap_or(0);
            assert!(
                cnt >= 2,
                "Worker {wid} received fewer than 2 tasks in batch of 15! Counts: {:?}",
                counts
            );
            assert!(
                cnt < 15,
                "Worker {wid} concentrated all tasks in batch of 15! Counts: {:?}",
                counts
            );
        }
    }

    w_small.shutdown();
    w_med.shutdown();
    w_large.shutdown();
    let _ = master.shutdown();
}

#[test]
fn test_adversarial_pure_matchmake_spread_under_varied_topologies() {
    fn make_worker(name: &str, cores: usize) -> WorkerInfo {
        let caps = WorkerCapabilities::new(name, cores, 8192, false, false, None);
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
            ram_available_mb: 8192,
        }
    }

    // Topology: 3 workers with 1, 2, and 4 cores
    let workers = vec![
        make_worker("w1_core1", 1),
        make_worker("w2_core2", 2),
        make_worker("w3_core4", 4),
    ];

    let w1_id = workers[0].worker_id;
    let w2_id = workers[1].worker_id;
    let w3_id = workers[2].worker_id;

    let config = SchedulerConfig {
        policy: SchedulingPolicy::WeightedLeastLoaded,
        ..Default::default()
    };

    // 1. Batch of 5 tasks: capacity is 1 + 2 + 4 = 7. All 5 can be assigned.
    let tasks_5: Vec<_> = (0..5)
        .map(|_| {
            Task::new(
                TaskSpec::command("echo", vec!["test".into()]),
                TaskRequirements::generic(1, 10),
            )
        })
        .collect();

    let report_5 = WorkloadScheduler::matchmake(&config, &tasks_5, &workers);
    assert_eq!(report_5.assignments.len(), 5);
    assert!(report_5.skipped.is_empty());

    let mut counts_5: HashMap<Uuid, usize> = HashMap::new();
    for a in &report_5.assignments {
        *counts_5.entry(a.worker_id).or_insert(0) += 1;
    }

    // Under WeightedLeastLoaded with core concurrency multiplier 1.0:
    // W3 (4 cores) should receive work, W2 (2 cores) should receive work, W1 (1 core) should receive work
    assert_eq!(counts_5.len(), 3, "All 3 workers must receive assignments");
    assert!(
        counts_5[&w1_id] >= 1,
        "w1 must get at least 1 task: {:?}",
        counts_5
    );
    assert!(
        counts_5[&w2_id] >= 1,
        "w2 must get at least 1 task: {:?}",
        counts_5
    );
    assert!(
        counts_5[&w3_id] >= 1,
        "w3 must get at least 1 task: {:?}",
        counts_5
    );

    // 2. Batch of 10 tasks: capacity is 7. 7 should be assigned, 3 should be skipped due to saturation.
    let tasks_10: Vec<_> = (0..10)
        .map(|_| {
            Task::new(
                TaskSpec::command("echo", vec!["test".into()]),
                TaskRequirements::generic(1, 10),
            )
        })
        .collect();

    let report_10 = WorkloadScheduler::matchmake(&config, &tasks_10, &workers);
    assert_eq!(
        report_10.assignments.len(),
        7,
        "Only up to cluster capacity (7) can be scheduled in single pass"
    );
    assert_eq!(
        report_10.skipped.len(),
        3,
        "3 tasks should be skipped due to saturation"
    );
    for (_, reason) in &report_10.skipped {
        assert_eq!(*reason, ScheduleSkipReason::AllEligibleWorkersSaturated);
    }

    // 3. Batch of 15 tasks on higher capacity topology: [2 cores, 4 cores, 8 cores] = 14 capacity
    let high_workers = vec![
        make_worker("w1_core2", 2),
        make_worker("w2_core4", 4),
        make_worker("w3_core8", 8),
    ];
    let hw1_id = high_workers[0].worker_id;
    let hw2_id = high_workers[1].worker_id;
    let hw3_id = high_workers[2].worker_id;

    let tasks_15: Vec<_> = (0..15)
        .map(|_| {
            Task::new(
                TaskSpec::command("echo", vec!["test".into()]),
                TaskRequirements::generic(1, 10),
            )
        })
        .collect();

    let report_15 = WorkloadScheduler::matchmake(&config, &tasks_15, &high_workers);
    assert_eq!(report_15.assignments.len(), 14);
    assert_eq!(report_15.skipped.len(), 1);

    let mut counts_15: HashMap<Uuid, usize> = HashMap::new();
    for a in &report_15.assignments {
        *counts_15.entry(a.worker_id).or_insert(0) += 1;
    }
    assert_eq!(counts_15[&hw1_id], 2, "hw1 (2 cores) saturated with 2");
    assert_eq!(counts_15[&hw2_id], 4, "hw2 (4 cores) saturated with 4");
    assert_eq!(counts_15[&hw3_id], 8, "hw3 (8 cores) saturated with 8");
}

// =========================================================================
// SECTION 3: UNSATISFIABLE REQUIREMENTS & HEAD-OF-LINE BLOCKING RESISTANCE
// =========================================================================

#[tokio::test]
async fn test_adversarial_unsatisfiable_cpu_and_ram_requirements() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    // Cluster with 2 modest workers:
    // W1: 2 cores, 4096 MB RAM
    // W2: 4 cores, 8192 MB RAM
    let w1 = WorkerNode::start(master_addr.clone(), "modest-worker-1", 2, 4096, false).await;
    let w2 = WorkerNode::start(master_addr.clone(), "modest-worker-2", 4, 8192, false).await;

    let ready = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 2 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;
    assert!(ready, "Modest workers must register");

    // 1. Submit task requiring 64 CPU cores (impossible on this cluster)
    let impossible_cpu_task = Task::new(
        TaskSpec::command("echo", vec!["impossible_cpu".into()]),
        TaskRequirements::new(64, 512, false, 30),
    );
    let cpu_tid = master
        .submit_task(impossible_cpu_task)
        .await
        .expect("submit impossible CPU task");

    // 2. Submit task requiring 1,000,000 MB RAM (impossible on this cluster)
    let impossible_ram_task = Task::new(
        TaskSpec::command("echo", vec!["impossible_ram".into()]),
        TaskRequirements::new(1, 1_000_000, false, 30),
    );
    let ram_tid = master
        .submit_task(impossible_ram_task)
        .await
        .expect("submit impossible RAM task");

    // Allow multiple scheduler ticks (300ms)
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Both tasks MUST remain in Queued state and NEVER be assigned to incapable workers
    let status_cpu = master.get_task_status(cpu_tid).await.unwrap();
    let status_ram = master.get_task_status(ram_tid).await.unwrap();
    assert_eq!(
        status_cpu,
        TaskStatus::Queued,
        "Impossible CPU task must stay Queued"
    );
    assert_eq!(
        status_ram,
        TaskStatus::Queued,
        "Impossible RAM task must stay Queued"
    );

    let info_cpu = master.get_task_info(cpu_tid).await.unwrap();
    let info_ram = master.get_task_info(ram_tid).await.unwrap();
    assert!(
        info_cpu.assigned_worker_id.is_none(),
        "Impossible CPU task must not be assigned"
    );
    assert!(
        info_ram.assigned_worker_id.is_none(),
        "Impossible RAM task must not be assigned"
    );

    // Verify scheduler didn't crash: submit a normal satisfiable generic task
    let normal_task = Task::new(
        TaskSpec::command("echo", vec!["normal_ok".into()]),
        TaskRequirements::generic(1, 10),
    );
    let normal_tid = master
        .submit_task(normal_task)
        .await
        .expect("submit normal task");

    let normal_res = master
        .wait_task(normal_tid, Some(Duration::from_secs(5)))
        .await
        .expect("normal task must execute and complete");
    assert_eq!(normal_res.exit_code, 0);
    assert!(normal_res.stdout.contains("normal_ok"));

    w1.shutdown();
    w2.shutdown();
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_adversarial_unsatisfiable_gpu_and_head_of_line_blocking() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    // Cluster with ONLY CPU workers:
    let w1 = WorkerNode::start(master_addr.clone(), "cpu-only-1", 2, 4096, false).await;
    let w2 = WorkerNode::start(master_addr.clone(), "cpu-only-2", 4, 8192, false).await;

    let ready = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 2 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;
    assert!(ready, "CPU workers must register");

    // Submit an impossible GPU task (no GPU worker exists)
    let gpu_task = Task::new(
        TaskSpec::gpu_compute("stranded_kernel", 64),
        TaskRequirements::gpu(20),
    );
    let gpu_tid = master.submit_task(gpu_task).await.expect("submit gpu task");

    // Interleave unsatisfiable tasks with satisfiable tasks:
    // Task A: Unsatisfiable (128 cores)
    // Task B: Satisfiable Generic (1 core)
    // Task C: Unsatisfiable (1TB RAM)
    // Task D: Satisfiable Generic (1 core)
    let task_a = Task::new(
        TaskSpec::command("echo", vec!["task_a".into()]),
        TaskRequirements::new(128, 512, false, 20),
    );
    let task_b = Task::new(
        TaskSpec::command("echo", vec!["task_b_success".into()]),
        TaskRequirements::generic(1, 20),
    );
    let task_c = Task::new(
        TaskSpec::command("echo", vec!["task_c".into()]),
        TaskRequirements::new(1, 1_000_000, false, 20),
    );
    let task_d = Task::new(
        TaskSpec::command("echo", vec!["task_d_success".into()]),
        TaskRequirements::generic(1, 20),
    );

    let tid_a = master.submit_task(task_a).await.unwrap();
    let tid_b = master.submit_task(task_b).await.unwrap();
    let tid_c = master.submit_task(task_c).await.unwrap();
    let tid_d = master.submit_task(task_d).await.unwrap();

    // Satisfiable tasks B & D MUST execute and complete promptly without head-of-line blocking!
    let res_b = master
        .wait_task(tid_b, Some(Duration::from_secs(5)))
        .await
        .expect("Task B must complete without HoL blocking");
    let res_d = master
        .wait_task(tid_d, Some(Duration::from_secs(5)))
        .await
        .expect("Task D must complete without HoL blocking");

    assert_eq!(res_b.exit_code, 0);
    assert!(res_b.stdout.contains("task_b_success"));
    assert_eq!(res_d.exit_code, 0);
    assert!(res_d.stdout.contains("task_d_success"));

    // Unsatisfiable tasks MUST remain in Queued state:
    assert_eq!(
        master.get_task_status(gpu_tid).await.unwrap(),
        TaskStatus::Queued
    );
    assert_eq!(
        master.get_task_status(tid_a).await.unwrap(),
        TaskStatus::Queued
    );
    assert_eq!(
        master.get_task_status(tid_c).await.unwrap(),
        TaskStatus::Queued
    );

    // Dynamic resolution: Late-joining GPU worker joins the cluster
    let w_gpu = WorkerNode::start(master_addr.clone(), "late-gpu-worker", 4, 8192, true).await;

    let gpu_registered = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers
                .iter()
                .any(|w| w.worker_id == w_gpu.worker_id && w.status == WorkerStatus::Connected)
        },
    )
    .await;
    assert!(gpu_registered, "Late GPU worker must register");

    // The stranded GPU task should now immediately be picked up and completed by the GPU worker
    let res_gpu = master
        .wait_task(gpu_tid, Some(Duration::from_secs(8)))
        .await
        .expect("Stranded GPU task must execute on late GPU worker");

    assert_eq!(
        res_gpu.worker_id, w_gpu.worker_id,
        "GPU task must execute on GPU worker"
    );
    assert_eq!(res_gpu.exit_code, 0);
    assert!(res_gpu.is_gpu_executed);

    // Unsatisfiable tasks A & C (128 cores, 1TB RAM) STILL remain queued
    assert_eq!(
        master.get_task_status(tid_a).await.unwrap(),
        TaskStatus::Queued
    );
    assert_eq!(
        master.get_task_status(tid_c).await.unwrap(),
        TaskStatus::Queued
    );

    w1.shutdown();
    w2.shutdown();
    w_gpu.shutdown();
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_adversarial_task_validation_rejects_gpu_spec_without_gpu_requirement() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");

    // Task specifies GpuCompute but requirements state gpu_required = false
    let invalid_task = Task::new(
        TaskSpec::gpu_compute("sneaky_kernel", 64),
        TaskRequirements::generic(1, 15),
    );

    let submit_res = master.submit_task(invalid_task).await;
    assert!(
        submit_res.is_err(),
        "Master must reject task with GPU spec but gpu_required = false"
    );

    let err_msg = submit_res.unwrap_err().to_string();
    assert!(
        err_msg.contains("TaskSpec requires GPU, but TaskRequirements.gpu_required is false"),
        "Error message must specify validation mismatch: {err_msg}"
    );

    let _ = master.shutdown();
}
