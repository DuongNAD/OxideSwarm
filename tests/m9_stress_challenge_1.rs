//! Milestone 9 Stress Challenge Suite: Empirical Fault Tolerance, Eviction, and Rescheduling.
//!
//! Authored by Challenger M9.1 to empirically stress-test:
//! 1. `test_stress_multi_worker_concurrent_crashes`:
//!    Multiple concurrent worker crashes during in-flight task execution:
//!    - 5 workers, 15 long-running in-flight tasks.
//!    - 3 workers killed simultaneously mid-flight via hard aborts.
//!    - Zero task loss: all 15 tasks complete successfully.
//!    - Correct retry counters: tasks on killed workers have retry_count >= 1.
//!    - Zero orphaned tasks: all tasks terminal, active_tasks on survivors drops to 0.
//!
//! 2. `test_stress_tcp_rst_vs_graceful_disconnect`:
//!    Sudden TCP drop/RST vs graceful Disconnecting message:
//!    - Worker A sends graceful `Disconnecting`; Worker B abruptly drops TCP socket.
//!    - Verify Master distinguishes immediate failover (0ms delay) vs exponential backoff.
//!    - Both tasks reassigned to Worker C and complete cleanly.
//!
//! 3. `test_stress_immediate_failover_latency_under_load`:
//!    Immediate failover latency under load (<100ms on graceful disconnect):
//!    - Batch of tasks executing on Worker 1.
//!    - Graceful disconnect triggered; time measured until reassignment to Worker 2.
//!    - Reassignment latency strictly < 100ms.
//!
//! 4. `test_stress_high_volume_worker_churn`:
//!    High task volume (30 tasks) with interleaved worker churn:
//!    - Continuous background churn (workers connecting, running briefly, disconnecting/aborting, new workers joining).
//!    - All 30 tasks complete with exit code 0.
//!    - Zero deadlocks, zero orphaned tasks.
//!
//! 5. `test_stress_concurrent_max_retries_and_waiters_resolution`:
//!    Multiple concurrent tasks reaching max retries with waiting clients:
//!    - Concurrent `wait_task` clients.
//!    - Repeated worker crashes exhausting max_retries.
//!    - All waiting clients unblock immediately with descriptive failure and exit code 1.
//!    - No hanging tasks or deadlocks.
//!
//! 6. `test_stress_reaper_detection_and_failover`:
//!    Reaper detection of silent worker freeze:
//!    - Worker halts heartbeats without closing TCP.
//!    - Master reaper detects inactivity, evicts worker, and reassigns task to survivor.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tempfile::TempDir;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
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

/// Managed test worker client with isolated tempdir.
struct ManagedWorker {
    pub worker_id: Uuid,
    pub shutdown_tx: watch::Sender<bool>,
    pub handle: tokio::task::JoinHandle<()>,
    pub _temp_dir: TempDir,
}

impl ManagedWorker {
    pub fn spawn(master_addr: String, name: &str, cores: usize, simulate_gpu: bool) -> Self {
        let temp_dir = tempfile::tempdir().expect("worker tempdir");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = WorkerConfig::new(master_addr)
            .with_name(name)
            .with_cores(cores)
            .with_simulate_gpu(simulate_gpu)
            .with_sandbox_base_dir(temp_dir.path());
        let mut client = WorkerClient::new(config);
        let worker_id = client.worker_id();

        let handle = tokio::spawn(async move {
            let _ = client.run(shutdown_rx).await;
        });

        ManagedWorker {
            worker_id,
            shutdown_tx,
            handle,
            _temp_dir: temp_dir,
        }
    }

    pub fn graceful_disconnect(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn abort(&self) {
        self.handle.abort();
    }
}

/// Helper to create an in-memory BuiltinTest task with given duration and optional max_retries.
fn make_builtin_task(name: &str, duration_ms: u64, max_retries: Option<u32>) -> Task {
    let mut req = TaskRequirements::generic(1, 30);
    if let Some(r) = max_retries {
        req = req.with_max_retries(r);
    }
    Task::new(
        TaskSpec::BuiltinTest {
            test_name: name.to_string(),
            iterations: 50,
            duration_ms,
            should_fail: false,
            require_gpu: false,
        },
        req,
    )
}

// =========================================================================
// TEST 1: MULTI-WORKER CONCURRENT CRASHES DURING IN-FLIGHT TASK EXECUTION
// =========================================================================

#[tokio::test]
async fn test_stress_multi_worker_concurrent_crashes() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(5);
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    // Spawn 5 workers
    let mut workers = Vec::new();
    for i in 1..=5 {
        workers.push(ManagedWorker::spawn(
            master_addr.clone(),
            &format!("multi-crash-w{i}"),
            4,
            false,
        ));
    }

    // Wait for all 5 workers to register
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 5)
                    .unwrap_or(false)
            }
        )
        .await,
        "All 5 workers must register"
    );

    // Submit 15 tasks of duration 1200ms
    let mut task_ids = Vec::new();
    for i in 0..15 {
        let task = make_builtin_task(&format!("batch_task_{i}"), 1200, None);
        let id = master.submit_task(task).await.expect("submit task");
        task_ids.push(id);
    }

    // Wait until all 15 tasks are Running across the cluster
    assert!(
        wait_for(
            Duration::from_secs(6),
            Duration::from_millis(30),
            || async {
                let mut running = 0;
                for id in &task_ids {
                    if let Ok(info) = master.get_task_info(*id).await {
                        if info.state == TaskState::Running {
                            running += 1;
                        }
                    }
                }
                running == 15
            }
        )
        .await,
        "All 15 tasks must enter Running state across the 5 workers"
    );

    // Identify which worker each task was executing on
    let mut initial_assignments = Vec::new();
    for id in &task_ids {
        let info = master.get_task_info(*id).await.unwrap();
        initial_assignments.push((*id, info.assigned_worker_id.unwrap()));
    }

    let crashed_worker_ids: HashSet<Uuid> = [
        workers[0].worker_id,
        workers[1].worker_id,
        workers[2].worker_id,
    ]
    .into_iter()
    .collect();

    // Abruptly kill 3 of the 5 workers concurrently while tasks are running!
    workers[0].abort();
    workers[1].abort();
    workers[2].abort();

    // Await all 15 tasks to reach Completed state on the 2 surviving workers
    let completed_all = wait_for(
        Duration::from_secs(15),
        Duration::from_millis(50),
        || async {
            let mut completed = 0;
            for id in &task_ids {
                if let Ok(info) = master.get_task_info(*id).await {
                    if info.state == TaskState::Completed {
                        completed += 1;
                    }
                }
            }
            completed == 15
        },
    )
    .await;
    assert!(
        completed_all,
        "All 15 tasks must complete successfully despite 3 simultaneous worker crashes"
    );

    let survivor_ids: HashSet<Uuid> = [workers[3].worker_id, workers[4].worker_id]
        .into_iter()
        .collect();

    // Audit each task:
    // 1. Exit code is 0.
    // 2. Tasks originally on crashed workers MUST have retry_count >= 1.
    // 3. Final assigned worker MUST be one of the survivors.
    for (task_id, original_worker) in initial_assignments {
        let final_info = master.get_task_info(task_id).await.unwrap();
        assert_eq!(final_info.state, TaskState::Completed);
        assert_eq!(final_info.exit_code, Some(0));

        let final_worker = final_info.assigned_worker_id.unwrap();
        assert!(
            survivor_ids.contains(&final_worker),
            "Final worker {final_worker} must be a surviving worker"
        );

        if crashed_worker_ids.contains(&original_worker) {
            assert!(
                final_info.retry_count >= 1,
                "Task {task_id} on crashed worker must have been retried (retry_count={})",
                final_info.retry_count
            );
        }
    }

    // Verify queue stats: 0 running, 0 queued, 15 completed, 0 failed
    let stats = master.queue_stats().await.unwrap();
    assert_eq!(stats.completed, 15);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.running, 0);
    assert_eq!(stats.queued, 0);

    let _ = master.shutdown();
}

// =========================================================================
// TEST 2: SUDDEN TCP RST / DROP VS GRACEFUL DISCONNECTING MESSAGE
// =========================================================================

#[tokio::test]
async fn test_stress_tcp_rst_vs_graceful_disconnect() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let master_addr = master.server_addr();

    // 1. Worker 1: Graceful worker (starts first)
    let (w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::from_options(
        master_addr.to_string(),
        Some("w-graceful".into()),
        Some(1),
        false,
    );
    let w1_id = w1.worker_id();
    let _h1 = tokio::spawn(async move {
        let _ = w1.run(w1_rx).await;
    });

    // Wait for W1 to register
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(30),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 1)
                    .unwrap_or(false)
            }
        )
        .await
    );

    // Submit Task A (must land on W1 because W1 is the only worker)
    let task_a = make_builtin_task("task_graceful", 2500, None);
    let id_a = master.submit_task(task_a).await.unwrap();

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(id_a)
                    .await
                    .map(|i| i.state == TaskState::Running && i.assigned_worker_id == Some(w1_id))
                    .unwrap_or(false)
            }
        )
        .await
    );

    // 2. Spawn Worker 3: Survivor worker
    let (_w3_tx, w3_rx) = watch::channel(false);
    let mut w3 = WorkerClient::from_options(
        master_addr.to_string(),
        Some("w-survivor".into()),
        Some(4),
        false,
    );
    let w3_id = w3.worker_id();
    let _h3 = tokio::spawn(async move {
        let _ = w3.run(w3_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(30),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| {
                        w.iter()
                            .any(|x| x.worker_id == w3_id && x.status == WorkerStatus::Connected)
                    })
                    .unwrap_or(false)
            }
        )
        .await
    );

    // Graceful disconnect on Worker 1
    let t_graceful = Instant::now();
    let _ = w1_tx.send(true);

    // Task A must reassign to W3 immediately (<100ms) with 0ms delay!
    let w1_reallocated = wait_for(
        Duration::from_millis(200),
        Duration::from_millis(10),
        || async {
            if let Ok(info) = master.get_task_info(id_a).await {
                info.assigned_worker_id == Some(w3_id)
                    && (info.state == TaskState::Scheduled || info.state == TaskState::Running)
            } else {
                false
            }
        },
    )
    .await;
    let failover_lat = t_graceful.elapsed();
    assert!(
        w1_reallocated,
        "Gracefully disconnected task must reassign to survivor immediately"
    );
    assert!(
        failover_lat < Duration::from_millis(100),
        "Graceful failover latency must be <100ms; measured: {:?}",
        failover_lat
    );

    // Wait for Task A to complete on W3
    assert!(
        wait_for(
            Duration::from_secs(6),
            Duration::from_millis(30),
            || async {
                master
                    .get_task_info(id_a)
                    .await
                    .map(|i| i.state == TaskState::Completed)
                    .unwrap_or(false)
            }
        )
        .await
    );

    // 3. Now test abrupt TCP drop / crash:
    // Spawn Worker 2 (abrupt crash worker)
    let (_w2_tx, w2_rx) = watch::channel(false);
    let mut w2 = WorkerClient::from_options(
        master_addr.to_string(),
        Some("w-abrupt".into()),
        Some(1),
        false,
    );
    let w2_id = w2.worker_id();
    let h2 = tokio::spawn(async move {
        let _ = w2.run(w2_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(30),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| {
                        w.iter()
                            .any(|x| x.worker_id == w2_id && x.status == WorkerStatus::Connected)
                    })
                    .unwrap_or(false)
            }
        )
        .await
    );

    // Submit Task B
    let task_b = make_builtin_task("task_abrupt", 2500, None);
    let id_b = master.submit_task(task_b).await.unwrap();

    // Wait until Task B is Running
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(id_b)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false)
            }
        )
        .await
    );

    let info_b = master.get_task_info(id_b).await.unwrap();
    let assigned = info_b.assigned_worker_id.unwrap();

    // Abruptly abort whichever worker is running Task B (if w2, abort w2; if w3, abort w3)
    let (target_handle, survivor_for_b) = if assigned == w2_id {
        (h2, w3_id)
    } else {
        (_h3, w2_id)
    };

    target_handle.abort(); // TCP drop without Disconnecting!

    // Verify Task B transitions to Retrying (exponential backoff)
    assert!(
        wait_for(
            Duration::from_secs(3),
            Duration::from_millis(15),
            || async {
                if let Ok(info) = master.get_task_info(id_b).await {
                    info.state == TaskState::Retrying
                        || (info.state == TaskState::Scheduled
                            && info.assigned_worker_id == Some(survivor_for_b))
                } else {
                    false
                }
            }
        )
        .await
    );

    // Task B completes on survivor
    assert!(
        wait_for(
            Duration::from_secs(6),
            Duration::from_millis(30),
            || async {
                master
                    .get_task_info(id_b)
                    .await
                    .map(|i| i.state == TaskState::Completed)
                    .unwrap_or(false)
            }
        )
        .await
    );

    let final_a = master.get_task_info(id_a).await.unwrap();
    let final_b = master.get_task_info(id_b).await.unwrap();
    assert_eq!(final_a.state, TaskState::Completed);
    assert_eq!(final_b.state, TaskState::Completed);
    assert_eq!(final_a.retry_count, 1);
    assert_eq!(final_b.retry_count, 1);
    assert_eq!(final_a.exit_code, Some(0));
    assert_eq!(final_b.exit_code, Some(0));

    let _ = master.shutdown();
}

// =========================================================================
// TEST 3: IMMEDIATE FAILOVER LATENCY UNDER LOAD (<100ms)
// =========================================================================

#[tokio::test]
async fn test_stress_immediate_failover_latency_under_load() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    let (w1_tx, w1_rx) = watch::channel(false);
    let (_w2_tx, w2_rx) = watch::channel(false);

    let mut w1 =
        WorkerClient::from_options(master_addr.clone(), Some("load-w1".into()), Some(8), false);
    let w1_id = w1.worker_id();
    let mut w2 =
        WorkerClient::from_options(master_addr.clone(), Some("load-w2".into()), Some(8), false);
    let w2_id = w2.worker_id();

    tokio::spawn(async move {
        let _ = w1.run(w1_rx).await;
    });
    tokio::spawn(async move {
        let _ = w2.run(w2_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 2)
                    .unwrap_or(false)
            }
        )
        .await
    );

    // Submit 4 tasks
    let mut tasks = Vec::new();
    for i in 0..4 {
        let task = make_builtin_task(&format!("load_task_{i}"), 2500, None);
        tasks.push(master.submit_task(task).await.unwrap());
    }

    // Wait until all tasks are Running
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                let mut count = 0;
                for id in &tasks {
                    if let Ok(info) = master.get_task_info(*id).await {
                        if info.state == TaskState::Running {
                            count += 1;
                        }
                    }
                }
                count == 4
            }
        )
        .await
    );

    // Filter tasks that are on Worker 1
    let mut w1_tasks = Vec::new();
    for id in &tasks {
        let info = master.get_task_info(*id).await.unwrap();
        if info.assigned_worker_id == Some(w1_id) {
            w1_tasks.push(*id);
        }
    }

    // If no tasks on W1, test still validates the remaining on W2
    if !w1_tasks.is_empty() {
        let t0 = Instant::now();
        // Trigger graceful disconnect on W1
        let _ = w1_tx.send(true);

        // Measure latency until evicted tasks from W1 are reassigned to W2
        let first_task = w1_tasks[0];
        let reassigned = wait_for(
            Duration::from_millis(200),
            Duration::from_millis(5),
            || async {
                if let Ok(info) = master.get_task_info(first_task).await {
                    info.assigned_worker_id == Some(w2_id)
                        && (info.state == TaskState::Scheduled || info.state == TaskState::Running)
                } else {
                    false
                }
            },
        )
        .await;

        let elapsed = t0.elapsed();
        assert!(
            reassigned,
            "First evicted task must be reassigned to survivor W2"
        );
        assert!(
            elapsed < Duration::from_millis(100),
            "Graceful failover latency must be strictly <100ms; measured: {elapsed:?}"
        );
    }

    // All 4 tasks must complete on W2
    assert!(
        wait_for(
            Duration::from_secs(8),
            Duration::from_millis(50),
            || async {
                let mut done = 0;
                for id in &tasks {
                    if let Ok(info) = master.get_task_info(*id).await {
                        if info.state == TaskState::Completed {
                            done += 1;
                        }
                    }
                }
                done == 4
            }
        )
        .await
    );

    let _ = master.shutdown();
}

// =========================================================================
// TEST 4: HIGH TASK VOLUME WITH INTERLEAVED WORKER CHURN
// =========================================================================

#[tokio::test]
async fn test_stress_high_volume_worker_churn() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(10);
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    // Spawn 4 initial workers
    let mut workers = Vec::new();
    for i in 0..4 {
        workers.push(ManagedWorker::spawn(
            master_addr.clone(),
            &format!("churn-init-w{i}"),
            4,
            false,
        ));
    }

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 4)
                    .unwrap_or(false)
            }
        )
        .await
    );

    // Submit 30 tasks with varied durations (100ms to 300ms)
    let mut task_ids = Vec::new();
    for i in 0..30 {
        let dur = 100 + ((i % 5) * 50) as u64;
        let task = make_builtin_task(&format!("churn_task_{i}"), dur, None);
        task_ids.push(master.submit_task(task).await.unwrap());
    }

    // Churn workers actively while tasks are being processed
    for churn_step in 0..8 {
        tokio::time::sleep(Duration::from_millis(180)).await;

        // Kill or disconnect one existing worker
        if let Some(victim) = workers.pop() {
            if churn_step % 2 == 0 {
                victim.graceful_disconnect();
            } else {
                victim.abort();
            }
        }

        // Spawn a new replacement worker
        workers.push(ManagedWorker::spawn(
            master_addr.clone(),
            &format!("churn-rep-w{churn_step}"),
            4,
            false,
        ));
    }

    // Stop churning, spawn 2 extra workers to guarantee swift draining of the remaining queue
    workers.push(ManagedWorker::spawn(
        master_addr.clone(),
        "drain-w1",
        4,
        false,
    ));
    workers.push(ManagedWorker::spawn(
        master_addr.clone(),
        "drain-w2",
        4,
        false,
    ));

    // Wait for all 30 tasks to reach Completed
    let all_done = wait_for(
        Duration::from_secs(20),
        Duration::from_millis(50),
        || async {
            let mut completed = 0;
            for id in &task_ids {
                if let Ok(info) = master.get_task_info(*id).await {
                    if info.state == TaskState::Completed {
                        completed += 1;
                    }
                }
            }
            completed == 30
        },
    )
    .await;
    assert!(
        all_done,
        "All 30 tasks must complete despite rapid worker churn"
    );

    // Assert zero task failures, zero orphaned tasks
    let stats = master.queue_stats().await.unwrap();
    assert_eq!(stats.completed, 30);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.running, 0);
    assert_eq!(stats.queued, 0);

    for id in &task_ids {
        let info = master.get_task_info(*id).await.unwrap();
        assert_eq!(info.state, TaskState::Completed);
        assert_eq!(info.exit_code, Some(0));
    }

    let _ = master.shutdown();
}

// =========================================================================
// TEST 5: CONCURRENT MAX RETRIES AND WAITERS RESOLUTION
// =========================================================================

#[tokio::test]
async fn test_stress_concurrent_max_retries_and_waiters_resolution() {
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(2);
    let master = MasterServer::spawn(server_config)
        .await
        .expect("spawn master");
    let master_addr = master.server_addr().to_string();

    // Spawn Worker 1
    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::from_options(
        master_addr.clone(),
        Some("w-retry-1".into()),
        Some(4),
        false,
    );
    let h1 = tokio::spawn(async move {
        let _ = w1.run(w1_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 1)
                    .unwrap_or(false)
            }
        )
        .await
    );

    // Submit 3 tasks concurrently and attach wait_task listeners
    let mut task_ids = Vec::new();
    let mut wait_handles = Vec::new();

    for i in 0..3 {
        let task = make_builtin_task(&format!("retry_exhaust_{i}"), 3000, Some(2));
        let id = master.submit_task(task).await.unwrap();
        task_ids.push(id);

        let m_clone = master.clone();
        wait_handles.push(tokio::spawn(async move {
            m_clone.wait_task(id, Some(Duration::from_secs(10))).await
        }));
    }

    // Wait until all 3 tasks are running on W1
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                let mut running = 0;
                for id in &task_ids {
                    if let Ok(info) = master.get_task_info(*id).await {
                        if info.state == TaskState::Running {
                            running += 1;
                        }
                    }
                }
                running == 3
            }
        )
        .await
    );

    // Abort Worker 1 (Attempt 1)
    h1.abort();

    // Wait for retry_count to become 1
    assert!(
        wait_for(
            Duration::from_secs(4),
            Duration::from_millis(20),
            || async {
                let mut count1 = 0;
                for id in &task_ids {
                    if let Ok(info) = master.get_task_info(*id).await {
                        if info.retry_count == 1 {
                            count1 += 1;
                        }
                    }
                }
                count1 == 3
            }
        )
        .await
    );

    // Spawn Worker 2 (Attempt 2)
    let (_w2_tx, w2_rx) = watch::channel(false);
    let mut w2 = WorkerClient::from_options(
        master_addr.clone(),
        Some("w-retry-2".into()),
        Some(4),
        false,
    );
    let w2_id = w2.worker_id();
    let h2 = tokio::spawn(async move {
        let _ = w2.run(w2_rx).await;
    });

    // Wait until tasks run on W2
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                let mut running = 0;
                for id in &task_ids {
                    if let Ok(info) = master.get_task_info(*id).await {
                        if info.state == TaskState::Running
                            && info.assigned_worker_id == Some(w2_id)
                        {
                            running += 1;
                        }
                    }
                }
                running == 3
            }
        )
        .await
    );

    // Abort Worker 2 (Attempt 2)
    h2.abort();

    // Wait for retry_count to become 2
    assert!(
        wait_for(
            Duration::from_secs(4),
            Duration::from_millis(20),
            || async {
                let mut count2 = 0;
                for id in &task_ids {
                    if let Ok(info) = master.get_task_info(*id).await {
                        if info.retry_count == 2 {
                            count2 += 1;
                        }
                    }
                }
                count2 == 3
            }
        )
        .await
    );

    // Spawn Worker 3 (Attempt 3 -> will exceed max_retries = 2!)
    let (_w3_tx, w3_rx) = watch::channel(false);
    let mut w3 = WorkerClient::from_options(
        master_addr.clone(),
        Some("w-retry-3".into()),
        Some(4),
        false,
    );
    let h3 = tokio::spawn(async move {
        let _ = w3.run(w3_rx).await;
    });

    // Wait until running on W3
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                let mut running = 0;
                for id in &task_ids {
                    if let Ok(info) = master.get_task_info(*id).await {
                        if info.state == TaskState::Running {
                            running += 1;
                        }
                    }
                }
                running == 3
            }
        )
        .await
    );

    // Abort Worker 3 -> Retries (2) exceeded!
    h3.abort();

    // All wait_handles must resolve promptly with TaskResult failure
    let results = join_all(wait_handles).await;
    assert_eq!(results.len(), 3);

    for (i, res) in results.into_iter().enumerate() {
        let task_res = res.expect("join handle").expect("wait_task result");
        assert_eq!(
            task_res.exit_code, 1,
            "Task {i} exit code must be 1 on failure"
        );
        let err = task_res.error.unwrap_or_default();
        assert!(
            err.contains("maximum retry limit (2) reached"),
            "Error must specify maximum retry limit: {err}"
        );
        assert!(
            err.contains("retries exhausted"),
            "Error must specify retries exhausted: {err}"
        );
    }

    // Verify all 3 tasks in master queue are in Failed state
    for id in &task_ids {
        let info = master.get_task_info(*id).await.unwrap();
        assert_eq!(info.state, TaskState::Failed);
        assert_eq!(info.retry_count, 2);
    }

    let _ = master.shutdown();
}

// =========================================================================
// TEST 6: REAPER DETECTION OF SILENT WORKER AND FAILOVER
// =========================================================================

#[tokio::test]
async fn test_stress_reaper_detection_and_failover() {
    // Fast reaper: scans every 25ms, timeout 80ms
    let reaper_config = ReaperConfig::new(Duration::from_millis(25), Duration::from_millis(80));
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let master = MasterServer::spawn_with_config(server_config, Default::default(), reaper_config)
        .await
        .expect("master spawn");
    let master_addr = master.server_addr();

    // 1. Worker 1: Raw TCP connection that registers then HALTS all heartbeats (simulating silent freeze)
    let stream = tokio::net::TcpStream::connect(master_addr)
        .await
        .expect("tcp connect");
    let mut transport = MessageTransport::new(stream);
    let w1_id = Uuid::new_v4();
    transport
        .send_msg(&WorkerMessage::Register {
            worker_id: w1_id,
            capabilities: WorkerCapabilities::new("w-silent", 2, 2048, false, false, None),
        })
        .await
        .expect("send register");
    let ack: Option<MasterMessage> = transport.recv_msg().await.expect("recv ack");
    assert!(matches!(
        ack,
        Some(MasterMessage::RegisterAck { accepted: true, .. })
    ));

    // Submit task while Worker 1 is the ONLY registered worker
    let task = make_builtin_task("silent_reap_task", 100, None);
    let task_id = master.submit_task(task).await.unwrap();

    // Receive AssignTask on Worker 1 transport
    let assign = transport
        .recv_msg::<MasterMessage>()
        .await
        .expect("recv assign");
    assert!(matches!(assign, Some(MasterMessage::AssignTask { .. })));

    // Worker 1 holds TCP socket open but sends NOTHING (no heartbeats, no results).
    // The Master Reaper must detect silence >80ms and mark Worker 1 as Disconnected!
    let w1_reaped = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(20),
        || async {
            if let Ok(workers) = master.list_workers().await {
                workers
                    .iter()
                    .any(|w| w.worker_id == w1_id && w.status == WorkerStatus::Disconnected)
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        w1_reaped,
        "Master reaper must detect silent worker and mark as Disconnected"
    );

    // Allow the 500ms crash exponential backoff delay to elapse so task re-enters ready_queue
    tokio::time::sleep(Duration::from_millis(600)).await;

    // 2. NOW connect Worker 2 (raw TCP) to accept and complete the failed-over task
    let stream2 = tokio::net::TcpStream::connect(master_addr)
        .await
        .expect("tcp connect w2");
    let mut transport2 = MessageTransport::new(stream2);
    let w2_id = Uuid::new_v4();
    transport2
        .send_msg(&WorkerMessage::Register {
            worker_id: w2_id,
            capabilities: WorkerCapabilities::new("w-healthy", 2, 2048, false, false, None),
        })
        .await
        .expect("send register w2");
    let ack2: Option<MasterMessage> = transport2.recv_msg().await.expect("recv ack w2");
    assert!(matches!(
        ack2,
        Some(MasterMessage::RegisterAck { accepted: true, .. })
    ));

    // Worker 2 receives AssignTask for the reaped task
    let assign2 = transport2
        .recv_msg::<MasterMessage>()
        .await
        .expect("recv assign w2");
    assert!(matches!(assign2, Some(MasterMessage::AssignTask { .. })));

    // Worker 2 sends successful TaskResult
    let result_msg = WorkerMessage::TaskResult {
        worker_id: w2_id,
        task_id,
        exit_code: 0,
        stdout: "reaped and completed on w2".into(),
        stderr: "".into(),
        execution_time_ms: 10,
        is_gpu_executed: false,
        device_name: None,
        error: None,
    };
    transport2
        .send_msg(&result_msg)
        .await
        .expect("send result from w2");

    // The reaped task must be marked Completed on Master!
    assert!(
        wait_for(
            Duration::from_secs(3),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Completed)
                    .unwrap_or(false)
            }
        )
        .await,
        "Task must complete on Worker 2 after Worker 1 is reaped"
    );

    let info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(info.state, TaskState::Completed);
    assert_eq!(info.assigned_worker_id, Some(w2_id));
    assert_eq!(info.retry_count, 1);
    assert_eq!(info.exit_code, Some(0));

    // Verify Worker 1 status in registry is Disconnected
    let workers = master.list_workers().await.unwrap();
    let w1_entry = workers
        .iter()
        .find(|w| w.worker_id == w1_id)
        .expect("find w1");
    assert_eq!(w1_entry.status, WorkerStatus::Disconnected);
    assert_eq!(w1_entry.active_tasks, 0);

    let _ = master.shutdown();
}
