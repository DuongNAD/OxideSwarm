//! Integration test suite for Milestone 4: Master Scheduling & Workload-Specific Routing.
//!
//! Validates:
//! 1. Generic task execution and result retrieval (AC3)
//! 2. Strict GPU task routing isolation (AC4):
//!    - Routes exclusively to simulated GPU worker
//!    - Non-GPU workers NEVER receive GPU tasks
//!    - Negative test: task stays Queued when GPU worker is absent, executes upon reconnect
//! 3. Batch of 5 independent tasks distributed in parallel across all 3 workers (AC5):
//!    - Verifies spread scheduling diversity (all 3 workers receive work)
//!    - Validates parallel execution speedup over sequential
//! 4. Worker disconnection during task execution triggers automatic retry & completion on survivor
//! 5. In-flight task cancellation emits exit code 130 and transitions to Cancelled

use std::collections::HashSet;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{
    MasterHandle, MasterServer, ServerConfig, DEFAULT_MASTER_MAX_BATCH_SIZE,
};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Polls master worker states up to `TEST_WAIT_TIMEOUT`, naming the awaited condition and observed statuses on failure.
async fn wait_for_workers<F>(master: &MasterHandle, condition: &str, mut predicate: F)
where
    F: FnMut(&[rusty_grid_master::WorkerInfo]) -> bool,
{
    let start = Instant::now();
    loop {
        let workers = master.list_workers().await.unwrap_or_default();
        if predicate(&workers) {
            return;
        }
        if start.elapsed() >= TEST_WAIT_TIMEOUT {
            let observed: Vec<_> = workers
                .into_iter()
                .map(|w| (w.worker_id, w.status))
                .collect();
            panic!(
                "{condition} within {}s; observed workers: {observed:?}",
                TEST_WAIT_TIMEOUT.as_secs()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls `get_task_status` up to `TEST_WAIT_TIMEOUT` until `task_id` reaches `TaskStatus::Running`.
async fn wait_for_task_running(
    master: &MasterHandle,
    task_id: rusty_grid_core::TaskId,
    context: &str,
) {
    let start = Instant::now();
    loop {
        let status = master.get_task_status(task_id).await;
        if matches!(status, Ok(TaskStatus::Running)) {
            return;
        }
        if start.elapsed() >= TEST_WAIT_TIMEOUT {
            panic!(
                "{context} ({task_id}) did not reach Running within {}s; observed status: {status:?}",
                TEST_WAIT_TIMEOUT.as_secs()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Helper struct managing a test cluster of 1 Master and 3 Workers.
struct TestCluster {
    master: MasterHandle,
    w1_id: Uuid,
    w2_id: Uuid,
    w3_gpu_id: Uuid,
    w1_shutdown: watch::Sender<bool>,
    w2_shutdown: watch::Sender<bool>,
    w3_shutdown: watch::Sender<bool>,
    master_addr: String,
}

impl TestCluster {
    async fn setup() -> Self {
        let sched_cfg = SchedulerConfig::default()
            .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
            .with_max_host_cpu_pct(100.0);
        let master = MasterServer::spawn_with_config(
            ServerConfig::new("127.0.0.1:0".parse().unwrap()),
            sched_cfg,
            ReaperConfig::default(),
        )
        .await
        .expect("MasterServer::spawn failed");
        let master_addr = master.server_addr().to_string();

        let (w1_tx, w1_rx) = watch::channel(false);
        let (w2_tx, w2_rx) = watch::channel(false);
        let (w3_tx, w3_rx) = watch::channel(false);

        // Worker 1: 2 CPU cores, CPU-only
        let mut w1 = WorkerClient::for_testing(
            master_addr.clone(),
            Some("worker-cpu-1".into()),
            Some(2),
            false,
        );
        let w1_id = w1.worker_id();

        // Worker 2: 4 CPU cores, CPU-only
        let mut w2 = WorkerClient::for_testing(
            master_addr.clone(),
            Some("worker-cpu-2".into()),
            Some(4),
            false,
        );
        let w2_id = w2.worker_id();

        // Worker 3: 8 CPU cores, Simulated GPU enabled
        let mut w3 = WorkerClient::for_testing(
            master_addr.clone(),
            Some("worker-gpu-3".into()),
            Some(8),
            true,
        );
        let w3_gpu_id = w3.worker_id();

        tokio::spawn(async move {
            let _ = w1.run(w1_rx).await;
        });
        tokio::spawn(async move {
            let _ = w2.run(w2_rx).await;
        });
        tokio::spawn(async move {
            let _ = w3.run(w3_rx).await;
        });

        // Await full registration of all 3 workers
        wait_for_workers(
            &master,
            &format!("expected 3 Connected workers (w1={w1_id}, w2={w2_id}, w3_gpu={w3_gpu_id})"),
            |workers| {
                workers.len() == 3 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
            },
        )
        .await;

        Self {
            master,
            w1_id,
            w2_id,
            w3_gpu_id,
            w1_shutdown: w1_tx,
            w2_shutdown: w2_tx,
            w3_shutdown: w3_tx,
            master_addr,
        }
    }

    async fn teardown(self) {
        let _ = self.w1_shutdown.send(true);
        let _ = self.w2_shutdown.send(true);
        let _ = self.w3_shutdown.send(true);
        let _ = self.master.shutdown();
    }
}

// ---------------------------------------------------------------------------
// TEST 1: Generic Task Execution and Result Retrieval (R2 & AC3)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_m4_generic_task_execution_and_result_retrieval() {
    let cluster = TestCluster::setup().await;

    let payload_marker = "rusty_grid_m4_generic_ok_12345";
    let task = Task::new(
        TaskSpec::command("echo", vec![payload_marker.into()]),
        TaskRequirements::generic(1, 60),
    );

    let task_id = cluster
        .master
        .submit_task(task)
        .await
        .expect("submit_task failed");

    let result = cluster
        .master
        .wait_task(task_id, Some(TEST_WAIT_TIMEOUT))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "wait_task for generic task {task_id} timed out or failed within {}s: {e}",
                TEST_WAIT_TIMEOUT.as_secs()
            )
        });

    assert_eq!(result.exit_code, 0, "Task exit code must be 0");
    assert!(
        result.stdout.contains(payload_marker),
        "Stdout must contain payload marker: {}",
        result.stdout
    );
    assert!(
        !result.is_gpu_executed,
        "Generic task must not be marked GPU executed"
    );
    assert!(result.error.is_none(), "Task result error must be None");

    let status = cluster
        .master
        .get_task_status(task_id)
        .await
        .expect("get_task_status failed");
    assert_eq!(
        status,
        TaskStatus::Completed,
        "Task status must be Completed"
    );

    let valid_workers = [cluster.w1_id, cluster.w2_id, cluster.w3_gpu_id];
    assert!(
        valid_workers.contains(&result.worker_id),
        "Result worker ID must be one of the registered cluster workers"
    );

    cluster.teardown().await;
}

// ---------------------------------------------------------------------------
// TEST 2: Strict GPU Task Routing Isolation (R3 & AC4)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_m4_strict_gpu_task_routing_isolation() {
    let cluster = TestCluster::setup().await;

    // Sub-test 2.1: Submit GPU task and verify it executes ONLY on simulated GPU worker
    let gpu_task = Task::new(
        TaskSpec::gpu_compute("gemm_verification_kernel", 64),
        TaskRequirements::gpu(60),
    );

    let task_id = cluster
        .master
        .submit_task(gpu_task)
        .await
        .expect("submit GPU task failed");

    let result = cluster
        .master
        .wait_task(task_id, Some(TEST_WAIT_TIMEOUT))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "wait_task for GPU task {task_id} timed out or failed within {}s: {e}",
                TEST_WAIT_TIMEOUT.as_secs()
            )
        });

    assert_eq!(
        result.worker_id, cluster.w3_gpu_id,
        "GPU task MUST be executed exclusively on Worker 3 (simulated GPU)"
    );
    assert_ne!(
        result.worker_id, cluster.w1_id,
        "Worker 1 (CPU) must NEVER receive GPU task"
    );
    assert_ne!(
        result.worker_id, cluster.w2_id,
        "Worker 2 (CPU) must NEVER receive GPU task"
    );
    assert!(result.is_gpu_executed, "is_gpu_executed must be true");
    assert_eq!(
        result.exit_code, 0,
        "GPU compute must succeed with exit code 0"
    );
    assert!(
        result.stdout.contains("[GPU COMPUTE SIMULATOR]"),
        "Stdout must contain GPU simulator banner"
    );
    assert!(
        result.stdout.contains("Status: VERIFIED_OK"),
        "Stdout must contain verification OK"
    );

    // Sub-test 2.2: Negative gating when no GPU worker is available
    // Disconnect Worker 3 (the only GPU worker)
    let _ = cluster.w3_shutdown.send(true);

    let w3_gpu_id = cluster.w3_gpu_id;
    wait_for_workers(
        &cluster.master,
        &format!("expected Worker 3 ({w3_gpu_id}) to transition to Disconnected"),
        |workers| {
            workers
                .iter()
                .find(|w| w.worker_id == w3_gpu_id)
                .map(|w| w.status)
                == Some(WorkerStatus::Disconnected)
        },
    )
    .await;

    // Submit a second GPU task while ONLY CPU workers remain active
    let gpu_task_2 = Task::new(
        TaskSpec::gpu_compute("second_gemm_kernel", 64),
        TaskRequirements::gpu(60),
    );
    let task_id_2 = cluster
        .master
        .submit_task(gpu_task_2)
        .await
        .expect("submit second GPU task failed");

    // Wait 400ms: task MUST remain in Queued state and NOT spill over to CPU workers
    tokio::time::sleep(Duration::from_millis(400)).await;
    let status_2 = cluster
        .master
        .get_task_status(task_id_2)
        .await
        .expect("get_task_status failed");
    assert_eq!(
        status_2,
        TaskStatus::Queued,
        "GPU task MUST remain Queued when no GPU worker is available; must NOT assign to CPU workers"
    );

    // Reconnect a new simulated GPU worker
    let (w3b_tx, w3b_rx) = watch::channel(false);
    let mut w3b = WorkerClient::for_testing(
        cluster.master_addr.clone(),
        Some("worker-gpu-3b".into()),
        Some(8),
        true,
    );
    let w3b_id = w3b.worker_id();
    tokio::spawn(async move {
        let _ = w3b.run(w3b_rx).await;
    });

    wait_for_workers(
        &cluster.master,
        &format!("expected reconnected GPU worker {w3b_id} to reach Connected"),
        |workers| {
            workers
                .iter()
                .any(|w| w.worker_id == w3b_id && w.status == WorkerStatus::Connected)
        },
    )
    .await;

    // The queued GPU task should now immediately be picked up by the new GPU worker and complete
    let result_2 = cluster
        .master
        .wait_task(task_id_2, Some(TEST_WAIT_TIMEOUT))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "wait_task for queued GPU task {task_id_2} after reconnecting {w3b_id} failed within {}s: {e}",
                TEST_WAIT_TIMEOUT.as_secs()
            )
        });

    assert_eq!(
        result_2.worker_id, w3b_id,
        "Task must execute on the new GPU worker"
    );
    assert_eq!(result_2.exit_code, 0);
    assert!(result_2.is_gpu_executed);

    let _ = w3b_tx.send(true);
    cluster.teardown().await;
}

// ---------------------------------------------------------------------------
// TEST 3: Batch of 5 Independent Tasks Distributed in Parallel (R3 & AC5)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_m4_parallel_batch_spread_scheduling() {
    let cluster = TestCluster::setup().await;
    let barrier_dir = tempfile::tempdir().expect("barrier tempdir");
    let barrier = barrier_dir.path().to_string_lossy().replace('\\', "/");

    // Create 5 independent barrier-gated tasks: each signals start and waits for `release`
    // (capped safely at 1200 x 50ms = 60s) so serial execution cannot reach 3 concurrent tasks.
    let tasks: Vec<Task> = (0..5)
        .map(|i| {
            let script = format!(
                "barrier='{barrier}'\n\
                 : > \"$barrier/started_{i}\"\n\
                 tries=0\n\
                 while [ ! -f \"$barrier/release\" ] && [ \"$tries\" -lt 1200 ]; do\n\
                   sleep 0.05\n\
                   tries=$((tries + 1))\n\
                 done\n\
                 echo batch_task_{i}"
            );
            Task::new(
                TaskSpec::command("sh", vec!["-c".into(), script]),
                TaskRequirements::generic(1, 60),
            )
        })
        .collect();

    let start_time = Instant::now();

    // Submit all 5 tasks
    let mut task_ids = Vec::new();
    for t in tasks {
        let id = cluster.master.submit_task(t).await.expect("submit failed");
        task_ids.push(id);
    }

    let count_started = || -> usize {
        std::fs::read_dir(barrier_dir.path())
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|e| e.file_name().to_string_lossy().starts_with("started_"))
                    .count()
            })
            .unwrap_or(0)
    };

    let mut concurrent_started;
    loop {
        concurrent_started = count_started();
        if concurrent_started >= 3 {
            break;
        }
        if start_time.elapsed() >= TEST_WAIT_TIMEOUT {
            let _ = std::fs::write(barrier_dir.path().join("release"), b"timeout\n");
            panic!(
                "expected at least 3 of 5 batch tasks to run concurrently within {}s; observed {concurrent_started} started markers",
                TEST_WAIT_TIMEOUT.as_secs()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let reached_3_elapsed = start_time.elapsed();
    std::fs::write(barrier_dir.path().join("release"), b"go\n").expect("write barrier release");

    // Await all 5 results concurrently
    let wait_futures: Vec<_> = task_ids
        .iter()
        .map(|&id| cluster.master.wait_task(id, Some(TEST_WAIT_TIMEOUT)))
        .collect();

    let results = join_all(wait_futures).await;
    let total_elapsed = start_time.elapsed();
    println!(
        "m4 parallel batch: {concurrent_started} tasks running concurrently after {reached_3_elapsed:?}, total batch {total_elapsed:?}"
    );

    // 1. All tasks must succeed with exit code 0
    for (idx, res) in results.iter().enumerate() {
        let r = res.as_ref().unwrap_or_else(|e| {
            panic!(
                "Task {idx} ({}) failed within {}s: {e}",
                task_ids[idx],
                TEST_WAIT_TIMEOUT.as_secs()
            )
        });
        assert_eq!(r.exit_code, 0, "Task {idx} must exit 0");
        assert!(
            r.stdout.contains(&format!("batch_task_{idx}")),
            "Task {idx} stdout missing marker"
        );
    }

    // 2. Diversity / Spread check: ALL 3 workers must receive at least 1 task
    let assigned_workers: HashSet<Uuid> = results
        .iter()
        .map(|r| r.as_ref().unwrap().worker_id)
        .collect();

    assert_eq!(
        assigned_workers.len(),
        3,
        "Batch tasks must be distributed across ALL 3 available workers; got: {:?}",
        assigned_workers
    );
    assert!(
        assigned_workers.contains(&cluster.w1_id),
        "Worker 1 must receive work"
    );
    assert!(
        assigned_workers.contains(&cluster.w2_id),
        "Worker 2 must receive work"
    );
    assert!(
        assigned_workers.contains(&cluster.w3_gpu_id),
        "Worker 3 must receive work"
    );

    // 3. No single worker should execute all 5 tasks
    for wid in [&cluster.w1_id, &cluster.w2_id, &cluster.w3_gpu_id] {
        let count = results
            .iter()
            .filter(|r| r.as_ref().unwrap().worker_id == *wid)
            .count();
        assert!(
            count < 5,
            "Worker {wid} executed all 5 tasks! Spread scheduling failed."
        );
        assert!(
            count >= 1,
            "Worker {wid} executed 0 tasks! Spread scheduling failed."
        );
    }

    cluster.teardown().await;
}

// ---------------------------------------------------------------------------
// TEST 4: Worker Disconnect Triggers Automatic Failover and Retry
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_m4_worker_disconnect_during_execution_triggers_retry() {
    let sched_config = SchedulerConfig::default().with_max_host_cpu_pct(100.0);
    let reaper_config = ReaperConfig::default();

    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::spawn_with_config(server_config, sched_config, reaper_config)
        .await
        .expect("spawn failed");
    let master_addr = master.server_addr().to_string();

    let (w_a_tx, w_a_rx) = watch::channel(false);
    let (w_b_tx, w_b_rx) = watch::channel(false);

    let dir_a = tempfile::tempdir().expect("tempdir");
    let dir_b = tempfile::tempdir().expect("tempdir");

    let cfg_a = WorkerConfig::new(master_addr.clone())
        .with_no_battery(true)
        .with_name("worker-A")
        .with_cores(2)
        .with_sandbox_base_dir(dir_a.path())
        .with_redirection_portal(false);
    let cfg_b = WorkerConfig::new(master_addr.clone())
        .with_no_battery(true)
        .with_name("worker-B")
        .with_cores(2)
        .with_sandbox_base_dir(dir_b.path())
        .with_redirection_portal(false);

    let mut w_a = WorkerClient::new(cfg_a);
    let mut w_b = WorkerClient::new(cfg_b);

    let w_a_id = w_a.worker_id();
    let w_b_id = w_b.worker_id();

    let w_a_handle = tokio::spawn(async move {
        let _ = w_a.run(w_a_rx).await;
    });
    let w_b_handle = tokio::spawn(async move {
        let _ = w_b.run(w_b_rx).await;
    });

    wait_for_workers(
        &master,
        &format!("expected 2 Connected workers (w_a={w_a_id}, w_b={w_b_id})"),
        |workers| workers.len() == 2 && workers.iter().all(|w| w.status == WorkerStatus::Connected),
    )
    .await;

    // Gate the first attempt until the test aborts the initial worker, then let the retry
    // on the survivor complete cleanly without racing against a short sleep window.
    let gate_dir = tempfile::tempdir().expect("failover gate tempdir");
    let gate = gate_dir.path().to_string_lossy().replace('\\', "/");
    let failover_script = format!(
        "gate='{gate}'\n\
         if [ ! -f \"$gate/first_aborted\" ]; then\n\
           : > \"$gate/first_started\"\n\
           tries=0\n\
           while [ ! -f \"$gate/first_aborted\" ] && [ \"$tries\" -lt 1200 ]; do\n\
             sleep 0.05\n\
             tries=$((tries + 1))\n\
           done\n\
           exit 1\n\
         fi\n\
         echo failover_complete_ok"
    );
    let task = Task::new(
        TaskSpec::command("sh", vec!["-c".into(), failover_script]),
        TaskRequirements::generic(1, 60),
    );

    let task_id = master.submit_task(task).await.expect("submit failed");

    // Wait until task is actively running and the first shell attempt created `first_started`
    let start_wait = Instant::now();
    loop {
        let status = master.get_task_status(task_id).await;
        if matches!(status, Ok(TaskStatus::Running))
            && gate_dir.path().join("first_started").exists()
        {
            break;
        }
        if start_wait.elapsed() >= TEST_WAIT_TIMEOUT {
            let _ = std::fs::write(gate_dir.path().join("first_aborted"), b"timeout\n");
            panic!(
                "Task {task_id} did not start first failover attempt within {}s; status={status:?}, first_started={}",
                TEST_WAIT_TIMEOUT.as_secs(),
                gate_dir.path().join("first_started").exists()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let initial_info = master
        .get_task_info(task_id)
        .await
        .expect("get_task_info failed");
    let failed_worker_id = initial_info
        .assigned_worker_id
        .expect("Task must have assigned worker");
    let survivor_worker_id = if failed_worker_id == w_a_id {
        w_b_id
    } else {
        w_a_id
    };

    // Abruptly kill the worker currently executing the task, then release the retry gate
    if failed_worker_id == w_a_id {
        w_a_handle.abort();
        let _ = w_a_tx.send(true);
    } else {
        w_b_handle.abort();
        let _ = w_b_tx.send(true);
    }
    std::fs::write(gate_dir.path().join("first_aborted"), b"aborted\n")
        .expect("write first_aborted marker");

    // Await completion on survivor worker
    let result = master
        .wait_task(task_id, Some(TEST_WAIT_TIMEOUT))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Failover task {task_id} did not complete on survivor {survivor_worker_id} within {}s: {e}",
                TEST_WAIT_TIMEOUT.as_secs()
            )
        });

    assert_eq!(result.exit_code, 0, "Failover task must succeed");
    assert!(
        result.stdout.contains("failover_complete_ok"),
        "Stdout must contain completion marker: {}",
        result.stdout
    );
    assert_eq!(
        result.worker_id, survivor_worker_id,
        "Task must have been reassigned and completed on survivor worker"
    );

    let final_info = master
        .get_task_info(task_id)
        .await
        .expect("final get_task_info failed");
    assert_eq!(final_info.retry_count, 1, "Task retry count must be 1");
    assert_eq!(final_info.state, TaskState::Completed);

    let _ = w_a_tx.send(true);
    let _ = w_b_tx.send(true);
    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------
// TEST 5: In-Flight Task Cancellation (Exit Code 130 and Cancelled State)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_m4_in_flight_task_cancellation() {
    let cluster = TestCluster::setup().await;

    // Sleep far longer than TEST_WAIT_TIMEOUT so natural exit cannot race with cancel_task
    let task = Task::new(
        TaskSpec::command("sleep", vec!["120".into()]),
        TaskRequirements::generic(1, 180),
    );

    let task_id = cluster
        .master
        .submit_task(task)
        .await
        .expect("submit failed");

    // Wait until task is actively running on a worker
    wait_for_task_running(&cluster.master, task_id, "in-flight cancel target").await;

    let cancel_start = Instant::now();

    // Issue task cancellation directive
    cluster
        .master
        .cancel_task(task_id)
        .await
        .expect("cancel_task failed");

    // Await cancellation outcome well before the 120s sleep could end on its own
    let result = cluster
        .master
        .wait_task(task_id, Some(TEST_WAIT_TIMEOUT))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "wait_task for cancelled task {task_id} did not return within {}s: {e}",
                TEST_WAIT_TIMEOUT.as_secs()
            )
        });

    let cancel_duration = cancel_start.elapsed();
    println!("m4 in-flight cancellation completed in {cancel_duration:?}");

    assert_eq!(
        result.exit_code, 130,
        "Cancelled task must report exit code 130 (SIGINT/Cancelled)"
    );

    let status = cluster
        .master
        .get_task_status(task_id)
        .await
        .expect("get_task_status failed");
    assert_eq!(
        status,
        TaskStatus::Cancelled,
        "Final status must be Cancelled"
    );

    // Verify worker recovered capacity by submitting a quick follow-up task
    let quick_task = Task::new(
        TaskSpec::command("echo", vec!["worker_reused_ok".into()]),
        TaskRequirements::generic(1, 60),
    );
    let quick_id = cluster
        .master
        .submit_task(quick_task)
        .await
        .expect("quick task submit failed");
    let quick_result = cluster
        .master
        .wait_task(quick_id, Some(TEST_WAIT_TIMEOUT))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "quick follow-up task {quick_id} after cancellation did not succeed within {}s: {e}",
                TEST_WAIT_TIMEOUT.as_secs()
            )
        });
    assert_eq!(quick_result.exit_code, 0);
    assert!(quick_result.stdout.contains("worker_reused_ok"));

    cluster.teardown().await;
}
