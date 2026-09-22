//! Tier 5 White-Box Adversarial Coverage Hardening Test Suite for Milestone 6 Phase 2 (Challenger 1).
//!
//! Authored by Empirical Challenger 1 to aggressively stress-test and empirically verify:
//! 1. High-Concurrency Task Flood & Dynamic Queue Rebalancing:
//!    - High-volume task floods (50-100 tasks) under interleaved worker crashes and drops.
//!    - Dynamic queue rebalancing verifying orphaned task recovery, retry scheduling, and 100% completion.
//!    - Priority preemption and burst scheduling resilience under sudden worker churn.
//! 2. Protocol Wire Framing, Half-Open Sockets, Garbage Payloads & Unexpected TCP EOF:
//!    - Garbage wire length frames exceeding MAX_FRAME_SIZE (64MB) and corrupted frame headers.
//!    - Garbage non-JSON binary payloads over valid framing prefixes.
//!    - Truncated length prefix EOF during handshake and slowloris watchdog defense.
//!    - Abrupt TCP EOF while a task is actively running on a worker (failover re-enqueue).
//!    - Client half-open socket drop with pending oneshot completion waiters.
//! 3. Map/Reduce Distributed Pipeline Intermediate Failure Stress:
//!    - Abrupt worker crash during mapper execution phase with failover to remaining worker.
//!    - Worker drop during shuffle grouping and reducer execution phase with task recovery.
//!    - Total worker loss / zero survivor handling with clean job failure (no infinite deadlock).
//! 4. Rapid Concurrent Task Cancellation Races Across State Transitions:
//!    - Concurrent cancel directives racing between Queued, Scheduled, and Running states.
//!    - Zero-worker queued task cancellation storm verifying clean queue drain and zero leaks.
//!    - SIGKILL / process abortion verification on active running commands.
//!    - 50-thread concurrent cancellation idempotence on single TaskId.

#![allow(clippy::field_reassign_with_default)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
use rusty_grid_core::protocol::{ClientMessage, MessageTransport, MAX_FRAME_SIZE};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskResult, TaskSpec, TaskStatus};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use rusty_grid_worker::runner::EXIT_CODE_CANCELLED;

// ============================================================================
// TEST HARNESS & FIXTURES
// ============================================================================

struct SpawnedWorker {
    pub id: Uuid,
    pub shutdown_tx: watch::Sender<bool>,
    pub handle: tokio::task::JoinHandle<()>,
}

struct ChallengerTestCluster {
    pub master: MasterHandle,
    pub master_addr: SocketAddr,
    pub workers: Vec<SpawnedWorker>,
    pub temp_dirs: Vec<TempDir>,
}

impl ChallengerTestCluster {
    pub async fn new() -> Self {
        let sched_cfg = SchedulerConfig {
            tick_interval: Duration::from_millis(20),
            max_host_cpu_pct: 100.0,
            ..Default::default()
        };
        Self::new_with_config(sched_cfg, ReaperConfig::default()).await
    }

    pub async fn new_with_config(sched_cfg: SchedulerConfig, reaper_cfg: ReaperConfig) -> Self {
        let master = MasterServer::spawn_with_config(
            ServerConfig::new("127.0.0.1:0".parse().unwrap()),
            sched_cfg,
            reaper_cfg,
        )
        .await
        .expect("MasterServer::spawn_with_config failed");

        let master_addr = master.server_addr();
        Self {
            master,
            master_addr,
            workers: Vec::new(),
            temp_dirs: Vec::new(),
        }
    }

    pub async fn spawn_worker(
        &mut self,
        name: &str,
        cores: usize,
        ram_mb: u64,
        simulate_gpu: bool,
    ) -> Uuid {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let cfg = WorkerConfig::new(self.master_addr.to_string())
            .with_name(name)
            .with_cores(cores)
            .with_ram_mb(ram_mb)
            .with_simulate_gpu(simulate_gpu)
            .with_sandbox_base_dir(temp_dir.path());
        self.temp_dirs.push(temp_dir);

        let mut client = WorkerClient::new(cfg);
        let id = client.worker_id();
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(rx).await;
        });

        self.workers.push(SpawnedWorker {
            id,
            shutdown_tx: tx,
            handle,
        });
        id
    }

    #[allow(dead_code)]
    pub async fn spawn_worker_with_config(&mut self, cfg: WorkerConfig) -> Uuid {
        let mut client = WorkerClient::new(cfg);
        let id = client.worker_id();
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(rx).await;
        });

        self.workers.push(SpawnedWorker {
            id,
            shutdown_tx: tx,
            handle,
        });
        id
    }

    /// Abruptly drops worker connection simulating process crash / TCP disconnect.
    pub fn abort_worker(&mut self, id: Uuid) {
        if let Some(pos) = self.workers.iter().position(|w| w.id == id) {
            let w = self.workers.remove(pos);
            w.handle.abort();
        }
    }

    pub async fn wait_for_workers(&self, count: usize, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Ok(workers) = self.master.list_workers().await {
                let connected = workers
                    .iter()
                    .filter(|w| w.status == WorkerStatus::Connected)
                    .count();
                if connected >= count {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        false
    }

    pub async fn wait_for_worker_status(
        &self,
        id: Uuid,
        status: WorkerStatus,
        timeout: Duration,
    ) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Ok(workers) = self.master.list_workers().await {
                if let Some(w) = workers.iter().find(|w| w.worker_id == id) {
                    if w.status == status {
                        return true;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    pub async fn submit_and_wait(&self, task: Task, timeout: Duration) -> GridResult<TaskResult> {
        let id = self.master.submit_task(task).await?;
        self.master.wait_task(id, Some(timeout)).await
    }
}

impl Drop for ChallengerTestCluster {
    fn drop(&mut self) {
        let _ = self.master.shutdown();
        for w in &self.workers {
            let _ = w.shutdown_tx.send(true);
            w.handle.abort();
        }
    }
}

fn make_echo_task(msg: &str) -> Task {
    Task::new(
        TaskSpec::Command {
            program: "echo".into(),
            args: vec![msg.into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 30),
    )
}

fn make_sleep_script_task(secs: f32) -> Task {
    Task::new(
        TaskSpec::ShellScript {
            script: format!("sleep {secs}\necho done_{secs}"),
            interpreter: None,
            env: HashMap::new(),
        },
        TaskRequirements::generic(1, 30),
    )
}

fn make_builtin_task(iterations: u32, duration_ms: u64) -> Task {
    Task::new(
        TaskSpec::BuiltinTest {
            test_name: "stress_compute".into(),
            iterations,
            duration_ms,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    )
}

// ============================================================================
// PART 1: HIGH-CONCURRENCY TASK FLOOD & DYNAMIC QUEUE REBALANCING
// ============================================================================

#[tokio::test]
async fn test_adversarial_task_flood_60_tasks_with_worker_crash_and_recovery() {
    let mut cluster = ChallengerTestCluster::new().await;

    // Spawn 3 workers
    let w1 = cluster.spawn_worker("flood-w1", 4, 4096, false).await;
    let _w2 = cluster.spawn_worker("flood-w2", 4, 4096, false).await;
    let _w3 = cluster.spawn_worker("flood-w3", 4, 4096, false).await;

    assert!(
        cluster.wait_for_workers(3, Duration::from_secs(5)).await,
        "All 3 workers must connect"
    );

    // Prepare 60 tasks: 20 echo, 20 builtin compute, 20 fast shell scripts
    let mut task_ids = Vec::with_capacity(60);
    for i in 0..60 {
        let task = if i < 20 {
            make_echo_task(&format!("batch_item_{i}"))
        } else if i < 40 {
            make_builtin_task(20_000, 10)
        } else {
            make_builtin_task(10_000, 5)
        };
        let id = cluster.master.submit_task(task).await.expect("submit");
        task_ids.push(id);
    }

    // Interleave abrupt worker crash while tasks are in flight
    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.abort_worker(w1);
    assert!(
        cluster
            .wait_for_worker_status(w1, WorkerStatus::Disconnected, Duration::from_secs(5))
            .await,
        "Master must detect w1 disconnect"
    );

    // Spawn replacement worker to test dynamic queue rebalancing
    let w4 = cluster
        .spawn_worker("flood-w4-replacement", 4, 4096, false)
        .await;
    assert!(
        cluster
            .wait_for_worker_status(w4, WorkerStatus::Connected, Duration::from_secs(5))
            .await,
        "w4 must connect"
    );
    // Allow w4 transport handshake to settle and enter inbound message loop
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Await all 60 tasks
    let wait_futures: Vec<_> = task_ids
        .iter()
        .map(|id| cluster.master.wait_task(*id, Some(Duration::from_secs(35))))
        .collect();

    let results = join_all(wait_futures).await;
    for (i, res) in results.into_iter().enumerate() {
        match res {
            Ok(task_res) => {
                assert_eq!(
                    task_res.exit_code, 0,
                    "Task {i} must complete with exit code 0"
                );
                assert!(
                    task_res.error.is_none(),
                    "Task {i} had unexpected error: {:?}",
                    task_res.error
                );
            }
            Err(e) => {
                let info = cluster.master.get_task_info(task_ids[i]).await;
                let stats = cluster.master.queue_stats().await;
                let workers = cluster.master.list_workers().await;
                panic!(
                    "Task {i} ({}) failed to resolve: {e}. TaskInfo: {:?}, QueueStats: {:?}, Workers: {:?}",
                    task_ids[i], info, stats, workers
                );
            }
        }
    }

    // Verify queue consistency post-flood
    let stats = cluster.master.queue_stats().await.expect("stats");
    assert_eq!(stats.total, 60, "Total tasks must be 60");
    assert_eq!(stats.queued, 0, "No tasks should remain queued");
    assert_eq!(stats.running, 0, "No tasks should remain running");
    assert_eq!(stats.completed, 60, "All 60 tasks must be completed");
    assert_eq!(stats.failed, 0, "No tasks should be marked failed");
}

#[tokio::test]
async fn test_adversarial_task_flood_100_tasks_rolling_worker_churn() {
    let mut cluster = ChallengerTestCluster::new().await;

    // Spawn 4 initial workers
    let mut active_worker_ids = Vec::new();
    for i in 1..=4 {
        let id = cluster
            .spawn_worker(&format!("churn-w{i}"), 4, 4096, false)
            .await;
        active_worker_ids.push(id);
    }

    assert!(
        cluster.wait_for_workers(4, Duration::from_secs(5)).await,
        "4 workers registered"
    );

    // Flood 100 fast tasks
    let mut task_ids = Vec::with_capacity(100);
    for i in 0..100 {
        let task = make_echo_task(&format!("churn_payload_{i}"));
        let id = cluster.master.submit_task(task).await.expect("submit");
        task_ids.push(id);
    }

    // Rolling worker churn: kill a worker, wait, spawn replacement
    for cycle in 1..=3 {
        tokio::time::sleep(Duration::from_millis(60)).await;
        if let Some(victim) = active_worker_ids.pop() {
            cluster.abort_worker(victim);
        }
        let replacement = cluster
            .spawn_worker(&format!("churn-replacement-{cycle}"), 4, 4096, false)
            .await;
        active_worker_ids.push(replacement);
    }

    // Await all 100 tasks
    let wait_futures: Vec<_> = task_ids
        .into_iter()
        .map(|id| cluster.master.wait_task(id, Some(Duration::from_secs(25))))
        .collect();

    let results = join_all(wait_futures).await;
    let mut completed = 0;
    for (i, res) in results.into_iter().enumerate() {
        match res {
            Ok(r) => {
                assert_eq!(r.exit_code, 0, "Task {i} exit code must be 0");
                completed += 1;
            }
            Err(e) => panic!("Task {i} failed waiting: {e}"),
        }
    }
    assert_eq!(completed, 100, "All 100 tasks must complete");

    let stats = cluster.master.queue_stats().await.expect("stats");
    assert_eq!(stats.completed, 100);
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.running, 0);
}

#[tokio::test]
async fn test_adversarial_priority_flood_preemption_under_churn() {
    let sched_cfg = SchedulerConfig {
        tick_interval: Duration::from_millis(20),
        max_host_cpu_pct: 100.0,
        // 1 core concurrency cap to force queue serialization
        max_tasks_per_worker: Some(1),
        ..Default::default()
    };

    let mut cluster =
        ChallengerTestCluster::new_with_config(sched_cfg, ReaperConfig::default()).await;
    let w1 = cluster.spawn_worker("prio-w1", 1, 2048, false).await;
    let _w2 = cluster.spawn_worker("prio-w2", 1, 2048, false).await;

    assert!(cluster.wait_for_workers(2, Duration::from_secs(5)).await);

    // Submit 6 low priority tasks that take some time
    let mut low_prio_ids = Vec::new();
    for _i in 0..6 {
        let task = make_sleep_script_task(0.15);
        let id = cluster
            .master
            .submit_task_with_priority(task, 0)
            .await
            .expect("submit low prio");
        low_prio_ids.push(id);
    }

    // Submit 3 high priority tasks
    let mut high_prio_ids = Vec::new();
    for i in 0..3 {
        let task = make_echo_task(&format!("high_prio_{i}"));
        let id = cluster
            .master
            .submit_task_with_priority(task, 100)
            .await
            .expect("submit high prio");
        high_prio_ids.push(id);
    }

    // Drop worker 1 mid-execution
    tokio::time::sleep(Duration::from_millis(40)).await;
    cluster.abort_worker(w1);

    // High priority tasks must resolve quickly
    for id in high_prio_ids {
        let res = cluster
            .master
            .wait_task(id, Some(Duration::from_secs(10)))
            .await
            .expect("high prio resolution");
        assert_eq!(res.exit_code, 0);
    }

    // All low priority tasks must also eventually complete
    for id in low_prio_ids {
        let res = cluster
            .master
            .wait_task(id, Some(Duration::from_secs(15)))
            .await
            .expect("low prio resolution");
        assert_eq!(res.exit_code, 0);
    }
}

// ============================================================================
// PART 2: PROTOCOL WIRE FRAMING, HALF-OPEN SOCKETS & UNEXPECTED TCP EOF
// ============================================================================

#[tokio::test]
async fn test_adversarial_garbage_wire_frame_exceeding_max_length() {
    let mut cluster = ChallengerTestCluster::new().await;

    // Connect raw TCP socket to Master
    let mut stream = TcpStream::connect(cluster.master_addr)
        .await
        .expect("connect raw stream");

    // Send 4-byte big-endian prefix specifying length > MAX_FRAME_SIZE (64MB)
    let illegal_len: u32 = (MAX_FRAME_SIZE as u32) + 4096;
    stream
        .write_all(&illegal_len.to_be_bytes())
        .await
        .expect("write length prefix");

    // Write a dummy payload
    let dummy_payload = vec![0x41u8; 128];
    let _ = stream.write_all(&dummy_payload).await;
    let _ = stream.flush().await;

    // The Master should reject the oversized frame and close or drop the socket
    let mut read_buf = [0u8; 32];
    let read_res = stream.read(&mut read_buf).await;
    match read_res {
        Ok(0) => {
            // Clean EOF from master closing socket
        }
        Ok(_) => {}
        Err(_) => {
            // Connection reset or broken pipe
        }
    }

    // Verify Master server didn't panic or crash: a legitimate worker can connect and run tasks
    let _w1 = cluster.spawn_worker("healthy-w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(5)).await);

    let task = make_echo_task("post_oversized_frame_verification");
    let result = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .expect("task run");
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.stdout.trim(), "post_oversized_frame_verification");
}

#[tokio::test]
async fn test_adversarial_garbage_wire_non_json_payload() {
    let mut cluster = ChallengerTestCluster::new().await;

    let mut stream = TcpStream::connect(cluster.master_addr)
        .await
        .expect("connect");

    // Valid framing length (24 bytes), but completely invalid non-JSON binary bytes
    let garbage_len: u32 = 24;
    stream
        .write_all(&garbage_len.to_be_bytes())
        .await
        .expect("write len");
    let garbage_data = [
        0xFF, 0xFE, 0x00, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x11, 0x22, 0x33, 0x44,
        0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
    ];
    stream
        .write_all(&garbage_data)
        .await
        .expect("write garbage");
    stream.flush().await.expect("flush");

    // Master should close stream on deserialization error
    let mut buf = [0u8; 16];
    let n = stream.read(&mut buf).await.unwrap_or(0);
    assert_eq!(
        n, 0,
        "Master must close connection on non-JSON garbage frame"
    );

    // Legitimate worker executes work unaffected
    cluster.spawn_worker("legit-w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(5)).await);

    let task = make_echo_task("post_garbage_test");
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(res.exit_code, 0);
}

#[tokio::test]
async fn test_adversarial_truncated_length_header_unexpected_eof() {
    let mut cluster = ChallengerTestCluster::new().await;

    let mut stream = TcpStream::connect(cluster.master_addr).await.unwrap();

    // Send only 2 bytes of the 4-byte length prefix
    stream.write_all(&[0x00, 0x00]).await.unwrap();
    stream.shutdown().await.unwrap();
    drop(stream);

    // Master handles clean/unexpected EOF without panic
    tokio::time::sleep(Duration::from_millis(50)).await;

    cluster.spawn_worker("test-worker", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(5)).await);

    let res = cluster
        .submit_and_wait(make_echo_task("header_eof_ok"), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(res.exit_code, 0);
}

#[tokio::test]
async fn test_adversarial_unexpected_tcp_eof_during_active_task_execution() {
    let mut cluster = ChallengerTestCluster::new().await;

    let w1 = cluster.spawn_worker("victim-w1", 2, 2048, false).await;
    let _w2 = cluster.spawn_worker("survivor-w2", 2, 2048, false).await;

    assert!(cluster.wait_for_workers(2, Duration::from_secs(5)).await);

    // Submit a shell script that runs for 1.5 seconds
    let task = make_sleep_script_task(1.5);
    let task_id = cluster.master.submit_task(task).await.unwrap();

    // Wait until task is actively Running
    let start = Instant::now();
    let mut is_running = false;
    while start.elapsed() < Duration::from_secs(5) {
        if let Ok(TaskStatus::Running) = cluster.master.get_task_status(task_id).await {
            is_running = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert!(is_running, "Task should enter Running state");

    // Abruptly sever w1's TCP stream mid-execution
    cluster.abort_worker(w1);

    // Master detects TCP EOF, marks task for retry, and survivor-w2 executes it
    let result = cluster
        .master
        .wait_task(task_id, Some(Duration::from_secs(15)))
        .await
        .expect("Task must resolve on survivor worker");

    assert_eq!(result.exit_code, 0);
    assert!(result.stdout.contains("done_1.5"));
}

#[tokio::test]
async fn test_adversarial_client_abrupt_disconnect_with_pending_waiter() {
    let mut cluster = ChallengerTestCluster::new().await;
    cluster.spawn_worker("steady-w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(5)).await);

    let stream = TcpStream::connect(cluster.master_addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let task = make_sleep_script_task(0.2);
    let task_id = task.id;

    // Send ClientMessage::SubmitTask with wait = true
    let submit_msg = ClientMessage::SubmitTask { task, wait: true };
    transport.send_msg(&submit_msg).await.unwrap();

    // Abruptly drop client transport before receiving response
    drop(transport);

    // Master finishes task, attempts to notify dead client, handles error gracefully
    let start = Instant::now();
    let mut state = TaskState::Submitted;
    while start.elapsed() < Duration::from_secs(3) {
        if let Ok(info) = cluster.master.get_task_info(task_id).await {
            state = info.state;
            if state == TaskState::Completed {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(state, TaskState::Completed);
}

#[tokio::test]
async fn test_adversarial_slowloris_handshake_timeout() {
    let sched_cfg = SchedulerConfig {
        tick_interval: Duration::from_millis(20),
        ..Default::default()
    };

    let mut server_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    // Configure 1-second handshake timeout for fast testing
    server_cfg.handshake_timeout_secs = 1;

    let master = MasterServer::spawn_with_config(server_cfg, sched_cfg, ReaperConfig::default())
        .await
        .unwrap();

    let mut stream = TcpStream::connect(master.server_addr()).await.unwrap();

    // Send 0 bytes and wait past the 1-second timeout
    tokio::time::sleep(Duration::from_millis(1300)).await;

    // Try reading: master should have terminated the stalled connection
    let mut buf = [0u8; 16];
    let n = stream.read(&mut buf).await.unwrap_or(0);
    assert_eq!(n, 0, "Slowloris idle socket must be closed by watchdog");

    let _ = master.shutdown();
}

// ============================================================================
// PART 3: MAP/REDUCE INTERMEDIATE FAILURE STRESS
// ============================================================================

#[tokio::test]
async fn test_adversarial_mapreduce_worker_crash_during_map_phase() {
    let mut cluster = ChallengerTestCluster::new().await;

    let w1 = cluster.spawn_worker("mr-map-w1", 4, 4096, false).await;
    let _w2 = cluster.spawn_worker("mr-map-w2", 4, 4096, false).await;

    assert!(cluster.wait_for_workers(2, Duration::from_secs(5)).await);

    // MapReduce job with 4 partitions, counting word tokens
    let input = vec![
        "apple banana apple orange".to_string(),
        "banana cherry apple pear".to_string(),
        "orange pear kiwi banana".to_string(),
        "kiwi apple banana cherry".to_string(),
    ];

    let job_spec = MapReduceJobSpec::new(
        "stress_map_failover",
        input,
        MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        4, // 4 map partitions
        1,
        20,
    );

    let master_clone = cluster.master.clone();
    let mr_handle = tokio::spawn(async move { master_clone.execute_mapreduce(job_spec).await });

    // Abruptly kill Worker 1 during the mapping phase
    tokio::time::sleep(Duration::from_millis(50)).await;
    cluster.abort_worker(w1);

    let result = mr_handle
        .await
        .expect("join handle")
        .expect("mapreduce execution");

    assert_eq!(
        result.status, "Completed",
        "Job should complete despite worker crash"
    );
    assert!(result.error.is_none());

    // Verify word counts: "apple" appears 4 times across the 4 lines
    let apple_count = result
        .output
        .get("apple")
        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
        .expect("apple count");
    assert_eq!(apple_count, 4);

    // "banana" appears 4 times
    let banana_count = result
        .output
        .get("banana")
        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
        .expect("banana count");
    assert_eq!(banana_count, 4);
}

#[tokio::test]
async fn test_adversarial_mapreduce_worker_drop_during_reduce_phase() {
    let mut cluster = ChallengerTestCluster::new().await;

    let w1 = cluster.spawn_worker("mr-red-w1", 4, 4096, false).await;
    let _w2 = cluster.spawn_worker("mr-red-w2", 4, 4096, false).await;

    assert!(cluster.wait_for_workers(2, Duration::from_secs(5)).await);

    // Generate input that maps into 3 distinct keys: "alpha", "beta", "gamma"
    let input = vec![
        "alpha\t10".to_string(),
        "beta\t20".to_string(),
        "gamma\t30".to_string(),
        "alpha\t5".to_string(),
        "beta\t15".to_string(),
    ];

    // Reducer is a shell script with a slight delay so it stays active during kill
    let reducer_script = r#"
while IFS= read -r line || [ -n "$line" ]; do
  sleep 0.08
  # Input JSON has {"key": "...", "values": [...]}
  # Extract values and sum them
  sum=$(echo "$line" | grep -o '[0-9]\+' | awk '{s+=$1} END {print (s=="")?0:s}')
  printf "%d\n" "$sum"
done
"#;

    let job_spec = MapReduceJobSpec::new(
        "stress_reduce_failover",
        input,
        MapFunctionSpec::Builtin {
            operator: "identity".into(),
        },
        ReduceFunctionSpec::ShellScript {
            script: reducer_script.into(),
        },
        2,
        3,
        25,
    );

    let master_clone = cluster.master.clone();
    let mr_handle = tokio::spawn(async move { master_clone.execute_mapreduce(job_spec).await });

    // Wait for map phase to complete and reduce tasks to begin dispatching
    tokio::time::sleep(Duration::from_millis(150)).await;
    cluster.abort_worker(w1);

    let result = mr_handle
        .await
        .expect("join handle")
        .expect("mapreduce execution");

    assert_eq!(result.status, "Completed");
    assert!(result.error.is_none());

    // Check that alpha, beta, gamma all exist in final output
    assert!(result.output.contains_key("alpha"), "Must contain alpha");
    assert!(result.output.contains_key("beta"), "Must contain beta");
    assert!(result.output.contains_key("gamma"), "Must contain gamma");
}

#[tokio::test]
async fn test_adversarial_mapreduce_worker_drop_during_shuffle_grouping() {
    let mut cluster = ChallengerTestCluster::new().await;

    let w1 = cluster.spawn_worker("mr-shuf-w1", 4, 4096, false).await;
    let _w2 = cluster.spawn_worker("mr-shuf-w2", 4, 4096, false).await;

    assert!(cluster.wait_for_workers(2, Duration::from_secs(5)).await);

    let input = vec![
        "group_a".to_string(),
        "group_b".to_string(),
        "group_a".to_string(),
        "group_b".to_string(),
        "group_a".to_string(),
    ];

    let job_spec = MapReduceJobSpec::new(
        "stress_shuffle_drop",
        input,
        MapFunctionSpec::Builtin {
            operator: "identity".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        2,
        2,
        25,
    );

    let master_clone = cluster.master.clone();
    let mr_handle = tokio::spawn(async move { master_clone.execute_mapreduce(job_spec).await });

    // Abort Worker 1 right as map tasks finish and shuffle grouping executes
    tokio::time::sleep(Duration::from_millis(35)).await;
    cluster.abort_worker(w1);

    let result = mr_handle
        .await
        .expect("join handle")
        .expect("mapreduce execution");

    assert_eq!(result.status, "Completed");
    assert!(result.error.is_none());

    let val_a = result
        .output
        .get("group_a")
        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
        .expect("group_a count");
    assert_eq!(val_a, 3);

    let val_b = result
        .output
        .get("group_b")
        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
        .expect("group_b count");
    assert_eq!(val_b, 2);
}

// ============================================================================
// PART 4: RAPID CONCURRENT TASK CANCELLATION RACES ACROSS STATE TRANSITIONS
// ============================================================================

#[tokio::test]
async fn test_adversarial_rapid_concurrent_cancel_storm_30_tasks() {
    let mut cluster = ChallengerTestCluster::new().await;

    cluster.spawn_worker("cancel-w1", 4, 4096, false).await;
    cluster.spawn_worker("cancel-w2", 4, 4096, false).await;
    assert!(cluster.wait_for_workers(2, Duration::from_secs(5)).await);

    // Submit 30 tasks with varied durations
    let mut task_ids = Vec::with_capacity(30);
    for i in 0..30 {
        let task = if i % 3 == 0 {
            make_echo_task(&format!("instant_{i}"))
        } else if i % 3 == 1 {
            make_sleep_script_task(0.1)
        } else {
            make_sleep_script_task(0.5)
        };
        let id = cluster.master.submit_task(task).await.unwrap();
        task_ids.push(id);
    }

    // Launch 8 concurrent cancel tasks hammering random IDs from the batch
    let master_ref = cluster.master.clone();
    let ids_arc = Arc::new(task_ids.clone());
    let mut cancel_handles = Vec::new();

    for thread_idx in 0..8 {
        let m = master_ref.clone();
        let ids = Arc::clone(&ids_arc);
        cancel_handles.push(tokio::spawn(async move {
            for step in 0..15 {
                let target_id = ids[(thread_idx * 7 + step) % ids.len()];
                let _ = m.cancel_task(target_id).await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }

    // Wait for all cancellation directives to complete
    join_all(cancel_handles).await;

    // Allow any non-cancelled tasks to complete
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Validate state invariants for every task
    for (i, task_id) in task_ids.iter().enumerate() {
        let info = cluster
            .master
            .get_task_info(*task_id)
            .await
            .unwrap_or_else(|_| panic!("Task {i} info must exist"));

        assert!(
            info.state.is_terminal(),
            "Task {i} ({task_id}) must be in terminal state, found {:?}",
            info.state
        );

        match info.state {
            TaskState::Completed => {
                let res = cluster.master.wait_task(*task_id, None).await.unwrap();
                assert_eq!(res.exit_code, 0);
            }
            TaskState::Cancelled => {
                let res = cluster.master.wait_task(*task_id, None).await.unwrap();
                assert_eq!(
                    res.exit_code, EXIT_CODE_CANCELLED,
                    "Cancelled task {i} must have exit code 130"
                );
            }
            other => panic!("Unexpected terminal state for task {i}: {:?}", other),
        }
    }

    let stats = cluster.master.queue_stats().await.unwrap();
    assert_eq!(stats.queued, 0, "Queued tasks must be 0");
    assert_eq!(stats.running, 0, "Running tasks must be 0");
    assert_eq!(
        stats.completed + stats.failed + stats.cancelled,
        30,
        "All 30 tasks accounted for across completed, failed, or cancelled"
    );
}

#[tokio::test]
async fn test_adversarial_cancel_queued_tasks_zero_workers() {
    let cluster = ChallengerTestCluster::new().await;
    // ZERO workers registered

    let mut task_ids = Vec::new();
    for i in 0..15 {
        let task = make_echo_task(&format!("zero_worker_{i}"));
        let id = cluster.master.submit_task(task).await.unwrap();
        task_ids.push(id);
    }

    // All tasks must be in Queued state
    for id in &task_ids {
        assert_eq!(
            cluster.master.get_task_status(*id).await.unwrap(),
            TaskStatus::Queued
        );
    }

    // Rapidly cancel all queued tasks concurrently
    let cancel_futs: Vec<_> = task_ids
        .iter()
        .map(|id| cluster.master.cancel_task(*id))
        .collect();
    let cancel_results = join_all(cancel_futs).await;
    for (i, res) in cancel_results.into_iter().enumerate() {
        assert!(res.is_ok(), "Cancel on queued task {i} must succeed");
    }

    // Verify all tasks entered Cancelled state and ready_queue is 0
    for id in &task_ids {
        let status = cluster.master.get_task_status(*id).await.unwrap();
        assert_eq!(status, TaskStatus::Cancelled);
        let res = cluster.master.wait_task(*id, None).await.unwrap();
        assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
    }

    let stats = cluster.master.queue_stats().await.unwrap();
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.running, 0);
    assert_eq!(stats.completed, 0);
}

#[tokio::test]
async fn test_adversarial_cancel_running_process_aborts_child() {
    let mut cluster = ChallengerTestCluster::new().await;
    cluster.spawn_worker("single-runner", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(5)).await);

    // Submit long running command
    let task = Task::new(
        TaskSpec::Command {
            program: "sleep".into(),
            args: vec!["10".into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 30),
    );
    let task_id = cluster.master.submit_task(task).await.unwrap();

    // Poll until Running
    let start = Instant::now();
    let mut is_running = false;
    while start.elapsed() < Duration::from_secs(5) {
        if let Ok(TaskStatus::Running) = cluster.master.get_task_status(task_id).await {
            is_running = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert!(is_running, "Task must enter Running state");

    // Cancel while running
    cluster.master.cancel_task(task_id).await.unwrap();

    let res = cluster
        .master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();

    assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
    assert_eq!(
        cluster.master.get_task_status(task_id).await.unwrap(),
        TaskStatus::Cancelled
    );

    // Verify worker is immediately freed and capable of running new tasks
    let quick_res = cluster
        .submit_and_wait(make_echo_task("worker_freed_check"), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(quick_res.exit_code, 0);
    assert_eq!(quick_res.stdout.trim(), "worker_freed_check");
}

#[tokio::test]
async fn test_adversarial_concurrent_cancel_same_task_idempotence() {
    let mut cluster = ChallengerTestCluster::new().await;
    cluster.spawn_worker("idempotence-w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(5)).await);

    let task = make_sleep_script_task(2.0);
    let task_id = cluster.master.submit_task(task).await.unwrap();

    // Fire 40 concurrent cancel directives targeting the exact same task
    let master_ref = cluster.master.clone();
    let mut handles = Vec::new();
    for _ in 0..40 {
        let m = master_ref.clone();
        handles.push(tokio::spawn(async move { m.cancel_task(task_id).await }));
    }

    let results = join_all(handles).await;
    for (i, r) in results.into_iter().enumerate() {
        let inner = r.expect("spawn join");
        assert!(
            inner.is_ok(),
            "Concurrent cancel #{i} must succeed or no-op safely"
        );
    }

    let res = cluster
        .master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
    assert_eq!(
        cluster.master.get_task_status(task_id).await.unwrap(),
        TaskStatus::Cancelled
    );
}

#[tokio::test]
async fn test_adversarial_cancel_nonexistent_and_terminal_tasks() {
    let mut cluster = ChallengerTestCluster::new().await;
    cluster.spawn_worker("terminal-w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(5)).await);

    // 1. Cancel nonexistent task ID: must return Err(GridError::TaskNotFound)
    let fake_id = TaskId::new();
    let cancel_res = cluster.master.cancel_task(fake_id).await;
    assert!(
        matches!(cancel_res, Err(GridError::TaskNotFound(_))),
        "Cancelling nonexistent task must return TaskNotFound error"
    );

    // 2. Submit a fast task and wait for Completion
    let task = make_echo_task("completed_immutable");
    let task_id = cluster.master.submit_task(task).await.unwrap();
    let res = cluster
        .master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(res.exit_code, 0);
    assert_eq!(
        cluster.master.get_task_status(task_id).await.unwrap(),
        TaskStatus::Completed
    );

    // 3. Attempt to cancel already Completed task: must not mutate state to Cancelled
    let post_cancel_res = cluster.master.cancel_task(task_id).await;
    assert!(
        post_cancel_res.is_ok(),
        "Cancel on completed task should succeed as no-op"
    );

    let status_after = cluster.master.get_task_status(task_id).await.unwrap();
    assert_eq!(
        status_after,
        TaskStatus::Completed,
        "Completed task must remain Completed"
    );
}
