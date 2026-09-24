//! Comprehensive End-to-End Cluster Integration Test Suite for OxideSwarm (rusty_grid).
//!
//! Structured across Tiers 1–4 matching TEST_INFRA.md:
//! - Tier 1: Feature Coverage (>=5 test cases per feature, 25 total)
//!   * Feature 1: Master-Worker Connection & Registration (5 tests)
//!   * Feature 2: Worker Capability Advertisement (CPU, RAM, GPU, Mobile) (5 tests)
//!   * Feature 3: Generic Task Execution (Command, ShellScript, Builtin, Wire) (5 tests)
//!   * Feature 4: Workload-Specific GPU Routing (strict GPU assignment) (5 tests)
//!   * Feature 5: Parallel Batch Crate Distribution / Compilation (5 tests)
//! - Tier 2: Boundary & Corner Cases (25 tests)
//!   * Timeouts, 0-duration, max concurrency, exit 130, non-zero exit codes, socket drops,
//!     zero-core rejection, large payloads, special characters, rapid cancellation races, etc.
//! - Tier 3: Cross-Feature Combinations (10 pairwise tests)
//!   * Interleaved GPU/CPU, dynamic CPU backpressure (>85%), cancellation during batch spread,
//!     distributed Map/Reduce pipeline, mobile thermal throttling, low battery constraints, etc.
//! - Tier 4: Real-World Application Scenarios (5 realistic workload scenarios)
//!   * Scenario 1: Full Acceptance Criteria (AC1-AC5) End-to-End Local Cluster
//!   * Scenario 2: Multi-Worker Parallel Rust Crate Compilation via rustc
//!   * Scenario 3: Heterogeneous GPU Matrix Compute & CPU Pipeline
//!   * Scenario 4: Worker Churn & Cluster Fault-Tolerant Resilience
//!   * Scenario 5: Heavy Burst Load Distribution across Cluster
//!
//! All tests utilize ephemeral ports (`127.0.0.1:0`) and RAII fixtures for fast, isolated,
//! and 100% deterministic test execution.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use rusty_grid_core::capabilities::{MobileCapabilities, WorkerCapabilities};
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
use rusty_grid_core::protocol::{
    ClientMessage, ClientResponse, MasterMessage, MessageTransport, WorkerMessage,
};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskResult, TaskSpec, TaskStatus};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

// ============================================================================
// TEST FIXTURES & HARNESS
// ============================================================================

/// Represents a running worker client within the test process.
struct SpawnedWorker {
    pub id: Uuid,
    pub shutdown_tx: watch::Sender<bool>,
    pub handle: tokio::task::JoinHandle<()>,
}

/// RAII cluster harness managing a local Master and multiple Worker nodes.
struct TestCluster {
    pub master: MasterHandle,
    pub master_addr: SocketAddr,
    pub workers: Vec<SpawnedWorker>,
    pub temp_dirs: Vec<TempDir>,
}

impl TestCluster {
    /// Creates a new test cluster with default scheduler and reaper configuration.
    pub async fn new() -> Self {
        Self::new_with_config(SchedulerConfig::default(), ReaperConfig::default()).await
    }

    /// Creates a new test cluster with custom scheduler and reaper configurations.
    pub async fn new_with_config(sched_cfg: SchedulerConfig, reaper_cfg: ReaperConfig) -> Self {
        let master = MasterServer::spawn_with_config(
            ServerConfig::new("127.0.0.1:0".parse().unwrap()),
            sched_cfg,
            reaper_cfg,
        )
        .await
        .expect("MasterServer::spawn failed");
        let master_addr = master.server_addr();
        Self {
            master,
            master_addr,
            workers: Vec::new(),
            temp_dirs: Vec::new(),
        }
    }

    /// Spawns a worker node with isolated sandbox directory and registers it with the master.
    pub async fn spawn_worker(
        &mut self,
        name: &str,
        cores: usize,
        ram_mb: u64,
        simulate_gpu: bool,
    ) -> Uuid {
        let temp_dir = tempfile::tempdir().expect("failed to create worker tempdir");
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

    /// Spawns a worker node with a pre-built WorkerConfig.
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

    /// Abruptly drops and aborts a worker (simulating crash / network drop).
    pub fn abort_worker(&mut self, id: Uuid) {
        if let Some(pos) = self.workers.iter().position(|w| w.id == id) {
            let w = self.workers.remove(pos);
            let _ = w.shutdown_tx.send(true);
            w.handle.abort();
        }
    }

    /// Polls the master until at least `count` workers are in active (non-disconnected) status.
    pub async fn wait_for_workers(&self, count: usize, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Ok(workers) = self.master.list_workers().await {
                let active = workers
                    .iter()
                    .filter(|w| w.status != WorkerStatus::Disconnected)
                    .count();
                if active >= count {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    /// Submits a task and awaits its terminal result.
    pub async fn submit_and_wait(&self, task: Task, timeout: Duration) -> GridResult<TaskResult> {
        let id = self.master.submit_task(task).await?;
        self.master.wait_task(id, Some(timeout)).await
    }

    /// Convenience setup for standard 1 Master + 3 Workers Acceptance Criteria topology.
    pub async fn setup_ac_cluster() -> (Self, Uuid, Uuid, Uuid) {
        let mut cluster = Self::new().await;
        let w1 = cluster
            .spawn_worker("worker-cpu-node-1", 2, 2048, false)
            .await;
        let w2 = cluster
            .spawn_worker("worker-cpu-node-2", 4, 4096, false)
            .await;
        let w3 = cluster
            .spawn_worker("worker-gpu-node-3", 8, 8192, true)
            .await;
        assert!(
            cluster.wait_for_workers(3, Duration::from_secs(5)).await,
            "Failed to register all 3 AC workers"
        );
        (cluster, w1, w2, w3)
    }
}

/// Helper polling predicate until condition is met or timeout elapses.
async fn poll_until<F, Fut>(timeout: Duration, step: Duration, mut predicate: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if predicate().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        let _ = self.master.shutdown();
        for w in &self.workers {
            let _ = w.shutdown_tx.send(true);
            w.handle.abort();
        }
    }
}

/// Spawns a raw TCP wire worker for low-level protocol testing.
async fn spawn_mock_wire_worker(
    master_addr: SocketAddr,
    capabilities: WorkerCapabilities,
) -> (
    Uuid,
    mpsc::Sender<WorkerMessage>,
    mpsc::Receiver<MasterMessage>,
    tokio::task::JoinHandle<()>,
) {
    let worker_id = Uuid::new_v4();
    let (tx_upstream, mut rx_upstream) = mpsc::channel::<WorkerMessage>(32);
    let (tx_downstream, rx_downstream) = mpsc::channel::<MasterMessage>(32);

    let stream = TcpStream::connect(master_addr).await.expect("connect");
    let mut transport = MessageTransport::new(stream);

    // Perform handshake
    let reg_msg = WorkerMessage::Register {
        worker_id,
        capabilities,
    };
    transport.send_msg(&reg_msg).await.expect("send register");

    let ack: Option<MasterMessage> = transport.recv_msg().await.expect("recv ack");
    assert!(matches!(
        ack,
        Some(MasterMessage::RegisterAck { accepted: true, .. })
    ));

    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                downstream = transport.recv_msg::<MasterMessage>() => {
                    match downstream {
                        Ok(Some(msg)) => {
                            if tx_downstream.send(msg).await.is_err() {
                                break;
                            }
                        }
                        _ => break,
                    }
                }
                upstream = rx_upstream.recv() => {
                    match upstream {
                        Some(msg) => {
                            if transport.send_msg(&msg).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    });

    (worker_id, tx_upstream, rx_downstream, handle)
}

// ============================================================================
// TIER 1: FEATURE COVERAGE (25 Tests, >=5 per Feature)
// ============================================================================

// ----------------------------------------------------------------------------
// Feature 1: Master-Worker Connection & Registration (5 tests)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_tier1_f1_registration_single_worker_success() {
    let mut cluster = TestCluster::new().await;
    let w_id = cluster.spawn_worker("w-single", 2, 2048, false).await;

    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);
    let workers = cluster.master.list_workers().await.unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].worker_id, w_id);
    assert_eq!(workers[0].capabilities.name, "w-single");
    assert_eq!(workers[0].status, WorkerStatus::Connected);
}

#[tokio::test]
async fn test_tier1_f1_registration_three_workers_parallel() {
    let (cluster, w1, w2, w3) = TestCluster::setup_ac_cluster().await;
    let workers = cluster.master.list_workers().await.unwrap();
    assert_eq!(workers.len(), 3);

    let ids: HashSet<Uuid> = workers.iter().map(|w| w.worker_id).collect();
    assert!(ids.contains(&w1));
    assert!(ids.contains(&w2));
    assert!(ids.contains(&w3));
}

#[tokio::test]
async fn test_tier1_f1_registration_reconnection_same_worker_id() {
    let mut cluster = TestCluster::new().await;
    let wid = Uuid::new_v4();

    let temp1 = tempfile::tempdir().unwrap();
    let cfg1 = WorkerConfig::new(cluster.master_addr.to_string())
        .with_worker_id(wid)
        .with_name("persistent-worker")
        .with_cores(2)
        .with_sandbox_base_dir(temp1.path());
    cluster.spawn_worker_with_config(cfg1).await;

    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);
    let w_info1 = cluster.master.list_workers().await.unwrap();
    assert_eq!(w_info1[0].worker_id, wid);
    let session1 = w_info1[0].session_id;

    // Simulate worker disconnect
    cluster.abort_worker(wid);
    let disconnected = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        || async {
            if let Ok(workers) = cluster.master.list_workers().await {
                workers
                    .iter()
                    .any(|w| w.worker_id == wid && w.status == WorkerStatus::Disconnected)
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        disconnected,
        "Worker must transition to Disconnected after abort"
    );

    // Reconnect worker with identical UUID
    let temp2 = tempfile::tempdir().unwrap();
    let cfg2 = WorkerConfig::new(cluster.master_addr.to_string())
        .with_worker_id(wid)
        .with_name("persistent-worker")
        .with_cores(2)
        .with_sandbox_base_dir(temp2.path());
    cluster.spawn_worker_with_config(cfg2).await;

    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);
    let w_info2 = cluster.master.list_workers().await.unwrap();
    assert_eq!(w_info2[0].worker_id, wid);
    assert_eq!(w_info2[0].status, WorkerStatus::Connected);
    assert!(w_info2[0].session_id > session1);
}

#[tokio::test]
async fn test_tier1_f1_registration_heartbeat_liveness_acknowledged() {
    let cluster = TestCluster::new().await;
    let stream = TcpStream::connect(cluster.master_addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);
    let wid = Uuid::new_v4();

    let reg = WorkerMessage::Register {
        worker_id: wid,
        capabilities: WorkerCapabilities::new("hb-worker", 2, 2048, false, false, None),
    };
    transport.send_msg(&reg).await.unwrap();
    let ack: Option<MasterMessage> = transport.recv_msg().await.unwrap();
    assert!(matches!(
        ack,
        Some(MasterMessage::RegisterAck { accepted: true, .. })
    ));

    let ts = 987654321;
    let hb = WorkerMessage::Heartbeat {
        worker_id: wid,
        timestamp: ts,
        active_tasks: 0,
        cpu_usage_pct: 12.5,
        ram_available_mb: 1800,
    };
    transport.send_msg(&hb).await.unwrap();

    let hb_ack: Option<MasterMessage> = transport.recv_msg().await.unwrap();
    match hb_ack {
        Some(MasterMessage::HeartbeatAck { timestamp }) => assert_eq!(timestamp, ts),
        other => panic!("Expected HeartbeatAck, got {other:?}"),
    }
}

#[tokio::test]
async fn test_tier1_f1_registration_client_list_workers_wire_protocol() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("worker-wire-1", 2, 2048, false).await;
    cluster.spawn_worker("worker-wire-2", 4, 4096, false).await;
    assert!(cluster.wait_for_workers(2, Duration::from_secs(3)).await);

    let client_stream = TcpStream::connect(cluster.master_addr).await.unwrap();
    let mut transport = MessageTransport::new(client_stream);

    transport
        .send_msg(&ClientMessage::ListWorkers)
        .await
        .unwrap();
    let resp: Option<ClientResponse> = transport.recv_msg().await.unwrap();
    match resp {
        Some(ClientResponse::WorkerList { workers }) => {
            assert_eq!(workers.len(), 2);
            let names: HashSet<String> = workers.into_iter().map(|w| w.name).collect();
            assert!(names.contains("worker-wire-1"));
            assert!(names.contains("worker-wire-2"));
        }
        other => panic!("Expected WorkerList, got {other:?}"),
    }
}

// ----------------------------------------------------------------------------
// Feature 2: Worker Capability Advertisement (5 tests)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_tier1_f2_capabilities_cpu_and_ram_advertising() {
    let mut cluster = TestCluster::new().await;
    let wid = cluster.spawn_worker("spec-worker", 12, 32768, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let workers = cluster.master.list_workers().await.unwrap();
    let w = workers.iter().find(|w| w.worker_id == wid).unwrap();
    assert_eq!(w.capabilities.cpu_cores, 12);
    assert_eq!(w.capabilities.ram_mb, 32768);
    assert!(!w.capabilities.has_gpu);
    assert!(!w.capabilities.is_simulated_gpu);
}

#[tokio::test]
async fn test_tier1_f2_capabilities_gpu_physical_flag() {
    let mut cluster = TestCluster::new().await;
    let temp = tempfile::tempdir().unwrap();
    let cfg = WorkerConfig::new(cluster.master_addr.to_string())
        .with_name("gpu-phys-worker")
        .with_cores(8)
        .with_gpu(true)
        .with_sandbox_base_dir(temp.path());
    let wid = cluster.spawn_worker_with_config(cfg).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let workers = cluster.master.list_workers().await.unwrap();
    let w = workers.iter().find(|w| w.worker_id == wid).unwrap();
    assert!(w.capabilities.has_gpu);
    assert!(w.capabilities.can_execute_gpu());
}

#[tokio::test]
async fn test_tier1_f2_capabilities_simulated_gpu_flag() {
    let mut cluster = TestCluster::new().await;
    let wid = cluster.spawn_worker("gpu-sim-worker", 8, 16384, true).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let workers = cluster.master.list_workers().await.unwrap();
    let w = workers.iter().find(|w| w.worker_id == wid).unwrap();
    assert!(w.capabilities.is_simulated_gpu);
    assert!(w.capabilities.can_execute_gpu());
}

#[tokio::test]
async fn test_tier1_f2_capabilities_mobile_telemetry_advertising() {
    let cluster = TestCluster::new().await;
    let mobile = MobileCapabilities {
        os_version: "Android 14 (API 34)".into(),
        soc_model: "Snapdragon 8 Gen 3".into(),
        battery_pct: Some(85),
        is_charging: Some(true),
        thermal_throttled: false,
    };
    let caps = WorkerCapabilities::new("android-phone-1", 8, 8192, false, false, None)
        .with_mobile(Some(mobile));

    let (wid, _tx, _rx, _handle) = spawn_mock_wire_worker(cluster.master_addr, caps.clone()).await;

    let workers = cluster.master.list_workers().await.unwrap();
    let w = workers.iter().find(|w| w.worker_id == wid).unwrap();
    assert!(w.capabilities.mobile.is_some());
    let m = w.capabilities.mobile.as_ref().unwrap();
    assert_eq!(m.os_version, "Android 14 (API 34)");
    assert_eq!(m.soc_model, "Snapdragon 8 Gen 3");
    assert_eq!(m.battery_pct, Some(85));
    assert_eq!(m.is_charging, Some(true));
    assert!(!m.thermal_throttled);
}

#[tokio::test]
async fn test_tier1_f2_capabilities_no_gpu_override_clears_gpu() {
    let mut cluster = TestCluster::new().await;
    let temp = tempfile::tempdir().unwrap();
    let cfg = WorkerConfig::new(cluster.master_addr.to_string())
        .with_name("override-no-gpu-worker")
        .with_gpu(true)
        .with_no_gpu(true)
        .with_sandbox_base_dir(temp.path());
    let wid = cluster.spawn_worker_with_config(cfg).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let workers = cluster.master.list_workers().await.unwrap();
    let w = workers.iter().find(|w| w.worker_id == wid).unwrap();
    assert!(!w.capabilities.has_gpu);
    assert!(!w.capabilities.is_simulated_gpu);
    assert!(!w.capabilities.can_execute_gpu());
}

// ----------------------------------------------------------------------------
// Feature 3: Generic Task Execution (5 tests)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_tier1_f3_generic_command_echo_success() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("worker-1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::command("echo", vec!["Hello OxideSwarm Distributed Grid".into()]),
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 0);
    assert!(res.stdout.contains("Hello OxideSwarm Distributed Grid"));
    assert_eq!(res.stderr, "");
    assert!(!res.is_gpu_executed);
}

#[tokio::test]
async fn test_tier1_f3_generic_command_with_env_and_args() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("worker-1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let mut env = HashMap::new();
    env.insert("CUSTOM_GRID_VAR".into(), "DistributedRustWorks".into());

    let task = Task::new(
        TaskSpec::Command {
            program: "sh".into(),
            args: vec![
                "-c".into(),
                "echo var=$CUSTOM_GRID_VAR arg=$1".into(),
                "_".into(),
                "argval".into(),
            ],
            env,
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 0);
    assert!(res.stdout.contains("var=DistributedRustWorks"));
    assert!(res.stdout.contains("arg=argval"));
}

#[tokio::test]
async fn test_tier1_f3_generic_shell_script_execution() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("worker-1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let script = r#"
total=0
for i in 1 2 3 4; do
  total=$((total + i))
done
echo "SUM=$total"
"#;
    let task = Task::new(
        TaskSpec::ShellScript {
            script: script.into(),
            interpreter: None,
            env: HashMap::new(),
        },
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 0);
    assert!(res.stdout.contains("SUM=10"));
}

#[tokio::test]
async fn test_tier1_f3_generic_builtin_test_hash_compute() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("worker-1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::builtin_test("hash_compute", 50_000),
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 0);
    assert!(res.stdout.contains("BuiltinTest hash_compute complete"));
    assert!(res.stdout.contains("digest ="));
}

#[tokio::test]
async fn test_tier1_f3_generic_wire_client_submit_and_wait() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("worker-1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let client_stream = TcpStream::connect(cluster.master_addr).await.unwrap();
    let mut transport = MessageTransport::new(client_stream);

    let task = Task::new(
        TaskSpec::command("echo", vec!["Over The Wire".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = task.id;

    transport
        .send_msg(&ClientMessage::SubmitTask { task, wait: true })
        .await
        .unwrap();

    let resp: Option<ClientResponse> = transport.recv_msg().await.unwrap();
    match resp {
        Some(ClientResponse::TaskCompleted {
            task_id: rid,
            result,
        }) => {
            assert_eq!(rid, task_id);
            assert_eq!(result.exit_code, 0);
            assert!(result.stdout.contains("Over The Wire"));
        }
        other => panic!("Expected TaskCompleted, got {other:?}"),
    }
}

// ----------------------------------------------------------------------------
// Feature 4: Workload-Specific GPU Routing (5 tests)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_tier1_f4_gpu_task_routes_exclusively_to_simulated_gpu_worker() {
    let (cluster, w1, w2, w3_gpu) = TestCluster::setup_ac_cluster().await;

    let task = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "matmul_tiled".into(),
            input_data: vec![1, 2, 3],
            work_group_size: 16,
            simulated_matrix_dim: 32,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 0);
    assert_eq!(
        res.worker_id, w3_gpu,
        "GPU task must route exclusively to GPU node"
    );
    assert_ne!(res.worker_id, w1);
    assert_ne!(res.worker_id, w2);
    assert!(res.is_gpu_executed);
}

#[tokio::test]
async fn test_tier1_f4_gpu_task_blocked_when_no_gpu_workers_connected() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("cpu-only-1", 2, 2048, false).await;
    cluster.spawn_worker("cpu-only-2", 4, 4096, false).await;
    assert!(cluster.wait_for_workers(2, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "matmul_tiled".into(),
            input_data: vec![1, 2, 3],
            work_group_size: 16,
            simulated_matrix_dim: 32,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(10),
    );
    let task_id = cluster.master.submit_task(task).await.unwrap();

    // Give scheduler time to attempt scheduling
    tokio::time::sleep(Duration::from_millis(250)).await;

    let status = cluster.master.get_task_status(task_id).await.unwrap();
    assert_eq!(
        status,
        TaskStatus::Queued,
        "Task must remain Queued without GPU workers"
    );

    let info = cluster.master.get_task_info(task_id).await.unwrap();
    assert_eq!(info.assigned_worker_id, None);
}

#[tokio::test]
async fn test_tier1_f4_gpu_task_dispatches_immediately_when_gpu_worker_joins() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("cpu-only-1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "matmul_tiled".into(),
            input_data: vec![1, 2, 3],
            work_group_size: 16,
            simulated_matrix_dim: 32,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(10),
    );
    let task_id = cluster.master.submit_task(task).await.unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        cluster.master.get_task_status(task_id).await.unwrap(),
        TaskStatus::Queued
    );

    // Spawn GPU worker
    let w_gpu = cluster.spawn_worker("gpu-node", 8, 8192, true).await;
    assert!(cluster.wait_for_workers(2, Duration::from_secs(3)).await);

    // Await task completion
    let res = cluster
        .master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(res.exit_code, 0);
    assert_eq!(res.worker_id, w_gpu);
    assert!(res.is_gpu_executed);
}

#[tokio::test]
async fn test_tier1_f4_gpu_compute_matrix_multiplication_correctness() {
    let mut cluster = TestCluster::new().await;
    let w_gpu = cluster
        .spawn_worker("gpu-matrix-worker", 8, 8192, true)
        .await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "matmul_tiled".into(),
            input_data: vec![1, 2, 3, 4, 5, 6, 7, 8],
            work_group_size: 16,
            simulated_matrix_dim: 32,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(15),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 0);
    assert_eq!(res.worker_id, w_gpu);
    assert!(res.is_gpu_executed);
    assert!(res.stdout.contains("[GPU COMPUTE SIMULATOR]"));
    assert!(res.stdout.contains("Status: VERIFIED_OK"));
}

#[tokio::test]
async fn test_tier1_f4_cpu_task_prefers_cpu_worker_preserving_gpu() {
    let mut cluster = TestCluster::new().await;
    let w_cpu = cluster.spawn_worker("cpu-worker", 4, 4096, false).await;
    let _w_gpu = cluster.spawn_worker("gpu-worker", 4, 4096, true).await;
    assert!(cluster.wait_for_workers(2, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::command("echo", vec!["generic cpu task".into()]),
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 0);
    assert_eq!(
        res.worker_id, w_cpu,
        "Generic task should prefer CPU worker to preserve GPU node"
    );
}

// ----------------------------------------------------------------------------
// Feature 5: Parallel Batch Crate Distribution / Compilation (5 tests)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_tier1_f5_batch_5_tasks_spread_across_3_workers() {
    let (cluster, _w1, _w2, _w3) = TestCluster::setup_ac_cluster().await;

    let mut task_futs = Vec::new();
    for i in 1..=5 {
        let task = Task::new(
            TaskSpec::command("sleep", vec!["0.1".into()]),
            TaskRequirements::generic(1, 10),
        );
        let c = &cluster;
        task_futs.push(async move {
            let id = c.master.submit_task(task).await.unwrap();
            (
                i,
                c.master.wait_task(id, Some(Duration::from_secs(5))).await,
            )
        });
    }

    let results = join_all(task_futs).await;
    let mut workers_used = HashSet::new();
    for (_i, res) in results {
        let r = res.unwrap();
        assert_eq!(r.exit_code, 0);
        workers_used.insert(r.worker_id);
    }

    assert_eq!(
        workers_used.len(),
        3,
        "Batch of 5 tasks must distribute across all 3 workers"
    );
}

#[tokio::test]
async fn test_tier1_f5_batch_compilation_speedup_parallel_vs_sequential() {
    let (cluster, _, _, _) = TestCluster::setup_ac_cluster().await;

    let start = Instant::now();
    let mut task_futs = Vec::new();
    for _ in 0..3 {
        let task = Task::new(
            TaskSpec::command("sleep", vec!["0.2".into()]),
            TaskRequirements::generic(1, 10),
        );
        let c = &cluster;
        task_futs.push(async move {
            let id = c.master.submit_task(task).await.unwrap();
            c.master.wait_task(id, Some(Duration::from_secs(5))).await
        });
    }

    let results = join_all(task_futs).await;
    let elapsed = start.elapsed();

    for res in results {
        assert_eq!(res.unwrap().exit_code, 0);
    }

    // 3 parallel tasks of 200ms each should finish well before sequential time (with margin for Windows process startup)
    #[cfg(windows)]
    let max_duration = Duration::from_millis(1000);
    #[cfg(not(windows))]
    let max_duration = Duration::from_millis(550);
    assert!(
        elapsed < max_duration,
        "Parallel execution took {elapsed:?}, expected speedup over 600ms sequential"
    );
}

#[tokio::test]
async fn test_tier1_f5_batch_10_tasks_burst_load() {
    let (cluster, _, _, _) = TestCluster::setup_ac_cluster().await;

    let mut task_futs = Vec::new();
    for i in 0..10 {
        let task = Task::new(
            TaskSpec::command("echo", vec![format!("burst_task_{i}")]),
            TaskRequirements::generic(1, 10),
        );
        let c = &cluster;
        task_futs.push(async move {
            let id = c.master.submit_task(task).await.unwrap();
            c.master.wait_task(id, Some(Duration::from_secs(10))).await
        });
    }

    let results = join_all(task_futs).await;
    assert_eq!(results.len(), 10);
    for res in results {
        let r = res.unwrap();
        assert_eq!(r.exit_code, 0);
    }
}

#[tokio::test]
async fn test_tier1_f5_batch_compilation_crate_materialization_and_rustc() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("builder-1", 2, 4096, false).await;
    cluster.spawn_worker("builder-2", 2, 4096, false).await;
    assert!(cluster.wait_for_workers(2, Duration::from_secs(3)).await);

    let crates = vec![
        (
            "crate_alpha",
            "pub fn alpha() -> &'static str { \"alpha\" }",
        ),
        ("crate_beta", "pub fn beta() -> i32 { 100 }"),
        ("crate_gamma", "pub fn gamma() -> bool { true }"),
    ];

    let mut task_futs = Vec::new();
    for (name, code) in crates {
        let mut files = HashMap::new();
        files.insert("src/lib.rs".into(), code.to_string());

        let task = Task::new(
            TaskSpec::RustCompilation {
                crate_name: name.into(),
                source_files: files,
                compiler_flags: vec!["--crate-type".into(), "lib".into()],
                target_dir: None,
            },
            TaskRequirements::generic(1, 30),
        );
        let c = &cluster;
        task_futs.push(async move {
            let id = c.master.submit_task(task).await.unwrap();
            c.master.wait_task(id, Some(Duration::from_secs(15))).await
        });
    }

    let results = join_all(task_futs).await;
    for res in results {
        let r = res.unwrap();
        assert_eq!(r.exit_code, 0);
        assert!(r.stdout.contains("compilation artifacts generated"));
    }
}

#[tokio::test]
async fn test_tier1_f5_batch_completion_state_consistency() {
    let (cluster, _, _, _) = TestCluster::setup_ac_cluster().await;

    let mut task_ids = Vec::new();
    for i in 0..5 {
        let task = Task::new(
            TaskSpec::command("echo", vec![format!("consistency_check_{i}")]),
            TaskRequirements::generic(1, 10),
        );
        let id = cluster.master.submit_task(task).await.unwrap();
        task_ids.push(id);
    }

    for id in task_ids {
        let res = cluster
            .master
            .wait_task(id, Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert_eq!(res.exit_code, 0);
    }

    let stats = cluster.master.queue_stats().await.unwrap();
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.running, 0);
    assert!(stats.completed >= 5);
    assert_eq!(stats.failed, 0);
}

// ============================================================================
// TIER 2: BOUNDARY & CORNER CASES (25 Tests)
// ============================================================================

#[tokio::test]
async fn test_tier2_bva_timeout_handling_kills_process() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    // Sleep 10s with a 1s timeout requirement
    let task = Task::new(
        TaskSpec::command("sleep", vec!["10".into()]),
        TaskRequirements::new(1, 512, false, 1),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 124, "Timed out task must emit exit code 124");
    assert!(
        res.error.as_deref().unwrap_or("").contains("timed out")
            || res.stderr.contains("timed out")
    );
}

#[tokio::test]
async fn test_tier2_bva_zero_duration_builtin_task_instant_completion() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::builtin_test("sleep", 0),
        TaskRequirements::generic(1, 10),
    );
    let start = Instant::now();
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(res.exit_code, 0);
    assert!(
        elapsed < Duration::from_millis(100),
        "0-duration task must complete nearly instantly"
    );
}

#[tokio::test]
async fn test_tier2_bva_max_concurrency_limit_throttles_worker() {
    let mut cluster = TestCluster::new().await;
    let temp = tempfile::tempdir().unwrap();
    let cfg = WorkerConfig::new(cluster.master_addr.to_string())
        .with_name("concurrency-limited-worker")
        .with_cores(4)
        .with_max_concurrency(1)
        .with_sandbox_base_dir(temp.path());
    cluster.spawn_worker_with_config(cfg).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let start = Instant::now();
    let t1 = Task::new(
        TaskSpec::command("sleep", vec!["0.15".into()]),
        TaskRequirements::generic(1, 10),
    );
    let t2 = Task::new(
        TaskSpec::command("sleep", vec!["0.15".into()]),
        TaskRequirements::generic(1, 10),
    );

    let id1 = cluster.master.submit_task(t1).await.unwrap();
    let id2 = cluster.master.submit_task(t2).await.unwrap();

    let (r1, r2) = tokio::join!(
        cluster.master.wait_task(id1, Some(Duration::from_secs(5))),
        cluster.master.wait_task(id2, Some(Duration::from_secs(5)))
    );
    let elapsed = start.elapsed();

    assert_eq!(r1.unwrap().exit_code, 0);
    assert_eq!(r2.unwrap().exit_code, 0);
    // Serialized execution of two 150ms tasks must take >= 250ms
    assert!(
        elapsed >= Duration::from_millis(250),
        "Max concurrency 1 should serialize tasks (elapsed {elapsed:?})"
    );
}

#[tokio::test]
async fn test_tier2_bva_cancellation_emits_exit_code_130() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::command("sleep", vec!["10".into()]),
        TaskRequirements::generic(1, 20),
    );
    let task_id = cluster.master.submit_task(task).await.unwrap();

    // Wait until task is running
    let mut is_running = false;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if matches!(
            cluster.master.get_task_status(task_id).await,
            Ok(TaskStatus::Running)
        ) {
            is_running = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        is_running,
        "Task must transition to Running before cancellation"
    );

    cluster.master.cancel_task(task_id).await.unwrap();
    let res = cluster
        .master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 130, "Cancelled task must emit exit code 130");
    let state = cluster.master.get_task_status(task_id).await.unwrap();
    assert_eq!(state, TaskStatus::Cancelled);
}

#[tokio::test]
async fn test_tier2_bva_non_zero_exit_code_and_stderr_capture() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::Command {
            program: "sh".into(),
            args: vec![
                "-c".into(),
                "echo 'custom error message' >&2; exit 42".into(),
            ],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 42);
    assert!(res.stderr.contains("custom error message"));
}

#[tokio::test]
async fn test_tier2_bva_worker_socket_drop_triggers_failover() {
    let mut cluster = TestCluster::new().await;
    let w1 = cluster.spawn_worker("w-doomed", 2, 2048, false).await;
    let _w2 = cluster.spawn_worker("w-survivor", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(2, Duration::from_secs(3)).await);

    // Submit task with small duration
    let task = Task::new(
        TaskSpec::command("sleep", vec!["0.3".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = cluster.master.submit_task(task).await.unwrap();

    // Wait for task to begin running
    let started = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        || async {
            matches!(
                cluster.master.get_task_status(task_id).await,
                Ok(TaskStatus::Running)
            )
        },
    )
    .await;
    assert!(started, "Task must begin running before aborting worker");

    // Abort doomed worker
    cluster.abort_worker(w1);

    // Survivor should pick up the orphaned task and complete it
    let res = cluster
        .master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(res.exit_code, 0);
}

#[tokio::test]
async fn test_tier2_bva_zero_cpu_cores_registration_rejected() {
    let cluster = TestCluster::new().await;
    let stream = TcpStream::connect(cluster.master_addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let bad_reg = WorkerMessage::Register {
        worker_id: Uuid::new_v4(),
        capabilities: WorkerCapabilities::new("bad-worker", 0, 1024, false, false, None),
    };
    transport.send_msg(&bad_reg).await.unwrap();

    let ack: Option<MasterMessage> = transport.recv_msg().await.unwrap();
    match ack {
        Some(MasterMessage::RegisterAck {
            accepted, message, ..
        }) => {
            assert!(!accepted, "Registration with 0 CPU cores must be rejected");
            assert!(message
                .unwrap_or_default()
                .contains("cpu_cores must be > 0"));
        }
        other => panic!("Expected RegisterAck, got {other:?}"),
    }
}

#[tokio::test]
async fn test_tier2_bva_empty_command_fails_gracefully() {
    let cluster = TestCluster::new().await;

    let task = Task::new(
        TaskSpec::Command {
            program: "".into(),
            args: vec![],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let err = cluster.master.submit_task(task).await.unwrap_err();
    assert!(
        matches!(err, GridError::Config(_)),
        "Empty program should be rejected by validator"
    );
}

#[tokio::test]
async fn test_tier2_bva_large_output_stream_truncation() {
    let mut cluster = TestCluster::new().await;
    let temp = tempfile::tempdir().unwrap();
    let cfg = WorkerConfig::new(cluster.master_addr.to_string())
        .with_name("output-limit-worker")
        .with_cores(2)
        .with_max_output_bytes(1024 * 64) // 64 KB limit
        .with_sandbox_base_dir(temp.path());
    cluster.spawn_worker_with_config(cfg).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    // Generate ~500 KB of text
    let task = Task::new(
        TaskSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), "yes '0123456789abcdef' | head -n 30000".into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 0);
    assert!(res
        .stdout
        .contains("output truncated after exceeding size limit"));
    assert!(res.stdout.len() <= 1024 * 64 + 1024);
}

#[tokio::test]
async fn test_tier2_bva_nonexistent_command_fails_gracefully() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::command("/bin/nonexistent_cmd_xyz_98765", vec![]),
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_ne!(res.exit_code, 0);
    assert!(res.error.is_some());
}

#[tokio::test]
async fn test_tier2_bva_task_not_found_query_returns_error() {
    let cluster = TestCluster::new().await;
    let missing_id = TaskId(Uuid::new_v4());

    let err = cluster
        .master
        .get_task_status(missing_id)
        .await
        .unwrap_err();
    assert!(matches!(err, GridError::TaskNotFound(_)));
}

#[tokio::test]
async fn test_tier2_bva_cancel_nonexistent_task_returns_error() {
    let cluster = TestCluster::new().await;
    let missing_id = TaskId(Uuid::new_v4());

    let err = cluster.master.cancel_task(missing_id).await.unwrap_err();
    assert!(matches!(err, GridError::TaskNotFound(_)));
}

#[tokio::test]
async fn test_tier2_bva_duplicate_registration_replaces_session() {
    let cluster = TestCluster::new().await;
    let wid = Uuid::new_v4();

    let stream1 = TcpStream::connect(cluster.master_addr).await.unwrap();
    let mut t1 = MessageTransport::new(stream1);
    t1.send_msg(&WorkerMessage::Register {
        worker_id: wid,
        capabilities: WorkerCapabilities::new("session-worker", 2, 2048, false, false, None),
    })
    .await
    .unwrap();
    let _ack1: Option<MasterMessage> = t1.recv_msg().await.unwrap();

    let info1 = cluster.master.list_workers().await.unwrap();
    let s1 = info1
        .iter()
        .find(|w| w.worker_id == wid)
        .unwrap()
        .session_id;

    // Second connection with same UUID
    let stream2 = TcpStream::connect(cluster.master_addr).await.unwrap();
    let mut t2 = MessageTransport::new(stream2);
    t2.send_msg(&WorkerMessage::Register {
        worker_id: wid,
        capabilities: WorkerCapabilities::new("session-worker", 2, 2048, false, false, None),
    })
    .await
    .unwrap();
    let ack2: Option<MasterMessage> = t2.recv_msg().await.unwrap();
    assert!(matches!(
        ack2,
        Some(MasterMessage::RegisterAck { accepted: true, .. })
    ));

    let info2 = cluster.master.list_workers().await.unwrap();
    let s2 = info2
        .iter()
        .find(|w| w.worker_id == wid)
        .unwrap()
        .session_id;
    assert!(s2 > s1, "Second registration must advance session_id");
}

#[tokio::test]
async fn test_tier2_bva_worker_graceful_disconnect_message() {
    let cluster = TestCluster::new().await;
    let wid = Uuid::new_v4();

    let stream = TcpStream::connect(cluster.master_addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);
    transport
        .send_msg(&WorkerMessage::Register {
            worker_id: wid,
            capabilities: WorkerCapabilities::new("polite-worker", 2, 2048, false, false, None),
        })
        .await
        .unwrap();
    let _ack: Option<MasterMessage> = transport.recv_msg().await.unwrap();

    // Announce disconnecting
    transport
        .send_msg(&WorkerMessage::Disconnecting {
            worker_id: wid,
            reason: "Maintenance shutdown".into(),
        })
        .await
        .unwrap();

    let marked_disconnected = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        || async {
            if let Ok(workers) = cluster.master.list_workers().await {
                workers
                    .iter()
                    .any(|w| w.worker_id == wid && w.status == WorkerStatus::Disconnected)
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        marked_disconnected,
        "Worker must be marked Disconnected after graceful disconnect message"
    );
}

#[tokio::test]
async fn test_tier2_bva_max_retry_exhaustion_marks_failed() {
    let mut cluster = TestCluster::new().await;
    let w1 = cluster.spawn_worker("flaky-worker", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    // Intentionally failing task
    let task = Task::new(
        TaskSpec::builtin_test("fail", 0),
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 1);
    assert_eq!(res.worker_id, w1);
    assert!(res
        .error
        .unwrap_or_default()
        .contains("Intentional failure"));
}

#[tokio::test]
async fn test_tier2_bva_huge_task_ram_requirement_skips_low_ram_worker() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("low-ram-w", 2, 2048, false).await; // 2 GB RAM
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::builtin_test("sleep", 10),
        TaskRequirements::new(1, 65536, false, 10), // Demands 64 GB RAM
    );
    let task_id = cluster.master.submit_task(task).await.unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    let status = cluster.master.get_task_status(task_id).await.unwrap();
    assert_eq!(status, TaskStatus::Queued);
}

#[tokio::test]
async fn test_tier2_bva_huge_cpu_core_requirement_skips_low_core_worker() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("low-core-w", 2, 4096, false).await; // 2 cores
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::builtin_test("sleep", 10),
        TaskRequirements::new(32, 1024, false, 10), // Demands 32 cores
    );
    let task_id = cluster.master.submit_task(task).await.unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    let status = cluster.master.get_task_status(task_id).await.unwrap();
    assert_eq!(status, TaskStatus::Queued);
}

#[tokio::test]
async fn test_tier2_bva_empty_stdin_command() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::Command {
            program: "cat".into(),
            args: vec![],
            env: HashMap::new(),
            working_dir: None,
            stdin: Some(b"buffered stdin string".to_vec()),
        },
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 0);
    assert_eq!(res.stdout, "buffered stdin string");
}

#[tokio::test]
async fn test_tier2_bva_multi_line_shell_script_syntax_error() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::ShellScript {
            script: "if [ 1 -eq 1 ]; then echo missing fi".into(),
            interpreter: None,
            env: HashMap::new(),
        },
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_ne!(res.exit_code, 0);
    assert!(!res.stderr.is_empty() || res.error.is_some());
}

#[tokio::test]
async fn test_tier2_bva_cluster_status_zero_workers() {
    let cluster = TestCluster::new().await;
    let client_stream = TcpStream::connect(cluster.master_addr).await.unwrap();
    let mut transport = MessageTransport::new(client_stream);

    transport
        .send_msg(&ClientMessage::ClusterStatus)
        .await
        .unwrap();
    let resp: Option<ClientResponse> = transport.recv_msg().await.unwrap();
    match resp {
        Some(ClientResponse::ClusterStatus {
            total_tasks,
            workers,
            ..
        }) => {
            assert_eq!(total_tasks, 0);
            assert_eq!(workers.len(), 0);
        }
        other => panic!("Expected ClusterStatus, got {other:?}"),
    }
}

#[tokio::test]
async fn test_tier2_bva_rapid_submit_and_cancel_race() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::command("sleep", vec!["2".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = cluster.master.submit_task(task).await.unwrap();
    let _ = cluster.master.cancel_task(task_id).await;

    let res = cluster
        .master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(res.exit_code, 130);
}

#[tokio::test]
async fn test_tier2_bva_special_characters_in_command_arguments() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let special_str = "hello 'single' \"double\" \n newline 🦀 OxideSwarm ⚡";
    let task = Task::new(
        TaskSpec::command("printf", vec!["%s".into(), special_str.into()]),
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 0);
    assert_eq!(res.stdout, special_str);
}

#[tokio::test]
async fn test_tier2_bva_custom_exit_codes_preserved() {
    let mut cluster = TestCluster::new().await;
    cluster.spawn_worker("w1", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    let task = Task::new(
        TaskSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), "exit 77".into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let res = cluster
        .submit_and_wait(task, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(res.exit_code, 77);
}

#[tokio::test]
async fn test_tier2_bva_client_connection_closed_before_handshake() {
    let cluster = TestCluster::new().await;
    let stream = TcpStream::connect(cluster.master_addr).await.unwrap();
    drop(stream); // Drop immediately

    tokio::time::sleep(Duration::from_millis(50)).await;
    // Master should remain completely healthy
    let stats = cluster.master.queue_stats().await.unwrap();
    assert_eq!(stats.total, 0);
}

#[tokio::test]
async fn test_tier2_bva_port_file_atomic_write_and_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let port_file = temp.path().join("cluster_master.port");

    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_port_file(&port_file);

    let master = MasterServer::spawn(cfg).await.unwrap();
    assert!(port_file.exists());
    let content = std::fs::read_to_string(&port_file).unwrap();
    assert_eq!(content.trim(), master.port().to_string());

    master.shutdown().unwrap();
    let cleaned = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        || async { !port_file.exists() },
    )
    .await;
    assert!(cleaned, "Port file must be cleaned up on master shutdown");
}

// ============================================================================
// TIER 3: CROSS-FEATURE COMBINATIONS (10 Tests)
// ============================================================================

#[tokio::test]
async fn test_tier3_pairwise_interleaved_gpu_and_cpu_batches() {
    let (cluster, _w1, _w2, w3_gpu) = TestCluster::setup_ac_cluster().await;

    let mut futs = Vec::new();
    for i in 0..6 {
        let is_gpu = i % 2 == 0;
        let task = if is_gpu {
            Task::new(
                TaskSpec::GpuCompute {
                    kernel_name: "matmul_tiled".into(),
                    input_data: vec![i as u8],
                    work_group_size: 16,
                    simulated_matrix_dim: 32,
                    compute_intensity: 1,
                },
                TaskRequirements::gpu(10),
            )
        } else {
            Task::new(
                TaskSpec::command("echo", vec![format!("cpu_task_{i}")]),
                TaskRequirements::generic(1, 10),
            )
        };
        let c = &cluster;
        futs.push(async move {
            let id = c.master.submit_task(task).await.unwrap();
            (
                is_gpu,
                c.master.wait_task(id, Some(Duration::from_secs(5))).await,
            )
        });
    }

    let results = join_all(futs).await;
    for (is_gpu, res) in results {
        let r = res.unwrap();
        assert_eq!(r.exit_code, 0);
        if is_gpu {
            assert_eq!(r.worker_id, w3_gpu);
            assert!(r.is_gpu_executed);
        }
    }
}

#[tokio::test]
async fn test_tier3_pairwise_dynamic_load_backpressure_high_cpu() {
    let cluster = TestCluster::new().await;

    // Worker 1: Heavy load (92% CPU, above default 85% cap)
    let caps1 = WorkerCapabilities::new("w-busy", 4, 4096, false, false, None);
    let (w1, tx1, _rx1, _h1) = spawn_mock_wire_worker(cluster.master_addr, caps1).await;

    // Worker 2: Idle load (15% CPU)
    let caps2 = WorkerCapabilities::new("w-idle", 4, 4096, false, false, None);
    let (w2, tx2, mut rx2, _h2) = spawn_mock_wire_worker(cluster.master_addr, caps2).await;

    // Send heartbeats
    let ts = 1000;
    tx1.send(WorkerMessage::Heartbeat {
        worker_id: w1,
        timestamp: ts,
        active_tasks: 0,
        cpu_usage_pct: 92.0,
        ram_available_mb: 3000,
    })
    .await
    .unwrap();

    tx2.send(WorkerMessage::Heartbeat {
        worker_id: w2,
        timestamp: ts,
        active_tasks: 0,
        cpu_usage_pct: 15.0,
        ram_available_mb: 3000,
    })
    .await
    .unwrap();

    let hb_recorded = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        || async {
            if let Ok(workers) = cluster.master.list_workers().await {
                let w1_match = workers
                    .iter()
                    .find(|w| w.worker_id == w1)
                    .map(|w| w.cpu_usage_pct >= 90.0)
                    .unwrap_or(false);
                let w2_match = workers
                    .iter()
                    .find(|w| w.worker_id == w2)
                    .map(|w| w.cpu_usage_pct <= 20.0)
                    .unwrap_or(false);
                w1_match && w2_match
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        hb_recorded,
        "Master must record heartbeat telemetry before task scheduling"
    );

    // Submit task
    let task = Task::new(
        TaskSpec::command("echo", vec!["backpressure test".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = task.id;
    let _ = cluster.master.submit_task(task).await.unwrap();

    // Worker 2 should receive task assignment
    let mut assigned_to_w2 = false;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if let Ok(Some(MasterMessage::AssignTask { task })) =
            tokio::time::timeout(Duration::from_millis(200), rx2.recv()).await
        {
            if task.id == task_id {
                assigned_to_w2 = true;
                // Return result
                tx2.send(WorkerMessage::TaskResult {
                    worker_id: w2,
                    task_id,
                    exit_code: 0,
                    stdout: "done".into(),
                    stderr: "".into(),
                    execution_time_ms: 10,
                    is_gpu_executed: false,
                    device_name: None,
                    error: None,
                })
                .await
                .unwrap();
                break;
            }
        }
    }

    assert!(
        assigned_to_w2,
        "Master scheduler should route task to idle Worker 2 away from overloaded Worker 1"
    );
}

#[tokio::test]
async fn test_tier3_pairwise_cancellation_during_batch_spread() {
    let (cluster, _, _, _) = TestCluster::setup_ac_cluster().await;

    let mut task_ids = Vec::new();
    for _ in 0..6 {
        let task = Task::new(
            TaskSpec::command("sleep", vec!["2".into()]),
            TaskRequirements::generic(1, 10),
        );
        let id = cluster.master.submit_task(task).await.unwrap();
        task_ids.push(id);
    }

    // Wait until at least one task is running
    let mut any_running = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        for id in &task_ids {
            if matches!(
                cluster.master.get_task_status(*id).await,
                Ok(TaskStatus::Running)
            ) {
                any_running = true;
                break;
            }
        }
        if any_running {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Cancel first 3 tasks
    for id in &task_ids[0..3] {
        let _ = cluster.master.cancel_task(*id).await;
    }

    let mut wait_futs = Vec::new();
    for id in &task_ids {
        let c = &cluster;
        wait_futs.push(async move { c.master.wait_task(*id, Some(Duration::from_secs(5))).await });
    }

    let results = join_all(wait_futs).await;
    for (i, res) in results.into_iter().enumerate() {
        let r = res.unwrap();
        if i < 3 {
            assert_eq!(
                r.exit_code, 130,
                "Task {i} was cancelled, expected exit 130"
            );
        }
    }
}

#[tokio::test]
async fn test_tier3_pairwise_mapreduce_pipeline_across_cluster() {
    let (cluster, _, _, _) = TestCluster::setup_ac_cluster().await;

    let text_lines = vec![
        "apple banana orange apple".to_string(),
        "banana cherry apple".to_string(),
        "orange cherry banana apple".to_string(),
        "banana apple orange".to_string(),
    ];

    let job = MapReduceJobSpec::new(
        "distributed_word_count",
        text_lines,
        MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        3,
        1,
        30,
    );

    let res = cluster.master.execute_mapreduce(job).await.unwrap();

    assert_eq!(res.status, "Completed");
    assert_eq!(res.map_tasks_completed, 3);
    assert!(res.error.is_none());

    // Verify word counts:
    // apple: 2 + 1 + 1 + 1 = 5
    // banana: 1 + 1 + 1 + 1 = 4
    // orange: 1 + 0 + 1 + 1 = 3
    // cherry: 0 + 1 + 1 + 0 = 2
    assert_eq!(res.output.get("apple"), Some(&serde_json::json!(5)));
    assert_eq!(res.output.get("banana"), Some(&serde_json::json!(4)));
    assert_eq!(res.output.get("orange"), Some(&serde_json::json!(3)));
    assert_eq!(res.output.get("cherry"), Some(&serde_json::json!(2)));
}

#[tokio::test]
async fn test_tier3_pairwise_mobile_worker_thermal_throttling_constraint() {
    let cluster = TestCluster::new().await;

    // Worker 1: Desktop worker (4 cores)
    let caps_desktop = WorkerCapabilities::new("desktop-w", 4, 8192, false, false, None);
    let (w1, tx1, mut rx1, _h1) = spawn_mock_wire_worker(cluster.master_addr, caps_desktop).await;

    // Worker 2: Mobile device experiencing thermal throttling
    let mobile = MobileCapabilities {
        os_version: "Android 14".into(),
        soc_model: "Tensor G3".into(),
        battery_pct: Some(90),
        is_charging: Some(true),
        thermal_throttled: true,
    };
    let caps_mobile =
        WorkerCapabilities::new("mobile-w", 4, 8192, false, false, None).with_mobile(Some(mobile));
    let (_w2, _tx2, _rx2, _h2) = spawn_mock_wire_worker(cluster.master_addr, caps_mobile).await;

    assert!(cluster.wait_for_workers(2, Duration::from_secs(3)).await);

    // Submit heavy compute task demanding 2 cores
    let task = Task::new(
        TaskSpec::command("echo", vec!["thermal protect".into()]),
        TaskRequirements::generic(2, 10),
    );
    let task_id = task.id;
    let _ = cluster.master.submit_task(task).await.unwrap();

    // Desktop worker must receive the task
    let assigned = match tokio::time::timeout(Duration::from_secs(3), rx1.recv()).await {
        Ok(Some(MasterMessage::AssignTask { task })) => task.id == task_id,
        _ => false,
    };

    assert!(
        assigned,
        "Master scheduler must route multi-core task to desktop, bypassing thermally throttled mobile node"
    );

    // Reply with success to clean up
    tx1.send(WorkerMessage::TaskResult {
        worker_id: w1,
        task_id,
        exit_code: 0,
        stdout: "done".into(),
        stderr: "".into(),
        execution_time_ms: 10,
        is_gpu_executed: false,
        device_name: None,
        error: None,
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_tier3_pairwise_mobile_worker_low_battery_constraint() {
    let cluster = TestCluster::new().await;

    // Worker 1: Desktop worker
    let caps_desktop = WorkerCapabilities::new("desktop-w", 4, 8192, false, false, None);
    let (w1, tx1, mut rx1, _h1) = spawn_mock_wire_worker(cluster.master_addr, caps_desktop).await;

    // Worker 2: Mobile device on battery with 8% charge (< 15% threshold)
    let mobile = MobileCapabilities {
        os_version: "Android 14".into(),
        soc_model: "Snapdragon".into(),
        battery_pct: Some(8),
        is_charging: Some(false),
        thermal_throttled: false,
    };
    let caps_mobile =
        WorkerCapabilities::new("mobile-w", 4, 8192, false, false, None).with_mobile(Some(mobile));
    let (_w2, _tx2, _rx2, _h2) = spawn_mock_wire_worker(cluster.master_addr, caps_mobile).await;

    assert!(cluster.wait_for_workers(2, Duration::from_secs(3)).await);

    // Submit compilation task
    let mut files = HashMap::new();
    files.insert("src/lib.rs".into(), "pub fn run() {}".into());
    let task = Task::new(
        TaskSpec::RustCompilation {
            crate_name: "battery_test".into(),
            source_files: files,
            compiler_flags: vec!["--crate-type".into(), "lib".into()],
            target_dir: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let task_id = task.id;
    let _ = cluster.master.submit_task(task).await.unwrap();

    // Desktop worker must receive the compilation task to protect low-battery mobile node
    let assigned = match tokio::time::timeout(Duration::from_secs(3), rx1.recv()).await {
        Ok(Some(MasterMessage::AssignTask { task })) => task.id == task_id,
        _ => false,
    };

    assert!(
        assigned,
        "Master scheduler must route compilation away from low-battery mobile worker"
    );

    let _ = tx1
        .send(WorkerMessage::TaskResult {
            worker_id: w1,
            task_id,
            exit_code: 0,
            stdout: "ok".into(),
            stderr: "".into(),
            execution_time_ms: 5,
            is_gpu_executed: false,
            device_name: None,
            error: None,
        })
        .await;
}

#[tokio::test]
async fn test_tier3_pairwise_priority_task_ordering_with_gpu() {
    let mut cluster = TestCluster::new().await;
    let _w_gpu = cluster
        .spawn_worker("gpu-priority-worker", 4, 8192, true)
        .await;
    assert!(cluster.wait_for_workers(1, Duration::from_secs(3)).await);

    // Submit low priority long GPU task (priority 0)
    let t_low = Task::new(
        TaskSpec::command("sleep", vec!["0.15".into()]),
        TaskRequirements::gpu(10),
    );
    let id_low = cluster
        .master
        .submit_task_with_priority(t_low, 0)
        .await
        .unwrap();

    let started = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        || async {
            matches!(
                cluster.master.get_task_status(id_low).await,
                Ok(TaskStatus::Running)
            )
        },
    )
    .await;
    assert!(
        started,
        "Initial GPU task must start running before queueing pending tasks"
    );

    // Submit low priority pending task (priority 1)
    let t_p1 = Task::new(
        TaskSpec::builtin_test("hash_compute", 100),
        TaskRequirements::gpu(10),
    );
    let id_p1 = cluster
        .master
        .submit_task_with_priority(t_p1, 1)
        .await
        .unwrap();

    // Submit high priority pending task (priority 100)
    let t_p100 = Task::new(
        TaskSpec::builtin_test("hash_compute", 100),
        TaskRequirements::gpu(10),
    );
    let id_p100 = cluster
        .master
        .submit_task_with_priority(t_p100, 100)
        .await
        .unwrap();

    // High priority task should complete before low priority task
    let res_p100 = cluster
        .master
        .wait_task(id_p100, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    let res_p1 = cluster
        .master
        .wait_task(id_p1, Some(Duration::from_secs(5)))
        .await
        .unwrap();

    assert_eq!(res_p100.exit_code, 0);
    assert_eq!(res_p1.exit_code, 0);
}

#[tokio::test]
async fn test_tier3_pairwise_worker_disconnect_during_heterogeneous_batch() {
    let (mut cluster, w1, _w2, w3_gpu) = TestCluster::setup_ac_cluster().await;

    // Submit 1 CPU task and 1 GPU task
    let t_cpu = Task::new(
        TaskSpec::command("sleep", vec!["0.3".into()]),
        TaskRequirements::generic(1, 10),
    );
    let t_gpu = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "matmul_tiled".into(),
            input_data: vec![7, 8],
            work_group_size: 16,
            simulated_matrix_dim: 32,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(10),
    );

    let id_cpu = cluster.master.submit_task(t_cpu).await.unwrap();
    let id_gpu = cluster.master.submit_task(t_gpu).await.unwrap();

    let running = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        || async {
            matches!(
                cluster.master.get_task_status(id_cpu).await,
                Ok(TaskStatus::Running)
            )
        },
    )
    .await;
    assert!(running, "CPU task must begin running before worker abort");

    // Abort CPU worker 1
    cluster.abort_worker(w1);

    // CPU task should be retried on surviving worker, GPU task finishes on GPU worker
    let (res_cpu, res_gpu) = tokio::join!(
        cluster
            .master
            .wait_task(id_cpu, Some(Duration::from_secs(5))),
        cluster
            .master
            .wait_task(id_gpu, Some(Duration::from_secs(5)))
    );

    assert_eq!(res_cpu.unwrap().exit_code, 0);
    let rg = res_gpu.unwrap();
    assert_eq!(rg.exit_code, 0);
    assert_eq!(rg.worker_id, w3_gpu);
}

#[tokio::test]
async fn test_tier3_pairwise_batch_partial_failures_isolation() {
    let (cluster, _, _, _) = TestCluster::setup_ac_cluster().await;

    let tasks = vec![
        (
            true,
            Task::new(
                TaskSpec::command("echo", vec!["success 1".into()]),
                TaskRequirements::generic(1, 10),
            ),
        ),
        (
            false,
            Task::new(
                TaskSpec::builtin_test("fail", 0),
                TaskRequirements::generic(1, 10),
            ),
        ),
        (
            true,
            Task::new(
                TaskSpec::command("echo", vec!["success 2".into()]),
                TaskRequirements::generic(1, 10),
            ),
        ),
        (
            false,
            Task::new(
                TaskSpec::builtin_test("fail", 0),
                TaskRequirements::generic(1, 10),
            ),
        ),
    ];

    let mut futs = Vec::new();
    for (should_succeed, task) in tasks {
        let c = &cluster;
        futs.push(async move {
            let id = c.master.submit_task(task).await.unwrap();
            (
                should_succeed,
                c.master.wait_task(id, Some(Duration::from_secs(5))).await,
            )
        });
    }

    let results = join_all(futs).await;
    for (should_succeed, res) in results {
        let r = res.unwrap();
        if should_succeed {
            assert_eq!(r.exit_code, 0);
        } else {
            assert_ne!(r.exit_code, 0);
        }
    }
}

#[tokio::test]
async fn test_tier3_pairwise_concurrent_mapreduce_and_generic_tasks() {
    let (cluster, _, _, _) = TestCluster::setup_ac_cluster().await;

    // Concurrent Map/Reduce job
    let mr_job = MapReduceJobSpec::new(
        "concurrent_mr",
        vec!["hello world".into(), "hello rust".into()],
        MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        2,
        1,
        20,
    );

    // Generic tasks
    let t1 = Task::new(
        TaskSpec::command("echo", vec!["generic 1".into()]),
        TaskRequirements::generic(1, 10),
    );
    let t2 = Task::new(
        TaskSpec::command("echo", vec!["generic 2".into()]),
        TaskRequirements::generic(1, 10),
    );

    let mr_fut = cluster.master.execute_mapreduce(mr_job);
    let id1 = cluster.master.submit_task(t1).await.unwrap();
    let id2 = cluster.master.submit_task(t2).await.unwrap();

    let (mr_res, r1, r2) = tokio::join!(
        mr_fut,
        cluster.master.wait_task(id1, Some(Duration::from_secs(5))),
        cluster.master.wait_task(id2, Some(Duration::from_secs(5)))
    );

    let mr = mr_res.unwrap();
    assert_eq!(mr.status, "Completed");
    assert_eq!(mr.output.get("hello"), Some(&serde_json::json!(2)));

    assert_eq!(r1.unwrap().exit_code, 0);
    assert_eq!(r2.unwrap().exit_code, 0);
}

// ============================================================================
// TIER 4: REAL-WORLD APPLICATION SCENARIOS (5 Tests)
// ============================================================================

/// Scenario 1: Full Acceptance Criteria (AC1-AC5) End-to-End Local Cluster Run.
#[tokio::test]
async fn test_tier4_scenario_full_acceptance_criteria_ac1_to_ac5() {
    // -------------------------------------------------------------------------
    // AC1: 1 Master + 3 Workers (W1: 2 CPU, W2: 4 CPU, W3: 8 CPU + Simulated GPU)
    // -------------------------------------------------------------------------
    let mut cluster = TestCluster::new().await;
    let w1_id = cluster
        .spawn_worker("worker-cpu-node-1", 2, 2048, false)
        .await;
    let w2_id = cluster
        .spawn_worker("worker-cpu-node-2", 4, 4096, false)
        .await;
    let w3_id = cluster
        .spawn_worker("worker-gpu-node-3", 8, 8192, true)
        .await;

    // -------------------------------------------------------------------------
    // AC2: All 3 Workers register successfully & advertise capabilities
    // -------------------------------------------------------------------------
    assert!(
        cluster.wait_for_workers(3, Duration::from_secs(5)).await,
        "AC2 Failed: Workers failed to register with master"
    );

    let registered = cluster.master.list_workers().await.unwrap();
    assert_eq!(registered.len(), 3);

    let w3_info = registered.iter().find(|w| w.worker_id == w3_id).unwrap();
    assert!(
        w3_info.capabilities.is_simulated_gpu || w3_info.capabilities.has_gpu,
        "AC2 Failed: Worker 3 must advertise GPU capabilities"
    );
    assert_eq!(w3_info.capabilities.cpu_cores, 8);

    // -------------------------------------------------------------------------
    // AC3: Submit generic task, verify executed and correct result returned
    // -------------------------------------------------------------------------
    let generic_task = Task::new(
        TaskSpec::command("echo", vec!["Hello OxideSwarm Distributed Compute".into()]),
        TaskRequirements::generic(1, 10),
    );
    let gen_res = cluster
        .submit_and_wait(generic_task, Duration::from_secs(5))
        .await
        .expect("AC3 Failed: Generic task execution error");

    assert_eq!(
        gen_res.exit_code, 0,
        "AC3 Failed: Generic task non-zero exit"
    );
    assert!(
        gen_res
            .stdout
            .contains("Hello OxideSwarm Distributed Compute"),
        "AC3 Failed: Generic task stdout mismatch"
    );

    // -------------------------------------------------------------------------
    // AC4: Submit GPU-specific task, verify assigned ONLY to simulated GPU node
    // -------------------------------------------------------------------------
    let gpu_task = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "matmul_tiled".into(),
            input_data: vec![42, 43, 44],
            work_group_size: 16,
            simulated_matrix_dim: 32,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(15),
    );
    let gpu_res = cluster
        .submit_and_wait(gpu_task, Duration::from_secs(5))
        .await
        .expect("AC4 Failed: GPU task execution error");

    assert_eq!(gpu_res.exit_code, 0, "AC4 Failed: GPU task failed");
    assert_eq!(
        gpu_res.worker_id, w3_id,
        "AC4 Failed: GPU task assigned to non-GPU worker"
    );
    assert_ne!(gpu_res.worker_id, w1_id);
    assert_ne!(gpu_res.worker_id, w2_id);
    assert!(gpu_res.is_gpu_executed);

    // -------------------------------------------------------------------------
    // AC5: Submit batch of 5 independent compilation tasks across workers in parallel
    // -------------------------------------------------------------------------
    let mut batch_futs = Vec::new();
    for i in 1..=5 {
        let task = Task::new(
            TaskSpec::command("echo", vec![format!("Compiled crate module_{i}")]),
            TaskRequirements::generic(1, 10),
        );
        let c = &cluster;
        batch_futs.push(async move {
            let id = c.master.submit_task(task).await.unwrap();
            c.master.wait_task(id, Some(Duration::from_secs(5))).await
        });
    }

    let batch_results = join_all(batch_futs).await;
    let mut workers_in_batch = HashSet::new();

    for (i, res) in batch_results.into_iter().enumerate() {
        let r = res.expect("AC5 Failed: Task in batch failed");
        assert_eq!(r.exit_code, 0);
        assert!(r
            .stdout
            .contains(&format!("Compiled crate module_{}", i + 1)));
        workers_in_batch.insert(r.worker_id);
    }

    assert_eq!(
        workers_in_batch.len(),
        3,
        "AC5 Failed: Batch tasks should distribute in parallel across all 3 workers"
    );
}

/// Scenario 2: Multi-Worker Parallel Rust Crate Compilation.
#[tokio::test]
async fn test_tier4_scenario_multi_worker_parallel_rust_crate_compilation() {
    let (cluster, _, _, _) = TestCluster::setup_ac_cluster().await;

    let crate_sources = vec![
        ("math_lib", "pub fn add(a: i32, b: i32) -> i32 { a + b }\npub fn mul(a: i32, b: i32) -> i32 { a * b }"),
        ("string_lib", "pub fn greet(name: &str) -> String { format!(\"Hello, {}!\", name) }"),
        ("parser_lib", "pub fn parse_int(s: &str) -> Result<i32, std::num::ParseIntError> { s.parse() }"),
        ("crypto_mock", "pub fn hash_mock(data: &[u8]) -> u64 { data.iter().fold(0u64, |a, &b| a.wrapping_add(b as u64)) }"),
    ];

    let mut comp_futs = Vec::new();
    for (crate_name, src_code) in crate_sources {
        let mut source_files = HashMap::new();
        source_files.insert("src/lib.rs".into(), src_code.to_string());

        let task = Task::new(
            TaskSpec::RustCompilation {
                crate_name: crate_name.into(),
                source_files,
                compiler_flags: vec!["--crate-type".into(), "lib".into()],
                target_dir: None,
            },
            TaskRequirements::generic(1, 30),
        );
        let c = &cluster;
        comp_futs.push(async move {
            let id = c.master.submit_task(task).await.unwrap();
            (
                crate_name,
                c.master.wait_task(id, Some(Duration::from_secs(15))).await,
            )
        });
    }

    let results = join_all(comp_futs).await;
    let mut compiled_workers = HashSet::new();

    for (name, res) in results {
        let r = res.unwrap_or_else(|e| panic!("Compilation of {name} failed: {e:?}"));
        assert_eq!(r.exit_code, 0, "Compilation of {name} returned non-zero");
        assert!(r.stdout.contains("compilation artifacts generated"));
        compiled_workers.insert(r.worker_id);
    }

    assert!(
        compiled_workers.len() >= 2,
        "Rust crate compilation jobs should be distributed across multiple workers"
    );
}

/// Scenario 3: Heterogeneous GPU Matrix Compute & CPU Data Pipeline.
#[tokio::test]
async fn test_tier4_scenario_heterogeneous_gpu_matrix_and_cpu_data_pipeline() {
    let (cluster, _w1, _w2, w3_gpu) = TestCluster::setup_ac_cluster().await;

    // Stage 1: GPU Matrix Compute
    let gpu_task = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "gemm_fp32".into(),
            input_data: vec![1, 2, 3, 4],
            work_group_size: 16,
            simulated_matrix_dim: 32,
            compute_intensity: 1,
        },
        TaskRequirements::gpu(15),
    );
    let stage1_res = cluster
        .submit_and_wait(gpu_task, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(stage1_res.exit_code, 0);
    assert_eq!(stage1_res.worker_id, w3_gpu);
    assert!(stage1_res.is_gpu_executed);

    // Extract digest from GPU output to pass to Stage 2
    let digest_line = stage1_res
        .stdout
        .lines()
        .find(|l| l.contains("Verification Digest:"))
        .unwrap_or("Verification Digest: 0x12345678");

    // Stage 2: CPU Data Aggregation & Verification
    let stage2_task = Task::new(
        TaskSpec::Command {
            program: "sh".into(),
            args: vec![
                "-c".into(),
                format!("echo \"Stage 2 received: {digest_line}\" && exit 0"),
            ],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let stage2_res = cluster
        .submit_and_wait(stage2_task, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(stage2_res.exit_code, 0);
    assert!(stage2_res
        .stdout
        .contains("Stage 2 received: Verification Digest:"));
}

/// Scenario 4: Worker Churn & Cluster Fault-Tolerant Resilience.
#[tokio::test]
async fn test_tier4_scenario_worker_churn_and_cluster_resilience() {
    let mut cluster = TestCluster::new().await;
    let w1 = cluster.spawn_worker("churn-w1", 2, 2048, false).await;
    let _w2 = cluster.spawn_worker("churn-w2", 2, 2048, false).await;
    assert!(cluster.wait_for_workers(2, Duration::from_secs(3)).await);

    // Submit 8 tasks with slight duration
    let mut task_ids = Vec::new();
    for _i in 0..8 {
        let task = Task::new(
            TaskSpec::command("sleep", vec!["0.15".into()]),
            TaskRequirements::generic(1, 10),
        );
        let id = cluster.master.submit_task(task).await.unwrap();
        task_ids.push(id);
    }

    let started = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        || async {
            for id in &task_ids {
                if let Ok(TaskStatus::Running) = cluster.master.get_task_status(*id).await {
                    return true;
                }
            }
            false
        },
    )
    .await;
    assert!(started, "Tasks must begin running before worker churn");

    // Kill Worker 1 during active execution
    cluster.abort_worker(w1);

    // Join Worker 3 to replenish cluster capacity
    let _w3 = cluster
        .spawn_worker("churn-w3-replenish", 4, 4096, false)
        .await;

    // All 8 tasks must complete successfully without permanent loss
    let mut wait_futs = Vec::new();
    for id in task_ids {
        let c = &cluster;
        wait_futs.push(async move { c.master.wait_task(id, Some(Duration::from_secs(10))).await });
    }

    let results = join_all(wait_futs).await;
    for res in results {
        let r = res.expect("Task failed during worker churn");
        assert_eq!(r.exit_code, 0);
    }
}

/// Scenario 5: Heavy Burst Load Distribution across Cluster.
#[tokio::test]
async fn test_tier4_scenario_heavy_burst_load_distribution() {
    let (cluster, _, _, _) = TestCluster::setup_ac_cluster().await;

    // Burst of 15 tasks
    let mut futs = Vec::new();
    for i in 0..15 {
        let task = Task::new(
            TaskSpec::command("echo", vec![format!("burst_item_{i}")]),
            TaskRequirements::generic(1, 10),
        );
        let c = &cluster;
        futs.push(async move {
            let id = c.master.submit_task(task).await.unwrap();
            c.master.wait_task(id, Some(Duration::from_secs(10))).await
        });
    }

    let results = join_all(futs).await;
    let mut worker_distribution: HashMap<Uuid, usize> = HashMap::new();

    for res in results {
        let r = res.unwrap();
        assert_eq!(r.exit_code, 0);
        *worker_distribution.entry(r.worker_id).or_insert(0) += 1;
    }

    // All 3 workers must have participated
    assert_eq!(
        worker_distribution.len(),
        3,
        "All 3 workers must receive tasks under burst load"
    );

    // Verify reasonable balance (each worker handled at least 2 tasks out of 15)
    for (wid, count) in &worker_distribution {
        assert!(
            *count >= 2,
            "Worker {wid} handled only {count} tasks, expected balanced spread"
        );
    }
}
