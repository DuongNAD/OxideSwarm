//! End-to-End Integration Verification: Ecosystem Convergence & AgentGridBridge.
//!
//! Verifies:
//! 1. `test_convergence_rust_agent_gpu_compute`: Rust AgentMeshClient submits GPU compute tasks via WebSocket.
//! 2. `test_convergence_rust_agent_compilation`: Rust AgentMeshClient submits distributed Rust compilation workloads.
//! 3. `test_convergence_python_agent_node_compute`: Python agent_node.py submits compute workloads via WebSocket CLI.
//! 4. `test_convergence_cluster_status_query`: Rust AgentMeshClient queries grid cluster status and active worker inventory.
//! 5. `test_convergence_multi_agent_burst_stress`: 5 concurrent clients submit bursts of compute/status tasks simultaneously with zero cross-talk.
//! 6. `test_convergence_error_propagation_and_timeout`: Error handling for unsupported commands, unattached bridge, and non-existent tasks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::watch;
use uuid::Uuid;

use agent_mesh::bridge::AgentGridBridge;
use agent_mesh::client::AgentMeshClient;
use agent_mesh::hub::AgentMeshHub;
use agent_mesh::protocol::AgentMeshEnvelope;

use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

/// Test harness orchestrating MasterServer, WorkerClients, AgentMeshHub, and AgentGridBridge.
#[allow(dead_code)]
struct ConvergenceCluster {
    pub master: MasterHandle,
    pub hub: AgentMeshHub,
    pub hub_url: String,
    pub hub_port: u16,
    pub workers: Vec<SpawnedWorker>,
}

#[allow(dead_code)]
struct SpawnedWorker {
    pub worker_id: Uuid,
    pub shutdown_tx: watch::Sender<bool>,
    pub handle: tokio::task::JoinHandle<()>,
    pub _temp_dir: TempDir,
}

impl SpawnedWorker {
    pub fn abort(&self) {
        let _ = self.shutdown_tx.send(true);
        self.handle.abort();
    }
}

impl ConvergenceCluster {
    pub async fn spawn(num_workers: usize) -> Self {
        // 1. Spawn Layer 1 Master Grid Server on ephemeral port
        let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("MasterServer::spawn failed");
        let master_addr = master.server_addr().to_string();

        // 2. Spawn Layer 3 AgentMeshHub on an ephemeral port with bridge attached
        let hub = AgentMeshHub::new();
        let bridge = Arc::new(AgentGridBridge::new(master.clone()));
        hub.attach_bridge(bridge).await;
        let hub_port = hub
            .start("127.0.0.1:0")
            .await
            .expect("AgentMeshHub start failed");
        let hub_url = format!("ws://127.0.0.1:{}/ws", hub_port);

        // 3. Spawn Layer 1 Grid Workers
        let mut workers = Vec::new();
        for i in 0..num_workers {
            let temp_dir = tempfile::tempdir().expect("worker tempdir");
            let cfg = WorkerConfig::new(master_addr.clone())
                .with_name(&format!("convergence-worker-{}", i + 1))
                .with_cores(4)
                .with_simulate_gpu(true)
                .with_sandbox_base_dir(temp_dir.path());

            let mut worker = WorkerClient::new(cfg);
            let worker_id = worker.worker_id();
            let (tx, rx) = watch::channel(false);

            let handle = tokio::spawn(async move {
                let _ = worker.run(rx).await;
            });

            workers.push(SpawnedWorker {
                worker_id,
                shutdown_tx: tx,
                handle,
                _temp_dir: temp_dir,
            });
        }

        // Wait for workers to connect to Master
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if let Ok(list) = master.list_workers().await {
                let connected = list
                    .iter()
                    .filter(|w| w.status == WorkerStatus::Connected)
                    .count();
                if connected >= num_workers {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        Self {
            master,
            hub,
            hub_url,
            hub_port,
            workers,
        }
    }

    pub async fn shutdown(self) {
        for w in &self.workers {
            w.abort();
        }
        self.hub.stop().await;
        let _ = self.master.shutdown();
    }
}

// =========================================================================
// TEST SUITE 1: Rust Agent Submitting GPU Compute Workloads via Bridge
// =========================================================================

#[tokio::test]
async fn test_convergence_rust_agent_gpu_compute() {
    let cluster = ConvergenceCluster::spawn(1).await;

    let client = AgentMeshClient::new(&cluster.hub_url, "rust-coding-agent", "rust-runtime");
    client.connect().await.expect("client connect failed");

    // 1. Submit grid compute job (matrix multiplication)
    let res = client
        .submit_grid_compute("matrix_multiply", 64, Some(15000))
        .await
        .expect("submit_grid_compute failed");

    match res {
        AgentMeshEnvelope::CommandResponse {
            from,
            to,
            command,
            status,
            exit_code,
            stdout,
            execution_duration_ms,
            payload,
            ..
        } => {
            assert_eq!(from, "grid");
            assert_eq!(to, "rust-coding-agent");
            assert_eq!(command.as_deref(), Some("grid_compute"));
            assert_eq!(status, "success");
            assert_eq!(exit_code, 0);
            assert!(
                stdout.contains("Execution Time")
                    || stdout.contains("VERIFIED_OK")
                    || stdout.contains("Matrix Dimension"),
                "Expected compute output in stdout, got: {}",
                stdout
            );
            assert!(execution_duration_ms > 0);
            assert!(payload.is_some());
            let p = payload.unwrap();
            assert_eq!(p.get("is_gpu_executed").and_then(|v| v.as_bool()), Some(true));
        }
        other => panic!("Expected CommandResponse, got {:?}", other),
    }

    client.close().await;
    cluster.shutdown().await;
}

// =========================================================================
// TEST SUITE 2: Rust Agent Submitting Compilation Workloads via Bridge
// =========================================================================

#[tokio::test]
async fn test_convergence_rust_agent_compilation() {
    let cluster = ConvergenceCluster::spawn(1).await;

    let client = AgentMeshClient::new(&cluster.hub_url, "rust-compiler-agent", "rust-runtime");
    client.connect().await.expect("client connect failed");

    let mut source_files = HashMap::new();
    source_files.insert(
        "src/lib.rs".to_string(),
        "pub fn add(a: i32, b: i32) -> i32 { a + b }\n".to_string(),
    );

    let res = client
        .submit_grid_compilation(
            "sample_crate",
            source_files,
            vec!["--crate-type".into(), "lib".into()],
            Some(30000),
        )
        .await
        .expect("submit_grid_compilation failed");

    match res {
        AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stdout,
            from,
            to,
            ..
        } => {
            assert_eq!(from, "grid");
            assert_eq!(to, "rust-compiler-agent");
            assert_eq!(status, "success");
            assert_eq!(exit_code, 0);
            assert!(
                stdout.contains("Compilation successful")
                    || stdout.contains("compilation artifacts generated"),
                "Expected compilation output in stdout, got: {}",
                stdout
            );
        }
        other => panic!("Expected CommandResponse, got {:?}", other),
    }

    client.close().await;
    cluster.shutdown().await;
}

// =========================================================================
// TEST SUITE 3: Python Agent Node Submitting Compute via WebSocket
// =========================================================================

#[tokio::test]
async fn test_convergence_python_agent_node_compute() {
    let cluster = ConvergenceCluster::spawn(1).await;

    let py_script = [
        std::path::PathBuf::from("scripts/agent_node.py"),
        std::path::PathBuf::from("../../scripts/agent_node.py"),
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/agent_node.py"),
    ]
    .into_iter()
    .find(|p| p.exists())
    .expect("scripts/agent_node.py must exist in repository");

    let python_bin = if cfg!(windows) { "python" } else { "python3" };

    // Execute Python agent node CLI with --submit-grid-compute
    let output = tokio::process::Command::new(python_bin)
        .arg(&py_script)
        .arg("--hub")
        .arg(&cluster.hub_url)
        .arg("--id")
        .arg("python-convergence-agent")
        .arg("--submit-grid-compute")
        .arg("matrix_multiply")
        .arg("--matrix-dim")
        .arg("32")
        .output()
        .await;

    match output {
        Ok(out) => {
            let stdout_str = String::from_utf8_lossy(&out.stdout);
            let stderr_str = String::from_utf8_lossy(&out.stderr);
            println!("Python STDOUT: {}", stdout_str);
            if !out.status.success() {
                println!("Python STDERR: {}", stderr_str);
            }
            assert!(out.status.success(), "Python agent node exited with failure");
            assert!(
                stdout_str.contains("\"status\": \"success\"") || stdout_str.contains("success"),
                "Expected success status in Python output"
            );
            assert!(
                stdout_str.contains("\"from\": \"grid\""),
                "Expected response from 'grid'"
            );
        }
        Err(e) => {
            panic!("Failed to spawn python agent node: {:?}", e);
        }
    }

    cluster.shutdown().await;
}

// =========================================================================
// TEST SUITE 4: Rust Agent Querying Grid Cluster Status via Bridge
// =========================================================================

#[tokio::test]
async fn test_convergence_cluster_status_query() {
    let cluster = ConvergenceCluster::spawn(2).await;

    let client = AgentMeshClient::new(&cluster.hub_url, "rust-observer-agent", "rust-runtime");
    client.connect().await.expect("client connect failed");

    let res = client
        .query_grid_status(Some(5000))
        .await
        .expect("query_grid_status failed");

    match res {
        AgentMeshEnvelope::CommandResponse {
            from,
            to,
            status,
            exit_code,
            payload,
            ..
        } => {
            assert_eq!(from, "grid");
            assert_eq!(to, "rust-observer-agent");
            assert_eq!(status, "success");
            assert_eq!(exit_code, 0);
            assert!(payload.is_some());
            let val = payload.unwrap();
            assert!(val.get("workers").is_some());
            let workers = val["workers"].as_array().expect("workers array");
            assert!(workers.len() >= 2);
            assert!(val.get("active_workers").and_then(|v| v.as_u64()).unwrap_or(0) >= 2);
            assert!(val.get("queue").is_some());
        }
        other => panic!("Expected CommandResponse, got {:?}", other),
    }

    client.close().await;
    cluster.shutdown().await;
}

// =========================================================================
// TEST SUITE 5: Concurrent Multi-Agent Workload Burst via Bridge
// =========================================================================

#[tokio::test]
async fn test_convergence_multi_agent_burst_stress() {
    let cluster = ConvergenceCluster::spawn(3).await;

    let mut clients = Vec::new();
    for i in 1..=5 {
        let node_id = format!("agent-burst-{}", i);
        let client = Arc::new(AgentMeshClient::new(&cluster.hub_url, &node_id, "rust-burst"));
        client.connect().await.expect("burst client connect failed");
        clients.push(client);
    }

    // Concurrently submit 15 compute tasks across the 5 agents
    let mut handles = Vec::new();
    for i in 0..15 {
        let client = Arc::clone(&clients[i % clients.len()]);
        let expected_node = client.node_id().to_string();

        handles.push(tokio::spawn(async move {
            let dim = 16 + (i as u32 * 4);
            let resp = client
                .submit_grid_compute("matrix_multiply", dim, Some(20000))
                .await
                .expect("burst compute failed");

            match resp {
                AgentMeshEnvelope::CommandResponse {
                    from,
                    to,
                    status,
                    exit_code,
                    ..
                } => {
                    assert_eq!(from, "grid");
                    assert_eq!(to, expected_node, "Cross-talk detected in agent burst!");
                    assert_eq!(status, "success");
                    assert_eq!(exit_code, 0);
                }
                other => panic!("Unexpected envelope in burst: {:?}", other),
            }
        }));
    }

    for h in handles {
        h.await.expect("join handle failed");
    }

    for c in clients {
        c.close().await;
    }
    cluster.shutdown().await;
}

// =========================================================================
// TEST SUITE 6: Error Propagation Safety for Invalid Tasks
// =========================================================================

#[tokio::test]
async fn test_convergence_error_propagation_and_timeout() {
    // 1. Test unsupported command on attached bridge
    let cluster = ConvergenceCluster::spawn(1).await;
    let client = AgentMeshClient::new(&cluster.hub_url, "rust-negative-agent", "rust-runtime");
    client.connect().await.expect("client connect failed");

    let res = client
        .send_command("grid", "grid_invalid_opcode_xyz", serde_json::json!({}), Some(5000))
        .await
        .expect("send_command failed");

    match res {
        AgentMeshEnvelope::CommandResponse { status, exit_code, error, stderr, .. } => {
            assert_eq!(status, "failed");
            assert_ne!(exit_code, 0);
            assert!(
                error.as_deref().unwrap_or("").contains("Unsupported")
                    || stderr.contains("Unsupported")
            );
        }
        AgentMeshEnvelope::DeliveryNack { error_code, .. } => {
            assert_eq!(error_code, "ERR_UNSUPPORTED_GRID_COMMAND");
        }
        other => panic!("Expected error response, got {:?}", other),
    }

    // Test non-existent task ID
    let not_found_res = client
        .submit_grid_status(Some("00000000-0000-0000-0000-000000000000"), Some(5000))
        .await
        .expect("submit_grid_status failed");

    match not_found_res {
        AgentMeshEnvelope::CommandResponse { status, exit_code, stderr, error, .. } => {
            assert_eq!(status, "failed");
            assert_ne!(exit_code, 0);
            assert!(
                stderr.contains("Task not found")
                    || error.as_deref().unwrap_or("").contains("Task not found")
            );
        }
        other => panic!("Expected CommandResponse failure, got {:?}", other),
    }

    client.close().await;
    cluster.shutdown().await;

    // 2. Test unattached bridge: hub without bridge attached returns DeliveryNack
    let hub_unattached = AgentMeshHub::new();
    let port = hub_unattached
        .start("127.0.0.1:0")
        .await
        .expect("Failed to start unattached hub");
    let unattached_url = format!("ws://127.0.0.1:{}/ws", port);

    let client_unattached = AgentMeshClient::new(&unattached_url, "agent-unattached", "test-rt");
    client_unattached.connect().await.expect("connect unattached");

    let nack_res = client_unattached
        .submit_grid_compute("matrix_multiply", 32, Some(5000))
        .await
        .expect("submit_grid_compute should resolve with NACK");

    match nack_res {
        AgentMeshEnvelope::DeliveryNack { error_code, reason, .. } => {
            assert_eq!(error_code, "ERR_BRIDGE_NOT_CONFIGURED");
            assert!(reason.contains("AgentGridBridge is not attached"));
        }
        other => panic!("Expected DeliveryNack, got {:?}", other),
    }

    client_unattached.close().await;
    hub_unattached.stop().await;
}
