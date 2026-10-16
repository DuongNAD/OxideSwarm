//! Integration tests for Agent Mesh <-> Master Grid runtime integration.
//!
//! Verifies:
//! 1. An AgentMeshHub with an attached Master Grid runtime handles grid_submit, grid_compute,
//!    and grid_status envelopes end-to-end without returning ERR_BRIDGE_NOT_CONFIGURED.
//! 2. An unattached hub (started without --grid-master) returns ERR_BRIDGE_NOT_CONFIGURED.
//! 3. CLI argument parsing correctly handles --grid-master with and without address, and preserves None when omitted.

use std::sync::Arc;
use std::time::Duration;
use clap::Parser;
use tempfile::TempDir;
use tokio::sync::watch;

use agent_mesh::{
    AgentGridBridge, AgentMeshClient, AgentMeshEnvelope, AgentMeshHub,
};
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

// CLI parser imported from agent-mesh binary
#[derive(Parser, Debug)]
#[command(name = "test-cli")]
struct TestCli {
    #[command(subcommand)]
    pub command: TestCommands,
}

#[derive(clap::Subcommand, Debug)]
enum TestCommands {
    Hub {
        #[arg(short, long, default_value = "127.0.0.1:8088")]
        listen: String,

        #[arg(long, env = "OXIDE_MESH_TOKEN", hide_env_values = true)]
        token: Option<String>,

        #[arg(long)]
        insecure: bool,

        #[arg(long, env = "OXIDE_GRID_MASTER", num_args = 0..=1, default_missing_value = "127.0.0.1:0")]
        grid_master: Option<String>,

        #[arg(long, env = "OXIDESWARM_SECRET", hide_env_values = true)]
        secret: Option<String>,
    },
}

#[test]
fn test_cli_grid_master_argument_parsing() {
    // 1. Omitted --grid-master -> None
    let cli1 = TestCli::parse_from(["app", "hub", "--listen", "127.0.0.1:9000"]);
    match cli1.command {
        TestCommands::Hub { grid_master, .. } => {
            assert!(grid_master.is_none(), "When omitted, grid_master must be None");
        }
    }

    // 2. Bare flag --grid-master without value -> default_missing_value "127.0.0.1:0"
    let cli2 = TestCli::parse_from(["app", "hub", "--listen", "127.0.0.1:9000", "--grid-master"]);
    match cli2.command {
        TestCommands::Hub { grid_master, .. } => {
            assert_eq!(
                grid_master.as_deref(),
                Some("127.0.0.1:0"),
                "Bare --grid-master must default to 127.0.0.1:0"
            );
        }
    }

    // 3. Flag with explicit address --grid-master 127.0.0.1:8089
    let cli3 = TestCli::parse_from([
        "app",
        "hub",
        "--listen",
        "127.0.0.1:9000",
        "--grid-master",
        "127.0.0.1:8089",
    ]);
    match cli3.command {
        TestCommands::Hub { grid_master, .. } => {
            assert_eq!(
                grid_master.as_deref(),
                Some("127.0.0.1:8089"),
                "Explicit --grid-master value must be preserved"
            );
        }
    }
}

struct TestCluster {
    pub master: MasterHandle,
    pub hub: AgentMeshHub,
    pub hub_url: String,
    pub worker_shutdown: watch::Sender<bool>,
    pub worker_handle: tokio::task::JoinHandle<()>,
    pub _worker_sandbox: TempDir,
}

impl TestCluster {
    pub async fn spawn() -> Self {
        // 1. Spawn embedded Master on dynamic port
        let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
        let master = MasterServer::spawn(server_config)
            .await
            .expect("MasterServer::spawn failed");
        let master_addr = master.server_addr();

        // 2. Spawn Hub and attach bridge
        let hub = AgentMeshHub::new();
        let bridge = Arc::new(AgentGridBridge::new(master.clone()));
        hub.attach_bridge(bridge).await;
        let hub_port = hub
            .start("127.0.0.1:0")
            .await
            .expect("AgentMeshHub start failed");
        let hub_url = format!("ws://127.0.0.1:{}/ws", hub_port);

        // 3. Connect a worker node to master so tasks can execute
        let sandbox = tempfile::tempdir().expect("worker tempdir");
        let worker_cfg = WorkerConfig::new(master_addr.to_string())
            .with_name("mesh-test-worker")
            .with_cores(4)
            .with_simulate_gpu(true)
            .with_sandbox_base_dir(sandbox.path());
        let mut worker = WorkerClient::new(worker_cfg);
        let (tx, rx) = watch::channel(false);
        let worker_handle = tokio::spawn(async move {
            let _ = worker.run(rx).await;
        });

        // Settle worker connection
        let mut connected = false;
        for _ in 0..100 {
            if let Ok(workers) = master.list_workers().await {
                if !workers.is_empty() {
                    connected = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(connected, "Worker must connect to Master");

        Self {
            master,
            hub,
            hub_url,
            worker_shutdown: tx,
            worker_handle,
            _worker_sandbox: sandbox,
        }
    }

    pub async fn teardown(self) {
        let _ = self.worker_shutdown.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), self.worker_handle).await;
        self.hub.stop().await;
        let _ = self.master.shutdown();
    }
}

#[tokio::test]
async fn test_attached_hub_processes_grid_envelopes_successfully() {
    let cluster = TestCluster::spawn().await;

    let client = AgentMeshClient::new(&cluster.hub_url, "test-agent-client", "rust-runtime");
    client.connect().await.expect("Client connects to hub");

    // 1. Test grid_status envelope
    let status_resp = client
        .query_grid_status(Some(10000))
        .await
        .expect("query_grid_status response");

    match status_resp {
        AgentMeshEnvelope::CommandResponse { status, stdout, .. } => {
            assert_eq!(status, "success", "grid_status should return success");
            assert!(
                stdout.contains("Active Workers") || stdout.contains("Grid Cluster Status"),
                "grid_status output should include active worker information: {stdout}"
            );
        }
        other => panic!("Expected CommandResponse for grid_status, got: {:?}", other),
    }

    // 2. Test grid_compute envelope (e.g. matrix_multiply builtin/GPU test)
    let compute_resp = client
        .submit_grid_compute("matrix_multiply", 32, Some(15000))
        .await
        .expect("submit_grid_compute response");

    match compute_resp {
        AgentMeshEnvelope::CommandResponse { status, exit_code, .. } => {
            assert_eq!(status, "success", "grid_compute should execute successfully");
            assert_eq!(exit_code, 0, "matrix_multiply should succeed with exit code 0");
        }
        other => panic!("Expected CommandResponse for grid_compute, got: {:?}", other),
    }

    // 3. Test grid_submit envelope
    let submit_resp = client
        .send_command(
            "grid",
            "grid_submit",
            serde_json::json!({
                "script": "echo 'OxideSwarm Grid Online'",
                "wait": true
            }),
            Some(15000),
        )
        .await
        .expect("send_command grid_submit response");

    match submit_resp {
        AgentMeshEnvelope::CommandResponse { status, stdout, exit_code, .. } => {
            assert_eq!(status, "success", "grid_submit should execute successfully");
            assert_eq!(exit_code, 0, "echo script should succeed");
            assert!(
                stdout.contains("OxideSwarm Grid Online"),
                "grid_submit output should contain stdout: {stdout}"
            );
        }
        other => panic!("Expected CommandResponse for grid_submit, got: {:?}", other),
    }

    client.close().await;
    cluster.teardown().await;
}

#[tokio::test]
async fn test_unattached_hub_returns_err_bridge_not_configured() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("hub start");
    let hub_url = format!("ws://127.0.0.1:{port}/ws");

    let client = AgentMeshClient::new(&hub_url, "unattached-client", "rust-rt");
    client.connect().await.expect("client connect");

    // grid_status on unattached hub must return ERR_BRIDGE_NOT_CONFIGURED
    let status_resp = client.query_grid_status(Some(5000)).await.expect("response");
    match status_resp {
        AgentMeshEnvelope::DeliveryNack { error_code, .. } => {
            assert_eq!(error_code, "ERR_BRIDGE_NOT_CONFIGURED");
        }
        other => panic!("Expected DeliveryNack, got: {:?}", other),
    }

    // grid_compute on unattached hub must return ERR_BRIDGE_NOT_CONFIGURED
    let compute_resp = client
        .submit_grid_compute("matrix_multiply", 16, Some(5000))
        .await
        .expect("response");
    match compute_resp {
        AgentMeshEnvelope::DeliveryNack { error_code, .. } => {
            assert_eq!(error_code, "ERR_BRIDGE_NOT_CONFIGURED");
        }
        other => panic!("Expected DeliveryNack, got: {:?}", other),
    }

    client.close().await;
    hub.stop().await;
}
