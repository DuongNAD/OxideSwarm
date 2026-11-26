//! End-to-end verification of `BUG-22`: `oxideswarm status --json`, plain `oxideswarm status`,
//! and `oxideswarm mcp` (`status` tool) count cancelled tasks separately from failed tasks.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use serde_json::{json, Value};
use tokio::sync::watch;

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

fn cli_cmd(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.current_dir(home)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_INSECURE")
        .env("NO_COLOR", "1");
    cmd
}

struct McpChild {
    child: Child,
    stdout: BufReader<std::process::ChildStdout>,
}

impl McpChild {
    fn spawn(home: &Path, master_addr: &str) -> Self {
        let mut cmd = cli_cmd(home);
        cmd.args(["mcp", "--master", master_addr])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = cmd.spawn().expect("spawn oxideswarm mcp");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        Self { child, stdout }
    }

    fn send(&mut self, msg: &Value) {
        let stdin = self.child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "{msg}").expect("write json line");
        stdin.flush().expect("flush stdin");
    }

    fn recv(&mut self) -> Value {
        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("read json line");
        serde_json::from_str(line.trim())
            .unwrap_or_else(|e| panic!("invalid JSON from mcp: {e} ({line:?})"))
    }
}

impl Drop for McpChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_status_json_and_mcp_count_one_failed_and_one_cancelled_separately() {
    let temp = tempfile::tempdir().expect("tempdir");
    let home = temp.path();
    let blob_dir = home.join("master_blobs");
    let sandbox_dir = home.join("worker_sandboxes");
    std::fs::create_dir_all(&blob_dir).expect("blob_dir");
    std::fs::create_dir_all(&sandbox_dir).expect("sandbox_dir");

    let server_cfg = ServerConfig::from_addr("127.0.0.1:0")
        .expect("server config")
        .with_discovery_port(0)
        .with_blob_store(blob_dir, 64 * 1024 * 1024);
    let master = MasterServer::spawn(server_cfg).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    let worker_cfg = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("bug22-worker")
        .with_cores(2)
        .with_ram_mb(4096)
        .with_sandbox_base_dir(&sandbox_dir);
    let mut worker = WorkerClient::new(worker_cfg);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker_handle = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    while master.registry().list_workers().await.is_empty() {
        assert!(Instant::now() < deadline, "worker did not register in time");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    // 1. Run 1 failing task via CLI (`exit 3`, no retries).
    let fail_out = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--type",
            "shell",
            "--max-retries",
            "0",
            "--command",
            "exit 3",
            "--wait",
        ])
        .output()
        .expect("run failing task");
    assert_eq!(
        fail_out.status.code(),
        Some(3),
        "expected exit code 3, stderr: {}",
        String::from_utf8_lossy(&fail_out.stderr)
    );

    // 2. Submit 1 unscheduled queued task and cancel it via CLI.
    let queued_out = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--require-tag",
            "no-such-worker",
            "--json",
            "--",
            "echo",
            "cancelled",
        ])
        .output()
        .expect("submit queued task to cancel");
    assert!(
        queued_out.status.success(),
        "queued submit failed: {}",
        String::from_utf8_lossy(&queued_out.stderr)
    );
    let queued_json: Value =
        serde_json::from_slice(&queued_out.stdout).expect("queued submit json");
    let cancel_id = queued_json["task_id"].as_str().expect("task_id");

    let cancel_out = cli_cmd(home)
        .args(["cancel", cancel_id, "--master", &master_addr])
        .output()
        .expect("cancel queued task");
    assert!(
        cancel_out.status.success(),
        "cancel failed: {}",
        String::from_utf8_lossy(&cancel_out.stderr)
    );

    // 3. `oxideswarm status --json` must report failed_tasks=1 and cancelled_tasks=1.
    let status_json_out = cli_cmd(home)
        .args(["status", "--master", &master_addr, "--json"])
        .output()
        .expect("status --json");
    assert!(
        status_json_out.status.success(),
        "status --json failed: {}",
        String::from_utf8_lossy(&status_json_out.stderr)
    );
    let status_val: Value =
        serde_json::from_slice(&status_json_out.stdout).expect("parse status --json");
    assert_eq!(status_val["total_tasks"], 2, "{status_val}");
    assert_eq!(status_val["pending_tasks"], 0, "{status_val}");
    assert_eq!(status_val["running_tasks"], 0, "{status_val}");
    assert_eq!(status_val["completed_tasks"], 0, "{status_val}");
    assert_eq!(status_val["failed_tasks"], 1, "{status_val}");
    assert_eq!(status_val["cancelled_tasks"], 1, "{status_val}");

    // 4. Plain `oxideswarm status` must print both Failed Tasks and Cancelled Tasks as 1.
    let status_plain_out = cli_cmd(home)
        .args(["status", "--master", &master_addr])
        .output()
        .expect("status plain");
    assert!(
        status_plain_out.status.success(),
        "status plain failed: {}",
        String::from_utf8_lossy(&status_plain_out.stderr)
    );
    let status_plain = String::from_utf8_lossy(&status_plain_out.stdout);
    assert!(
        status_plain.contains("Failed Tasks:     1")
            && status_plain.contains("Cancelled Tasks:  1"),
        "expected Failed Tasks: 1 and Cancelled Tasks: 1 in:\n{status_plain}"
    );

    // 5. MCP `status` tool must also report failed_tasks=1 and cancelled_tasks=1.
    let mut mcp = McpChild::spawn(home, &master_addr);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": { "tools": {} },
            "clientInfo": { "name": "bug22-test", "version": "1.0.0" }
        }
    }));
    let init = mcp.recv();
    assert_eq!(init["id"], 1);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    }));
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "status",
            "arguments": {}
        }
    }));
    let mcp_resp = mcp.recv();
    assert_eq!(mcp_resp["id"], 2, "{mcp_resp}");
    let text = mcp_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("missing mcp text content: {mcp_resp}"));
    let mcp_status: Value = serde_json::from_str(text).expect("parse mcp status json");
    assert_eq!(mcp_status["total_tasks"], 2, "{mcp_status}");
    assert_eq!(mcp_status["pending_tasks"], 0, "{mcp_status}");
    assert_eq!(mcp_status["running_tasks"], 0, "{mcp_status}");
    assert_eq!(mcp_status["completed_tasks"], 0, "{mcp_status}");
    assert_eq!(mcp_status["failed_tasks"], 1, "{mcp_status}");
    assert_eq!(mcp_status["cancelled_tasks"], 1, "{mcp_status}");
    drop(mcp);

    let _ = shutdown_tx.send(true);
    if tokio::time::timeout(Duration::from_secs(30), &mut worker_handle)
        .await
        .is_err()
    {
        worker_handle.abort();
        let _ = worker_handle.await;
    }
    let _ = master.shutdown();
    drop(temp);
}
