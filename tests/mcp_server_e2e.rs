//! End-to-end integration tests for Model Context Protocol (MCP) server.
//!
//! Validates:
//! 1. Full protocol lifecycle: `initialize`, `notifications/initialized`, `ping`, `tools/list`, and `tools/call`.
//! 2. Execution of all 5 standard tools: `status`, `workers`, `submit`, `task_result`, and `doctor`.
//! 3. Strict stdout hygiene: every single line emitted to stdout MUST be a valid JSON-RPC 2.0 frame.
//! 4. Fail-closed authentication: calls with an invalid or missing secret are rejected cleanly.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::watch;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

struct TestCluster {
    _master: MasterHandle,
    master_addr: String,
    secret: ClusterSecret,
    secret_hex: String,
    workers: Vec<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    _sandbox_dirs: Vec<TempDir>,
}

impl TestCluster {
    async fn spawn(secret_seed: u8) -> Self {
        let secret = ClusterSecret::from_bytes(&[secret_seed; 32]).expect("valid secret");
        let secret_hex = secret.to_hex();

        let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(true)
            .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
            .with_cluster_secret(secret.clone());

        let master = MasterServer::spawn_with_config(
            server_config,
            SchedulerConfig::default(),
            ReaperConfig::default(),
        )
        .await
        .expect("master starts");

        let master_addr = master.server_addr().to_string();

        let mut cluster = Self {
            _master: master,
            master_addr,
            secret,
            secret_hex,
            workers: Vec::new(),
            _sandbox_dirs: Vec::new(),
        };

        // Spawn a test worker
        cluster.spawn_worker("worker-mcp-e2e", &["tag:fast", "region:local"], true);
        cluster.wait_for_workers(1).await;

        cluster
    }

    fn spawn_worker(&mut self, name: &str, tags: &[&str], simulate_gpu: bool) {
        let sandbox_dir = tempfile::tempdir().expect("sandbox dir");
        let config = WorkerConfig::new(self.master_addr.clone())
            .with_name(name)
            .with_cluster_secret(self.secret.clone())
            .with_cores(4)
            .with_ram_mb(4096)
            .with_simulate_gpu(simulate_gpu)
            .with_tags(tags.iter().map(|t| t.to_string()).collect())
            .with_sandbox_base_dir(sandbox_dir.path())
            .with_redirection_portal(false);

        self._sandbox_dirs.push(sandbox_dir);
        let mut client = WorkerClient::new(config);
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(rx).await;
        });
        self.workers.push((tx, handle));
    }

    async fn wait_for_workers(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Ok(workers) = self._master.list_workers().await {
                if workers
                    .iter()
                    .filter(|w| w.status != WorkerStatus::Disconnected)
                    .count()
                    >= count
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        panic!("{count} worker(s) failed to register within timeout");
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for (tx, handle) in &self.workers {
            let _ = tx.send(true);
            handle.abort();
        }
    }
}

/// Helper to run child process with piped stdio and isolated environment.
struct McpProcess {
    child: Child,
    stdout: BufReader<std::process::ChildStdout>,
    lines_received: Vec<String>,
}

impl McpProcess {
    fn spawn(home: &Path, master_addr: &str, secret_hex: Option<&str>) -> Self {
        let mut cmd = Command::new(BIN);
        cmd.arg("mcp")
            .arg("--master")
            .arg(master_addr)
            .env("HOME", home)
            .env("USERPROFILE", home)
            .env_remove("OXIDESWARM_HOME")
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("OXIDESWARM_SECRET_FILE")
            .env_remove("RUSTY_GRID_CONFIG")
            .env_remove("RUSTY_GRID_MASTER")
            .env_remove("RUSTY_GRID_MASTER_ADDR")
            .env_remove("RUSTY_GRID_INSECURE")
            .env("NO_COLOR", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        if let Some(sec) = secret_hex {
            cmd.arg("--secret").arg(sec);
        }

        let mut child = cmd.spawn().expect("failed to spawn oxideswarm mcp process");
        let stdout = BufReader::new(child.stdout.take().expect("stdout must be open"));

        Self {
            child,
            stdout,
            lines_received: Vec::new(),
        }
    }

    fn send_line(&mut self, line: &str) {
        let stdin = self.child.stdin.as_mut().expect("stdin must be open");
        stdin.write_all(line.as_bytes()).expect("write to stdin");
        stdin.write_all(b"\n").expect("write newline to stdin");
        stdin.flush().expect("flush stdin");
    }

    fn read_json_response(&mut self) -> Value {
        let mut line = String::new();
        self.stdout
            .read_line(&mut line)
            .expect("read response line");
        let trimmed = line.trim();
        assert!(
            !trimmed.is_empty(),
            "expected non-empty JSON-RPC line from child stdout"
        );
        self.lines_received.push(trimmed.to_string());

        serde_json::from_str(trimmed)
            .unwrap_or_else(|e| panic!("failed to parse JSON from stdout line '{trimmed}': {e}"))
    }
}

impl Drop for McpProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_server_full_lifecycle_and_stdout_hygiene() {
    let cluster = TestCluster::spawn(0x42).await;
    let home = tempfile::tempdir().expect("temp home");

    let mut mcp = McpProcess::spawn(home.path(), &cluster.master_addr, Some(&cluster.secret_hex));

    // 1. Handshake: initialize
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "clientInfo": { "name": "e2e-agent", "version": "1.0.0" }
            }
        })
        .to_string(),
    );

    let init_resp = mcp.read_json_response();
    assert_eq!(init_resp["jsonrpc"], "2.0");
    assert_eq!(init_resp["id"], 1);
    assert_eq!(init_resp["result"]["protocolVersion"], "2024-11-05");
    assert_eq!(init_resp["result"]["serverInfo"]["name"], "oxideswarm");

    // 2. Notification: initialized
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        })
        .to_string(),
    );

    // 3. Ping
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "ping"
        })
        .to_string(),
    );
    let ping_resp = mcp.read_json_response();
    assert_eq!(ping_resp["id"], 2);
    assert_eq!(ping_resp["result"], json!({}));

    // 4. Tools list
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/list"
        })
        .to_string(),
    );
    let tools_resp = mcp.read_json_response();
    assert_eq!(tools_resp["id"], 3);
    let tools = tools_resp["result"]["tools"]
        .as_array()
        .expect("tools array");
    let tool_names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(tool_names.contains(&"status"));
    assert!(tool_names.contains(&"workers"));
    assert!(tool_names.contains(&"submit"));
    assert!(tool_names.contains(&"task_result"));
    assert!(tool_names.contains(&"doctor"));

    // 5. Call tool: status
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": { "workers": true }
            }
        })
        .to_string(),
    );
    let status_resp = mcp.read_json_response();
    if status_resp["result"]["isError"] == true {
        panic!("status_resp returned error: {:?}", status_resp);
    }
    assert_eq!(status_resp["id"], 4);
    assert_eq!(status_resp["result"]["isError"], false);
    let status_text = status_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let status_val: Value = serde_json::from_str(status_text).unwrap();
    assert_eq!(status_val["cluster_status"], "ok");
    assert!(status_val["connected_workers"].as_u64().unwrap() >= 1);

    // 6. Call tool: workers
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "workers",
                "arguments": { "connected_only": true }
            }
        })
        .to_string(),
    );
    let workers_resp = mcp.read_json_response();
    assert_eq!(workers_resp["id"], 5);
    assert_eq!(workers_resp["result"]["isError"], false);
    let workers_text = workers_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let workers_arr: Vec<Value> = serde_json::from_str(workers_text).unwrap();
    assert!(!workers_arr.is_empty());
    assert_eq!(workers_arr[0]["name"], "worker-mcp-e2e");
    assert_eq!(workers_arr[0]["connected"], true);

    // 7. Call tool: submit (synchronous wait: true)
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "echo",
                    "args": ["VERIFIED_MCP_PAYLOAD"],
                    "wait": true
                }
            }
        })
        .to_string(),
    );
    let submit_resp = mcp.read_json_response();
    assert_eq!(submit_resp["id"], 6);
    assert_eq!(submit_resp["result"]["isError"], false);
    let submit_text = submit_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let submit_val: Value = serde_json::from_str(submit_text).unwrap();
    assert_eq!(submit_val["exit_code"], 0);
    assert!(submit_val["stdout"]
        .as_str()
        .unwrap()
        .contains("VERIFIED_MCP_PAYLOAD"));

    // 8. Call tool: submit (async wait: false) followed by task_result with wait: true
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "echo",
                    "args": ["ASYNC_POLL_VERIFIED"],
                    "wait": false
                }
            }
        })
        .to_string(),
    );
    let async_submit_resp = mcp.read_json_response();
    assert_eq!(async_submit_resp["id"], 7);
    assert_eq!(async_submit_resp["result"]["isError"], false);
    let async_submit_text = async_submit_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let async_submit_val: Value = serde_json::from_str(async_submit_text).unwrap();
    let task_id = async_submit_val["task_id"].as_str().unwrap();

    // Query task_result
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {
                "name": "task_result",
                "arguments": {
                    "task_id": task_id,
                    "wait": true,
                    "timeout_secs": 10
                }
            }
        })
        .to_string(),
    );
    let task_res_resp = mcp.read_json_response();
    assert_eq!(task_res_resp["id"], 8);
    assert_eq!(task_res_resp["result"]["isError"], false);
    let task_res_text = task_res_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let task_res_val: Value = serde_json::from_str(task_res_text).unwrap();
    assert_eq!(task_res_val["task_id"], task_id);
    assert_eq!(task_res_val["exit_code"], 0);
    assert!(task_res_val["stdout"]
        .as_str()
        .unwrap()
        .contains("ASYNC_POLL_VERIFIED"));

    // 9. Call tool: doctor
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {
                "name": "doctor",
                "arguments": {
                    "expect_workers": 1,
                    "skip_gpu": true,
                    "skip_spread": true
                }
            }
        })
        .to_string(),
    );
    let doctor_resp = mcp.read_json_response();
    assert_eq!(doctor_resp["id"], 9);
    assert_eq!(doctor_resp["result"]["isError"], false);
    let doctor_text = doctor_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let doctor_val: Value = serde_json::from_str(doctor_text).unwrap();
    let checks = doctor_val["checks"].as_array().expect("checks array");
    assert!(!checks.is_empty());
    assert_eq!(checks[0]["id"], "C1");
    assert_eq!(checks[0]["outcome"], "pass");

    // 10. STDOUT HYGIENE VERIFICATION
    // Assert that EVERY single line emitted on stdout was valid JSON-RPC 2.0.
    for (idx, line) in mcp.lines_received.iter().enumerate() {
        let parsed: Result<Value, _> = serde_json::from_str(line);
        assert!(
            parsed.is_ok(),
            "Line {idx} on stdout was not valid JSON: '{line}'"
        );
        let val = parsed.unwrap();
        assert_eq!(
            val["jsonrpc"], "2.0",
            "Line {idx} on stdout missing jsonrpc 2.0: '{line}'"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_server_authentication_failure_handling() {
    let cluster = TestCluster::spawn(0x77).await;
    let home = tempfile::tempdir().expect("temp home");

    // Wrong secret hex (32 zero bytes)
    let bad_secret_hex = "0000000000000000000000000000000000000000000000000000000000000000";
    let mut mcp = McpProcess::spawn(home.path(), &cluster.master_addr, Some(bad_secret_hex));

    // Initialize succeeds (local protocol handshake)
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "clientInfo": { "name": "bad-auth-agent", "version": "1.0" }
            }
        })
        .to_string(),
    );

    let init_resp = mcp.read_json_response();
    assert_eq!(init_resp["jsonrpc"], "2.0");
    assert_eq!(init_resp["id"], 1);

    // Calling status should fail gracefully due to auth failure
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": {}
            }
        })
        .to_string(),
    );

    let status_resp = mcp.read_json_response();
    assert_eq!(status_resp["id"], 2);
    // Either isError == true with auth rejection detail, or JSON-RPC error
    let is_error = status_resp["result"]["isError"].as_bool().unwrap_or(false)
        || status_resp.get("error").is_some();
    assert!(
        is_error,
        "Tool call with invalid cluster secret must report an error"
    );
}
