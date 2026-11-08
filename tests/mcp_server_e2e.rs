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

        let blob_dir = tempfile::tempdir().expect("blob store dir");
        let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(true)
            .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
            .with_cluster_secret(secret.clone())
            .with_blob_store(blob_dir.path(), 64 * 1024 * 1024);

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
            _sandbox_dirs: vec![blob_dir],
        };

        // Spawn a test worker
        cluster.spawn_worker("worker-mcp-e2e", &["tag:fast", "region:local"], true);
        cluster.wait_for_workers(1).await;

        cluster
    }

    fn spawn_worker(&mut self, name: &str, tags: &[&str], simulate_gpu: bool) {
        let sandbox_dir = tempfile::tempdir().expect("sandbox dir");
        let config = WorkerConfig::new(self.master_addr.clone())
            .with_no_battery(true)
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

/// Returns a `Command` for `oxideswarm` isolated in `home` (both `HOME`/`USERPROFILE` and `.current_dir`).
fn mcp_command(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.current_dir(home)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env_remove("OXIDESWARM_HOME")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_INSECURE")
        .env("NO_COLOR", "1");
    cmd
}

/// Helper to run child process with piped stdio and isolated environment.
struct McpProcess {
    child: Child,
    stdout: BufReader<std::process::ChildStdout>,
    lines_received: Vec<String>,
}

impl McpProcess {
    fn spawn(home: &Path, master_addr: &str, secret_hex: Option<&str>) -> Self {
        let mut cmd = mcp_command(home);
        cmd.arg("mcp")
            .arg("--master")
            .arg(master_addr)
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
    assert!(tool_names.contains(&"workspaces"));

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

#[test]
fn test_mcp_cli_rejects_sse_flags() {
    let home = tempfile::tempdir().expect("temp home");
    for extra in [&["--sse"][..], &["--sse-bind", "127.0.0.1:8085"][..]] {
        let out = mcp_command(home.path())
            .arg("mcp")
            .args(extra)
            .output()
            .expect("spawn oxideswarm mcp");
        assert!(
            !out.status.success(),
            "expected clap to reject {:?}, got exit {:?}",
            extra,
            out.status
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("unexpected argument"),
            "expected clap 'unexpected argument' in stderr for {:?}, got: {stderr}",
            extra
        );
    }
}

fn wait_for_exit_with_open_stdin(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(s) = child.try_wait().expect("try_wait") {
            return s;
        }
        assert!(
            Instant::now() < deadline,
            "mcp should exit before reading from open stdin"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn run_mcp_initialize_and_exit(mut child: Child) -> (String, String) {
    {
        let stdin = child.stdin.as_mut().expect("stdin");
        let req = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "1.0" }
            }
        });
        writeln!(stdin, "{req}").expect("write initialize");
        stdin.flush().expect("flush");
    }
    drop(child.stdin.take());

    let out = child
        .wait_with_output()
        .expect("wait_with_output for mcp child");
    assert!(
        out.status.success(),
        "mcp should exit 0 after EOF, got {:?}",
        out.status
    );
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn test_mcp_refuses_remote_master_without_secret_and_allows_insecure() {
    let home = tempfile::tempdir().expect("temp home");

    // 1. Without a secret or --insecure, `mcp --master 192.0.2.10:8088` exits 6 before reading stdin
    let mut child = mcp_command(home.path())
        .args(["mcp", "--master", "192.0.2.10:8088"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp without secret");

    let status = wait_for_exit_with_open_stdin(&mut child);
    assert_eq!(
        status.code(),
        Some(6),
        "mcp without secret to remote master must exit with code 6"
    );

    // 2. With --insecure, it starts, warns on stderr, and answers initialize
    let child_insecure = mcp_command(home.path())
        .args(["mcp", "--master", "192.0.2.10:8088", "--insecure"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp with --insecure");

    let (stdout_str, stderr_str) = run_mcp_initialize_and_exit(child_insecure);
    assert!(
        stdout_str.contains("\"protocolVersion\":\"2024-11-05\""),
        "expected initialize response on stdout, got: {stdout_str}"
    );
    assert!(
        stderr_str.contains("WARNING: --insecure"),
        "expected --insecure warning on stderr, got: {stderr_str}"
    );
}

#[test]
fn test_mcp_skips_working_directory_config_file_unless_explicitly_pointed_to() {
    let home = tempfile::tempdir().expect("temp home");
    let cwd_cfg = home.path().join("rusty-grid.toml");
    std::fs::write(
        &cwd_cfg,
        r#"[status]
master = "192.0.2.10:8088"
insecure = true
"#,
    )
    .expect("write working-dir rusty-grid.toml");

    // 1. Bare `oxideswarm mcp` started in a folder holding ./rusty-grid.toml skips it,
    // logs a one-line notice on stderr, has no --insecure warning, and targets 127.0.0.1:8088.
    let mut child = mcp_command(home.path())
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp in working dir with rusty-grid.toml");

    {
        let stdin = child.stdin.as_mut().expect("stdin");
        let init_req = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "1.0" }
            }
        });
        let status_req = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": {}
            }
        });
        writeln!(stdin, "{init_req}").expect("write initialize");
        writeln!(stdin, "{status_req}").expect("write status call");
        stdin.flush().expect("flush");
    }
    drop(child.stdin.take());

    let out = child
        .wait_with_output()
        .expect("wait_with_output for bare mcp");
    assert!(
        out.status.success(),
        "bare mcp should exit 0 after EOF, got {:?}",
        out.status
    );
    let stdout_str = String::from_utf8_lossy(&out.stdout);
    let stderr_str = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr_str.contains("Skipping config file ./rusty-grid.toml in the working directory"),
        "expected working-directory skip line on stderr, got: {stderr_str}"
    );
    assert!(
        !stderr_str.contains("WARNING: --insecure"),
        "working-directory insecure = true must not be applied, stderr: {stderr_str}"
    );
    assert!(
        !stdout_str.contains("192.0.2.10") && !stderr_str.contains("192.0.2.10"),
        "status tool call must not target the skipped working-dir config host 192.0.2.10, stdout: {stdout_str}, stderr: {stderr_str}"
    );

    // 2. With RUSTY_GRID_CONFIG pointing to the same file, mcp uses it and starts with --insecure warning.
    let child_with_env_cfg = mcp_command(home.path())
        .arg("mcp")
        .env("RUSTY_GRID_CONFIG", &cwd_cfg)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp with RUSTY_GRID_CONFIG");

    let (stdout_env, stderr_env) = run_mcp_initialize_and_exit(child_with_env_cfg);
    assert!(
        stdout_env.contains("\"protocolVersion\":\"2024-11-05\""),
        "expected initialize response on stdout, got: {stdout_env}"
    );
    assert!(
        stderr_env.contains("WARNING: --insecure"),
        "expected --insecure warning when RUSTY_GRID_CONFIG is set, got: {stderr_env}"
    );
    assert!(
        !stderr_env.contains("Skipping config file"),
        "explicit RUSTY_GRID_CONFIG must not print the skip notice, got: {stderr_env}"
    );

    // 3. With `--config` pointing to the file, mcp uses it without printing the skip notice.
    let child_with_flag_cfg = mcp_command(home.path())
        .args(["--config", cwd_cfg.to_str().unwrap(), "mcp"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp with --config");

    let (stdout_flag, stderr_flag) = run_mcp_initialize_and_exit(child_with_flag_cfg);
    assert!(
        stdout_flag.contains("\"protocolVersion\":\"2024-11-05\""),
        "expected initialize response on stdout, got: {stdout_flag}"
    );
    assert!(
        stderr_flag.contains("WARNING: --insecure"),
        "expected --insecure warning when --config is passed, got: {stderr_flag}"
    );
    assert!(
        !stderr_flag.contains("Skipping config file"),
        "explicit --config must not print the skip notice, got: {stderr_flag}"
    );

    // 4. Bare `oxideswarm mcp` in a working directory holding `./rusty-grid.json` skips it too.
    std::fs::remove_file(&cwd_cfg).expect("remove rusty-grid.toml");
    let cwd_json = home.path().join("rusty-grid.json");
    std::fs::write(
        &cwd_json,
        r#"{"status": {"master": "192.0.2.10:8088", "insecure": true}}"#,
    )
    .expect("write working-dir rusty-grid.json");

    let child_json = mcp_command(home.path())
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp in working dir with rusty-grid.json");

    let (stdout_json, stderr_json) = run_mcp_initialize_and_exit(child_json);
    assert!(
        stdout_json.contains("\"protocolVersion\":\"2024-11-05\""),
        "expected initialize response on stdout, got: {stdout_json}"
    );
    assert!(
        stderr_json.contains("Skipping config file ./rusty-grid.json in the working directory"),
        "expected working-directory skip line for ./rusty-grid.json on stderr, got: {stderr_json}"
    );
    assert!(
        !stderr_json.contains("WARNING: --insecure"),
        "working-directory rusty-grid.json insecure = true must not be applied, stderr: {stderr_json}"
    );
    std::fs::remove_file(&cwd_json).expect("remove rusty-grid.json");

    // 5. `~/.oxideswarm/rusty-grid.toml` is loaded automatically, and when the working directory
    // IS `~/.oxideswarm`, no misleading skip notice is printed for the file that gets loaded next.
    let ox_dir = home.path().join(".oxideswarm");
    std::fs::create_dir_all(&ox_dir).expect("create ~/.oxideswarm");
    let user_cfg = ox_dir.join("rusty-grid.toml");
    std::fs::write(
        &user_cfg,
        r#"[status]
master = "192.0.2.10:8088"
insecure = true
"#,
    )
    .expect("write ~/.oxideswarm/rusty-grid.toml");

    let child_user_cfg = mcp_command(home.path())
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp loading ~/.oxideswarm/rusty-grid.toml");

    let (stdout_user, stderr_user) = run_mcp_initialize_and_exit(child_user_cfg);
    assert!(
        stdout_user.contains("\"protocolVersion\":\"2024-11-05\""),
        "expected initialize response on stdout, got: {stdout_user}"
    );
    assert!(
        stderr_user.contains("WARNING: --insecure"),
        "expected ~/.oxideswarm/rusty-grid.toml to be loaded, got stderr: {stderr_user}"
    );
    assert!(
        !stderr_user.contains("Skipping config file"),
        "expected no skip notice when working dir has no config file, got: {stderr_user}"
    );

    let child_in_ox_dir = mcp_command(home.path())
        .current_dir(&ox_dir)
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp with cwd = ~/.oxideswarm");

    let (stdout_ox_cwd, stderr_ox_cwd) = run_mcp_initialize_and_exit(child_in_ox_dir);
    assert!(
        stdout_ox_cwd.contains("\"protocolVersion\":\"2024-11-05\""),
        "expected initialize response on stdout, got: {stdout_ox_cwd}"
    );
    assert!(
        stderr_ox_cwd.contains("WARNING: --insecure"),
        "expected ~/.oxideswarm/rusty-grid.toml to be loaded when cwd is ~/.oxideswarm, got: {stderr_ox_cwd}"
    );
    assert!(
        !stderr_ox_cwd.contains("Skipping config file"),
        "must not print skip notice when ./rusty-grid.toml is the ~/.oxideswarm/rusty-grid.toml loaded next, got: {stderr_ox_cwd}"
    );

    // 6. When the working directory is `~/.oxideswarm` and ALSO holds `./rusty-grid.json`,
    // `mcp` loads `rusty-grid.toml` without a skip notice for the `.toml`, and still prints the
    // skip notice for `./rusty-grid.json`.
    let ox_json = ox_dir.join("rusty-grid.json");
    std::fs::write(
        &ox_json,
        r#"{"status": {"master": "192.0.2.99:8088", "insecure": true}}"#,
    )
    .expect("write ~/.oxideswarm/rusty-grid.json");

    let child_in_ox_dir_with_json = mcp_command(home.path())
        .current_dir(&ox_dir)
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp with cwd = ~/.oxideswarm containing both toml and json");

    let (stdout_ox_both, stderr_ox_both) = run_mcp_initialize_and_exit(child_in_ox_dir_with_json);
    assert!(
        stdout_ox_both.contains("\"protocolVersion\":\"2024-11-05\""),
        "expected initialize response on stdout, got: {stdout_ox_both}"
    );
    assert!(
        stderr_ox_both.contains("Skipping config file ./rusty-grid.json in the working directory"),
        "expected skip notice for ./rusty-grid.json when ./rusty-grid.toml is the loaded user config, got: {stderr_ox_both}"
    );
    assert!(
        !stderr_ox_both.contains("Skipping config file ./rusty-grid.toml"),
        "must not print skip notice for ./rusty-grid.toml, got: {stderr_ox_both}"
    );
}

#[test]
fn test_mcp_insecure_env_and_status_config_precedence() {
    let home = tempfile::tempdir().expect("temp home");

    // Case 1: RUSTY_GRID_INSECURE=1 starts mcp against a remote master with the warning.
    let child_env_insecure = mcp_command(home.path())
        .args(["mcp", "--master", "192.0.2.10:8088"])
        .env("RUSTY_GRID_INSECURE", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp with RUSTY_GRID_INSECURE=1");

    let (stdout_str, stderr_str) = run_mcp_initialize_and_exit(child_env_insecure);
    assert!(
        stdout_str.contains("\"protocolVersion\":\"2024-11-05\""),
        "expected initialize response on stdout, got: {stdout_str}"
    );
    assert!(
        stderr_str.contains("WARNING: --insecure"),
        "expected --insecure warning with RUSTY_GRID_INSECURE=1, got: {stderr_str}"
    );

    // Case 2: RUSTY_GRID_INSECURE=0 with [status] insecure = true exits 6.
    let cfg_insecure_true = home.path().join("status-insecure.toml");
    std::fs::write(
        &cfg_insecure_true,
        r#"[status]
master = "192.0.2.10:8088"
insecure = true
"#,
    )
    .expect("write status-insecure.toml");

    let mut child_env_zero = mcp_command(home.path())
        .arg("mcp")
        .env("RUSTY_GRID_CONFIG", &cfg_insecure_true)
        .env("RUSTY_GRID_INSECURE", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp with RUSTY_GRID_INSECURE=0 and [status] insecure = true");

    let status_env_zero = wait_for_exit_with_open_stdin(&mut child_env_zero);
    assert_eq!(
        status_env_zero.code(),
        Some(6),
        "RUSTY_GRID_INSECURE=0 must override [status] insecure = true and exit 6"
    );

    // Case 3: [status] master without --master (and without insecure) exits 6.
    let cfg_remote_only = home.path().join("status-remote.toml");
    std::fs::write(
        &cfg_remote_only,
        r#"[status]
master = "192.0.2.10:8088"
"#,
    )
    .expect("write status-remote.toml");

    let mut child_status_master = mcp_command(home.path())
        .arg("mcp")
        .env("RUSTY_GRID_CONFIG", &cfg_remote_only)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mcp with [status] master");

    let status_remote = wait_for_exit_with_open_stdin(&mut child_status_master);
    assert_eq!(
        status_remote.code(),
        Some(6),
        "[status] master without --master or a secret must exit 6"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_safe_llm_outputs_1mib_binary_and_20mib_stdout() {
    let cluster = TestCluster::spawn(0x6C).await;
    let home = tempfile::tempdir().expect("temp home");
    let outside = tempfile::tempdir().expect("outside dir");
    std::fs::write(outside.path().join("outside.txt"), b"secret").unwrap();

    // Prepare a Cargo.toml at the cwd root, a 64 KiB binary seed (containing non-UTF-8 bytes),
    // and a 64 KiB text seed inside `home` (the MCP cwd).
    std::fs::write(
        home.path().join("Cargo.toml"),
        b"[package]\nname = \"mcp-root-e2e\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    let seed_bin: Vec<u8> = (0..65_536)
        .map(|i| ((i * 131 + 0x80) & 0xFF) as u8)
        .collect();
    std::fs::write(home.path().join("seed.bin"), &seed_bin).unwrap();
    let chunk64k = vec![b'A'; 65_536];
    std::fs::write(home.path().join("chunk64k.txt"), &chunk64k).unwrap();

    let mut expected_1mib = Vec::with_capacity(1024 * 1024);
    for _ in 0..16 {
        expected_1mib.extend_from_slice(&seed_bin);
    }
    let expected_hash = rusty_grid_core::blob::BlobId::of(&expected_1mib).to_hex();

    let mut mcp = McpProcess::spawn(home.path(), &cluster.master_addr, Some(&cluster.secret_hex));

    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "clientInfo": { "name": "mcp2-test", "version": "1.0" }
            }
        })
        .to_string(),
    );
    let _ = mcp.read_json_response();

    // 1. Stage `files: ["."]` and verify the task reads `Cargo.toml` at the sandbox root.
    let root_script = "cat Cargo.toml";
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "shell",
                    "script": root_script,
                    "files": ["."],
                    "wait": true,
                    "timeout_secs": 20
                }
            }
        })
        .to_string(),
    );
    let root_resp = mcp.read_json_response();
    assert_eq!(
        root_resp["result"]["isError"], false,
        "submit files:[\".\"] failed: {root_resp:?}"
    );
    let root_body: Value =
        serde_json::from_str(root_resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(root_body["exit_code"], 0);
    assert!(
        root_body["stdout"]
            .as_str()
            .unwrap()
            .contains("mcp-root-e2e"),
        "expected Cargo.toml at sandbox root when staged with files:[\".\"], got: {}",
        root_body["stdout"]
    );

    // 2. Verify `submit` refuses paths outside the working directory (`..` and absolute outside path)
    //    and symbolic links in `files` and `output_dir`.
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "echo",
                    "files": ["../outside.txt"]
                }
            }
        })
        .to_string(),
    );
    let parent_resp = mcp.read_json_response();
    assert_eq!(parent_resp["result"]["isError"], true);

    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 20,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "echo",
                    "output_dir": "../outside_out"
                }
            }
        })
        .to_string(),
    );
    let out_parent_resp = mcp.read_json_response();
    assert_eq!(out_parent_resp["result"]["isError"], true);

    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 21,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "echo",
                    "output_dir": outside.path().join("escaped_out").to_string_lossy()
                }
            }
        })
        .to_string(),
    );
    let out_abs_resp = mcp.read_json_response();
    assert_eq!(out_abs_resp["result"]["isError"], true);

    #[cfg(unix)]
    {
        let symlink_path = home.path().join("symlink_escape.txt");
        std::os::unix::fs::symlink(outside.path().join("outside.txt"), &symlink_path).unwrap();
        mcp.send_line(
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "submit",
                    "arguments": {
                        "type": "command",
                        "command": "echo",
                        "files": ["symlink_escape.txt"]
                    }
                }
            })
            .to_string(),
        );
        let sym_resp = mcp.read_json_response();
        assert_eq!(sym_resp["result"]["isError"], true);
        let sym_text = sym_resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(sym_text.contains("symbolic link"), "{sym_text}");
        std::fs::remove_file(&symlink_path).unwrap();

        let symlink_out = home.path().join("symlink_out_dir");
        std::os::unix::fs::symlink(outside.path(), &symlink_out).unwrap();
        mcp.send_line(
            &json!({
                "jsonrpc": "2.0",
                "id": 30,
                "method": "tools/call",
                "params": {
                    "name": "submit",
                    "arguments": {
                        "type": "command",
                        "command": "echo",
                        "output_dir": "symlink_out_dir"
                    }
                }
            })
            .to_string(),
        );
        let sym_out_resp = mcp.read_json_response();
        assert_eq!(sym_out_resp["result"]["isError"], true);
        let sym_out_text = sym_out_resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap();
        assert!(sym_out_text.contains("symbolic link"), "{sym_out_text}");
        std::fs::remove_file(&symlink_out).unwrap();
    }

    // 2. Submit a task that stages `seed.bin` and `chunk64k.txt` via blobs, writes a 1 MiB binary
    //    output `out.bin`, and writes 20 MiB of stdout ending in `FINAL_SUMMARY_MARKER_MCP2_OK`.
    let script = "\
cat seed.bin seed.bin seed.bin seed.bin > b256k.bin
cat b256k.bin b256k.bin b256k.bin b256k.bin > out.bin
echo START_OF_20MIB_STDOUT
cat chunk64k.txt chunk64k.txt chunk64k.txt chunk64k.txt > t256k.txt
cat t256k.txt t256k.txt t256k.txt t256k.txt > t1m.txt
cat t1m.txt t1m.txt t1m.txt t1m.txt t1m.txt > t5m.txt
cat t5m.txt t5m.txt t5m.txt t5m.txt
echo FINAL_SUMMARY_MARKER_MCP2_OK
";

    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "shell",
                    "script": script,
                    "files": ["seed.bin", "chunk64k.txt"],
                    "outputs": ["out.bin"],
                    "wait": false
                }
            }
        })
        .to_string(),
    );
    let submit_resp = mcp.read_json_response();
    assert_eq!(
        submit_resp["result"]["isError"], false,
        "submit failed: {submit_resp:?}"
    );
    let submit_body: Value = serde_json::from_str(
        submit_resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let task_id = submit_body["task_id"].as_str().unwrap();

    // 3. Call `task_result` with `output_dir` and `max_output_bytes: 4096`.
    let out_dir = home.path().join("downloaded_outputs");
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "task_result",
                "arguments": {
                    "task_id": task_id,
                    "wait": true,
                    "timeout_secs": 30,
                    "output_dir": out_dir.to_string_lossy(),
                    "max_output_bytes": 4096
                }
            }
        })
        .to_string(),
    );
    let res_resp = mcp.read_json_response();
    let raw_reply_line = mcp.lines_received.last().expect("last reply line");
    assert!(
        raw_reply_line.len() < 8 * 1024,
        "JSON-RPC reply ({} bytes) must be under 8 KiB",
        raw_reply_line.len()
    );
    assert_eq!(
        res_resp["result"]["isError"], false,
        "task_result failed: {res_resp:?}"
    );

    let res_body: Value =
        serde_json::from_str(res_resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(res_body["exit_code"], 0);
    assert_eq!(res_body["truncated"], true);
    assert_eq!(res_body["worker_truncated"], true);
    assert_eq!(res_body["stdout_truncated"], true);
    assert_eq!(res_body["stdout_worker_truncated"], true);
    assert!(
        res_body["captured_bytes"].as_u64().unwrap() >= 4 * 1024 * 1024,
        "expected captured_bytes >= 4 MiB, got {}",
        res_body["captured_bytes"]
    );
    assert_eq!(
        res_body["stdout_captured_bytes"],
        res_body["captured_bytes"]
    );

    let stdout_text = res_body["stdout"].as_str().unwrap();
    assert!(
        stdout_text.contains("START_OF_20MIB_STDOUT"),
        "expected head banner in truncated stdout, got len {}",
        stdout_text.len()
    );
    assert!(
        stdout_text.contains("FINAL_SUMMARY_MARKER_MCP2_OK"),
        "expected final marker at end of 20 MiB stdout to be preserved in tail, got: {stdout_text}"
    );

    let artifacts = res_body["artifacts"].as_array().expect("artifacts array");
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0]["path"], "out.bin");
    assert_eq!(artifacts[0]["size_bytes"], 1024 * 1024);
    assert_eq!(artifacts[0]["blob_id"], expected_hash);
    assert!(
        artifacts[0].get("data_preview").is_none(),
        "data_preview must be removed from artifacts"
    );

    let downloaded_bytes = std::fs::read(out_dir.join("out.bin")).expect("downloaded out.bin");
    assert_eq!(downloaded_bytes.len(), 1024 * 1024);
    let actual_hash = rusty_grid_core::blob::BlobId::of(&downloaded_bytes).to_hex();
    assert_eq!(
        actual_hash, expected_hash,
        "downloaded 1 MiB binary must match expected BLAKE3 hash without UTF-8 corruption"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_server_workspace_submission_and_workspaces_tool() {
    use rusty_grid_core::capabilities::WorkerCapabilities;
    use rusty_grid_core::cluster_auth::client_handshake;
    use rusty_grid_core::protocol::{
        MasterMessage, MessageTransport, ProtocolCapabilities, WorkerMessage,
        PROTOCOL_VERSION_CURRENT,
    };
    use rusty_grid_core::task::{TaskSpec, WorkspaceSource};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    let cluster = TestCluster::spawn(0x7D).await;
    let home = tempfile::tempdir().expect("temp home");

    // Populate a small workspace tree inside `home` with `.gitignore` excluding `/target/`.
    std::fs::create_dir_all(home.path().join("src")).unwrap();
    std::fs::create_dir_all(home.path().join("target")).unwrap();
    std::fs::write(
        home.path().join("Cargo.toml"),
        b"[package]\nname = \"ws-e2e\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    std::fs::write(home.path().join(".gitignore"), b"/target/\n").unwrap();
    std::fs::write(
        home.path().join("src/lib.rs"),
        b"pub fn add(a: i32, b: i32) -> i32 { a + b }\n",
    )
    .unwrap();
    std::fs::write(home.path().join("target/ignored.o"), b"ignored-artifact").unwrap();

    // Connect a worker session that advertises `ProtocolCapabilities::WORKSPACES`.
    let ws_worker_id = Uuid::new_v4();
    let stream = tokio::net::TcpStream::connect(&cluster.master_addr)
        .await
        .expect("connect workspace worker");
    let mut worker_transport = MessageTransport::new(stream);
    let session_key = client_handshake(&mut worker_transport, &cluster.secret)
        .await
        .expect("workspace worker handshake");
    worker_transport.enable_encryption(
        &session_key,
        rusty_grid_core::protocol::EndpointRole::Client,
    );

    let mut caps = WorkerCapabilities::new("ws-worker-e2e", 4, 4096, false, false, None);
    caps.tags = vec!["os:macos".to_string(), "arch:aarch64".to_string()];
    worker_transport
        .send_msg(&WorkerMessage::Register {
            worker_id: ws_worker_id,
            capabilities: caps,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported() | ProtocolCapabilities::WORKSPACES,
        })
        .await
        .expect("send Register");
    match worker_transport
        .recv_msg::<MasterMessage>()
        .await
        .expect("recv RegisterAck")
    {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("expected MasterMessage::RegisterAck, got {other:?}"),
    }

    let assigned_tasks: Arc<Mutex<Vec<rusty_grid_core::task::Task>>> =
        Arc::new(Mutex::new(Vec::new()));
    let assigned_tasks_clone = Arc::clone(&assigned_tasks);
    let worker_loop = tokio::spawn(async move {
        while let Ok(Some(msg)) = worker_transport.recv_msg::<MasterMessage>().await {
            if let MasterMessage::AssignTask { task } = msg {
                let task_id = task.id;
                let ws_name = task
                    .workspace
                    .as_ref()
                    .map(|w| w.name.clone())
                    .unwrap_or_default();
                let cwd = match &task.spec {
                    TaskSpec::Command { working_dir, .. } => working_dir.clone(),
                    _ => None,
                };
                assigned_tasks_clone.lock().await.push(task);
                let stdout = format!("ws={ws_name} cwd={cwd:?}\n").into_bytes();
                let _ = worker_transport
                    .send_msg(&WorkerMessage::TaskResult {
                        worker_id: ws_worker_id,
                        task_id,
                        exit_code: 0,
                        stdout: stdout.into(),
                        stderr: Vec::new().into(),
                        execution_time_ms: 15,
                        is_gpu_executed: false,
                        device_name: None,
                        error: None,
                        asset_bytes: None,
                        artifacts: Vec::new(),
                        outputs: Vec::new(),
                    })
                    .await;
            }
        }
    });

    let mut mcp = McpProcess::spawn(home.path(), &cluster.master_addr, Some(&cluster.secret_hex));
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "clientInfo": { "name": "ws-e2e-agent", "version": "1.0.0" }
            }
        })
        .to_string(),
    );
    let _ = mcp.read_json_response();

    // 1. `workspaces` is empty before any workspace task completes.
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "workspaces",
                "arguments": {}
            }
        })
        .to_string(),
    );
    let ws_empty_resp = mcp.read_json_response();
    assert_eq!(ws_empty_resp["result"]["isError"], false);
    let ws_empty_arr: Vec<Value> = serde_json::from_str(
        ws_empty_resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert!(ws_empty_arr.is_empty());

    // 2. Passing `workspace` together with `files` is refused with JSON-RPC -32602.
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "cargo",
                    "files": ["Cargo.toml"],
                    "workspace": {
                        "name": "oxideswarm"
                    }
                }
            }
        })
        .to_string(),
    );
    let conflict_resp = mcp.read_json_response();
    assert_eq!(conflict_resp["id"], 3);
    assert_eq!(conflict_resp["error"]["code"], -32602);

    // 3. Submit a workspace task syncing `.` with `cwd: "src"` and no explicit `timeout_secs`.
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "cargo",
                    "args": ["test", "--lib"],
                    "workspace": {
                        "name": "oxideswarm",
                        "sync_dir": ".",
                        "cwd": "src"
                    },
                    "wait": true
                }
            }
        })
        .to_string(),
    );
    let submit_resp = mcp.read_json_response();
    assert_eq!(
        submit_resp["result"]["isError"], false,
        "workspace submit failed: {submit_resp:?}"
    );
    let submit_body: Value = serde_json::from_str(
        submit_resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(submit_body["exit_code"], 0);
    assert!(submit_body["stdout"]
        .as_str()
        .unwrap()
        .contains("ws=oxideswarm"));
    assert_eq!(submit_body["workspace"]["name"], "oxideswarm");
    assert_eq!(submit_body["workspace"]["worker"], ws_worker_id.to_string());
    assert_eq!(submit_body["workspace"]["files"], 3);
    assert!(submit_body["workspace"]["uploaded_bytes"].as_u64().unwrap() > 0);
    assert_eq!(submit_body["workspace"]["skipped"], 0);

    {
        let tasks = assigned_tasks.lock().await;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].requirements.timeout_secs, 1800);
        let ws = tasks[0].workspace.as_ref().expect("workspace spec");
        assert_eq!(ws.name, "oxideswarm");
        assert!(matches!(ws.source, Some(WorkspaceSource::Tree { .. })));
        match &tasks[0].spec {
            TaskSpec::Command { working_dir, .. } => {
                assert_eq!(*working_dir, Some(PathBuf::from("src")));
            }
            other => panic!("expected Command spec, got {other:?}"),
        }
    }

    // 4. Submit a workspace task with `sync_dir: null` (no sync).
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "cargo",
                    "args": ["check"],
                    "workspace": {
                        "name": "oxideswarm",
                        "sync_dir": null
                    },
                    "wait": true
                }
            }
        })
        .to_string(),
    );
    let nosync_resp = mcp.read_json_response();
    assert_eq!(nosync_resp["result"]["isError"], false);
    let nosync_body: Value = serde_json::from_str(
        nosync_resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(nosync_body["workspace"]["name"], "oxideswarm");
    assert_eq!(nosync_body["workspace"]["worker"], ws_worker_id.to_string());
    assert_eq!(nosync_body["workspace"]["files"], 0);
    assert_eq!(nosync_body["workspace"]["uploaded_bytes"], 0);
    assert_eq!(nosync_body["workspace"]["skipped"], 0);

    // 5. Query the `workspaces` MCP tool and CLI subcommand: both report the recorded affinity.
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "workspaces",
                "arguments": {}
            }
        })
        .to_string(),
    );
    let ws_resp = mcp.read_json_response();
    assert_eq!(ws_resp["result"]["isError"], false);
    let ws_arr: Vec<Value> =
        serde_json::from_str(ws_resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(ws_arr.len(), 1);
    assert_eq!(ws_arr[0]["name"], "oxideswarm");
    assert_eq!(ws_arr[0]["worker"], ws_worker_id.to_string());
    assert!(ws_arr[0]["last_used_utc"].as_u64().unwrap() > 0);

    let cli_out = mcp_command(home.path())
        .args([
            "--secret",
            &cluster.secret_hex,
            "workspaces",
            "--master",
            &cluster.master_addr,
            "--json",
        ])
        .output()
        .expect("run oxideswarm workspaces --json");
    assert!(
        cli_out.status.success(),
        "workspaces CLI failed: {}",
        String::from_utf8_lossy(&cli_out.stderr)
    );
    let cli_arr: Vec<Value> = serde_json::from_slice(&cli_out.stdout).unwrap();
    assert_eq!(cli_arr.len(), 1);
    assert_eq!(cli_arr[0]["name"], "oxideswarm");
    assert_eq!(cli_arr[0]["worker"], ws_worker_id.to_string());
    assert!(cli_arr[0]["last_used_utc"].as_u64().unwrap() > 0);

    worker_loop.abort();
}
