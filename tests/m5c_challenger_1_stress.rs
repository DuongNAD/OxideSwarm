//! Adversarial Stress Test Suite for Milestone M5C: MCP Server for AI Agents (Requirement R3).
//!
//! Author: Challenger 1 (critic, specialist)
//! Focus Areas:
//! 1. Protocol framing stress:
//!    - Malformed JSON lines (syntax errors, truncated objects, non-JSON strings, stray tokens).
//!    - Missing/invalid JSON-RPC fields (missing jsonrpc, invalid version, missing method, invalid id types).
//!    - Unknown method names (responds with standard JSON-RPC -32601).
//!    - Stream resilience: server remains responsive and handles subsequent valid requests without terminating stdio stream.
//! 2. Tool call adversarial validation:
//!    - `status`: invalid UUID format, non-existent UUIDs, malformed arguments.
//!    - `workers`: contradictory and empty filters (gpu_only on non-GPU, invalid OS/arch strings).
//!    - `submit`: missing required fields (shell without script/command), non-existent staged files,
//!      sandbox path traversal attempts, malformed placement tags (spaces, reserved prefixes, length limits).
//!    - `task_result`: missing task_id, invalid UUID, non-existent UUID, zero timeout.
//!    - `doctor`: negative and extreme worker expectations, probe timeouts, skipping checks.
//! 3. Stdout hygiene adversarial check:
//!    - Under high-volume error generation, deliberate invalid requests, and backend disconnection:
//!    - 100% of lines emitted on stdout parse as valid JSON-RPC 2.0 frames.
//!    - Zero bytes of tracing logs, warnings, panics, or unformatted text on stdout.

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

// ============================================================================
// Test Cluster Harness
// ============================================================================

struct TestCluster {
    master: MasterHandle,
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
            master,
            master_addr,
            secret,
            secret_hex,
            workers: Vec::new(),
            _sandbox_dirs: Vec::new(),
        };

        // Spawn a test worker without GPU
        cluster.spawn_worker(
            "worker-challenger-m5c",
            &["tag:fast", "region:local"],
            false,
        );
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
            if let Ok(workers) = self.master.list_workers().await {
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

    fn shutdown_master(&self) {
        let _ = self.master.shutdown();
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

// ============================================================================
// McpProcess Helper with Strict Line Capture
// ============================================================================

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
        } else {
            cmd.arg("--insecure");
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
        let bytes_read = self
            .stdout
            .read_line(&mut line)
            .expect("read response line");
        assert!(
            bytes_read > 0,
            "stdout EOF encountered; process unexpectedly terminated"
        );
        let trimmed = line.trim();
        assert!(
            !trimmed.is_empty(),
            "expected non-empty JSON-RPC line from child stdout"
        );
        self.lines_received.push(trimmed.to_string());

        serde_json::from_str(trimmed)
            .unwrap_or_else(|e| panic!("failed to parse JSON from stdout line '{trimmed}': {e}"))
    }

    fn assert_all_stdout_hygiene(&self) {
        assert!(
            !self.lines_received.is_empty(),
            "at least one line must have been captured"
        );
        for (idx, line) in self.lines_received.iter().enumerate() {
            let parsed: Result<Value, _> = serde_json::from_str(line);
            assert!(
                parsed.is_ok(),
                "Line {idx} on stdout was not valid JSON: '{line}'"
            );
            let val = parsed.unwrap();
            assert!(val.is_object(), "Line {idx} on stdout is not a JSON object");
            assert_eq!(
                val["jsonrpc"], "2.0",
                "Line {idx} missing jsonrpc '2.0': '{line}'"
            );
            // Verify no tracing leaks as raw substrings
            assert!(
                !line.starts_with("INFO"),
                "Line {idx} leaked INFO tracing on stdout: '{line}'"
            );
            assert!(
                !line.starts_with("DEBUG"),
                "Line {idx} leaked DEBUG tracing on stdout: '{line}'"
            );
            assert!(
                !line.starts_with("WARN"),
                "Line {idx} leaked WARN tracing on stdout: '{line}'"
            );
            assert!(
                !line.starts_with("ERROR"),
                "Line {idx} leaked ERROR tracing on stdout: '{line}'"
            );
        }
    }
}

impl Drop for McpProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

// ============================================================================
// 1. Protocol Framing Stress Test
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_protocol_framing_stress() {
    let home = tempfile::tempdir().expect("temp home");
    // Mcp process can run framing checks against dummy master since framing errors are local to stdio loop
    let mut mcp = McpProcess::spawn(home.path(), "127.0.0.1:9", None);

    // 1.1 Non-JSON raw strings
    let raw_garbage_cases = [
        "THIS_IS_NOT_JSON",
        "<<< XML TAG >>>",
        "SELECT * FROM tasks;",
        "HTTP/1.1 200 OK",
        "{ not closed",
        "{\"jsonrpc\": \"2.0\", \"id\": 1,",
        "{\"jsonrpc\": \"2.0\", \"id\": }",
        "[1, 2, 3]",
        "\"raw string literal\"",
        "12345678",
        "null",
        "true",
    ];

    for (idx, garbage) in raw_garbage_cases.iter().enumerate() {
        mcp.send_line(garbage);
        let resp = mcp.read_json_response();
        assert_eq!(resp["jsonrpc"], "2.0", "case {idx}: jsonrpc must be 2.0");
        assert!(
            resp.get("error").is_some(),
            "case {idx} ('{garbage}') must return error object"
        );
        let err_code = resp["error"]["code"].as_i64().expect("error code");
        assert_eq!(
            err_code, -32700,
            "case {idx} ('{garbage}') expected PARSE_ERROR (-32700), got {err_code}"
        );
    }

    // 1.2 Empty lines and whitespace (must be skipped gracefully with no reply and no crash)
    mcp.send_line("");
    mcp.send_line("   ");
    mcp.send_line("\t\t\t");
    mcp.send_line("\r");

    // Verify server is still alive after whitespace by sending a ping
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 100,
            "method": "ping"
        })
        .to_string(),
    );
    let ping_resp = mcp.read_json_response();
    assert_eq!(ping_resp["id"], 100);
    assert_eq!(ping_resp["result"], json!({}));

    // 1.3 Missing required JSON-RPC fields
    let missing_field_cases = [
        ("{}", -32700),
        ("{\"id\": 101, \"method\": \"ping\"}", -32700), // missing jsonrpc
        ("{\"jsonrpc\": \"2.0\", \"id\": 102}", -32700), // missing method
    ];

    for (idx, (payload, expected_code)) in missing_field_cases.iter().enumerate() {
        mcp.send_line(payload);
        let resp = mcp.read_json_response();
        assert_eq!(resp["jsonrpc"], "2.0");
        let err_code = resp["error"]["code"].as_i64().expect("error code");
        assert_eq!(
            err_code, *expected_code,
            "missing field case {idx} ('{payload}')"
        );
    }

    // 1.4 Invalid JSON-RPC protocol version
    let bad_version_cases = [
        json!({"jsonrpc": "1.0", "id": 201, "method": "ping"}),
        json!({"jsonrpc": "2.1", "id": 202, "method": "ping"}),
        json!({"jsonrpc": "", "id": 203, "method": "ping"}),
        json!({"jsonrpc": "mcp", "id": 204, "method": "ping"}),
    ];

    for (idx, payload) in bad_version_cases.iter().enumerate() {
        mcp.send_line(&payload.to_string());
        let resp = mcp.read_json_response();
        assert_eq!(resp["jsonrpc"], "2.0");
        let expected_id = payload["id"].clone();
        assert_eq!(resp["id"], expected_id);
        let err_code = resp["error"]["code"].as_i64().expect("error code");
        assert_eq!(
            err_code, -32600,
            "bad version case {idx} expected INVALID_REQUEST (-32600), got {err_code}"
        );
    }

    // 1.5 Unknown method names
    let unknown_methods = [
        "non_existent_method",
        "tools/execute",
        "admin/shutdown",
        "$/cancelRequest",
        "system/reboot",
    ];

    for (idx, method) in unknown_methods.iter().enumerate() {
        let req_id = 300 + idx as i64;
        mcp.send_line(
            &json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": method
            })
            .to_string(),
        );
        let resp = mcp.read_json_response();
        assert_eq!(resp["id"], req_id);
        let err_code = resp["error"]["code"].as_i64().expect("error code");
        assert_eq!(
            err_code, -32601,
            "unknown method case '{method}' expected METHOD_NOT_FOUND (-32601)"
        );
    }

    // 1.6 Varied id types (string, large integer, null)
    let varied_ids = [
        json!("req-uuid-999-aaa"),
        json!("string_id_with_symbols@#$%"),
        json!(9007199254740991i64), // JS Number.MAX_SAFE_INTEGER
        json!(0),
        json!(-42),
        Value::Null,
    ];

    for (idx, test_id) in varied_ids.iter().enumerate() {
        mcp.send_line(
            &json!({
                "jsonrpc": "2.0",
                "id": test_id,
                "method": "ping"
            })
            .to_string(),
        );
        let resp = mcp.read_json_response();
        assert_eq!(
            resp["id"], *test_id,
            "varied id case {idx} id echo mismatch"
        );
        assert_eq!(resp["result"], json!({}));
    }

    // 1.7 Stream liveness check: After all the stress above, server responds cleanly to tools/list
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 9999,
            "method": "tools/list"
        })
        .to_string(),
    );
    let list_resp = mcp.read_json_response();
    assert_eq!(list_resp["id"], 9999);
    assert!(list_resp["result"]["tools"].is_array());

    // Verify 100% of captured lines conform to JSON-RPC stdout hygiene
    mcp.assert_all_stdout_hygiene();
}

// ============================================================================
// 2. Tool Call Adversarial Validation Test
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_tool_adversarial_validation() {
    let cluster = TestCluster::spawn(0x55).await;
    let home = tempfile::tempdir().expect("temp home");

    let mut mcp = McpProcess::spawn(home.path(), &cluster.master_addr, Some(&cluster.secret_hex));

    // Initialize handshake
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "clientInfo": { "name": "challenger-agent", "version": "1.0" }
            }
        })
        .to_string(),
    );
    let init_resp = mcp.read_json_response();
    assert_eq!(init_resp["id"], 1);

    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        })
        .to_string(),
    );

    // 2.1 Missing 'name' in tools/call
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": { "arguments": {} }
        })
        .to_string(),
    );
    let missing_name_resp = mcp.read_json_response();
    assert_eq!(missing_name_resp["id"], 2);
    assert_eq!(missing_name_resp["error"]["code"], -32602); // INVALID_PARAMS

    // 2.2 Unknown tool name in tools/call
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "non_existent_tool_xyz",
                "arguments": {}
            }
        })
        .to_string(),
    );
    let unknown_tool_resp = mcp.read_json_response();
    assert_eq!(unknown_tool_resp["id"], 3);
    assert_eq!(unknown_tool_resp["result"]["isError"], true);
    let text = unknown_tool_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(text.contains("Unknown tool: 'non_existent_tool_xyz'"));

    // 2.3 `status` tool adversarial validation
    // - Invalid UUID format
    let bad_uuids = [
        "not-a-uuid",
        "12345",
        "0000-0000-0000",
        "g0000000-0000-0000-0000-000000000000",
        "",
    ];
    for (idx, bad_uuid) in bad_uuids.iter().enumerate() {
        let req_id = 10 + idx as i64;
        mcp.send_line(
            &json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {
                    "name": "status",
                    "arguments": { "task_id": bad_uuid }
                }
            })
            .to_string(),
        );
        let resp = mcp.read_json_response();
        assert_eq!(resp["id"], req_id);
        assert_eq!(
            resp["result"]["isError"], true,
            "bad uuid '{bad_uuid}' must yield isError=true"
        );
        let err_text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(
            err_text.contains("Invalid task_id UUID"),
            "expected 'Invalid task_id UUID' in '{err_text}'"
        );
    }

    // - Non-existent UUID
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 20,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": { "task_id": "00000000-0000-0000-0000-000000000000" }
            }
        })
        .to_string(),
    );
    let not_found_resp = mcp.read_json_response();
    assert_eq!(not_found_resp["id"], 20);
    assert_eq!(not_found_resp["result"]["isError"], true);
    let nf_text = not_found_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(
        nf_text.contains("not found"),
        "expected 'not found' in '{nf_text}'"
    );

    // 2.4 `workers` tool contradictory and empty filters
    // - gpu_only: true on non-GPU cluster
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 30,
            "method": "tools/call",
            "params": {
                "name": "workers",
                "arguments": { "gpu_only": true }
            }
        })
        .to_string(),
    );
    let gpu_filter_resp = mcp.read_json_response();
    assert_eq!(gpu_filter_resp["id"], 30);
    assert_eq!(gpu_filter_resp["result"]["isError"], false);
    let gpu_text = gpu_filter_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let gpu_arr: Vec<Value> = serde_json::from_str(gpu_text).unwrap();
    assert!(
        gpu_arr.is_empty(),
        "gpu_only filter on non-gpu cluster must yield 0 workers"
    );

    // - Invalid OS filter
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 31,
            "method": "tools/call",
            "params": {
                "name": "workers",
                "arguments": { "os": "beos_haiku_1998" }
            }
        })
        .to_string(),
    );
    let os_resp = mcp.read_json_response();
    assert_eq!(os_resp["id"], 31);
    assert_eq!(os_resp["result"]["isError"], false);
    let os_text = os_resp["result"]["content"][0]["text"].as_str().unwrap();
    let os_arr: Vec<Value> = serde_json::from_str(os_text).unwrap();
    assert!(os_arr.is_empty());

    // - Invalid arch filter
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 32,
            "method": "tools/call",
            "params": {
                "name": "workers",
                "arguments": { "arch": "sparc64_unknown" }
            }
        })
        .to_string(),
    );
    let arch_resp = mcp.read_json_response();
    assert_eq!(arch_resp["id"], 32);
    assert_eq!(arch_resp["result"]["isError"], false);
    let arch_text = arch_resp["result"]["content"][0]["text"].as_str().unwrap();
    let arch_arr: Vec<Value> = serde_json::from_str(arch_text).unwrap();
    assert!(arch_arr.is_empty());

    // - Empty filter
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 33,
            "method": "tools/call",
            "params": {
                "name": "workers",
                "arguments": {}
            }
        })
        .to_string(),
    );
    let empty_filter_resp = mcp.read_json_response();
    assert_eq!(empty_filter_resp["id"], 33);
    assert_eq!(empty_filter_resp["result"]["isError"], false);
    let ef_text = empty_filter_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let ef_arr: Vec<Value> = serde_json::from_str(ef_text).unwrap();
    assert_eq!(ef_arr.len(), 1, "expected 1 connected worker");

    // 2.5 `submit` tool adversarial validation
    // - Shell task missing script and command
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 40,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": { "type": "shell" }
            }
        })
        .to_string(),
    );
    let shell_missing_resp = mcp.read_json_response();
    assert_eq!(shell_missing_resp["id"], 40);
    assert_eq!(shell_missing_resp["result"]["isError"], true);
    let sm_text = shell_missing_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(sm_text.contains("Shell task requires 'script' or 'command'"));

    // - Non-existent staged file
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 41,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "echo",
                    "files": ["/this/file/does/not/exist_at_all.dat"]
                }
            }
        })
        .to_string(),
    );
    let non_exist_file_resp = mcp.read_json_response();
    assert_eq!(non_exist_file_resp["id"], 41);
    assert_eq!(non_exist_file_resp["result"]["isError"], true);
    let nef_text = non_exist_file_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(nef_text.contains("Staged files error"));

    // - Path traversal attempt in staged file sandbox destination
    let temp_input = tempfile::NamedTempFile::new().expect("temp input file");
    std::fs::write(temp_input.path(), b"staged content").unwrap();
    let traversal_spec = format!("{}:../../escape.txt", temp_input.path().display());

    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 42,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "echo",
                    "files": [traversal_spec]
                }
            }
        })
        .to_string(),
    );
    let trav_resp = mcp.read_json_response();
    assert_eq!(trav_resp["id"], 42);
    assert_eq!(trav_resp["result"]["isError"], true);
    let trav_text = trav_resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(trav_text.contains("Staged files error"));

    // - Malformed placement tags
    let bad_tag_cases = [
        "tag with whitespace",
        "os:linux",    // reserved prefix
        "arch:x86_64", // reserved prefix
        "state:busy",  // reserved prefix
        "id:fake",     // reserved prefix
        "link:dummy",  // reserved prefix
        "tag=value",   // equals sign forbidden in user tag
        "tag,value",   // comma forbidden
        "tag_exceeding_sixty_four_bytes_limit_padding_padding_padding_padding_padding",
    ];

    for (idx, bad_tag) in bad_tag_cases.iter().enumerate() {
        let req_id = 50 + idx as i64;
        mcp.send_line(
            &json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {
                    "name": "submit",
                    "arguments": {
                        "type": "command",
                        "command": "echo",
                        "require_tags": [bad_tag]
                    }
                }
            })
            .to_string(),
        );
        let resp = mcp.read_json_response();
        assert_eq!(resp["id"], req_id);
        assert_eq!(
            resp["result"]["isError"], true,
            "bad tag '{bad_tag}' must fail"
        );
        let tag_err_text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(tag_err_text.contains("Invalid placement constraints"));
    }

    // 2.6 `task_result` tool adversarial validation
    // - Missing task_id
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 70,
            "method": "tools/call",
            "params": {
                "name": "task_result",
                "arguments": {}
            }
        })
        .to_string(),
    );
    let missing_tid_resp = mcp.read_json_response();
    assert_eq!(missing_tid_resp["id"], 70);
    assert_eq!(missing_tid_resp["result"]["isError"], true);
    assert!(missing_tid_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("Missing required property 'task_id'"));

    // - Invalid UUID
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 71,
            "method": "tools/call",
            "params": {
                "name": "task_result",
                "arguments": { "task_id": "not-a-valid-uuid" }
            }
        })
        .to_string(),
    );
    let bad_tid_resp = mcp.read_json_response();
    assert_eq!(bad_tid_resp["id"], 71);
    assert_eq!(bad_tid_resp["result"]["isError"], true);
    assert!(bad_tid_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("Invalid task_id UUID"));

    // - Non-existent UUID
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 72,
            "method": "tools/call",
            "params": {
                "name": "task_result",
                "arguments": { "task_id": "11111111-2222-3333-4444-555555555555" }
            }
        })
        .to_string(),
    );
    let nf_tid_resp = mcp.read_json_response();
    assert_eq!(nf_tid_resp["id"], 72);
    assert_eq!(nf_tid_resp["result"]["isError"], true);
    assert!(nf_tid_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("not found"));

    // - Zero timeout with wait: true on a non-existent task
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 73,
            "method": "tools/call",
            "params": {
                "name": "task_result",
                "arguments": {
                    "task_id": "22222222-3333-4444-5555-666666666666",
                    "wait": true,
                    "timeout_secs": 0
                }
            }
        })
        .to_string(),
    );
    let zero_timeout_resp = mcp.read_json_response();
    assert_eq!(zero_timeout_resp["id"], 73);
    assert_eq!(zero_timeout_resp["result"]["isError"], true);

    // 2.7 `doctor` tool adversarial validation
    // - Negative worker expectation (falls back safely to default 1)
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 80,
            "method": "tools/call",
            "params": {
                "name": "doctor",
                "arguments": {
                    "expect_workers": -10,
                    "skip_gpu": true,
                    "skip_spread": true
                }
            }
        })
        .to_string(),
    );
    let doc_neg_resp = mcp.read_json_response();
    assert_eq!(doc_neg_resp["id"], 80);
    assert_eq!(doc_neg_resp["result"]["isError"], false);
    let doc_neg_text = doc_neg_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let doc_neg_val: Value = serde_json::from_str(doc_neg_text).unwrap();
    assert_eq!(doc_neg_val["checks"][0]["id"], "C1");
    assert_eq!(doc_neg_val["checks"][0]["outcome"], "pass");

    // - Extreme worker expectation (C2 fails cleanly without panicking)
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 81,
            "method": "tools/call",
            "params": {
                "name": "doctor",
                "arguments": {
                    "expect_workers": 999999,
                    "skip_gpu": true,
                    "skip_spread": true
                }
            }
        })
        .to_string(),
    );
    let doc_ext_resp = mcp.read_json_response();
    assert_eq!(doc_ext_resp["id"], 81);
    assert_eq!(doc_ext_resp["result"]["isError"], false);
    let doc_ext_text = doc_ext_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let doc_ext_val: Value = serde_json::from_str(doc_ext_text).unwrap();
    let c2 = &doc_ext_val["checks"][1];
    assert_eq!(c2["id"], "C2");
    assert_eq!(
        c2["outcome"], "fail",
        "expect_workers 999999 on 1 worker cluster must fail C2"
    );

    // Final stdout hygiene check on entire session
    mcp.assert_all_stdout_hygiene();
}

// ============================================================================
// 3. Stdout Hygiene Under Intense Error Generation & Backend Disconnection
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_stdout_hygiene_under_intense_stress_and_disconnection() {
    let cluster = TestCluster::spawn(0x88).await;
    let home = tempfile::tempdir().expect("temp home");

    let mut mcp = McpProcess::spawn(home.path(), &cluster.master_addr, Some(&cluster.secret_hex));

    // Handshake
    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "clientInfo": { "name": "stress-agent", "version": "1.0" }
            }
        })
        .to_string(),
    );
    let _ = mcp.read_json_response();

    mcp.send_line(
        &json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        })
        .to_string(),
    );

    // Barrage of 40 malformed & invalid requests
    for i in 0..40 {
        match i % 4 {
            0 => mcp.send_line(&format!("MALFORMED_RAW_FRAME_{i}")),
            1 => mcp.send_line(&format!("{{\"jsonrpc\": \"2.0\", \"id\": {i},")),
            2 => mcp.send_line(
                &json!({
                    "jsonrpc": "1.0",
                    "id": i,
                    "method": "ping"
                })
                .to_string(),
            ),
            _ => mcp.send_line(
                &json!({
                    "jsonrpc": "2.0",
                    "id": i,
                    "method": format!("unknown_method_{i}")
                })
                .to_string(),
            ),
        }
        let _ = mcp.read_json_response();
    }

    // Now, deliberately TERMINATE the Master cluster to induce backend disconnection
    cluster.shutdown_master();
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Send tool requests while the backend is dead
    let dead_backend_tools = ["status", "workers", "submit", "task_result", "doctor"];
    for (idx, tool_name) in dead_backend_tools.iter().enumerate() {
        let req_id = 1000 + idx as i64;
        let args = if *tool_name == "task_result" {
            json!({ "task_id": "00000000-0000-0000-0000-000000000000" })
        } else {
            json!({})
        };

        mcp.send_line(
            &json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": args
                }
            })
            .to_string(),
        );

        let resp = mcp.read_json_response();
        assert_eq!(resp["id"], req_id);
        // Under disconnected backend, tool call must report error gracefully
        let is_err = resp["result"]["isError"].as_bool().unwrap_or(false)
            || resp.get("error").is_some()
            || resp["result"]["content"][0]["text"]
                .as_str()
                .map(|t| t.contains("fail") || t.contains("Error") || t.contains("connect"))
                .unwrap_or(false);
        assert!(
            is_err,
            "tool '{tool_name}' must report failure when master is dead"
        );
    }

    // STRICT STDOUT HYGIENE VERIFICATION
    // Over 45 lines captured from stdout during intense error barrage + dead backend.
    // Every single line MUST parse as a JSON-RPC 2.0 object with ZERO tracing/debug leak.
    mcp.assert_all_stdout_hygiene();
    assert!(
        mcp.lines_received.len() >= 45,
        "expected at least 45 responses captured"
    );
}
