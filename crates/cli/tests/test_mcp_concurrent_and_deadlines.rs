//! End-to-end integration tests for `MCP-1`:
//! - Concurrent MCP server (`reader`, `writer`, and one spawned task per `tools/call`)
//! - `submit {wait: true, require_tags: ["absent"], wait_secs: 2}` answered after `ping`,
//!   returning `task_id`, `timed_out: true`, and an unmatched-placement `warning`
//! - Default `max_retries` inherits cluster default (`None`) instead of `0`
//! - `cancel` tool and `notifications/cancelled`
//! - `notifications/progress` when the client passes a `progressToken`

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use serde_json::{json, Value};
use tokio::sync::watch;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

struct McpHarness {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout_rx: std::sync::mpsc::Receiver<Value>,
}

impl McpHarness {
    fn spawn(home: &Path, master_addr: &str) -> Self {
        let mut child = Command::new(BIN)
            .current_dir(home)
            .env("HOME", home)
            .env("USERPROFILE", home)
            .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("OXIDESWARM_SECRET_FILE")
            .env_remove("RUSTY_GRID_CONFIG")
            .env_remove("RUSTY_GRID_MASTER")
            .env_remove("RUSTY_GRID_MASTER_ADDR")
            .env_remove("RUSTY_GRID_INSECURE")
            .args(["mcp", "--master", master_addr])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn oxideswarm mcp");

        let stdin = child.stdin.take().expect("take stdin");
        let stdout: ChildStdout = child.stdout.take().expect("take stdout");
        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let Ok(l) = line else { break };
                let trimmed = l.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let val: Value =
                    serde_json::from_str(trimmed).expect("stdout line must be valid JSON-RPC");
                if tx.send(val).is_err() {
                    break;
                }
            }
        });

        Self {
            child,
            stdin: Some(stdin),
            stdout_rx: rx,
        }
    }

    fn send(&mut self, msg: &Value) {
        let stdin = self.stdin.as_mut().expect("stdin open");
        let line = serde_json::to_string(msg).expect("serialize json");
        stdin
            .write_all(format!("{line}\n").as_bytes())
            .expect("write line");
        stdin.flush().expect("flush stdin");
    }

    fn recv_timeout(&self, timeout: Duration) -> Value {
        self.stdout_rx
            .recv_timeout(timeout)
            .expect("timed out waiting for JSON-RPC frame from MCP stdout")
    }
}

impl Drop for McpHarness {
    fn drop(&mut self) {
        self.stdin.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn spawn_cluster() -> (
    rusty_grid_master::MasterHandle,
    String,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let temp = tempfile::tempdir().expect("tempdir");
    let sandbox_dir = temp.path().join("sandboxes");
    std::fs::create_dir_all(&sandbox_dir).expect("create sandbox_dir");

    let cfg = ServerConfig::from_addr("127.0.0.1:0").expect("server cfg");
    let master = MasterServer::spawn(cfg).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    let worker_cfg = WorkerConfig::new(&master_addr)
        .with_name("mcp1-worker")
        .with_cores(4)
        .with_ram_mb(4096)
        .with_tags(vec!["present".to_string()])
        .with_sandbox_base_dir(&sandbox_dir);
    let mut worker = WorkerClient::new(worker_cfg);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while master.registry().list_workers().await.is_empty() {
        assert!(Instant::now() < deadline, "worker did not register in time");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    (master, master_addr, shutdown_tx, worker_handle, temp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp1_concurrent_ping_beats_submit_wait_deadline_and_warns_unmatched() {
    let (master, master_addr, shutdown_tx, worker_handle, temp) = spawn_cluster().await;
    let mut mcp = McpHarness::spawn(temp.path(), &master_addr);

    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "mcp1-test", "version": "1.0" }
        }
    }));
    let init = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(init["id"], 1);

    // Send `submit {wait:true, require_tags:["absent"], wait_secs:2}` (id 10)
    // followed immediately by `ping` (id 11).
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 10,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["never-runs"],
                "wait": true,
                "require_tags": ["absent"],
                "wait_secs": 2
            }
        }
    }));
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 11,
        "method": "ping"
    }));

    // 1. `ping` MUST be answered first while `submit` is still waiting on its 2s deadline.
    let first = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(
        first["id"], 11,
        "ping (id 11) must be answered before the 2s submit wait finishes, got: {first}"
    );
    assert_eq!(first["result"], json!({}));

    // 2. `submit` finishes around its 2s deadline and returns a task_id with `timed_out: true`
    // and a warning that no connected worker matches `require:tag=absent`.
    let second = mcp.recv_timeout(Duration::from_secs(6));
    assert_eq!(
        second["id"], 10,
        "second frame must be submit (id 10): {second}"
    );
    assert_eq!(second["result"]["isError"], false);
    let body_text = second["result"]["content"][0]["text"]
        .as_str()
        .expect("submit result text");
    let body: Value = serde_json::from_str(body_text).expect("parse submit body JSON");
    assert_eq!(
        body["timed_out"], true,
        "expected timed_out: true in {body}"
    );
    let task_id_str = body["task_id"].as_str().expect("task_id string");
    let task_uuid = Uuid::parse_str(task_id_str).expect("valid task_id UUID");
    assert!(
        body["warning"]
            .as_str()
            .unwrap_or("")
            .contains("no connected worker matches"),
        "expected unmatched worker warning in submit response, got: {body}"
    );

    // 3. Verify that default `max_retries` on the queued task is `None` (cluster default), not `Some(0)`.
    let queued_task = master
        .queue()
        .get_task_spec(&rusty_grid_core::TaskId(task_uuid))
        .await
        .expect("task must exist in master queue");
    assert_eq!(
        queued_task.requirements.max_retries, None,
        "omitted max_retries in MCP submit must leave TaskRequirements.max_retries as None"
    );

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp1_cancel_tool_cancelled_notification_and_progress_tokens() {
    let (master, master_addr, shutdown_tx, worker_handle, temp) = spawn_cluster().await;
    let mut mcp = McpHarness::spawn(temp.path(), &master_addr);

    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "mcp1-test", "version": "1.0" }
        }
    }));
    let _ = mcp.recv_timeout(Duration::from_secs(5));

    // 1. Submit a fast task with `_meta.progressToken` and verify `notifications/progress` arrives.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 20,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "_meta": { "progressToken": "progress-tok-1" },
            "arguments": {
                "type": "shell",
                "interpreter": "python3",
                "script": "print('progress-ok')",
                "wait": true,
                "wait_secs": 10
            }
        }
    }));

    let mut saw_progress = false;
    let mut submit_done = None;
    for _ in 0..10 {
        let frame = mcp.recv_timeout(Duration::from_secs(5));
        if frame.get("method").and_then(Value::as_str) == Some("notifications/progress") {
            assert_eq!(frame["params"]["progressToken"], "progress-tok-1");
            saw_progress = true;
        } else if frame.get("id") == Some(&json!(20)) {
            submit_done = Some(frame);
            break;
        }
    }
    assert!(
        saw_progress,
        "expected notifications/progress for progress-tok-1"
    );
    let submit_done = submit_done.expect("submit response for id 20");
    assert_eq!(submit_done["result"]["isError"], false);

    // 2. Submit a long-running sleep task asynchronously, then cancel it with the `cancel` tool.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 30,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "shell",
                "interpreter": "python3",
                "script": "import time; time.sleep(30)",
                "wait": false
            }
        }
    }));
    let async_resp = mcp.recv_timeout(Duration::from_secs(5));
    let async_body: Value = serde_json::from_str(
        async_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("async text"),
    )
    .expect("parse async json");
    let sleep_task_id = async_body["task_id"].as_str().expect("task_id").to_string();

    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 31,
        "method": "tools/call",
        "params": {
            "name": "cancel",
            "arguments": {
                "task_id": sleep_task_id
            }
        }
    }));
    let cancel_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(cancel_resp["id"], 31);
    let cancel_body: Value = serde_json::from_str(
        cancel_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("cancel text"),
    )
    .expect("parse cancel json");
    assert_eq!(cancel_body["cancelled"], true);

    // 3. Submit a `wait: true` unfulfillable task (id 40), cancel it via `notifications/cancelled`,
    // and verify the task is cancelled on the master and no response is emitted for id 40.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 40,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["hold"],
                "require_tags": ["absent"],
                "wait": true,
                "wait_secs": 30
            }
        }
    }));

    // Wait until the task appears in the master queue.
    let deadline = Instant::now() + Duration::from_secs(5);
    let held_task_id = loop {
        let tasks = master.queue().list_tasks().await;
        if let Some(info) = tasks.iter().find(|t| format!("{:?}", t.state) == "Queued") {
            break info.task_id;
        }
        assert!(
            Instant::now() < deadline,
            "queued task did not appear on master"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    };

    // Send `notifications/cancelled` for requestId 40, then `ping` (id 41).
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {
            "requestId": 40,
            "reason": "user aborted wait"
        }
    }));
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 41,
        "method": "ping"
    }));

    let ping_after_cancel = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(
        ping_after_cancel["id"], 41,
        "cancelled request 40 must not emit a response before ping 41: {ping_after_cancel}"
    );

    // Verify the master task was cancelled by the `notifications/cancelled` handler.
    let cancel_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let info = master
            .queue()
            .get_task(&held_task_id)
            .await
            .expect("held task in queue");
        if format!("{:?}", info.state).contains("Cancelled") {
            break;
        }
        assert!(
            Instant::now() < cancel_deadline,
            "notifications/cancelled did not cancel the master task, state: {:?}",
            info.state
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    // Wait briefly so any late response for cancelled id 40 would have been flushed,
    // then send ping 42 and verify the next frame is id 42 (never id 40).
    tokio::time::sleep(Duration::from_millis(150)).await;
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 42,
        "method": "ping"
    }));
    let post_cancel_ping = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(
        post_cancel_ping["id"], 42,
        "cancelled call id 40 must never emit a late response: {post_cancel_ping}"
    );

    // 4. Submit an unfulfillable async task and verify `task_result {wait: true, wait_secs: 1}`
    // honors `wait_secs` (returns `timed_out: true` within ~2s) and honors `notifications/cancelled`.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 49,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["queued-for-task-result"],
                "require_tags": ["absent"],
                "wait": false
            }
        }
    }));
    let sub49 = mcp.recv_timeout(Duration::from_secs(5));
    let sub49_body: Value = serde_json::from_str(
        sub49["result"]["content"][0]["text"]
            .as_str()
            .expect("sub49 text"),
    )
    .expect("sub49 json");
    let tr_task_id = sub49_body["task_id"]
        .as_str()
        .expect("tr_task_id")
        .to_string();

    let tr_start = Instant::now();
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 50,
        "method": "tools/call",
        "params": {
            "name": "task_result",
            "arguments": {
                "task_id": tr_task_id,
                "wait": true,
                "wait_secs": 1
            }
        }
    }));
    let tr_resp = mcp.recv_timeout(Duration::from_secs(4));
    assert_eq!(tr_resp["id"], 50);
    assert!(
        tr_start.elapsed() < Duration::from_secs(3),
        "task_result wait_secs: 1 took too long: {:?}",
        tr_start.elapsed()
    );
    let tr_body: Value = serde_json::from_str(
        tr_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("tr_resp text"),
    )
    .expect("tr_resp json");
    assert_eq!(
        tr_body["timed_out"], true,
        "expected timed_out: true from task_result wait_secs: 1, got: {tr_body}"
    );

    // Now start `task_result {wait: true, wait_secs: 30}` (id 51) and cancel it via `notifications/cancelled`.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 51,
        "method": "tools/call",
        "params": {
            "name": "task_result",
            "arguments": {
                "task_id": tr_task_id,
                "wait": true,
                "wait_secs": 30
            }
        }
    }));
    tokio::time::sleep(Duration::from_millis(100)).await;
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {
            "requestId": 51,
            "reason": "abort task_result wait"
        }
    }));
    tokio::time::sleep(Duration::from_millis(200)).await;
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 52,
        "method": "ping"
    }));
    let ping52 = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(
        ping52["id"], 52,
        "cancelled task_result (id 51) must not emit a response: {ping52}"
    );

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp1_no_id_and_duplicate_id_do_not_spin_and_exit_on_eof() {
    let (master, master_addr, shutdown_tx, worker_handle, temp) = spawn_cluster().await;
    let mut mcp = McpHarness::spawn(temp.path(), &master_addr);

    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "mcp1-f31-1", "version": "1.0" }
        }
    }));
    let _ = mcp.recv_timeout(Duration::from_secs(5));

    // Case 1 (F31-1): `tools/call` with no `id` followed by `ping`.
    // Both must be answered and the server must not spin on a dropped watch::Sender.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["no-id-call"],
                "require_tags": ["absent"],
                "wait": true,
                "wait_secs": 1
            }
        }
    }));
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "ping"
    }));

    let first = mcp.recv_timeout(Duration::from_secs(3));
    assert_eq!(first["id"], 2, "ping must answer first: {first}");
    let second = mcp.recv_timeout(Duration::from_secs(4));
    assert!(
        second.get("id").is_none() || second["id"].is_null(),
        "expected id-less submit response after 1s deadline, got: {second}"
    );
    let second_body: Value = serde_json::from_str(
        second["result"]["content"][0]["text"]
            .as_str()
            .expect("no-id submit text"),
    )
    .expect("no-id submit json");
    assert_eq!(second_body["timed_out"], true);

    // Case 2 (F31-1): Duplicate in-flight `id: 99` sent twice.
    // The duplicate must be rejected immediately with INVALID_REQUEST (-32600),
    // and the original call must finish at its 1s deadline.
    let dup_call = json!({
        "jsonrpc": "2.0",
        "id": 99,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["dup-id-call"],
                "require_tags": ["absent"],
                "wait": true,
                "wait_secs": 1
            }
        }
    });
    mcp.send(&dup_call);
    mcp.send(&dup_call);

    let r1 = mcp.recv_timeout(Duration::from_secs(4));
    let r2 = mcp.recv_timeout(Duration::from_secs(4));
    assert_eq!(r1["id"], 99);
    assert_eq!(r2["id"], 99);
    let (err_frame, ok_frame) = if r1.get("error").is_some() {
        (r1, r2)
    } else {
        (r2, r1)
    };
    assert_eq!(
        err_frame["error"]["code"], -32600,
        "expected INVALID_REQUEST (-32600) for duplicate in-flight id: {err_frame}"
    );
    let ok_body: Value = serde_json::from_str(
        ok_frame["result"]["content"][0]["text"]
            .as_str()
            .expect("ok_frame text"),
    )
    .expect("ok_frame json");
    assert_eq!(ok_body["timed_out"], true);

    // Case 3 (F31-4): Start a 12s wait, close stdin (EOF), and assert the process exits within 2s.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 100,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["eof-wait"],
                "require_tags": ["absent"],
                "wait": true,
                "wait_secs": 12
            }
        }
    }));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let eof_start = Instant::now();
    drop(mcp.stdin.take());
    let exit_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(status) = mcp.child.try_wait().expect("try_wait") {
            assert!(status.success(), "expected exit 0 on EOF, got {status:?}");
            break;
        }
        assert!(
            Instant::now() < exit_deadline,
            "mcp did not exit within 2s of EOF while a 12s wait was in flight (elapsed {:?})",
            eof_start.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp1_cancelled_async_submit_leaves_no_orphan_tasks_and_default_wait_margin_surfaces_exit_124(
) {
    let (master, master_addr, shutdown_tx, worker_handle, temp) = spawn_cluster().await;
    let mut mcp = McpHarness::spawn(temp.path(), &master_addr);

    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "mcp1-f31-2-3", "version": "1.0" }
        }
    }));
    let _ = mcp.recv_timeout(Duration::from_secs(5));

    // F31-2: 25 `submit {wait: false}` calls each immediately followed by `notifications/cancelled`
    // must leave 0 pending tasks on the master.
    for req_id in 200..225 {
        mcp.send(&json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "echo",
                    "args": ["orphan-check"],
                    "require_tags": ["absent"],
                    "wait": false
                }
            }
        }));
        mcp.send(&json!({
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": {
                "requestId": req_id,
                "reason": "immediate cancel"
            }
        }));
    }

    let settle_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let tasks = master.queue().list_tasks().await;
        let pending = tasks
            .iter()
            .filter(|t| {
                matches!(
                    format!("{:?}", t.state).as_str(),
                    "Submitted" | "Queued" | "Scheduled" | "Running" | "Retrying"
                )
            })
            .count();
        if pending == 0 {
            break;
        }
        assert!(
            Instant::now() < settle_deadline,
            "expected 0 pending orphan tasks after cancelled wait:false burst, still have {pending}"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    // F31-3: `submit {timeout_secs: 2, wait: true}` WITHOUT `wait_secs` on a 10s sleep script
    // must default `wait_secs` to `timeout_secs + 5` so the worker's watchdog timeout (exit 124) wins.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 300,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "shell",
                "interpreter": "python3",
                "script": "import time; time.sleep(10)",
                "timeout_secs": 2,
                "wait": true
            }
        }
    }));
    let timeout_resp = mcp.recv_timeout(Duration::from_secs(10));
    assert_eq!(timeout_resp["id"], 300);
    let timeout_body: Value = serde_json::from_str(
        timeout_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("timeout_resp text"),
    )
    .expect("timeout_resp json");
    assert_eq!(
        timeout_body["status"], "failed",
        "expected worker timeout failure, got: {timeout_body}"
    );
    assert_eq!(
        timeout_body["exit_code"], 124,
        "expected exit_code 124 from worker timeout when wait_secs is omitted, got: {timeout_body}"
    );

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp1_silent_master_bounded_by_wait_secs_and_exits_on_eof() {
    let temp = tempfile::tempdir().expect("tempdir");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind silent listener");
    let silent_addr = listener.local_addr().expect("local_addr").to_string();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept() {
            held.push(stream);
        }
    });

    let mut mcp = McpHarness::spawn(temp.path(), &silent_addr);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "mcp1-f31-5", "version": "1.0" }
        }
    }));
    let _ = mcp.recv_timeout(Duration::from_secs(5));

    let start = Instant::now();
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 10,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["silent"],
                "wait": true,
                "wait_secs": 1
            }
        }
    }));
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 11,
        "method": "tools/call",
        "params": {
            "name": "cancel",
            "arguments": {
                "task_id": "00000000-0000-0000-0000-000000000001"
            }
        }
    }));
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 12,
        "method": "ping"
    }));

    let mut saw_ping = false;
    let mut saw_submit = false;
    let mut saw_cancel = false;
    for _ in 0..3 {
        let frame = mcp.recv_timeout(Duration::from_secs(4));
        match frame["id"].as_i64() {
            Some(12) => saw_ping = true,
            Some(10) => {
                saw_submit = true;
                let text = frame["result"]["content"][0]["text"].as_str().unwrap_or("");
                let is_err = frame["result"]["isError"].as_bool().unwrap_or(false);
                let timed_out = serde_json::from_str::<Value>(text)
                    .ok()
                    .and_then(|v| v["timed_out"].as_bool())
                    .unwrap_or(false);
                assert!(
                    is_err || timed_out,
                    "expected error or timed_out against silent master, got: {frame}"
                );
            }
            Some(11) => {
                saw_cancel = true;
                assert_eq!(frame["result"]["isError"], true);
            }
            other => panic!("unexpected frame id {other:?}: {frame}"),
        }
    }
    assert!(saw_ping && saw_submit && saw_cancel);
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "silent master calls took too long: {:?}",
        start.elapsed()
    );

    drop(mcp.stdin.take());
    let exit_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(status) = mcp.child.try_wait().expect("try_wait") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < exit_deadline,
            "mcp did not exit within 2s after EOF against silent master"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
