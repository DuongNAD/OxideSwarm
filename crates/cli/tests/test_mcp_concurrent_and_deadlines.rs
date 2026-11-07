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

    let cfg = ServerConfig::from_addr("127.0.0.1:0")
        .expect("server cfg")
        .with_heartbeat_interval(60);
    let master = MasterServer::spawn(cfg).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    let worker_cfg = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("mcp1-worker")
        .with_cores(4)
        .with_ram_mb(4096)
        .with_no_blender(true)
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

    // Reusing id 51 on a new `tools/call` must succeed immediately (not -32600 duplicate id),
    // proving `tool_task_result` actually exited its wait loop upon `notifications/cancelled`.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 51,
        "method": "tools/call",
        "params": {
            "name": "status",
            "arguments": {}
        }
    }));
    let reused51 = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(reused51["id"], 51);
    assert!(
        reused51.get("error").is_none(),
        "reusing id 51 after cancelling task_result must succeed (proving task_result exited on cancel), got: {reused51}"
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

    // Case 1 (F31-1): `tools/call` with `"id": null` (`request_id_key == None`) followed by `ping`.
    // Both must be answered and the server must not spin on a dropped watch::Sender.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": null,
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

    // Case 3 (F31-4): Start a 12s wait, close stdin (EOF), and assert the process exits within
    // the 4s grace + 500ms cancel window AND cancels the waited task on the master.
    let tasks_before_eof: std::collections::HashSet<rusty_grid_core::TaskId> = master
        .queue()
        .list_tasks()
        .await
        .into_iter()
        .map(|t| t.task_id)
        .collect();
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
    let eof_task_id = {
        let q_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(t) = master
                .queue()
                .list_tasks()
                .await
                .into_iter()
                .find(|t| !tasks_before_eof.contains(&t.task_id))
            {
                break t.task_id;
            }
            assert!(
                Instant::now() < q_deadline,
                "eof-wait task did not appear in master queue"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    let eof_start = Instant::now();
    drop(mcp.stdin.take());
    let exit_deadline = Instant::now() + Duration::from_millis(6000);
    loop {
        if let Some(status) = mcp.child.try_wait().expect("try_wait") {
            assert!(status.success(), "expected exit 0 on EOF, got {status:?}");
            break;
        }
        assert!(
            Instant::now() < exit_deadline,
            "mcp did not exit within 6s of EOF while a 12s wait was in flight (elapsed {:?})",
            eof_start.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let eof_task_info = master
        .queue()
        .get_task(&eof_task_id)
        .await
        .expect("eof_task_id in master queue");
    assert!(
        format!("{:?}", eof_task_info.state).contains("Cancelled"),
        "EOF shutdown must send CancelTask for in-flight submit {{wait: true}}, got state: {:?}",
        eof_task_info.state
    );

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
    let timeout_deadline = Instant::now() + Duration::from_secs(15);
    let mut skipped_reply_task_ids: Vec<rusty_grid_core::task::TaskId> = Vec::new();
    let timeout_resp = loop {
        let remaining = timeout_deadline
            .checked_duration_since(Instant::now())
            .expect("timed out waiting for id 300 response");
        let resp = mcp.recv_timeout(remaining);
        if resp["id"] == 300 {
            break resp;
        }
        let id = resp["id"].as_i64().unwrap_or(-1);
        assert!(
            (200..225).contains(&id),
            "unexpected JSON-RPC response id while waiting for 300: {resp}"
        );
        let body: Value = serde_json::from_str(
            resp["result"]["content"][0]["text"]
                .as_str()
                .expect("skipped submit reply text"),
        )
        .expect("skipped submit reply json");
        let tid_str = body["task_id"]
            .as_str()
            .expect("skipped submit reply must contain task_id");
        let tid = Uuid::parse_str(tid_str)
            .map(rusty_grid_core::task::TaskId)
            .expect("valid task_id uuid");
        skipped_reply_task_ids.push(tid);
    };
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

    // After id 300 has replied, all 25 requests (200..225) have been read and processed by the
    // MCP server. Poll with a deadline until no task is pending and assert every skipped reply's
    // task ended Cancelled.
    let post_300_deadline = Instant::now() + Duration::from_secs(5);
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
        let all_skipped_cancelled = skipped_reply_task_ids.iter().all(|tid| {
            tasks
                .iter()
                .any(|t| t.task_id == *tid && format!("{:?}", t.state).contains("Cancelled"))
        });
        if pending == 0 && all_skipped_cancelled {
            break;
        }
        assert!(
            Instant::now() < post_300_deadline,
            "expected 0 pending tasks and all {} skipped replies Cancelled after id 300, still have pending={pending}, tasks={:?}",
            skipped_reply_task_ids.len(),
            tasks.iter().map(|t| (t.task_id, &t.state)).collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    // Explicit probe for the post-reply cancellation path (design 0030): submit wait:false, read
    // its reply so it leaves `in_flight` and enters `completed_submits`, then send
    // `notifications/cancelled` and verify the task transitions to Cancelled.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 299,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["late-cancel-probe"],
                "require_tags": ["absent"],
                "wait": false
            }
        }
    }));
    let probe_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(probe_resp["id"], 299);
    let probe_body: Value = serde_json::from_str(
        probe_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("probe_resp text"),
    )
    .expect("probe_resp json");
    let probe_tid = Uuid::parse_str(probe_body["task_id"].as_str().expect("probe task_id"))
        .map(rusty_grid_core::task::TaskId)
        .expect("valid probe task_id");
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {
            "requestId": 299,
            "reason": "cancel after wait:false reply"
        }
    }));
    let probe_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let info = master
            .queue()
            .get_task(&probe_tid)
            .await
            .expect("probe task must exist");
        if format!("{:?}", info.state).contains("Cancelled") {
            break;
        }
        assert!(
            Instant::now() < probe_deadline,
            "post-reply notifications/cancelled must cancel task {probe_tid}, got state {:?}",
            info.state
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    // Request-ID reuse guard (design 0030): reusing id 298 for another request (`ping`) must
    // evict id 298 from `completed_submits` so a later `notifications/cancelled` for 298 does
    // not cancel the older task.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 298,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["reuse-id-guard"],
                "require_tags": ["absent"],
                "wait": false
            }
        }
    }));
    let reuse_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(reuse_resp["id"], 298);
    let reuse_body: Value = serde_json::from_str(
        reuse_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("reuse_resp text"),
    )
    .expect("reuse_resp json");
    let reuse_tid = Uuid::parse_str(reuse_body["task_id"].as_str().expect("reuse task_id"))
        .map(rusty_grid_core::task::TaskId)
        .expect("valid reuse task_id");
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 298,
        "method": "ping"
    }));
    let ping_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(ping_resp["id"], 298);

    // Submit a positive-control task (`id: 297`) whose post-reply cancellation proves the MCP
    // server has processed the preceding `notifications/cancelled` for `id: 298` without a fixed sleep.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 297,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["reuse-positive-control"],
                "require_tags": ["absent"],
                "wait": false
            }
        }
    }));
    let control_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(control_resp["id"], 297);
    let control_body: Value = serde_json::from_str(
        control_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("control_resp text"),
    )
    .expect("control_resp json");
    let control_tid = Uuid::parse_str(control_body["task_id"].as_str().expect("control task_id"))
        .map(rusty_grid_core::task::TaskId)
        .expect("valid control task_id");

    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {
            "requestId": 298,
            "reason": "cancel after id reuse"
        }
    }));
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {
            "requestId": 297,
            "reason": "positive control after 298"
        }
    }));
    let control_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let info = master
            .queue()
            .get_task(&control_tid)
            .await
            .expect("control task must exist");
        if format!("{:?}", info.state) == "Cancelled" {
            break;
        }
        assert!(
            Instant::now() < control_deadline,
            "positive-control task {control_tid} (id 297) must become Cancelled, got {:?}",
            info.state
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let reuse_info = master
        .queue()
        .get_task(&reuse_tid)
        .await
        .expect("reuse task must exist");
    assert_eq!(
        format!("{:?}", reuse_info.state),
        "Queued",
        "notifications/cancelled after request-id reuse must not cancel the older task"
    );
    let _ = master.queue().cancel_task(&reuse_tid, None).await;

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
                let parsed = serde_json::from_str::<Value>(text).ok();
                let timed_out = parsed
                    .as_ref()
                    .and_then(|v| v["timed_out"].as_bool())
                    .unwrap_or(false);
                assert!(
                    is_err || timed_out,
                    "expected error or timed_out against silent master, got: {frame}"
                );
                if timed_out {
                    let v = parsed.as_ref().unwrap();
                    assert_eq!(v["status"], "Unknown");
                    assert_eq!(v["state"], "unknown");
                }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp1_rendezvous_auto_discovery_piped_eof_replies_and_zero_wait_secs() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp = tempfile::tempdir().expect("tempdir");
    let home = temp.path();
    let sandbox_dir = home.join("sandboxes");
    std::fs::create_dir_all(&sandbox_dir).expect("create sandbox_dir");

    // Configure the master and worker with a ClusterSecret so `wait_secs: 0` (N31-3)
    // must run the multi-step authenticated handshake (which fails if `.max(1)` in
    // `tool_submit` is removed).
    let secret = rusty_grid_core::cluster_auth::ClusterSecret::generate().expect("generate secret");
    let secret_hex = secret.to_hex();
    let cfg = ServerConfig::from_addr("127.0.0.1:0")
        .expect("server cfg")
        .with_cluster_secret(secret.clone());
    let master = MasterServer::spawn(cfg).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    let worker_cfg = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("mcp1-auth-worker")
        .with_cores(4)
        .with_ram_mb(4096)
        .with_no_blender(true)
        .with_tags(vec!["present".to_string()])
        .with_cluster_secret(secret.clone())
        .with_sandbox_base_dir(&sandbox_dir);
    let mut worker = WorkerClient::new(worker_cfg);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });

    let reg_deadline = Instant::now() + Duration::from_secs(5);
    while master.registry().list_workers().await.is_empty() {
        assert!(
            Instant::now() < reg_deadline,
            "worker did not register in time"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    // 1. N1 + N31-2 + N31-3: Bind a silent UDP port (so UDP discovery times out after 1.5s) and a
    // local HTTP rendezvous server serving a signed RendezvousLease for `master_addr`.
    let silent_udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind silent udp");
    let disc_port = silent_udp.local_addr().expect("udp local_addr").port();

    // Forwarder in front of `master_addr` with a 5ms handshake latency (> Tokio's 1ms timer
    // wheel tick) so `connect_master`'s authenticated handshake cannot complete inside a single
    // sub-millisecond poll if `.max(1)` in `tool_submit` is removed.
    let auth_proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind auth_proxy_listener");
    let auth_proxy_addr = auth_proxy_listener
        .local_addr()
        .expect("auth_proxy_addr")
        .to_string();
    let master_addr_for_proxy = master_addr.clone();
    let auth_proxy_handle = tokio::spawn(async move {
        while let Ok((mut client_stream, _)) = auth_proxy_listener.accept().await {
            let upstream = master_addr_for_proxy.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let Ok(mut upstream_stream) = tokio::net::TcpStream::connect(&upstream).await
                else {
                    return;
                };
                let _ =
                    tokio::io::copy_bidirectional(&mut client_stream, &mut upstream_stream).await;
            });
        }
    });

    let rv_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind rv listener");
    let rv_addr = rv_listener.local_addr().expect("rv local_addr");
    let mut lease = rusty_grid_core::discovery::RendezvousLease::new(
        &auth_proxy_addr,
        "http://127.0.0.1:8080",
        "mcp1-rv-host",
        None,
        1,
        60,
    );
    lease.sign(&secret);
    let lease_json = serde_json::to_string(&lease).expect("serialize lease");
    let rv_hits = Arc::new(AtomicUsize::new(0));
    let rv_hits_srv = Arc::clone(&rv_hits);
    std::thread::spawn(move || {
        while let Ok((mut stream, _)) = rv_listener.accept() {
            rv_hits_srv.fetch_add(1, Ordering::SeqCst);
            let mut req_buf = [0u8; 512];
            let _ = std::io::Read::read(&mut stream, &mut req_buf);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                lease_json.len(),
                lease_json
            );
            let _ = std::io::Write::write_all(&mut stream, resp.as_bytes());
        }
    });

    let mut auto_child = Command::new(BIN)
        .current_dir(home)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .env("OXIDESWARM_RENDEZVOUS", format!("http://{rv_addr}"))
        .env("OXIDESWARM_DISCOVERY_CANDIDATES", "127.0.0.1:9")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .args([
            "mcp",
            "--master",
            &format!("auto:{disc_port}"),
            "--secret",
            &secret_hex,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn oxideswarm mcp --master auto");

    let mut auto_stdin = auto_child.stdin.take().expect("auto stdin");
    let auto_stdout = auto_child.stdout.take().expect("auto stdout");
    let (auto_tx, auto_rx) = std::sync::mpsc::channel::<Value>();
    std::thread::spawn(move || {
        for line in BufReader::new(auto_stdout).lines() {
            let Ok(l) = line else { break };
            if let Ok(v) = serde_json::from_str::<Value>(l.trim()) {
                let _ = auto_tx.send(v);
            }
        }
    });

    // Step 1a: Send `initialize` (id 1) and `status` (id 2), and wait for both replies so
    // `resolved_master_addr` is populated in `McpState`.
    for msg in [
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "mcp1-rv", "version": "1.0" }
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": { "name": "status", "arguments": {} }
        }),
    ] {
        let s = serde_json::to_string(&msg).unwrap();
        auto_stdin.write_all(format!("{s}\n").as_bytes()).unwrap();
    }
    auto_stdin.flush().unwrap();

    let mut saw_status = false;
    for _ in 0..2 {
        let frame = auto_rx
            .recv_timeout(Duration::from_secs(8))
            .expect("recv init/status from auto mcp");
        match frame["id"].as_i64() {
            Some(1) => {}
            Some(2) => {
                assert_eq!(
                    frame["result"]["isError"], false,
                    "status over --master auto rendezvous failed: {frame}"
                );
                let body: Value =
                    serde_json::from_str(frame["result"]["content"][0]["text"].as_str().unwrap())
                        .unwrap();
                assert_eq!(body["cluster_status"], "ok");
                saw_status = true;
            }
            other => panic!("unexpected frame id {other:?}: {frame}"),
        }
    }
    assert!(saw_status);
    assert_eq!(
        rv_hits.load(Ordering::SeqCst),
        1,
        "initial status call must query rendezvous exactly once"
    );

    // Step 1b (N31-2 + N31-3): Now send `submit {wait: true, wait_secs: 0}` (id 3) against the
    // authenticated master. Because `resolved_master_addr` is cached in
    // `McpState::resolve_target_addr`, it must NOT query the rendezvous server again and must
    // answer well under the 1.5s UDP timeout; and because `.max(1)` in `tool_submit` gives the
    // authenticated handshake a 1s floor, it must succeed despite `wait_secs: 0`.
    let cached_start = Instant::now();
    let wait_zero_msg = json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["wait-zero-over-auto"],
                "require_tags": ["absent"],
                "wait": true,
                "wait_secs": 0
            }
        }
    });
    auto_stdin
        .write_all(format!("{}\n", serde_json::to_string(&wait_zero_msg).unwrap()).as_bytes())
        .unwrap();
    auto_stdin.flush().unwrap();

    let frame = auto_rx
        .recv_timeout(Duration::from_secs(4))
        .expect("recv wait_secs:0 frame from auto mcp");
    assert_eq!(frame["id"], 3);
    assert_eq!(
        frame["result"]["isError"], false,
        "submit {{wait:true, wait_secs:0}} against authenticated master failed: {frame}"
    );
    assert!(
        cached_start.elapsed() < Duration::from_millis(800),
        "cached --master auto address must avoid repeating 1.5s discovery, took {:?}",
        cached_start.elapsed()
    );
    assert_eq!(
        rv_hits.load(Ordering::SeqCst),
        1,
        "subsequent RPCs must use cached resolved_master_addr without hitting rendezvous again"
    );
    let body: Value =
        serde_json::from_str(frame["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(body["timed_out"], true);
    assert!(
        body["warning"]
            .as_str()
            .unwrap_or("")
            .contains("no connected worker matches"),
        "expected placement warning under --master auto, got: {body}"
    );
    let tid = Uuid::parse_str(body["task_id"].as_str().unwrap()).unwrap();
    assert!(
        master
            .queue()
            .get_task(&rusty_grid_core::TaskId(tid))
            .await
            .is_some(),
        "submit {{wait:true, wait_secs:0}} must submit the task to the master"
    );
    drop(auto_stdin);
    let _ = auto_child.kill();
    let _ = auto_child.wait();
    auth_proxy_handle.abort();

    // 2. N2: Pipe `doctor` and a 1s `submit {wait: true}` followed immediately by EOF,
    // and assert BOTH replies are written to stdout before the server exits.
    let mut piped_child = Command::new(BIN)
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
        .args(["mcp", "--master", &master_addr, "--secret", &secret_hex])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn piped mcp");

    {
        let mut p_stdin = piped_child.stdin.take().expect("piped stdin");
        for msg in [
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": { "name": "mcp1-piped", "version": "1.0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "doctor",
                    "arguments": {
                        "expect_workers": 1,
                        "skip_gpu": true,
                        "skip_spread": true
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "submit",
                    "arguments": {
                        "type": "shell",
                        "interpreter": "python3",
                        "script": "import time; time.sleep(1); print('piped-1s-ok')",
                        "wait": true
                    }
                }
            }),
        ] {
            let s = serde_json::to_string(&msg).unwrap();
            p_stdin.write_all(format!("{s}\n").as_bytes()).unwrap();
        }
        p_stdin.flush().unwrap();
        // Drop `p_stdin` immediately to send EOF while `doctor` and 1s `submit` are in flight.
    }

    let piped_out = piped_child.wait_with_output().expect("wait piped mcp");
    assert!(piped_out.status.success());
    let stdout_str = String::from_utf8_lossy(&piped_out.stdout);
    let frames: Vec<Value> = stdout_str
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<Value>(l).expect("valid json line"))
        .collect();
    let doc_frame = frames
        .iter()
        .find(|f| f["id"] == 2)
        .expect("doctor reply (id 2) must be written before EOF exit");
    assert_eq!(doc_frame["result"]["isError"], false);
    let sub_frame = frames
        .iter()
        .find(|f| f["id"] == 3)
        .expect("1s submit reply (id 3) must be written before EOF exit");
    assert_eq!(sub_frame["result"]["isError"], false);
    assert!(
        sub_frame["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("")
            .contains("piped-1s-ok"),
        "expected piped-1s-ok in submit reply: {sub_frame}"
    );

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp1_slow_6mb_response_succeeds_and_post_submit_cancel_check_fires() {
    use rusty_grid_core::protocol::{
        serialize_message, ClientMessage, ClientResponse, MessageTransport, WireCodec,
    };
    use rusty_grid_core::task::{TaskId, TaskResult};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;

    let temp = tempfile::tempdir().expect("tempdir");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock master");
    let mock_addr = listener.local_addr().expect("mock_addr").to_string();

    let submitted_sent = Arc::new(AtomicBool::new(false));
    let cancel_after_submitted = Arc::new(AtomicBool::new(false));
    let submitted_sent_srv = Arc::clone(&submitted_sent);
    let cancel_after_srv = Arc::clone(&cancel_after_submitted);

    let srv_handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let sub_sent = Arc::clone(&submitted_sent_srv);
            let cancel_after = Arc::clone(&cancel_after_srv);
            tokio::spawn(async move {
                let mut transport = MessageTransport::new(stream);
                while let Ok(Some(msg)) = transport.recv_msg::<ClientMessage>().await {
                    match msg {
                        ClientMessage::ListWorkers => {
                            let _ = transport
                                .send_msg(&ClientResponse::WorkerList {
                                    workers: Vec::new(),
                                })
                                .await;
                        }
                        ClientMessage::GetTaskResult { task_id } => {
                            // N3: Build a 6 MB TaskCompleted response and stream it at ~1.5 MB/s
                            // (~4 seconds total, > 2s DEFAULT_MASTER_RPC_TIMEOUT, with continuous
                            // progress every 40ms).
                            let big_stdout = "A".repeat(6 * 1024 * 1024);
                            let resp = ClientResponse::TaskCompleted {
                                task_id,
                                result: TaskResult::success(
                                    Uuid::new_v4(),
                                    task_id,
                                    big_stdout.into_bytes(),
                                    10,
                                    false,
                                ),
                            };
                            let payload =
                                serialize_message(&resp, WireCodec::Bincode).expect("serialize");
                            let mut framed = Vec::with_capacity(4 + payload.len());
                            framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                            framed.extend_from_slice(&payload);
                            let raw_stream = transport.get_mut();
                            for chunk in framed.chunks(64 * 1024) {
                                if raw_stream.write_all(chunk).await.is_err() {
                                    break;
                                }
                                let _ = raw_stream.flush().await;
                                tokio::time::sleep(Duration::from_millis(35)).await;
                            }
                        }
                        ClientMessage::SubmitTask { task, wait: false } => {
                            // Delay TaskSubmitted by 200ms so notifications/cancelled arrives at 50ms
                            // while SubmitTask is in flight, exercising the post-TaskSubmitted cancel
                            // check at mcp.rs:1174.
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            sub_sent.store(true, Ordering::SeqCst);
                            let _ = transport
                                .send_msg(&ClientResponse::TaskSubmitted { task_id: task.id })
                                .await;
                        }
                        ClientMessage::CancelTask { task_id } => {
                            if sub_sent.load(Ordering::SeqCst) {
                                cancel_after.store(true, Ordering::SeqCst);
                            }
                            let _ = transport
                                .send_msg(&ClientResponse::TaskCancelled {
                                    task_id,
                                    success: true,
                                })
                                .await;
                        }
                        _ => {}
                    }
                }
            });
        }
    });

    let mut mcp = McpHarness::spawn(temp.path(), &mock_addr);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "mcp1-n3-mutant", "version": "1.0" }
        }
    }));
    let _ = mcp.recv_timeout(Duration::from_secs(5));

    // 1. N3: 6 MB `task_result` without wait over a ~1.8 MB/s link (~3.5s > 2s) must succeed.
    let tid = TaskId::new();
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 10,
        "method": "tools/call",
        "params": {
            "name": "task_result",
            "arguments": {
                "task_id": tid.to_string(),
                "wait": false
            }
        }
    }));
    let big_resp = mcp.recv_timeout(Duration::from_secs(12));
    assert_eq!(big_resp["id"], 10);
    assert_eq!(
        big_resp["result"]["isError"], false,
        "6 MB task_result over slow link must succeed: {big_resp}"
    );
    let big_body: Value = serde_json::from_str(
        big_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("big text"),
    )
    .expect("parse big json");
    assert_eq!(
        big_body["stdout_captured_bytes"].as_u64(),
        Some(6 * 1024 * 1024),
        "expected full 6 MB stdout_captured_bytes: {big_body}"
    );
    assert_eq!(big_body["stdout_truncated"].as_bool(), Some(true));
    assert_eq!(big_body["stdout_worker_truncated"].as_bool(), Some(false));
    let big_stdout = big_body["stdout"].as_str().expect("stdout string");
    assert!(
        big_stdout.len() <= 16_384,
        "default max_output_bytes (16384) must bound stdout: len={}",
        big_stdout.len()
    );
    assert!(
        big_stdout.starts_with("AAAA")
            && big_stdout.ends_with("AAAA")
            && big_stdout.contains("\n...[truncated "),
        "expected head + tail with truncation notice in stdout"
    );

    // 2. Mutant B (mcp.rs:1174): Cancel an in-flight `submit {wait: false}` at 50ms while
    // `SubmitTask` takes 200ms, and assert that `CancelTask` is sent AFTER `TaskSubmitted`.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 20,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["delayed-submit"],
                "wait": false
            }
        }
    }));
    tokio::time::sleep(Duration::from_millis(50)).await;
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {
            "requestId": 20,
            "reason": "cancel while SubmitTask in flight"
        }
    }));

    let deadline = Instant::now() + Duration::from_secs(3);
    while !cancel_after_submitted.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "expected CancelTask to be sent after TaskSubmitted (mcp.rs:1174)"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    srv_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp1_pooled_slow_submit_reply_does_not_resend_and_returns_task_id() {
    use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let (master, master_addr, shutdown_tx, worker_handle, temp) = spawn_cluster().await;

    // Proxy in front of the real MasterServer: on connection #0 (which is pooled after the
    // pre-flight `ListWorkers`), delay the `TaskSubmitted` reply by 1.5s so the 1s `wait_secs: 1`
    // receive bound fires after `SubmitTask` was already written to the Master.
    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy");
    let proxy_addr = proxy_listener.local_addr().expect("proxy_addr").to_string();
    let conn_counter = Arc::new(AtomicUsize::new(0));
    let submit_forwards = Arc::new(AtomicUsize::new(0));
    let conn_counter_srv = Arc::clone(&conn_counter);
    let submit_forwards_srv = Arc::clone(&submit_forwards);
    let master_addr_srv = master_addr.clone();

    let proxy_handle = tokio::spawn(async move {
        while let Ok((client_stream, _)) = proxy_listener.accept().await {
            let conn_idx = conn_counter_srv.fetch_add(1, Ordering::SeqCst);
            let submits = Arc::clone(&submit_forwards_srv);
            let upstream_addr = master_addr_srv.clone();
            tokio::spawn(async move {
                let Ok(upstream_stream) = tokio::net::TcpStream::connect(&upstream_addr).await
                else {
                    return;
                };
                let mut client_tx = MessageTransport::new(client_stream);
                let mut upstream_tx = MessageTransport::new(upstream_stream);
                while let Ok(Some(msg)) = client_tx.recv_msg::<ClientMessage>().await {
                    let is_submit = matches!(msg, ClientMessage::SubmitTask { .. });
                    if is_submit {
                        submits.fetch_add(1, Ordering::SeqCst);
                    }
                    if upstream_tx.send_msg(&msg).await.is_err() {
                        break;
                    }
                    let Ok(Some(resp)) = upstream_tx.recv_msg::<ClientResponse>().await else {
                        break;
                    };
                    if conn_idx == 0 && is_submit {
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                    }
                    if client_tx.send_msg(&resp).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let mut mcp = McpHarness::spawn(temp.path(), &proxy_addr);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "mcp1-n31-1", "version": "1.0" }
        }
    }));
    let _ = mcp.recv_timeout(Duration::from_secs(5));

    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["n31-1-once"],
                "require_tags": ["absent"],
                "wait": false,
                "wait_secs": 1
            }
        }
    }));

    let resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(resp["id"], 2);
    assert_eq!(
        resp["result"]["isError"], false,
        "slow pooled SubmitTask reply must not resend and fail with already exists: {resp}"
    );
    let body: Value = serde_json::from_str(
        resp["result"]["content"][0]["text"]
            .as_str()
            .expect("submit text"),
    )
    .expect("submit json");
    assert_eq!(body["timed_out"], true, "expected timed_out: true: {body}");
    assert_eq!(body["state"], "Queued", "expected Queued state: {body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or("")
            .contains("query its outcome"),
        "expected advice to query task outcome: {body}"
    );
    let tid = Uuid::parse_str(body["task_id"].as_str().expect("task_id")).expect("valid uuid");
    assert_eq!(
        submit_forwards.load(Ordering::SeqCst),
        1,
        "SubmitTask must be sent to the Master only once"
    );
    assert!(
        master
            .queue()
            .get_task(&rusty_grid_core::TaskId(tid))
            .await
            .is_some(),
        "Master must hold the submitted task"
    );

    proxy_handle.abort();
    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp1_post_write_eof_corrupt_not_found_cancel_status_recovery_and_zero_timeout_result()
{
    use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
    use rusty_grid_core::task::{TaskId, TaskSpec};
    use rusty_grid_master::queue::TaskState;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;

    let (master, master_addr, shutdown_tx, worker_handle, temp) = spawn_cluster().await;

    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy");
    let proxy_addr = proxy_listener.local_addr().expect("proxy_addr").to_string();
    let eof_submits = Arc::new(AtomicUsize::new(0));
    let corrupt_submits = Arc::new(AtomicUsize::new(0));
    let f3_submits = Arc::new(AtomicUsize::new(0));
    let f3_lost_submits = Arc::new(AtomicUsize::new(0));
    let status_stalls = Arc::new(AtomicUsize::new(0));
    let stall_next_status = Arc::new(AtomicBool::new(false));
    let drop_next_cancel_reply = Arc::new(AtomicBool::new(false));
    let cancel_reply_drops = Arc::new(AtomicUsize::new(0));
    let f3_held_tid: Arc<tokio::sync::Mutex<Option<TaskId>>> =
        Arc::new(tokio::sync::Mutex::new(None));
    let (f3_not_found_tx, f3_not_found_rx) = watch::channel(false);
    let (f3_orig_landed_tx, f3_orig_landed_rx) = watch::channel(false);
    let cancelled_tid: Arc<tokio::sync::Mutex<Option<TaskId>>> =
        Arc::new(tokio::sync::Mutex::new(None));

    let eof_submits_srv = Arc::clone(&eof_submits);
    let corrupt_submits_srv = Arc::clone(&corrupt_submits);
    let f3_submits_srv = Arc::clone(&f3_submits);
    let f3_lost_submits_srv = Arc::clone(&f3_lost_submits);
    let status_stalls_srv = Arc::clone(&status_stalls);
    let stall_status_srv = Arc::clone(&stall_next_status);
    let drop_cancel_srv = Arc::clone(&drop_next_cancel_reply);
    let cancel_drops_srv = Arc::clone(&cancel_reply_drops);
    let f3_held_tid_srv = Arc::clone(&f3_held_tid);
    let f3_not_found_tx_srv = Arc::new(f3_not_found_tx);
    let f3_orig_landed_tx_srv = Arc::new(f3_orig_landed_tx);
    let cancelled_tid_srv = Arc::clone(&cancelled_tid);
    let master_addr_srv = master_addr.clone();

    let proxy_handle = tokio::spawn(async move {
        while let Ok((client_stream, _)) = proxy_listener.accept().await {
            let eof_s = Arc::clone(&eof_submits_srv);
            let corrupt_s = Arc::clone(&corrupt_submits_srv);
            let f3_s = Arc::clone(&f3_submits_srv);
            let f3_lost_s = Arc::clone(&f3_lost_submits_srv);
            let st_stalls = Arc::clone(&status_stalls_srv);
            let stall_st = Arc::clone(&stall_status_srv);
            let drop_cancel = Arc::clone(&drop_cancel_srv);
            let cancel_drops = Arc::clone(&cancel_drops_srv);
            let f3_tid_slot = Arc::clone(&f3_held_tid_srv);
            let nf_tx = Arc::clone(&f3_not_found_tx_srv);
            let mut nf_rx = f3_not_found_rx.clone();
            let orig_tx = Arc::clone(&f3_orig_landed_tx_srv);
            let mut orig_rx = f3_orig_landed_rx.clone();
            let cancel_tid = Arc::clone(&cancelled_tid_srv);
            let upstream_addr = master_addr_srv.clone();
            tokio::spawn(async move {
                let Ok(upstream_stream) = tokio::net::TcpStream::connect(&upstream_addr).await
                else {
                    return;
                };
                let mut client_tx = MessageTransport::new(client_stream);
                let mut upstream_tx = MessageTransport::new(upstream_stream);
                while let Ok(Some(msg)) = client_tx.recv_msg::<ClientMessage>().await {
                    if matches!(
                        msg,
                        ClientMessage::ListWorkers | ClientMessage::ClusterStatus
                    ) && stall_st.swap(false, Ordering::SeqCst)
                    {
                        // F2: Stall pooled connection past the 2s idle timeout on the
                        // `ListWorkers` frame that `status` sends so `status` must recover
                        // on a fresh connection.
                        st_stalls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(2400)).await;
                        break;
                    }
                    if matches!(msg, ClientMessage::GetTaskResult { .. }) {
                        // Add a few ms of proxy latency on `GetTaskResult` so `timeout_secs: 0`
                        // deterministically exceeds Tokio's 1ms timer tick if the wait loop runs.
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    if let ClientMessage::GetTaskStatus { task_id } = msg {
                        if *f3_tid_slot.lock().await == Some(task_id) {
                            if upstream_tx.send_msg(&msg).await.is_err() {
                                break;
                            }
                            let Ok(Some(resp)) = upstream_tx.recv_msg::<ClientResponse>().await
                            else {
                                break;
                            };
                            if client_tx.send_msg(&resp).await.is_err() {
                                break;
                            }
                            // Signal the held original `SubmitTask` to land on the Master right
                            // after `GetTaskStatus` answered `not found` and before the resend.
                            let _ = nf_tx.send(true);
                            continue;
                        }
                    }
                    if matches!(msg, ClientMessage::CancelTask { .. })
                        && drop_cancel.swap(false, Ordering::SeqCst)
                    {
                        // Forward the pooled `CancelTask` so the Master cancels the task, then
                        // drop the pooled client socket before returning `TaskCancelled` so
                        // `McpState::request_with_retry_info` retries `CancelTask` on a fresh
                        // connection (which gets `success: false` from the Master and exercises
                        // the `GetTaskStatus` re-check in `tool_cancel`).
                        cancel_drops.fetch_add(1, Ordering::SeqCst);
                        let _ = upstream_tx.send_msg(&msg).await;
                        let _ = upstream_tx.recv_msg::<ClientResponse>().await;
                        break;
                    }
                    if let ClientMessage::SubmitTask { ref task, .. } = msg {
                        let tag = match &task.spec {
                            TaskSpec::Command { args, .. } => {
                                args.first().map(String::as_str).unwrap_or("")
                            }
                            _ => "",
                        };
                        match tag {
                            "f1-eof" if eof_s.fetch_add(1, Ordering::SeqCst) == 0 => {
                                if upstream_tx.send_msg(&msg).await.is_err() {
                                    break;
                                }
                                let _ = upstream_tx.recv_msg::<ClientResponse>().await;
                                // Close pooled client connection right after forwarding SubmitTask.
                                break;
                            }
                            "f1-corrupt" if corrupt_s.fetch_add(1, Ordering::SeqCst) == 0 => {
                                if upstream_tx.send_msg(&msg).await.is_err() {
                                    break;
                                }
                                let _ = upstream_tx.recv_msg::<ClientResponse>().await;
                                // Send undecodable frame header (> MAX_FRAME_LENGTH) after write.
                                let mut raw = client_tx.into_inner().into_inner();
                                let _ = raw.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).await;
                                let _ = raw.flush().await;
                                break;
                            }
                            "f3-drop" => {
                                if f3_s.fetch_add(1, Ordering::SeqCst) == 0 {
                                    // Hold the original `SubmitTask` until `GetTaskStatus` at 1.0s
                                    // has answered `not found`, then forward the held original to
                                    // the Master FIRST so the subsequent resend receives
                                    // `Task ... already exists in the queue` from the Master.
                                    *f3_tid_slot.lock().await = Some(task.id);
                                    let _ = tokio::time::timeout(Duration::from_secs(3), async {
                                        while !*nf_rx.borrow_and_update() {
                                            if nf_rx.changed().await.is_err() {
                                                break;
                                            }
                                        }
                                    })
                                    .await;
                                    let _ = upstream_tx.send_msg(&msg).await;
                                    let _ = upstream_tx.recv_msg::<ClientResponse>().await;
                                    let _ = orig_tx.send(true);
                                    break;
                                }
                                // Wait for the held original to land on the Master before
                                // forwarding the resend so the Master answers `already exists`.
                                let _ = tokio::time::timeout(Duration::from_secs(3), async {
                                    while !*orig_rx.borrow_and_update() {
                                        if orig_rx.changed().await.is_err() {
                                            break;
                                        }
                                    }
                                })
                                .await;
                            }
                            "f3-lost" if f3_lost_s.fetch_add(1, Ordering::SeqCst) == 0 => {
                                // Drop the first `SubmitTask` without forwarding it to the Master
                                // so `GetTaskStatus` answers `not found` and the resend lands first
                                // (`ClientResponse::TaskSubmitted`).
                                break;
                            }
                            "f4-cancel" => {
                                *cancel_tid.lock().await = Some(task.id);
                                // Delay forwarding SubmitTask by 200ms so notifications/cancelled
                                // at 50ms arrives before the task exists on the Master, then
                                // forward SubmitTask and close the client socket so only the
                                // post-write cancel check in `tool_submit` cancels the task.
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                if upstream_tx.send_msg(&msg).await.is_err() {
                                    break;
                                }
                                let _ = upstream_tx.recv_msg::<ClientResponse>().await;
                                break;
                            }
                            _ => {}
                        }
                    }
                    if upstream_tx.send_msg(&msg).await.is_err() {
                        break;
                    }
                    let Ok(Some(resp)) = upstream_tx.recv_msg::<ClientResponse>().await else {
                        break;
                    };
                    if client_tx.send_msg(&resp).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let mut mcp = McpHarness::spawn(temp.path(), &proxy_addr);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "mcp1-f1-f4", "version": "1.0" }
        }
    }));
    let _ = mcp.recv_timeout(Duration::from_secs(5));

    // 1. F1 (EOF on pooled connection right after forwarding SubmitTask):
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["f1-eof"],
                "require_tags": ["absent"],
                "wait": false
            }
        }
    }));
    let eof_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(eof_resp["id"], 2);
    assert_eq!(
        eof_resp["result"]["isError"], false,
        "EOF after pooled SubmitTask write must recover via GetTaskStatus without resending: {eof_resp}"
    );
    let eof_body: Value = serde_json::from_str(
        eof_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("eof text"),
    )
    .expect("eof json");
    assert_eq!(eof_body["timed_out"], true);
    assert_eq!(eof_body["state"], "Queued");
    assert!(
        eof_body["message"]
            .as_str()
            .unwrap_or("")
            .contains("query its outcome"),
        "expected advice to query outcome: {eof_body}"
    );
    let eof_tid =
        Uuid::parse_str(eof_body["task_id"].as_str().expect("task_id")).expect("valid uuid");
    assert_eq!(
        eof_submits.load(Ordering::SeqCst),
        1,
        "EOF on pooled connection must not resend SubmitTask"
    );
    assert_eq!(
        master.queue().list_tasks().await.len(),
        1,
        "Master must hold exactly one task after f1-eof"
    );
    assert!(
        master.queue().get_task(&TaskId(eof_tid)).await.is_some(),
        "Master must hold the task from f1-eof"
    );

    // 2. F1 (Undecodable frame on pooled connection right after forwarding SubmitTask):
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["f1-corrupt"],
                "require_tags": ["absent"],
                "wait": false
            }
        }
    }));
    let corrupt_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(corrupt_resp["id"], 3);
    assert_eq!(
        corrupt_resp["result"]["isError"], false,
        "Decode error after pooled SubmitTask write must recover via GetTaskStatus without resending: {corrupt_resp}"
    );
    let corrupt_body: Value = serde_json::from_str(
        corrupt_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("corrupt text"),
    )
    .expect("corrupt json");
    assert_eq!(corrupt_body["timed_out"], true);
    assert_eq!(corrupt_body["state"], "Queued");
    let corrupt_tid =
        Uuid::parse_str(corrupt_body["task_id"].as_str().expect("task_id")).expect("valid uuid");
    assert_eq!(
        corrupt_submits.load(Ordering::SeqCst),
        1,
        "Decode error on pooled connection must not resend SubmitTask"
    );
    assert!(
        master
            .queue()
            .get_task(&TaskId(corrupt_tid))
            .await
            .is_some(),
        "Master must hold the task from f1-corrupt"
    );

    // 3. F2: `status` on a pooled connection that stops answering (times out after 2s)
    // must recover on a fresh connection.
    stall_next_status.store(true, Ordering::SeqCst);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "status",
            "arguments": {}
        }
    }));
    let st_resp = mcp.recv_timeout(Duration::from_secs(6));
    assert_eq!(st_resp["id"], 4);
    assert_eq!(
        st_resp["result"]["isError"], false,
        "idempotent `status` must recover on a fresh connection after pooled timeout: {st_resp}"
    );
    let st_body: Value = serde_json::from_str(
        st_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("status text"),
    )
    .expect("status json");
    assert_eq!(st_body["cluster_status"], "ok");
    assert_eq!(
        status_stalls.load(Ordering::SeqCst),
        1,
        "status must have hit the pooled ListWorkers stall before recovering"
    );

    // 4a. F3 (`already exists` arm): When the first `SubmitTask` is held until `GetTaskStatus`
    // at 1.0s answers `Task ... not found` and then lands on the Master before the resend,
    // the resend receives `already exists` from the Master and `submit` must treat it as
    // `status: "submitted"` (`isError: false`), leaving exactly one task for that `task_id`.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["f3-drop"],
                "require_tags": ["absent"],
                "wait": false,
                "wait_secs": 1
            }
        }
    }));
    let drop_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(drop_resp["id"], 5);
    assert_eq!(
        drop_resp["result"]["isError"], false,
        "SubmitTask resend that hits `already exists` after held original lands must succeed: {drop_resp}"
    );
    let drop_body: Value = serde_json::from_str(
        drop_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("drop text"),
    )
    .expect("drop json");
    assert_eq!(drop_body["status"], "submitted");
    let f3_tid =
        Uuid::parse_str(drop_body["task_id"].as_str().expect("f3 task_id")).expect("valid uuid");
    assert_eq!(
        f3_submits.load(Ordering::SeqCst),
        2,
        "expected initial SubmitTask + 1 same-task_id resend after not-found"
    );
    assert!(
        master.queue().get_task(&TaskId(f3_tid)).await.is_some(),
        "Master must hold the task from f3-drop"
    );
    assert_eq!(
        master.queue().list_tasks().await.len(),
        3,
        "Master must hold exactly one task each from f1-eof, f1-corrupt, and f3-drop"
    );

    // 4b. F3 (`TaskSubmitted` arm): When the first `SubmitTask` is lost completely when the
    // pooled connection closes, `GetTaskStatus` answers `not found` and the resend lands first
    // (`ClientResponse::TaskSubmitted`).
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 50,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["f3-lost"],
                "require_tags": ["absent"],
                "wait": false,
                "wait_secs": 1
            }
        }
    }));
    let lost_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(lost_resp["id"], 50);
    assert_eq!(
        lost_resp["result"]["isError"], false,
        "SubmitTask resend that lands first must succeed: {lost_resp}"
    );
    let lost_body: Value = serde_json::from_str(
        lost_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("lost text"),
    )
    .expect("lost json");
    assert_eq!(lost_body["status"], "submitted");
    assert_eq!(f3_lost_submits.load(Ordering::SeqCst), 2);
    assert_eq!(master.queue().list_tasks().await.len(), 4);

    // 4c. `tool_cancel` pooled-retry re-check:
    // Drop the pooled connection after forwarding the first `CancelTask` for `f3_tid` (so the
    // Master cancels `f3_tid`, then answers `success: false` on the fresh-connection retry).
    // `tool_cancel` must re-check `GetTaskStatus` on a pooled retry and report `cancelled: true`,
    // whereas a subsequent ordinary `cancel` on that already-cancelled task (no pooled retry)
    // must report `cancelled: false`.
    drop_next_cancel_reply.store(true, Ordering::SeqCst);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 51,
        "method": "tools/call",
        "params": {
            "name": "cancel",
            "arguments": {
                "task_id": f3_tid.to_string()
            }
        }
    }));
    let cancel_retry_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(cancel_retry_resp["id"], 51);
    assert_eq!(
        cancel_retry_resp["result"]["isError"], false,
        "{cancel_retry_resp}"
    );
    let cancel_retry_body: Value = serde_json::from_str(
        cancel_retry_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("cancel_retry text"),
    )
    .expect("cancel_retry json");
    assert_eq!(
        cancel_retry_body["cancelled"], true,
        "pooled-retry CancelTask must re-check GetTaskStatus and report cancelled: true: {cancel_retry_body}"
    );
    assert_eq!(
        cancel_reply_drops.load(Ordering::SeqCst),
        1,
        "expected first pooled CancelTask reply to be dropped by proxy"
    );

    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 52,
        "method": "tools/call",
        "params": {
            "name": "cancel",
            "arguments": {
                "task_id": f3_tid.to_string()
            }
        }
    }));
    let cancel_repeat_resp = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(cancel_repeat_resp["id"], 52);
    assert_eq!(
        cancel_repeat_resp["result"]["isError"], false,
        "{cancel_repeat_resp}"
    );
    let cancel_repeat_body: Value = serde_json::from_str(
        cancel_repeat_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("cancel_repeat text"),
    )
    .expect("cancel_repeat json");
    assert_eq!(
        cancel_repeat_body["cancelled"], false,
        "ordinary CancelTask on an already-cancelled task (without pooled retry) must report cancelled: false: {cancel_repeat_body}"
    );

    // 5. F4: Cancel check in the post-write failure branch of `tool_submit`.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 6,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["f4-cancel"],
                "require_tags": ["absent"],
                "wait": false
            }
        }
    }));
    tokio::time::sleep(Duration::from_millis(50)).await;
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {
            "requestId": 6,
            "reason": "cancel while SubmitTask in flight before EOF"
        }
    }));

    let cancel_deadline = Instant::now() + Duration::from_secs(4);
    loop {
        if let Some(tid) = *cancelled_tid.lock().await {
            if let Some(info) = master.queue().get_task(&tid).await {
                if info.state == TaskState::Cancelled {
                    break;
                }
            }
        }
        assert!(
            Instant::now() < cancel_deadline,
            "expected post-write cancel check in `tool_submit` to cancel the task on the Master"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // 6. `task_result` with `wait: true` and `timeout_secs: 0` on a missing task and a finished task.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {
            "name": "task_result",
            "arguments": {
                "task_id": "22222222-3333-4444-5555-666666666666",
                "wait": true,
                "timeout_secs": 0
            }
        }
    }));
    let missing_res = mcp.recv_timeout(Duration::from_secs(5));
    assert_eq!(missing_res["id"], 7);
    assert_eq!(
        missing_res["result"]["isError"], true,
        "task_result {{wait:true, timeout_secs:0}} on missing task must return isError: true: {missing_res}"
    );
    assert!(
        missing_res["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("")
            .contains("not found"),
        "expected not found error: {missing_res}"
    );

    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 8,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": "echo",
                "args": ["zero-wait-done"],
                "wait": true,
                "wait_secs": 10
            }
        }
    }));
    let done_sub = mcp.recv_timeout(Duration::from_secs(10));
    assert_eq!(done_sub["id"], 8);
    assert_eq!(done_sub["result"]["isError"], false, "{done_sub}");
    let done_sub_body: Value = serde_json::from_str(
        done_sub["result"]["content"][0]["text"]
            .as_str()
            .expect("done_sub text"),
    )
    .expect("done_sub json");
    let done_tid = done_sub_body["task_id"]
        .as_str()
        .expect("done task_id")
        .to_string();

    let done_deadline = Instant::now() + Duration::from_secs(15);
    let mut req_id = 9u64;
    let done_res_body = loop {
        for w in master.registry().list_workers().await {
            let _ = master
                .registry()
                .record_heartbeat(&w.worker_id, 0, 1000, 10.0, 4096, None)
                .await;
        }

        mcp.send(&json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "tools/call",
            "params": {
                "name": "task_result",
                "arguments": {
                    "task_id": done_tid,
                    "wait": true,
                    "timeout_secs": 0
                }
            }
        }));
        let done_res = mcp.recv_timeout(Duration::from_secs(5));
        assert_eq!(done_res["id"], req_id);
        req_id += 1;
        assert_eq!(
            done_res["result"]["isError"], false,
            "task_result {{wait:true, timeout_secs:0}} on task must return result: {done_res}"
        );
        let body: Value = serde_json::from_str(
            done_res["result"]["content"][0]["text"]
                .as_str()
                .expect("done_res text"),
        )
        .expect("done_res json");
        if body["status"] == "completed" {
            break body;
        }
        assert!(
            Instant::now() < done_deadline,
            "timed out waiting for task {done_tid} to reach completed status, last response: {body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(done_res_body["status"], "completed");
    assert!(
        done_res_body["stdout"]
            .as_str()
            .unwrap_or("")
            .contains("zero-wait-done"),
        "expected stdout from finished task: {done_res_body}"
    );

    proxy_handle.abort();
    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}
