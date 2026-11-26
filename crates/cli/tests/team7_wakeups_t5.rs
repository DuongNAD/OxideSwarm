//! T5 wake-up acceptance tests (drafted RED-first, design 0111 D8.4, TEAM-7).
//!
//! Covers:
//! 1. `oxideswarm mail watch --consumer <id> --hook` blocks on `MailWait`, prints one sanitized
//!    line per arriving message to stderr, exits with code 2 when mail arrives, and exits 0 on
//!    `--max-wait` timeout with no mail.
//! 2. `oxideswarm mail watch --exec <cmd>` runs `<cmd>` with `OXIDESWARM_MAIL_*` metadata in env
//!    without shell-evaluating untrusted message topics or bodies.
//! 3. MCP `mail_wait` wakes immediately when matching mail arrives, returns `timed_out: true` when
//!    empty, and aborts cleanly on `ctx.cancel_tx`.
//! 4. `scripts/agent_bridge.py --backend grid` performs `send`, `peek`, `inbox`, `watch --hook`
//!    (exit code 2), and `inbox --ack all` end-to-end against a live loopback master.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use rusty_grid_cli::connect::connect_master;
use rusty_grid_cli::mcp::{call_tool_with_context, McpState, ToolCallContext};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

async fn spawn_mailbox_master(
    blob_dir: &std::path::Path,
    state_dir: &std::path::Path,
) -> MasterHandle {
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_discovery_port(0)
        .with_blob_store(blob_dir, 8 * 1024 * 1024)
        .with_state_dir(state_dir);
    MasterServer::spawn(cfg).await.expect("master starts")
}

async fn send_mail_rpc(addr: &str, to: Vec<String>, topic: &str, body: &str) -> u64 {
    let mut transport = connect_master(addr, None).await.expect("connect to master");
    transport
        .send_msg(&ClientMessage::MailSend {
            to,
            topic: topic.to_string(),
            body: body.to_string(),
            attachments: vec![],
        })
        .await
        .expect("send MailSend");
    let resp = transport
        .recv_msg::<ClientResponse>()
        .await
        .expect("recv MailSent")
        .expect("frame");
    transport.close().await;
    match resp {
        ClientResponse::MailSent { id } => id,
        other => panic!("expected MailSent, got {other:?}"),
    }
}

fn find_python3() -> Option<&'static str> {
    let candidates = if cfg!(windows) {
        ["python", "python3"]
    } else {
        ["python3", "python"]
    };
    for cand in candidates {
        if let Ok(out) = std::process::Command::new(cand).arg("--version").output() {
            if out.status.success() {
                let text = String::from_utf8_lossy(&out.stdout);
                if text.trim_start().starts_with("Python 3.") {
                    return Some(cand);
                }
            }
        }
    }
    None
}

#[tokio::test]
async fn t5_mail_watch_hook_blocks_prints_one_line_per_message_and_exits_2() {
    let blob_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let ox_home = work.path().join("ox_home");
    std::fs::create_dir_all(&ox_home).unwrap();

    let master = spawn_mailbox_master(blob_dir.path(), state_dir.path()).await;
    let addr = master.server_addr().to_string();

    // Pre-enqueue two messages to verify `--hook` prints one summary line per message on arrival.
    send_mail_rpc(&addr, vec!["agent-win-2".into()], "batch.1", "first").await;
    send_mail_rpc(&addr, vec!["agent-win-2".into()], "batch.2", "second").await;

    let batch_hook = tokio::process::Command::new(BIN)
        .env("OXIDESWARM_HOME", &ox_home)
        .args([
            "mail",
            "watch",
            "--master",
            &addr,
            "--consumer",
            "agent-win-2",
            "--hook",
            "--max-wait",
            "10",
            "--timeout-secs",
            "10",
        ])
        .output()
        .await
        .expect("run batch hook watch");
    assert_eq!(
        batch_hook.status.code(),
        Some(2),
        "hook must exit 2 when mail is present; stderr={}",
        String::from_utf8_lossy(&batch_hook.stderr)
    );
    assert!(
        batch_hook.stdout.is_empty(),
        "hook mode must keep stdout empty, got: {}",
        String::from_utf8_lossy(&batch_hook.stdout)
    );
    let batch_stderr = String::from_utf8_lossy(&batch_hook.stderr);
    assert!(
        batch_stderr.contains("#1 ") && batch_stderr.contains("topic=batch.1"),
        "{batch_stderr}"
    );
    assert!(
        batch_stderr.contains("#2 ") && batch_stderr.contains("topic=batch.2"),
        "{batch_stderr}"
    );

    // Ack #2 so cursor advances cleanly, then spawn `--hook` on an empty inbox while sending #3.
    let ack = tokio::process::Command::new(BIN)
        .env("OXIDESWARM_HOME", &ox_home)
        .args([
            "mail",
            "ack",
            "--master",
            &addr,
            "--consumer",
            "agent-win-2",
            "--id",
            "2",
        ])
        .output()
        .await
        .expect("ack batch");
    assert!(ack.status.success());

    let child = tokio::process::Command::new(BIN)
        .env("OXIDESWARM_HOME", &ox_home)
        .args([
            "mail",
            "watch",
            "--master",
            &addr,
            "--consumer",
            "agent-win-2",
            "--hook",
            "--max-wait",
            "15",
            "--timeout-secs",
            "15",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn waiting hook");

    tokio::time::sleep(Duration::from_millis(250)).await;
    let id3 = send_mail_rpc(&addr, vec!["agent-win-2".into()], "wake.now", "wake up").await;
    assert_eq!(id3, 3);

    let out = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .expect("hook watch must wake promptly after MailSend")
        .expect("collect hook output");
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("#3 ") && stderr.contains("topic=wake.now"),
        "{stderr}"
    );
    assert!(stderr.contains("untrusted data"), "{stderr}");

    // Empty wait expiry in `--hook` mode must exit 0 silently.
    let empty_hook = tokio::process::Command::new(BIN)
        .env("OXIDESWARM_HOME", &ox_home)
        .args([
            "mail",
            "watch",
            "--master",
            &addr,
            "--consumer",
            "idle-consumer",
            "--hook",
            "--max-wait",
            "1",
            "--timeout-secs",
            "1",
        ])
        .output()
        .await
        .expect("run empty hook");
    assert_eq!(
        empty_hook.status.code(),
        Some(0),
        "empty expired hook should exit 0; stderr={}",
        String::from_utf8_lossy(&empty_hook.stderr)
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn t5_mail_watch_exec_runs_command_with_env_without_shell_eval() {
    let Some(py) = find_python3() else {
        eprintln!("skipping python --exec test: python3 not available on PATH");
        return;
    };
    let blob_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let ox_home = work.path().join("ox_home");
    std::fs::create_dir_all(&ox_home).unwrap();

    let master = spawn_mailbox_master(blob_dir.path(), state_dir.path()).await;
    let addr = master.server_addr().to_string();

    let marker_file = work.path().join("exec_marker.json");
    let pwn_file = work.path().join("should_not_exist.txt");
    let script_path = work.path().join("record_env.py");
    std::fs::write(
        &script_path,
        format!(
            "import json, os\nopen(r'''{}''', 'w', encoding='utf-8').write(json.dumps({{k: os.environ.get(k) for k in ['OXIDESWARM_MAIL_ID','OXIDESWARM_MAIL_FROM','OXIDESWARM_MAIL_TOPIC','OXIDESWARM_MAIL_CONSUMER','OXIDESWARM_EVENT_KIND']}}))\n",
            marker_file.display()
        ),
    )
    .unwrap();

    let exec_cmd = format!("{py} \"{}\"", script_path.display());
    let child = tokio::process::Command::new(BIN)
        .env("OXIDESWARM_HOME", &ox_home)
        .args([
            "mail",
            "watch",
            "--master",
            &addr,
            "--consumer",
            "agent-win-2",
            "--once",
            "--max-wait",
            "15",
            "--timeout-secs",
            "15",
            "--exec",
            &exec_cmd,
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn exec watch");

    tokio::time::sleep(Duration::from_millis(250)).await;
    let hostile_topic = format!("$(echo bad > {}) & del_check", pwn_file.display());
    send_mail_rpc(
        &addr,
        vec!["agent-win-2".into()],
        &hostile_topic,
        "$(touch should_not_exist)",
    )
    .await;

    let out = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .expect("--exec watch finishes")
        .expect("collect output");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "watch --once --exec failed: {stderr}");
    assert!(
        !stderr.contains("[mail watch --exec WARN]"),
        "unexpected --exec warning: {stderr}"
    );
    assert!(
        !pwn_file.exists(),
        "mail content must never be evaluated by shell"
    );

    let captured: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&marker_file).expect("marker written"))
            .unwrap();
    assert_eq!(captured["OXIDESWARM_MAIL_ID"], "1");
    assert_eq!(captured["OXIDESWARM_MAIL_CONSUMER"], "agent-win-2");
    assert_eq!(captured["OXIDESWARM_MAIL_TOPIC"], hostile_topic);
    assert_eq!(captured["OXIDESWARM_EVENT_KIND"], "message");

    let _ = master.shutdown();
}

#[tokio::test]
async fn t5_mcp_mail_wait_wakes_on_send_times_out_and_cancels() {
    let tools = rusty_grid_cli::mcp::available_tools();
    assert!(
        tools.iter().any(|t| t.name == "mail_wait"),
        "MCP tool list must expose mail_wait"
    );

    let blob_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let master = spawn_mailbox_master(blob_dir.path(), state_dir.path()).await;
    let addr = master.server_addr().to_string();
    let state = Arc::new(McpState::new(addr.clone(), None));

    // 1. Empty non-blocking check (`timeout_secs: 0`) returns `timed_out: true`.
    let res_empty = call_tool_with_context(
        &state,
        "mail_wait",
        &json!({"consumer": "agent-win-2", "timeout_secs": 0}),
        &mut ToolCallContext::default(),
    )
    .await;
    assert!(!res_empty.is_error, "{:?}", res_empty.content);
    let rusty_grid_cli::mcp::ToolContent::Text { ref text } = res_empty.content[0];
    let parsed_empty: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(parsed_empty["timed_out"], true);
    assert_eq!(parsed_empty["messages"].as_array().unwrap().len(), 0);

    // 2. Blocking wait wakes immediately when matching mail is sent.
    let wait_state = Arc::clone(&state);
    let waiter = tokio::spawn(async move {
        let mut ctx = ToolCallContext::default();
        call_tool_with_context(
            &wait_state,
            "mail_wait",
            &json!({"consumer": "agent-win-2", "topics": ["topic:team-7"], "timeout_secs": 15}),
            &mut ctx,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(120)).await;
    let msg_id = send_mail_rpc(&addr, vec!["topic:team-7".into()], "wake", "hello mcp").await;

    let res_wake = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("MCP mail_wait must wake promptly")
        .unwrap();
    assert!(!res_wake.is_error, "{:?}", res_wake.content);
    let rusty_grid_cli::mcp::ToolContent::Text { ref text } = res_wake.content[0];
    let parsed_wake: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(parsed_wake["timed_out"], false);
    assert_eq!(parsed_wake["messages"][0]["id"], msg_id);
    assert!(parsed_wake["data_notice"]
        .as_str()
        .unwrap()
        .contains("untrusted data"));

    // 3. Cancellation via `ctx.cancel_tx` aborts cleanly without waiting for full timeout.
    let mut cancel_ctx = ToolCallContext::default();
    let cancel_tx = cancel_ctx.cancel_tx.clone();
    let cancel_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(120)).await;
        let _ = cancel_tx.send(true);
    });
    let res_cancel = tokio::time::timeout(
        Duration::from_secs(5),
        call_tool_with_context(
            &state,
            "mail_wait",
            &json!({"consumer": "idle-mcp", "timeout_secs": 30}),
            &mut cancel_ctx,
        ),
    )
    .await
    .expect("cancelled MCP mail_wait must return promptly");
    cancel_handle.await.unwrap();
    assert!(cancel_ctx.aborted_by_cancel);
    assert!(res_cancel.is_error);

    state.close().await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn t5_agent_bridge_grid_backend_roundtrip_and_hook_against_live_master() {
    let Some(py) = find_python3() else {
        eprintln!("skipping agent_bridge grid live test: python3 not available on PATH");
        return;
    };
    let blob_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let home = work.path().join("home");
    let ox_home = work.path().join("ox_home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&ox_home).unwrap();

    let master = spawn_mailbox_master(blob_dir.path(), state_dir.path()).await;
    let addr = master.server_addr().to_string();

    let bridge_script = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("scripts")
        .join("agent_bridge.py");

    let run_bridge = |args: Vec<&str>| {
        let mut cmd = tokio::process::Command::new(py);
        cmd.arg(&bridge_script)
            .args(args)
            .env("HOME", &home)
            .env("USERPROFILE", &home)
            .env("OXIDESWARM_HOME", &ox_home)
            .env("OXIDESWARM_BIN", BIN)
            .env("OXIDESWARM_BRIDGE_DISABLE_DRIVE_SCAN", "1");
        cmd
    };

    let send_out = run_bridge(vec![
        "--backend",
        "grid",
        "--master",
        &addr,
        "send",
        "--agent-id",
        "agent-mac",
        "--peer-id",
        "agent-win-2",
        "--title",
        "Wake check",
        "--body",
        "Grid hello",
    ])
    .output()
    .await
    .expect("run bridge send");
    assert!(
        send_out.status.success(),
        "bridge send failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&send_out.stdout),
        String::from_utf8_lossy(&send_out.stderr)
    );

    let peek_out = run_bridge(vec![
        "--backend",
        "grid",
        "--master",
        &addr,
        "peek",
        "--agent-id",
        "agent-win-2",
    ])
    .output()
    .await
    .expect("run bridge peek");
    assert!(peek_out.status.success());
    let peek_stdout = String::from_utf8_lossy(&peek_out.stdout);
    assert!(peek_stdout.contains("1 unread"), "peek={peek_stdout}");

    let inbox_out = run_bridge(vec![
        "--backend",
        "grid",
        "--master",
        &addr,
        "inbox",
        "--agent-id",
        "agent-win-2",
    ])
    .output()
    .await
    .expect("run bridge inbox");
    assert!(inbox_out.status.success());
    let inbox_stdout = String::from_utf8_lossy(&inbox_out.stdout);
    assert!(
        inbox_stdout.contains("Wake check") && inbox_stdout.contains("Grid hello"),
        "inbox={inbox_stdout}"
    );

    let watch_hook = run_bridge(vec![
        "--backend",
        "grid",
        "--master",
        &addr,
        "watch",
        "--agent-id",
        "agent-win-2",
        "--consumer",
        "agy",
        "--hook",
        "--max-wait",
        "10",
    ])
    .output()
    .await
    .expect("run bridge watch --hook");
    assert_eq!(
        watch_hook.status.code(),
        Some(2),
        "bridge --backend grid watch --hook should exit 2; stderr={}",
        String::from_utf8_lossy(&watch_hook.stderr)
    );

    let ack_out = run_bridge(vec![
        "--backend",
        "grid",
        "--master",
        &addr,
        "inbox",
        "--agent-id",
        "agent-win-2",
        "--ack",
        "all",
    ])
    .output()
    .await
    .expect("run bridge inbox --ack all");
    assert!(
        ack_out.status.success(),
        "bridge ack failed: {}",
        String::from_utf8_lossy(&ack_out.stderr)
    );

    let peek_after = run_bridge(vec![
        "--backend",
        "grid",
        "--master",
        &addr,
        "peek",
        "--agent-id",
        "agent-win-2",
    ])
    .output()
    .await
    .expect("run bridge peek after ack");
    assert!(peek_after.status.success());
    assert!(peek_after.stdout.is_empty());

    // Re-arming watch --consumer agy --hook after wake + inbox --ack must NOT re-wake.
    let watch_rearm = run_bridge(vec![
        "--backend",
        "grid",
        "--master",
        &addr,
        "watch",
        "--agent-id",
        "agent-win-2",
        "--consumer",
        "agy",
        "--hook",
        "--max-wait",
        "1",
    ])
    .output()
    .await
    .expect("run re-armed bridge watch --hook");
    assert_eq!(
        watch_rearm.status.code(),
        Some(0),
        "re-armed watch --hook must exit 0 with no infinite re-wake loop; stderr={}",
        String::from_utf8_lossy(&watch_rearm.stderr)
    );

    let _ = master.shutdown();
}
