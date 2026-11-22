//! Integration & binary E2E tests for MESH-2 (`Deliver mesh data to consumer`).
//!
//! Covers all 6 MESH-2 acceptance scenarios:
//! 1. `agent-mesh data --to missing-node` exits non-zero promptly with NACK diagnostics.
//! 2. `agent-mesh node --inbox-dir <DIR>` persists verified single-chunk, binary, empty, and
//!    multi-chunk payloads atomically with matching SHA-256; `agent-mesh data` exits 0 and prints
//!    correlation ID and SHA-256.
//! 3. Corrupted SHA-256 payloads elicit `DeliveryNack(ERR_PAYLOAD_CORRUPT)` and write zero files.
//! 4. Invalid `--inbox-dir` (regular file / symlink) fails upfront at startup; read-only
//!    `--inbox-dir` emits `DeliveryNack(ERR_PERSIST_FAILED)` with zero partial `.tmp` files and
//!    causes `agent-mesh data` to exit non-zero.
//! 5. Bounded `received_payloads` buffer enforces count and byte caps under bursts.
//! 6. Honest `/api/nodes` and `/api/status` telemetry (`online`, `stale`, disconnect cleanup) and
//!    accurate `total_chunks` on multi-chunk transfers.

use agent_mesh::{
    compute_sha256, AgentMeshClient, AgentMeshEnvelope, AgentMeshHub, HubConfig,
    ACK_STATUS_CHUNK_RECEIVED, ACK_STATUS_DATA_VERIFIED, DATA_CHUNK_WINDOW,
    DEFAULT_DATA_CHUNK_BYTES, ERR_INBOX_FULL, ERR_NODE_NOT_FOUND, ERR_PAYLOAD_CORRUPT,
};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn agent_mesh_bin() -> &'static str {
    env!("CARGO_BIN_EXE_agent-mesh")
}

async fn wait_for_node_registered(http_base: &str, node_id: &str) {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(resp) = client.get(format!("{http_base}/api/nodes")).send().await {
            if let Ok(nodes) = resp.json::<Vec<serde_json::Value>>().await {
                if nodes
                    .iter()
                    .any(|n| n.get("node_id").and_then(|v| v.as_str()) == Some(node_id))
                {
                    return;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for node '{node_id}' to register at {http_base}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

type RawWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Next envelope on a raw WebSocket peer (60 s guard against a hung test).
async fn next_envelope(ws: &mut RawWs) -> AgentMeshEnvelope {
    use futures::StreamExt;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(60), ws.next())
            .await
            .expect("no frame within 60 s")
            .expect("socket closed")
            .expect("socket error");
        if let Ok(text) = frame.to_text() {
            if text.is_empty() {
                continue;
            }
            return AgentMeshEnvelope::from_json_str(text).expect("valid envelope");
        }
    }
}

/// Next reply on a raw sender socket that is not per-chunk flow control (`chunk_received`).
async fn next_final_reply(ws: &mut RawWs) -> AgentMeshEnvelope {
    loop {
        let env = next_envelope(ws).await;
        match env {
            AgentMeshEnvelope::DeliveryAck { ref status, .. }
                if status == ACK_STATUS_CHUNK_RECEIVED => {}
            other => return other,
        }
    }
}

/// Opens a raw WebSocket to the hub and registers it as `node_id`.
async fn raw_node(ws_url: &str, node_id: &str) -> RawWs {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::protocol::Message as WsMsg;
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url).await.unwrap();
    let reg = serde_json::json!({
        "type": "NodeRegistration",
        "correlation_id": format!("reg-{node_id}"),
        "from": node_id,
        "to": "hub",
        "platform": "linux",
    });
    ws.send(WsMsg::Text(reg.to_string())).await.unwrap();
    match next_envelope(&mut ws).await {
        AgentMeshEnvelope::NodeRegistrationAck { .. } => {}
        other => panic!("registration of {node_id} failed: {other:?}"),
    }
    ws
}

async fn raw_send(ws: &mut RawWs, env: &AgentMeshEnvelope) {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::protocol::Message as WsMsg;
    ws.send(WsMsg::Text(env.to_json_string().unwrap()))
        .await
        .unwrap();
}

fn spawn_hub_process() -> (ChildGuard, u16, String, String) {
    let mut cmd = Command::new(agent_mesh_bin());
    cmd.args(["hub", "--listen", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = cmd.spawn().expect("spawn agent-mesh hub");
    let stdout = child.stdout.take().expect("hub stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    let ws_url = loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for hub startup banner"
        );
        line.clear();
        let n = reader.read_line(&mut line).expect("read hub stdout");
        assert!(n > 0, "hub exited before printing ws URL");
        if line.contains("[HUB] OxideRelay Hub running at ") {
            if let Some(pos) = line.find("ws://") {
                break line[pos..].trim().to_string();
            }
        }
    };
    let port_str = ws_url
        .trim_start_matches("ws://127.0.0.1:")
        .trim_end_matches("/ws");
    let port: u16 = port_str.parse().expect("parse hub port");
    let http_base = format!("http://127.0.0.1:{port}");
    // Drain stdout in background so hub never blocks on full pipe.
    std::thread::spawn(move || {
        let mut sink = String::new();
        while reader.read_line(&mut sink).unwrap_or(0) > 0 {
            sink.clear();
        }
    });
    (ChildGuard::new(child), port, ws_url, http_base)
}

#[test]
fn test_node_cli_rejects_regular_file_and_symlink_inbox_dir_at_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let file_path = tmp.path().join("regular_file.txt");
    std::fs::write(&file_path, b"not a directory").unwrap();

    let out_file = Command::new(agent_mesh_bin())
        .args([
            "node",
            "--hub",
            "ws://127.0.0.1:1/ws",
            "--id",
            "bad-inbox-node",
            "--inbox-dir",
            file_path.to_str().unwrap(),
        ])
        .output()
        .expect("run agent-mesh node");
    assert!(
        !out_file.status.success(),
        "node --inbox-dir <file> must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out_file.stderr);
    assert!(
        stderr.contains("not a directory") || stderr.contains("inbox"),
        "unexpected stderr: {stderr}"
    );

    #[cfg(unix)]
    {
        let real_dir = tmp.path().join("real_dir");
        std::fs::create_dir(&real_dir).unwrap();
        let link_path = tmp.path().join("symlink_inbox");
        std::os::unix::fs::symlink(&real_dir, &link_path).unwrap();

        let out_link = Command::new(agent_mesh_bin())
            .args([
                "node",
                "--hub",
                "ws://127.0.0.1:1/ws",
                "--id",
                "bad-symlink-node",
                "--inbox-dir",
                link_path.to_str().unwrap(),
            ])
            .output()
            .expect("run agent-mesh node");
        assert!(
            !out_link.status.success(),
            "node --inbox-dir <symlink> must exit non-zero"
        );
        let link_stderr = String::from_utf8_lossy(&out_link.stderr);
        assert!(
            link_stderr.contains("symlink"),
            "unexpected symlink stderr: {link_stderr}"
        );
    }
}

#[tokio::test]
async fn test_data_cli_missing_target_exits_nonzero_and_happy_path_persists_to_inbox() {
    let (_hub_guard, _port, ws_url, http_base) = spawn_hub_process();

    // 1. Missing node must exit non-zero with the hub's NACK, not by running out its timeout.
    let missing_out = Command::new(agent_mesh_bin())
        .args([
            "data",
            "--hub",
            &ws_url,
            "--from",
            "cli-tester",
            "--to",
            "missing-node-404",
            "--payload",
            "should-fail",
            "--timeout-ms",
            "60000",
        ])
        .output()
        .expect("run agent-mesh data");
    assert!(
        !missing_out.status.success(),
        "agent-mesh data --to missing-node must exit non-zero"
    );
    let missing_err = String::from_utf8_lossy(&missing_out.stderr);
    assert!(
        missing_err.contains(ERR_NODE_NOT_FOUND),
        "expected ERR_NODE_NOT_FOUND in stderr, got: {missing_err}"
    );
    assert!(
        !missing_err.contains("Timed out"),
        "the refusal must come from the hub's NACK, not from the timeout: {missing_err}"
    );

    // 2. Start consumer node with --inbox-dir and verify CLI data round-trip.
    let tmp = tempfile::tempdir().unwrap();
    let inbox = tmp.path().join("consumer-inbox");
    let _consumer_guard = ChildGuard::new(
        Command::new(agent_mesh_bin())
            .args([
                "node",
                "--hub",
                &ws_url,
                "--id",
                "cli-consumer",
                "--platform",
                "macos",
                "--inbox-dir",
                inbox.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn consumer node"),
    );
    wait_for_node_registered(&http_base, "cli-consumer").await;

    let payload = "OxideSwarm MESH-2 verified delivery payload";
    let expected_sha = compute_sha256(payload.as_bytes());
    let ok_out = Command::new(agent_mesh_bin())
        .args([
            "data",
            "--hub",
            &ws_url,
            "--from",
            "cli-sender",
            "--to",
            "cli-consumer",
            "--payload",
            payload,
            "--timeout-ms",
            "5000",
        ])
        .output()
        .expect("run agent-mesh data");
    assert!(
        ok_out.status.success(),
        "agent-mesh data failed: stderr={}",
        String::from_utf8_lossy(&ok_out.stderr)
    );
    let stdout = String::from_utf8_lossy(&ok_out.stdout);
    assert!(
        stdout.contains("Correlation ID:") && stdout.contains(&expected_sha),
        "stdout missing correlation ID or SHA-256: {stdout}"
    );

    let files: Vec<_> = std::fs::read_dir(&inbox)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert_eq!(files.len(), 1, "expected exactly 1 persisted payload file");
    let disk_bytes = std::fs::read(files[0].path()).unwrap();
    assert_eq!(disk_bytes, payload.as_bytes());
    assert_eq!(compute_sha256(&disk_bytes), expected_sha);

    // 3. Corrupted SHA-256 frame sent to the running consumer node must NACK and write zero new files.
    let raw_sender = AgentMeshClient::new(&ws_url, "tamper-sender", "linux");
    raw_sender.connect().await.unwrap();
    let mut bad_env =
        AgentMeshEnvelope::new_data_payload("tamper-sender", "cli-consumer", b"evil").0;
    let bad_corr = bad_env.correlation_id().to_string();
    if let AgentMeshEnvelope::DataPayload {
        ref mut checksum_sha256,
        ..
    } = bad_env
    {
        *checksum_sha256 = Some("f".repeat(64));
    }
    raw_sender.send_envelope(bad_env).await.unwrap();
    // Use send_data_payload_with_ack next to ensure consumer processed everything in order
    let empty_receipt = raw_sender
        .send_data_payload_with_ack("cli-consumer", b"", Some(5000))
        .await
        .expect("empty payload should succeed");
    assert_eq!(empty_receipt.sha256, compute_sha256(b""));
    let _ = bad_corr;

    // 4. Send >1 MiB multi-chunk payload and verify total_chunks > 1 & exact SHA-256 on disk.
    let multi_bytes: Vec<u8> = (0..(DEFAULT_DATA_CHUNK_BYTES + 4096))
        .map(|i| (i % 256) as u8)
        .collect();
    let multi_receipt = raw_sender
        .send_data_payload_with_ack("cli-consumer", &multi_bytes, Some(10000))
        .await
        .expect("multi-chunk payload should succeed");
    assert_eq!(multi_receipt.total_chunks, 2);
    assert_eq!(multi_receipt.sha256, compute_sha256(&multi_bytes));

    // Check inbox has exactly 3 valid files (CLI payload, 0-byte empty payload, 1MiB+4KiB multi-chunk),
    // and ZERO files from the corrupted frame.
    let all_files: Vec<_> = std::fs::read_dir(&inbox)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert_eq!(all_files.len(), 3);
    let mut disk_hashes: Vec<String> = all_files
        .iter()
        .map(|e| compute_sha256(&std::fs::read(e.path()).unwrap()))
        .collect();
    disk_hashes.sort();
    let mut expected_hashes = vec![
        expected_sha,
        compute_sha256(b""),
        compute_sha256(&multi_bytes),
    ];
    expected_hashes.sort();
    assert_eq!(disk_hashes, expected_hashes);

    raw_sender.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn test_readonly_inbox_dir_emits_nack_and_exits_nonzero() {
    use std::os::unix::fs::PermissionsExt;

    // Root bypasses DAC 0555 directory permissions on Unix; skip if running as uid 0.
    if std::process::Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .is_some_and(|s| s.trim() == "0")
    {
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let inbox = tmp.path().join("readonly-inbox");
    std::fs::create_dir(&inbox).unwrap();
    std::fs::set_permissions(&inbox, std::fs::Permissions::from_mode(0o555)).unwrap();

    struct PermReset(std::path::PathBuf);
    impl Drop for PermReset {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        }
    }
    let _perm_guard = PermReset(inbox.clone());

    let (_hub_guard, _port, ws_url, http_base) = spawn_hub_process();
    let _consumer_guard = ChildGuard::new(
        Command::new(agent_mesh_bin())
            .args([
                "node",
                "--hub",
                &ws_url,
                "--id",
                "ro-consumer",
                "--inbox-dir",
                inbox.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn ro consumer"),
    );
    wait_for_node_registered(&http_base, "ro-consumer").await;

    let out = Command::new(agent_mesh_bin())
        .args([
            "data",
            "--hub",
            &ws_url,
            "--from",
            "ro-sender",
            "--to",
            "ro-consumer",
            "--payload",
            "cannot-write",
            "--timeout-ms",
            "5000",
        ])
        .output()
        .expect("run agent-mesh data");
    assert!(
        !out.status.success(),
        "agent-mesh data to read-only inbox node must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("ERR_PERSIST_FAILED"),
        "expected ERR_PERSIST_FAILED in stderr, got: {stderr}"
    );
    assert_eq!(std::fs::read_dir(&inbox).unwrap().count(), 0);
}

#[tokio::test]
async fn test_bounded_received_payloads_and_honest_api_telemetry() {
    let cfg = HubConfig::default().with_heartbeat_stale_timeout(Duration::from_millis(150));
    let hub = AgentMeshHub::with_config(cfg);
    let port = hub.start("127.0.0.1:0").await.expect("start hub");
    let ws_url = format!("ws://127.0.0.1:{port}/ws");
    let http_base = format!("http://127.0.0.1:{port}");

    let sender = AgentMeshClient::new(&ws_url, "burst-sender", "macos");
    let receiver = AgentMeshClient::new(&ws_url, "burst-receiver", "linux")
        .with_max_received_payloads(8)
        .with_max_received_bytes(1024);

    sender.connect().await.unwrap();
    receiver.connect().await.unwrap();

    // Burst 20 x 128B payloads: byte cap (1024) limits retained items to <= 8.
    for i in 0..20u8 {
        let mut buf = vec![b'A' + (i % 26); 128];
        buf[0] = i;
        let receipt = sender
            .send_data_payload_with_ack("burst-receiver", &buf, Some(3000))
            .await
            .expect("burst send with ack");
        assert_eq!(receipt.total_chunks, 1);
    }

    let retained = receiver.received_data_payloads().await;
    assert_eq!(retained.len(), 8);

    // Wait past heartbeat_stale_timeout and inspect /api/nodes + /api/status.
    tokio::time::sleep(Duration::from_millis(220)).await;
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let http = reqwest::Client::new();
    let nodes: Vec<serde_json::Value> = http
        .get(format!("{http_base}/api/nodes"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(nodes.len(), 2);
    assert!(nodes.iter().all(|n| n["status"] == "stale"));

    let status: serde_json::Value = http
        .get(format!("{http_base}/api/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["status"], "online");
    assert_eq!(status["connected_nodes"], 2);
    assert_eq!(status["stale_nodes"], 2);
    assert_eq!(status["online_nodes"], 0);
    assert!(status["stats"]["delivered_acks"].as_u64().unwrap() >= 20);

    sender.close().await;
    receiver.close().await;

    // The hub notices the closes asynchronously: poll (the bound only stops a hung test).
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let nodes_after: Vec<serde_json::Value> = http
            .get(format!("{http_base}/api/nodes"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if nodes_after.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "nodes still listed after close: {nodes_after:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    hub.stop().await;
}

#[tokio::test]
async fn test_corrupt_checksum_nack_and_binary_bytes_e2e() {
    let tmp = tempfile::tempdir().unwrap();
    let inbox = agent_mesh::validate_inbox_dir(&tmp.path().join("bin-inbox")).unwrap();

    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("start hub");
    let ws_url = format!("ws://127.0.0.1:{port}/ws");

    let consumer = AgentMeshClient::new(&ws_url, "bin-consumer", "linux").with_inbox_dir(&inbox);
    let sender = AgentMeshClient::new(&ws_url, "bin-sender", "macos");
    consumer.connect().await.unwrap();
    sender.connect().await.unwrap();

    // Arbitrary binary bytes including NUL, high-bit, 0xFF.
    let raw_binary: Vec<u8> = (0..=255u8).collect();
    let receipt = sender
        .send_data_payload_chunked_with_ack("bin-consumer", &raw_binary, 100, Some(5000))
        .await
        .expect("chunked binary delivery must succeed");
    assert_eq!(receipt.total_chunks, 3);
    assert_eq!(receipt.sha256, compute_sha256(&raw_binary));

    let files: Vec<_> = std::fs::read_dir(&inbox)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert_eq!(files.len(), 1);
    assert_eq!(std::fs::read(files[0].path()).unwrap(), raw_binary);

    // Corrupt multi-chunk transfer (wrong total_sha256) must emit DeliveryNack(ERR_PAYLOAD_CORRUPT)
    // over the wire and leave zero extra files in inbox.
    let (mut raw_ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::protocol::Message as WsMsg;
    let reg = serde_json::json!({
        "type": "NodeRegistration",
        "correlation_id": "reg-raw-tamper",
        "from": "raw-tamper",
        "to": "hub",
        "platform": "linux",
    });
    raw_ws.send(WsMsg::Text(reg.to_string())).await.unwrap();
    let _reg_ack = raw_ws.next().await.unwrap().unwrap();

    let (mut envs, corr_id, _) =
        AgentMeshEnvelope::new_data_payload_chunks("raw-tamper", "bin-consumer", b"abcdef", 3);
    for env in &mut envs {
        if let AgentMeshEnvelope::DataPayload {
            metadata: Some(ref mut meta),
            ..
        } = env
        {
            meta.insert("total_sha256".to_string(), "1".repeat(64));
        }
    }
    for env in envs {
        raw_ws
            .send(WsMsg::Text(env.to_json_string().unwrap()))
            .await
            .unwrap();
    }
    let reply_env = next_final_reply(&mut raw_ws).await;
    match reply_env {
        AgentMeshEnvelope::DeliveryNack {
            correlation_id,
            error_code,
            ..
        } => {
            assert_eq!(correlation_id, corr_id);
            assert_eq!(error_code, ERR_PAYLOAD_CORRUPT);
        }
        other => panic!("Expected DeliveryNack(ERR_PAYLOAD_CORRUPT), got {other:?}"),
    }

    // Synchronize with a valid send afterwards
    let follow = sender
        .send_data_payload_with_ack("bin-consumer", b"sync", Some(5000))
        .await
        .expect("follow-up must succeed");
    assert_eq!(follow.status, "data_received_verified");
    // Only 2 files: raw_binary + "sync"
    assert_eq!(std::fs::read_dir(&inbox).unwrap().count(), 2);

    sender.close().await;
    consumer.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_data_cli_payload_file_binary_stdin_and_multichunk() {
    use std::io::Write;

    let (_hub_guard, _port, ws_url, http_base) = spawn_hub_process();
    let tmp = tempfile::tempdir().unwrap();
    let inbox = tmp.path().join("file-consumer-inbox");

    let _consumer_guard = ChildGuard::new(
        Command::new(agent_mesh_bin())
            .args([
                "node",
                "--hub",
                &ws_url,
                "--id",
                "file-consumer",
                "--platform",
                "macos",
                "--inbox-dir",
                inbox.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn consumer node"),
    );
    wait_for_node_registered(&http_base, "file-consumer").await;

    // 1. Binary payload containing NUL (0x00), 0x80, and 0xFF via --payload-file.
    let bin_bytes: Vec<u8> = [0x00u8, 0x80, 0xff, 0xfe, 0x01, 0x42].repeat(16);
    let bin_sha = compute_sha256(&bin_bytes);
    let bin_path = tmp.path().join("binary_payload.bin");
    std::fs::write(&bin_path, &bin_bytes).unwrap();

    let bin_out = Command::new(agent_mesh_bin())
        .args([
            "data",
            "--hub",
            &ws_url,
            "--from",
            "cli-bin-sender",
            "--to",
            "file-consumer",
            "--payload-file",
            bin_path.to_str().unwrap(),
            "--timeout-ms",
            "5000",
        ])
        .output()
        .expect("run agent-mesh data --payload-file binary");
    assert!(
        bin_out.status.success(),
        "binary --payload-file failed: stderr={}",
        String::from_utf8_lossy(&bin_out.stderr)
    );
    let bin_stdout = String::from_utf8_lossy(&bin_out.stdout);
    assert!(
        bin_stdout.contains("Correlation ID:")
            && bin_stdout.contains(&bin_sha)
            && bin_stdout.contains("Chunks: 1"),
        "unexpected binary stdout: {bin_stdout}"
    );

    // 2. >1 MiB multi-chunk binary payload via --payload-file (exceeds macOS ARG_MAX).
    let multi_bytes: Vec<u8> = (0..(DEFAULT_DATA_CHUNK_BYTES + 16_384))
        .map(|i| (i % 256) as u8)
        .collect();
    let multi_sha = compute_sha256(&multi_bytes);
    let multi_path = tmp.path().join("multichunk_payload.bin");
    std::fs::write(&multi_path, &multi_bytes).unwrap();

    let multi_out = Command::new(agent_mesh_bin())
        .args([
            "data",
            "--hub",
            &ws_url,
            "--from",
            "cli-multi-sender",
            "--to",
            "file-consumer",
            "--payload-file",
            multi_path.to_str().unwrap(),
            "--timeout-ms",
            "15000",
        ])
        .output()
        .expect("run agent-mesh data --payload-file >1MiB");
    assert!(
        multi_out.status.success(),
        ">1 MiB --payload-file failed: stderr={}",
        String::from_utf8_lossy(&multi_out.stderr)
    );
    let multi_stdout = String::from_utf8_lossy(&multi_out.stdout);
    assert!(
        multi_stdout.contains("Correlation ID:")
            && multi_stdout.contains(&multi_sha)
            && multi_stdout.contains("Chunks: 2"),
        "unexpected multi-chunk stdout: {multi_stdout}"
    );

    // 3. Raw binary payload piped over stdin via `--payload-file -`.
    let stdin_bytes: Vec<u8> = b"\x00\x80\xffraw-stdin-bytes\xfe\x00\xff".to_vec();
    let stdin_sha = compute_sha256(&stdin_bytes);
    let mut stdin_child = Command::new(agent_mesh_bin())
        .args([
            "data",
            "--hub",
            &ws_url,
            "--from",
            "cli-stdin-sender",
            "--to",
            "file-consumer",
            "--payload-file",
            "-",
            "--timeout-ms",
            "5000",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn agent-mesh data --payload-file -");
    {
        let mut stdin_pipe = stdin_child.stdin.take().expect("stdin pipe");
        stdin_pipe.write_all(&stdin_bytes).unwrap();
    }
    let stdin_out = stdin_child.wait_with_output().unwrap();
    assert!(
        stdin_out.status.success(),
        "stdin --payload-file - failed: stderr={}",
        String::from_utf8_lossy(&stdin_out.stderr)
    );
    let stdin_stdout = String::from_utf8_lossy(&stdin_out.stdout);
    assert!(
        stdin_stdout.contains("Correlation ID:")
            && stdin_stdout.contains(&stdin_sha)
            && stdin_stdout.contains("Chunks: 1"),
        "unexpected stdin stdout: {stdin_stdout}"
    );

    // 4. Verify exact persisted bytes on disk (zero .tmp* leftovers).
    let entries: Vec<_> = std::fs::read_dir(&inbox)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert_eq!(entries.len(), 3, "expected 3 persisted files in inbox");
    let mut persisted_by_sha: std::collections::HashMap<String, Vec<u8>> =
        std::collections::HashMap::new();
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        assert!(!name.starts_with(".tmp_"), "unexpected temp file {name}");
        let raw = std::fs::read(entry.path()).unwrap();
        persisted_by_sha.insert(compute_sha256(&raw), raw);
    }
    assert_eq!(persisted_by_sha.get(&bin_sha), Some(&bin_bytes));
    assert_eq!(persisted_by_sha.get(&multi_sha), Some(&multi_bytes));
    assert_eq!(persisted_by_sha.get(&stdin_sha), Some(&stdin_bytes));
}

#[tokio::test]
async fn test_chunk_edge_cases_duplicate_seq_inconsistent_total_and_oob_seq() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::protocol::Message as WsMsg;

    let tmp = tempfile::tempdir().unwrap();
    let inbox = agent_mesh::validate_inbox_dir(&tmp.path().join("chunk-edge-inbox")).unwrap();

    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("start hub");
    let ws_url = format!("ws://127.0.0.1:{port}/ws");

    let consumer = AgentMeshClient::new(&ws_url, "chunk-consumer", "linux").with_inbox_dir(&inbox);
    consumer.connect().await.unwrap();

    let (mut raw_ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
    let reg = serde_json::json!({
        "type": "NodeRegistration",
        "correlation_id": "reg-edge-sender",
        "from": "edge-sender",
        "to": "hub",
        "platform": "linux",
    });
    raw_ws.send(WsMsg::Text(reg.to_string())).await.unwrap();
    let _reg_ack = raw_ws.next().await.unwrap().unwrap();

    // 1. Duplicate chunk sequence (0, 0, 1) reassembles cleanly to DeliveryAck and persists exact bytes.
    let dup_payload = b"hello-chunked-dup";
    let (dup_chunks, dup_corr, dup_sha) =
        AgentMeshEnvelope::new_data_payload_chunks("edge-sender", "chunk-consumer", dup_payload, 8);
    assert_eq!(dup_chunks.len(), 3);
    raw_ws
        .send(WsMsg::Text(dup_chunks[0].to_json_string().unwrap()))
        .await
        .unwrap();
    raw_ws
        .send(WsMsg::Text(dup_chunks[0].to_json_string().unwrap()))
        .await
        .unwrap();
    raw_ws
        .send(WsMsg::Text(dup_chunks[1].to_json_string().unwrap()))
        .await
        .unwrap();
    raw_ws
        .send(WsMsg::Text(dup_chunks[2].to_json_string().unwrap()))
        .await
        .unwrap();

    let ack_env = next_final_reply(&mut raw_ws).await;
    match ack_env {
        AgentMeshEnvelope::DeliveryAck {
            correlation_id,
            status,
            ..
        } => {
            assert_eq!(correlation_id, dup_corr);
            assert_eq!(status, "data_received_verified");
        }
        other => panic!("Expected DeliveryAck for duplicate chunk sequence, got {other:?}"),
    }
    let files: Vec<_> = std::fs::read_dir(&inbox)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert_eq!(files.len(), 1);
    let persisted = std::fs::read(files[0].path()).unwrap();
    assert_eq!(persisted, dup_payload);
    assert_eq!(compute_sha256(&persisted), dup_sha);

    // 2. Inconsistent total_chunks (chunk 0 claims 2, chunk 1 claims 3) must return DeliveryNack(ERR_PAYLOAD_CORRUPT).
    let (mut bad_total_chunks, bad_total_corr, _) =
        AgentMeshEnvelope::new_data_payload_chunks("edge-sender", "chunk-consumer", b"abcd", 2);
    assert_eq!(bad_total_chunks.len(), 2);
    if let AgentMeshEnvelope::DataPayload {
        ref mut total_chunks,
        ..
    } = bad_total_chunks[1]
    {
        *total_chunks = 3;
    }
    for env in bad_total_chunks {
        raw_ws
            .send(WsMsg::Text(env.to_json_string().unwrap()))
            .await
            .unwrap();
    }
    let nack_total_env = next_final_reply(&mut raw_ws).await;
    match nack_total_env {
        AgentMeshEnvelope::DeliveryNack {
            correlation_id,
            error_code,
            ..
        } => {
            assert_eq!(correlation_id, bad_total_corr);
            assert_eq!(error_code, ERR_PAYLOAD_CORRUPT);
        }
        other => {
            panic!("Expected DeliveryNack(ERR_PAYLOAD_CORRUPT) for inconsistent total_chunks, got {other:?}")
        }
    }

    // 3. Out-of-range sequence_number (sequence_number=2 >= total_chunks=2) must return DeliveryNack(ERR_PAYLOAD_CORRUPT).
    let (mut oob_chunks, oob_corr, _) =
        AgentMeshEnvelope::new_data_payload_chunks("edge-sender", "chunk-consumer", b"wxyz", 2);
    assert_eq!(oob_chunks.len(), 2);
    if let AgentMeshEnvelope::DataPayload {
        ref mut sequence_number,
        ..
    } = oob_chunks[1]
    {
        *sequence_number = 2;
    }
    raw_ws
        .send(WsMsg::Text(oob_chunks[1].to_json_string().unwrap()))
        .await
        .unwrap();
    let nack_oob_env = next_final_reply(&mut raw_ws).await;
    match nack_oob_env {
        AgentMeshEnvelope::DeliveryNack {
            correlation_id,
            error_code,
            ..
        } => {
            assert_eq!(correlation_id, oob_corr);
            assert_eq!(error_code, ERR_PAYLOAD_CORRUPT);
        }
        other => panic!(
            "Expected DeliveryNack(ERR_PAYLOAD_CORRUPT) for out-of-range sequence_number, got {other:?}"
        ),
    }

    // Verify neither rejected transfer created any new file in inbox.
    assert_eq!(std::fs::read_dir(&inbox).unwrap().count(), 1);

    let _ = raw_ws.close(None).await;
    consumer.close().await;
    hub.stop().await;
}

fn data_payload_parts(env: &AgentMeshEnvelope) -> (String, String, u32, u32) {
    match env {
        AgentMeshEnvelope::DataPayload {
            correlation_id,
            from,
            sequence_number,
            total_chunks,
            ..
        } => (
            correlation_id.clone(),
            from.clone(),
            *sequence_number,
            *total_chunks,
        ),
        other => panic!("expected DataPayload, got {other:?}"),
    }
}

/// Item 1 of the MESH-2 review (design 0107 D2): the sender accepts only acknowledgements from the
/// target. A third node that learned the correlation id can neither confirm nor fail the transfer.
#[tokio::test]
async fn test_sender_ignores_acks_and_nacks_from_a_node_other_than_the_target() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("start hub");
    let ws_url = format!("ws://127.0.0.1:{port}/ws");

    let mut target = raw_node(&ws_url, "honest-target").await;
    let impostor = AgentMeshClient::new(&ws_url, "impostor", "linux");
    impostor.connect().await.unwrap();
    let sender = std::sync::Arc::new(AgentMeshClient::new(&ws_url, "ack-sender", "macos"));
    sender.connect().await.unwrap();

    // Case A: a forged "verified" ACK, then the target's NACK. The transfer must fail.
    let s = sender.clone();
    let task_a = tokio::spawn(async move {
        s.send_data_payload_with_ack("honest-target", b"payload-a", Some(60_000))
            .await
    });
    let (corr_a, from_a, _, _) = data_payload_parts(&next_envelope(&mut target).await);
    assert_eq!(from_a, "ack-sender");
    impostor
        .send_envelope(AgentMeshEnvelope::new_delivery_ack(
            "impostor",
            "ack-sender",
            &corr_a,
            ACK_STATUS_DATA_VERIFIED,
        ))
        .await
        .unwrap();
    // The hub handles one socket's frames in order, so once the impostor's query is answered the
    // forged ACK is already in the sender's queue, ahead of the target's NACK below.
    impostor.query_node_list(Some(60_000)).await.unwrap();
    raw_send(
        &mut target,
        &AgentMeshEnvelope::new_delivery_nack(
            "honest-target",
            "ack-sender",
            &corr_a,
            ERR_PAYLOAD_CORRUPT,
            "refused by the real target",
        ),
    )
    .await;
    let err = task_a
        .await
        .unwrap()
        .expect_err("a forged ACK from another node must not confirm delivery");
    assert!(err.to_string().contains(ERR_PAYLOAD_CORRUPT), "{err}");

    // Case B: a forged NACK, then the target's verified ACK. The transfer must succeed.
    let s = sender.clone();
    let task_b = tokio::spawn(async move {
        s.send_data_payload_with_ack("honest-target", b"payload-b", Some(60_000))
            .await
    });
    let (corr_b, _, _, _) = data_payload_parts(&next_envelope(&mut target).await);
    impostor
        .send_envelope(AgentMeshEnvelope::new_delivery_nack(
            "impostor",
            "ack-sender",
            &corr_b,
            ERR_PAYLOAD_CORRUPT,
            "forged refusal",
        ))
        .await
        .unwrap();
    impostor.query_node_list(Some(60_000)).await.unwrap();
    raw_send(
        &mut target,
        &AgentMeshEnvelope::new_delivery_ack(
            "honest-target",
            "ack-sender",
            &corr_b,
            ACK_STATUS_DATA_VERIFIED,
        ),
    )
    .await;
    let receipt = task_b
        .await
        .unwrap()
        .expect("a forged NACK from another node must not fail the transfer");
    assert_eq!(receipt.responder, "honest-target");
    assert_eq!(receipt.status, ACK_STATUS_DATA_VERIFIED);

    sender.close().await;
    impostor.close().await;
    hub.stop().await;
}

/// Item 1 of the MESH-2 review: a consumer that answers the first chunk with
/// `data_received_verified` (as the old Python node did for every chunk) has not verified the
/// payload, and the sender must not report delivery.
#[tokio::test]
async fn test_sender_refuses_a_receipt_that_does_not_cover_every_chunk() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("start hub");
    let ws_url = format!("ws://127.0.0.1:{port}/ws");

    let sender = std::sync::Arc::new(AgentMeshClient::new(&ws_url, "chunk-sender", "macos"));
    sender.connect().await.unwrap();

    // One fresh target per round, so chunks still in flight from one round never reach the next.
    for total in [2usize, 3] {
        let target_id = format!("per-chunk-acker-{total}");
        let mut target = raw_node(&ws_url, &target_id).await;
        let payload = vec![b'z'; 4 * total];
        let s = sender.clone();
        let to = target_id.clone();
        let task = tokio::spawn(async move {
            s.send_data_payload_chunked_with_ack(&to, &payload, 4, Some(60_000))
                .await
        });
        let (corr, _, seq, chunks) = data_payload_parts(&next_envelope(&mut target).await);
        assert_eq!((seq, chunks as usize), (0, total));
        raw_send(
            &mut target,
            &AgentMeshEnvelope::new_delivery_ack(
                &target_id,
                "chunk-sender",
                &corr,
                ACK_STATUS_DATA_VERIFIED,
            ),
        )
        .await;
        let err = task
            .await
            .unwrap()
            .expect_err("a receipt for chunk 0 is not a receipt for the payload");
        let msg = err.to_string();
        assert!(msg.contains("confirmed"), "{total} chunks: {msg}");
    }

    sender.close().await;
    hub.stop().await;
}

/// Item 3 of the MESH-2 review (design 0107 D3): the sender keeps at most `DATA_CHUNK_WINDOW`
/// chunks unacknowledged, so a slow consumer cannot be pushed past the hub's queue.
#[tokio::test]
async fn test_sender_paces_chunks_by_the_consumers_acknowledgements() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("start hub");
    let ws_url = format!("ws://127.0.0.1:{port}/ws");

    let mut consumer = raw_node(&ws_url, "slow-consumer").await;
    let sender = AgentMeshClient::new(&ws_url, "paced-sender", "macos");
    sender.connect().await.unwrap();

    let total = DATA_CHUNK_WINDOW + 3;
    let payload: Vec<u8> = (0..(8 * total)).map(|i| i as u8).collect();
    let expected_sha = compute_sha256(&payload);
    let task = tokio::spawn(async move {
        let res = sender
            .send_data_payload_chunked_with_ack("slow-consumer", &payload, 8, Some(60_000))
            .await;
        sender.close().await;
        res
    });

    let mut acked = 0usize;
    let mut next_seq = 0usize;
    let mut corr = String::new();
    while next_seq < total {
        // Take the whole window the sender is allowed to have in flight.
        while next_seq < acked + DATA_CHUNK_WINDOW && next_seq < total {
            let (c, _, seq, _) = data_payload_parts(&next_envelope(&mut consumer).await);
            assert_eq!(seq as usize, next_seq);
            corr = c;
            next_seq += 1;
        }
        if next_seq < total {
            // The window is full: nothing more may arrive until a chunk is acknowledged. (A
            // correct sender passes this however slow the runner is.)
            let extra = tokio::time::timeout(
                Duration::from_millis(300),
                futures::StreamExt::next(&mut consumer),
            )
            .await;
            assert!(
                extra.is_err(),
                "sender exceeded the window of {DATA_CHUNK_WINDOW}: {extra:?}"
            );
        }
        // Acknowledge one chunk (never the last one: it gets the final receipt).
        if acked < total - 1 {
            raw_send(
                &mut consumer,
                &AgentMeshEnvelope::new_delivery_ack(
                    "slow-consumer",
                    "paced-sender",
                    &corr,
                    ACK_STATUS_CHUNK_RECEIVED,
                ),
            )
            .await;
            acked += 1;
        }
    }
    while acked < total - 1 {
        raw_send(
            &mut consumer,
            &AgentMeshEnvelope::new_delivery_ack(
                "slow-consumer",
                "paced-sender",
                &corr,
                ACK_STATUS_CHUNK_RECEIVED,
            ),
        )
        .await;
        acked += 1;
    }
    raw_send(
        &mut consumer,
        &AgentMeshEnvelope::new_delivery_ack(
            "slow-consumer",
            "paced-sender",
            &corr,
            ACK_STATUS_DATA_VERIFIED,
        ),
    )
    .await;
    let receipt = task.await.unwrap().expect("paced transfer succeeds");
    assert_eq!(receipt.total_chunks as usize, total);
    assert_eq!(receipt.sha256, expected_sha);

    hub.stop().await;
}

/// Item 3 of the MESH-2 review: a transfer much larger than the consumer's hub queue goes through,
/// because only the window is ever queued. The chunks are all NUL bytes, the worst JSON expansion.
#[tokio::test]
async fn test_transfer_larger_than_the_hub_queue_is_delivered() {
    let chunk = 64 * 1024;
    let cfg = HubConfig {
        max_queued_bytes: DATA_CHUNK_WINDOW * agent_mesh::max_chunk_json_bytes(chunk) + 64 * 1024,
        ..HubConfig::default()
    };
    let hub = AgentMeshHub::with_config(cfg);
    let port = hub.start("127.0.0.1:0").await.expect("start hub");
    let ws_url = format!("ws://127.0.0.1:{port}/ws");

    let consumer = AgentMeshClient::new(&ws_url, "queue-consumer", "linux");
    let sender = AgentMeshClient::new(&ws_url, "queue-sender", "macos");
    consumer.connect().await.unwrap();
    sender.connect().await.unwrap();

    let payload = vec![0u8; chunk * 24];
    let receipt = sender
        .send_data_payload_chunked_with_ack("queue-consumer", &payload, chunk, Some(60_000))
        .await
        .expect("a paced transfer must not get the consumer kicked");
    assert_eq!(receipt.total_chunks, 24);
    assert!(consumer.is_connected(), "consumer was disconnected");
    let received = consumer.received_data_payloads().await;
    assert_eq!(received.len(), 1);

    sender.close().await;
    consumer.close().await;
    hub.stop().await;
}

/// Item 2 of the MESH-2 review: `--inbox-max-files` / `--inbox-max-mb` refuse payloads with
/// `ERR_INBOX_FULL` and the sender CLI exits non-zero.
#[tokio::test]
async fn test_inbox_quota_flags_refuse_payloads_with_err_inbox_full() {
    let (_hub_guard, _port, ws_url, http_base) = spawn_hub_process();
    let tmp = tempfile::tempdir().unwrap();
    let inbox = tmp.path().join("quota-inbox");
    let _consumer_guard = ChildGuard::new(
        Command::new(agent_mesh_bin())
            .args([
                "node",
                "--hub",
                &ws_url,
                "--id",
                "quota-consumer",
                "--inbox-dir",
                inbox.to_str().unwrap(),
                "--inbox-max-files",
                "1",
                "--inbox-max-mb",
                "1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn quota consumer"),
    );
    wait_for_node_registered(&http_base, "quota-consumer").await;

    let send = |payload: &str| {
        Command::new(agent_mesh_bin())
            .args([
                "data",
                "--hub",
                &ws_url,
                "--from",
                "quota-sender",
                "--to",
                "quota-consumer",
                "--payload",
                payload,
                "--timeout-ms",
                "60000",
            ])
            .output()
            .expect("run agent-mesh data")
    };
    let first = send("fits");
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let second = send("does-not-fit");
    assert!(!second.status.success(), "the second file must be refused");
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains(ERR_INBOX_FULL), "{stderr}");
    assert!(stderr.contains("--inbox-max-files"), "{stderr}");

    let names: Vec<String> = std::fs::read_dir(&inbox)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names.len(), 1, "{names:?}");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(inbox.join(&names[0]))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "inbox file mode {mode:o}");
        let dir_mode = std::fs::metadata(&inbox).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "created inbox dir mode {dir_mode:o}");
    }
}
