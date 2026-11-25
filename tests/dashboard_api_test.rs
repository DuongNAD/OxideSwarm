//! Dashboard REST/stream API behaviour against a real `MasterServer` dashboard listener.
//!
//! * `POST /api/tasks` answers malformed input with 400 (it used to run a default `echo` and report
//!   success) and honours `gpu_required`, `timeout_secs`, `cores` and `ram_mb`.
//! * `GET /api/tasks` is ordered newest first with a real limit and offset.
//! * `GET /api/status` ships a capped `active_list`, spells task states like the other endpoints
//!   (snake_case, `TaskState::as_str`), and publishes `discovery_hosts` to authenticated callers only.
//! * WebSocket/SSE clients are capped, oversized WebSocket frames are refused, and snapshot requests
//!   are spaced out.
//!
//! The dashboard denies unauthenticated requests by default; these tests opt in to the local dev
//! bypass (`AuthConfig::dev()`) where they need an anonymous local caller.

use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

use rusty_grid_core::noise_auth::{
    compute_enroll_proof, derive_pairing_code, invite_payload_check_hex, v3_client_handshake,
    v3_client_handshake_with_hash, HandshakePurpose, MemberRole, NodeKeypair, PublicKey,
};
use rusty_grid_core::protocol::{
    ClientMessage, ClientResponse, EndpointRole, ProtocolCapabilities, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_core::{MasterMessage, MessageTransport, WorkerCapabilities, WorkerMessage};
use rusty_grid_master::auth::{AuthConfig, AuthScope};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::server::{AuthMode, MasterHandle, MasterServer, MemberStore, ServerConfig};

async fn spawn_master(auth: AuthConfig) -> (MasterHandle, SocketAddr) {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth);
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let addr = master.dashboard_addr().expect("dashboard addr");
    (master, addr)
}

fn client() -> reqwest::Client {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

fn echo_task(label: &str) -> Task {
    Task::new(
        TaskSpec::command("echo", vec![label.to_string()]),
        TaskRequirements::generic(1, 10),
    )
}

/// Submits `n` queued tasks (no worker is connected, so they stay queued) and returns their ids
/// oldest first.
async fn submit_many(master: &MasterHandle, n: usize) -> Vec<TaskId> {
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        ids.push(
            master
                .submit_task(echo_task(&format!("t{i}")))
                .await
                .unwrap(),
        );
    }
    ids
}

// ---------------------------------------------------------------------------------------------
// POST /api/tasks
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_post_tasks_rejects_malformed_input_with_400_and_runs_nothing() {
    let (master, addr) = spawn_master(AuthConfig::dev()).await;
    let url = format!("http://{addr}/api/tasks");
    let client = client();

    let cases: Vec<(&str, &str, &str)> = vec![
        ("not json at all", "application/json", "garbage body"),
        ("", "application/json", "empty body"),
        (
            r#"{"cmd":"nvidia-smi"}"#,
            "application/json",
            "misspelt field",
        ),
        (
            r#"{"command":"x","gpu":true}"#,
            "application/json",
            "unknown field",
        ),
        (r#"{}"#, "application/json", "no command"),
        (r#"{"command":""}"#, "application/json", "empty command"),
        (r#"{"command":"   "}"#, "application/json", "blank command"),
        (r#"{"command":123}"#, "application/json", "wrong type"),
        (
            r#"{"command":"x","timeout_secs":0}"#,
            "application/json",
            "zero timeout",
        ),
        (
            r#"{"command":"x","cores":0}"#,
            "application/json",
            "zero cores",
        ),
        (
            r#"{"command":"x","timeout_secs":-5}"#,
            "application/json",
            "negative timeout",
        ),
        (
            r#"{"command":"echo hi"}"#,
            "text/plain",
            "wrong content type",
        ),
    ];
    for (body, content_type, what) in cases {
        let resp = client
            .post(&url)
            .header("content-type", content_type)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{what}: {body:?}");
        let json: Value = resp
            .json()
            .await
            .unwrap_or_else(|e| panic!("{what}: error body must be JSON: {e}"));
        assert!(
            json["message"].as_str().is_some_and(|m| !m.is_empty()),
            "{what}: the reply must say what was wrong: {json}"
        );
    }

    assert_eq!(
        master.queue_stats().await.unwrap().total,
        0,
        "a rejected request must not have queued anything (it used to queue a default `echo`)"
    );
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_post_tasks_honours_gpu_timeout_cores_and_ram() {
    let (master, addr) = spawn_master(AuthConfig::dev()).await;
    let url = format!("http://{addr}/api/tasks");
    let client = client();

    let resp = client
        .post(&url)
        .json(&serde_json::json!({
            "command": "python3 train.py",
            "gpu_required": true,
            "timeout_secs": 3600,
            "cores": 2,
            "ram_mb": 1024
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let id = TaskId(Uuid::parse_str(body["task_id"].as_str().unwrap()).unwrap());

    let task = master
        .queue()
        .get_task_spec(&id)
        .await
        .expect("queued task");
    assert_eq!(task.requirements.timeout_secs, 3600);
    assert_eq!(task.requirements.cpu_cores, 2);
    assert_eq!(task.requirements.ram_mb, 1024);
    assert!(task.requirements.gpu_required);
    assert!(
        matches!(&task.spec, TaskSpec::ShellScript { script, .. } if script == "python3 train.py")
    );
    assert!(master.get_task_info(id).await.unwrap().gpu_required);

    // The minimal body keeps the old defaults.
    let resp = client
        .post(&url)
        .json(&serde_json::json!({ "command": "echo hi" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let id = TaskId(Uuid::parse_str(body["task_id"].as_str().unwrap()).unwrap());
    let task = master.queue().get_task_spec(&id).await.unwrap();
    assert_eq!(task.requirements.timeout_secs, 60);
    assert_eq!(task.requirements.cpu_cores, 1);
    assert_eq!(task.requirements.ram_mb, 0);
    assert!(!task.requirements.gpu_required);

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// GET /api/tasks, GET /api/status
// ---------------------------------------------------------------------------------------------

fn ids_of(list: &Value) -> Vec<String> {
    list.as_array()
        .expect("array")
        .iter()
        .map(|t| t["task_id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn test_get_tasks_is_newest_first_with_a_real_limit_offset_and_cap() {
    let (master, addr) = spawn_master(AuthConfig::dev()).await;
    let base = format!("http://{addr}");
    let client = client();
    let ids: Vec<String> = submit_many(&master, 30)
        .await
        .iter()
        .map(|i| i.to_string())
        .collect();
    let newest_first: Vec<String> = ids.iter().rev().cloned().collect();

    let get = |query: &str| {
        let url = format!("{base}/api/tasks{query}");
        let client = client.clone();
        async move {
            client
                .get(url)
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        }
    };

    assert_eq!(
        ids_of(&get("?limit=5").await),
        newest_first[..5],
        "limit=N is the N newest"
    );
    assert_eq!(ids_of(&get("?limit=5&offset=5").await), newest_first[5..10]);
    assert_eq!(
        ids_of(&get("").await),
        newest_first,
        "30 tasks fit under the default limit"
    );
    assert_eq!(get("?limit=0").await.as_array().unwrap().len(), 0);
    assert_eq!(get("?offset=100").await.as_array().unwrap().len(), 0);
    assert_eq!(
        ids_of(&get("?state=Queued&limit=3").await),
        newest_first[..3]
    );
    assert!(get("?state=running").await.as_array().unwrap().is_empty());

    // Past the default and the hard cap.
    submit_many(&master, 1100).await;
    assert_eq!(
        get("").await.as_array().unwrap().len(),
        100,
        "default limit"
    );
    assert_eq!(
        get("?limit=5000").await.as_array().unwrap().len(),
        1000,
        "hard cap"
    );
    assert_eq!(
        get("?limit=18446744073709551615")
            .await
            .as_array()
            .unwrap()
            .len(),
        1000
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_status_caps_the_active_list_and_spells_states_like_the_task_endpoint() {
    let (master, addr) = spawn_master(AuthConfig::dev()).await;
    let base = format!("http://{addr}");
    let client = client();
    let ids = submit_many(&master, 150).await;
    let newest = *ids.last().unwrap();

    let status: Value = client
        .get(format!("{base}/api/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tasks = &status["tasks"];
    assert_eq!(tasks["queued"], 150);
    assert_eq!(tasks["active_total"], 150);
    let active = tasks["active_list"].as_array().unwrap();
    assert_eq!(active.len(), 100, "the list is capped, the counts are not");
    assert_eq!(active[0]["id"], newest.to_string(), "newest first");

    // One spelling everywhere: /api/status, /api/tasks, /api/tasks/:id.
    let detail: Value = client
        .get(format!("{base}/api/tasks/{newest}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["state"], "queued");
    assert_eq!(
        active[0]["state"], detail["state"],
        "/api/status and /api/tasks/:id must agree"
    );
    let listed: Value = client
        .get(format!("{base}/api/tasks?limit=1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed[0]["state"], detail["state"]);
    assert!(active.iter().all(|t| t["state"] == "queued"));

    // A terminal state is spelled the same way by the endpoint the SPA polls.
    master.cancel_task(newest).await.unwrap();
    let detail: Value = client
        .get(format!("{base}/api/tasks/{newest}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["state"], TaskState::Cancelled.as_str());
    assert_eq!(detail["state"], "cancelled");
    let status: Value = client
        .get(format!("{base}/api/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["tasks"]["cancelled"], 1);
    assert_eq!(status["tasks"]["active_total"], 149);

    let _ = master.shutdown();
}

async fn register_fake_worker(master: &MasterHandle, name: &str, remote: &str) {
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    // Keep the receiver alive for the test's duration.
    std::mem::forget(rx);
    master
        .registry()
        .register(
            Uuid::new_v4(),
            WorkerCapabilities::new(name, 4, 4096, false, false, None),
            remote.parse().unwrap(),
            tx,
            None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_status_publishes_discovery_hosts_to_authenticated_callers_only() {
    // Token-protected dashboard: the token holder sees the hosts, an anonymous caller sees a 401.
    let auth = AuthConfig {
        dev_mode: false,
        ..AuthConfig::default()
    }
    .with_token("observer-token", "obs", &[AuthScope::Observer]);
    let (master, addr) = spawn_master(auth).await;
    register_fake_worker(&master, "phone", "192.168.7.20:50000").await;
    register_fake_worker(&master, "pc", "192.168.7.21:50001").await;
    register_fake_worker(&master, "local", "127.0.0.1:50002").await;
    let client = client();

    let anonymous = client
        .get(format!("http://{addr}/api/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    let status: Value = client
        .get(format!("http://{addr}/api/status"))
        .bearer_auth("observer-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        status["discovery_hosts"],
        serde_json::json!(["192.168.7.20:8080", "192.168.7.21:8080"]),
        "workers' standby portals, loopback left out"
    );
    let _ = master.shutdown();

    // The dev bypass is an authenticated (Admin) caller too.
    let (dev_master, dev_addr) = spawn_master(AuthConfig::dev()).await;
    register_fake_worker(&dev_master, "phone", "192.168.7.20:50000").await;
    let status: Value = client
        .get(format!("http://{dev_addr}/api/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        status["discovery_hosts"],
        serde_json::json!(["192.168.7.20:8080"])
    );
    let _ = dev_master.shutdown();

    // With authentication switched off there is no caller identity: the addresses are withheld.
    let open = AuthConfig {
        enabled: false,
        ..AuthConfig::default()
    };
    let (open_master, open_addr) = spawn_master(open).await;
    register_fake_worker(&open_master, "phone", "192.168.7.20:50000").await;
    let status: Value = client
        .get(format!("http://{open_addr}/api/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(status.get("discovery_hosts").is_none(), "{status}");
    let _ = open_master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------------------------

/// Sends `GET /api/stream/sse` over a raw socket and returns the open stream plus the status code.
async fn open_sse(addr: SocketAddr) -> (TcpStream, u16) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(
            format!(
                "GET /api/stream/sse HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte))
            .await
            .expect("response head")
            .expect("read");
        assert!(n > 0, "closed before the response head");
        head.push(byte[0]);
    }
    let status = String::from_utf8_lossy(&head)
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap();
    (stream, status)
}

#[tokio::test]
async fn test_open_streams_are_capped() {
    let (master, addr) = spawn_master(AuthConfig::dev()).await;
    let limit = rusty_grid_master::web_ui::MAX_STREAM_CLIENTS;

    let mut open = Vec::new();
    for i in 0..limit {
        let (stream, status) = open_sse(addr).await;
        assert_eq!(status, 200, "stream {i} is within the limit");
        open.push(stream);
    }
    let (_refused, status) = open_sse(addr).await;
    assert_eq!(status, 503, "stream {} is over the limit", limit + 1);

    // A WebSocket is refused the same way (the upgrade is answered with 503, not 101).
    match connect_async(format!("ws://{addr}/ws")).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            assert_eq!(resp.status(), 503);
        }
        other => panic!("a WebSocket over the limit must be refused with 503, got {other:?}"),
    }

    drop(open);
    let _ = master.shutdown();
}

async fn next_snapshot_frames(
    read: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
    window: Duration,
) -> usize {
    let deadline = tokio::time::Instant::now() + window;
    let mut snapshots = 0;
    while let Ok(Some(Ok(msg))) = tokio::time::timeout_at(deadline, read.next()).await {
        if let Message::Text(text) = msg {
            if text.contains("\"Snapshot\"") {
                snapshots += 1;
            }
        }
    }
    snapshots
}

#[tokio::test]
async fn test_websocket_frames_are_size_limited_and_snapshot_requests_are_spaced() {
    let (master, addr) = spawn_master(AuthConfig::dev()).await;

    // An oversized frame ends the connection instead of being buffered and parsed.
    let (mut ws, _) = connect_async(format!("ws://{addr}/ws")).await.unwrap();
    let first = ws.next().await.unwrap().unwrap();
    assert!(first.to_text().unwrap().contains("Snapshot"));
    ws.send(Message::Text("x".repeat(64 * 1024))).await.ok();
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return true,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await
    .expect("the server must drop a client that sends an oversized frame");
    assert!(closed);

    // A client hammering `snapshot` gets at most one extra snapshot per interval, not one per request.
    let (ws, _) = connect_async(format!("ws://{addr}/ws")).await.unwrap();
    let (mut write, mut read) = ws.split();
    for _ in 0..10 {
        write
            .send(Message::Text(r#"{"action":"snapshot"}"#.into()))
            .await
            .unwrap();
    }
    let received = next_snapshot_frames(&mut read, Duration::from_millis(600)).await;
    assert!(
        (2..=3).contains(&received),
        "initial + first requested snapshot, the rest spaced out; got {received} within 600 ms"
    );

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Members, Pending Enrollments, Active Connections & Invites (ENROLL-1d)
// ---------------------------------------------------------------------------------------------

async fn spawn_v3_master(
    auth_mode: AuthMode,
    auth: AuthConfig,
    session_resume_hold: Duration,
) -> (tempfile::TempDir, MasterHandle, SocketAddr, NodeKeypair) {
    let dir = tempfile::tempdir().expect("tempdir");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let master_kp = NodeKeypair::generate().unwrap();
    let members = MemberStore::load_from_dir(Some(dir.path())).unwrap();

    let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_state_dir(dir.path().to_path_buf())
        .with_auth_mode(auth_mode)
        .with_ephemeral_key(false)
        .with_master_noise_key(master_kp.clone())
        .with_members(members)
        .with_session_resume_hold(session_resume_hold)
        .with_dashboard_port(0)
        .with_auth_config(auth);
    config.enable_discovery = false;
    #[cfg(feature = "p2p")]
    {
        config.enable_p2p = false;
    }

    let master = MasterServer::spawn(config).await.expect("spawn v3 master");
    let addr = master.dashboard_addr().expect("dashboard addr");
    (dir, master, addr, master_kp)
}

fn decode_base64url(input: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf: u32 = 0;
    let mut bits: u8 = 0;
    for b in input.bytes() {
        let val = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            other => panic!("unexpected base64url byte {other}"),
        };
        buf = (buf << 6) | (val as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xff) as u8);
        }
    }
    out
}

fn parse_invite_token(token: &str) -> Value {
    let body = token
        .strip_prefix("oxs1-invite:")
        .expect("oxs1-invite prefix");
    let (payload_b64, check) = body.rsplit_once('.').expect("checksum separator");
    assert_eq!(
        invite_payload_check_hex(payload_b64),
        check,
        "invite checksum must match invite_payload_check_hex"
    );
    let json_bytes = decode_base64url(payload_b64);
    serde_json::from_slice(&json_bytes).expect("invite JSON payload")
}

fn enroll_member_for_test(
    members: &MemberStore,
    kp: &NodeKeypair,
    name: &str,
    roles: Vec<MemberRole>,
) {
    enroll_pubkey_for_test(members, *kp.public_key(), name, roles);
}

fn enroll_pubkey_for_test(
    members: &MemberStore,
    pk: PublicKey,
    name: &str,
    roles: Vec<MemberRole>,
) {
    let inv = members.create_invite(roles, 1, 3600, false).unwrap();
    let h = [0x33u8; 32];
    let proof = compute_enroll_proof(&inv.invite_secret.0, &h);
    members
        .handle_enroll_request(
            pk,
            &h,
            "1111-2222-3333-4444",
            "127.0.0.1:44000",
            &inv.invite_id,
            &proof,
            name.to_string(),
            "linux".to_string(),
            "x86_64".to_string(),
        )
        .unwrap();
}

fn deterministic_keys_with_same_4hex_prefix() -> (String, PublicKey, PublicKey, PublicKey) {
    let mut seen: std::collections::HashMap<String, PublicKey> = std::collections::HashMap::new();
    let mut pair: Option<(String, PublicKey, PublicKey)> = None;
    for i in 0u32..200_000 {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&i.to_le_bytes());
        let pk = PublicKey::from_bytes(bytes);
        let prefix = pk.member_id()[..4].to_string();
        if let Some(first) = seen.get(&prefix) {
            match pair {
                None => pair = Some((prefix, *first, pk)),
                Some((shared_prefix, pk_a, pk_b)) if shared_prefix != prefix => {
                    return (shared_prefix, pk_a, pk_b, pk);
                }
                Some(_) => {}
            }
        } else {
            seen.insert(prefix, pk);
        }
    }
    panic!("failed to find deterministic keys sharing a 4-hex member ID prefix");
}

#[tokio::test]
async fn test_members_list_active_connections_and_secret_mode_invite_refusal() {
    let client = client();

    // 1. In --auth secret mode, POST /api/invites refuses cleanly with 400.
    let (_dir_sec, secret_master, sec_addr, _) =
        spawn_v3_master(AuthMode::Secret, AuthConfig::dev(), Duration::from_secs(5)).await;
    let sec_resp = client
        .post(format!("http://{sec_addr}/api/invites"))
        .json(&serde_json::json!({ "roles": ["worker"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(sec_resp.status(), StatusCode::BAD_REQUEST);
    let sec_err: Value = sec_resp.json().await.unwrap();
    assert!(
        sec_err["message"]
            .as_str()
            .is_some_and(|m| m.contains("--auth secret")),
        "{sec_err}"
    );
    let _ = secret_master.shutdown();

    // 2. In --auth members mode, GET /api/members shows enrolled members + live TCP lanes.
    let auth = AuthConfig {
        dev_mode: false,
        ..AuthConfig::default()
    }
    .with_token("adm-tok", "admin", &[AuthScope::Admin]);
    let (_dir, master, dash_addr, master_kp) =
        spawn_v3_master(AuthMode::Members, auth, Duration::from_secs(5)).await;
    let master_pub = *master_kp.public_key();

    let kp_a = NodeKeypair::generate().unwrap();
    let kp_b = NodeKeypair::generate().unwrap();
    enroll_member_for_test(
        master.members(),
        &kp_a,
        "alpha",
        vec![MemberRole::Worker, MemberRole::Client],
    );
    enroll_member_for_test(master.members(), &kp_b, "beta", vec![MemberRole::Worker]);

    // Open both a v3 client TCP stream and a v3 worker TCP stream for alpha.
    let s_cli = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut t_cli = MessageTransport::new(s_cli);
    let sk_cli = v3_client_handshake(&mut t_cli, &kp_a, &master_pub, HandshakePurpose::Member)
        .await
        .unwrap();
    t_cli.enable_encryption(&sk_cli, EndpointRole::Client);
    t_cli.send_msg(&ClientMessage::Hello).await.unwrap();
    assert!(matches!(
        t_cli.recv_msg::<ClientResponse>().await.unwrap(),
        Some(ClientResponse::Hello { .. })
    ));

    let s_wrk = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut t_wrk = MessageTransport::new(s_wrk);
    let sk_wrk = v3_client_handshake(&mut t_wrk, &kp_a, &master_pub, HandshakePurpose::Member)
        .await
        .unwrap();
    t_wrk.enable_encryption(&sk_wrk, EndpointRole::Client);
    t_wrk
        .send_msg(&WorkerMessage::Register {
            worker_id: Uuid::new_v4(),
            capabilities: WorkerCapabilities::new("alpha-w", 2, 2048, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
    assert!(matches!(
        t_wrk.recv_msg::<MasterMessage>().await.unwrap(),
        Some(MasterMessage::RegisterAck { accepted: true, .. })
    ));

    let resp = client
        .get(format!("http://{dash_addr}/api/members"))
        .bearer_auth("adm-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body_text = resp.text().await.unwrap();
    assert!(!body_text.contains("pairing_code"), "{body_text}");
    assert!(!body_text.contains("invite_secret"), "{body_text}");
    let body: Value = serde_json::from_str(&body_text).unwrap();

    let members_arr = body["members"].as_array().unwrap();
    assert_eq!(members_arr.len(), 2);
    let alpha_entry = members_arr.iter().find(|m| m["name"] == "alpha").unwrap();
    assert_eq!(alpha_entry["fingerprint"], kp_a.fingerprint());
    assert!(body["pending"].as_array().unwrap().is_empty());

    let active = body["active_connections"].as_array().unwrap();
    let alpha_lanes: Vec<&str> = active
        .iter()
        .filter(|c| c["member_id"] == kp_a.member_id())
        .map(|c| c["lane"].as_str().unwrap())
        .collect();
    assert!(alpha_lanes.contains(&"client_tcp"), "{active:?}");
    assert!(alpha_lanes.contains(&"worker_tcp"), "{active:?}");

    drop(t_cli);
    drop(t_wrk);
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_invites_create_list_revoke_and_admin_rules() {
    let (_dir, master, addr, master_kp) =
        spawn_v3_master(AuthMode::Members, AuthConfig::dev(), Duration::from_secs(5)).await;
    let client = client();
    let base = format!("http://{addr}");

    // Admin invite rules: require_approval=false or max_uses != 1 must fail.
    let bad_admin_1 = client
        .post(format!("{base}/api/invites"))
        .json(&serde_json::json!({
            "roles": ["admin"],
            "require_approval": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_admin_1.status(), StatusCode::BAD_REQUEST);

    let bad_admin_2 = client
        .post(format!("{base}/api/invites"))
        .json(&serde_json::json!({
            "roles": ["admin"],
            "max_uses": 2
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_admin_2.status(), StatusCode::BAD_REQUEST);

    // Valid admin invite (defaults: max_uses=1, require_approval=true).
    let ok_admin = client
        .post(format!("{base}/api/invites"))
        .json(&serde_json::json!({ "roles": ["admin"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(ok_admin.status(), StatusCode::OK);
    let admin_json: Value = ok_admin.json().await.unwrap();
    assert_eq!(admin_json["max_uses"], 1);
    assert_eq!(admin_json["require_approval"], true);
    assert_eq!(admin_json["master_fingerprint"], master_kp.fingerprint());
    let parsed_admin = parse_invite_token(admin_json["invite"].as_str().unwrap());
    assert_eq!(parsed_admin["id"], admin_json["invite_id"]);
    assert_eq!(parsed_admin["mk"], master_kp.public_key().to_hex());

    // Worker invite with custom uses and empty-body defaults check.
    let ok_worker = client
        .post(format!("{base}/api/invites"))
        .json(&serde_json::json!({
            "roles": ["worker", "client"],
            "max_uses": 3,
            "expires_in_secs": 3600,
            "require_approval": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(ok_worker.status(), StatusCode::OK);
    let worker_json: Value = ok_worker.json().await.unwrap();
    let worker_inv_id = worker_json["invite_id"].as_str().unwrap().to_string();

    // GET /api/invites never exposes invite_secret or oxs1-invite: token.
    let list_resp = client
        .get(format!("{base}/api/invites"))
        .send()
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_text = list_resp.text().await.unwrap();
    assert!(!list_text.contains("invite_secret"), "{list_text}");
    assert!(!list_text.contains("oxs1-invite:"), "{list_text}");
    let list_json: Value = serde_json::from_str(&list_text).unwrap();
    assert_eq!(list_json["invites"].as_array().unwrap().len(), 2);

    // DELETE /api/invites/{id_prefix} revokes the invite; unknown ID returns 404.
    let del_resp = client
        .delete(format!("{base}/api/invites/{}", &worker_inv_id[..8]))
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.status(), StatusCode::OK);
    let del_json: Value = del_resp.json().await.unwrap();
    assert_eq!(del_json["revoked"], worker_inv_id);

    let list_after: Value = client
        .get(format!("{base}/api/invites"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let revoked_entry = list_after["invites"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["invite_id"] == worker_inv_id)
        .unwrap();
    assert_eq!(revoked_entry["revoked"], true);

    let not_found = client
        .delete(format!("{base}/api/invites/deadbeef"))
        .send()
        .await
        .unwrap();
    assert_eq!(not_found.status(), StatusCode::NOT_FOUND);

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_live_v3_enroll_approve_bad_code_and_reject() {
    let (_dir, master, dash_addr, master_kp) =
        spawn_v3_master(AuthMode::Members, AuthConfig::dev(), Duration::from_secs(5)).await;
    let client = client();
    let base = format!("http://{dash_addr}");
    let master_pub = *master_kp.public_key();

    // Create a 2-use approval-required worker invite via the dashboard API.
    let inv_resp: Value = client
        .post(format!("{base}/api/invites"))
        .json(&serde_json::json!({
            "roles": ["worker"],
            "max_uses": 2,
            "expires_in_secs": 3600,
            "require_approval": true
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let invite_payload = parse_invite_token(inv_resp["invite"].as_str().unwrap());
    let invite_id = invite_payload["id"].as_str().unwrap().to_string();
    let invite_secret_hex = invite_payload["s"].as_str().unwrap().to_string();
    let mut invite_secret = [0u8; 32];
    for i in 0..32 {
        invite_secret[i] = u8::from_str_radix(&invite_secret_hex[i * 2..i * 2 + 2], 16).unwrap();
    }

    // 1. Live joiner #1 connects via OXS-AUTH v3 HandshakePurpose::Enroll.
    let kp1 = NodeKeypair::generate().unwrap();
    let s1 = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut t1 = MessageTransport::new(s1);
    let (sk1, hh1) =
        v3_client_handshake_with_hash(&mut t1, &kp1, &master_pub, HandshakePurpose::Enroll)
            .await
            .unwrap();
    t1.enable_encryption(&sk1, EndpointRole::Client);
    let code1 = derive_pairing_code(&hh1).as_str().to_string();
    let proof1 = compute_enroll_proof(&invite_secret, &hh1);
    t1.send_msg(&ClientMessage::Enroll {
        invite_id: invite_id.clone(),
        proof: proof1,
        name: "joiner-one".into(),
        os: "linux".into(),
        arch: "x86_64".into(),
    })
    .await
    .unwrap();
    let pid1 = match t1.recv_msg::<ClientResponse>().await.unwrap().unwrap() {
        ClientResponse::EnrollPending { pending_id } => pending_id,
        other => panic!("expected EnrollPending, got {other:?}"),
    };

    // GET /api/members lists the pending entry WITHOUT pairing_code.
    let mem_text = client
        .get(format!("{base}/api/members"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!mem_text.contains("pairing_code"), "{mem_text}");
    assert!(
        !mem_text.contains(&code1),
        "pairing code must not leak in GET /api/members"
    );
    let mem_json: Value = serde_json::from_str(&mem_text).unwrap();
    assert_eq!(mem_json["pending"].as_array().unwrap().len(), 1);
    assert_eq!(mem_json["pending"][0]["pending_id"], pid1);

    // Wrong pairing code fails with 400 and leaves pending & uses_remaining intact.
    let wrong_code = if code1 == "000000" {
        "111111"
    } else {
        "000000"
    };
    let bad_app = client
        .post(format!("{base}/api/members/{pid1}/approve"))
        .json(&serde_json::json!({ "pairing_code": wrong_code }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_app.status(), StatusCode::BAD_REQUEST);

    // Correct pairing code + master fingerprint approves, sends Enrolled, and persists after Hello.
    let good_app = client
        .post(format!("{base}/api/members/{pid1}/approve"))
        .json(&serde_json::json!({
            "pairing_code": code1,
            "fingerprint": master_kp.fingerprint(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(good_app.status(), StatusCode::OK);
    let app_json: Value = good_app.json().await.unwrap();
    assert_eq!(app_json["id"], kp1.member_id());
    assert_eq!(app_json["name"], "joiner-one");

    let enrolled_msg = loop {
        match t1.recv_msg::<ClientResponse>().await.unwrap().unwrap() {
            ClientResponse::EnrollKeepalive => continue,
            other => break other,
        }
    };
    assert!(matches!(enrolled_msg, ClientResponse::Enrolled { .. }));
    t1.send_msg(&ClientMessage::Hello).await.unwrap();
    assert!(matches!(
        t1.recv_msg::<ClientResponse>().await.unwrap(),
        Some(ClientResponse::Hello { .. })
    ));
    drop(t1);

    // Invite now has 1 use remaining and joiner-one is enrolled.
    let invs_mid: Value = client
        .get(format!("{base}/api/invites"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(invs_mid["invites"][0]["uses_remaining"], 1);

    // 2. Live joiner #2 connects and is rejected via POST /api/members/{pid2}/reject.
    let kp2 = NodeKeypair::generate().unwrap();
    let s2 = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut t2 = MessageTransport::new(s2);
    let (sk2, hh2) =
        v3_client_handshake_with_hash(&mut t2, &kp2, &master_pub, HandshakePurpose::Enroll)
            .await
            .unwrap();
    t2.enable_encryption(&sk2, EndpointRole::Client);
    let proof2 = compute_enroll_proof(&invite_secret, &hh2);
    t2.send_msg(&ClientMessage::Enroll {
        invite_id: invite_id.clone(),
        proof: proof2,
        name: "joiner-two".into(),
        os: "macos".into(),
        arch: "aarch64".into(),
    })
    .await
    .unwrap();
    let pid2 = match t2.recv_msg::<ClientResponse>().await.unwrap().unwrap() {
        ClientResponse::EnrollPending { pending_id } => pending_id,
        other => panic!("expected EnrollPending, got {other:?}"),
    };

    let rej_resp = client
        .post(format!("{base}/api/members/{pid2}/reject"))
        .send()
        .await
        .unwrap();
    assert_eq!(rej_resp.status(), StatusCode::OK);
    let rej_json: Value = rej_resp.json().await.unwrap();
    assert_eq!(rej_json["rejected"], pid2);

    let rejected_msg = loop {
        match t2.recv_msg::<ClientResponse>().await.unwrap().unwrap() {
            ClientResponse::EnrollKeepalive => continue,
            other => break other,
        }
    };
    assert!(matches!(
        rejected_msg,
        ClientResponse::EnrollRejected { .. }
    ));

    // Invite still has 1 use remaining; joiner-two is not in members.
    let mem_final: Value = client
        .get(format!("{base}/api/members"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mem_final["members"].as_array().unwrap().len(), 1);
    let invs_final: Value = client
        .get(format!("{base}/api/invites"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(invs_final["invites"][0]["uses_remaining"], 1);

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_member_rename_and_revoke_closes_connections_and_requeues_tasks() {
    // Use a 60s session_resume_hold to prove dashboard revocation bypasses resume hold and
    // requeues immediately onto another worker.
    let (_dir, master, dash_addr, master_kp) = spawn_v3_master(
        AuthMode::Members,
        AuthConfig::dev(),
        Duration::from_secs(60),
    )
    .await;
    let client = client();
    let base = format!("http://{dash_addr}");
    let master_pub = *master_kp.public_key();

    let kp_rev = NodeKeypair::generate().unwrap();
    let kp_kept = NodeKeypair::generate().unwrap();
    enroll_member_for_test(
        master.members(),
        &kp_rev,
        "rev-node",
        vec![MemberRole::Worker, MemberRole::Client],
    );
    enroll_member_for_test(
        master.members(),
        &kp_kept,
        "kept-node",
        vec![MemberRole::Worker],
    );

    // Rename by ID and then by name; invalid name -> 400; unknown target -> 404.
    let bad_rename = client
        .post(format!("{base}/api/members/{}/rename", kp_rev.member_id()))
        .json(&serde_json::json!({ "name": "   " }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_rename.status(), StatusCode::BAD_REQUEST);

    let missing_rename = client
        .post(format!("{base}/api/members/no-such-member/rename"))
        .json(&serde_json::json!({ "name": "valid" }))
        .send()
        .await
        .unwrap();
    assert_eq!(missing_rename.status(), StatusCode::NOT_FOUND);

    let rename_1 = client
        .post(format!("{base}/api/members/{}/rename", kp_rev.member_id()))
        .json(&serde_json::json!({ "name": "rev-renamed" }))
        .send()
        .await
        .unwrap();
    assert_eq!(rename_1.status(), StatusCode::OK);
    let rename_2 = client
        .post(format!("{base}/api/members/rev-renamed/rename"))
        .json(&serde_json::json!({ "name": "rev-final" }))
        .send()
        .await
        .unwrap();
    assert_eq!(rename_2.status(), StatusCode::OK);

    // Connect a v3 client and a v3 worker for kp_rev, plus a v3 worker for kp_kept.
    let s_cli = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut t_cli = MessageTransport::new(s_cli);
    let sk_cli = v3_client_handshake(&mut t_cli, &kp_rev, &master_pub, HandshakePurpose::Member)
        .await
        .unwrap();
    t_cli.enable_encryption(&sk_cli, EndpointRole::Client);
    t_cli.send_msg(&ClientMessage::Hello).await.unwrap();
    assert!(matches!(
        t_cli.recv_msg::<ClientResponse>().await.unwrap(),
        Some(ClientResponse::Hello { .. })
    ));

    let wid_rev = Uuid::new_v4();
    let s_w_rev = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut t_w_rev = MessageTransport::new(s_w_rev);
    let sk_w_rev =
        v3_client_handshake(&mut t_w_rev, &kp_rev, &master_pub, HandshakePurpose::Member)
            .await
            .unwrap();
    t_w_rev.enable_encryption(&sk_w_rev, EndpointRole::Client);
    t_w_rev
        .send_msg(&WorkerMessage::Register {
            worker_id: wid_rev,
            capabilities: WorkerCapabilities::new("w-rev", 2, 2048, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
    assert!(matches!(
        t_w_rev.recv_msg::<MasterMessage>().await.unwrap(),
        Some(MasterMessage::RegisterAck { accepted: true, .. })
    ));

    // Submit a task while only w-rev is connected so w-rev definitely receives it first.
    let task_id = master
        .submit_task(echo_task("requeue-on-revoke"))
        .await
        .unwrap();

    // Wait for t_w_rev to receive AssignTask.
    let assigned_on_rev = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match t_w_rev.recv_msg::<MasterMessage>().await.unwrap() {
                Some(MasterMessage::AssignTask { task: t, .. }) => return t.id,
                Some(_) => continue,
                None => panic!("w-rev closed before AssignTask"),
            }
        }
    })
    .await
    .expect("w-rev assigned task");
    assert_eq!(assigned_on_rev, task_id);

    // Now connect w-kept so it can pick up the task as soon as kp_rev is revoked.
    let wid_kept = Uuid::new_v4();
    let s_w_kept = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut t_w_kept = MessageTransport::new(s_w_kept);
    let sk_w_kept = v3_client_handshake(
        &mut t_w_kept,
        &kp_kept,
        &master_pub,
        HandshakePurpose::Member,
    )
    .await
    .unwrap();
    t_w_kept.enable_encryption(&sk_w_kept, EndpointRole::Client);
    t_w_kept
        .send_msg(&WorkerMessage::Register {
            worker_id: wid_kept,
            capabilities: WorkerCapabilities::new("w-kept", 2, 2048, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
    assert!(matches!(
        t_w_kept.recv_msg::<MasterMessage>().await.unwrap(),
        Some(MasterMessage::RegisterAck { accepted: true, .. })
    ));

    // Revoke by display name ("rev-final") via dashboard API!
    let revoke_resp = client
        .post(format!("{base}/api/members/rev-final/revoke"))
        .send()
        .await
        .unwrap();
    assert_eq!(revoke_resp.status(), StatusCode::OK);
    let rev_json: Value = revoke_resp.json().await.unwrap();
    assert_eq!(rev_json["member_id"], kp_rev.member_id());
    assert!(
        rev_json["disconnected_sessions"].as_u64().unwrap() >= 2,
        "{rev_json}"
    );
    assert_eq!(rev_json["revoked_workers"], 1);

    // Both TCP connections of kp_rev close immediately.
    let cli_closed =
        tokio::time::timeout(Duration::from_secs(3), t_cli.recv_msg::<ClientResponse>())
            .await
            .expect("client stream must close promptly");
    assert!(
        matches!(cli_closed, Ok(None) | Err(_)),
        "expected closed client stream, got {cli_closed:?}"
    );
    let wrk_closed =
        tokio::time::timeout(Duration::from_secs(3), t_w_rev.recv_msg::<MasterMessage>())
            .await
            .expect("worker stream must close promptly");
    assert!(
        matches!(wrk_closed, Ok(None) | Err(_)),
        "expected closed worker stream, got {wrk_closed:?}"
    );

    // Task requeues immediately onto wid_kept (far faster than the 60s resume hold).
    let reassigned_on_kept = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match t_w_kept.recv_msg::<MasterMessage>().await.unwrap() {
                Some(MasterMessage::AssignTask { task: t, .. }) => return t.id,
                Some(_) => continue,
                None => panic!("w-kept closed unexpectedly"),
            }
        }
    })
    .await
    .expect("task must requeue onto kept worker immediately");
    assert_eq!(reassigned_on_kept, task_id);

    // Subsequent v3 handshake by revoked key is refused.
    let s_retry = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut t_retry = MessageTransport::new(s_retry);
    assert!(
        v3_client_handshake(&mut t_retry, &kp_rev, &master_pub, HandshakePurpose::Member)
            .await
            .is_err(),
        "revoked member must be refused at subsequent v3 handshake"
    );

    // GET /api/members no longer lists kp_rev.
    let mem_after: Value = client
        .get(format!("{base}/api/members"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let remaining_ids: Vec<&str> = mem_after["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert_eq!(remaining_ids, vec![kp_kept.member_id().as_str()]);

    drop(t_w_kept);
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_ambiguous_hex_prefix_returns_400_and_blocks_name_shadowing() {
    let (_dir, master, dash_addr, _master_kp) =
        spawn_v3_master(AuthMode::Members, AuthConfig::dev(), Duration::from_secs(5)).await;
    let client = client();
    let base = format!("http://{dash_addr}");

    let (shared_prefix, pk_a, pk_b, pk_shadower) = deterministic_keys_with_same_4hex_prefix();
    enroll_pubkey_for_test(master.members(), pk_a, "honest-a", vec![MemberRole::Worker]);
    enroll_pubkey_for_test(master.members(), pk_b, "honest-b", vec![MemberRole::Worker]);
    enroll_pubkey_for_test(
        master.members(),
        pk_shadower,
        &shared_prefix,
        vec![MemberRole::Worker],
    );

    // 1. With a third member whose display name equals the ambiguous 4-hex prefix,
    //    both rename and revoke must return 400 Bad Request (ambiguous), not 200.
    let rename_shadowed = client
        .post(format!("{base}/api/members/{shared_prefix}/rename"))
        .json(&serde_json::json!({ "name": "hijacked" }))
        .send()
        .await
        .unwrap();
    assert_eq!(rename_shadowed.status(), StatusCode::BAD_REQUEST);
    let rename_err: Value = rename_shadowed.json().await.unwrap();
    assert!(
        rename_err["message"]
            .as_str()
            .is_some_and(|m| m.contains("ambiguous")),
        "{rename_err}"
    );

    let revoke_shadowed = client
        .post(format!("{base}/api/members/{shared_prefix}/revoke"))
        .send()
        .await
        .unwrap();
    assert_eq!(revoke_shadowed.status(), StatusCode::BAD_REQUEST);
    let revoke_err: Value = revoke_shadowed.json().await.unwrap();
    assert!(
        revoke_err["message"]
            .as_str()
            .is_some_and(|m| m.contains("ambiguous")),
        "{revoke_err}"
    );

    // All 3 members remain untouched.
    let mem_before: Value = client
        .get(format!("{base}/api/members"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mem_before["members"].as_array().unwrap().len(), 3);
    let shadower_entry = mem_before["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["id"] == pk_shadower.member_id())
        .expect("shadower still present");
    assert_eq!(shadower_entry["name"], shared_prefix);

    // 2. Remove the shadower by full ID; ambiguous prefix with no matching display name must still
    //    return 400 Bad Request (not 404 Not Found) on both rename and revoke.
    let revoke_shadower = client
        .post(format!(
            "{base}/api/members/{}/revoke",
            pk_shadower.member_id()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(revoke_shadower.status(), StatusCode::OK);

    let rename_unmatched = client
        .post(format!("{base}/api/members/{shared_prefix}/rename"))
        .json(&serde_json::json!({ "name": "still-blocked" }))
        .send()
        .await
        .unwrap();
    assert_eq!(rename_unmatched.status(), StatusCode::BAD_REQUEST);
    let rename_unmatched_err: Value = rename_unmatched.json().await.unwrap();
    assert!(
        rename_unmatched_err["message"]
            .as_str()
            .is_some_and(|m| m.contains("ambiguous")),
        "{rename_unmatched_err}"
    );

    let revoke_unmatched = client
        .post(format!("{base}/api/members/{shared_prefix}/revoke"))
        .send()
        .await
        .unwrap();
    assert_eq!(revoke_unmatched.status(), StatusCode::BAD_REQUEST);
    let revoke_unmatched_err: Value = revoke_unmatched.json().await.unwrap();
    assert!(
        revoke_unmatched_err["message"]
            .as_str()
            .is_some_and(|m| m.contains("ambiguous")),
        "{revoke_unmatched_err}"
    );

    // Remaining 2 honest members remain untouched.
    let mem_after: Value = client
        .get(format!("{base}/api/members"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mem_after["members"].as_array().unwrap().len(), 2);

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_dashboard_exposes_worker_speed_in_status_and_workers_api() {
    use rusty_grid_core::task::TaskResult;

    let (master, addr) = spawn_master(AuthConfig::dev()).await;
    let client = client();
    let base = format!("http://{addr}");

    async fn connect_wire(
        master_addr: SocketAddr,
        name: &str,
        tag: &str,
    ) -> (
        Uuid,
        tokio::sync::mpsc::Sender<WorkerMessage>,
        tokio::sync::mpsc::Receiver<MasterMessage>,
        tokio::task::JoinHandle<()>,
    ) {
        let wid = Uuid::new_v4();
        let (tx_up, mut rx_up) = tokio::sync::mpsc::channel::<WorkerMessage>(16);
        let (tx_down, rx_down) = tokio::sync::mpsc::channel::<MasterMessage>(16);
        let stream = TcpStream::connect(master_addr).await.unwrap();
        let mut transport = MessageTransport::new(stream);
        transport
            .send_msg(&WorkerMessage::Register {
                worker_id: wid,
                capabilities: WorkerCapabilities::new(name, 4, 8192, false, false, None)
                    .with_tags(vec![tag.into()]),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        let ack: Option<MasterMessage> = transport.recv_msg().await.unwrap();
        assert!(matches!(
            ack,
            Some(MasterMessage::RegisterAck { accepted: true, .. })
        ));
        let h = tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = transport.recv_msg::<MasterMessage>() => {
                        match msg {
                            Ok(Some(m)) => {
                                if tx_down.send(m).await.is_err() {
                                    break;
                                }
                            }
                            _ => break,
                        }
                    }
                    up = rx_up.recv() => {
                        match up {
                            Some(m) => {
                                if transport.send_msg(&m).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });
        (wid, tx_up, rx_down, h)
    }

    let (w_fast, tx_fast, mut rx_fast, h_fast) =
        connect_wire(master.server_addr(), "dash-fast", "tier-fast").await;
    let (w_slow, tx_slow, mut rx_slow, h_slow) =
        connect_wire(master.server_addr(), "dash-slow", "tier-slow").await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if master.list_workers().await.unwrap_or_default().len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let shared_spec = TaskSpec::command("echo", vec!["bench".into()]);
    for (wid, tx, rx, tag, ms) in [
        (
            w_fast,
            &tx_fast,
            &mut rx_fast,
            "require:tag=tier-fast",
            100u64,
        ),
        (
            w_slow,
            &tx_slow,
            &mut rx_slow,
            "require:tag=tier-slow",
            250u64,
        ),
    ] {
        let tid = master
            .submit_task(
                Task::new(shared_spec.clone(), TaskRequirements::generic(1, 32))
                    .with_tags(vec![tag.into()]),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(MasterMessage::AssignTask { task }) = rx.recv().await {
                    if task.id == tid {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        tx.send(WorkerMessage::from(TaskResult::success(
            wid, tid, "ok", ms, false,
        )))
        .await
        .unwrap();
        assert!(master
            .wait_task(tid, Some(Duration::from_secs(10)))
            .await
            .unwrap()
            .is_success());
    }

    let status: Value = client
        .get(format!("{base}/api/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let s_workers = status["workers"].as_array().unwrap();
    let sf_fast = s_workers.iter().find(|w| w["name"] == "dash-fast").unwrap()["speed_factor"]
        .as_f64()
        .unwrap();
    let sf_slow = s_workers.iter().find(|w| w["name"] == "dash-slow").unwrap()["speed_factor"]
        .as_f64()
        .unwrap();
    assert!((sf_fast - 1.0).abs() < 1e-6, "got {sf_fast}");
    assert!((sf_slow - 2.5).abs() < 1e-6, "got {sf_slow}");

    let api_workers: Value = client
        .get(format!("{base}/api/workers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let aw = api_workers.as_array().unwrap();
    let fast_tags = aw
        .iter()
        .find(|w| w["capabilities"]["name"] == "dash-fast")
        .unwrap()["capabilities"]["tags"]
        .as_array()
        .unwrap();
    let slow_tags = aw
        .iter()
        .find(|w| w["capabilities"]["name"] == "dash-slow")
        .unwrap()["capabilities"]["tags"]
        .as_array()
        .unwrap();
    assert!(fast_tags.iter().any(|t| t == "speed:1.0"), "{fast_tags:?}");
    assert!(slow_tags.iter().any(|t| t == "speed:2.5"), "{slow_tags:?}");

    let html = client
        .get(format!("{base}/"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(html.contains("badge-speed") && html.contains("mSpeed"));

    h_fast.abort();
    h_slow.abort();
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_invites_http_validation_boundaries_and_observer_rbac() {
    let auth = AuthConfig {
        dev_mode: false,
        ..AuthConfig::default()
    }
    .with_token("admin-token-123456", "admin", &[AuthScope::Admin])
    .with_token("obs-token-123456", "obs", &[AuthScope::Observer]);
    let (_dir, master, addr, _) =
        spawn_v3_master(AuthMode::Members, auth, Duration::from_secs(5)).await;
    let client = client();
    let base = format!("http://{addr}");
    let admin = "Bearer admin-token-123456";
    let obs = "Bearer obs-token-123456";

    // Observer denied on admin-only member & invite endpoints (403)
    for path in ["/api/members", "/api/invites"] {
        let resp = client
            .get(format!("{base}{path}"))
            .header("Authorization", obs)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "GET {path}");
    }

    // Empty/whitespace body without Content-Type succeeds with defaults (200)
    for body in ["", "   "] {
        let resp = client
            .post(format!("{base}/api/invites"))
            .header("Authorization", admin)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // Non-empty body with non-JSON Content-Type -> 400
    let resp = client
        .post(format!("{base}/api/invites"))
        .header("Authorization", admin)
        .header("Content-Type", "text/plain")
        .body(r#"{"max_uses":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Invalid payloads -> 400
    let invalid_bodies = [
        r#"{"bogus":1}"#.to_string(),
        r#"{"max_uses":0}"#.to_string(),
        r#"{"expires_in_secs":0}"#.to_string(),
        r#"{"roles":[]}"#.to_string(),
        r#"{"roles":["admin"],"require_approval":false,"max_uses":1}"#.to_string(),
        r#"{"roles":["admin"],"require_approval":true,"max_uses":2}"#.to_string(),
        r#"{"master_addr":"127.0.0.1:8088 extra"}"#.to_string(),
        serde_json::json!({"master_addr": "a".repeat(2049)}).to_string(),
    ];
    for payload in invalid_bodies {
        let resp = client
            .post(format!("{base}/api/invites"))
            .header("Authorization", admin)
            .header("Content-Type", "application/json")
            .body(payload.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "expected 400 for {payload}"
        );
    }

    // Valid edge cases: roles dedup, whitespace-only master_addr fallback, 2048-char master_addr
    let dedup_resp = client
        .post(format!("{base}/api/invites"))
        .header("Authorization", admin)
        .json(&serde_json::json!({"roles": ["worker", "worker"], "master_addr": "   "}))
        .send()
        .await
        .unwrap();
    assert_eq!(dedup_resp.status(), StatusCode::OK);
    let dedup_json: Value = dedup_resp.json().await.unwrap();
    assert_eq!(dedup_json["roles"], serde_json::json!(["worker"]));

    let max_addr_resp = client
        .post(format!("{base}/api/invites"))
        .header("Authorization", admin)
        .json(&serde_json::json!({"master_addr": "a".repeat(2048)}))
        .send()
        .await
        .unwrap();
    assert_eq!(max_addr_resp.status(), StatusCode::OK);

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_member_duplicate_rename_and_concurrent_revoke_rename_race() {
    let (_dir, master, addr, _) =
        spawn_v3_master(AuthMode::Members, AuthConfig::dev(), Duration::from_secs(5)).await;
    let client = client();
    let base = format!("http://{addr}");

    let kp_a = NodeKeypair::generate().unwrap();
    let kp_b = NodeKeypair::generate().unwrap();
    enroll_member_for_test(master.members(), &kp_a, "alpha", vec![MemberRole::Worker]);
    enroll_member_for_test(master.members(), &kp_b, "beta", vec![MemberRole::Worker]);
    let id_b = kp_b.member_id().to_string();

    // Rename B -> "alpha" (duplicates A's display name) succeeds (200),
    // and subsequent name-based lookup is ambiguous (400).
    let dup_rename = client
        .post(format!("{base}/api/members/{id_b}/rename"))
        .json(&serde_json::json!({ "name": "alpha" }))
        .send()
        .await
        .unwrap();
    assert_eq!(dup_rename.status(), StatusCode::OK);

    let ambig_revoke = client
        .post(format!("{base}/api/members/alpha/revoke"))
        .send()
        .await
        .unwrap();
    assert_eq!(ambig_revoke.status(), StatusCode::BAD_REQUEST);
    let ambig_err: Value = ambig_revoke.json().await.unwrap();
    assert!(
        ambig_err["message"]
            .as_str()
            .is_some_and(|m| m.contains("ambiguous")),
        "{ambig_err}"
    );

    // Unknown member returns 404.
    let unknown_rename = client
        .post(format!("{base}/api/members/does-not-exist/rename"))
        .json(&serde_json::json!({ "name": "nope" }))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown_rename.status(), StatusCode::NOT_FOUND);

    // Concurrent revoke vs rename by full ID on B: serialized cleanly, zero 500s,
    // and subsequent rename/revoke on B both return 404.
    let revoke_fut = client
        .post(format!("{base}/api/members/{id_b}/revoke"))
        .send();
    let rename_fut = client
        .post(format!("{base}/api/members/{id_b}/rename"))
        .json(&serde_json::json!({ "name": "beta-raced" }))
        .send();
    let (r_rev, r_ren) = tokio::join!(revoke_fut, rename_fut);
    let s_rev = r_rev.unwrap().status();
    let s_ren = r_ren.unwrap().status();
    assert_eq!(s_rev, StatusCode::OK);
    assert!(
        s_ren == StatusCode::OK || s_ren == StatusCode::NOT_FOUND,
        "unexpected rename status during concurrent revoke: {s_ren}"
    );

    let post_rev = client
        .post(format!("{base}/api/members/{id_b}/revoke"))
        .send()
        .await
        .unwrap();
    assert_eq!(post_rev.status(), StatusCode::NOT_FOUND);
    let post_ren = client
        .post(format!("{base}/api/members/{id_b}/rename"))
        .json(&serde_json::json!({ "name": "after-revoke" }))
        .send()
        .await
        .unwrap();
    assert_eq!(post_ren.status(), StatusCode::NOT_FOUND);

    let _ = master.shutdown();
}
