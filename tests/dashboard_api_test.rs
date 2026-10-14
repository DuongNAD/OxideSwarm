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

use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_core::WorkerCapabilities;
use rusty_grid_master::auth::{AuthConfig, AuthScope};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};

async fn spawn_master(auth: AuthConfig) -> (MasterHandle, SocketAddr) {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth);
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let addr = master.dashboard_addr().expect("dashboard addr");
    (master, addr)
}

fn client() -> reqwest::Client {
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
