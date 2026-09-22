//! Automated Integration Test Suite for Milestone 10: Embedded Web Observability Dashboard.
//!
//! Validates:
//! 1. `test_dashboard_http_rest_endpoints`:
//!    - HTTP GET / -> 200 OK, HTML document with brand & embedded SPA.
//!    - HTTP GET /api/status -> 200 OK, well-formed ClusterStatusDto schema.
//!    - HTTP GET /api/workers -> 200 OK, reflects registered workers.
//!    - HTTP GET /api/tasks -> 200 OK, reflects submitted & executed tasks.
//!    - HTTP GET /api/tasks/:task_id -> 200 OK for valid task, 404 for unknown task.
//! 2. `test_dashboard_websocket_telemetry_stream`:
//!    - WebSocket handshake /ws -> HTTP 101 Switching Protocols.
//!    - First frame MUST be initial `ClusterSnapshot` DTO.
//!    - Subsequent frames stream live `TaskUpdated`, `WorkerRegistered`, or `StatsUpdated` events.
//!    - Ping/Pong frame handling and clean disconnection.
//! 3. `test_dashboard_sse_stream`:
//!    - SSE endpoint /api/stream/sse returns text/event-stream and streams initial snapshot.

use futures::{SinkExt, StreamExt};
use reqwest::StatusCode;
use std::time::Duration;
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::dashboard::dto::{ClusterStatusDto, DashboardStreamMessage};
use rusty_grid_master::queue::TaskInfo;
use rusty_grid_master::registry::{WorkerInfo, WorkerStatus};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;

/// Test 1: HTTP REST endpoints validation with ephemeral port 0.
#[tokio::test]
async fn test_dashboard_http_rest_endpoints() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let dash_port_file = temp_dir.path().join("dashboard.port");

    // 1. Launch Master with ephemeral dashboard port (0) and dashboard port file
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_dashboard_port_file(&dash_port_file);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master
        .dashboard_addr()
        .expect("dashboard addr must be assigned");
    assert_ne!(
        dash_addr.port(),
        0,
        "Ephemeral port must resolve to non-zero port"
    );

    // Verify port file was written with the exact bound port
    tokio::time::sleep(Duration::from_millis(50)).await;
    let port_str = tokio::fs::read_to_string(&dash_port_file)
        .await
        .expect("read dashboard port file");
    assert_eq!(port_str.trim().parse::<u16>().unwrap(), dash_addr.port());

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");

    let base_url = format!("http://{}", dash_addr);

    // 2. Test GET / (Single Page Application HTML)
    let resp = client.get(&base_url).send().await.expect("GET /");
    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .expect("content-type")
        .to_str()
        .unwrap();
    assert!(
        content_type.contains("text/html"),
        "Expected text/html, got {content_type}"
    );
    let html = resp.text().await.expect("read html body");
    assert!(
        html.contains("<!DOCTYPE html>"),
        "Must be valid HTML5 document"
    );
    assert!(html.contains("OxideSwarm"), "Must contain OxideSwarm brand");
    assert!(html.contains("Cockpit"), "Must contain Cockpit title");

    // 3. Test GET /api/status
    let resp = client
        .get(format!("{}/api/status", base_url))
        .send()
        .await
        .expect("GET /api/status");
    assert_eq!(resp.status(), StatusCode::OK);
    let status: ClusterStatusDto = resp.json().await.expect("parse ClusterStatusDto");
    assert_eq!(status.workers.total, 0);
    assert_eq!(status.workers.connected, 0);
    assert_eq!(status.tasks.total, 0);
    assert_eq!(status.master_addr, master.server_addr().to_string());
    assert!(!status.version.is_empty());

    // 4. Test GET /api/workers (initial empty)
    let resp = client
        .get(format!("{}/api/workers", base_url))
        .send()
        .await
        .expect("GET /api/workers");
    assert_eq!(resp.status(), StatusCode::OK);
    let workers: Vec<WorkerInfo> = resp.json().await.expect("parse Vec<WorkerInfo>");
    assert!(workers.is_empty());

    // 5. Connect a worker and verify GET /api/workers updates
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
    let mut worker = WorkerClient::from_options(
        master.server_addr().to_string(),
        Some("dash-test-worker".into()),
        Some(4),
        false,
    );
    let worker_id = worker.worker_id();
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(worker_shutdown_rx).await;
    });

    // Wait for worker registration
    tokio::time::sleep(Duration::from_millis(150)).await;

    let resp = client
        .get(format!("{}/api/workers", base_url))
        .send()
        .await
        .expect("GET /api/workers after join");
    assert_eq!(resp.status(), StatusCode::OK);
    let workers: Vec<WorkerInfo> = resp.json().await.expect("parse workers after join");
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].worker_id, worker_id);
    assert_eq!(workers[0].status, WorkerStatus::Connected);
    assert_eq!(workers[0].capabilities.cpu_cores, 4);

    // 6. Test GET /api/tasks (initial empty, then with task)
    let resp = client
        .get(format!("{}/api/tasks", base_url))
        .send()
        .await
        .expect("GET /api/tasks");
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<TaskInfo> = resp.json().await.expect("parse Vec<TaskInfo>");
    assert!(tasks.is_empty());

    // Submit a task
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "dashboard_rest_test".into(),
            iterations: 10,
            duration_ms: 50,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 10),
    );
    let task_id = master.submit_task(task).await.expect("submit task");

    // Wait for task completion
    let res = master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .expect("wait task");
    assert_eq!(res.exit_code, 0);

    // Verify GET /api/tasks returns the task
    let resp = client
        .get(format!("{}/api/tasks", base_url))
        .send()
        .await
        .expect("GET /api/tasks after completion");
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<TaskInfo> = resp.json().await.expect("parse tasks after completion");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_id, task_id);

    // 7. Test GET /api/tasks/:task_id
    let resp = client
        .get(format!("{}/api/tasks/{}", base_url, task_id))
        .send()
        .await
        .expect("GET /api/tasks/:task_id");
    assert_eq!(resp.status(), StatusCode::OK);
    let single_task: TaskInfo = resp.json().await.expect("parse single task");
    assert_eq!(single_task.task_id, task_id);

    // Non-existent task returns 404
    let random_uuid = Uuid::new_v4();
    let resp = client
        .get(format!("{}/api/tasks/{}", base_url, random_uuid))
        .send()
        .await
        .expect("GET non-existent task");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Teardown
    let _ = worker_shutdown_tx.send(true);
    let _ = worker_handle.await;
    master.shutdown().expect("shutdown master");
}

/// Test 2: Live WebSocket telemetry streaming using tokio-tungstenite.
#[tokio::test]
async fn test_dashboard_websocket_telemetry_stream() {
    // 1. Launch Master with ephemeral dashboard port (0)
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_dashboard_port(0);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr present");

    // 2. Connect WebSocket client to /ws
    let ws_url = format!("ws://{}/ws", dash_addr);
    let (ws_stream, response) = connect_async(&ws_url).await.expect("connect to websocket");

    assert_eq!(
        response.status(),
        StatusCode::SWITCHING_PROTOCOLS,
        "WebSocket upgrade must succeed with HTTP 101"
    );

    let (mut write, mut read) = ws_stream.split();

    // 3. First frame MUST be initial ClusterSnapshot
    let first_msg = tokio::time::timeout(Duration::from_secs(3), read.next())
        .await
        .expect("timeout waiting for initial snapshot")
        .expect("stream ended")
        .expect("valid websocket frame");

    let first_text = first_msg.to_text().expect("must be text frame");
    let initial_stream_msg: DashboardStreamMessage =
        serde_json::from_str(first_text).expect("parse initial DashboardStreamMessage");

    match initial_stream_msg {
        DashboardStreamMessage::Snapshot(snapshot) => {
            assert_eq!(snapshot.workers.len(), 0);
            assert_eq!(snapshot.tasks.len(), 0);
            assert!(!snapshot.version.is_empty());
        }
        other => panic!("Expected Snapshot frame, got {:?}", other),
    }

    // 4. Submit task and verify subsequent streaming event receipt
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "ws_stream_test".into(),
            iterations: 5,
            duration_ms: 20,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 10),
    );
    let task_id = master.submit_task(task).await.expect("submit task");

    // Read subsequent frames until we observe an event relating to task or stats
    let mut received_event = false;
    let event_deadline = tokio::time::Instant::now() + Duration::from_secs(4);

    while tokio::time::Instant::now() < event_deadline {
        if let Ok(Some(Ok(msg))) =
            tokio::time::timeout(Duration::from_millis(500), read.next()).await
        {
            if let Ok(text) = msg.to_text() {
                if let Ok(event) = serde_json::from_str::<DashboardStreamMessage>(text) {
                    match event {
                        DashboardStreamMessage::TaskUpdated(info) if info.task_id == task_id => {
                            received_event = true;
                            break;
                        }
                        DashboardStreamMessage::StatsUpdated(_) => {
                            received_event = true;
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    assert!(
        received_event,
        "Must receive dynamic event frame over WebSocket"
    );

    // 5. Verify Ping/Pong frame handling
    let ping_payload = b"oxide_ping".to_vec();
    write
        .send(Message::Ping(ping_payload.clone()))
        .await
        .expect("send ping");

    let mut received_pong = false;
    let pong_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < pong_deadline {
        if let Ok(Some(Ok(Message::Pong(payload)))) =
            tokio::time::timeout(Duration::from_millis(500), read.next()).await
        {
            if payload == ping_payload {
                received_pong = true;
                break;
            }
        }
    }
    assert!(received_pong, "Must respond with Pong to WebSocket Ping");

    // 6. Clean disconnection
    write.close().await.expect("close websocket");
    master.shutdown().expect("shutdown master");
}

/// Test 3: Server-Sent Events (SSE) telemetry stream validation.
#[tokio::test]
async fn test_dashboard_sse_stream() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_dashboard_port(0);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr present");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");

    let mut resp = client
        .get(format!("http://{}/api/stream/sse", dash_addr))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("GET /api/stream/sse");

    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .expect("content-type")
        .to_str()
        .unwrap();
    assert!(
        content_type.contains("text/event-stream"),
        "Expected text/event-stream, got {content_type}"
    );

    // Read first chunk from SSE stream
    if let Some(chunk) = resp.chunk().await.expect("read first sse chunk") {
        let text = String::from_utf8_lossy(&chunk);
        assert!(text.contains("data:"), "SSE chunk must contain event data");
    }

    master.shutdown().expect("shutdown master");
}
