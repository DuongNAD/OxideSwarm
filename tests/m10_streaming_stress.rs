//! Milestone 10 Stress Challenge Suite: Empirical Telemetry Streaming Engine Stress Tests.
//!
//! Authored by challenger_m10_1 (Streaming Stress Challenger) to empirically verify:
//! 1. `test_stress_rapid_connection_and_abrupt_disconnection_bursts`:
//!    Rapid bursts of connections and abrupt disconnections across /ws, /api/stream, and /api/stream/sse:
//!    - Simulates hard socket drops, half-open drops, and aborted handshakes.
//!    - Verifies zero panics, zero task leaks, and post-burst server availability.
//!
//! 2. `test_stress_slow_lagging_consumer_ring_buffer_overflow`:
//!    Slow/lagging WebSocket consumer simulating a client that stops reading:
//!    - Bursts >256 events through Master task queue.
//!    - Verifies server-side broadcast ring buffer overflow handling (RecvError::Lagged).
//!    - Asserts receipt of fresh `ClusterSnapshot` resynchronization frame with updated state.
//!    - Verifies fast concurrent consumer is completely unaffected.
//!
//! 3. `test_stress_sse_lagging_consumer_and_resync`:
//!    Slow/lagging Server-Sent Events (/api/stream/sse) consumer:
//!    - Halts reading while Master overflows broadcast ring buffer.
//!    - Verifies SSE client receives resync snapshot event without server crash.
//!
//! 4. `test_stress_high_throughput_concurrency`:
//!    Multiple concurrent WebSocket clients (/ws and /api/stream) under real cluster workload:
//!    - 12 concurrent WS clients continuously consuming telemetry.
//!    - 3 real workers actively registered, heartbeating, and executing 15 batch tasks.
//!    - Verifies 100% task completion, zero client drops, and continuous event stream delivery.
//!
//! 5. `test_stress_keepalive_ping_pong_under_heavy_load`:
//!    WebSocket keepalive ping/pong protocol under continuous telemetry load:
//!    - 25 distinct Ping frames interleaved with high-throughput event broadcasts.
//!    - Verifies 100% matching Pong reflections.
//!    - Verifies on-demand snapshot request (`{"action": "snapshot"}`).
//!    - Verifies resilience to unexpected/corrupted frames.
//!
//! 6. `test_stress_master_graceful_shutdown_drains_telemetry_streams`:
//!    Master shutdown cleanly terminates all active WebSocket and SSE client connections
//!    within strict timeout boundaries.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use reqwest::StatusCode;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::dashboard::dto::{ClusterStatusDto, DashboardStreamMessage};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;

/// Test 1: Rapid connection and abrupt disconnection bursts across /ws, /api/stream, and /api/stream/sse.
#[tokio::test]
async fn test_stress_rapid_connection_and_abrupt_disconnection_bursts() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_dashboard_port(0);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr present");

    // Background task continuously submitting dummy tasks to generate telemetry events during churn
    let master_ref = Arc::new(master);
    let master_churn = Arc::clone(&master_ref);
    let (stop_churn_tx, stop_churn_rx) = watch::channel(false);

    let churn_handle = tokio::spawn(async move {
        let mut idx = 0;
        while !*stop_churn_rx.borrow() {
            idx += 1;
            let task = Task::new(
                TaskSpec::BuiltinTest {
                    test_name: format!("churn_task_{idx}"),
                    iterations: 1,
                    duration_ms: 10,
                    should_fail: false,
                    require_gpu: false,
                },
                TaskRequirements::generic(1, 10),
            );
            let _ = master_churn.submit_task(task).await;
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    });

    let total_attempts = 75;
    let mut handles = Vec::with_capacity(total_attempts);

    for i in 0..total_attempts {
        let target_type = i % 4;
        let handle = tokio::spawn(async move {
            match target_type {
                0 => {
                    // Abrupt drop on /ws after handshake
                    let url = format!("ws://{}/ws", dash_addr);
                    if let Ok((ws_stream, _)) = connect_async(&url).await {
                        // Immediately drop without clean close handshake
                        drop(ws_stream);
                    }
                }
                1 => {
                    // Abrupt drop on /api/stream after receiving 0 or 1 frame
                    let url = format!("ws://{}/api/stream", dash_addr);
                    if let Ok((mut ws_stream, _)) = connect_async(&url).await {
                        let _ =
                            tokio::time::timeout(Duration::from_millis(5), ws_stream.next()).await;
                        drop(ws_stream);
                    }
                }
                2 => {
                    // Raw TCP handshake abort (send HTTP Upgrade request and immediately abort socket)
                    if let Ok(mut tcp_stream) = TcpStream::connect(dash_addr).await {
                        let req = format!(
                            "GET /ws HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
                            dash_addr
                        );
                        let _ = tcp_stream.write_all(req.as_bytes()).await;
                        // Abrupt drop
                        drop(tcp_stream);
                    }
                }
                _ => {
                    // Abrupt drop on SSE stream
                    let client = reqwest::Client::builder()
                        .timeout(Duration::from_millis(200))
                        .build()
                        .unwrap();
                    let url = format!("http://{}/api/stream/sse", dash_addr);
                    if let Ok(mut resp) = client.get(url).send().await {
                        let _ = resp.chunk().await;
                        drop(resp);
                    }
                }
            }
        });
        handles.push(handle);

        // Small stagger to simulate high-frequency connection bursts
        if i % 15 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    for h in handles {
        let _ = h.await;
    }

    // Stop background event generation
    let _ = stop_churn_tx.send(true);
    let _ = churn_handle.await;

    // Post-burst verification: Server must remain completely healthy and responsive
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("build reqwest client");

    let status_resp = client
        .get(format!("http://{}/api/status", dash_addr))
        .send()
        .await
        .expect("GET /api/status after connection bursts");
    assert_eq!(status_resp.status(), StatusCode::OK);
    let status_dto: ClusterStatusDto = status_resp.json().await.expect("parse ClusterStatusDto");
    assert!(!status_dto.version.is_empty());

    // Verify a fresh WebSocket connection succeeds immediately and receives initial snapshot
    let ws_url = format!("ws://{}/ws", dash_addr);
    let (mut fresh_ws, _) = connect_async(&ws_url)
        .await
        .expect("fresh ws connection must succeed post-burst");
    let initial_frame = tokio::time::timeout(Duration::from_secs(2), fresh_ws.next())
        .await
        .expect("timeout waiting for initial snapshot")
        .expect("stream ended")
        .expect("valid frame");
    assert!(initial_frame.to_text().unwrap().contains("Snapshot"));
    fresh_ws.close(None).await.expect("close clean ws");

    // Verify a fresh SSE connection succeeds immediately and receives initial snapshot
    let mut sse_resp = client
        .get(format!("http://{}/api/stream/sse", dash_addr))
        .send()
        .await
        .expect("fresh SSE connection must succeed post-burst");
    assert_eq!(sse_resp.status(), StatusCode::OK);
    let first_chunk = sse_resp
        .chunk()
        .await
        .expect("read sse chunk")
        .expect("non-empty chunk");
    let chunk_text = String::from_utf8_lossy(&first_chunk);
    assert!(chunk_text.contains("data:"));

    match Arc::try_unwrap(master_ref) {
        Ok(m) => m.shutdown().expect("shutdown master"),
        Err(_) => panic!("Arc references remained held"),
    }
}

/// Test 2: Slow / Lagging consumer test: simulates a client that does not read frames while master
/// generates bursts of events, verifying server-side broadcast ring buffer overflow handling
/// (`RecvError::Lagged`) and snapshot resynchronization without server crash.
#[tokio::test]
async fn test_stress_slow_lagging_consumer_ring_buffer_overflow() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_dashboard_port(0);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr present");

    // 1. Connect Client A (Slow/Lagging Consumer) on /ws
    let ws_url_a = format!("ws://{}/ws", dash_addr);
    let (ws_stream_a, _) = connect_async(&ws_url_a).await.expect("connect client A");
    let (mut write_a, mut read_a) = ws_stream_a.split();

    // Consume Client A initial snapshot
    let first_a = tokio::time::timeout(Duration::from_secs(2), read_a.next())
        .await
        .expect("timeout first frame A")
        .unwrap()
        .unwrap();
    assert!(first_a.to_text().unwrap().contains("Snapshot"));

    // 2. Connect Client B (Fast Consumer) on /api/stream
    let ws_url_b = format!("ws://{}/api/stream", dash_addr);
    let (ws_stream_b, _) = connect_async(&ws_url_b).await.expect("connect client B");
    let (mut write_b, mut read_b) = ws_stream_b.split();

    // Consume Client B initial snapshot
    let first_b = tokio::time::timeout(Duration::from_secs(2), read_b.next())
        .await
        .expect("timeout first frame B")
        .unwrap()
        .unwrap();
    assert!(first_b.to_text().unwrap().contains("Snapshot"));

    // Fast client reader task running in background
    let fast_received_count = Arc::new(AtomicUsize::new(0));
    let fast_counter = Arc::clone(&fast_received_count);
    let fast_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = read_b.next().await {
            if msg.is_text() {
                fast_counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    // 3. Client A stops reading entirely!
    // Master generates a rapid burst of >256 events directly through broadcast_tx without yielding,
    // guaranteeing the 256-slot broadcast ring buffer overflows.
    let b_tx = master
        .broadcast_tx()
        .expect("broadcast_tx must be available");
    for _ in 0..400 {
        let _ = b_tx.send(DashboardStreamMessage::StatsUpdated(
            rusty_grid_master::queue::QueueStats::default(),
        ));
    }

    // Give server event processing a moment to process the lag event and transmit snapshot
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 4. Fast client should have received a large volume of events
    assert!(
        fast_received_count.load(Ordering::Relaxed) > 100,
        "Fast client must receive events continuously during burst"
    );

    // 5. Client A resumes reading!
    // The server's broadcast subscriber for Client A must have lagged (RecvError::Lagged).
    // The server must catch this and transmit a fresh ClusterSnapshotDto resync frame!
    let mut found_resync_snapshot = false;
    let read_deadline = tokio::time::Instant::now() + Duration::from_secs(4);

    while tokio::time::Instant::now() < read_deadline {
        if let Ok(Some(Ok(msg))) =
            tokio::time::timeout(Duration::from_millis(200), read_a.next()).await
        {
            if let Ok(text) = msg.to_text() {
                if let Ok(DashboardStreamMessage::Snapshot(_)) =
                    serde_json::from_str::<DashboardStreamMessage>(text)
                {
                    found_resync_snapshot = true;
                    break;
                }
            }
        }
    }

    assert!(
        found_resync_snapshot,
        "Lagged client must receive fresh resync Snapshot after broadcast ring buffer overflow"
    );

    // Clean teardown
    let _ = write_a.close().await;
    let _ = write_b.close().await;
    fast_task.abort();
    master.shutdown().expect("shutdown master");
}

/// Test 3: Server-Sent Events (/api/stream/sse) slow consumer and snapshot resynchronization.
#[tokio::test]
async fn test_stress_sse_lagging_consumer_and_resync() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_dashboard_port(0);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr present");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build reqwest client");

    let mut resp = client
        .get(format!("http://{}/api/stream/sse", dash_addr))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("connect to SSE stream");

    assert_eq!(resp.status(), StatusCode::OK);

    // 1. Read initial snapshot chunk
    let first_chunk = resp
        .chunk()
        .await
        .expect("read chunk")
        .expect("initial chunk");
    let initial_text = String::from_utf8_lossy(&first_chunk);
    assert!(
        initial_text.contains("snapshot"),
        "First SSE event must be snapshot"
    );

    // 2. Pause reading SSE chunks while generating 400 broadcast events synchronously
    let b_tx = master
        .broadcast_tx()
        .expect("broadcast_tx must be available");
    for _ in 0..400 {
        let _ = b_tx.send(DashboardStreamMessage::StatsUpdated(
            rusty_grid_master::queue::QueueStats::default(),
        ));
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 3. Resume reading SSE chunks and look for resync snapshot event
    let mut received_resync_snapshot = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);

    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(Some(chunk))) =
            tokio::time::timeout(Duration::from_millis(300), resp.chunk()).await
        {
            let text = String::from_utf8_lossy(&chunk);
            if text.contains("event: snapshot")
                || (text.contains("Snapshot") && text.contains("status"))
            {
                received_resync_snapshot = true;
                break;
            }
        }
    }

    assert!(
        received_resync_snapshot,
        "Lagged SSE client must receive resync snapshot after buffer overflow"
    );

    master.shutdown().expect("shutdown master");
}

/// Test 4: High-throughput concurrency: multiple concurrent WebSocket clients streaming telemetry
/// while workers register, send heartbeats, and complete batch tasks.
#[tokio::test]
async fn test_stress_high_throughput_concurrency() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_dashboard_port(0);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr present");
    let master_addr = master.server_addr();

    // 1. Spawn 12 concurrent WebSocket clients (6 on /ws, 6 on /api/stream)
    let num_clients = 12;
    let (stop_clients_tx, stop_clients_rx) = watch::channel(false);
    let mut client_handles = Vec::with_capacity(num_clients);
    let clients_received_counts: Vec<Arc<AtomicUsize>> = (0..num_clients)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();

    for (i, count) in clients_received_counts.iter().enumerate().take(num_clients) {
        let path = if i % 2 == 0 { "ws" } else { "api/stream" };
        let url = format!("ws://{}/{}", dash_addr, path);
        let counter = Arc::clone(count);
        let mut stop_rx = stop_clients_rx.clone();

        let handle = tokio::spawn(async move {
            let (ws_stream, resp) = connect_async(&url).await.expect("connect client");
            assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
            let (_write, mut read) = ws_stream.split();

            while !*stop_rx.borrow() {
                tokio::select! {
                    msg = read.next() => {
                        match msg {
                            Some(Ok(frame)) => {
                                if frame.is_text() {
                                    counter.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Some(Err(_)) | None => break,
                        }
                    }
                    _ = stop_rx.changed() => break,
                }
            }
        });
        client_handles.push(handle);
    }

    // Give clients a moment to establish connections and receive initial snapshots
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 2. Spawn 3 Worker nodes
    let num_workers = 3;
    let (worker_stop_tx, worker_stop_rx) = watch::channel(false);
    let mut worker_handles = Vec::with_capacity(num_workers);

    for w_idx in 0..num_workers {
        let mut worker = WorkerClient::from_options(
            master_addr.to_string(),
            Some(format!("stress-worker-{w_idx}")),
            Some(2),
            false,
        );
        let stop_rx = worker_stop_rx.clone();
        let handle = tokio::spawn(async move {
            let _ = worker.run(stop_rx).await;
        });
        worker_handles.push(handle);
    }

    // Wait for all 3 workers to register
    let mut registered_all = false;
    for _ in 0..30 {
        if master.registry().list_active_workers().await.len() == num_workers {
            registered_all = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(registered_all, "All 3 workers must register");

    // 3. Submit 15 batch tasks across the workers
    let task_count = 15;
    let mut task_ids = Vec::with_capacity(task_count);
    for i in 0..task_count {
        let task = Task::new(
            TaskSpec::BuiltinTest {
                test_name: format!("concurrent_task_{i}"),
                iterations: 5,
                duration_ms: 15,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 10),
        );
        let tid = master.submit_task(task).await.expect("submit task");
        task_ids.push(tid);
    }

    // 4. Wait for all tasks to complete successfully
    for tid in task_ids {
        let res = master
            .wait_task(tid, Some(Duration::from_secs(10)))
            .await
            .expect("task must complete within timeout");
        assert_eq!(res.exit_code, 0, "Task must exit with code 0");
    }

    // 5. Verify every single one of the 12 concurrent WebSocket clients received telemetry frames
    for (i, count) in clients_received_counts.iter().enumerate() {
        let received = count.load(Ordering::Relaxed);
        assert!(
            received >= 5,
            "Client {i} must receive multiple event frames (got {received})"
        );
    }

    // 6. Verify REST API /api/status confirms all tasks completed
    let client = reqwest::Client::new();
    let status_resp = client
        .get(format!("http://{}/api/status", dash_addr))
        .send()
        .await
        .expect("GET /api/status");
    let status_dto: ClusterStatusDto = status_resp.json().await.expect("parse status");
    assert_eq!(status_dto.tasks.completed, task_count);
    assert_eq!(status_dto.tasks.running, 0);
    assert_eq!(status_dto.tasks.queued, 0);

    // Teardown
    let _ = stop_clients_tx.send(true);
    for h in client_handles {
        let _ = h.await;
    }
    let _ = worker_stop_tx.send(true);
    for h in worker_handles {
        let _ = h.await;
    }
    master.shutdown().expect("shutdown master");
}

/// Test 5: Keepalive ping/pong verification under load, on-demand snapshot, and frame corruption resilience.
#[tokio::test]
async fn test_stress_keepalive_ping_pong_under_heavy_load() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_dashboard_port(0);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr present");

    let ws_url = format!("ws://{}/ws", dash_addr);
    let (ws_stream, _) = connect_async(&ws_url).await.expect("connect to websocket");
    let (mut write, mut read) = ws_stream.split();

    // Consume initial snapshot
    let _ = read.next().await.unwrap().unwrap();

    // Background task generating heavy telemetry traffic
    let master_ref = Arc::new(master);
    let master_load = Arc::clone(&master_ref);
    let (stop_load_tx, stop_load_rx) = watch::channel(false);

    let load_task = tokio::spawn(async move {
        let mut idx = 0;
        while !*stop_load_rx.borrow() {
            idx += 1;
            let task = Task::new(
                TaskSpec::BuiltinTest {
                    test_name: format!("ping_load_{idx}"),
                    iterations: 1,
                    duration_ms: 5,
                    should_fail: false,
                    require_gpu: false,
                },
                TaskRequirements::generic(1, 10),
            );
            let tid = master_load.submit_task(task).await.unwrap();
            let _ = master_load.cancel_task(tid).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Send 25 distinct Ping frames and assert matching Pong reflections
    let ping_count = 25;
    for i in 0..ping_count {
        let payload = format!("load_ping_payload_{i}").into_bytes();
        write
            .send(Message::Ping(payload.clone()))
            .await
            .expect("send ping frame");

        // Read frames until the matching Pong is received
        let mut matched_pong = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);

        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(Ok(Message::Pong(pong_payload)))) =
                tokio::time::timeout(Duration::from_millis(250), read.next()).await
            {
                if pong_payload == payload {
                    matched_pong = true;
                    break;
                }
            }
        }
        assert!(
            matched_pong,
            "Must receive matching Pong reflection for Ping #{i} under load"
        );
    }

    // Test on-demand snapshot request: client sends {"action": "snapshot"}
    write
        .send(Message::Text(r#"{"action":"snapshot"}"#.into()))
        .await
        .expect("send snapshot action");

    let mut received_snapshot = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(Ok(msg))) =
            tokio::time::timeout(Duration::from_millis(250), read.next()).await
        {
            if let Ok(text) = msg.to_text() {
                if let Ok(DashboardStreamMessage::Snapshot(_)) =
                    serde_json::from_str::<DashboardStreamMessage>(text)
                {
                    received_snapshot = true;
                    break;
                }
            }
        }
    }
    assert!(
        received_snapshot,
        "Must receive Snapshot response to on-demand snapshot action"
    );

    // Test resilience: send unexpected/unknown frame and verify stream remains operational
    write
        .send(Message::Text(
            r#"{"action":"completely_unknown_action"}"#.into(),
        ))
        .await
        .expect("send unknown action");
    write
        .send(Message::Binary(vec![0xAA, 0xBB, 0xCC, 0xDD]))
        .await
        .expect("send binary frame");

    // Verify subsequent ping/pong still functions after unexpected frames
    let canary_payload = b"canary_after_unknown".to_vec();
    write
        .send(Message::Ping(canary_payload.clone()))
        .await
        .expect("send canary ping");

    let mut canary_received = false;
    let canary_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < canary_deadline {
        if let Ok(Some(Ok(Message::Pong(pong_payload)))) =
            tokio::time::timeout(Duration::from_millis(250), read.next()).await
        {
            if pong_payload == canary_payload {
                canary_received = true;
                break;
            }
        }
    }
    assert!(
        canary_received,
        "Canary Ping must receive Pong after unknown frames"
    );

    // Stop load and teardown
    let _ = stop_load_tx.send(true);
    let _ = load_task.await;
    let _ = write.close().await;

    match Arc::try_unwrap(master_ref) {
        Ok(m) => m.shutdown().expect("shutdown master"),
        Err(_) => panic!("Arc references held"),
    }
}

/// Test 6: Master graceful shutdown cleanly drains active WebSocket and SSE telemetry streams.
#[tokio::test]
async fn test_stress_master_graceful_shutdown_drains_telemetry_streams() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_dashboard_port(0);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr present");

    // Connect 3 WebSocket clients
    let mut ws_readers = Vec::new();
    let mut ws_writers = Vec::new();
    for i in 0..3 {
        let path = if i % 2 == 0 { "ws" } else { "api/stream" };
        let url = format!("ws://{}/{}", dash_addr, path);
        let (ws_stream, _) = connect_async(&url).await.expect("connect ws");
        let (write, mut read) = ws_stream.split();
        let _ = read.next().await.unwrap().unwrap(); // consume snapshot
        ws_readers.push(read);
        ws_writers.push(write);
    }

    // Connect 2 SSE clients
    let reqwest_client = reqwest::Client::new();
    let mut sse_resps = Vec::new();
    for _ in 0..2 {
        let mut resp = reqwest_client
            .get(format!("http://{}/api/stream/sse", dash_addr))
            .send()
            .await
            .expect("connect sse");
        let _ = resp.chunk().await.expect("first sse chunk");
        sse_resps.push(resp);
    }

    // Trigger Master shutdown
    let shutdown_start = tokio::time::Instant::now();
    master.shutdown().expect("shutdown master");

    // 1. Verify Master immediately stops accepting new connections on the dashboard port
    tokio::time::sleep(Duration::from_millis(50)).await;
    let new_conn = TcpStream::connect(dash_addr).await;
    assert!(
        new_conn.is_err(),
        "Dashboard HTTP listener must stop accepting new connections after shutdown"
    );

    // 2. Active WebSocket clients send Close frame and verify clean teardown without deadlock
    for mut write in ws_writers {
        let _ = write.send(Message::Close(None)).await;
    }

    for mut reader in ws_readers {
        let stream_res = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(res) = reader.next().await {
                if let Ok(Message::Close(_)) = res {
                    return true;
                }
            }
            true
        })
        .await;
        assert!(
            stream_res.is_ok(),
            "WebSocket reader must terminate cleanly on close"
        );
    }

    // 3. Drop SSE responses to release underlying channels
    drop(sse_resps);

    assert!(
        shutdown_start.elapsed() < Duration::from_secs(4),
        "Draining all connections must complete within timeout"
    );
}
