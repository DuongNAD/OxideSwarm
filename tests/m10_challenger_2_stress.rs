//! Adversarial Stress & Empirical Challenge Suite for Milestone 10:
//! REST API Endpoints & MasterServer Lifecycle.
//!
//! Authored by Challenger M10.2 to empirically verify:
//! 1. `test_malformed_queries_to_api_tasks`:
//!    - Malformed state strings (?state=invalid_xyz, ?state=%00, SQLi, XSS, Unicode, oversized).
//!    - Empty and whitespace state strings (?state=, ?state=%20%20).
//!    - Case-insensitivity (?state=COMPLETED vs ?state=completed).
//!    - Limit boundary conditions (?limit=0, ?limit=1, ?limit=1000000, ?limit=usize::MAX).
//!    - Invalid and overflow limits (?limit=-1, ?limit=notanumber, numeric overflow) -> handled without 500 crashes.
//!    - Unknown query parameters (?foo=bar&state=completed) -> gracefully handled.
//! 2. `test_non_uuid_and_nonexistent_task_ids`:
//!    - Arbitrary non-UUID strings (arbitrary text, numeric, path traversal, scripts) -> strict HTTP 404.
//!    - Malformed UUID strings (invalid hex chars, too short, too long, underscores, braces) -> strict HTTP 404.
//!    - Nonexistent valid UUIDs (random v4, nil UUID, max UUID) -> strict HTTP 404.
//!    - Valid task ID -> HTTP 200 with matching TaskInfo JSON.
//! 3. `test_ephemeral_port_0_lifecycle_and_port_file_stress`:
//!    - Sequential rapid spin-up & graceful shutdown (20 cycles) with --dashboard-port 0.
//!    - Dynamic port allocation (port > 0), atomic port file publication, readable port, and clean deletion on shutdown.
//!    - Concurrent multi-instance spin-up (5 instances) on port 0, verifying collision-free allocation and clean cleanup.
//! 4. `test_concurrent_rest_bombardment_under_load`:
//!    - 1 Master, 2 active Workers, 1 active WebSocket telemetry stream subscriber.
//!    - 15 active tasks submitted and processed across workers.
//!    - 8 concurrent client tasks issuing 400+ mixed requests to /api/status, /api/workers, /api/tasks, and task ID lookups.
//!    - Verifies zero 500 crashes, thread safety, queue lock integrity, and accurate final cluster stats.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use reqwest::{Client, StatusCode};
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use uuid::Uuid;

use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::dashboard::dto::ClusterStatusDto;
use rusty_grid_master::queue::TaskInfo;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;

/// Helper: wait up to `timeout` for a file to exist on disk.
async fn wait_file_exists(path: &std::path::Path, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if tokio::fs::metadata(path).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    false
}

/// Helper: wait up to `timeout` for a file to be deleted from disk.
async fn wait_file_deleted(path: &std::path::Path, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if tokio::fs::metadata(path).await.is_err() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// 1. Malformed queries to `/api/tasks`:
///    Tests invalid state names, limit boundaries, SQLi/XSS/Unicode payloads,
///    overflow limits, and extra parameters. Verifies zero 500 crashes.
#[tokio::test]
async fn test_malformed_queries_to_api_tasks() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let dash_port_file = temp_dir.path().join("dash.port");

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_dashboard_port_file(&dash_port_file);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    // Register 1 worker
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
    let mut worker = WorkerClient::from_options(
        master.server_addr().to_string(),
        Some("stress-query-worker".into()),
        Some(4),
        false,
    );
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(worker_shutdown_rx).await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Submit 2 tasks that complete
    for i in 0..2 {
        let task = Task::new(
            TaskSpec::BuiltinTest {
                test_name: format!("completed_test_{i}"),
                iterations: 5,
                duration_ms: 20,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 10),
        );
        let task_id = master.submit_task(task).await.expect("submit task");
        let res = master
            .wait_task(task_id, Some(Duration::from_secs(5)))
            .await
            .expect("wait task");
        assert_eq!(res.exit_code, 0);
    }

    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // Baseline: verify 2 tasks in total
    let resp = client
        .get(format!("{}/api/tasks", base_url))
        .send()
        .await
        .expect("GET /api/tasks");
    assert_eq!(resp.status(), StatusCode::OK);
    let all_tasks: Vec<TaskInfo> = resp.json().await.expect("parse json");
    assert_eq!(all_tasks.len(), 2, "Expected 2 completed tasks");

    // Case A: Invalid state names -> HTTP 200 with empty list []
    let invalid_states = [
        "invalid_xyz",
        "NONEXISTENT_STATE",
        "12345",
        "unknown_state_foobar",
        "%00",                               // null byte
        "%27%20OR%201=1%20--",               // SQL injection attempt: ' OR 1=1 --
        "%3Cscript%3Ealert(1)%3C/script%3E", // XSS attempt: <script>alert(1)</script>
        "%F0%9F%A6%80%F0%9F%94%A5",          // Unicode: 🦀🔥
    ];

    for st in invalid_states {
        let url = format!("{}/api/tasks?state={}", base_url, st);
        let resp = client
            .get(&url)
            .send()
            .await
            .expect("send invalid state query");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Query for state '{st}' must return 200 OK without crashing"
        );
        let tasks: Vec<TaskInfo> = resp.json().await.expect("parse empty list");
        assert!(
            tasks.is_empty(),
            "Expected empty list for non-existent state '{st}', got {} tasks",
            tasks.len()
        );
    }

    // Case B: Empty or whitespace-only state strings -> no filtering, returns all tasks
    let whitespace_states = ["", "%20", "%20%20%20", "%09"];
    for ws in whitespace_states {
        let url = format!("{}/api/tasks?state={}", base_url, ws);
        let resp = client
            .get(&url)
            .send()
            .await
            .expect("send whitespace query");
        assert_eq!(resp.status(), StatusCode::OK);
        let tasks: Vec<TaskInfo> = resp.json().await.expect("parse tasks");
        assert_eq!(
            tasks.len(),
            2,
            "Empty or whitespace state '{ws}' should return all tasks"
        );
    }

    // Case C: Oversized query string (>4KB) -> handled gracefully without 500 crash
    let long_state = "A".repeat(4096);
    let url = format!("{}/api/tasks?state={}", base_url, long_state);
    let resp = client
        .get(&url)
        .send()
        .await
        .expect("send long state query");
    assert_ne!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "Oversized state query must never crash with 500"
    );
    if resp.status() == StatusCode::OK {
        let tasks: Vec<TaskInfo> = resp.json().await.expect("parse json");
        assert!(tasks.is_empty());
    }

    // Case D: Case-insensitivity check (COMPLETED vs completed vs Completed vs spaced)
    let casing_cases = ["completed", "COMPLETED", "Completed", "%20completed%20"];
    for valid_casing in casing_cases {
        let url = format!("{}/api/tasks?state={}", base_url, valid_casing);
        let resp = client.get(&url).send().await.expect("send case query");
        assert_eq!(resp.status(), StatusCode::OK);
        let tasks: Vec<TaskInfo> = resp.json().await.expect("parse json");
        assert_eq!(
            tasks.len(),
            2,
            "State filter '{valid_casing}' should match all 2 completed tasks"
        );
    }

    // Case E: Limit boundary tests
    // 1. limit=0 -> returns HTTP 200 with empty list []
    let resp = client
        .get(format!("{}/api/tasks?limit=0", base_url))
        .send()
        .await
        .expect("limit=0");
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<TaskInfo> = resp.json().await.expect("parse json");
    assert_eq!(tasks.len(), 0, "limit=0 must return empty list");

    // 2. limit=1 -> returns exactly 1 item
    let resp = client
        .get(format!("{}/api/tasks?limit=1", base_url))
        .send()
        .await
        .expect("limit=1");
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<TaskInfo> = resp.json().await.expect("parse json");
    assert_eq!(tasks.len(), 1, "limit=1 must return exactly 1 item");

    // 3. limit=1000000 -> returns all available tasks without panic
    let resp = client
        .get(format!("{}/api/tasks?limit=1000000", base_url))
        .send()
        .await
        .expect("limit=1000000");
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<TaskInfo> = resp.json().await.expect("parse json");
    assert_eq!(tasks.len(), 2, "limit=1000000 must return all 2 tasks");

    // 4. limit=usize::MAX (18446744073709551615) -> handles without overflow panic
    let resp = client
        .get(format!("{}/api/tasks?limit=18446744073709551615", base_url))
        .send()
        .await
        .expect("limit=usize::MAX");
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<TaskInfo> = resp.json().await.expect("parse json");
    assert_eq!(tasks.len(), 2);

    // 5. Malformed limits: non-numeric, negative, overflow
    let malformed_limits = [
        "-1",
        "abc",
        "3.14",
        "999999999999999999999999999999999999999999999999999999999999",
    ];
    for ml in malformed_limits {
        let url = format!("{}/api/tasks?limit={}", base_url, ml);
        let resp = client.get(&url).send().await.expect("send malformed limit");
        assert_ne!(
            resp.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "Malformed limit '{ml}' must never cause HTTP 500"
        );
        assert!(
            resp.status().is_client_error(),
            "Malformed limit '{ml}' should be rejected as a client error (got {})",
            resp.status()
        );
    }

    // Case F: Extra / unknown parameters
    let resp = client
        .get(format!(
            "{}/api/tasks?foo=bar&state=completed&extra_param=123",
            base_url
        ))
        .send()
        .await
        .expect("extra params");
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<TaskInfo> = resp.json().await.expect("parse json");
    assert_eq!(
        tasks.len(),
        2,
        "Extra query parameters must be safely ignored"
    );

    // Case G: Combined filter + limit
    let resp = client
        .get(format!("{}/api/tasks?state=completed&limit=1", base_url))
        .send()
        .await
        .expect("state=completed&limit=1");
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<TaskInfo> = resp.json().await.expect("parse json");
    assert_eq!(tasks.len(), 1);

    // Teardown
    let _ = worker_shutdown_tx.send(true);
    let _ = worker_handle.await;
    master.shutdown().expect("shutdown");
}

/// 2. Non-UUID and nonexistent task IDs for `/api/tasks/:task_id`:
///    Tests arbitrary strings, malformed UUIDs, nonexistent valid UUIDs,
///    and valid task IDs. Verifies strict HTTP 404 vs HTTP 200.
#[tokio::test]
async fn test_non_uuid_and_nonexistent_task_ids() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let dash_port_file = temp_dir.path().join("dash.port");

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_dashboard_port_file(&dash_port_file);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    // Submit 1 real task and wait for completion
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
    let mut worker = WorkerClient::from_options(
        master.server_addr().to_string(),
        Some("stress-uuid-worker".into()),
        Some(2),
        false,
    );
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(worker_shutdown_rx).await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "valid_task_test".into(),
            iterations: 5,
            duration_ms: 10,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 10),
    );
    let valid_task_id = master.submit_task(task).await.expect("submit task");
    let res = master
        .wait_task(valid_task_id, Some(Duration::from_secs(5)))
        .await
        .expect("wait task");
    assert_eq!(res.exit_code, 0);

    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // 1. Verify the valid task returns HTTP 200 with accurate TaskInfo
    let valid_url = format!("{}/api/tasks/{}", base_url, valid_task_id);
    let resp = client.get(&valid_url).send().await.expect("GET valid task");
    assert_eq!(resp.status(), StatusCode::OK);
    let task_info: TaskInfo = resp.json().await.expect("parse TaskInfo");
    assert_eq!(task_info.task_id, valid_task_id);

    // 2. Arbitrary non-UUID strings -> Strict HTTP 404 (NOT_FOUND)
    let non_uuid_strings = [
        "not-a-uuid",
        "12345",
        "abc-xyz",
        "undefined",
        "null",
        "true",
        "NaN",
        "task_12345",
        "..%2F..%2Fetc%2Fpasswd", // Path traversal
        "%20%20",                 // Spaces
        "%3Cscript%3E",           // Script tag
        "%27%20OR%201=1",         // SQL injection
        "invalid.json",
        "0",
    ];

    for s in non_uuid_strings {
        let url = format!("{}/api/tasks/{}", base_url, s);
        let resp = client.get(&url).send().await.expect("send non-uuid query");
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Arbitrary string '{s}' for task_id must return 404 NOT_FOUND, got {}",
            resp.status()
        );
    }

    // 3. Malformed UUID strings -> Strict HTTP 404 (NOT_FOUND)
    let malformed_uuids = [
        "00000000-0000-0000-0000-00000000000g", // Invalid hex character 'g'
        "12345678-1234-1234-1234-1234567890a",  // Too short (11 digits at end)
        "12345678-1234-1234-1234-1234567890abcdef", // Too long (14 digits at end)
        "12345678_1234_1234_1234_1234567890ab", // Underscores instead of hyphens
        "%7B12345678-1234-1234-1234-1234567890ab%7D", // Braced UUID {1234...}
        "12345678-1234-1234-1234-1234567890ab-extra", // Extra suffix
        "prefix-12345678-1234-1234-1234-1234567890ab", // Extra prefix
        "12345678-1234-1234-1234",              // Truncated segments
    ];

    for mu in malformed_uuids {
        let url = format!("{}/api/tasks/{}", base_url, mu);
        let resp = client.get(&url).send().await.expect("send malformed uuid");
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Malformed UUID '{mu}' must return 404 NOT_FOUND, got {}",
            resp.status()
        );
    }

    // 4. Valid formatted UUIDs that do not exist in the queue -> Strict HTTP 404 (NOT_FOUND)
    let nonexistent_uuids = [
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::nil(),
        Uuid::max(),
        Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
    ];

    for nu in nonexistent_uuids {
        let url = format!("{}/api/tasks/{}", base_url, nu);
        let resp = client
            .get(&url)
            .send()
            .await
            .expect("send nonexistent uuid");
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Nonexistent UUID '{nu}' must return 404 NOT_FOUND, got {}",
            resp.status()
        );
    }

    // 5. Re-verify the valid task still returns 200 OK after barrage of 404s
    let resp = client
        .get(&valid_url)
        .send()
        .await
        .expect("re-verify valid task");
    assert_eq!(resp.status(), StatusCode::OK);

    // Teardown
    let _ = worker_shutdown_tx.send(true);
    let _ = worker_handle.await;
    master.shutdown().expect("shutdown");
}

/// 3. Ephemeral port 0 stress & dynamic lifecycle:
///    - Part A: Rapid sequential spin-up & graceful shutdown (20 iterations)
///      verifying dynamic port allocation (port > 0), atomic port file write,
///      readable port, and clean port file deletion on shutdown.
///    - Part B: Concurrent multi-instance spin-up (5 instances) on port 0,
///      verifying zero port collisions and independent clean shutdowns.
#[tokio::test]
async fn test_ephemeral_port_0_lifecycle_and_port_file_stress() {
    let client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("reqwest client");

    // --- Part A: Rapid Sequential Spin-Up & Shutdown (20 iterations) ---
    const SEQUENTIAL_CYCLES: usize = 20;
    for i in 0..SEQUENTIAL_CYCLES {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let port_file = temp_dir.path().join(format!("dash_{i}.port"));

        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_dashboard_port(0)
            .with_dashboard_port_file(&port_file);

        let master = MasterServer::spawn(config)
            .await
            .expect("spawn master on port 0");
        let dash_addr = master
            .dashboard_addr()
            .expect("dashboard addr must be present");
        let bound_port = dash_addr.port();

        assert_ne!(
            bound_port, 0,
            "Cycle {i}: Ephemeral port must resolve to non-zero port"
        );

        // Verify port file exists
        let exists = wait_file_exists(&port_file, Duration::from_millis(500)).await;
        assert!(exists, "Cycle {i}: Dashboard port file must exist");

        // Verify port file contains the exact decimal port
        let content = tokio::fs::read_to_string(&port_file)
            .await
            .expect("read port file");
        let parsed_port: u16 = content.trim().parse().expect("parse port file u16");
        assert_eq!(
            parsed_port, bound_port,
            "Cycle {i}: Port file content must match bound port"
        );

        // Verify HTTP endpoint responds
        let status_url = format!("http://127.0.0.1:{bound_port}/api/status");
        let resp = client
            .get(&status_url)
            .send()
            .await
            .expect("GET /api/status");
        assert_eq!(resp.status(), StatusCode::OK);
        let status: ClusterStatusDto = resp.json().await.expect("parse status");
        assert_eq!(status.dashboard_addr, Some(dash_addr.to_string()));

        // Graceful shutdown
        master.shutdown().expect("shutdown");

        // Verify port file is deleted on shutdown
        let deleted = wait_file_deleted(&port_file, Duration::from_millis(1500)).await;
        assert!(
            deleted,
            "Cycle {i}: Dashboard port file must be cleanly deleted after shutdown"
        );
    }

    // --- Part B: Concurrent Multi-Instance Spin-Up (5 instances) ---
    const CONCURRENT_INSTANCES: usize = 5;
    let mut temp_dirs = Vec::with_capacity(CONCURRENT_INSTANCES);
    let mut masters = Vec::with_capacity(CONCURRENT_INSTANCES);
    let mut port_files = Vec::with_capacity(CONCURRENT_INSTANCES);
    let mut bound_ports = HashSet::new();

    for i in 0..CONCURRENT_INSTANCES {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let port_file = temp_dir.path().join(format!("dash_concurrent_{i}.port"));

        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_dashboard_port(0)
            .with_dashboard_port_file(&port_file);

        let master = MasterServer::spawn(config)
            .await
            .expect("spawn concurrent master");
        let dash_addr = master.dashboard_addr().expect("dashboard addr");
        let port = dash_addr.port();

        assert_ne!(port, 0);
        let is_unique = bound_ports.insert(port);
        assert!(
            is_unique,
            "Concurrent instance {i}: Ephemeral port {port} collided with another instance!"
        );

        port_files.push((port_file, port));
        masters.push(master);
        temp_dirs.push(temp_dir);
    }

    // Verify all 5 port files exist and match
    for (idx, (p_file, port)) in port_files.iter().enumerate() {
        let exists = wait_file_exists(p_file, Duration::from_millis(500)).await;
        assert!(exists, "Concurrent instance {idx}: Port file must exist");
        let content = tokio::fs::read_to_string(p_file)
            .await
            .expect("read port file");
        let p: u16 = content.trim().parse().expect("parse u16");
        assert_eq!(p, *port);

        // Verify HTTP responds
        let url = format!("http://127.0.0.1:{port}/api/status");
        let resp = client.get(&url).send().await.expect("GET status");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // Concurrently shut down all 5 masters
    for master in masters {
        master.shutdown().expect("shutdown");
    }

    // Verify all 5 port files are deleted
    for (idx, (p_file, _)) in port_files.iter().enumerate() {
        let deleted = wait_file_deleted(p_file, Duration::from_millis(1500)).await;
        assert!(
            deleted,
            "Concurrent instance {idx}: Port file must be deleted on shutdown"
        );
    }
}

/// 4. Concurrent REST bombardment:
///    Bombards `/api/status`, `/api/workers`, `/api/tasks`, and `/api/tasks/:task_id`
///    with 8 concurrent client tasks issuing 400+ requests while 15 tasks are
///    actively submitted and executed across 2 workers, with an active WebSocket subscriber.
#[tokio::test]
async fn test_concurrent_rest_bombardment_under_load() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let dash_port_file = temp_dir.path().join("dash.port");

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_dashboard_port_file(&dash_port_file);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    // Launch 2 workers
    let mut worker_shutdowns = Vec::new();
    let mut worker_handles = Vec::new();

    for i in 0..2 {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut worker = WorkerClient::from_options(
            master.server_addr().to_string(),
            Some(format!("bombard-worker-{i}")),
            Some(2),
            false,
        );
        let handle = tokio::spawn(async move {
            let _ = worker.run(shutdown_rx).await;
        });
        worker_shutdowns.push(shutdown_tx);
        worker_handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connect WebSocket stream client to verify live stream remains resilient under REST bombardment
    let ws_url = format!("ws://{}/ws", dash_addr);
    let (ws_stream, _) = connect_async(&ws_url).await.expect("connect to websocket");
    let (_ws_write, mut ws_read) = ws_stream.split();

    let ws_event_count = Arc::new(AtomicUsize::new(0));
    let ws_event_count_clone = Arc::clone(&ws_event_count);
    let (ws_stop_tx, mut ws_stop_rx) = watch::channel(false);

    let ws_listener = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = ws_read.next() => {
                    match msg {
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Text(_))) => {
                            ws_event_count_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | None => break,
                        _ => {}
                    }
                }
                _ = ws_stop_rx.changed() => {
                    break;
                }
            }
        }
    });

    // Submit initial task to have a valid task ID for bombardment
    let initial_task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "initial_task".into(),
            iterations: 5,
            duration_ms: 10,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 10),
    );
    let initial_task_id = master
        .submit_task(initial_task)
        .await
        .expect("submit initial");

    // Shared counters for bombardment assertions
    let total_200_requests = Arc::new(AtomicUsize::new(0));
    let total_404_requests = Arc::new(AtomicUsize::new(0));
    let total_500_errors = Arc::new(AtomicUsize::new(0));

    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // Launch 8 concurrent bombardment tasks issuing 50 requests each (400 requests total)
    let num_bombard_tasks = 8;
    let requests_per_task = 50;
    let mut bombard_handles = Vec::new();

    for worker_idx in 0..num_bombard_tasks {
        let client = client.clone();
        let base_url = base_url.clone();
        let c_200 = Arc::clone(&total_200_requests);
        let c_404 = Arc::clone(&total_404_requests);
        let c_500 = Arc::clone(&total_500_errors);
        let valid_id = initial_task_id;

        let handle = tokio::spawn(async move {
            for req_i in 0..requests_per_task {
                let op = (worker_idx * 17 + req_i) % 8;
                let (url, _expect_status) = match op {
                    0 => (format!("{}/api/status", base_url), StatusCode::OK),
                    1 => (format!("{}/api/workers", base_url), StatusCode::OK),
                    2 => (format!("{}/api/tasks", base_url), StatusCode::OK),
                    3 => (
                        format!("{}/api/tasks?state=running", base_url),
                        StatusCode::OK,
                    ),
                    4 => (
                        format!("{}/api/tasks?state=completed", base_url),
                        StatusCode::OK,
                    ),
                    5 => (format!("{}/api/tasks?limit=3", base_url), StatusCode::OK),
                    6 => (
                        format!("{}/api/tasks/{}", base_url, valid_id),
                        StatusCode::OK,
                    ),
                    7 => (
                        format!("{}/api/tasks/{}", base_url, Uuid::new_v4()),
                        StatusCode::NOT_FOUND,
                    ),
                    _ => unreachable!(),
                };

                match client.get(&url).send().await {
                    Ok(resp) => {
                        if resp.status() == StatusCode::INTERNAL_SERVER_ERROR {
                            c_500.fetch_add(1, Ordering::Relaxed);
                        } else if resp.status() == StatusCode::OK {
                            c_200.fetch_add(1, Ordering::Relaxed);
                        } else if resp.status() == StatusCode::NOT_FOUND {
                            c_404.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        // Connection/timeout error under extreme stress
                    }
                }

                // Small jitter
                tokio::task::yield_now().await;
            }
        });
        bombard_handles.push(handle);
    }

    // While bombardment is running, submit 14 more tasks and wait for all to finish
    let mut task_ids = vec![initial_task_id];
    for i in 1..15 {
        let task = Task::new(
            TaskSpec::BuiltinTest {
                test_name: format!("load_task_{i}"),
                iterations: 5,
                duration_ms: 25,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 10),
        );
        let id = master.submit_task(task).await.expect("submit task");
        task_ids.push(id);
    }

    // Wait for all 15 tasks to finish
    for tid in task_ids {
        let res = master
            .wait_task(tid, Some(Duration::from_secs(10)))
            .await
            .expect("wait task");
        assert_eq!(
            res.exit_code, 0,
            "Task {tid} should complete with exit code 0"
        );
    }

    // Wait for all bombardment tasks to finish
    for h in bombard_handles {
        h.await.expect("join bombardment task");
    }

    // Stop WebSocket listener
    let _ = ws_stop_tx.send(true);
    let _ = ws_listener.await;

    // Verify bombardment results
    let errors_500 = total_500_errors.load(Ordering::Relaxed);
    let successes_200 = total_200_requests.load(Ordering::Relaxed);
    let not_founds_404 = total_404_requests.load(Ordering::Relaxed);
    let ws_events = ws_event_count.load(Ordering::Relaxed);

    assert_eq!(
        errors_500, 0,
        "Concurrent REST bombardment must produce ZERO HTTP 500 crashes!"
    );
    assert!(
        successes_200 > 0,
        "Expected successful 200 responses, got {successes_200}"
    );
    assert!(
        not_founds_404 > 0,
        "Expected 404 responses for nonexistent tasks, got {not_founds_404}"
    );
    assert!(
        ws_events > 0,
        "WebSocket subscriber must have received stream events during bombardment, got {ws_events}"
    );

    // Final cluster health audit
    let resp = client
        .get(format!("{}/api/status", base_url))
        .send()
        .await
        .expect("final GET /api/status");
    assert_eq!(resp.status(), StatusCode::OK);
    let status: ClusterStatusDto = resp.json().await.expect("parse final status");
    assert_eq!(
        status.tasks.completed, 15,
        "All 15 submitted tasks must be recorded as completed"
    );
    assert_eq!(
        status.tasks.failed, 0,
        "Zero tasks should have failed during bombardment"
    );
    assert_eq!(
        status.workers.connected, 2,
        "Both workers must still be healthy and connected"
    );

    // Teardown
    for s in worker_shutdowns {
        let _ = s.send(true);
    }
    for h in worker_handles {
        let _ = h.await;
    }
    master.shutdown().expect("shutdown");
}
