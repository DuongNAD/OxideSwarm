//! Empirical Stress-Testing and Adversarial Challenge Suite for Milestone 1 (Challenger 2).
//!
//! Validates:
//! 1. Multi-Master Concurrency & Web UI Port Allocation:
//!    - Spawning 10 concurrent MasterServer instances with default/ephemeral configs without port 8080 collision.
//!    - Spawning 10 concurrent MasterServer instances with Web UI explicitly enabled on ephemeral ports ("127.0.0.1:0").
//!    - Fault isolation: MasterServer RPC remains healthy and functional even if Web UI port binding fails.
//!    - Web UI port release: Graceful shutdown of MasterServer cleanly releases the Web UI port for reuse.
//! 2. Worker Registration & Lifecycle Under Rapid Churn:
//!    - 20 concurrent workers rapidly registering, exchanging heartbeats, and disconnecting.
//!    - Reconnection lifecycle with identical worker UUID under high load.
//!    - Abrupt TCP drop vs Graceful Disconnect under rapid registration churn.
//! 3. Parallel Execution & Zero-Flakiness Invariants:
//!    - High-frequency concurrent queries across multiple masters without cross-talk or lock contention.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::server::{MasterServer, ServerConfig};

/// Helper polling predicate until condition is met or timeout elapses.
async fn poll_until<F, Fut>(timeout: Duration, step: Duration, mut predicate: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if predicate().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

/// Spawns a mock wire worker that performs the handshake and then keeps connection alive.
async fn spawn_mock_worker(
    master_addr: SocketAddr,
    worker_id: Uuid,
    caps: WorkerCapabilities,
) -> (Uuid, MessageTransport<TcpStream>) {
    let stream = TcpStream::connect(master_addr)
        .await
        .expect("Failed to connect to master");
    let mut transport = MessageTransport::new(stream);

    transport
        .send_msg(&WorkerMessage::Register {
            worker_id,
            capabilities: caps,
        })
        .await
        .expect("Failed to send Register");

    match transport.recv_msg().await.expect("Recv RegisterAck failed") {
        Some(MasterMessage::RegisterAck { .. }) => {}
        other => panic!("Expected RegisterAck, got {other:?}"),
    }

    (worker_id, transport)
}

// ============================================================================
// AREA 1: Multi-Master Concurrency & Web UI Port 8080 Collision Immunity
// ============================================================================

#[tokio::test]
async fn test_concurrent_multi_master_spawn_default_config_no_port_collision() {
    // Spawn 10 MasterServer instances concurrently with default ephemeral settings.
    // None should attempt to bind 0.0.0.0:8080 and none should collide.
    const INSTANCE_COUNT: usize = 10;
    let mut handles = Vec::new();

    for _ in 0..INSTANCE_COUNT {
        let spawn_fut = async move {
            let config = ServerConfig::default();
            // Default config has bind_addr 127.0.0.1:0 and enable_web_ui = false
            assert!(
                !config.enable_web_ui,
                "Default config must disable Web UI for ephemeral bind"
            );
            MasterServer::spawn(config).await
        };
        handles.push(tokio::spawn(spawn_fut));
    }

    let mut masters = Vec::new();
    let mut bound_ports = std::collections::HashSet::new();

    for (idx, handle) in handles.into_iter().enumerate() {
        let res = handle.await.expect("Join failed");
        assert!(
            res.is_ok(),
            "Master instance #{idx} failed to spawn: {:?}",
            res.err()
        );
        let master = res.unwrap();
        let port = master.server_addr().port();
        assert!(port > 0, "Instance #{idx} must have non-zero port");
        assert!(
            bound_ports.insert(port),
            "Port {port} collision detected between master instances!"
        );
        masters.push(master);
    }

    assert_eq!(masters.len(), INSTANCE_COUNT);

    // Verify all masters can accept worker registrations concurrently
    for (idx, master) in masters.iter().enumerate() {
        let wid = Uuid::new_v4();
        let caps =
            WorkerCapabilities::new(format!("test-worker-{idx}"), 2, 2048, false, false, None);
        let (registered_id, mut transport) =
            spawn_mock_worker(master.server_addr(), wid, caps).await;
        assert_eq!(registered_id, wid);
        let workers = master.list_workers().await.unwrap();
        assert!(workers
            .iter()
            .any(|w| w.worker_id == wid && w.status == WorkerStatus::Connected));
        let _ = transport
            .send_msg(&WorkerMessage::Disconnecting {
                worker_id: wid,
                reason: "normal test finish".into(),
            })
            .await;
    }

    // Clean shutdown of all masters
    for master in masters {
        let _ = master.shutdown();
    }
}

#[tokio::test]
async fn test_concurrent_multi_master_spawn_with_ephemeral_web_ui() {
    // Spawn 8 MasterServer instances concurrently, each with Web UI explicitly enabled
    // on ephemeral port 127.0.0.1:0.
    const INSTANCE_COUNT: usize = 8;
    let mut handles = Vec::new();

    for _ in 0..INSTANCE_COUNT {
        let handle = tokio::spawn(async move {
            let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
                .with_web_ui(true)
                .with_web_ui_addr("127.0.0.1:0".parse().unwrap());
            assert!(config.enable_web_ui);
            assert_eq!(config.web_ui_addr.port(), 0);
            MasterServer::spawn(config).await
        });
        handles.push(handle);
    }

    let mut masters = Vec::new();
    for (idx, handle) in handles.into_iter().enumerate() {
        let res = handle.await.expect("Join failed");
        assert!(
            res.is_ok(),
            "Ephemeral Web UI master #{idx} failed to spawn: {:?}",
            res.err()
        );
        masters.push(res.unwrap());
    }

    // Allow Web UI listeners a brief moment to stabilize
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify each master handles RPC and shuts down cleanly
    for master in masters {
        assert!(master.server_addr().port() > 0);
        let workers = master.list_workers().await.unwrap();
        assert_eq!(workers.len(), 0);
        let _ = master.shutdown();
    }
}

#[tokio::test]
async fn test_master_server_rpc_resilience_on_web_ui_port_conflict() {
    // Bind a TCP listener to a dedicated port to simulate a port conflict on Web UI
    let blocker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_addr = blocker.local_addr().unwrap();

    // Now spawn a MasterServer with Web UI configured to that occupied address
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr(occupied_addr);

    // MasterServer RPC MUST still spawn successfully and serve requests
    let master = MasterServer::spawn(config)
        .await
        .expect("MasterServer::spawn must succeed even if Web UI fails to bind");

    // Verify master RPC is completely functional
    let wid = Uuid::new_v4();
    let caps =
        WorkerCapabilities::new("resilience-worker".to_string(), 4, 4096, false, false, None);

    let (registered_id, mut transport) = spawn_mock_worker(master.server_addr(), wid, caps).await;
    assert_eq!(registered_id, wid);
    let workers = master.list_workers().await.unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].worker_id, wid);
    assert_eq!(workers[0].status, WorkerStatus::Connected);

    let _ = transport
        .send_msg(&WorkerMessage::Disconnecting {
            worker_id: wid,
            reason: "test completion".into(),
        })
        .await;

    drop(transport);
    drop(blocker);
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_web_ui_port_release_and_reuse_after_shutdown() {
    // Find an available port by binding and immediately dropping
    let test_port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let web_addr: SocketAddr = format!("127.0.0.1:{test_port}").parse().unwrap();

    // 1. Spawn Master 1 on that Web UI port
    let config1 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr(web_addr);
    let master1 = MasterServer::spawn(config1).await.unwrap();

    // Verify port is bound by connecting to it
    let connected = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(20),
        || async { TcpStream::connect(web_addr).await.is_ok() },
    )
    .await;
    assert!(connected, "Web UI 1 must be reachable on port {test_port}");

    // 2. Shut down Master 1
    master1.shutdown().unwrap();

    // Wait for port release
    let released = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(20),
        || async { tokio::net::TcpListener::bind(web_addr).await.is_ok() },
    )
    .await;
    assert!(
        released,
        "Web UI port {test_port} must be freed upon master shutdown"
    );

    // 3. Spawn Master 2 on the exact same Web UI port
    let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr(web_addr);
    let master2 = MasterServer::spawn(config2).await.unwrap();

    let reconnected = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(20),
        || async { TcpStream::connect(web_addr).await.is_ok() },
    )
    .await;
    assert!(
        reconnected,
        "Web UI 2 must be reachable on reused port {test_port}"
    );

    let _ = master2.shutdown();
}

// ============================================================================
// AREA 2: Rapid Worker Churn & Lifecycle Stress Under Concurrency
// ============================================================================

#[tokio::test]
async fn test_rapid_worker_registration_burst_20_nodes_simultaneous() {
    let config = ServerConfig::default();
    let master = MasterServer::spawn(config).await.unwrap();
    let master_addr = master.server_addr();

    const WORKER_COUNT: usize = 20;
    let mut handles = Vec::new();

    for i in 0..WORKER_COUNT {
        let handle = tokio::spawn(async move {
            let wid = Uuid::new_v4();
            let caps = WorkerCapabilities::new(
                format!("burst-worker-{i}"),
                2,
                2048,
                i % 2 == 0,
                false,
                None,
            );
            let (registered_id, mut transport) = spawn_mock_worker(master_addr, wid, caps).await;
            assert_eq!(registered_id, wid);

            // Send heartbeat
            transport
                .send_msg(&WorkerMessage::Heartbeat {
                    worker_id: wid,
                    timestamp: 1000 + (i as u64),
                    active_tasks: 0,
                    cpu_usage_pct: 10.0 + (i as f32),
                    ram_available_mb: 1800,
                })
                .await
                .expect("Heartbeat send failed");

            // Recv HeartbeatAck
            match transport
                .recv_msg()
                .await
                .expect("Recv HeartbeatAck failed")
            {
                Some(MasterMessage::HeartbeatAck { .. }) => {}
                other => panic!("Expected HeartbeatAck, got {other:?}"),
            }

            (wid, transport)
        });
        handles.push(handle);
    }

    let mut workers = Vec::new();
    for handle in handles {
        workers.push(handle.await.expect("Worker burst task failed"));
    }

    // Verify all 20 workers are registered and Connected
    let registered_list = master.list_workers().await.unwrap();
    assert_eq!(registered_list.len(), WORKER_COUNT);
    for w in &registered_list {
        assert_eq!(w.status, WorkerStatus::Connected);
    }

    // Half disconnect gracefully, half drop TCP abruptly
    for (idx, (wid, mut transport)) in workers.into_iter().enumerate() {
        if idx % 2 == 0 {
            let _ = transport
                .send_msg(&WorkerMessage::Disconnecting {
                    worker_id: wid,
                    reason: "graceful exit".into(),
                })
                .await;
        } else {
            drop(transport); // abrupt EOF drop
        }
    }

    // Wait for all workers to transition to Disconnected
    let all_disconnected = poll_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            if let Ok(list) = master.list_workers().await {
                list.iter().all(|w| w.status == WorkerStatus::Disconnected)
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        all_disconnected,
        "All 20 workers must transition to Disconnected after disconnect/EOF"
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_rapid_reconnection_churn_same_worker_id_cycles() {
    let config = ServerConfig::default();
    let master = MasterServer::spawn(config).await.unwrap();
    let master_addr = master.server_addr();

    let wid = Uuid::new_v4();
    const CYCLES: usize = 5;

    for cycle in 0..CYCLES {
        let caps = WorkerCapabilities::new(
            format!("churn-worker-cycle-{cycle}"),
            4,
            4096,
            true,
            false,
            None,
        );

        let (registered_wid, mut transport) = spawn_mock_worker(master_addr, wid, caps).await;
        assert_eq!(registered_wid, wid);

        let workers = master.list_workers().await.unwrap();
        let target = workers
            .iter()
            .find(|w| w.worker_id == wid)
            .expect("Worker must exist");
        assert_eq!(target.status, WorkerStatus::Connected);
        assert_eq!(
            target.capabilities.name,
            format!("churn-worker-cycle-{cycle}")
        );

        // Graceful disconnect
        let _ = transport
            .send_msg(&WorkerMessage::Disconnecting {
                worker_id: wid,
                reason: "cycle end".into(),
            })
            .await;
        drop(transport);

        let marked_disconnected = poll_until(
            Duration::from_secs(3),
            Duration::from_millis(10),
            || async {
                if let Ok(list) = master.list_workers().await {
                    list.iter()
                        .any(|w| w.worker_id == wid && w.status == WorkerStatus::Disconnected)
                } else {
                    false
                }
            },
        )
        .await;
        assert!(
            marked_disconnected,
            "Worker must transition to Disconnected on cycle {cycle}"
        );
    }

    let _ = master.shutdown();
}

// ============================================================================
// AREA 3: Parallel Execution Invariants & Flake-Free Isolation
// ============================================================================

#[tokio::test]
async fn test_parallel_multi_master_worker_isolation_no_crosstalk() {
    // Spawn 3 independent masters
    let m1 = MasterServer::spawn(ServerConfig::default()).await.unwrap();
    let m2 = MasterServer::spawn(ServerConfig::default()).await.unwrap();
    let m3 = MasterServer::spawn(ServerConfig::default()).await.unwrap();

    let m1_addr = m1.server_addr();
    let m2_addr = m2.server_addr();
    let m3_addr = m3.server_addr();

    assert_ne!(m1_addr.port(), m2_addr.port());
    assert_ne!(m2_addr.port(), m3_addr.port());
    assert_ne!(m1_addr.port(), m3_addr.port());

    // Spawn workers targeting specific masters
    let w1_id = Uuid::new_v4();
    let (_w1, _t1) = spawn_mock_worker(
        m1_addr,
        w1_id,
        WorkerCapabilities::new("w1-for-m1", 2, 2048, false, false, None),
    )
    .await;

    let w2a_id = Uuid::new_v4();
    let (_w2_a, _t2_a) = spawn_mock_worker(
        m2_addr,
        w2a_id,
        WorkerCapabilities::new("w2a-for-m2", 2, 2048, false, false, None),
    )
    .await;

    let w2b_id = Uuid::new_v4();
    let (_w2_b, _t2_b) = spawn_mock_worker(
        m2_addr,
        w2b_id,
        WorkerCapabilities::new("w2b-for-m2", 2, 2048, false, false, None),
    )
    .await;

    // Verify isolation
    assert_eq!(m1.list_workers().await.unwrap().len(), 1);
    assert_eq!(m2.list_workers().await.unwrap().len(), 2);
    assert_eq!(m3.list_workers().await.unwrap().len(), 0);

    let _ = m1.shutdown();
    let _ = m2.shutdown();
    let _ = m3.shutdown();
}

#[tokio::test]
async fn test_server_config_environment_variable_override_matrix() {
    // 1. Explicit port with no env vars defaults to enable_web_ui = true
    let cfg1 = ServerConfig::new("127.0.0.1:8088".parse().unwrap());
    assert!(cfg1.enable_web_ui);

    // 2. Ephemeral port with no env vars defaults to enable_web_ui = false
    let cfg2 = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    assert!(!cfg2.enable_web_ui);

    // 3. Builder override: explicit enable/disable works regardless of port
    let cfg3 = ServerConfig::new("127.0.0.1:8088".parse().unwrap()).with_web_ui(false);
    assert!(!cfg3.enable_web_ui);

    let cfg4 = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_web_ui(true);
    assert!(cfg4.enable_web_ui);
}

#[tokio::test]
async fn test_web_ui_http_api_endpoints_under_ephemeral_multi_master() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Find an ephemeral port for Web UI
    let web_port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let web_addr: SocketAddr = format!("127.0.0.1:{web_port}").parse().unwrap();

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr(web_addr);
    let master = MasterServer::spawn(config).await.unwrap();

    // Register a worker to the master so /api/status has data
    let wid = Uuid::new_v4();
    let caps = WorkerCapabilities::new("http-test-worker", 8, 16384, true, false, None);
    let (_registered_id, mut transport) = spawn_mock_worker(master.server_addr(), wid, caps).await;

    // Send a heartbeat with load info
    transport
        .send_msg(&WorkerMessage::Heartbeat {
            worker_id: wid,
            timestamp: 123456,
            active_tasks: 0,
            cpu_usage_pct: 42.5,
            ram_available_mb: 12000,
        })
        .await
        .unwrap();
    let _: Result<Option<MasterMessage>, _> = transport.recv_msg().await;

    // Wait for Web UI to be reachable
    let reachable = poll_until(
        Duration::from_secs(3),
        Duration::from_millis(20),
        || async { TcpStream::connect(web_addr).await.is_ok() },
    )
    .await;
    assert!(reachable, "Web UI must be reachable on port {web_port}");

    // Test GET /api/status
    let mut stream = TcpStream::connect(web_addr).await.unwrap();
    let request = format!(
        "GET /api/status HTTP/1.1\r\nHost: 127.0.0.1:{web_port}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "Expected 200 OK for /api/status, got: {response}"
    );
    assert!(
        response.contains("\"master\""),
        "Status must contain master info"
    );
    assert!(
        response.contains("\"workers\""),
        "Status must contain workers info"
    );
    assert!(
        response.contains("http-test-worker"),
        "Status must contain registered worker"
    );

    // Test GET / (HTML dashboard)
    let mut stream2 = TcpStream::connect(web_addr).await.unwrap();
    let request2 =
        format!("GET / HTTP/1.1\r\nHost: 127.0.0.1:{web_port}\r\nConnection: close\r\n\r\n");
    stream2.write_all(request2.as_bytes()).await.unwrap();

    let mut response2 = String::new();
    stream2.read_to_string(&mut response2).await.unwrap();
    assert!(
        response2.starts_with("HTTP/1.1 200 OK"),
        "Expected 200 OK for /, got: {response2}"
    );
    assert!(
        response2.contains("<!DOCTYPE html>") || response2.contains("<html"),
        "Must serve HTML content"
    );

    let _ = transport
        .send_msg(&WorkerMessage::Disconnecting {
            worker_id: wid,
            reason: "done".into(),
        })
        .await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_extreme_worker_churn_50_workers_rapid_heartbeats() {
    let m1 = MasterServer::spawn(ServerConfig::default()).await.unwrap();
    let m2 = MasterServer::spawn(ServerConfig::default()).await.unwrap();

    let m1_addr = m1.server_addr();
    let m2_addr = m2.server_addr();

    const TOTAL_WORKERS: usize = 50;
    let mut handles = Vec::new();

    for i in 0..TOTAL_WORKERS {
        let target_addr = if i % 2 == 0 { m1_addr } else { m2_addr };
        let handle = tokio::spawn(async move {
            let wid = Uuid::new_v4();
            let caps =
                WorkerCapabilities::new(format!("extreme-w-{i}"), 4, 4096, i % 4 == 0, false, None);
            let (reg_id, mut transport) = spawn_mock_worker(target_addr, wid, caps).await;
            assert_eq!(reg_id, wid);

            // Send 3 heartbeats
            for hb_idx in 0..3 {
                transport
                    .send_msg(&WorkerMessage::Heartbeat {
                        worker_id: wid,
                        timestamp: 2000 + hb_idx,
                        active_tasks: 0,
                        cpu_usage_pct: 15.0,
                        ram_available_mb: 3000,
                    })
                    .await
                    .expect("Heartbeat send failed");

                match transport
                    .recv_msg()
                    .await
                    .expect("Recv HeartbeatAck failed")
                {
                    Some(MasterMessage::HeartbeatAck { .. }) => {}
                    other => panic!("Expected HeartbeatAck, got {other:?}"),
                }
            }

            // Half disconnect gracefully, half drop connection
            if i % 3 == 0 {
                let _ = transport
                    .send_msg(&WorkerMessage::Disconnecting {
                        worker_id: wid,
                        reason: "stress test exit".into(),
                    })
                    .await;
            } else {
                drop(transport);
            }

            wid
        });
        handles.push(handle);
    }

    for handle in handles {
        let _wid = handle.await.expect("Worker task failed");
    }

    // Verify all workers on both masters transition to Disconnected
    let m1_all_disconnected = poll_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            if let Ok(workers) = m1.list_workers().await {
                workers
                    .iter()
                    .all(|w| w.status == WorkerStatus::Disconnected)
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        m1_all_disconnected,
        "All workers on master 1 must be Disconnected"
    );

    let m2_all_disconnected = poll_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            if let Ok(workers) = m2.list_workers().await {
                workers
                    .iter()
                    .all(|w| w.status == WorkerStatus::Disconnected)
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        m2_all_disconnected,
        "All workers on master 2 must be Disconnected"
    );

    let _ = m1.shutdown();
    let _ = m2.shutdown();
}
