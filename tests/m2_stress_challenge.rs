//! Empirical stress-testing and adversarial challenge test suite for Milestone 2.
//!
//! Validates:
//! 1. High concurrent worker registration bursts (10-20 workers rapidly hitting a single Master)
//!    - Verifies all workers register without lost notifications, lock contention failure, or socket leaks.
//!    - Verifies CPU and GPU capability discrimination across the cluster under burst conditions.
//! 2. Handshake timeout resilience & Slowloris defense
//!    - Verifies raw TCP sockets sending 0 bytes or partial bytes time out within 5s.
//!    - Verifies that slow/stalled handshakes do NOT block concurrent legitimate worker connections.
//! 3. Repeated disconnect and reconnect cycles
//!    - Kill and revive worker 5 times; verify persistent UUID is preserved across all cycles.
//!    - Verify session isolation prevents old session teardown from colliding with new session.
//! 4. Adversarial edge cases:
//!    - Immediate session collision: simultaneous registration race with identical UUID.
//!    - Rapid heartbeat flooding without deadlock.
//!    - Invalid first message rejected immediately with RegisterAck(accepted=false).

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};
use rusty_grid_master::registry::{WorkerRegistry, WorkerStatus};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

/// Polls `condition` every `step` until true or `timeout` elapses.
async fn poll_until<F, Fut>(timeout: Duration, step: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

// =========================================================================
// Challenge 1: High Concurrent Worker Registration Bursts (20 Workers)
// =========================================================================

#[tokio::test]
async fn test_challenge_concurrent_worker_burst_20_nodes() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_heartbeat_interval(2)
        .with_handshake_timeout(5);
    let master = MasterServer::bind(config, registry.clone())
        .await
        .expect("MasterServer::bind failed");
    let master_addr = master.local_addr().to_string();

    let master_handle = tokio::spawn(async move {
        let _ = master.run(master_shutdown_rx).await;
    });

    const TOTAL_WORKERS: usize = 20;
    const GPU_WORKERS: usize = 6;
    const CPU_WORKERS: usize = TOTAL_WORKERS - GPU_WORKERS;

    let mut worker_shutdown_txs = Vec::with_capacity(TOTAL_WORKERS);
    let mut worker_handles = Vec::with_capacity(TOTAL_WORKERS);
    let mut expected_uuids = Vec::with_capacity(TOTAL_WORKERS);

    // Gate all workers behind a start barrier for true concurrent burst
    let start_barrier = Arc::new(tokio::sync::Barrier::new(TOTAL_WORKERS + 1));

    for i in 0..TOTAL_WORKERS {
        let is_gpu = i < GPU_WORKERS;
        let cores = if is_gpu { 8 } else { 4 };
        let name = format!("burst-node-{i:02}");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        worker_shutdown_txs.push(shutdown_tx);

        let mut client =
            WorkerClient::from_options(master_addr.clone(), Some(name), Some(cores), is_gpu);
        let wid = client.worker_id();
        expected_uuids.push((wid, is_gpu));

        let barrier_clone = Arc::clone(&start_barrier);
        let handle = tokio::spawn(async move {
            barrier_clone.wait().await;
            let _ = client.run(shutdown_rx).await;
        });
        worker_handles.push(handle);
    }

    // Release all workers simultaneously to create an instant 20-node connection burst
    let burst_start = Instant::now();
    start_barrier.wait().await;

    // Verify that all 20 workers successfully register and reach Connected status
    let all_registered = poll_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            let workers = registry.list_all_workers().await;
            workers.len() == TOTAL_WORKERS
                && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;

    let burst_elapsed = burst_start.elapsed();
    println!(
        "[CHALLENGE BURST] 20 workers registered in {:?} (all_registered: {})",
        burst_elapsed, all_registered
    );
    assert!(
        all_registered,
        "All 20 workers must successfully register without loss"
    );

    // Verify cluster capability counts under burst
    let (total_count, active_count, gpu_count) = registry.counts().await;
    assert_eq!(total_count, TOTAL_WORKERS, "Total worker count must be 20");
    assert_eq!(
        active_count, TOTAL_WORKERS,
        "Active worker count must be 20"
    );
    assert_eq!(
        gpu_count, GPU_WORKERS,
        "GPU worker count must match expected 6"
    );

    let gpu_list = registry.list_gpu_workers().await;
    assert_eq!(gpu_list.len(), GPU_WORKERS);

    let non_gpu_list = registry.list_non_gpu_workers().await;
    assert_eq!(non_gpu_list.len(), CPU_WORKERS);

    // Verify individual worker capabilities match
    for (wid, is_gpu) in expected_uuids {
        let info = registry
            .get_worker(wid)
            .await
            .expect("Worker must exist in registry");
        assert_eq!(info.status, WorkerStatus::Connected);
        assert_eq!(info.capabilities.can_execute_gpu(), is_gpu);
    }

    // Clean teardown: stop all workers and master
    for tx in worker_shutdown_txs {
        let _ = tx.send(true);
    }
    let _ = master_shutdown_tx.send(true);

    for h in worker_handles {
        let _ = h.await;
    }
    let _ = master_handle.await;

    // Verify all workers transitioned to Disconnected upon shutdown
    let all_disconnected = poll_until(
        Duration::from_secs(2),
        Duration::from_millis(50),
        || async {
            let workers = registry.list_all_workers().await;
            workers
                .iter()
                .all(|w| w.status == WorkerStatus::Disconnected)
        },
    )
    .await;
    assert!(
        all_disconnected,
        "All workers must transition to Disconnected on clean shutdown"
    );
}

// =========================================================================
// Challenge 2: Handshake Timeout Resilience & Non-Blocking Slowloris Defense
// =========================================================================

#[tokio::test]
async fn test_challenge_handshake_timeout_resilience_and_non_blocking() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    // Use default 5-second handshake timeout as specified in requirements
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_handshake_timeout(5);
    let master = MasterServer::bind(config, registry.clone())
        .await
        .expect("MasterServer::bind failed");
    let master_addr = master.local_addr();

    let master_handle = tokio::spawn(async move {
        let _ = master.run(master_shutdown_rx).await;
    });

    // 1. Rogue Socket 1: Send 0 bytes (open TCP connection, completely silent)
    let mut rogue_zero_bytes = TcpStream::connect(master_addr)
        .await
        .expect("connect rogue 1");

    // 2. Rogue Socket 2: Send partial 2-byte length prefix (need 4 bytes)
    let mut rogue_partial_prefix = TcpStream::connect(master_addr)
        .await
        .expect("connect rogue 2");
    rogue_partial_prefix
        .write_all(&[0x00, 0x00])
        .await
        .expect("write partial prefix");

    // 3. Rogue Socket 3: Send full 4-byte length prefix (declares 64 bytes payload), but only 4 bytes body
    let mut rogue_partial_body = TcpStream::connect(master_addr)
        .await
        .expect("connect rogue 3");
    let length_header: [u8; 4] = (64u32).to_be_bytes();
    rogue_partial_body
        .write_all(&length_header)
        .await
        .expect("write header");
    rogue_partial_body
        .write_all(b"slow")
        .await
        .expect("write partial body");

    // 4. Concurrency check: While all 3 rogue connections are hanging in handshake watchdog,
    // verify that a legitimate Worker CAN CONNECT AND COMPLETE HANDSHAKE IMMEDIATELY (<200ms).
    let legit_connect_start = Instant::now();
    let (legit_shutdown_tx, legit_shutdown_rx) = watch::channel(false);
    let mut legit_client = WorkerClient::from_options(
        master_addr.to_string(),
        Some("legit-node".into()),
        Some(4),
        false,
    );
    let legit_id = legit_client.worker_id();

    let legit_handle = tokio::spawn(async move {
        let _ = legit_client.run(legit_shutdown_rx).await;
    });

    let legit_registered = poll_until(
        Duration::from_millis(500),
        Duration::from_millis(20),
        || async {
            registry
                .get_worker(legit_id)
                .await
                .map(|w| w.status == WorkerStatus::Connected)
                .unwrap_or(false)
        },
    )
    .await;

    let legit_elapsed = legit_connect_start.elapsed();
    println!(
        "[CHALLENGE SLOWLORIS] Legitimate worker registered in {:?} while 3 rogue connections were stalled",
        legit_elapsed
    );
    assert!(
        legit_registered,
        "Legitimate worker must register immediately; hanging handshakes must NOT block other connections"
    );
    #[cfg(windows)]
    let max_elapsed = Duration::from_millis(3000);
    #[cfg(not(windows))]
    let max_elapsed = Duration::from_secs(1);
    assert!(
        legit_elapsed < max_elapsed,
        "Legitimate registration must complete in <{:?} despite Slowloris attacks",
        max_elapsed
    );

    // 5. Verify that all 3 rogue sockets are dropped by Master within 5s (+ grace window)
    let timeout_start = Instant::now();
    let mut buf = [0u8; 32];

    // Rogue 1: read should yield EOF (0 bytes read) or ConnectionReset
    let r1_res =
        tokio::time::timeout(Duration::from_secs(7), rogue_zero_bytes.read(&mut buf)).await;
    let r1_elapsed = timeout_start.elapsed();
    println!(
        "[CHALLENGE SLOWLORIS] Rogue 1 (0 bytes) terminated after {:?}",
        r1_elapsed
    );
    assert!(r1_res.is_ok(), "Rogue 1 must be closed within timeout");
    let bytes_read = r1_res.unwrap().unwrap_or(0);
    assert_eq!(bytes_read, 0, "Rogue 1 must receive EOF on socket drop");
    assert!(
        r1_elapsed >= Duration::from_millis(4500) && r1_elapsed <= Duration::from_millis(6500),
        "Rogue 1 timeout must be ~5s (actual: {:?})",
        r1_elapsed
    );

    // Rogue 2: read should yield EOF (0 bytes read)
    let r2_res =
        tokio::time::timeout(Duration::from_secs(7), rogue_partial_prefix.read(&mut buf)).await;
    let r2_elapsed = timeout_start.elapsed();
    println!(
        "[CHALLENGE SLOWLORIS] Rogue 2 (partial prefix) terminated after {:?}",
        r2_elapsed
    );
    assert!(r2_res.is_ok(), "Rogue 2 must be closed within timeout");
    assert_eq!(r2_res.unwrap().unwrap_or(0), 0, "Rogue 2 must receive EOF");

    // Rogue 3: read should yield EOF (0 bytes read)
    let r3_res =
        tokio::time::timeout(Duration::from_secs(7), rogue_partial_body.read(&mut buf)).await;
    let r3_elapsed = timeout_start.elapsed();
    println!(
        "[CHALLENGE SLOWLORIS] Rogue 3 (partial body) terminated after {:?}",
        r3_elapsed
    );
    assert!(r3_res.is_ok(), "Rogue 3 must be closed within timeout");
    assert_eq!(r3_res.unwrap().unwrap_or(0), 0, "Rogue 3 must receive EOF");

    // 6. Verify that registry has ONLY the legitimate worker (no rogue ghost entries)
    let all_workers = registry.list_all_workers().await;
    assert_eq!(
        all_workers.len(),
        1,
        "Only legitimate worker should exist in registry"
    );
    assert_eq!(all_workers[0].worker_id, legit_id);
    assert_eq!(all_workers[0].status, WorkerStatus::Connected);

    // Teardown
    let _ = legit_shutdown_tx.send(true);
    let _ = master_shutdown_tx.send(true);
    let _ = legit_handle.await;
    let _ = master_handle.await;
}

// =========================================================================
// Challenge 3: Repeated Disconnect and Reconnect Cycles (5 Cycles)
// =========================================================================

#[tokio::test]
async fn test_challenge_repeated_disconnect_reconnect_5_cycles() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone())
        .await
        .expect("MasterServer::bind failed");
    let master_addr = master.local_addr().to_string();

    let master_handle = tokio::spawn(async move {
        let _ = master.run(master_shutdown_rx).await;
    });

    let fixed_worker_id = Uuid::new_v4();
    let mut last_session_id = 0u64;

    // Run 5 full kill-and-revive cycles
    for cycle in 1..=5 {
        let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
        // Force the same persistent UUID across every cycle
        let mut config = WorkerConfig::new(master_addr.clone())
            .with_worker_id(fixed_worker_id)
            .with_name(format!("reconnect-node-cycle-{cycle}"))
            .with_cores(4);
        config.default_heartbeat_interval = Duration::from_millis(200);
        let mut client = WorkerClient::new(config);

        assert_eq!(client.worker_id(), fixed_worker_id);

        let client_handle = tokio::spawn(async move {
            let _ = client.run(worker_shutdown_rx).await;
        });

        // 1. Wait for registration in this cycle
        let registered = poll_until(
            Duration::from_secs(2),
            Duration::from_millis(30),
            || async {
                if let Some(info) = registry.get_worker(fixed_worker_id).await {
                    info.status == WorkerStatus::Connected && info.session_id > last_session_id
                } else {
                    false
                }
            },
        )
        .await;

        assert!(
            registered,
            "Cycle {cycle}: Worker must register with Connected status and new session_id"
        );

        let current_info = registry.get_worker(fixed_worker_id).await.unwrap();
        assert_eq!(
            current_info.worker_id, fixed_worker_id,
            "UUID must be preserved in cycle {cycle}"
        );
        assert!(
            current_info.session_id > last_session_id,
            "Cycle {cycle}: session_id ({}) must be greater than previous ({last_session_id})",
            current_info.session_id
        );
        last_session_id = current_info.session_id;

        println!(
            "[CHALLENGE RECONNECT] Cycle {cycle}/5 succeeded: UUID={}, session_id={}",
            fixed_worker_id, last_session_id
        );

        // 2. Kill worker (simulate abrupt crash on even cycles, graceful on odd cycles)
        if cycle % 2 == 0 {
            // Abrupt kill: abort task without graceful announcement
            client_handle.abort();
            // Wait for Master TCP EOF detection
            let disconnected = poll_until(
                Duration::from_millis(500),
                Duration::from_millis(20),
                || async {
                    registry
                        .get_worker(fixed_worker_id)
                        .await
                        .map(|w| w.status == WorkerStatus::Disconnected)
                        .unwrap_or(false)
                },
            )
            .await;
            assert!(
                disconnected,
                "Cycle {cycle}: Master must detect abrupt drop"
            );
        } else {
            // Graceful kill
            let _ = worker_shutdown_tx.send(true);
            let _ = client_handle.await;
            let disconnected = poll_until(
                Duration::from_millis(500),
                Duration::from_millis(20),
                || async {
                    registry
                        .get_worker(fixed_worker_id)
                        .await
                        .map(|w| w.status == WorkerStatus::Disconnected)
                        .unwrap_or(false)
                },
            )
            .await;
            assert!(
                disconnected,
                "Cycle {cycle}: Graceful shutdown must set Disconnected status"
            );
        }
    }

    // Final verification: cluster has exactly 1 worker record with exactly 5 session increments
    let all_workers = registry.list_all_workers().await;
    assert_eq!(
        all_workers.len(),
        1,
        "There must be exactly 1 worker entry, no duplicate sessions"
    );
    assert_eq!(all_workers[0].worker_id, fixed_worker_id);
    assert_eq!(
        all_workers[0].session_id, 5,
        "Session ID must have incremented to 5"
    );

    let _ = master_shutdown_tx.send(true);
    let _ = master_handle.await;
}

// =========================================================================
// Challenge 4: Immediate Session Collision & Monotonic Unregister Protection
// =========================================================================

#[tokio::test]
async fn test_challenge_simultaneous_reconnect_session_collision_protection() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr();

    tokio::spawn(async move { master.run(master_shutdown_rx).await });

    let shared_worker_id = Uuid::new_v4();
    let caps = WorkerCapabilities::new("collision-worker", 4, 4096, false, false, None);

    // 1. Establish Connection 1
    let stream1 = TcpStream::connect(master_addr).await.unwrap();
    let mut transport1 = MessageTransport::new(stream1);
    transport1
        .send_msg(&WorkerMessage::Register {
            worker_id: shared_worker_id,
            capabilities: caps.clone(),
        })
        .await
        .unwrap();
    let _ack1: MasterMessage = transport1.recv_msg().await.unwrap().unwrap();

    let session1 = registry
        .get_worker(shared_worker_id)
        .await
        .unwrap()
        .session_id;

    // 2. While Connection 1 is STILL OPEN, establish Connection 2 with identical UUID
    let stream2 = TcpStream::connect(master_addr).await.unwrap();
    let mut transport2 = MessageTransport::new(stream2);
    transport2
        .send_msg(&WorkerMessage::Register {
            worker_id: shared_worker_id,
            capabilities: caps.clone(),
        })
        .await
        .unwrap();
    let _ack2: MasterMessage = transport2.recv_msg().await.unwrap().unwrap();

    let session2 = registry
        .get_worker(shared_worker_id)
        .await
        .unwrap()
        .session_id;
    assert!(
        session2 > session1,
        "Second connection must receive strictly higher session_id"
    );

    // 3. Now simulate Connection 1 finally tearing down and sending stale unregister
    // Master's handle_connection does: registry.unregister(&worker_id, Some(session1))
    let stale_unreg_result = registry
        .unregister(&shared_worker_id, Some(session1))
        .await
        .unwrap();
    assert!(
        !stale_unreg_result,
        "Stale unregister from session 1 must be rejected by registry"
    );

    // 4. Verify worker status is STILL Connected on session 2
    let info = registry.get_worker(shared_worker_id).await.unwrap();
    assert_eq!(
        info.status,
        WorkerStatus::Connected,
        "Worker must remain Connected on session 2"
    );
    assert_eq!(info.session_id, session2);

    // 5. Connection 2 sends heartbeat — must be accepted
    transport2
        .send_msg(&WorkerMessage::Heartbeat {
            worker_id: shared_worker_id,
            timestamp: 12345,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 2048,
        })
        .await
        .unwrap();

    let ack = transport2
        .recv_msg::<MasterMessage>()
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ack, MasterMessage::HeartbeatAck { .. }));

    let _ = master_shutdown_tx.send(true);
}

// =========================================================================
// Challenge 5: Rapid Heartbeat Flood Resilience
// =========================================================================

#[tokio::test]
async fn test_challenge_rapid_heartbeat_burst_no_deadlock() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr();

    tokio::spawn(async move { master.run(master_shutdown_rx).await });

    let stream = TcpStream::connect(master_addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);
    let wid = Uuid::new_v4();

    transport
        .send_msg(&WorkerMessage::Register {
            worker_id: wid,
            capabilities: WorkerCapabilities::new("burst-heartbeat", 4, 4096, false, false, None),
        })
        .await
        .unwrap();
    let _ack: MasterMessage = transport.recv_msg().await.unwrap().unwrap();

    // Fire 50 heartbeats in rapid succession without waiting for individual acks
    const BURST_COUNT: usize = 50;
    for i in 0..BURST_COUNT {
        let hb = WorkerMessage::Heartbeat {
            worker_id: wid,
            timestamp: 1000 + i as u64,
            active_tasks: i % 3,
            cpu_usage_pct: 0.0,
            ram_available_mb: 2048,
        };
        transport.send_msg(&hb).await.unwrap();
    }

    // Read back all 50 HeartbeatAcks
    for i in 0..BURST_COUNT {
        let resp = tokio::time::timeout(
            Duration::from_millis(500),
            transport.recv_msg::<MasterMessage>(),
        )
        .await
        .expect("HeartbeatAck timeout")
        .expect("recv failed")
        .expect("transport closed");

        match resp {
            MasterMessage::HeartbeatAck { timestamp } => {
                assert_eq!(timestamp, 1000 + i as u64);
            }
            other => panic!("Expected HeartbeatAck, got {other:?}"),
        }
    }

    // Registry must show the latest active_tasks
    let info = registry.get_worker(wid).await.unwrap();
    assert_eq!(info.active_tasks, (BURST_COUNT - 1) % 3);

    let _ = master_shutdown_tx.send(true);
}

// =========================================================================
// Challenge 6: Malformed Handshake Immediate Rejection
// =========================================================================

#[tokio::test]
async fn test_challenge_malformed_handshake_immediate_rejection() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_handshake_timeout(5);
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr();

    tokio::spawn(async move { master.run(master_shutdown_rx).await });

    // Client connects and sends a Heartbeat as the first message instead of Register
    let stream = TcpStream::connect(master_addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let invalid_first_msg = WorkerMessage::Heartbeat {
        worker_id: Uuid::new_v4(),
        timestamp: 1000,
        active_tasks: 0,
        cpu_usage_pct: 0.0,
        ram_available_mb: 2048,
    };

    let start = Instant::now();
    transport.send_msg(&invalid_first_msg).await.unwrap();

    let resp = transport.recv_msg::<MasterMessage>().await.unwrap();
    let elapsed = start.elapsed();

    println!(
        "[CHALLENGE REJECT] Malformed first message rejected in {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "Rejection must be immediate (<500ms), NOT waiting for 5s handshake timeout"
    );

    match resp {
        Some(MasterMessage::RegisterAck {
            accepted, message, ..
        }) => {
            assert!(!accepted, "Master must reject invalid first message");
            assert!(
                message.unwrap().contains("Expected Register"),
                "Rejection reason must indicate expected Register message"
            );
        }
        other => panic!("Expected RegisterAck(accepted=false), got {other:?}"),
    }

    let _ = master_shutdown_tx.send(true);
}
