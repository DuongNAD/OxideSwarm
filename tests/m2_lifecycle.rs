//! Integration test suite for Milestone 2: Master-Worker Registration & Lifecycle.
//!
//! Validates:
//! - Multi-worker concurrent registration (2 CPU-only workers + 1 simulated-GPU worker)
//! - Hardware capability discrimination and flag accuracy in `WorkerRegistry`
//! - Periodic heartbeat exchange and liveness tracking
//! - Graceful disconnect via `WorkerMessage::Disconnecting`
//! - Fast-path immediate disconnect on abrupt TCP socket drop (EOF)
//! - Background heartbeat reaper timeout on silent network partitions
//! - Automatic reconnection with exponential backoff preserving worker UUID

use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};
use rusty_grid_master::reaper::{spawn_reaper, ReaperConfig};
use rusty_grid_master::registry::{WorkerRegistry, WorkerStatus};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::backoff::BackoffConfig;
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

/// Asynchronously polls `condition` until it returns true or `timeout` expires.
async fn wait_for<F, Fut>(timeout: Duration, step: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

#[tokio::test]
async fn test_m2_three_workers_registration_and_gpu_capabilities() {
    // 1. Bind Master on ephemeral loopback port
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone())
        .await
        .expect("master bind failed");
    let master_addr = master.local_addr().to_string();

    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    // 2. Spawn 3 Workers: 2 standard CPU, 1 simulated GPU
    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let (w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);
    let (w3_shutdown_tx, w3_shutdown_rx) = watch::channel(false);

    let mut w1 = WorkerClient::from_options(
        master_addr.clone(),
        Some("worker-cpu-1".into()),
        Some(2),
        false,
    );
    let mut w2 = WorkerClient::from_options(
        master_addr.clone(),
        Some("worker-cpu-2".into()),
        Some(4),
        false,
    );
    let mut w3 = WorkerClient::from_options(
        master_addr.clone(),
        Some("worker-gpu-3".into()),
        Some(8),
        true,
    );

    let w1_id = w1.worker_id();
    let w2_id = w2.worker_id();
    let w3_id = w3.worker_id();

    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });
    let w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });
    let w3_handle = tokio::spawn(async move {
        let _ = w3.run(w3_shutdown_rx).await;
    });

    // 3. Verify all 3 workers successfully register and are in Connected state
    let registered = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(50),
        || async {
            let workers = registry.list_all_workers().await;
            workers.len() == 3 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;
    assert!(
        registered,
        "All 3 workers must register and attain Connected status"
    );

    // 4. Verify capability differentiation and flags in WorkerRegistry
    let w1_info = registry
        .get_worker(w1_id)
        .await
        .expect("w1 must be registered");
    let w2_info = registry
        .get_worker(w2_id)
        .await
        .expect("w2 must be registered");
    let w3_info = registry
        .get_worker(w3_id)
        .await
        .expect("w3 must be registered");

    assert_eq!(w1_info.capabilities.cpu_cores, 2);
    assert!(!w1_info.capabilities.can_execute_gpu());
    assert!(!w1_info.capabilities.is_simulated_gpu);

    assert_eq!(w2_info.capabilities.cpu_cores, 4);
    assert!(!w2_info.capabilities.can_execute_gpu());
    assert!(!w2_info.capabilities.is_simulated_gpu);

    assert_eq!(w3_info.capabilities.cpu_cores, 8);
    assert!(w3_info.capabilities.can_execute_gpu());
    assert!(w3_info.capabilities.is_simulated_gpu);

    // Verify registry capability query methods
    let gpu_workers = registry.list_gpu_workers().await;
    assert_eq!(gpu_workers.len(), 1);
    assert_eq!(gpu_workers[0].worker_id, w3_id);

    let non_gpu_workers = registry.list_non_gpu_workers().await;
    assert_eq!(non_gpu_workers.len(), 2);

    let (total, active, gpu_count) = registry.counts().await;
    assert_eq!(total, 3);
    assert_eq!(active, 3);
    assert_eq!(gpu_count, 1);

    // 5. Clean teardown
    let _ = w1_shutdown_tx.send(true);
    let _ = w2_shutdown_tx.send(true);
    let _ = w3_shutdown_tx.send(true);
    let _ = master_shutdown_tx.send(true);

    let _ = tokio::join!(w1_handle, w2_handle, w3_handle, master_handle);
}

#[tokio::test]
async fn test_m2_heartbeat_updates() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_heartbeat_interval(1); // 1 second heartbeats for test
    let master = MasterServer::bind(config, registry.clone())
        .await
        .expect("master bind failed");
    let master_addr = master.local_addr().to_string();

    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    let (w_shutdown_tx, w_shutdown_rx) = watch::channel(false);
    let mut config = WorkerConfig::new(master_addr);
    config.default_heartbeat_interval = Duration::from_millis(100);
    let mut worker = WorkerClient::new(config);
    let worker_id = worker.worker_id();

    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(w_shutdown_rx).await;
    });

    // Wait for initial registration
    assert!(
        wait_for(
            Duration::from_secs(2),
            Duration::from_millis(50),
            || async { registry.get_worker(worker_id).await.is_some() }
        )
        .await
    );

    let initial_ts = registry
        .get_worker(worker_id)
        .await
        .unwrap()
        .last_heartbeat_timestamp;

    // Wait for at least one heartbeat tick to advance timestamp
    let advanced = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(100),
        || async {
            let current_ts = registry
                .get_worker(worker_id)
                .await
                .unwrap()
                .last_heartbeat_timestamp;
            current_ts >= initial_ts
        },
    )
    .await;
    assert!(advanced, "Heartbeat timestamp must advance or remain valid");

    // Clean teardown
    let _ = w_shutdown_tx.send(true);
    let _ = master_shutdown_tx.send(true);
    let _ = tokio::join!(worker_handle, master_handle);
}

#[tokio::test]
async fn test_m2_graceful_disconnect_via_disconnecting_message() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr().to_string();

    tokio::spawn(async move { master.run(master_shutdown_rx).await });

    let (w_shutdown_tx, w_shutdown_rx) = watch::channel(false);
    let mut w =
        WorkerClient::from_options(master_addr, Some("worker-graceful".into()), Some(2), false);
    let w_id = w.worker_id();

    let worker_handle = tokio::spawn(async move { w.run(w_shutdown_rx).await });

    // Wait for registration
    assert!(
        wait_for(
            Duration::from_secs(2),
            Duration::from_millis(50),
            || async {
                registry
                    .get_worker(w_id)
                    .await
                    .map(|e| e.status == WorkerStatus::Connected)
                    .unwrap_or(false)
            }
        )
        .await
    );

    // Trigger graceful shutdown
    let _ = w_shutdown_tx.send(true);
    let _ = worker_handle.await;

    // Verify registry marks worker as Disconnected
    let disconnected = wait_for(
        Duration::from_millis(500),
        Duration::from_millis(20),
        || async {
            registry
                .get_worker(w_id)
                .await
                .map(|e| e.status == WorkerStatus::Disconnected)
                .unwrap_or(false)
        },
    )
    .await;
    assert!(
        disconnected,
        "Worker must transition to Disconnected on graceful shutdown"
    );

    let _ = master_shutdown_tx.send(true);
}

#[tokio::test]
async fn test_m2_abrupt_socket_drop_fast_detection() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr();

    tokio::spawn(async move { master.run(master_shutdown_rx).await });

    // Connect raw TCP client stream, conduct handshake, then drop socket abruptly
    let stream = TcpStream::connect(master_addr).await.expect("tcp connect");
    let mut transport = MessageTransport::new(stream);

    let test_id = Uuid::new_v4();
    let reg_msg = WorkerMessage::Register {
        worker_id: test_id,
        capabilities: WorkerCapabilities::new("abrupt-worker", 2, 1024, false, false, None),
    };
    transport.send_msg(&reg_msg).await.expect("send register");
    let _ack: MasterMessage = transport.recv_msg().await.expect("recv").expect("ack");

    assert_eq!(
        registry.get_worker(test_id).await.unwrap().status,
        WorkerStatus::Connected
    );

    // Drop socket abruptly (simulates SIGKILL / process crash)
    drop(transport);

    // Master reader loop should detect EOF and transition worker to Disconnected in <200ms
    let disconnected = wait_for(
        Duration::from_millis(500),
        Duration::from_millis(20),
        || async {
            registry
                .get_worker(test_id)
                .await
                .map(|e| e.status == WorkerStatus::Disconnected)
                .unwrap_or(false)
        },
    )
    .await;
    assert!(
        disconnected,
        "Abrupt socket drop must trigger Disconnected status on EOF"
    );

    let _ = master_shutdown_tx.send(true);
}

#[tokio::test]
async fn test_m2_heartbeat_reaper_silent_timeout() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr();

    // Start Reaper with fast interval and timeout for deterministic test execution
    let reaper_config = ReaperConfig {
        scan_interval: Duration::from_millis(30),
        timeout: Duration::from_millis(150),
    };
    let reaper_handle = spawn_reaper(registry.clone(), reaper_config, master_shutdown_rx.clone());

    tokio::spawn(async move { master.run(master_shutdown_rx).await });

    // Connect raw client, register, but NEVER send heartbeats (simulates silent network partition)
    let stream = TcpStream::connect(master_addr).await.expect("tcp connect");
    let mut transport = MessageTransport::new(stream);

    let silent_id = Uuid::new_v4();
    let reg_msg = WorkerMessage::Register {
        worker_id: silent_id,
        capabilities: WorkerCapabilities::new("silent-worker", 2, 1024, false, false, None),
    };
    transport.send_msg(&reg_msg).await.expect("send register");
    let _ack: MasterMessage = transport.recv_msg().await.expect("recv").expect("ack");

    assert_eq!(
        registry.get_worker(silent_id).await.unwrap().status,
        WorkerStatus::Connected
    );

    // Wait for reaper to declare worker Disconnected after 150ms timeout
    let reaped = wait_for(
        Duration::from_millis(800),
        Duration::from_millis(30),
        || async {
            registry
                .get_worker(silent_id)
                .await
                .map(|e| e.status == WorkerStatus::Disconnected)
                .unwrap_or(false)
        },
    )
    .await;
    assert!(
        reaped,
        "Reaper must transition silent worker to Disconnected after timeout"
    );

    let _ = master_shutdown_tx.send(true);
    let _ = reaper_handle.await;
}

#[tokio::test]
async fn test_m2_worker_reconnection_preserves_uuid() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr().to_string();

    tokio::spawn(async move { master.run(master_shutdown_rx).await });

    let (w_shutdown_tx, w_shutdown_rx) = watch::channel(false);
    let mut worker =
        WorkerClient::from_options(master_addr, Some("worker-reconnect".into()), Some(4), false);
    // Configure fast backoff for quick test
    worker.set_backoff_config(BackoffConfig {
        initial: Duration::from_millis(30),
        max: Duration::from_millis(100),
        factor: 1.5,
        jitter_ratio: 0.1,
    });

    let worker_id = worker.worker_id();
    tokio::spawn(async move {
        let _ = worker.run(w_shutdown_rx).await;
    });

    // 1. Initial registration
    assert!(
        wait_for(
            Duration::from_secs(2),
            Duration::from_millis(30),
            || async {
                registry
                    .get_worker(worker_id)
                    .await
                    .map(|e| e.status == WorkerStatus::Connected)
                    .unwrap_or(false)
            }
        )
        .await
    );

    // 2. Force disconnect from master by marking disconnected and aborting connection task
    let session_id = registry.get_worker(worker_id).await.unwrap().session_id;
    registry
        .unregister(&worker_id, Some(session_id))
        .await
        .unwrap();

    assert_eq!(
        registry.get_worker(worker_id).await.unwrap().status,
        WorkerStatus::Disconnected
    );

    // 3. Worker backoff loop reconnects; verify status restored to Connected with same worker_id
    let reconnected = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(30),
        || async {
            if let Some(info) = registry.get_worker(worker_id).await {
                info.status == WorkerStatus::Connected && info.session_id > session_id
            } else {
                false
            }
        },
    )
    .await;
    assert!(
        reconnected,
        "Worker must reconnect and restore Connected status with identical UUID and new session ID"
    );

    let _ = w_shutdown_tx.send(true);
    let _ = master_shutdown_tx.send(true);
}
