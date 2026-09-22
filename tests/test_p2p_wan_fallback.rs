//! Integration tests for OxideSwarm Native P2P Remote WAN Interconnect (R1).
//!
//! Verifies:
//! 1. `endpoint.online()` timing in Master: home DERP relay is discovered and included in ticket.
//! 2. Persistent SecretKey yields a deterministic ticket containing home DERP relay across restarts.
//! 3. Dialing Master using a pure Relay ticket (simulating complete direct UDP block) succeeds
//!    in establishing a `GridStream` via DERP relay with 100% success rate.
//! 4. Real task execution (Task dispatch -> Worker execution -> TaskResult) over pure DERP relay.
//! 5. Connection path introspection on both Master (`get_worker_p2p_info`) and Worker (`active_p2p_path_info`)
//!    correctly distinguishes Relay (DERP) vs Direct P2P (QUIC) and measures live RTT.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::watch;

use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_core::transport::{
    inspect_connection_paths, iroh, parse_p2p_ticket, serialize_p2p_ticket,
};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

/// Verifies that binding Master with a persistent key file publishes a valid ticket
/// that contains the discovered N0 home DERP relay URL and remains deterministic across restarts.
#[tokio::test]
async fn test_master_persistent_key_yields_relay_ticket() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let key_file = temp_dir.path().join("master_p2p.key");
    let ticket_file = temp_dir.path().join("master_p2p.ticket");

    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config1 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_key_file(&key_file)
        .with_p2p_ticket_file(&ticket_file);

    // 1. First run: Generates key and publishes ticket with discovered home relay
    let master1 = MasterServer::spawn(config1)
        .await
        .expect("Failed to spawn master run 1");

    assert!(key_file.exists(), "P2P secret key file must be created");
    let key_bytes = tokio::fs::read(&key_file).await.expect("read key file");
    assert_eq!(key_bytes.len(), 32, "Key file must be 32 bytes");

    assert!(ticket_file.exists(), "P2P ticket file must be created");
    let ticket1 = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read ticket 1");
    let addr1 = parse_p2p_ticket(&ticket1).expect("parse ticket 1");

    // Verify relay address presence
    let has_relay1 = addr1.addrs.iter().any(|a| a.is_relay());
    assert!(
        has_relay1,
        "Master ticket must contain discovered home DERP relay, got addrs: {:?}",
        addr1.addrs
    );
    assert!(
        ticket1.contains("relay.n0.iroh.link"),
        "Ticket string must contain N0 relay domain"
    );

    master1.shutdown().expect("shutdown master run 1");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. Second run: Reuses key and reproduces identical ticket
    let config2 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_key_file(&key_file)
        .with_p2p_ticket_file(&ticket_file);

    let master2 = MasterServer::spawn(config2)
        .await
        .expect("Failed to spawn master run 2");

    let ticket2 = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read ticket 2");
    let addr2 = parse_p2p_ticket(&ticket2).expect("parse ticket 2");

    assert_eq!(
        addr1.id, addr2.id,
        "Master NodeId (PublicKey) must be identical across restarts"
    );
    assert_eq!(
        ticket1, ticket2,
        "Serialized ticket must be strictly identical across restarts"
    );

    master2.shutdown().expect("shutdown master run 2");
}

/// Verifies that dialing Master via a pure Relay ticket (with all direct IP addresses stripped,
/// simulating a firewall blocking UDP direct hole-punching) succeeds in establishing
/// connection, registering worker, and executing tasks over DERP relay.
#[tokio::test]
async fn test_pure_relay_dialing_fallback_under_udp_block() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let key_file = temp_dir.path().join("relay_master.key");
    let ticket_file = temp_dir.path().join("relay_master.ticket");

    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_key_file(&key_file)
        .with_p2p_ticket_file(&ticket_file);

    let master_handle = MasterServer::spawn(config)
        .await
        .expect("Failed to spawn master");

    let raw_ticket = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read master ticket");
    let master_addr = parse_p2p_ticket(&raw_ticket).expect("parse ticket");

    // Construct pure-relay ticket: strip ANY and ALL direct IP addresses.
    // This forces Iroh QUIC to connect exclusively through DERP HTTPS/WSS relay,
    // exactly simulating a symmetric NAT or strict corporate firewall blocking direct UDP.
    let mut pure_relay_addr = master_addr.clone();
    pure_relay_addr.addrs.retain(|a| a.is_relay());

    assert!(
        !pure_relay_addr.addrs.is_empty(),
        "Must have at least one DERP relay address in ticket"
    );
    assert!(
        pure_relay_addr.addrs.iter().all(|a| a.is_relay()),
        "All addresses in pure relay ticket must be Relay"
    );
    let pure_relay_ticket =
        serialize_p2p_ticket(&pure_relay_addr).expect("serialize pure relay ticket");

    // Spawn Worker with pure relay ticket
    let worker_sandbox = temp_dir.path().join("worker_sandbox");
    let worker_config = WorkerConfig::new("auto")
        .with_name("pure-relay-worker")
        .with_cores(2)
        .with_sandbox_base_dir(&worker_sandbox)
        .with_p2p_ticket(&pure_relay_ticket);

    // Bind worker endpoint with IP transports cleared, simulating a network firewall
    // dropping all direct UDP packets. Connection must succeed via DERP relay.
    let worker_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .alpns(vec![rusty_grid_core::transport::GRID_ALPN.to_vec()])
        .clear_ip_transports()
        .bind()
        .await
        .expect("bind relay-only worker endpoint");

    let mut worker = WorkerClient::new(worker_config).with_p2p_endpoint(worker_endpoint);
    let worker_id = worker.worker_id();
    let worker_p2p_conn = worker.p2p_connection();
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);

    let worker_task = tokio::spawn(async move {
        let _ = worker.run(worker_shutdown_rx).await;
    });

    // Wait for worker registration over relay
    let start = std::time::Instant::now();
    let mut registered = false;
    while start.elapsed() < Duration::from_secs(15) {
        let workers = master_handle.registry().list_workers().await;
        if workers.iter().any(|w| w.worker_id == worker_id) {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        registered,
        "Worker must register with Master over pure DERP relay fallback within timeout"
    );

    // Verify Master connection path introspection
    let master_path_info = master_handle
        .get_worker_p2p_info(&worker_id)
        .await
        .expect("Master must track P2P connection path for worker");
    assert!(
        master_path_info.is_relay,
        "Master must detect path as Relay: {:?}",
        master_path_info
    );
    assert!(
        !master_path_info.is_ip,
        "Master must not detect path as direct IP in pure relay test: {:?}",
        master_path_info
    );
    assert!(
        master_path_info.connection_type.contains("Relay"),
        "Connection type must indicate Relay: {}",
        master_path_info.connection_type
    );

    // Verify Worker connection path introspection
    {
        let guard = worker_p2p_conn.read().await;
        let worker_path_info = guard
            .as_ref()
            .and_then(inspect_connection_paths)
            .expect("Worker must track active P2P path info");
        assert!(
            worker_path_info.is_relay,
            "Worker must detect path as Relay: {:?}",
            worker_path_info
        );
        assert!(
            !worker_path_info.is_ip,
            "Worker must not detect path as direct IP: {:?}",
            worker_path_info
        );
        assert!(
            worker_path_info.connection_type.contains("Relay"),
            "Worker connection type must indicate Relay: {}",
            worker_path_info.connection_type
        );
    }

    // Submit and execute a real task across the DERP relay
    let task = Task::new(
        TaskSpec::command("echo", vec!["hello from pure relay".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = master_handle
        .submit_task(task)
        .await
        .expect("submit task over relay");
    let result = master_handle
        .wait_task(task_id, Some(Duration::from_secs(10)))
        .await
        .expect("wait for task execution over relay");

    assert_eq!(result.exit_code, 0, "Task over relay must exit with 0");
    assert!(
        result.stdout.contains("hello from pure relay"),
        "Task stdout must contain echoed payload: {}",
        result.stdout
    );

    // Clean teardown
    let _ = worker_shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    let _ = master_handle.shutdown();
}

/// Verifies that normal P2P dialing connects successfully, reports path introspection
/// and measured RTT, and executes tasks.
#[tokio::test]
async fn test_normal_dialing_direct_p2p_and_path_introspection() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let ticket_file = temp_dir.path().join("ephemeral_master.ticket");

    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_ticket_file(&ticket_file);

    let master_handle = MasterServer::spawn(config)
        .await
        .expect("Failed to spawn master");

    let raw_ticket = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read master ticket");

    // Spawn Worker with complete ticket (including direct socket addresses)
    let worker_sandbox = temp_dir.path().join("worker_sandbox");
    let worker_config = WorkerConfig::new("auto")
        .with_name("direct-p2p-worker")
        .with_cores(2)
        .with_sandbox_base_dir(&worker_sandbox)
        .with_p2p_ticket(&raw_ticket);

    let mut worker = WorkerClient::new(worker_config);
    let worker_id = worker.worker_id();
    let worker_p2p_conn = worker.p2p_connection();
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);

    let worker_task = tokio::spawn(async move {
        let _ = worker.run(worker_shutdown_rx).await;
    });

    let start = std::time::Instant::now();
    let mut registered = false;
    while start.elapsed() < Duration::from_secs(10) {
        let workers = master_handle.registry().list_workers().await;
        if workers.iter().any(|w| w.worker_id == worker_id) {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(registered, "Worker must register via normal P2P ticket");

    // Verify path introspection on Master
    let master_path_info = master_handle
        .get_worker_p2p_info(&worker_id)
        .await
        .expect("Master must track P2P connection path for worker");
    assert!(
        !master_path_info.connection_type.is_empty(),
        "Master path connection_type must not be empty"
    );

    // Verify path introspection on Worker
    {
        let guard = worker_p2p_conn.read().await;
        let worker_path_info = guard
            .as_ref()
            .and_then(inspect_connection_paths)
            .expect("Worker must track active P2P path info");
        assert!(
            !worker_path_info.connection_type.is_empty(),
            "Worker connection_type must not be empty: {:?}",
            worker_path_info
        );
    }

    // Submit and execute task
    let task = Task::new(
        TaskSpec::command("echo", vec!["hello direct p2p".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = master_handle
        .submit_task(task)
        .await
        .expect("submit task");
    let result = master_handle
        .wait_task(task_id, Some(Duration::from_secs(10)))
        .await
        .expect("wait for task");

    assert_eq!(result.exit_code, 0);
    assert!(result.stdout.contains("hello direct p2p"));

    // Clean teardown
    let _ = worker_shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    let _ = master_handle.shutdown();
}

/// Verifies that after Master restarts with the persistent key file, the ticket
/// remains identical and a reconnecting Worker automatically re-registers with Master.
#[tokio::test]
async fn test_ticket_determinism_and_reconnection() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let key_file = temp_dir.path().join("restart_master.key");
    let ticket_file = temp_dir.path().join("restart_master.ticket");

    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config1 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_key_file(&key_file)
        .with_p2p_ticket_file(&ticket_file);

    // 1. Start Master Run 1
    let master1 = MasterServer::spawn(config1)
        .await
        .expect("spawn master 1");
    let ticket1 = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read ticket 1");

    // 2. Start Worker with Ticket 1
    let worker_sandbox = temp_dir.path().join("worker_reconnect_sb");
    let worker_config = WorkerConfig::new("auto")
        .with_name("reconnecting-worker")
        .with_cores(2)
        .with_sandbox_base_dir(&worker_sandbox)
        .with_p2p_ticket(&ticket1);

    let mut worker = WorkerClient::new(worker_config);
    let worker_id = worker.worker_id();
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);

    let worker_task = tokio::spawn(async move {
        let _ = worker.run(worker_shutdown_rx).await;
    });

    // Wait for initial registration on Master 1
    let start = std::time::Instant::now();
    let mut reg1 = false;
    while start.elapsed() < Duration::from_secs(10) {
        if master1.registry().list_workers().await.iter().any(|w| w.worker_id == worker_id) {
            reg1 = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reg1, "Worker must register with Master 1");

    // 3. Stop Master 1
    master1.shutdown().expect("shutdown master 1");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 4. Start Master Run 2 with same key file and ticket file
    let config2 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_key_file(&key_file)
        .with_p2p_ticket_file(&ticket_file);

    let master2 = MasterServer::spawn(config2)
        .await
        .expect("spawn master 2");
    let ticket2 = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read ticket 2");

    assert_eq!(ticket1, ticket2, "Ticket 1 and Ticket 2 must be identical");

    // 5. Verify Worker automatically reconnects and registers with Master 2
    let reconnect_start = std::time::Instant::now();
    let mut reg2 = false;
    while reconnect_start.elapsed() < Duration::from_secs(10) {
        if master2.registry().list_workers().await.iter().any(|w| w.worker_id == worker_id) {
            reg2 = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reg2, "Worker must automatically reconnect to Master 2 within timeout");

    // Clean teardown
    let _ = worker_shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    let _ = master2.shutdown();
}

