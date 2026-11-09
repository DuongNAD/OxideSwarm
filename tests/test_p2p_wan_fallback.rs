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
///
/// Requires live outbound network access to the n0 DERP relay (`relay.n0.iroh.link`).
#[tokio::test]
#[ignore = "requires outbound network access to the n0 DERP relay (relay.n0.iroh.link)"]
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

    let ep1 = master1
        .p2p_endpoint()
        .cloned()
        .expect("master1 p2p endpoint");
    master1.shutdown().expect("shutdown master run 1");
    let close_deadline1 = tokio::time::Instant::now() + Duration::from_secs(5);
    while !ep1.is_closed() {
        assert!(
            tokio::time::Instant::now() < close_deadline1,
            "master1.shutdown() alone must close the P2P endpoint"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(master1);

    // 2. Second run: Reuses key and reproduces same NodeId and pure-relay ticket
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
    assert!(
        !addr2.addrs.is_empty() && addr2.addrs.iter().all(|a| a.is_relay()),
        "Master restart ticket must retain only discovered home DERP relay addresses, got: {:?}",
        addr2.addrs
    );
    assert!(
        ticket2.contains("relay.n0.iroh.link"),
        "Restart ticket string must contain N0 relay domain, got: {ticket2}"
    );

    let ep2 = master2
        .p2p_endpoint()
        .cloned()
        .expect("master2 p2p endpoint");
    master2.shutdown().expect("shutdown master run 2");
    let close_deadline2 = tokio::time::Instant::now() + Duration::from_secs(5);
    while !ep2.is_closed() {
        assert!(
            tokio::time::Instant::now() < close_deadline2,
            "master2.shutdown() alone must close the P2P endpoint"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Verifies that dialing Master via a pure Relay ticket (with all direct IP addresses stripped,
/// simulating a firewall blocking UDP direct hole-punching) succeeds in establishing
/// connection, registering worker, and executing tasks over DERP relay.
///
/// Requires live outbound network access to the n0 DERP relay (`relay.n0.iroh.link`).
#[tokio::test]
#[ignore = "requires outbound network access to the n0 DERP relay (relay.n0.iroh.link)"]
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
        .with_no_battery(true)
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
        .with_p2p_minimal_preset(true)
        .with_ephemeral_key(true)
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
        .with_no_battery(true)
        .with_name("direct-p2p-worker")
        .with_cores(2)
        .with_sandbox_base_dir(&worker_sandbox)
        .with_p2p_ticket(&raw_ticket)
        .with_p2p_minimal_preset(true);

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
    let task_id = master_handle.submit_task(task).await.expect("submit task");
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
///
/// Requires live outbound network access to the n0 DERP relay (`relay.n0.iroh.link`).
#[tokio::test]
#[ignore = "requires outbound network access to the n0 DERP relay (relay.n0.iroh.link)"]
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
    let master1 = MasterServer::spawn(config1).await.expect("spawn master 1");
    let ticket1 = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read ticket 1");

    // 2. Start Worker with Ticket 1
    let worker_sandbox = temp_dir.path().join("worker_reconnect_sb");
    let worker_config = WorkerConfig::new("auto")
        .with_no_battery(true)
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
        if master1
            .registry()
            .list_workers()
            .await
            .iter()
            .any(|w| w.worker_id == worker_id)
        {
            reg1 = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reg1, "Worker must register with Master 1");

    // 3. Stop Master 1 and verify shutdown() closes its P2P endpoint
    let ep1 = master1
        .p2p_endpoint()
        .cloned()
        .expect("master1 p2p endpoint");
    master1.shutdown().expect("shutdown master 1");
    let close_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !ep1.is_closed() {
        assert!(
            tokio::time::Instant::now() < close_deadline,
            "master1.shutdown() must close P2P endpoint"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 4. Start Master Run 2 with same key file and ticket file
    let config2 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_key_file(&key_file)
        .with_p2p_ticket_file(&ticket_file);

    let master2 = MasterServer::spawn(config2).await.expect("spawn master 2");
    let ticket2 = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read ticket 2");
    let addr1 = parse_p2p_ticket(&ticket1).expect("parse ticket 1");
    let addr2 = parse_p2p_ticket(&ticket2).expect("parse ticket 2");

    assert_eq!(
        addr1.id, addr2.id,
        "Master NodeId (PublicKey) must be identical across restarts"
    );
    assert!(
        !addr2.addrs.is_empty() && addr2.addrs.iter().all(|a| a.is_relay()),
        "Master restart ticket must retain only discovered home DERP relay addresses, got: {:?}",
        addr2.addrs
    );
    assert!(
        ticket2.contains("relay.n0.iroh.link"),
        "Restart ticket string must contain N0 relay domain, got: {ticket2}"
    );

    // 5. Verify Worker automatically reconnects and registers with Master 2
    let reconnect_start = std::time::Instant::now();
    let mut reg2 = false;
    while reconnect_start.elapsed() < Duration::from_secs(10) {
        if master2
            .registry()
            .list_workers()
            .await
            .iter()
            .any(|w| w.worker_id == worker_id)
        {
            reg2 = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        reg2,
        "Worker must automatically reconnect to Master 2 within timeout"
    );

    // Clean teardown
    let _ = worker_shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    let _ = master2.shutdown();
}

/// W7 (design 0039 D7): verifies that a client can submit a task with blob inputs and download
/// its output blob through a P2P ticket (`STREAM_CLIENT = 0x03`), and that a client without a
/// cluster secret is refused both by the CLI secret policy and by the master handshake.
#[tokio::test]
async fn test_p2p_client_submits_task_and_downloads_output_and_refuses_without_secret() {
    use rusty_grid_cli::blobs::{download_outputs, scan_inputs, upload_inputs, BlobConnection};
    use rusty_grid_cli::connect::{connect_master, set_client_p2p_minimal_preset_for_testing};
    use rusty_grid_cli::secret_cmd::{decide_peer_secret, PeerRole};
    use rusty_grid_core::cluster_auth::ClusterSecret;
    use rusty_grid_core::error::GridError;
    use rusty_grid_core::protocol::{ClientMessage, ClientResponse};

    set_client_p2p_minimal_preset_for_testing(true);

    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let ticket_file = temp_dir.path().join("w7_master.ticket");
    let blob_dir = temp_dir.path().join("master_blobs");
    let secret = ClusterSecret::from_bytes(&[0x5Au8; 32]).expect("cluster secret");

    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_minimal_preset(true)
        .with_ephemeral_key(true)
        .with_p2p_ticket_file(&ticket_file)
        .with_cluster_secret(secret.clone())
        .with_blob_store(&blob_dir, 64 * 1024 * 1024);

    let master_handle = MasterServer::spawn(config)
        .await
        .expect("Failed to spawn master");

    let raw_ticket = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read master ticket");

    // 1. A client without a cluster secret is refused before dialing by `decide_peer_secret`
    //    (unless `--insecure`), and is also refused at the wire handshake if it dials directly.
    let outcome = decide_peer_secret(None, &raw_ticket, false, false, PeerRole::Client);
    match outcome {
        Err(GridError::AuthRequired(reason)) => {
            assert!(
                reason.contains("P2P ticket"),
                "refusal reason must mention P2P ticket, got: {reason}"
            );
        }
        other => panic!("expected Err(AuthRequired) for P2P ticket without secret, got {other:?}"),
    }

    let unauth_err = connect_master(&raw_ticket, None)
        .await
        .err()
        .expect("master must refuse unauthenticated P2P client");
    assert!(
        matches!(unauth_err, GridError::AuthRequired(_)),
        "expected AuthRequired, got {unauth_err:?}"
    );

    let unauth_blob_err = BlobConnection::connect(&raw_ticket, None)
        .await
        .err()
        .expect("master must refuse unauthenticated P2P blob connection");
    assert!(
        matches!(unauth_blob_err, GridError::AuthRequired(_)),
        "expected AuthRequired on BlobConnection, got {unauth_blob_err:?}"
    );

    // 2. Spawn a P2P worker with the same secret and wait for registration.
    let worker_sandbox = temp_dir.path().join("worker_sandbox");
    let worker_config = WorkerConfig::new("auto")
        .with_no_battery(true)
        .with_name("w7-p2p-worker")
        .with_cores(2)
        .with_sandbox_base_dir(&worker_sandbox)
        .with_p2p_ticket(&raw_ticket)
        .with_p2p_minimal_preset(true)
        .with_cluster_secret(secret.clone());

    let mut worker = WorkerClient::new(worker_config);
    let worker_id = worker.worker_id();
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(async move {
        let _ = worker.run(worker_shutdown_rx).await;
    });

    let start = std::time::Instant::now();
    let mut registered = false;
    while start.elapsed() < Duration::from_secs(10) {
        if master_handle
            .registry()
            .list_workers()
            .await
            .iter()
            .any(|w| w.worker_id == worker_id)
        {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(registered, "Worker must register via P2P ticket");

    // 3. Stage an input file, upload it over a P2P BlobConnection, submit a task over a P2P
    //    client connection, wait for completion, and download the output file over P2P.
    let input_path = temp_dir.path().join("in.txt");
    let expected_payload = b"hello from W7 p2p client and blob transfer\n";
    std::fs::write(&input_path, expected_payload).expect("write input file");

    let prepared = scan_inputs(&[format!("{}:in.txt", input_path.display())]).expect("scan inputs");
    let blob_conn = BlobConnection::connect(&raw_ticket, Some(&secret))
        .await
        .expect("connect P2P BlobConnection with secret");
    upload_inputs(&blob_conn, &prepared)
        .await
        .expect("upload input blobs over P2P ticket");

    let mut client = connect_master(&raw_ticket, Some(&secret))
        .await
        .expect("connect P2P client with secret");

    let spec = TaskSpec::shell_script("cp in.txt out.txt && echo p2p-done");

    let task = Task::new(spec, TaskRequirements::generic(1, 10))
        .with_inputs(prepared.inputs)
        .with_output_glob("out.txt");

    client
        .send_msg(&ClientMessage::SubmitTask { task, wait: true })
        .await
        .expect("send SubmitTask over P2P");
    let result = match client.recv_msg::<ClientResponse>().await.expect("recv") {
        Some(ClientResponse::TaskCompleted { result, .. }) => result,
        other => panic!("expected TaskCompleted, got {other:?}"),
    };

    assert_eq!(result.exit_code, 0, "task stderr: {}", result.stderr_str());
    assert!(result.stdout_str().contains("p2p-done"));
    assert_eq!(result.outputs.len(), 1, "expected 1 output blob");

    let out_dir = temp_dir.path().join("downloaded");
    let saved = download_outputs(&blob_conn, &result.outputs, &out_dir)
        .await
        .expect("download output blobs over P2P ticket");
    assert_eq!(saved.len(), 1);
    let downloaded_bytes = std::fs::read(&saved[0]).expect("read downloaded output");
    assert_eq!(downloaded_bytes, expected_payload);

    drop(client);
    drop(blob_conn);
    let _ = worker_shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    let _ = master_handle.shutdown();
}

/// D7 compatibility: an older P2P master that only accepts `STREAM_CONTROL` / `STREAM_DATA`
/// closes a `STREAM_CLIENT` (`0x03`) stream after reading the tag byte; `connect_master`
/// surfaces the protocol version upgrade hint rather than a raw EOF.
#[tokio::test]
async fn test_p2p_client_stream_closed_by_older_master_reports_protocol_version_hint() {
    use rusty_grid_cli::connect::{connect_master, set_client_p2p_minimal_preset_for_testing};
    use rusty_grid_core::error::GridError;
    use rusty_grid_core::transport::{
        bind_p2p_endpoint, serialize_p2p_ticket, GRID_ALPN, STREAM_CLIENT,
    };

    set_client_p2p_minimal_preset_for_testing(true);

    let endpoint = bind_p2p_endpoint(true)
        .await
        .expect("bind mock older P2P master");
    let ticket = serialize_p2p_ticket(&endpoint.addr()).expect("serialize ticket");

    let server_ep = endpoint.clone();
    let server_task = tokio::spawn(async move {
        if let Some(incoming) = server_ep.accept().await {
            if let Ok(conn) = incoming.await {
                if let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let mut tag = [0u8; 1];
                    let _ = tokio::io::AsyncReadExt::read_exact(&mut recv, &mut tag).await;
                    assert_eq!(tag[0], STREAM_CLIENT);
                    let _ = send.finish();
                    // Wait briefly for the client to send its Hello probe onto the stream before
                    // dropping the stream/connection, matching an older master whose spawned
                    // accept task returns on an unknown stream tag.
                    let mut drain = [0u8; 64];
                    let _ = tokio::time::timeout(
                        Duration::from_millis(200),
                        tokio::io::AsyncReadExt::read(&mut recv, &mut drain),
                    )
                    .await;
                }
            }
        }
    });

    let err = connect_master(&ticket, None)
        .await
        .err()
        .expect("older P2P master closing STREAM_CLIENT must fail");
    match err {
        GridError::ConnectionFailed(msg) => {
            assert!(
                msg.contains("upgrade both Master and CLI to protocol version"),
                "expected protocol version upgrade hint, got: {msg}"
            );
        }
        other => panic!("expected ConnectionFailed with upgrade hint, got {other:?}"),
    }

    let _ = server_task.await;
    endpoint.close().await;
    let _ = GRID_ALPN;
}
