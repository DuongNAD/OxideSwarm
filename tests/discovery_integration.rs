//! Integration tests for OxideSwarm Master UDP LAN Discovery and Standby Worker Redirection.
//!
//! Verifies:
//! 1. Master UDP beacon broadcasting & unicast probe response (`OX_DISCOVER`).
//! 2. Worker auto-discovery (`master_addr = "auto:<port>"` or `"auto"`).
//! 3. Worker standby redirection portal (HTTP 307 + `/api/status` JSON).
//! 4. Mobile fast-probing simulation across cluster candidate nodes.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::watch;

use rusty_grid_core::discovery::{
    discover_master, MasterBeacon, DISCOVERY_MAGIC_REQUEST, DISCOVERY_SERVICE_NAME,
};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{spawn_worker_redirection_portal, WorkerClient, WorkerConfig};

#[tokio::test]
async fn test_master_beacon_broadcast_and_unicast_query() {
    let discovery_port = 29001;
    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config = ServerConfig::new(master_bind)
        .with_discovery(true)
        .with_discovery_port(discovery_port);

    let master_handle = MasterServer::spawn(config)
        .await
        .expect("Failed to spawn master");
    let master_tcp_port = master_handle.server_addr().port();

    // 1. Probe via core discover_master function
    let discovered = discover_master(Duration::from_secs(2), discovery_port).await;
    assert!(
        discovered.is_some(),
        "discover_master must discover active Master on UDP port {}",
        discovery_port
    );
    let beacon = discovered.unwrap();
    assert_eq!(beacon.service, DISCOVERY_SERVICE_NAME);
    assert!(beacon
        .cluster_addr
        .ends_with(&format!(":{}", master_tcp_port)));
    assert!(!beacon.hostname.is_empty());

    // 2. Direct Unicast UDP probe with DISCOVERY_MAGIC_REQUEST
    let client_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("Bind client udp");
    let target_discovery_addr: SocketAddr =
        format!("127.0.0.1:{}", discovery_port).parse().unwrap();
    client_socket
        .send_to(DISCOVERY_MAGIC_REQUEST, target_discovery_addr)
        .await
        .expect("Send magic request");

    let mut buf = [0u8; 1024];
    let (len, _from) =
        tokio::time::timeout(Duration::from_secs(1), client_socket.recv_from(&mut buf))
            .await
            .expect("Timeout receiving unicast discovery response")
            .expect("Recv error");

    let unicast_beacon: MasterBeacon =
        serde_json::from_slice(&buf[..len]).expect("Deserialize beacon");
    assert_eq!(unicast_beacon.service, DISCOVERY_SERVICE_NAME);
    assert!(unicast_beacon
        .cluster_addr
        .ends_with(&format!(":{}", master_tcp_port)));

    // 3. Negative test: Send invalid magic bytes - master should ignore
    client_socket
        .send_to(b"INVALID_QUERY_XYZ", target_discovery_addr)
        .await
        .expect("Send invalid query");

    let timeout_res = tokio::time::timeout(
        Duration::from_millis(200),
        client_socket.recv_from(&mut buf),
    )
    .await;
    assert!(
        timeout_res.is_err(),
        "Master discovery must ignore unrecognized UDP packets"
    );

    let _ = master_handle.shutdown();
}

#[tokio::test]
async fn test_worker_e2e_connection_via_auto_discovery() {
    let discovery_port = 29002;
    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config = ServerConfig::new(master_bind)
        .with_discovery(true)
        .with_discovery_port(discovery_port);

    let master_handle = MasterServer::spawn(config)
        .await
        .expect("Failed to spawn master");
    let master_registry = master_handle.registry();

    // Configure worker with auto-discovery specifying port
    let temp_dir = tempfile::tempdir().expect("failed to create worker tempdir");
    let worker_config = WorkerConfig::new(format!("auto:{}", discovery_port))
        .with_name("auto-discovery-worker-test")
        .with_cores(2)
        .with_sandbox_base_dir(temp_dir.path());

    let mut worker = WorkerClient::new(worker_config);
    let worker_id = worker.worker_id();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let worker_task = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });

    // Wait for worker to register with master via auto-discovered address
    let start = std::time::Instant::now();
    let mut registered = false;
    while start.elapsed() < Duration::from_secs(5) {
        let workers = master_registry.list_workers().await;
        if workers.iter().any(|w| w.worker_id == worker_id) {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        registered,
        "Worker must successfully discover and register with Master via auto discovery"
    );

    // Clean shutdown
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    let _ = master_handle.shutdown();
}

#[tokio::test]
async fn test_standby_worker_redirection_portal_and_http_probing() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let portal_port = 29003;
    let target_master = "192.168.1.123:8088".to_string();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        spawn_worker_redirection_portal(portal_port, target_master, shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    // 1. HTTP GET / -> 307 Redirect
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", portal_port))
        .await
        .expect("Connect to portal");
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("Send GET /");

    let mut buf = vec![0u8; 2048];
    let n = stream.read(&mut buf).await.expect("Read response");
    let resp = String::from_utf8_lossy(&buf[..n]);

    assert!(resp.contains("307 Temporary Redirect"));
    assert!(resp.contains("Location: http://192.168.1.123:8080"));
    assert!(resp.contains("X-OxideSwarm-Master: 192.168.1.123:8088"));

    // 2. HTTP GET /api/status -> JSON status with WORKER_PORTAL role
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", portal_port))
        .await
        .expect("Connect to portal");
    stream
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("Send GET /api/status");

    let n = stream.read(&mut buf).await.expect("Read response");
    let resp = String::from_utf8_lossy(&buf[..n]);

    assert!(resp.contains("200 OK"));
    assert!(resp.contains("application/json"));
    assert!(resp.contains("\"role\":\"WORKER_PORTAL\""));
    assert!(resp.contains("http://192.168.1.123:8080"));

    let _ = shutdown_tx.send(true);
}

#[tokio::test]
async fn test_mobile_cluster_probe_simulation() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Node A is a standby worker running a redirection portal
    let node_a_port = 29004;
    let (node_a_shutdown_tx, node_a_shutdown_rx) = watch::channel(false);
    let target_master_for_a = "127.0.0.1:29005".to_string();

    tokio::spawn(async move {
        spawn_worker_redirection_portal(node_a_port, target_master_for_a, node_a_shutdown_rx).await;
    });

    // Node B is the active master running web UI simulated on port 29006
    let node_b_port = 29006;
    let node_b_listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", node_b_port))
        .await
        .expect("Bind node B");

    let node_b_task = tokio::spawn(async move {
        if let Ok((mut stream, _)) = node_b_listener.accept().await {
            let mut buf = [0u8; 1024];
            if let Ok(n) = stream.read(&mut buf).await {
                if n > 0 {
                    let body = r#"{"cluster_state":"healthy","active_workers":3,"total_cores":24}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Simulate mobile browser / app querying Candidate A (which is Standby Worker)
    let mut stream_a = TcpStream::connect(format!("127.0.0.1:{}", node_a_port))
        .await
        .expect("Connect to candidate A");
    stream_a
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("Query candidate A");

    let mut buf_a = vec![0u8; 1024];
    let n_a = stream_a.read(&mut buf_a).await.expect("Read candidate A");
    let resp_a = String::from_utf8_lossy(&buf_a[..n_a]);

    // Verify Mobile detects that Node A is a WORKER_PORTAL redirecting to Master
    assert!(resp_a.contains("\"role\":\"WORKER_PORTAL\""));
    assert!(resp_a.contains("\"redirect_url\":\"http://127.0.0.1:8080\""));

    // Simulate mobile querying Candidate B (Master)
    let mut stream_b = TcpStream::connect(format!("127.0.0.1:{}", node_b_port))
        .await
        .expect("Connect to candidate B");
    stream_b
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("Query candidate B");

    let mut buf_b = vec![0u8; 1024];
    let n_b = stream_b.read(&mut buf_b).await.expect("Read candidate B");
    let resp_b = String::from_utf8_lossy(&buf_b[..n_b]);

    // Verify Mobile detects that Node B is the active Master
    assert!(resp_b.contains("cluster_state"));
    assert!(resp_b.contains("healthy"));

    let _ = node_a_shutdown_tx.send(true);
    let _ = node_b_task.await;
}

#[tokio::test]
async fn test_worker_redirection_portal_with_auto_master() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let discovery_port = 29011;
    let portal_port = 29012;
    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config = ServerConfig::new(master_bind)
        .with_discovery(true)
        .with_discovery_port(discovery_port);

    let master_handle = MasterServer::spawn(config)
        .await
        .expect("Failed to spawn master");
    let master_tcp_port = master_handle.server_addr().port();
    let master_registry = master_handle.registry();

    let temp_dir = tempfile::tempdir().expect("worker tempdir");
    let worker_config = WorkerConfig::new(format!("auto:{}", discovery_port))
        .with_name("portal-auto-worker-test")
        .with_cores(2)
        .with_sandbox_base_dir(temp_dir.path())
        .with_redirection_portal(true)
        .with_redirection_portal_port(portal_port);

    let mut worker = WorkerClient::new(worker_config);
    let worker_id = worker.worker_id();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let worker_task = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });

    // Wait for worker to register with master
    let start = std::time::Instant::now();
    let mut registered = false;
    while start.elapsed() < Duration::from_secs(5) {
        let workers = master_registry.list_workers().await;
        if workers.iter().any(|w| w.worker_id == worker_id) {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(registered, "Worker must register with master");

    // Wait for portal to update state
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 1. Query worker portal /api/status - must point to discovered master, NOT "auto" or null!
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", portal_port))
        .await
        .expect("Connect to portal");
    stream
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("Query portal /api/status");

    let mut buf = vec![0u8; 2048];
    let n = stream.read(&mut buf).await.expect("Read response");
    let resp = String::from_utf8_lossy(&buf[..n]);

    assert!(resp.contains("200 OK"));
    assert!(resp.contains("\"role\":\"WORKER_PORTAL\""));
    assert!(resp.contains("\"status\":\"connected_to_master\""));
    assert!(
        resp.contains(&format!(":{}", master_tcp_port)),
        "Must contain master TCP port"
    );

    // 2. Query worker portal / (root navigation) - must return 307 redirecting to master
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", portal_port))
        .await
        .expect("Connect to portal for root GET");
    stream2
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("Query portal /");

    let n2 = stream2.read(&mut buf).await.expect("Read response");
    let resp2 = String::from_utf8_lossy(&buf[..n2]);

    assert!(resp2.contains("307 Temporary Redirect"));
    assert!(resp2.contains(&format!(":{}", master_tcp_port)));

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    let _ = master_handle.shutdown();
}

#[tokio::test]
async fn test_redirection_portal_standby_page_when_no_master() {
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::RwLock;

    let portal_port = 29013;
    let beacon_state: Arc<RwLock<Option<MasterBeacon>>> = Arc::new(RwLock::new(None));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        spawn_worker_redirection_portal(portal_port, beacon_state, shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 1. When master is not found, GET / returns 200 OK with Standby HTML (NOT 307 redirect to loopback!)
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", portal_port))
        .await
        .expect("Connect to portal");
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("Send GET /");

    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.expect("Read response");
    let resp = String::from_utf8_lossy(&buf[..n]);

    assert!(resp.contains("200 OK"), "Standby page should return 200 OK");
    assert!(resp.contains("OxideSwarm Standby Node (Worker)"));
    assert!(resp.contains("probeCluster()"));

    // 2. GET /api/status returns searching_master status
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", portal_port))
        .await
        .expect("Connect to portal");
    stream2
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("Send GET /api/status");

    let n2 = stream2.read(&mut buf).await.expect("Read response");
    let resp2 = String::from_utf8_lossy(&buf[..n2]);

    assert!(resp2.contains("200 OK"));
    assert!(resp2.contains("\"status\":\"searching_master\""));

    let _ = shutdown_tx.send(true);
}

#[tokio::test]
async fn test_http_candidate_probing_fallback() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let test_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = test_listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = test_listener.accept().await {
            let mut req_buf = [0u8; 1024];
            let _ = stream.read(&mut req_buf).await;

            let json = serde_json::json!({
                "master": {
                    "host": "FallBackMaster",
                    "role": "MASTER",
                    "description": "P2P Coordinator"
                },
                "workers": [{"name": "worker-1"}]
            })
            .to_string();

            let resp = format!(
                "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                json.len(),
                json
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.flush().await;
            let _ = stream.shutdown().await;
        }
    });

    let beacon = rusty_grid_core::discovery::probe_http_candidate(
        "127.0.0.1",
        port,
        Duration::from_millis(600),
    )
    .await;

    assert!(
        beacon.is_some(),
        "HTTP probing must discover simulated master"
    );
    let b = beacon.unwrap();
    assert_eq!(b.hostname, "FallBackMaster");
    assert_eq!(b.cluster_addr, "127.0.0.1:8088");
    assert_eq!(b.worker_count, 1);
}
