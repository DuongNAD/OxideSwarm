use super::*;
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::protocol::{
    ClientMessage, ClientResponse, EndpointRole, ProtocolCapabilities, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::Task;
use rusty_grid_core::{MasterMessage, MessageTransport, WorkerCapabilities, WorkerMessage};
use tokio::net::TcpStream;

use crate::queue::TaskState;

#[tokio::test]
async fn test_master_server_bind_ephemeral_and_port_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let port_file = temp_dir.path().join("master.port");

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_port_file(&port_file)
        .with_handshake_timeout(2);

    let registry = WorkerRegistry::new();
    let server = MasterServer::bind(config, registry).await.unwrap();

    assert!(server.port() > 0);
    assert!(port_file.exists());

    let content = std::fs::read_to_string(&port_file).unwrap();
    assert_eq!(content.trim(), server.port().to_string());
}

#[tokio::test]
async fn test_master_server_handshake_and_reject_zero_cores() {
    let server = MasterServer::bind_addr("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        server.run(shutdown_rx).await.unwrap();
    });

    // Connect client with 0 CPU cores -> should be rejected
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let bad_msg = WorkerMessage::Register {
        worker_id: Uuid::new_v4(),
        capabilities: WorkerCapabilities::new("bad-worker", 0, 1024, false, false, None),
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: ProtocolCapabilities::all_supported(),
    };
    transport.send_msg(&bad_msg).await.unwrap();

    let resp: Option<MasterMessage> = transport.recv_msg().await.unwrap();
    match resp {
        Some(MasterMessage::RegisterAck { accepted, .. }) => {
            assert!(!accepted, "Master must reject worker with 0 cores");
        }
        other => panic!("Expected RegisterAck, got {other:?}"),
    }

    let _ = shutdown_tx.send(true);
}

#[tokio::test]
async fn test_server_config_web_ui_options() {
    let default_cfg = ServerConfig::default();
    assert!(!default_cfg.enable_web_ui);

    let ephemeral_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    assert!(!ephemeral_cfg.enable_web_ui);

    let explicit_cfg = ServerConfig::new("127.0.0.1:8088".parse().unwrap());
    assert!(explicit_cfg.enable_web_ui);

    let custom_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap());
    assert!(custom_cfg.enable_web_ui);
    assert_eq!(custom_cfg.web_ui_addr.port(), 0);
}

#[tokio::test]
async fn test_master_server_spawn_with_ephemeral_web_ui() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap());
    let handle = MasterServer::spawn(config).await.unwrap();
    assert!(handle.server_addr().port() > 0);
    let _ = handle.shutdown();
}

#[tokio::test]
async fn test_master_server_discovery_service() {
    let disc_port = 18089;
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_discovery(true)
        .with_discovery_port(disc_port);

    let handle = MasterServer::spawn(config).await.unwrap();
    assert!(handle.server_addr().port() > 0);

    // Discovery probe using the configured discovery port
    let discovered =
        rusty_grid_core::discovery::discover_master(Duration::from_millis(1500), disc_port).await;

    assert!(
        discovered.is_some(),
        "Master discovery service should respond to probe"
    );
    let beacon = discovered.unwrap();
    assert_eq!(
        beacon.service,
        rusty_grid_core::discovery::DISCOVERY_SERVICE_NAME
    );
    assert!(beacon
        .cluster_addr
        .ends_with(&format!(":{}", handle.server_addr().port())));

    let _ = handle.shutdown();
}

#[tokio::test]
async fn test_loopback_master_binds_discovery_socket_to_loopback_not_all_interfaces() {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    // 1. Pure address mapping: loopback listen IPs bind to loopback; non-loopback and
    //    unspecified listen IPs bind to 0.0.0.0.
    assert_eq!(
        discovery_bind_addr(IpAddr::V4(Ipv4Addr::LOCALHOST), 8089),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8089)
    );
    assert_eq!(
        discovery_bind_addr(IpAddr::V6(Ipv6Addr::LOCALHOST), 8089),
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 8089)
    );
    assert_eq!(
        discovery_bind_addr(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8089),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8089)
    );
    assert_eq!(
        discovery_bind_addr("192.0.2.10".parse().unwrap(), 8089),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8089)
    );

    // 2. Live loopback master: binds 127.0.0.1:<disc_port>, answers unicast probes on
    //    127.0.0.1:<disc_port>, and leaves non-loopback interfaces unbound.
    // Bind-and-drop kept (TEST-52): the master never publishes the UDP port its beacon binds.
    let probe_sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let disc_port = probe_sock.local_addr().unwrap().port();
    drop(probe_sock);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_discovery(true)
        .with_discovery_port(disc_port);

    let handle = MasterServer::spawn(config).await.unwrap();
    assert!(handle.server_addr().ip().is_loopback());

    // Wait until the loopback discovery socket is bound and answering unicast probes.
    let discovered =
        rusty_grid_core::discovery::discover_master(Duration::from_millis(1500), disc_port)
            .await
            .expect("loopback master discovery socket should answer unicast probe");
    assert_eq!(discovered.cluster_addr, handle.server_addr().to_string());

    // 127.0.0.1:<disc_port> is occupied by the master's loopback discovery socket.
    assert!(
        std::net::UdpSocket::bind(("127.0.0.1", disc_port)).is_err(),
        "loopback master should hold 127.0.0.1:{disc_port}"
    );

    // Non-loopback interface on the same port must remain free (proving the master did not
    // bind 0.0.0.0:<disc_port>).
    let lan_ip = rusty_grid_core::discovery::get_local_ip_or_loopback();
    if !lan_ip.is_loopback() {
        let lan_bind = std::net::UdpSocket::bind((lan_ip, disc_port));
        assert!(
            lan_bind.is_ok(),
            "loopback master must not bind 0.0.0.0:{disc_port} (binding {lan_ip}:{disc_port} failed: {:?})",
            lan_bind.err()
        );
    }

    let _ = handle.shutdown();
}

#[test]
fn test_beacon_advertises_the_dashboard_where_it_really_listens() {
    use std::net::{IpAddr, Ipv4Addr};
    let lan: IpAddr = "192.0.2.7".parse().unwrap();
    let addr = |text: &str| Some(text.parse::<SocketAddr>().unwrap());

    // Loopback (the default) or no dashboard at all: nobody on the network can reach it.
    assert_eq!(advertised_web_ui_url(addr("127.0.0.1:8080"), lan), "");
    assert_eq!(advertised_web_ui_url(addr("[::1]:8080"), lan), "");
    assert_eq!(advertised_web_ui_url(None, lan), "");
    // A wildcard bind is reached through the machine's LAN address...
    assert_eq!(
        advertised_web_ui_url(addr("0.0.0.0:8081"), lan),
        "http://192.0.2.7:8081"
    );
    // ...unless the machine has none.
    assert_eq!(
        advertised_web_ui_url(addr("0.0.0.0:8081"), IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ""
    );
    // A specific interface is advertised as bound, whatever the LAN address says.
    assert_eq!(
        advertised_web_ui_url(addr("198.51.100.2:9000"), lan),
        "http://198.51.100.2:9000"
    );
    assert_eq!(
        advertised_web_ui_url(addr("[2001:db8::1]:9000"), lan),
        "http://[2001:db8::1]:9000"
    );
}

#[cfg(feature = "dashboard")]
#[tokio::test]
async fn test_beacon_of_a_master_with_a_loopback_dashboard_advertises_no_dashboard() {
    let disc_port = 18091;
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
        .with_discovery(true)
        .with_discovery_port(disc_port);
    let handle = MasterServer::spawn(config).await.unwrap();
    assert!(handle.dashboard_addr().unwrap().ip().is_loopback());

    let beacon =
        rusty_grid_core::discovery::discover_master(Duration::from_millis(1500), disc_port)
            .await
            .expect("the master answers the probe");
    assert!(beacon
        .cluster_addr
        .ends_with(&format!(":{}", handle.server_addr().port())));
    assert_eq!(
        beacon.web_ui_url, "",
        "the dashboard listens on loopback only: advertising a LAN URL would point nowhere"
    );
    let _ = handle.shutdown();
}

#[tokio::test]
async fn test_timed_out_wait_task_does_not_leak_its_waiter() {
    let handle = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    // No worker: the task stays queued, so the wait times out.
    let task = Task::new(
        rusty_grid_core::task::TaskSpec::command("echo", vec!["x".into()]),
        rusty_grid_core::task::TaskRequirements::generic(1, 30),
    );
    let task_id = handle.submit_task(task).await.unwrap();
    for _ in 0..3 {
        let err = handle
            .wait_task(task_id, Some(Duration::from_millis(30)))
            .await
            .unwrap_err();
        assert!(matches!(err, GridError::Timeout { .. }), "{err}");
    }
    assert!(
        handle.waiters.read().await.get(&task_id).is_none(),
        "timed-out waits must not accumulate dead senders until the task ends"
    );

    // A live waiter is not disturbed by another waiter timing out.
    let live = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.wait_task(task_id, None).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = handle
        .wait_task(task_id, Some(Duration::from_millis(30)))
        .await;
    assert_eq!(
        handle.waiters.read().await.get(&task_id).map(Vec::len),
        Some(1)
    );
    handle.cancel_task(task_id).await.unwrap();
    let cancelled = live.await.unwrap().unwrap();
    assert_eq!(cancelled.exit_code, 130);
    assert!(handle.waiters.read().await.get(&task_id).is_none());
    let _ = handle.shutdown();
}

#[tokio::test]
async fn test_superseded_session_sweep_survives_the_abort_of_its_caller() {
    // The caller of the sweep is the new session's handler; a third registration aborts it
    // while the sweep waits for the queue lock. The tasks must still be failed over.
    let queue = TaskQueue::new();
    let notify = Arc::new(tokio::sync::Notify::new());
    let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
    let worker_id = Uuid::new_v4();
    let task = Task::new(
        rusty_grid_core::task::TaskSpec::command("echo", vec!["x".into()]),
        rusty_grid_core::task::TaskRequirements::generic(1, 30),
    );
    let task_id = queue.submit(task).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(task_id, worker_id, 1)])
        .await;
    queue
        .mark_running_from(worker_id, Some(1), &task_id)
        .await
        .unwrap();

    let lock = queue.hold_write_lock_for_test().await;
    let caller = {
        let (queue, notify, waiters) = (queue.clone(), notify.clone(), waiters.clone());
        tokio::spawn(async move {
            failover_superseded_session(
                &queue,
                &notify,
                &waiters,
                worker_id,
                1,
                2,
                #[cfg(feature = "dashboard")]
                None,
            )
            .await;
        })
    };
    // Let the sweep start and park on the lock, then abort its caller like a third
    // registration of the same worker id does.
    tokio::time::sleep(Duration::from_millis(100)).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(lock);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while queue.get_state(&task_id).await != Some(TaskState::Queued) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the task is still assigned to the dead session 1"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(queue.get_task(&task_id).await.unwrap().retry_count, 1);
}

// --- Step B: accept-loop robustness, connection budgets, key file permissions ---

#[test]
fn test_accept_backoff_grows_to_a_ceiling_and_starts_over_after_a_success() {
    let mut backoff = AcceptBackoff::new();
    let pauses: Vec<u64> = (0..8)
        .map(|_| backoff.failure().as_millis() as u64)
        .collect();
    assert_eq!(pauses, vec![50, 100, 200, 400, 800, 1000, 1000, 1000]);
    backoff.reset();
    assert_eq!(backoff.failure(), Duration::from_millis(50));
}

#[test]
fn test_connection_limits_keep_two_separate_budgets_and_do_not_leak_permits() {
    let limits = ConnectionLimits::new(2, 1);
    let (first_conn, first_handshake) = limits.admit().expect("first peer fits");

    // The pre-auth budget (1) is used up although a connection slot is free, and the refused
    // attempt must not keep the connection slot it briefly took.
    assert!(limits.admit().is_none());
    assert_eq!(limits.connections.available_permits(), 1);

    // The first peer authenticated: its pre-auth slot is back, its connection slot is not.
    drop(first_handshake);
    let (second_conn, second_handshake) = limits.admit().expect("second peer fits");
    drop(second_handshake);
    assert!(
        limits.admit().is_none(),
        "two open connections exhaust a budget of two"
    );

    drop(first_conn);
    assert!(
        limits.admit().is_some(),
        "a closed connection frees its slot"
    );
    drop(second_conn);
}

#[cfg(all(feature = "p2p", unix))]
#[tokio::test]
async fn test_p2p_master_key_is_created_private_and_a_loose_one_is_tightened() {
    use std::os::unix::fs::PermissionsExt;
    let mode_of = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("oxideswarm-home").join("master_key.bin");
    let created = resolve_p2p_secret_key(Some(&path)).await.unwrap();
    assert_eq!(
        mode_of(&path),
        0o600,
        "a new key must be readable by its owner only"
    );
    assert_eq!(
        mode_of(path.parent().unwrap()),
        0o700,
        "so must the directory created for it"
    );

    // Reloading keeps the identity and the mode.
    let reloaded = resolve_p2p_secret_key(Some(&path)).await.unwrap();
    assert_eq!(created.to_bytes(), reloaded.to_bytes());
    assert_eq!(mode_of(&path), 0o600);

    // A key left world-readable by an older version (or by hand) is tightened on start.
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let tightened = resolve_p2p_secret_key(Some(&path)).await.unwrap();
    assert_eq!(
        created.to_bytes(),
        tightened.to_bytes(),
        "tightening must not regenerate the key"
    );
    assert_eq!(mode_of(&path), 0o600);

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o664)).unwrap();
    resolve_p2p_secret_key(Some(&path)).await.unwrap();
    assert_eq!(mode_of(&path), 0o600);
}

#[test]
fn test_dashboard_defaults_to_loopback() {
    let config = ServerConfig::default();
    assert!(config.web_ui_addr.ip().is_loopback());
    assert!(config.dashboard_bind_ip.is_none());
    // Binding the grid listener to every interface does not move the dashboard.
    let wide = ServerConfig::new("0.0.0.0:8088".parse().unwrap());
    assert!(wide.web_ui_addr.ip().is_loopback());
    assert!(
        !config.implicit_local_dashboard,
        "the library default stays strict"
    );
}

#[tokio::test]
async fn test_master_with_larger_max_active_bytes_accepts_what_default_refuses_and_reports_bytes() {
    let make_task = |stdin_len: usize| {
        Task::new(
            rusty_grid_core::task::TaskSpec::Command {
                program: "echo".into(),
                args: vec!["ok".into()],
                env: Default::default(),
                working_dir: None,
                stdin: Some(vec![b'x'; stdin_len]),
            },
            rusty_grid_core::task::TaskRequirements::generic(1, 30),
        )
    };

    // 1. Over TCP (`ClientMessage::SubmitTask`): a tight cap accepts the first task and
    //    refuses the second with a message naming the bytes in use, limit in bytes, and
    //    `--max-active-mb`, whereas a MasterServer with a larger `max_active_bytes` accepts both.
    let tight = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_active_bytes(1_500),
    )
    .await
    .unwrap();
    let stream = TcpStream::connect(tight.server_addr()).await.unwrap();
    let mut client = MessageTransport::new(stream);

    client
        .send_msg(&ClientMessage::SubmitTask {
            task: make_task(100),
            wait: false,
        })
        .await
        .unwrap();
    match client.recv_msg::<ClientResponse>().await.unwrap() {
        Some(ClientResponse::TaskSubmitted { .. }) => {}
        other => panic!("expected first task to be accepted, got {other:?}"),
    }

    client
        .send_msg(&ClientMessage::SubmitTask {
            task: make_task(600),
            wait: false,
        })
        .await
        .unwrap();
    match client.recv_msg::<ClientResponse>().await.unwrap() {
        Some(ClientResponse::Error { message }) => {
            assert!(message.contains("bytes in use"), "got: {message}");
            assert!(message.contains("limit 1500 bytes"), "got: {message}");
            assert!(message.contains("--max-active-mb"), "got: {message}");
        }
        other => panic!("expected byte-limit refusal on second task, got {other:?}"),
    }
    let _ = tight.shutdown();

    let roomy = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_active_bytes(4_000),
    )
    .await
    .unwrap();
    let stream = TcpStream::connect(roomy.server_addr()).await.unwrap();
    let mut client = MessageTransport::new(stream);
    for len in [100, 600] {
        client
            .send_msg(&ClientMessage::SubmitTask {
                task: make_task(len),
                wait: false,
            })
            .await
            .unwrap();
        match client.recv_msg::<ClientResponse>().await.unwrap() {
            Some(ClientResponse::TaskSubmitted { .. }) => {}
            other => panic!("expected roomy master to accept task ({len}), got {other:?}"),
        }
    }
    let _ = roomy.shutdown();

    // 2. Default `ServerConfig` (`DEFAULT_MAX_ACTIVE_BYTES`) refuses a submission that exceeds
    //    the default 256 MiB active byte cap, while a `MasterServer` started with a larger
    //    `max_active_bytes` accepts it.
    let default_master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let err = default_master
        .submit_task(make_task(crate::queue::DEFAULT_MAX_ACTIVE_BYTES))
        .await
        .unwrap_err();
    assert!(
        matches!(err, GridError::QueueByteLimitExceeded { .. }),
        "default master must refuse task exceeding DEFAULT_MAX_ACTIVE_BYTES, got {err:?}"
    );
    let _ = default_master.shutdown();

    let larger_master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_max_active_bytes(crate::queue::DEFAULT_MAX_ACTIVE_BYTES + 8 * 1024 * 1024),
    )
    .await
    .unwrap();
    larger_master
        .submit_task(make_task(crate::queue::DEFAULT_MAX_ACTIVE_BYTES))
        .await
        .expect("master with larger max_active_bytes must accept the submission");
    let _ = larger_master.shutdown();
}

#[tokio::test]
async fn test_mapreduce_cancelled_when_client_disconnects() {
    // Item 4: when a client submitting `SubmitMapReduce` disconnects while partitions are
    // running or waiting for room in the queue, `handle_client_connection` must cancel the
    // MapReduce job so running partitions are cancelled and waiting partitions never enter
    // the queue when room later opens.
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_active_bytes(2_500),
    )
    .await
    .unwrap();

    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut client = MessageTransport::new(stream);
    let job = rusty_grid_core::mapreduce::MapReduceJobSpec::new(
        "disconnect_job",
        vec!["x".repeat(1_200), "y".repeat(1_200), "z".repeat(1_200)],
        rusty_grid_core::mapreduce::MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        rusty_grid_core::mapreduce::ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        3,
        1,
        30,
    );
    client
        .send_msg(&ClientMessage::SubmitMapReduce { job })
        .await
        .unwrap();

    // Wait until at least one partition is queued (and the remaining partitions are waiting
    // for active-byte room because 3 * ~1_400 bytes > 2_500 bytes).
    let mut saw_queued = false;
    for _ in 0..50 {
        if master.queue().stats().await.queued > 0 {
            saw_queued = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(saw_queued, "at least one map partition should be queued");

    // Disconnect the client while the MapReduce job is in flight.
    drop(client);

    // Wait briefly for `handle_client_connection` to notice EOF, drop `mapreduce.execute`,
    // and let `AbandonGuard` cancel the queued partition without queuing the waiting ones.
    let mut drained = false;
    for _ in 0..50 {
        let stats = master.queue().stats().await;
        if stats.queued == 0 && stats.active_bytes == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let final_stats = master.queue().stats().await;
    assert!(
        drained,
        "client disconnect must cancel running/queued partitions and queue no waiting ones, got {final_stats:?}"
    );
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_oversized_master_frames_fail_only_the_task_and_return_client_error() {
    use rusty_grid_core::protocol::{
        serialize_message, ProtocolCapabilities, WireCodec, MAX_FRAME_SIZE,
        PROTOCOL_VERSION_CURRENT,
    };
    use rusty_grid_core::task::{Bytes, TaskRequirements, TaskSpec};
    use std::collections::HashMap;

    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let addr = master.server_addr();

    // Register a mock worker with 1 CPU slot so a second task can only be dispatched if
    // `handle_oversized_master_message` releases the worker slot (`decrement_active_tasks`).
    let worker_stream = TcpStream::connect(addr).await.unwrap();
    let mut worker = MessageTransport::new(worker_stream);
    let worker_id = Uuid::new_v4();
    worker
        .send_msg(&WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities {
                name: "bug11-worker".into(),
                cpu_cores: 1,
                ram_mb: 8192,
                has_gpu: false,
                is_simulated_gpu: false,
                gpu_device_name: None,
                tags: vec![],
                mobile: None,
                has_blender: false,
                blender_version: None,
                vram_mb: None,
                has_cuda: false,
            },
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    match worker.recv_msg::<MasterMessage>().await.unwrap() {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("expected RegisterAck, got {other:?}"),
    }

    // 1. Submit an oversized task (> 64 MiB stdin) with a registered waiter and a normal
    //    sibling task on a 1-core worker.
    //    The oversized assignment must wake its waiter with the frame-limit failure, release
    //    the worker's single slot without killing the worker session, and allow the sibling
    //    task to be dispatched and finish.
    let oversized_task = Task::new(
        TaskSpec::Command {
            program: "cat".into(),
            args: vec![],
            env: HashMap::new(),
            working_dir: None,
            stdin: Some(vec![b'x'; MAX_FRAME_SIZE + 1024]),
        },
        TaskRequirements::generic(1, 64),
    );
    let oversized_id = oversized_task.id;
    let (oversized_waiter_tx, oversized_waiter_rx) = tokio::sync::oneshot::channel();
    master
        .waiters
        .write()
        .await
        .entry(oversized_id)
        .or_default()
        .push(oversized_waiter_tx);
    master.submit_task(oversized_task).await.unwrap();

    // The registered waiter must wake with the failure result, and the worker's registry slot
    // must be released (`active_tasks == 0` and status not `Busy`).
    let oversized_res = tokio::time::timeout(Duration::from_secs(2), oversized_waiter_rx)
        .await
        .expect("oversized task waiter must be woken when assignment fails")
        .expect("oversized task waiter channel must receive TaskResult");
    assert!(!oversized_res.is_success());
    let err_msg = oversized_res.error.unwrap_or_default();
    assert!(
        err_msg.contains("frame") && err_msg.contains(&MAX_FRAME_SIZE.to_string()),
        "oversized task failure reason must mention the frame limit, got: {err_msg}"
    );
    assert_eq!(
        master.get_task_info(oversized_id).await.unwrap().state,
        crate::queue::TaskState::Failed
    );
    let worker_after_fail = master.registry.get_worker(worker_id).await.unwrap();
    assert_eq!(
        worker_after_fail.active_tasks, 0,
        "worker registry active_tasks must return to 0 after unsendable assignment"
    );
    assert_ne!(
        worker_after_fail.status,
        crate::registry::WorkerStatus::Busy,
        "worker registry status must not stay Busy after unsendable assignment"
    );

    let sibling_task = Task::new(
        TaskSpec::command("echo", vec!["sibling".into()]),
        TaskRequirements::generic(1, 64),
    );
    let sibling_id = sibling_task.id;
    master.submit_task(sibling_task).await.unwrap();

    // The 1-slot worker session must still be alive, have its slot released, and receive the
    // sibling task assignment.
    let assigned = tokio::time::timeout(Duration::from_secs(2), worker.recv_msg::<MasterMessage>())
        .await
        .expect("worker session must receive sibling AssignTask without timing out")
        .expect("worker transport must not error")
        .expect("worker transport must not be closed");
    match assigned {
        MasterMessage::AssignTask { task } => assert_eq!(task.id, sibling_id),
        other => panic!("expected sibling AssignTask, got {other:?}"),
    }

    // Complete the sibling task with a result whose WorkerMessage::TaskResult frame fits
    // MAX_FRAME_SIZE (by 8 bytes), so its ClientResponse::TaskCompleted frame (which carries
    // an extra 24 bytes in bincode) exceeds MAX_FRAME_SIZE by 16 bytes.
    let mut probe_msg = WorkerMessage::TaskResult {
        worker_id,
        task_id: sibling_id,
        exit_code: 0,
        stdout: Bytes::new(),
        stderr: Bytes::new(),
        execution_time_ms: 1,
        is_gpu_executed: false,
        device_name: None,
        error: None,
        asset_bytes: None,
        artifacts: vec![],
        outputs: vec![],
    };
    let empty_size = serialize_message(&probe_msg, WireCodec::Bincode)
        .unwrap()
        .len();
    let stdout_len = MAX_FRAME_SIZE - empty_size - 8;
    if let WorkerMessage::TaskResult { ref mut stdout, .. } = probe_msg {
        *stdout = Bytes::from(vec![b'a'; stdout_len]);
    }
    let worker_frame_len = serialize_message(&probe_msg, WireCodec::Bincode)
        .unwrap()
        .len();
    assert!(
        worker_frame_len <= MAX_FRAME_SIZE,
        "worker frame must fit MAX_FRAME_SIZE: {worker_frame_len}"
    );

    worker.send_msg(&probe_msg).await.unwrap();

    // Wait for sibling task to complete on the master.
    for _ in 0..80 {
        if let Ok(info) = master.get_task_info(sibling_id).await {
            if info.state == crate::queue::TaskState::Completed {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        master.get_task_info(sibling_id).await.unwrap().state,
        crate::queue::TaskState::Completed
    );

    // 2. A client requesting the sibling task result (whose ClientResponse::TaskCompleted
    //    exceeds MAX_FRAME_SIZE) must receive ClientResponse::Error instead of EOF.
    let client_stream = TcpStream::connect(addr).await.unwrap();
    let mut client = MessageTransport::new(client_stream);
    client
        .send_msg(&ClientMessage::GetTaskResult {
            task_id: sibling_id,
        })
        .await
        .unwrap();
    match client.recv_msg::<ClientResponse>().await.unwrap() {
        Some(ClientResponse::Error { message }) => {
            assert!(
                message.contains(&sibling_id.to_string())
                    && message.contains("frame")
                    && message.contains(&MAX_FRAME_SIZE.to_string())
                    && message.contains("--output"),
                "expected frame limit error message naming task and --output, got: {message}"
            );
        }
        other => {
            panic!("expected ClientResponse::Error for oversized client reply, got {other:?}")
        }
    }

    // 3. A task with a 33 MiB checkpoint dispatched via AssignTaskWithCheckpoint carries the
    //    checkpoint once (`latest_checkpoint: Some(...)`, `task.latest_checkpoint: None`), so
    //    it fits inside the 64 MiB frame limit instead of doubling to 66 MiB.
    let mut cp_task = Task::new(
        TaskSpec::command("echo", vec!["cp".into()]),
        TaskRequirements::generic(1, 64),
    );
    cp_task.latest_checkpoint = Some(rusty_grid_core::task::CheckpointData::new(
        1,
        vec![b'c'; 33 * 1024 * 1024],
    ));
    let cp_task_id = cp_task.id;
    master.submit_task(cp_task).await.unwrap();

    let cp_assigned =
        tokio::time::timeout(Duration::from_secs(2), worker.recv_msg::<MasterMessage>())
            .await
            .expect("worker must receive 33 MiB AssignTaskWithCheckpoint")
            .expect("worker transport must not error")
            .expect("worker transport must not be closed");
    match cp_assigned {
        MasterMessage::AssignTaskWithCheckpoint {
            task,
            latest_checkpoint,
        } => {
            assert_eq!(task.id, cp_task_id);
            assert!(
                task.latest_checkpoint.is_none(),
                "checkpoint must not be duplicated in task.latest_checkpoint"
            );
            assert_eq!(
                latest_checkpoint.as_ref().map(|c| c.delta_state.len()),
                Some(33 * 1024 * 1024)
            );
        }
        other => panic!("expected AssignTaskWithCheckpoint, got {other:?}"),
    }

    let _ = master.shutdown();

    // 4. Verify the multiplexed (QUIC 2-lane) data-writer path (`handle_multiplexed_connection`)
    //    also fails only the oversized task, releases the worker slot, wakes its waiter, and
    //    keeps the data lane open for the next assignment.
    let mux_registry = WorkerRegistry::new();
    let mux_queue = TaskQueue::new();
    let mux_notify = Arc::new(tokio::sync::Notify::new());
    let mux_waiters: WaiterMap = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
    let (stop_tx, stop_rx) = watch::channel(false);
    let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
    let (d_master, d_worker) = tokio::io::duplex(64 * 1024);

    let mux_reg_clone = mux_registry.clone();
    let mux_queue_clone = mux_queue.clone();
    let mux_notify_clone = Arc::clone(&mux_notify);
    let mux_waiters_clone = Arc::clone(&mux_waiters);
    let mux_conn = tokio::spawn(async move {
        let _ = handle_multiplexed_connection(
            c_master,
            d_master,
            "127.0.0.1:50000".parse().unwrap(),
            mux_reg_clone,
            mux_queue_clone,
            mux_notify_clone,
            mux_waiters_clone,
            10,
            Duration::from_secs(5),
            stop_rx,
            None,
            WireCodec::Bincode,
            None,
            MasterAuthContext::default(),
            None,
            None,
            None,
            #[cfg(feature = "dashboard")]
            None,
            #[cfg(feature = "p2p")]
            None,
            #[cfg(feature = "p2p")]
            None,
        )
        .await;
    });

    let mut mux_ctrl = MessageTransport::new(c_worker);
    let mut mux_data = MessageTransport::new(d_worker);
    let mux_worker_id = Uuid::new_v4();
    mux_ctrl
        .send_msg(&WorkerMessage::Register {
            worker_id: mux_worker_id,
            capabilities: WorkerCapabilities {
                name: "bug11-mux-worker".into(),
                cpu_cores: 1,
                ram_mb: 8192,
                has_gpu: false,
                is_simulated_gpu: false,
                gpu_device_name: None,
                tags: vec![],
                mobile: None,
                has_blender: false,
                blender_version: None,
                vram_mb: None,
                has_cuda: false,
            },
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    match mux_ctrl.recv_msg::<MasterMessage>().await.unwrap() {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("expected mux RegisterAck, got {other:?}"),
    }

    let mux_oversized = Task::new(
        TaskSpec::Command {
            program: "cat".into(),
            args: vec![],
            env: HashMap::new(),
            working_dir: None,
            stdin: Some(vec![b'y'; MAX_FRAME_SIZE + 1024]),
        },
        TaskRequirements::generic(1, 64),
    );
    let mux_oversized_id = mux_oversized.id;
    let (mux_waiter_tx, mux_waiter_rx) = tokio::sync::oneshot::channel();
    mux_waiters
        .write()
        .await
        .entry(mux_oversized_id)
        .or_default()
        .push(mux_waiter_tx);
    mux_queue.submit(mux_oversized).await.unwrap();

    let mux_scheduler = crate::scheduler::WorkloadScheduler::new(
        crate::scheduler::SchedulerConfig::default(),
        mux_registry.clone(),
        mux_queue.clone(),
        Arc::clone(&mux_notify),
    );
    assert_eq!(
        mux_scheduler
            .schedule_batch(10)
            .await
            .unwrap()
            .assignments
            .len(),
        1
    );

    let mux_res = tokio::time::timeout(Duration::from_secs(2), mux_waiter_rx)
        .await
        .expect("mux waiter must be woken when data-lane assignment fails")
        .expect("mux waiter must receive TaskResult");
    assert!(!mux_res.is_success());
    let mux_worker_after_fail = mux_registry.get_worker(mux_worker_id).await.unwrap();
    assert_eq!(
        mux_worker_after_fail.active_tasks, 0,
        "mux worker registry active_tasks must return to 0 after unsendable assignment"
    );
    assert_ne!(
        mux_worker_after_fail.status,
        crate::registry::WorkerStatus::Busy,
        "mux worker registry status must not stay Busy after unsendable assignment"
    );

    // Because the 1-slot mux worker's slot was released, a second task can now be scheduled
    // and arrives on `mux_data`.
    let mux_sibling = Task::new(
        TaskSpec::command("echo", vec!["mux-ok".into()]),
        TaskRequirements::generic(1, 64),
    );
    let mux_sibling_id = mux_sibling.id;
    mux_queue.submit(mux_sibling).await.unwrap();
    assert_eq!(
        mux_scheduler
            .schedule_batch(10)
            .await
            .unwrap()
            .assignments
            .len(),
        1
    );

    let mux_assigned =
        tokio::time::timeout(Duration::from_secs(2), mux_data.recv_msg::<MasterMessage>())
            .await
            .expect("mux data lane must receive sibling AssignTask")
            .expect("mux data lane must not error")
            .expect("mux data lane must stay open");
    match mux_assigned {
        MasterMessage::AssignTask { task } => assert_eq!(task.id, mux_sibling_id),
        other => panic!("expected mux sibling AssignTask, got {other:?}"),
    }

    let _ = stop_tx.send(true);
    mux_conn.abort();
}

#[tokio::test]
async fn test_claim_master_role_is_refused_with_and_without_secret_and_sends_no_tasks() {
    use rusty_grid_core::arbitrage::{MasterLeaseTicket, LEASE_VALIDITY_DURATION_MS};
    use rusty_grid_core::cluster_auth::client_handshake;
    use rusty_grid_core::task::{TaskRequirements, TaskSpec};

    for with_secret in [false, true] {
        let secret = with_secret.then(|| ClusterSecret::generate().unwrap());
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let waiters: WaiterMap = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        let (stop_tx, stop_rx) = watch::channel(false);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let signing_secret = secret
            .clone()
            .unwrap_or_else(|| ClusterSecret::generate().unwrap());
        let high_score_ticket = MasterLeaseTicket::issue(
            Uuid::new_v4(),
            Uuid::new_v4(),
            10,
            9500,
            now_ms,
            Duration::from_millis(LEASE_VALIDITY_DURATION_MS),
            &signing_secret,
        );

        let assert_refused_ack = |ack: MasterMessage, context: &str| match ack {
            MasterMessage::RoleHandoverAck {
                accepted,
                reason,
                task_queue_snapshot,
                connected_workers,
            } => {
                assert!(
                    !accepted,
                    "ClaimMasterRole must be refused ({context}, with_secret={with_secret})"
                );
                assert_eq!(
                    reason.as_deref(),
                    Some("Automatic master failover is not supported; use manual role switching"),
                    "unexpected refusal reason ({context}, with_secret={with_secret})"
                );
                assert!(
                    task_queue_snapshot.is_empty(),
                    "refused ClaimMasterRole must not leak task_queue_snapshot ({context}, with_secret={with_secret})"
                );
                assert!(
                    connected_workers.is_empty(),
                    "refused ClaimMasterRole must not leak connected_workers ({context}, with_secret={with_secret})"
                );
            }
            other => panic!(
                "expected RoleHandoverAck ({context}, with_secret={with_secret}), got {other:?}"
            ),
        };

        let spawn_tcp = |m_end: tokio::io::DuplexStream, port: u16| {
            let reg_clone = registry.clone();
            let queue_clone = queue.clone();
            let notify_clone = Arc::clone(&scheduler_notify);
            let waiters_clone = Arc::clone(&waiters);
            let stop_rx_clone = stop_rx.clone();
            let secret_clone = secret.clone();
            tokio::spawn(async move {
                handle_connection(
                    m_end,
                    SocketAddr::from(([127, 0, 0, 1], port)),
                    reg_clone,
                    queue_clone,
                    notify_clone,
                    waiters_clone,
                    10,
                    Duration::from_secs(5),
                    stop_rx_clone,
                    None,
                    WireCodec::Bincode,
                    secret_clone,
                    MasterAuthContext::default(),
                    None,
                    4,
                    None,
                    None,
                    #[cfg(feature = "dashboard")]
                    None,
                    #[cfg(feature = "p2p")]
                    None,
                    #[cfg(feature = "p2p")]
                    None,
                )
                .await
            })
        };

        let spawn_mux =
            |c_master: tokio::io::DuplexStream, d_master: tokio::io::DuplexStream, port: u16| {
                let reg_clone = registry.clone();
                let queue_clone = queue.clone();
                let notify_clone = Arc::clone(&scheduler_notify);
                let waiters_clone = Arc::clone(&waiters);
                let stop_rx_clone = stop_rx.clone();
                let secret_clone = secret.clone();
                tokio::spawn(async move {
                    handle_multiplexed_connection(
                        c_master,
                        d_master,
                        SocketAddr::from(([127, 0, 0, 1], port)),
                        reg_clone,
                        queue_clone,
                        notify_clone,
                        waiters_clone,
                        10,
                        Duration::from_secs(5),
                        stop_rx_clone,
                        None,
                        WireCodec::Bincode,
                        secret_clone,
                        MasterAuthContext::default(),
                        None,
                        None,
                        None,
                        #[cfg(feature = "dashboard")]
                        None,
                        #[cfg(feature = "p2p")]
                        None,
                        #[cfg(feature = "p2p")]
                        None,
                    )
                    .await
                })
            };

        // 1. Before any task is queued: send ClaimMasterRole as handshake message on handle_connection.
        {
            let (m_end, w_end) = tokio::io::duplex(64 * 1024);
            let conn_task = spawn_tcp(m_end, 50001);

            let mut w_transport = MessageTransport::new(w_end);
            if let Some(ref sec) = secret {
                let key = client_handshake(&mut w_transport, sec).await.unwrap();
                w_transport.enable_encryption(&key, EndpointRole::Client);
            }
            w_transport
                .send_msg(&WorkerMessage::ClaimMasterRole {
                    ticket: high_score_ticket.clone(),
                })
                .await
                .unwrap();
            let ack = w_transport
                .recv_msg::<MasterMessage>()
                .await
                .unwrap()
                .expect("handshake ClaimMasterRole must receive RoleHandoverAck");
            assert_refused_ack(ack, "pre-queue handshake");
            conn_task.await.unwrap().unwrap();
            assert!(registry.list_workers().await.is_empty());
        }

        // 2. Queue a task so the queue is non-empty, then test TCP handshake, multiplexed handshake,
        //    active TCP session, and active multiplexed session.
        let queued_task = Task::new(
            TaskSpec::command("echo", vec!["queued-task".into()]),
            TaskRequirements::generic(1, 64),
        );
        queue.submit(queued_task).await.unwrap();
        assert_eq!(queue.get_schedulable_tasks().await.len(), 1);

        // 2a. TCP handshake ClaimMasterRole after a task is queued.
        {
            let (m_end, w_end) = tokio::io::duplex(64 * 1024);
            let conn_task = spawn_tcp(m_end, 50002);

            let mut w_transport = MessageTransport::new(w_end);
            if let Some(ref sec) = secret {
                let key = client_handshake(&mut w_transport, sec).await.unwrap();
                w_transport.enable_encryption(&key, EndpointRole::Client);
            }
            w_transport
                .send_msg(&WorkerMessage::ClaimMasterRole {
                    ticket: high_score_ticket.clone(),
                })
                .await
                .unwrap();
            let ack = w_transport
                .recv_msg::<MasterMessage>()
                .await
                .unwrap()
                .expect("post-queue handshake ClaimMasterRole must receive RoleHandoverAck");
            assert_refused_ack(ack, "post-queue TCP handshake");
            conn_task.await.unwrap().unwrap();
            assert!(
                registry.list_workers().await.is_empty(),
                "refused TCP handshake claim must not register a worker"
            );
        }

        // 2b. Multiplexed (P2P) handshake ClaimMasterRole as first control message after a task is queued.
        {
            let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
            let (d_master, _d_worker) = tokio::io::duplex(64 * 1024);
            let mux_hs_task = spawn_mux(c_master, d_master, 50003);

            let mut ctrl_transport = MessageTransport::new(c_worker);
            if let Some(ref sec) = secret {
                let key = client_handshake(&mut ctrl_transport, sec).await.unwrap();
                ctrl_transport.enable_encryption(&key, EndpointRole::Client);
            }
            ctrl_transport
                .send_msg(&WorkerMessage::ClaimMasterRole {
                    ticket: high_score_ticket.clone(),
                })
                .await
                .unwrap();
            let ack = ctrl_transport
                .recv_msg::<MasterMessage>()
                .await
                .unwrap()
                .expect("post-queue mux handshake ClaimMasterRole must receive RoleHandoverAck");
            assert_refused_ack(ack, "post-queue multiplexed handshake");
            drop(ctrl_transport);
            mux_hs_task.await.unwrap().unwrap();
            assert!(
                registry.list_workers().await.is_empty(),
                "refused multiplexed handshake claim must not register a worker"
            );
        }

        // 2c. Active registered session on handle_connection.
        {
            let (m_end, w_end) = tokio::io::duplex(64 * 1024);
            let conn_task = spawn_tcp(m_end, 50004);

            let mut w_transport = MessageTransport::new(w_end);
            if let Some(ref sec) = secret {
                let key = client_handshake(&mut w_transport, sec).await.unwrap();
                w_transport.enable_encryption(&key, EndpointRole::Client);
            }
            let worker_id = Uuid::new_v4();
            w_transport
                .send_msg(&WorkerMessage::Register {
                    worker_id,
                    capabilities: WorkerCapabilities::new(
                        "feat6-worker",
                        2,
                        4096,
                        false,
                        false,
                        None,
                    ),
                    protocol_version: PROTOCOL_VERSION_CURRENT,
                    features: ProtocolCapabilities::all_supported()
                        | ProtocolCapabilities::ROLE_ARBITRAGE,
                })
                .await
                .unwrap();
            match w_transport.recv_msg::<MasterMessage>().await.unwrap() {
                Some(MasterMessage::RegisterAck {
                    accepted: true,
                    features,
                    ..
                }) => {
                    assert!(
                        !features.contains(ProtocolCapabilities::ROLE_ARBITRAGE),
                        "negotiated features must not include ROLE_ARBITRAGE"
                    );
                }
                other => panic!("expected RegisterAck, got {other:?}"),
            }

            w_transport
                .send_msg(&WorkerMessage::ClaimMasterRole {
                    ticket: high_score_ticket.clone(),
                })
                .await
                .unwrap();
            let ack = w_transport
                .recv_msg::<MasterMessage>()
                .await
                .unwrap()
                .expect("active session ClaimMasterRole must receive RoleHandoverAck");
            assert_refused_ack(ack, "post-queue active TCP session");
            assert_eq!(
                registry.get_worker(worker_id).await.map(|w| w.status),
                Some(crate::registry::WorkerStatus::Connected),
                "worker must remain Connected in registry after refused ClaimMasterRole on active TCP session"
            );

            // Session remains alive and responds to Heartbeat after refusal.
            w_transport
                .send_msg(&WorkerMessage::Heartbeat {
                    worker_id,
                    timestamp: 42,
                    active_tasks: 0,
                    cpu_usage_pct: 10.0,
                    ram_available_mb: 2048,
                })
                .await
                .unwrap();
            match w_transport.recv_msg::<MasterMessage>().await.unwrap() {
                Some(MasterMessage::HeartbeatAck { timestamp: 42 }) => {}
                other => {
                    panic!("expected HeartbeatAck after refused ClaimMasterRole, got {other:?}")
                }
            }
            assert_eq!(
                registry.get_worker(worker_id).await.map(|w| w.status),
                Some(crate::registry::WorkerStatus::Connected)
            );
            conn_task.abort();
        }

        // 2d. Active multiplexed session on handle_multiplexed_connection.
        {
            let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
            let (d_master, _d_worker) = tokio::io::duplex(64 * 1024);
            let mux_task = spawn_mux(c_master, d_master, 50005);

            let mut ctrl_transport = MessageTransport::new(c_worker);
            if let Some(ref sec) = secret {
                let key = client_handshake(&mut ctrl_transport, sec).await.unwrap();
                ctrl_transport.enable_encryption(&key, EndpointRole::Client);
            }
            let mux_worker_id = Uuid::new_v4();
            ctrl_transport
                .send_msg(&WorkerMessage::Register {
                    worker_id: mux_worker_id,
                    capabilities: WorkerCapabilities::new(
                        "feat6-mux-worker",
                        2,
                        4096,
                        false,
                        false,
                        None,
                    ),
                    protocol_version: PROTOCOL_VERSION_CURRENT,
                    features: ProtocolCapabilities::all_supported()
                        | ProtocolCapabilities::ROLE_ARBITRAGE,
                })
                .await
                .unwrap();
            match ctrl_transport.recv_msg::<MasterMessage>().await.unwrap() {
                Some(MasterMessage::RegisterAck {
                    accepted: true,
                    features,
                    ..
                }) => {
                    assert!(
                        !features.contains(ProtocolCapabilities::ROLE_ARBITRAGE),
                        "mux negotiated features must not include ROLE_ARBITRAGE"
                    );
                }
                other => panic!("expected mux RegisterAck, got {other:?}"),
            }

            ctrl_transport
                .send_msg(&WorkerMessage::ClaimMasterRole {
                    ticket: high_score_ticket.clone(),
                })
                .await
                .unwrap();
            let ack = ctrl_transport
                .recv_msg::<MasterMessage>()
                .await
                .unwrap()
                .expect("mux active session ClaimMasterRole must receive RoleHandoverAck");
            assert_refused_ack(ack, "post-queue active multiplexed session");
            assert_eq!(
                registry.get_worker(mux_worker_id).await.map(|w| w.status),
                Some(crate::registry::WorkerStatus::Connected),
                "worker must remain Connected in registry after refused ClaimMasterRole on active multiplexed session"
            );

            // Multiplexed control lane remains alive and responds to Heartbeat after refusal.
            ctrl_transport
                .send_msg(&WorkerMessage::Heartbeat {
                    worker_id: mux_worker_id,
                    timestamp: 84,
                    active_tasks: 0,
                    cpu_usage_pct: 12.0,
                    ram_available_mb: 2048,
                })
                .await
                .unwrap();
            match ctrl_transport.recv_msg::<MasterMessage>().await.unwrap() {
                Some(MasterMessage::HeartbeatAck { timestamp: 84 }) => {}
                other => {
                    panic!("expected mux HeartbeatAck after refused ClaimMasterRole, got {other:?}")
                }
            }
            assert_eq!(
                registry.get_worker(mux_worker_id).await.map(|w| w.status),
                Some(crate::registry::WorkerStatus::Connected)
            );

            let _ = stop_tx.send(true);
            mux_task.abort();
        }
    }
}

#[tokio::test]
async fn test_master_server_bind_enforces_max_queue_size_and_json_wire_codec() {
    use rusty_grid_core::task::{TaskRequirements, TaskSpec};
    let server_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_max_queue_size(Some(1))
        .with_wire_codec(WireCodec::Json);
    let server = MasterServer::bind(server_cfg, WorkerRegistry::new())
        .await
        .unwrap();
    let addr = server.local_addr;
    let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let server_task = tokio::spawn(server.run(stop_rx));

    // Connect with a Bincode client and submit 1st task: master must reply in Json and accept.
    let stream1 = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut client1 = MessageTransport::with_codec(stream1, WireCodec::Bincode);
    client1
        .send_msg(&ClientMessage::SubmitTask {
            task: Task::new(
                TaskSpec::command("echo", vec!["first".into()]),
                TaskRequirements::generic(1, 30),
            ),
            wait: false,
        })
        .await
        .unwrap();
    let (reply1, codec1) = client1
        .recv_msg_with_codec::<ClientResponse>()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        codec1,
        WireCodec::Json,
        "MasterServer::bind with WireCodec::Json must reply in Json even to a Bincode client"
    );
    assert!(
        matches!(reply1, ClientResponse::TaskSubmitted { .. }),
        "1st submit must succeed under max_queue_size = Some(1), got: {reply1:?}"
    );

    // Submit 2nd task while 1st task is queued: must be rejected by `max_queue_size = Some(1)`.
    let stream2 = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut client2 = MessageTransport::with_codec(stream2, WireCodec::Bincode);
    client2
        .send_msg(&ClientMessage::SubmitTask {
            task: Task::new(
                TaskSpec::command("echo", vec!["second".into()]),
                TaskRequirements::generic(1, 30),
            ),
            wait: false,
        })
        .await
        .unwrap();
    let (reply2, codec2) = client2
        .recv_msg_with_codec::<ClientResponse>()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(codec2, WireCodec::Json);
    match reply2 {
        ClientResponse::Error { message } => {
            assert!(
                message.contains(
                    "Task queue is full (1 queued or running tasks; wait, or raise --max-queue-size)"
                ),
                "expected actionable QueueFull(1) error from MasterServer::bind, got: {message}"
            );
        }
        other => panic!("expected ClientResponse::Error for 2nd submit, got {other:?}"),
    }

    server_task.abort();

    // Also verify multiplexed P2P (`handle_multiplexed_connection`) with `WireCodec::Json`:
    // a worker registering with Bincode must receive both its `RegisterAck` (control lane)
    // and `AssignTask` (data lane) in `WireCodec::Json`.
    let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
    let (d_master, d_worker) = tokio::io::duplex(64 * 1024);
    let (_mux_stop_tx, mux_stop_rx) = tokio::sync::watch::channel(false);
    let mux_reg1 = WorkerRegistry::new();
    let mux_queue1 = TaskQueue::new();
    let mux_notify1 = Arc::new(tokio::sync::Notify::new());
    let reg1_clone = mux_reg1.clone();
    let queue1_clone = mux_queue1.clone();
    let notify1_clone = Arc::clone(&mux_notify1);
    let mux_task = tokio::spawn(async move {
        handle_multiplexed_connection(
            c_master,
            d_master,
            SocketAddr::from(([127, 0, 0, 1], 50099)),
            reg1_clone,
            queue1_clone,
            notify1_clone,
            Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            10,
            Duration::from_secs(5),
            mux_stop_rx,
            None,
            WireCodec::Json,
            None,
            MasterAuthContext::default(),
            None,
            None,
            None,
            #[cfg(feature = "dashboard")]
            None,
            #[cfg(feature = "p2p")]
            None,
            #[cfg(feature = "p2p")]
            None,
        )
        .await
    });

    let mut w_ctrl = MessageTransport::with_codec(c_worker, WireCodec::Bincode);
    let mut w_data = MessageTransport::with_codec(d_worker, WireCodec::Bincode);
    w_ctrl
        .send_msg(&WorkerMessage::Register {
            worker_id: Uuid::new_v4(),
            capabilities: WorkerCapabilities::new("mux-json-check", 2, 4096, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: rusty_grid_core::protocol::ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    let (mux_reply, mux_codec) = w_ctrl
        .recv_msg_with_codec::<MasterMessage>()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mux_codec,
        WireCodec::Json,
        "handle_multiplexed_connection with WireCodec::Json must reply in Json on ctrl_transport even to a Bincode worker"
    );
    assert!(matches!(
        mux_reply,
        MasterMessage::RegisterAck { accepted: true, .. }
    ));

    mux_queue1
        .submit(Task::new(
            TaskSpec::command("echo", vec!["mux-json-data-lane".into()]),
            TaskRequirements::generic(1, 30),
        ))
        .await
        .unwrap();
    let sched1 = WorkloadScheduler::new(
        SchedulerConfig::default(),
        mux_reg1,
        mux_queue1,
        mux_notify1,
    );
    assert_eq!(sched1.schedule_once().await.unwrap().assignments.len(), 1);
    let (json_data_msg, json_data_codec) = w_data
        .recv_msg_with_codec::<MasterMessage>()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        json_data_codec,
        WireCodec::Json,
        "handle_multiplexed_connection with WireCodec::Json must send AssignTask in Json on data_transport even to a Bincode worker"
    );
    assert!(matches!(json_data_msg, MasterMessage::AssignTask { .. }));
    mux_task.abort();

    // Conversely, when `handle_multiplexed_connection` is configured with `WireCodec::Bincode`
    // (default), a worker registering with `WireCodec::Json` must have `detected_codec` mirrored
    // onto BOTH `ctrl_transport` (`RegisterAck`) and `data_transport` (`AssignTask`).
    let (c_master2, c_worker2) = tokio::io::duplex(64 * 1024);
    let (d_master2, d_worker2) = tokio::io::duplex(64 * 1024);
    let (_mux_stop_tx2, mux_stop_rx2) = tokio::sync::watch::channel(false);
    let mux_reg2 = WorkerRegistry::new();
    let mux_queue2 = TaskQueue::new();
    let mux_notify2 = Arc::new(tokio::sync::Notify::new());
    let reg2_clone = mux_reg2.clone();
    let queue2_clone = mux_queue2.clone();
    let notify2_clone = Arc::clone(&mux_notify2);
    let mux_task2 = tokio::spawn(async move {
        handle_multiplexed_connection(
            c_master2,
            d_master2,
            SocketAddr::from(([127, 0, 0, 1], 50100)),
            reg2_clone,
            queue2_clone,
            notify2_clone,
            Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            10,
            Duration::from_secs(5),
            mux_stop_rx2,
            None,
            WireCodec::Bincode,
            None,
            MasterAuthContext::default(),
            None,
            None,
            None,
            #[cfg(feature = "dashboard")]
            None,
            #[cfg(feature = "p2p")]
            None,
            #[cfg(feature = "p2p")]
            None,
        )
        .await
    });

    let mut w_ctrl2 = MessageTransport::with_codec(c_worker2, WireCodec::Json);
    let mut w_data2 = MessageTransport::with_codec(d_worker2, WireCodec::Json);
    w_ctrl2
        .send_msg(&WorkerMessage::Register {
            worker_id: Uuid::new_v4(),
            capabilities: WorkerCapabilities::new("mux-mirror-check", 2, 4096, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: rusty_grid_core::protocol::ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    let (mux_reply2, mux_codec2) = w_ctrl2
        .recv_msg_with_codec::<MasterMessage>()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mux_codec2,
        WireCodec::Json,
        "handle_multiplexed_connection with WireCodec::Bincode must mirror detected Json codec onto ctrl_transport"
    );
    assert!(matches!(
        mux_reply2,
        MasterMessage::RegisterAck { accepted: true, .. }
    ));

    mux_queue2
        .submit(Task::new(
            TaskSpec::command("echo", vec!["mux-data-lane".into()]),
            TaskRequirements::generic(1, 30),
        ))
        .await
        .unwrap();
    let sched2 = WorkloadScheduler::new(
        SchedulerConfig::default(),
        mux_reg2,
        mux_queue2,
        mux_notify2,
    );
    assert_eq!(sched2.schedule_once().await.unwrap().assignments.len(), 1);
    let (data_msg, data_codec) = w_data2
        .recv_msg_with_codec::<MasterMessage>()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        data_codec,
        WireCodec::Json,
        "handle_multiplexed_connection with WireCodec::Bincode must mirror detected Json codec onto data_transport"
    );
    assert!(matches!(data_msg, MasterMessage::AssignTask { .. }));
    mux_task2.abort();
}

/// design 0082 D2.3: a `SubmitTask` with an unknown critical extension, or over a limit, gets a
/// `ClientResponse::Error` and nothing is queued; an unknown non-critical one is accepted.
#[tokio::test]
async fn test_submit_task_refuses_unknown_critical_and_oversized_extensions() {
    use rusty_grid_core::task::{Extension, TaskRequirements, TaskSpec, MAX_EXTENSION_BYTES};
    let server = MasterServer::bind(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        WorkerRegistry::new(),
    )
    .await
    .unwrap();
    let addr = server.local_addr;
    let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let server_task = tokio::spawn(server.run(stop_rx));

    let submit = |extension: Extension| async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut client = MessageTransport::with_codec(stream, WireCodec::Bincode);
        let mut task = Task::new(
            TaskSpec::command("echo", vec!["ext".into()]),
            TaskRequirements::generic(1, 30),
        );
        task.extensions = vec![extension];
        client
            .send_msg(&ClientMessage::SubmitTask { task, wait: false })
            .await
            .unwrap();
        client
            .recv_msg_with_codec::<ClientResponse>()
            .await
            .unwrap()
            .unwrap()
            .0
    };

    match submit(Extension {
        id: 0x1234,
        critical: true,
        bytes: vec![1],
    })
    .await
    {
        ClientResponse::Error { message } => {
            assert!(
                message.contains("needs extension 4660, which this master does not support"),
                "{message}"
            );
        }
        other => panic!("expected an error for an unknown critical extension, got {other:?}"),
    }
    match submit(Extension {
        id: 0x1234,
        critical: false,
        bytes: vec![0; MAX_EXTENSION_BYTES + 1],
    })
    .await
    {
        ClientResponse::Error { message } => {
            assert!(message.contains("limit is 65536"), "{message}")
        }
        other => panic!("expected an error for an oversized extension, got {other:?}"),
    }
    assert!(matches!(
        submit(Extension {
            id: 0x1234,
            critical: false,
            bytes: vec![1],
        })
        .await,
        ClientResponse::TaskSubmitted { .. }
    ));
    server_task.abort();
}

#[tokio::test]
async fn tier5_tasks_and_blobs_case6_cancel_140_ignores_stale_speculative_after_revive() {
    use crate::reaper::ReaperConfig;
    use crate::registry::{HeartbeatOutcome, WorkerStatus};
    use crate::scheduler::SchedulerConfig;
    use rusty_grid_core::placement::OPT_REPEATABLE_TAG;
    use rusty_grid_core::task::{TaskRequirements, TaskSpec};
    use std::time::Duration;

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        SchedulerConfig::default(),
        ReaperConfig::new(Duration::from_secs(600), Duration::from_secs(600)),
    )
    .await
    .unwrap();

    let w_primary = Uuid::new_v4();
    let w_spec = Uuid::new_v4();
    let (tx_primary, mut rx_primary) = tokio::sync::mpsc::channel(16);
    let (tx_spec, mut rx_spec) = tokio::sync::mpsc::channel(16);

    let mut caps_primary = WorkerCapabilities::new("primary", 4, 8192, false, false, None);
    caps_primary.tags.push("role=primary".into());
    let s_primary = master
        .registry
        .register(
            w_primary,
            caps_primary,
            "127.0.0.1:9701".parse().unwrap(),
            tx_primary,
            None,
        )
        .await
        .unwrap();

    let mut caps_spec = WorkerCapabilities::new("spec", 4, 8192, false, false, None);
    caps_spec.tags.push("role=spec".into());
    let s_spec = master
        .registry
        .register(
            w_spec,
            caps_spec,
            "127.0.0.1:9702".parse().unwrap(),
            tx_spec,
            None,
        )
        .await
        .unwrap();

    let t1 = master
        .submit_task(
            Task::new(
                TaskSpec::command("echo", vec!["case6-t1".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec![
                OPT_REPEATABLE_TAG.to_string(),
                "require:worker=primary".into(),
            ]),
        )
        .await
        .unwrap();

    let assigned_t1 = tokio::time::timeout(Duration::from_secs(2), rx_primary.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t1, MasterMessage::AssignTask { ref task } if task.id == t1));
    master
        .queue
        .mark_running_from(w_primary, Some(s_primary), &t1)
        .await
        .unwrap();

    let (spec_sender, spec_live) = master
        .registry
        .reserve_dispatch(&w_spec, s_spec)
        .await
        .unwrap();
    let spec_outcome = master
        .queue
        .try_start_speculative_and_deliver(
            t1,
            w_spec,
            s_spec,
            Arc::clone(&spec_live),
            Some(PROTOCOL_VERSION_CURRENT),
            |task| {
                spec_sender
                    .try_send(MasterMessage::AssignTask { task })
                    .is_ok()
            },
        )
        .await;
    assert_eq!(
        spec_outcome,
        crate::queue::SpeculativeStartOutcome::Delivered
    );
    let _ = tokio::time::timeout(Duration::from_secs(2), rx_spec.recv())
        .await
        .unwrap()
        .unwrap();

    // Reap w_spec ONLY (advance past w_spec's heartbeat while keeping w_primary fresh), then revive
    // w_spec on the same session_id before the reaper sweeps t1's speculative slot.
    let spec_hb = master
        .registry
        .last_heartbeat_instant(&w_spec)
        .await
        .unwrap();
    let reaped = master
        .registry
        .reap_stale_sessions_at(spec_hb + Duration::from_secs(30), Duration::from_secs(5))
        .await;
    assert!(reaped.contains(&(w_spec, s_spec)));

    let revived = master
        .registry
        .record_heartbeat_session(&w_spec, 0, 1001, 10.0, 4096, Some(s_spec))
        .await
        .unwrap();
    assert_eq!(revived, HeartbeatOutcome::Revived);

    let t2 = master
        .submit_task(
            Task::new(
                TaskSpec::command("echo", vec!["case6-t2".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec!["require:worker=spec".into()]),
        )
        .await
        .unwrap();
    let assigned_t2 = tokio::time::timeout(Duration::from_secs(2), rx_spec.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t2, MasterMessage::AssignTask { ref task } if task.id == t2));

    let before = master.registry.get_worker(w_spec).await.unwrap();
    assert_eq!(before.status, WorkerStatus::Busy);
    assert_eq!(before.active_tasks, 1);

    // Pre-sweep cancel of t1 hits tasks_and_blobs.rs:140; must NOT release t2's post-revive slot.
    master.cancel_task(t1).await.unwrap();

    let after = master.registry.get_worker(w_spec).await.unwrap();
    assert_eq!(after.status, WorkerStatus::Busy);
    assert_eq!(after.active_tasks, 1);
    assert_eq!(
        master.queue.get_task(&t2).await.unwrap().state,
        TaskState::Scheduled
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn tier5_tasks_and_blobs_case7_result_spec_loser_664_ignores_stale_speculative_after_revive()
{
    use crate::reaper::ReaperConfig;
    use crate::registry::{HeartbeatOutcome, WorkerStatus};
    use crate::scheduler::SchedulerConfig;
    use rusty_grid_core::placement::OPT_REPEATABLE_TAG;
    use rusty_grid_core::task::{TaskRequirements, TaskResult, TaskSpec};
    use std::time::Duration;

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        SchedulerConfig::default(),
        ReaperConfig::new(Duration::from_secs(600), Duration::from_secs(600)),
    )
    .await
    .unwrap();

    let w_primary = Uuid::new_v4();
    let w_spec = Uuid::new_v4();
    let (tx_primary, mut rx_primary) = tokio::sync::mpsc::channel(16);
    let (tx_spec, mut rx_spec) = tokio::sync::mpsc::channel(16);

    let mut caps_primary = WorkerCapabilities::new("primary", 4, 8192, false, false, None);
    caps_primary.tags.push("role=primary".into());
    let s_primary = master
        .registry
        .register(
            w_primary,
            caps_primary,
            "127.0.0.1:9703".parse().unwrap(),
            tx_primary,
            None,
        )
        .await
        .unwrap();

    let mut caps_spec = WorkerCapabilities::new("spec", 4, 8192, false, false, None);
    caps_spec.tags.push("role=spec".into());
    let s_spec = master
        .registry
        .register(
            w_spec,
            caps_spec,
            "127.0.0.1:9704".parse().unwrap(),
            tx_spec,
            None,
        )
        .await
        .unwrap();

    let t1 = master
        .submit_task(
            Task::new(
                TaskSpec::command("echo", vec!["case7-t1".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec![
                OPT_REPEATABLE_TAG.to_string(),
                "require:worker=primary".into(),
            ]),
        )
        .await
        .unwrap();

    let assigned_t1 = tokio::time::timeout(Duration::from_secs(2), rx_primary.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t1, MasterMessage::AssignTask { ref task } if task.id == t1));
    master
        .queue
        .mark_running_from(w_primary, Some(s_primary), &t1)
        .await
        .unwrap();

    let (spec_sender, spec_live) = master
        .registry
        .reserve_dispatch(&w_spec, s_spec)
        .await
        .unwrap();
    assert_eq!(
        master
            .queue
            .try_start_speculative_and_deliver(
                t1,
                w_spec,
                s_spec,
                Arc::clone(&spec_live),
                Some(PROTOCOL_VERSION_CURRENT),
                |task| spec_sender
                    .try_send(MasterMessage::AssignTask { task })
                    .is_ok(),
            )
            .await,
        crate::queue::SpeculativeStartOutcome::Delivered
    );
    let _ = tokio::time::timeout(Duration::from_secs(2), rx_spec.recv())
        .await
        .unwrap()
        .unwrap();

    let spec_hb = master
        .registry
        .last_heartbeat_instant(&w_spec)
        .await
        .unwrap();
    let reaped = master
        .registry
        .reap_stale_sessions_at(spec_hb + Duration::from_secs(30), Duration::from_secs(5))
        .await;
    assert!(reaped.contains(&(w_spec, s_spec)));

    assert_eq!(
        master
            .registry
            .record_heartbeat_session(&w_spec, 0, 1002, 10.0, 4096, Some(s_spec))
            .await
            .unwrap(),
        HeartbeatOutcome::Revived
    );
    // Keep w_primary alive in case reap_stale_sessions_at touched it (both registered ~together).
    let _ = master
        .registry
        .record_heartbeat_session(&w_primary, 1, 1002, 10.0, 4096, Some(s_primary))
        .await
        .unwrap();

    let t2 = master
        .submit_task(
            Task::new(
                TaskSpec::command("echo", vec!["case7-t2".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec!["require:worker=spec".into()]),
        )
        .await
        .unwrap();
    let assigned_t2 = tokio::time::timeout(Duration::from_secs(2), rx_spec.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t2, MasterMessage::AssignTask { ref task } if task.id == t2));

    let before = master.registry.get_worker(w_spec).await.unwrap();
    assert_eq!(before.status, WorkerStatus::Busy);
    assert_eq!(before.active_tasks, 1);

    // Primary wins t1 pre-sweep -> losing-sibling cleanup hits tasks_and_blobs.rs:664.
    super::tasks_and_blobs::handle_task_result(
        &master.registry,
        &master.queue,
        &master.scheduler_notify,
        &master.waiters,
        TaskResult::success(w_primary, t1, "ok\n", 15, false),
    )
    .await;

    let after = master.registry.get_worker(w_spec).await.unwrap();
    assert_eq!(after.status, WorkerStatus::Busy);
    assert_eq!(after.active_tasks, 1);
    assert_eq!(
        master.queue.get_task(&t2).await.unwrap().state,
        TaskState::Scheduled
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn tier5_tasks_and_blobs_case8_result_primary_loser_664_ignores_stale_primary_after_revive() {
    use crate::reaper::ReaperConfig;
    use crate::registry::{HeartbeatOutcome, WorkerStatus};
    use crate::scheduler::SchedulerConfig;
    use rusty_grid_core::placement::OPT_REPEATABLE_TAG;
    use rusty_grid_core::task::{TaskRequirements, TaskResult, TaskSpec};
    use std::time::Duration;

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        SchedulerConfig::default(),
        ReaperConfig::new(Duration::from_secs(600), Duration::from_secs(600)),
    )
    .await
    .unwrap();

    let w_primary = Uuid::new_v4();
    let w_spec = Uuid::new_v4();
    let (tx_primary, mut rx_primary) = tokio::sync::mpsc::channel(16);
    let (tx_spec, mut rx_spec) = tokio::sync::mpsc::channel(16);

    let mut caps_primary = WorkerCapabilities::new("primary", 4, 8192, false, false, None);
    caps_primary.tags.push("role=primary".into());
    let s_primary = master
        .registry
        .register(
            w_primary,
            caps_primary,
            "127.0.0.1:9705".parse().unwrap(),
            tx_primary,
            None,
        )
        .await
        .unwrap();

    let mut caps_spec = WorkerCapabilities::new("spec", 4, 8192, false, false, None);
    caps_spec.tags.push("role=spec".into());
    let s_spec = master
        .registry
        .register(
            w_spec,
            caps_spec,
            "127.0.0.1:9706".parse().unwrap(),
            tx_spec,
            None,
        )
        .await
        .unwrap();

    let t1 = master
        .submit_task(
            Task::new(
                TaskSpec::command("echo", vec!["case8-t1".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec![
                OPT_REPEATABLE_TAG.to_string(),
                "require:worker=primary".into(),
            ]),
        )
        .await
        .unwrap();

    let assigned_t1 = tokio::time::timeout(Duration::from_secs(2), rx_primary.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t1, MasterMessage::AssignTask { ref task } if task.id == t1));
    master
        .queue
        .mark_running_from(w_primary, Some(s_primary), &t1)
        .await
        .unwrap();

    let (spec_sender, spec_live) = master
        .registry
        .reserve_dispatch(&w_spec, s_spec)
        .await
        .unwrap();
    assert_eq!(
        master
            .queue
            .try_start_speculative_and_deliver(
                t1,
                w_spec,
                s_spec,
                Arc::clone(&spec_live),
                Some(PROTOCOL_VERSION_CURRENT),
                |task| spec_sender
                    .try_send(MasterMessage::AssignTask { task })
                    .is_ok(),
            )
            .await,
        crate::queue::SpeculativeStartOutcome::Delivered
    );
    let _ = tokio::time::timeout(Duration::from_secs(2), rx_spec.recv())
        .await
        .unwrap()
        .unwrap();
    master
        .queue
        .mark_running_from(w_spec, Some(s_spec), &t1)
        .await
        .unwrap();

    // Reap & revive w_primary, then dispatch t2 onto w_primary.
    let prim_hb = master
        .registry
        .last_heartbeat_instant(&w_primary)
        .await
        .unwrap();
    let reaped = master
        .registry
        .reap_stale_sessions_at(prim_hb + Duration::from_secs(30), Duration::from_secs(5))
        .await;
    assert!(reaped.contains(&(w_primary, s_primary)));

    assert_eq!(
        master
            .registry
            .record_heartbeat_session(&w_primary, 0, 1003, 10.0, 4096, Some(s_primary))
            .await
            .unwrap(),
        HeartbeatOutcome::Revived
    );
    let _ = master
        .registry
        .record_heartbeat_session(&w_spec, 1, 1003, 10.0, 4096, Some(s_spec))
        .await
        .unwrap();

    let t2 = master
        .submit_task(
            Task::new(
                TaskSpec::command("echo", vec!["case8-t2".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec!["require:worker=primary".into()]),
        )
        .await
        .unwrap();
    let assigned_t2 = tokio::time::timeout(Duration::from_secs(2), rx_primary.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t2, MasterMessage::AssignTask { ref task } if task.id == t2));

    let before = master.registry.get_worker(w_primary).await.unwrap();
    assert_eq!(before.status, WorkerStatus::Busy);
    assert_eq!(before.active_tasks, 1);

    // Speculative wins t1 pre-sweep -> primary losing-sibling cleanup hits tasks_and_blobs.rs:664.
    super::tasks_and_blobs::handle_task_result(
        &master.registry,
        &master.queue,
        &master.scheduler_notify,
        &master.waiters,
        TaskResult::success(w_spec, t1, "fast\n", 12, false),
    )
    .await;

    let after = master.registry.get_worker(w_primary).await.unwrap();
    assert_eq!(after.status, WorkerStatus::Busy);
    assert_eq!(after.active_tasks, 1);
    assert_eq!(
        master.queue.get_task(&t2).await.unwrap().state,
        TaskState::Scheduled
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn tier5_tasks_and_blobs_case9_late_result_627_ignores_stale_session_after_revive() {
    use crate::reaper::ReaperConfig;
    use crate::registry::{HeartbeatOutcome, WorkerStatus};
    use crate::scheduler::SchedulerConfig;
    use rusty_grid_core::task::{TaskRequirements, TaskResult, TaskSpec};
    use std::time::Duration;

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        SchedulerConfig::default(),
        ReaperConfig::new(Duration::from_secs(600), Duration::from_secs(600)),
    )
    .await
    .unwrap();

    let w = Uuid::new_v4();
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let session = master
        .registry
        .register(
            w,
            WorkerCapabilities::new("late-worker", 4, 8192, false, false, None),
            "127.0.0.1:9707".parse().unwrap(),
            tx,
            None,
        )
        .await
        .unwrap();

    let t1 = master
        .submit_task(Task::new(
            TaskSpec::command("sleep", vec!["60".into()]),
            TaskRequirements::generic(1, 64),
        ))
        .await
        .unwrap();
    let assigned_t1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t1, MasterMessage::AssignTask { ref task } if task.id == t1));
    master
        .queue
        .mark_running_from(w, Some(session), &t1)
        .await
        .unwrap();

    // Cancel t1 (state becomes Cancelled; assigned_worker_id/session/session_live remain for late result),
    // then reap + sweep w (sweep skips terminal t1), revive w, and dispatch t2.
    master.cancel_task(t1).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();

    let hb = master.registry.last_heartbeat_instant(&w).await.unwrap();
    let reaped = master
        .registry
        .reap_stale_sessions_at(hb + Duration::from_secs(30), Duration::from_secs(5))
        .await;
    assert_eq!(reaped, vec![(w, session)]);
    master
        .queue
        .handle_session_disconnected(&w, Some(session), "reaped", false)
        .await;

    assert_eq!(
        master
            .registry
            .record_heartbeat_session(&w, 0, 1004, 10.0, 4096, Some(session))
            .await
            .unwrap(),
        HeartbeatOutcome::Revived
    );

    let t2 = master
        .submit_task(Task::new(
            TaskSpec::command("echo", vec!["case9-t2".into()]),
            TaskRequirements::generic(1, 64),
        ))
        .await
        .unwrap();
    let assigned_t2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t2, MasterMessage::AssignTask { ref task } if task.id == t2));

    let before = master.registry.get_worker(w).await.unwrap();
    assert_eq!(before.status, WorkerStatus::Busy);
    assert_eq!(before.active_tasks, 1);

    // Late result for t1 hits tasks_and_blobs.rs:627; must NOT decrement t2's post-revive slot.
    super::tasks_and_blobs::handle_task_result(
        &master.registry,
        &master.queue,
        &master.scheduler_notify,
        &master.waiters,
        TaskResult::failure(w, t1, 130, "", "", 5, Some("cancelled".into())),
    )
    .await;

    let after = master.registry.get_worker(w).await.unwrap();
    assert_eq!(after.status, WorkerStatus::Busy);
    assert_eq!(after.active_tasks, 1);
    assert_eq!(
        master.queue.get_task(&t2).await.unwrap().state,
        TaskState::Scheduled
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn tier5_tasks_and_blobs_case10_oversized_854_ignores_stale_session_after_revive() {
    use crate::reaper::ReaperConfig;
    use crate::registry::{HeartbeatOutcome, WorkerStatus};
    use crate::scheduler::SchedulerConfig;
    use rusty_grid_core::protocol::MAX_FRAME_SIZE;
    use rusty_grid_core::task::{TaskRequirements, TaskSpec};
    use std::time::Duration;

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        SchedulerConfig::default(),
        ReaperConfig::new(Duration::from_secs(600), Duration::from_secs(600)),
    )
    .await
    .unwrap();

    let w = Uuid::new_v4();
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let session = master
        .registry
        .register(
            w,
            WorkerCapabilities::new("oversized-worker", 4, 8192, false, false, None),
            "127.0.0.1:9708".parse().unwrap(),
            tx,
            None,
        )
        .await
        .unwrap();

    let t1_task = Task::new(
        TaskSpec::command("echo", vec!["case10-t1".into()]),
        TaskRequirements::generic(1, 64),
    );
    let t1 = master.submit_task(t1_task).await.unwrap();
    let assigned_t1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let oversized_msg = match assigned_t1 {
        MasterMessage::AssignTask { ref task } if task.id == t1 => assigned_t1,
        other => panic!("expected AssignTask for t1, got {other:?}"),
    };

    let hb = master.registry.last_heartbeat_instant(&w).await.unwrap();
    let reaped = master
        .registry
        .reap_stale_sessions_at(hb + Duration::from_secs(30), Duration::from_secs(5))
        .await;
    assert_eq!(reaped, vec![(w, session)]);

    assert_eq!(
        master
            .registry
            .record_heartbeat_session(&w, 0, 1005, 10.0, 4096, Some(session))
            .await
            .unwrap(),
        HeartbeatOutcome::Revived
    );

    let t2 = master
        .submit_task(Task::new(
            TaskSpec::command("echo", vec!["case10-t2".into()]),
            TaskRequirements::generic(1, 64),
        ))
        .await
        .unwrap();
    let assigned_t2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t2, MasterMessage::AssignTask { ref task } if task.id == t2));

    let before = master.registry.get_worker(w).await.unwrap();
    assert_eq!(before.status, WorkerStatus::Busy);
    assert_eq!(before.active_tasks, 1);

    // Pre-sweep oversized-frame failure for t1 hits tasks_and_blobs.rs:854; must NOT decrement
    // t2's post-revive slot.
    super::tasks_and_blobs::handle_oversized_master_message(
        oversized_msg,
        MAX_FRAME_SIZE + 1,
        MAX_FRAME_SIZE,
        &master.registry,
        &master.queue,
        &master.scheduler_notify,
        &master.waiters,
        w,
        session,
        None,
        #[cfg(feature = "dashboard")]
        None,
    )
    .await;

    let after = master.registry.get_worker(w).await.unwrap();
    assert_eq!(after.status, WorkerStatus::Busy);
    assert_eq!(after.active_tasks, 1);
    assert_eq!(
        master.queue.get_task(&t2).await.unwrap().state,
        TaskState::Scheduled
    );

    let _ = master.shutdown();
}

async fn connect_and_register_tcp_worker(
    addr: SocketAddr,
    worker_id: Uuid,
    name: &str,
) -> MessageTransport<TcpStream> {
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);
    transport
        .send_msg(&WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities::new(name, 4, 8192, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            transport.recv_msg::<MasterMessage>()
        )
        .await
        .unwrap()
        .unwrap(),
        Some(MasterMessage::RegisterAck { accepted: true, .. })
    ));
    transport
}

async fn finish_tcp_task_and_verify_idle(
    master: &MasterHandle,
    transport: &mut MessageTransport<TcpStream>,
    worker_id: Uuid,
    task_id: rusty_grid_core::task::TaskId,
    hb_ts: u64,
) {
    use crate::registry::WorkerStatus;
    use rusty_grid_core::task::Bytes;

    transport
        .send_msg(&WorkerMessage::TaskResult {
            worker_id,
            task_id,
            exit_code: 0,
            stdout: Bytes::from_static(b"ok\n"),
            stderr: Bytes::new(),
            execution_time_ms: 10,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: vec![],
            outputs: vec![],
        })
        .await
        .unwrap();
    let ack = tokio::time::timeout(
        Duration::from_secs(2),
        transport.recv_msg::<MasterMessage>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        ack,
        Some(MasterMessage::CancelTask {
            task_id: tid,
            reason: Some(ref r),
        }) if tid == task_id
            && r.starts_with(rusty_grid_core::protocol::master_features::RESULT_ACK_REASON)
    ));

    transport
        .send_msg(&WorkerMessage::Heartbeat {
            worker_id,
            timestamp: hb_ts,
            active_tasks: 0,
            cpu_usage_pct: 5.0,
            ram_available_mb: 4096,
        })
        .await
        .unwrap();
    let hb_ack = tokio::time::timeout(
        Duration::from_secs(2),
        transport.recv_msg::<MasterMessage>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        hb_ack,
        Some(MasterMessage::HeartbeatAck { timestamp }) if timestamp == hb_ts
    ));

    let after_done = master.registry.get_worker(worker_id).await.unwrap();
    assert_eq!(after_done.status, WorkerStatus::Connected);
    assert_eq!(after_done.active_tasks, 0);
    assert_eq!(
        master.queue.get_state(&task_id).await,
        Some(TaskState::Completed)
    );
}

#[tokio::test]
async fn tier5_resume_session_case11a_resent_while_running_does_not_double_credit() {
    use crate::reaper::ReaperConfig;
    use crate::registry::WorkerStatus;
    use crate::scheduler::SchedulerConfig;
    use rusty_grid_core::task::{TaskRequirements, TaskSpec, TaskStatus};

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_session_resume_hold(Duration::from_secs(30)),
        SchedulerConfig::default(),
        ReaperConfig::new(Duration::from_secs(600), Duration::from_secs(600)),
    )
    .await
    .unwrap();

    let worker_id = Uuid::new_v4();
    let mut transport =
        connect_and_register_tcp_worker(master.server_addr(), worker_id, "case11a-worker").await;

    let t1 = master
        .submit_task(Task::new(
            TaskSpec::command("sleep", vec!["10".into()]),
            TaskRequirements::generic(1, 64),
        ))
        .await
        .unwrap();
    let assigned = tokio::time::timeout(
        Duration::from_secs(2),
        transport.recv_msg::<MasterMessage>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(assigned, Some(MasterMessage::AssignTask { ref task }) if task.id == t1));

    transport
        .send_msg(&WorkerMessage::TaskProgress {
            worker_id,
            task_id: t1,
            status: TaskStatus::Running,
        })
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while master.queue.get_state(&t1).await != Some(TaskState::Running) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "t1 did not become Running"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let before = master.registry.get_worker(worker_id).await.unwrap();
    assert_eq!(before.status, WorkerStatus::Busy);
    assert_eq!(before.active_tasks, 1);

    // Re-send ResumeSession while t1 is already Running on the same session, then Heartbeat.
    transport
        .send_msg(&WorkerMessage::ResumeSession {
            worker_id,
            running: vec![t1],
        })
        .await
        .unwrap();
    transport
        .send_msg(&WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 1001,
            active_tasks: 1,
            cpu_usage_pct: 5.0,
            ram_available_mb: 4096,
        })
        .await
        .unwrap();
    let reply = tokio::time::timeout(
        Duration::from_secs(2),
        transport.recv_msg::<MasterMessage>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        matches!(reply, Some(MasterMessage::HeartbeatAck { timestamp: 1001 })),
        "expected HeartbeatAck without CancelTask, got {reply:?}"
    );

    let after_resume = master.registry.get_worker(worker_id).await.unwrap();
    assert_eq!(after_resume.status, WorkerStatus::Busy);
    assert_eq!(after_resume.active_tasks, 1);

    finish_tcp_task_and_verify_idle(&master, &mut transport, worker_id, t1, 1002).await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn tier5_resume_session_case11b_duplicate_ids_after_reconnect_adopts_once() {
    use crate::reaper::ReaperConfig;
    use crate::registry::WorkerStatus;
    use crate::scheduler::SchedulerConfig;
    use rusty_grid_core::task::{TaskRequirements, TaskSpec, TaskStatus};

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_session_resume_hold(Duration::from_secs(30)),
        SchedulerConfig::default(),
        ReaperConfig::new(Duration::from_secs(600), Duration::from_secs(600)),
    )
    .await
    .unwrap();

    let worker_id = Uuid::new_v4();
    let mut transport1 =
        connect_and_register_tcp_worker(master.server_addr(), worker_id, "case11b-worker").await;

    let t1 = master
        .submit_task(Task::new(
            TaskSpec::command("sleep", vec!["10".into()]),
            TaskRequirements::generic(1, 64),
        ))
        .await
        .unwrap();
    let assigned = tokio::time::timeout(
        Duration::from_secs(2),
        transport1.recv_msg::<MasterMessage>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(assigned, Some(MasterMessage::AssignTask { ref task }) if task.id == t1));

    transport1
        .send_msg(&WorkerMessage::TaskProgress {
            worker_id,
            task_id: t1,
            status: TaskStatus::Running,
        })
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while master.queue.get_state(&t1).await != Some(TaskState::Running) {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    drop(transport1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while master.queue.get_state(&t1).await != Some(TaskState::Queued)
        || master.registry.get_worker(worker_id).await.unwrap().status != WorkerStatus::Disconnected
    {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut transport2 =
        connect_and_register_tcp_worker(master.server_addr(), worker_id, "case11b-worker").await;

    transport2
        .send_msg(&WorkerMessage::ResumeSession {
            worker_id,
            running: vec![t1, t1],
        })
        .await
        .unwrap();
    transport2
        .send_msg(&WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 1002,
            active_tasks: 1,
            cpu_usage_pct: 5.0,
            ram_available_mb: 4096,
        })
        .await
        .unwrap();
    let reply = tokio::time::timeout(
        Duration::from_secs(2),
        transport2.recv_msg::<MasterMessage>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        matches!(reply, Some(MasterMessage::HeartbeatAck { timestamp: 1002 })),
        "expected HeartbeatAck without CancelTask, got {reply:?}"
    );

    assert_eq!(master.queue.get_state(&t1).await, Some(TaskState::Running));
    let after_dup = master.registry.get_worker(worker_id).await.unwrap();
    assert_eq!(after_dup.status, WorkerStatus::Busy);
    assert_eq!(after_dup.active_tasks, 1);

    finish_tcp_task_and_verify_idle(&master, &mut transport2, worker_id, t1, 1003).await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn tier5_resume_session_case11c_while_scheduled_does_not_cancel_or_double_credit() {
    use crate::reaper::ReaperConfig;
    use crate::registry::WorkerStatus;
    use crate::scheduler::SchedulerConfig;
    use rusty_grid_core::task::{TaskRequirements, TaskSpec};

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_session_resume_hold(Duration::from_secs(30)),
        SchedulerConfig::default(),
        ReaperConfig::new(Duration::from_secs(600), Duration::from_secs(600)),
    )
    .await
    .unwrap();

    let worker_id = Uuid::new_v4();
    let mut transport =
        connect_and_register_tcp_worker(master.server_addr(), worker_id, "case11c-worker").await;

    let t1 = master
        .submit_task(Task::new(
            TaskSpec::command("echo", vec!["case11c".into()]),
            TaskRequirements::generic(1, 64),
        ))
        .await
        .unwrap();
    let assigned = tokio::time::timeout(
        Duration::from_secs(2),
        transport.recv_msg::<MasterMessage>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(assigned, Some(MasterMessage::AssignTask { ref task }) if task.id == t1));
    assert_eq!(
        master.queue.get_state(&t1).await,
        Some(TaskState::Scheduled)
    );

    // Send ResumeSession while t1 is still Scheduled (before TaskProgress(Running)).
    transport
        .send_msg(&WorkerMessage::ResumeSession {
            worker_id,
            running: vec![t1],
        })
        .await
        .unwrap();
    transport
        .send_msg(&WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 1003,
            active_tasks: 1,
            cpu_usage_pct: 5.0,
            ram_available_mb: 4096,
        })
        .await
        .unwrap();
    let reply = tokio::time::timeout(
        Duration::from_secs(2),
        transport.recv_msg::<MasterMessage>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        matches!(reply, Some(MasterMessage::HeartbeatAck { timestamp: 1003 })),
        "expected HeartbeatAck without spurious CancelTask, got {reply:?}"
    );

    assert_eq!(
        master.queue.get_state(&t1).await,
        Some(TaskState::Scheduled)
    );
    let after_resume = master.registry.get_worker(worker_id).await.unwrap();
    assert_eq!(after_resume.status, WorkerStatus::Busy);
    assert_eq!(after_resume.active_tasks, 1);

    finish_tcp_task_and_verify_idle(&master, &mut transport, worker_id, t1, 1004).await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn tier5_resume_session_effect_d_cancelled_before_bind_preserves_post_revive_slot() {
    use crate::reaper::ReaperConfig;
    use crate::registry::{HeartbeatOutcome, WorkerStatus};
    use crate::scheduler::SchedulerConfig;
    use rusty_grid_core::task::{TaskRequirements, TaskResult, TaskSpec};

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_session_resume_hold(Duration::from_secs(30)),
        SchedulerConfig::default(),
        ReaperConfig::new(Duration::from_secs(600), Duration::from_secs(600)),
    )
    .await
    .unwrap();

    let w = Uuid::new_v4();
    master.queue.set_worker_session_resume(w, true);
    let (tx1, mut rx1) = tokio::sync::mpsc::channel(16);
    let s1 = master
        .registry
        .register(
            w,
            WorkerCapabilities::new("effect-d-worker", 4, 8192, false, false, None),
            "127.0.0.1:9709".parse().unwrap(),
            tx1,
            None,
        )
        .await
        .unwrap();

    let t1 = master
        .submit_task(Task::new(
            TaskSpec::command("sleep", vec!["60".into()]),
            TaskRequirements::generic(1, 64),
        ))
        .await
        .unwrap();
    let assigned_t1 = tokio::time::timeout(Duration::from_secs(2), rx1.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t1, MasterMessage::AssignTask { ref task } if task.id == t1));
    master
        .queue
        .mark_running_from(w, Some(s1), &t1)
        .await
        .unwrap();

    // Disconnect s1 so t1 enters an adoptable_by hold.
    assert!(master.registry.unregister(&w, Some(s1)).await.unwrap());
    master
        .queue
        .handle_session_disconnected(&w, Some(s1), "drop", false)
        .await;
    assert_eq!(master.queue.get_state(&t1).await, Some(TaskState::Queued));

    // Reconnect on s2 and simulate the Effect D interleaving:
    // adopt_session_tasks -> concurrent cancel_task -> add_adopted_tasks -> set_assigned_session_live.
    let (tx2, mut rx2) = tokio::sync::mpsc::channel(16);
    let s2 = master
        .registry
        .register(
            w,
            WorkerCapabilities::new("effect-d-worker", 4, 8192, false, false, None),
            "127.0.0.1:9710".parse().unwrap(),
            tx2,
            None,
        )
        .await
        .unwrap();

    let outcome = master.queue.adopt_session_tasks(w, s2, &[t1]).await;
    assert_eq!(outcome.newly_adopted, vec![t1]);

    master.cancel_task(t1).await.unwrap();
    let cancel_msg = tokio::time::timeout(Duration::from_secs(2), rx2.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(cancel_msg, MasterMessage::CancelTask { task_id, .. } if task_id == t1));

    let live = master
        .registry
        .add_adopted_tasks(&w, s2, outcome.newly_adopted.len())
        .await
        .expect("s2 is connected and must return session_live");
    master
        .queue
        .set_assigned_session_live(&outcome.newly_adopted, w, s2, live)
        .await;

    // Reap + revive s2, then dispatch t2.
    let hb = master.registry.last_heartbeat_instant(&w).await.unwrap();
    let reaped = master
        .registry
        .reap_stale_sessions_at(hb + Duration::from_secs(30), Duration::from_secs(5))
        .await;
    assert_eq!(reaped, vec![(w, s2)]);
    assert_eq!(
        master
            .registry
            .record_heartbeat_session(&w, 0, 1006, 10.0, 4096, Some(s2))
            .await
            .unwrap(),
        HeartbeatOutcome::Revived
    );

    let t2 = master
        .submit_task(Task::new(
            TaskSpec::command("echo", vec!["effect-d-t2".into()]),
            TaskRequirements::generic(1, 64),
        ))
        .await
        .unwrap();
    let assigned_t2 = tokio::time::timeout(Duration::from_secs(2), rx2.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(assigned_t2, MasterMessage::AssignTask { ref task } if task.id == t2));

    let before = master.registry.get_worker(w).await.unwrap();
    assert_eq!(before.status, WorkerStatus::Busy);
    assert_eq!(before.active_tasks, 1);

    // Late TaskResult for cancelled t1 must hit release_dispatch_if_live and leave t2's slot intact.
    super::tasks_and_blobs::handle_task_result(
        &master.registry,
        &master.queue,
        &master.scheduler_notify,
        &master.waiters,
        TaskResult::failure(w, t1, 130, "", "", 5, Some("cancelled".into())),
    )
    .await;

    let after = master.registry.get_worker(w).await.unwrap();
    assert_eq!(after.status, WorkerStatus::Busy);
    assert_eq!(after.active_tasks, 1);
    assert_eq!(
        master.queue.get_task(&t2).await.unwrap().state,
        TaskState::Scheduled
    );

    let _ = master.shutdown();
}
