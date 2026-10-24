//! End-to-end tests for cluster authentication (OXS-AUTH) on the master's grid port.
//!
//! The worker crate has no secret option yet, so peers here speak the protocol directly over TCP
//! (or a QUIC bi-stream) and drive `rusty_grid_core::cluster_auth::client_handshake` themselves.
//!
//! Validates, against a real `MasterServer`:
//! * a peer holding the secret can register a worker and submit work, and the work really runs;
//! * an unauthenticated `ClientMessage::SubmitTask` is refused and nothing is queued, dispatched or
//!   executed (asserted through a side-effect file), while an *open* master (control) runs the very
//!   same request, so the assertion is not vacuous;
//! * an unauthenticated or wrong-secret worker never registers and cannot take over a worker id;
//! * secret-less peers of a secret master (and the reverse) get an actionable error;
//! * pre-authentication limits (oversized first frame, silent peer) cut the connection off;
//! * the discovery beacon is signed only for authenticated probes;
//! * the same handshake protects the QUIC/iroh control stream.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::watch;
use tokio_util::bytes::Bytes;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::cluster_auth::{client_handshake, ClusterSecret, WIRE_FORMAT_AUTH};
use rusty_grid_core::discovery::{build_auth_probe, MasterBeacon, DISCOVERY_MAGIC_REQUEST};
use rusty_grid_core::protocol::{
    derive_aead_keys, serialize_message, ClientMessage, ClientResponse, EndpointRole,
    MasterMessage, MessageTransport, ProtocolCapabilities, ProtocolError, WireCodec, WorkerMessage,
    PROTOCOL_VERSION_CURRENT, WIRE_FORMAT_ENCRYPTED,
};
use rusty_grid_core::task::{Bytes as TaskBytes, Task, TaskRequirements, TaskSpec};
use rusty_grid_core::{GridError, GridResult};
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;

fn secret(seed: u8) -> ClusterSecret {
    ClusterSecret::from_bytes(&[seed; 32]).unwrap()
}

async fn spawn_master(secret: Option<&ClusterSecret>) -> MasterHandle {
    let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_handshake_timeout(2);
    if let Some(secret) = secret {
        config = config.with_cluster_secret(secret.clone());
    }
    MasterServer::spawn(config).await.expect("spawn master")
}

#[allow(dead_code)]
async fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    condition()
}

async fn worker_names(master: &MasterHandle) -> Vec<String> {
    master
        .list_workers()
        .await
        .expect("list workers")
        .into_iter()
        .map(|w| w.capabilities.name)
        .collect()
}

fn shell_task(script: String) -> Task {
    Task::new(
        TaskSpec::new_shell_script(script),
        TaskRequirements::generic(1, 30),
    )
}

/// `touch '<path>'`: the side effect every test uses to prove (or disprove) execution.
fn touch_script(path: &std::path::Path) -> String {
    format!("touch '{}'", path.display())
}

fn register_msg(worker_id: Uuid, name: &str) -> WorkerMessage {
    WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities::new(name, 2, 4096, false, false, None),
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: ProtocolCapabilities::all_supported(),
    }
}

/// A protocol-level worker: authenticates (when given a secret), registers, records every shell
/// script the master assigns to it and, if `execute` is set, really runs it with `sh -c`.
struct FakeWorker {
    worker_id: Uuid,
    assigned: Arc<Mutex<Vec<String>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl FakeWorker {
    fn assigned(&self) -> Vec<String> {
        self.assigned.lock().unwrap().clone()
    }
}

async fn start_fake_worker(
    addr: SocketAddr,
    secret: Option<&ClusterSecret>,
    execute: bool,
) -> GridResult<FakeWorker> {
    let stream = TcpStream::connect(addr).await.map_err(GridError::Io)?;
    let mut transport = MessageTransport::new(stream);
    if let Some(secret) = secret {
        let session_key = client_handshake(&mut transport, secret).await?;
        transport.enable_encryption(&session_key, EndpointRole::Client);
    }
    let worker_id = Uuid::new_v4();
    transport
        .send_msg(&register_msg(worker_id, "fake-worker"))
        .await?;
    match transport.recv_msg::<MasterMessage>().await? {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => {
            return Err(GridError::RegistrationRejected(format!(
                "unexpected answer to Register: {other:?}"
            )))
        }
    }

    let (mut writer, mut reader) = transport.split();
    let assigned = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&assigned);
    let task = tokio::spawn(async move {
        while let Ok(Some(msg)) = reader.recv_msg::<MasterMessage>().await {
            let MasterMessage::AssignTask { task } = msg else {
                continue;
            };
            if let TaskSpec::ShellScript { script, .. } = &task.spec {
                log.lock().unwrap().push(script.clone());
                if execute {
                    let _ = tokio::process::Command::new("sh")
                        .arg("-c")
                        .arg(script)
                        .status()
                        .await;
                }
            }
            let _ = writer
                .send_msg(&WorkerMessage::TaskResult {
                    worker_id,
                    task_id: task.id,
                    exit_code: 0,
                    stdout: TaskBytes::from_static(b""),
                    stderr: TaskBytes::from_static(b""),
                    execution_time_ms: 1,
                    is_gpu_executed: false,
                    device_name: None,
                    error: None,
                    artifacts: Vec::new(),
            asset_bytes: None,
        })
                .await;
        }
    });
    Ok(FakeWorker {
        worker_id,
        assigned,
        _task: task,
    })
}

/// Submits `task` as a client that authenticates first. Returns the master's answer.
#[allow(dead_code)]
async fn submit_authenticated(
    addr: SocketAddr,
    secret: &ClusterSecret,
    task: Task,
) -> GridResult<ClientResponse> {
    let stream = TcpStream::connect(addr).await.map_err(GridError::Io)?;
    let mut transport = MessageTransport::new(stream);
    let session_key = client_handshake(&mut transport, secret).await?;
    transport.enable_encryption(&session_key, EndpointRole::Client);
    transport
        .send_msg(&ClientMessage::SubmitTask { task, wait: false })
        .await?;
    transport
        .recv_msg::<ClientResponse>()
        .await?
        .ok_or(GridError::ConnectionClosed)
}

/// What an unauthenticated client observes after sending a request to a secret-protected master:
/// either the connection is closed or it carries an authentication frame that no typed decoder
/// accepts. A real protocol answer would be a security failure.
async fn assert_request_is_refused(transport: &mut MessageTransport<TcpStream>) {
    match tokio::time::timeout(
        Duration::from_secs(3),
        transport.recv_msg::<ClientResponse>(),
    )
    .await
    {
        Ok(Ok(Some(answer))) => {
            panic!("unauthenticated peer received a protocol answer: {answer:?}")
        }
        Ok(Ok(None)) | Ok(Err(_)) => {}
        Err(_) => panic!("master left an unauthenticated connection hanging"),
    }
}

// ---------------------------------------------------------------------------------------------
// Matching secret: everything works
// ---------------------------------------------------------------------------------------------

#[cfg(unix)]
#[tokio::test]
async fn test_matching_secret_registers_worker_and_runs_submitted_task() {
    let secret = secret(1);
    let master = spawn_master(Some(&secret)).await;
    let addr = master.server_addr();

    let _worker = start_fake_worker(addr, Some(&secret), true)
        .await
        .expect("authenticated worker must register");
    assert_eq!(worker_names(&master).await, vec!["fake-worker"]);

    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("authorized.marker");
    let answer = submit_authenticated(addr, &secret, shell_task(touch_script(&marker)))
        .await
        .expect("authenticated submit");
    assert!(
        matches!(answer, ClientResponse::TaskSubmitted { .. }),
        "got {answer:?}"
    );
    assert!(
        wait_until(Duration::from_secs(5), || marker.exists()).await,
        "the authorized task must have run on the worker"
    );

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Unauthenticated SubmitTask runs nothing
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_unauthenticated_submit_task_is_refused_and_runs_nothing() {
    let secret = secret(2);
    let master = spawn_master(Some(&secret)).await;
    let addr = master.server_addr();

    // A legitimate, authenticated worker that would happily execute whatever it is given.
    let worker = start_fake_worker(addr, Some(&secret), cfg!(unix))
        .await
        .expect("authenticated worker must register");

    let dir = tempfile::tempdir().unwrap();
    let attack_marker = dir.path().join("attacker.marker");

    // The attacker never performs the handshake and sends the request straight away.
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut attacker = MessageTransport::new(stream);
    attacker
        .send_msg(&ClientMessage::SubmitTask {
            task: shell_task(touch_script(&attack_marker)),
            wait: false,
        })
        .await
        .expect("send attack");
    assert_request_is_refused(&mut attacker).await;

    // Give a (wrongly) accepted task ample time to be scheduled, dispatched and executed.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        master.list_tasks().await.unwrap().is_empty(),
        "the refused request must not have created a task"
    );
    assert!(
        worker.assigned().is_empty(),
        "nothing may be dispatched to workers: {:?}",
        worker.assigned()
    );
    assert!(
        !attack_marker.exists(),
        "the attacker's script must never run"
    );

    let _ = master.shutdown();
}

/// Control for the test above: without a secret the very same request is accepted and executed, so
/// the secret is what stops it (and the assertions above are capable of failing).
#[cfg(unix)]
#[tokio::test]
async fn test_control_open_master_runs_unauthenticated_submission() {
    let master = spawn_master(None).await;
    let addr = master.server_addr();
    let _worker = start_fake_worker(addr, None, true).await.expect("worker");

    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("open.marker");
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut client = MessageTransport::new(stream);
    client
        .send_msg(&ClientMessage::SubmitTask {
            task: shell_task(touch_script(&marker)),
            wait: false,
        })
        .await
        .unwrap();
    assert!(matches!(
        client.recv_msg::<ClientResponse>().await.unwrap(),
        Some(ClientResponse::TaskSubmitted { .. })
    ));
    assert!(
        wait_until(Duration::from_secs(5), || marker.exists()).await,
        "an open master runs unauthenticated work (this is what the secret prevents)"
    );

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Workers that cannot authenticate never register
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_unauthenticated_worker_cannot_register_or_take_over_a_worker_id() {
    let secret = secret(3);
    let master = spawn_master(Some(&secret)).await;
    let addr = master.server_addr();
    let legit = start_fake_worker(addr, Some(&secret), false)
        .await
        .expect("legit worker");

    // Same worker id, no handshake: the Register must be dropped on the floor.
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut attacker = MessageTransport::new(stream);
    attacker
        .send_msg(&register_msg(legit.worker_id, "attacker-worker"))
        .await
        .unwrap();
    match tokio::time::timeout(Duration::from_secs(3), attacker.recv_msg::<MasterMessage>()).await {
        Ok(Ok(Some(answer))) => panic!("unauthenticated Register was answered: {answer:?}"),
        Ok(Ok(None)) | Ok(Err(_)) => {}
        Err(_) => panic!("master left the unauthenticated connection hanging"),
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        worker_names(&master).await,
        vec!["fake-worker"],
        "the original worker must still own its id"
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_wrong_secret_worker_never_registers() {
    let master = spawn_master(Some(&secret(4))).await;
    let addr = master.server_addr();

    // An honest client with the wrong secret refuses to continue: the master's proof does not verify.
    let err = start_fake_worker(addr, Some(&secret(5)), false)
        .await
        .err()
        .expect("a worker with the wrong secret must not register");
    assert!(matches!(err, GridError::AuthFailed(_)), "got {err:?}");

    // A dishonest client that ignores the master's proof and answers with a forged one is rejected
    // by the master with the generic reason.
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut forger = MessageTransport::new(stream);
    let mut hello = vec![WIRE_FORMAT_AUTH, 0x01, 1];
    hello.extend_from_slice(&[7u8; 32]);
    forger.send_raw_frame(Bytes::from(hello)).await.unwrap();
    let challenge = forger.recv_raw_frame().await.unwrap().expect("CHALLENGE");
    assert_eq!(challenge.len(), 2 + 32 + 32);
    assert_eq!(&challenge[..2], &[WIRE_FORMAT_AUTH, 0x02]);
    let mut proof = vec![WIRE_FORMAT_AUTH, 0x03];
    proof.extend_from_slice(&[0u8; 32]);
    forger.send_raw_frame(Bytes::from(proof)).await.unwrap();
    let reject = forger.recv_raw_frame().await.unwrap().expect("REJECT");
    assert_eq!(&reject[..2], &[WIRE_FORMAT_AUTH, 0x05]);
    assert_eq!(&reject[2..], b"authentication failed");

    // Continuing on that connection as if authenticated gets nowhere.
    let _ = forger
        .send_msg(&register_msg(Uuid::new_v4(), "forged-worker"))
        .await;
    match tokio::time::timeout(Duration::from_secs(3), forger.recv_msg::<MasterMessage>()).await {
        Ok(Ok(Some(answer))) => panic!("forged session was answered: {answer:?}"),
        Ok(_) => {}
        Err(_) => panic!("master left the rejected connection open"),
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        worker_names(&master).await.is_empty(),
        "no worker may have registered"
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_worker_without_secret_support_never_registers_with_a_secret_master() {
    // The real WorkerClient (public API as it is today) has no secret, like every worker deployed
    // before authentication existed.
    let master = spawn_master(Some(&secret(6))).await;
    let addr = master.server_addr().to_string();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker = WorkerClient::from_options(addr, Some("plain-worker".into()), Some(2), false);
    let handle = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        worker_names(&master).await.is_empty(),
        "a worker that cannot authenticate must never be registered"
    );

    let _ = shutdown_tx.send(true);
    handle.abort();
    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Mismatched configuration gives actionable errors
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_secretless_client_gets_auth_required_from_a_secret_master() {
    let master = spawn_master(Some(&secret(7))).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut client = MessageTransport::new(stream);
    client
        .send_msg(&ClientMessage::ClusterStatus)
        .await
        .unwrap();

    let err: GridError = client
        .recv_msg::<ClientResponse>()
        .await
        .expect_err("the master answers with an authentication frame, not a typed response")
        .into();
    match err {
        GridError::AuthRequired(msg) => {
            assert!(msg.contains("cluster secret"), "{msg}");
            assert!(msg.contains("OXIDESWARM_SECRET"), "{msg}");
        }
        other => panic!("expected AuthRequired, got {other:?}"),
    }
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_secret_client_is_told_when_the_master_has_no_secret() {
    let master = spawn_master(None).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut client = MessageTransport::new(stream);

    let err = client_handshake(&mut client, &secret(8))
        .await
        .expect_err("a master without a secret cannot prove anything");
    match err {
        GridError::AuthFailed(msg) => assert!(msg.contains("no cluster secret"), "{msg}"),
        other => panic!("expected AuthFailed, got {other:?}"),
    }
    assert!(worker_names(&master).await.is_empty());
    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Pre-authentication limits
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_oversized_first_frame_is_cut_off_and_master_stays_healthy() {
    let secret = secret(9);
    let master = spawn_master(Some(&secret)).await;
    let addr = master.server_addr();

    // Announce a 10 MB frame: it must be refused from the length header alone.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&(10_000_000u32).to_be_bytes())
        .await
        .unwrap();
    let _ = stream.write_all(&[0u8; 64]).await;
    let mut buf = [0u8; 256];
    let started = Instant::now();
    match tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(n)) => {
            // At most one generic REJECT frame, then the connection ends.
            assert!(
                n >= 6 && buf[4] == WIRE_FORMAT_AUTH && buf[5] == 0x05,
                "{:?}",
                &buf[..n]
            );
        }
        Err(_) => panic!("master kept waiting for a 10 MB unauthenticated frame"),
    }
    assert!(started.elapsed() < Duration::from_secs(3));

    // The legitimate path is unaffected.
    start_fake_worker(addr, Some(&secret), false)
        .await
        .expect("authenticated worker still registers");

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_silent_unauthenticated_peer_is_dropped_after_the_handshake_timeout() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_handshake_timeout(1)
        .with_cluster_secret(secret(10));
    let master = MasterServer::spawn(config).await.expect("spawn master");

    let mut stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let started = Instant::now();
    let mut buf = [0u8; 16];
    match tokio::time::timeout(Duration::from_secs(4), stream.read(&mut buf)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(n)) => panic!("silent peer received {n} bytes"),
        Err(_) => panic!("silent peer was never dropped"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the handshake timeout must bound how long a silent peer is kept"
    );
    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Configuration hygiene and discovery
// ---------------------------------------------------------------------------------------------

#[test]
fn test_server_config_debug_does_not_leak_the_secret() {
    let secret = secret(11);
    let config =
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_cluster_secret(secret.clone());
    let rendered = format!("{config:?}");
    assert!(!rendered.contains(&secret.to_hex()), "{rendered}");
    assert!(rendered.contains("redacted"));
}

#[tokio::test]
async fn test_discovery_beacon_is_signed_only_for_authenticated_probes() {
    let secret = secret(12);
    let udp_port = std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_discovery(true)
        .with_discovery_port(udp_port)
        .with_cluster_secret(secret.clone());
    let master = MasterServer::spawn(config).await.expect("spawn master");

    let probe_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target: SocketAddr = format!("127.0.0.1:{udp_port}").parse().unwrap();
    // The responder binds asynchronously: keep probing until it answers.
    let ask = |probe: Vec<u8>| {
        let socket = &probe_socket;
        async move {
            let mut buf = [0u8; 2048];
            for _ in 0..30 {
                socket.send_to(&probe, target).await.unwrap();
                if let Ok(Ok((len, _))) =
                    tokio::time::timeout(Duration::from_millis(100), socket.recv_from(&mut buf))
                        .await
                {
                    return MasterBeacon::from_bytes(&buf[..len]);
                }
            }
            None
        }
    };

    // A legacy probe is answered, but the beacon carries no proof of the secret.
    let plain = ask(DISCOVERY_MAGIC_REQUEST.to_vec())
        .await
        .expect("beacon for plain probe");
    assert!(plain.mac.is_none());

    // An authenticated probe gets a beacon bound to its nonce and to the secret.
    let nonce = [0x5a; 16];
    let signed = ask(build_auth_probe(&nonce))
        .await
        .expect("beacon for authenticated probe");
    assert!(signed.verify(&secret, &nonce));
    assert!(!signed.verify(&self::secret(13), &nonce), "wrong secret");
    assert!(
        !signed.verify(&secret, &[0xa5; 16]),
        "replayed for another probe"
    );

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// QUIC / iroh: the same handshake guards the control stream
// ---------------------------------------------------------------------------------------------

#[cfg(feature = "p2p")]
mod quic {
    use super::*;
    use rusty_grid_core::transport::{
        iroh, parse_p2p_ticket, BiStream, GRID_ALPN, STREAM_CONTROL, STREAM_DATA,
    };

    type ControlLane =
        MessageTransport<BiStream<iroh::endpoint::RecvStream, iroh::endpoint::SendStream>>;

    /// The control lane plus everything that must stay alive for the lane to keep working.
    struct Lanes {
        control: ControlLane,
        _keep_alive: (
            iroh::endpoint::Connection,
            iroh::endpoint::SendStream,
            iroh::endpoint::RecvStream,
        ),
    }

    /// Opens the two tagged lanes a worker opens and returns the control lane as a transport.
    async fn open_lanes(client: &iroh::Endpoint, master_addr: iroh::EndpointAddr) -> Lanes {
        let conn = client
            .connect(master_addr, GRID_ALPN)
            .await
            .expect("connect to master over QUIC");
        let (mut ctrl_send, ctrl_recv) = conn.open_bi().await.expect("control stream");
        ctrl_send.write_all(&[STREAM_CONTROL]).await.unwrap();
        let (mut data_send, data_recv) = conn.open_bi().await.expect("data stream");
        data_send.write_all(&[STREAM_DATA]).await.unwrap();
        Lanes {
            control: MessageTransport::new(BiStream::new(ctrl_recv, ctrl_send)),
            _keep_alive: (conn, data_send, data_recv),
        }
    }

    async fn local_client() -> iroh::Endpoint {
        iroh::Endpoint::builder(iroh::endpoint::presets::N0)
            .relay_mode(iroh::endpoint::RelayMode::Disabled)
            .alpns(vec![GRID_ALPN.to_vec()])
            .bind()
            .await
            .expect("bind client endpoint")
    }

    #[tokio::test]
    async fn test_quic_control_stream_requires_the_cluster_secret() {
        let secret = secret(20);
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_p2p(true)
            .with_ephemeral_key(true)
            .with_handshake_timeout(3)
            .with_cluster_secret(secret.clone());
        let master = MasterServer::spawn(config).await.expect("spawn master");
        let ticket = master.p2p_ticket().expect("p2p ticket").to_string();
        let mut master_addr = parse_p2p_ticket(&ticket).expect("parse ticket");
        // Reach the master directly over the local interface; no relay is involved.
        master_addr.addrs.retain(|a| !a.is_relay());
        let client = local_client().await;

        // Having the ticket is not enough: Register without the handshake is dropped.
        let mut unauthenticated = open_lanes(&client, master_addr.clone()).await;
        unauthenticated
            .control
            .send_msg(&register_msg(Uuid::new_v4(), "quic-intruder"))
            .await
            .unwrap();
        match tokio::time::timeout(
            Duration::from_secs(5),
            unauthenticated.control.recv_msg::<MasterMessage>(),
        )
        .await
        {
            Ok(Ok(Some(answer))) => {
                panic!("unauthenticated QUIC Register was answered: {answer:?}")
            }
            Ok(_) => {}
            Err(_) => panic!("master left the unauthenticated QUIC stream hanging"),
        }

        // The wrong secret does not get through either.
        let mut wrong = open_lanes(&client, master_addr.clone()).await;
        let err = client_handshake(&mut wrong.control, &super::secret(21))
            .await
            .expect_err("wrong secret");
        assert!(matches!(err, GridError::AuthFailed(_)), "got {err:?}");

        assert!(worker_names(&master).await.is_empty());

        // The right secret registers.
        let mut authenticated = open_lanes(&client, master_addr).await;
        let session_key = client_handshake(&mut authenticated.control, &secret)
            .await
            .expect("handshake with the right secret");
        authenticated
            .control
            .enable_encryption(&session_key, EndpointRole::Client);
        let worker_id = Uuid::new_v4();
        authenticated
            .control
            .send_msg(&register_msg(worker_id, "quic-worker"))
            .await
            .unwrap();
        match authenticated
            .control
            .recv_msg::<MasterMessage>()
            .await
            .unwrap()
        {
            Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
            other => panic!("expected an accepted RegisterAck, got {other:?}"),
        }
        assert_eq!(worker_names(&master).await, vec!["quic-worker"]);

        let _ = master.shutdown();
    }
}

#[test]
fn test_protocol_error_for_auth_frames_maps_to_auth_required() {
    // What the typed decoder reports for a REJECT frame, as used by every secret-less client.
    let err: GridError = ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH).into();
    assert!(matches!(err, GridError::AuthRequired(_)));
}

// =============================================================================================
// Milestone 3: Authenticated TCP Stream Encryption (AEAD Record Layer) Verification
// =============================================================================================

#[tokio::test]
async fn test_all_post_handshake_frames_use_wire_format_encrypted() {
    let secret = secret(42);
    let master = spawn_master(Some(&secret)).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    assert!(!transport.is_encrypted());
    let session_key = client_handshake(&mut transport, &secret)
        .await
        .expect("handshake succeeds");
    assert!(!transport.is_encrypted());

    // Upgrade client transport
    transport.enable_encryption(&session_key, EndpointRole::Client);
    assert!(transport.is_encrypted());

    // Send Register
    let worker_id = Uuid::new_v4();
    transport
        .send_msg(&register_msg(worker_id, "encrypted-worker"))
        .await
        .expect("send encrypted register");

    // Read the raw frame from the wire directly to inspect its wire format byte
    let raw_frame = transport
        .recv_raw_frame()
        .await
        .expect("recv raw frame succeeds")
        .expect("frame present");

    // Wire format byte MUST be WIRE_FORMAT_ENCRYPTED (0x04)
    assert_eq!(
        raw_frame[0], WIRE_FORMAT_ENCRYPTED,
        "Master must respond with wire-encrypted frame (0x04)"
    );
    // Sequence counter in AAD/frame header must be 0 for the first frame
    let seq = u64::from_be_bytes(raw_frame[1..9].try_into().unwrap());
    assert_eq!(
        seq, 0,
        "First encrypted frame from server must have sequence 0"
    );

    let (_, mut reader_session) = derive_aead_keys(&session_key, EndpointRole::Client);
    let decrypted = reader_session
        .decrypt_frame(&raw_frame)
        .expect("decrypt raw frame");
    let (ack, _): (MasterMessage, _) =
        rusty_grid_core::protocol::deserialize_message(&decrypted).expect("deserialize ack");
    assert!(matches!(
        ack,
        MasterMessage::RegisterAck { accepted: true, .. }
    ));

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_tampered_ciphertext_causes_connection_rejection() {
    let secret = secret(43);
    let master = spawn_master(Some(&secret)).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let session_key = client_handshake(&mut transport, &secret)
        .await
        .expect("handshake succeeds");

    let (mut writer_session, _) = derive_aead_keys(&session_key, EndpointRole::Client);

    let worker_id = Uuid::new_v4();
    let payload = serialize_message(
        &register_msg(worker_id, "tamper-worker"),
        WireCodec::Bincode,
    )
    .unwrap();
    let encrypted_frame = writer_session.encrypt_frame(&payload).expect("seal frame");

    // Tamper with the ciphertext (flip a bit in the encrypted payload)
    let mut tampered = encrypted_frame.to_vec();
    let len = tampered.len();
    tampered[len - 1] ^= 0x55;

    // Send the tampered frame directly
    transport
        .send_raw_frame(Bytes::from(tampered))
        .await
        .unwrap();

    // Master AEAD reader must fail authentication and immediately drop/close the connection
    let resp: Result<Option<MasterMessage>, _> = transport.recv_msg().await;
    match resp {
        Ok(None) => {} // Clean connection closed by master
        Err(_) => {}   // Connection reset or protocol error
        Ok(Some(msg)) => panic!("Master must not accept tampered ciphertext frame! Got: {msg:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_replayed_sequence_number_causes_connection_rejection() {
    let secret = secret(44);
    let master = spawn_master(Some(&secret)).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    let session_key = client_handshake(&mut transport, &secret)
        .await
        .expect("handshake succeeds");

    let (mut writer_session, mut reader_session) =
        derive_aead_keys(&session_key, EndpointRole::Client);

    let worker_id = Uuid::new_v4();
    let payload = serialize_message(
        &register_msg(worker_id, "replay-worker"),
        WireCodec::Bincode,
    )
    .unwrap();

    // Encrypt frame 1 with seq = 0
    let frame_seq0 = writer_session
        .encrypt_frame(&payload)
        .expect("seal frame 0");
    // Send frame 0 legitimately
    transport.send_raw_frame(frame_seq0.clone()).await.unwrap();

    // Read ack from master
    let raw_ack = transport
        .recv_raw_frame()
        .await
        .unwrap()
        .expect("ack frame");
    assert_eq!(raw_ack[0], WIRE_FORMAT_ENCRYPTED);
    let _ = reader_session.decrypt_frame(&raw_ack).expect("decrypt ack");

    // Replay frame_seq0 (seq = 0 again, when master expects seq = 1)
    transport.send_raw_frame(frame_seq0).await.unwrap();

    // Master AEAD reader MUST reject the replayed sequence number and terminate connection
    let resp: Result<Option<MasterMessage>, _> = transport.recv_msg().await;
    match resp {
        Ok(None) => {} // Connection closed
        Err(_) => {}   // Reset or protocol error
        Ok(Some(msg)) => panic!("Master must reject replayed sequence number! Got: {msg:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_unauthenticated_loopback_fallback_works_seamlessly() {
    // Master with no secret configured (cluster_secret: None)
    let master = spawn_master(None).await;
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut transport = MessageTransport::new(stream);

    // Without secret, transport remains unencrypted
    assert!(!transport.is_encrypted());

    // Client sends request without handshake
    transport
        .send_msg(&ClientMessage::ClusterStatus)
        .await
        .expect("send cluster status");

    // Receives plain bincode/json response with zero crypto overhead
    let resp = transport
        .recv_msg::<ClientResponse>()
        .await
        .expect("recv cluster status")
        .expect("response present");

    assert!(matches!(resp, ClientResponse::ClusterStatus { .. }));
    assert!(!transport.is_encrypted());

    let _ = master.shutdown();
}
