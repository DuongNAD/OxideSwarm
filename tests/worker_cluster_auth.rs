//! Worker-side cluster authentication (OXS-AUTH): a worker proves knowledge of the shared secret
//! before it registers, refuses masters that cannot prove it, never hot-loops on an authentication
//! failure, and stays interruptible by shutdown while it connects and handshakes.
//!
//! Master-side enforcement (unauthenticated peers, QUIC, pre-auth limits) is covered by
//! `cluster_auth_e2e`; here the worker is the system under test, against a real `MasterServer` or
//! a scripted TCP peer.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use rusty_grid_core::cluster_auth::{server_handshake, ClusterSecret};
use rusty_grid_core::protocol::{MessageTransport, WorkerMessage};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_core::{GridError, GridResult};
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig, AUTH_FAILURE_MIN_BACKOFF};

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

fn worker_config(addr: SocketAddr, name: &str, secret: Option<&ClusterSecret>) -> WorkerConfig {
    let mut config = WorkerConfig::new(addr.to_string())
        .with_name(name)
        .with_cores(2)
        .with_redirection_portal(false);
    if let Some(secret) = secret {
        config = config.with_cluster_secret(secret.clone());
    }
    config
}

struct RunningWorker {
    shutdown_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
    _sandbox: tempfile::TempDir,
}

impl RunningWorker {
    fn start(config: WorkerConfig) -> Self {
        let sandbox = tempfile::tempdir().unwrap();
        let mut client = WorkerClient::new(config.with_sandbox_base_dir(sandbox.path()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(shutdown_rx).await;
        });
        Self {
            shutdown_tx,
            handle,
            _sandbox: sandbox,
        }
    }

    /// Requests shutdown and returns how long `run()` took to return.
    async fn stop(self) -> Duration {
        let started = Instant::now();
        let _ = self.shutdown_tx.send(true);
        tokio::time::timeout(Duration::from_secs(10), self.handle)
            .await
            .expect("worker did not stop within 10 s of the shutdown signal")
            .expect("worker task panicked");
        started.elapsed()
    }
}

async fn registered_workers(master: &MasterHandle) -> usize {
    master.list_workers().await.expect("list workers").len()
}

async fn wait_for_workers(master: &MasterHandle, count: usize, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if registered_workers(master).await == count {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    registered_workers(master).await == count
}

/// Runs the worker's handshake once against `addr` and returns its verdict.
async fn handshake_once(addr: SocketAddr, config: WorkerConfig) -> GridResult<Duration> {
    let client = WorkerClient::new(config);
    let stream = TcpStream::connect(addr).await.map_err(GridError::Io)?;
    let mut transport = MessageTransport::new(stream);
    client.perform_handshake(&mut transport).await
}

/// A TCP peer that runs the master side of the handshake with `secret` on every connection and
/// counts the connections. It never gets to see a `Register`: every handshake it takes part in
/// either fails or is dropped right after.
struct ScriptedMaster {
    addr: SocketAddr,
    attempts: Arc<AtomicUsize>,
    handshakes_ok: Arc<AtomicUsize>,
    _task: tokio::task::JoinHandle<()>,
}

impl ScriptedMaster {
    async fn start(secret: ClusterSecret) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let handshakes_ok = Arc::new(AtomicUsize::new(0));
        let (a, ok) = (Arc::clone(&attempts), Arc::clone(&handshakes_ok));
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                a.fetch_add(1, Ordering::SeqCst);
                let (secret, ok) = (secret.clone(), Arc::clone(&ok));
                tokio::spawn(async move {
                    let mut transport = MessageTransport::new(stream);
                    if server_handshake(&mut transport, &secret).await.is_ok() {
                        ok.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        });
        Self {
            addr,
            attempts,
            handshakes_ok,
            _task: task,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Matching secret
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_worker_with_matching_secret_registers_and_runs_tasks() {
    let secret = secret(1);
    let master = spawn_master(Some(&secret)).await;
    let worker = RunningWorker::start(worker_config(
        master.server_addr(),
        "authed-worker",
        Some(&secret),
    ));

    assert!(
        wait_for_workers(&master, 1, Duration::from_secs(10)).await,
        "an authenticated worker must register with a master that holds the same secret"
    );

    let task = Task::new(
        TaskSpec::command("echo", vec!["authenticated".into()]),
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit task");
    let result = master
        .wait_task(task_id, Some(Duration::from_secs(20)))
        .await
        .expect("task must complete on the authenticated worker");
    assert_eq!(result.exit_code, 0);
    assert!(result.stdout_str().contains("authenticated"));

    worker.stop().await;
    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Refusals surface as AuthRequired / AuthFailed and never register
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_worker_without_secret_gets_auth_required_from_secret_master() {
    let master = spawn_master(Some(&secret(2))).await;

    let verdict = handshake_once(
        master.server_addr(),
        worker_config(master.server_addr(), "w", None),
    )
    .await;
    match verdict {
        Err(GridError::AuthRequired(msg)) => {
            assert!(msg.contains("cluster secret"), "{msg}");
            assert!(
                msg.contains("OXIDESWARM_SECRET"),
                "actionable hint expected: {msg}"
            );
        }
        other => panic!("expected AuthRequired, got {other:?}"),
    }

    // The same worker run as a service never registers either.
    let worker = RunningWorker::start(worker_config(master.server_addr(), "no-secret", None));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(registered_workers(&master).await, 0);
    worker.stop().await;
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_worker_with_wrong_secret_gets_auth_failed_and_never_registers() {
    let master = spawn_master(Some(&secret(3))).await;
    let wrong = secret(4);

    // The master never proves knowledge of *our* secret, so the worker aborts before sending
    // anything about itself.
    let verdict = handshake_once(
        master.server_addr(),
        worker_config(master.server_addr(), "w", Some(&wrong)),
    )
    .await;
    match verdict {
        Err(GridError::AuthFailed(msg)) => assert!(!msg.is_empty()),
        other => panic!("expected AuthFailed, got {other:?}"),
    }

    let worker = RunningWorker::start(worker_config(
        master.server_addr(),
        "wrong-secret",
        Some(&wrong),
    ));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(registered_workers(&master).await, 0);
    worker.stop().await;
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_worker_with_secret_refuses_master_without_secret() {
    let master = spawn_master(None).await;
    let ours = secret(5);

    let verdict = handshake_once(
        master.server_addr(),
        worker_config(master.server_addr(), "w", Some(&ours)),
    )
    .await;
    match verdict {
        Err(GridError::AuthFailed(msg)) => {
            assert!(msg.contains("no cluster secret"), "{msg}");
        }
        other => panic!("expected AuthFailed from a secret-less master, got {other:?}"),
    }

    let worker = RunningWorker::start(worker_config(master.server_addr(), "w", Some(&ours)));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        registered_workers(&master).await,
        0,
        "a worker holding a secret must not obey a master that cannot prove it holds one"
    );
    worker.stop().await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_rogue_master_with_other_secret_never_sees_a_register() {
    let rogue = ScriptedMaster::start(secret(6)).await;
    let verdict =
        handshake_once(rogue.addr, worker_config(rogue.addr, "w", Some(&secret(7)))).await;
    assert!(
        matches!(verdict, Err(GridError::AuthFailed(_))),
        "got {verdict:?}"
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(rogue.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        rogue.handshakes_ok.load(Ordering::SeqCst),
        0,
        "the worker must abort before proving anything to a master that is not ours"
    );
}

// ---------------------------------------------------------------------------------------------
// No hot loop, still interruptible
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_auth_failure_backs_off_instead_of_hot_looping_and_shutdown_interrupts_it() {
    // Backoff configured as aggressively as possible: only the auth-failure floor can slow it.
    let rogue = ScriptedMaster::start(secret(8)).await;
    let sandbox = tempfile::tempdir().unwrap();
    let mut client = WorkerClient::new(
        worker_config(rogue.addr, "backoff-probe", Some(&secret(9)))
            .with_sandbox_base_dir(sandbox.path()),
    );
    client.set_backoff_config(rusty_grid_worker::BackoffConfig::new(
        Duration::from_millis(1),
        Duration::from_millis(5),
        1.0,
        0.0,
    ));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = client.run(shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        rogue.attempts.load(Ordering::SeqCst),
        1,
        "an authentication failure must not be retried within {AUTH_FAILURE_MIN_BACKOFF:?}"
    );

    // The 30 s wait is cut short by shutdown.
    let started = Instant::now();
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("shutdown must interrupt the authentication backoff")
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn test_auth_failure_floor_is_at_least_thirty_seconds() {
    assert!(AUTH_FAILURE_MIN_BACKOFF >= Duration::from_secs(30));
}

#[tokio::test]
async fn test_shutdown_interrupts_a_handshake_with_a_silent_master() {
    // A peer that accepts the connection and then says nothing. With a long handshake timeout the
    // worker would sit in the handshake; shutdown must still end it promptly.
    for secret in [None, Some(self::secret(10))] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _silent = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });

        let worker = RunningWorker::start(
            worker_config(addr, "silent-master", secret.as_ref())
                .with_handshake_timeout(Duration::from_secs(120)),
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        let took = worker.stop().await;
        assert!(
            took < Duration::from_secs(2),
            "shutdown took {took:?} with a silent master (secret: {})",
            secret.is_some()
        );
    }
}

#[tokio::test]
async fn test_unanswered_handshake_times_out_with_a_secret() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _silent = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });

    let started = Instant::now();
    let verdict = handshake_once(
        addr,
        worker_config(addr, "w", Some(&secret(11)))
            .with_handshake_timeout(Duration::from_millis(300)),
    )
    .await;
    assert!(
        matches!(verdict, Err(GridError::Timeout { .. })),
        "got {verdict:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn test_register_message_is_unchanged_without_a_secret() {
    // Backwards compatibility: no secret configured => the very first frame is still Register.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut transport = MessageTransport::new(stream);
        transport.recv_msg::<WorkerMessage>().await
    });
    let worker = RunningWorker::start(worker_config(addr, "legacy", None));
    let first = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("worker must connect")
        .unwrap()
        .expect("decodable frame")
        .expect("a frame");
    assert!(matches!(first, WorkerMessage::Register { .. }), "{first:?}");
    worker.stop().await;
}

// ---------------------------------------------------------------------------------------------
// P2P / QUIC: the same handshake runs on the control stream (needs the iroh relays, like the
// other P2P tests)
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_p2p_worker_authenticates_on_the_control_stream() {
    let dir = tempfile::tempdir().unwrap();
    let ticket_file = dir.path().join("master.ticket");
    let secret = secret(20);
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_p2p(true)
            .with_p2p_key_file(dir.path().join("master.key"))
            .with_p2p_ticket_file(&ticket_file)
            .with_cluster_secret(secret.clone()),
    )
    .await
    .expect("spawn p2p master");
    let ticket = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read ticket")
        .trim()
        .to_string();

    // The iroh ticket only addresses the master; the right secret is what lets a worker in.
    let good = RunningWorker::start(
        WorkerConfig::new("auto")
            .with_name("p2p-authed")
            .with_cores(2)
            .with_redirection_portal(false)
            .with_p2p_ticket(&ticket)
            .with_cluster_secret(secret.clone()),
    );
    assert!(
        wait_for_workers(&master, 1, Duration::from_secs(30)).await,
        "a P2P worker holding the secret must register"
    );

    // Same ticket, wrong or missing secret: connected at the QUIC level, refused at OXS-AUTH.
    let wrong = RunningWorker::start(
        WorkerConfig::new("auto")
            .with_name("p2p-wrong-secret")
            .with_cores(2)
            .with_redirection_portal(false)
            .with_p2p_ticket(&ticket)
            .with_cluster_secret(self::secret(21)),
    );
    let anonymous = RunningWorker::start(
        WorkerConfig::new("auto")
            .with_name("p2p-no-secret")
            .with_cores(2)
            .with_redirection_portal(false)
            .with_p2p_ticket(&ticket),
    );
    tokio::time::sleep(Duration::from_secs(6)).await;
    let names: Vec<String> = master
        .list_workers()
        .await
        .unwrap()
        .into_iter()
        .map(|w| w.capabilities.name)
        .collect();
    assert_eq!(
        names,
        vec!["p2p-authed"],
        "only the authenticated worker may register"
    );

    wrong.stop().await;
    anonymous.stop().await;
    good.stop().await;
    let _ = master.shutdown();
}
