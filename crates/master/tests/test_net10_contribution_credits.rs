//! NET-10 Option B integration tests: two enrolled v3 participants on loopback, exact
//! double-entry core-ms conservation, restart persistence in `--state-dir`, bounded rollover
//! compaction, `GET /api/credits` RBAC (`Observer`), and worker rejections never booking credit.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::noise_auth::{
    v3_client_handshake, HandshakePurpose, MemberEntry, MemberRole, NodeKeypair, PublicKey,
};
use rusty_grid_core::protocol::{
    ClientMessage, ClientResponse, EndpointRole, ProtocolCapabilities, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskResult, TaskSpec};
use rusty_grid_core::{MasterMessage, MessageTransport, WorkerCapabilities, WorkerMessage};
use rusty_grid_master::auth::{AuthConfig, AuthScope};
use rusty_grid_master::credits::CreditsSnapshot;
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::server::{AuthMode, MasterHandle, MasterServer, MemberStore, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

struct V3TestWorker {
    shutdown: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
}

impl V3TestWorker {
    fn spawn(
        master_addr: SocketAddr,
        name: &str,
        cores: usize,
        node_key: NodeKeypair,
        master_pub: PublicKey,
        sandbox_base: &Path,
    ) -> Self {
        let (shutdown, rx) = watch::channel(false);
        let config = WorkerConfig::new(master_addr.to_string())
            .with_no_battery(true)
            .with_name(name)
            .with_cores(cores)
            .with_node_key(node_key)
            .with_master_public_key(master_pub)
            .with_sandbox_base_dir(sandbox_base);
        let mut client = WorkerClient::new(config);
        let handle = tokio::spawn(async move {
            let _ = client.run(rx).await;
        });
        Self { shutdown, handle }
    }

    async fn stop(self) {
        let _ = self.shutdown.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), self.handle).await;
    }
}

fn make_members(alice_pub: &PublicKey, bob_pub: &PublicKey) -> MemberStore {
    MemberStore::from_entries(vec![
        MemberEntry::new(
            "alice",
            *alice_pub,
            vec![MemberRole::Worker, MemberRole::Client],
        ),
        MemberEntry::new(
            "bob",
            *bob_pub,
            vec![MemberRole::Worker, MemberRole::Client],
        ),
    ])
}

async fn wait_for_two_workers(master: &MasterHandle) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(workers) = master.list_workers().await {
            if workers.len() == 2 {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for both v3 workers to register"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn submit_v3_and_wait(
    addr: SocketAddr,
    client_kp: &NodeKeypair,
    master_pub: &PublicKey,
    target_worker: &str,
    cpu_cores: usize,
    tag: &str,
) -> TaskResult {
    let stream = TcpStream::connect(addr).await.expect("connect v3 client");
    let mut t = MessageTransport::new(stream);
    let sk = v3_client_handshake(&mut t, client_kp, master_pub, HandshakePurpose::Member)
        .await
        .expect("v3 client handshake");
    t.enable_encryption(&sk, EndpointRole::Client);

    let task = Task::new(
        TaskSpec::Command {
            program: "echo".into(),
            args: vec![tag.into()],
            env: Default::default(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(cpu_cores, 30),
    )
    .with_tags(vec![format!("require:worker={target_worker}")]);

    t.send_msg(&ClientMessage::SubmitTask { task, wait: true })
        .await
        .expect("send SubmitTask");
    match t
        .recv_msg::<ClientResponse>()
        .await
        .expect("recv ClientResponse")
    {
        Some(ClientResponse::TaskCompleted { result, .. }) => {
            assert!(result.is_success(), "task {tag} failed: {result:?}");
            result
        }
        other => panic!("expected TaskCompleted for {tag}, got {other:?}"),
    }
}

async fn spawn_master_with_retry(config: ServerConfig) -> MasterHandle {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match MasterServer::spawn(config.clone()).await {
            Ok(handle) => return handle,
            Err(e) if Instant::now() < deadline => {
                let msg = e.to_string();
                if msg.contains("store.lock")
                    || msg.contains("Resource temporarily unavailable")
                    || msg.contains("held by another")
                {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    continue;
                }
                panic!("unexpected MasterServer::spawn error: {e}");
            }
            Err(e) => panic!("MasterServer::spawn failed past deadline: {e}"),
        }
    }
}

fn assert_zero_sum_all_windows(snap: &CreditsSnapshot) {
    for sum in [&snap.net_sum_24h, &snap.net_sum_7d, &snap.net_sum_all_time] {
        assert_eq!(sum.tasks, 0);
        assert_eq!(sum.core_ms, 0);
        assert_eq!(sum.gpu_ms, 0);
        assert_eq!(sum.tunnel_sessions, 0);
        assert_eq!(sum.tunnel_duration_ms, 0);
        assert_eq!(sum.tunnel_bytes, 0);
    }
}

#[tokio::test]
async fn test_net10_two_v3_participants_conservation_restart_and_rbac() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");
    let alice_sb = temp.path().join("alice-sb");
    let bob_sb = temp.path().join("bob-sb");

    let master_kp = NodeKeypair::generate().expect("master kp");
    let master_pub = *master_kp.public_key();
    let alice_kp = NodeKeypair::generate().expect("alice kp");
    let bob_kp = NodeKeypair::generate().expect("bob kp");
    let alice_id = alice_kp.public_key().member_id();
    let bob_id = bob_kp.public_key().member_id();

    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_state_dir(&state_dir)
        .with_auth_mode(AuthMode::Members)
        .with_master_noise_key(master_kp.clone())
        .with_members(make_members(alice_kp.public_key(), bob_kp.public_key()))
        .with_dashboard_port(0)
        .with_auth_config(AuthConfig::new().with_token(
            "observer-token",
            "observer",
            &[AuthScope::Observer],
        ));

    let master = MasterServer::spawn(cfg.clone())
        .await
        .expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");

    let w_alice = V3TestWorker::spawn(
        master.server_addr(),
        "alice-worker",
        2,
        alice_kp.clone(),
        master_pub,
        &alice_sb,
    );
    let w_bob = V3TestWorker::spawn(
        master.server_addr(),
        "bob-worker",
        2,
        bob_kp.clone(),
        master_pub,
        &bob_sb,
    );
    wait_for_two_workers(&master).await;

    // 1) alice submits 1-core task run by bob-worker.
    let r1 = submit_v3_and_wait(
        master.server_addr(),
        &alice_kp,
        &master_pub,
        "bob-worker",
        1,
        "alice-to-bob",
    )
    .await;
    // 2) bob submits 2-core task run by alice-worker.
    let r2 = submit_v3_and_wait(
        master.server_addr(),
        &bob_kp,
        &master_pub,
        "alice-worker",
        2,
        "bob-to-alice",
    )
    .await;
    // 3) alice submits 1-core self-run task on alice-worker.
    let r3 = submit_v3_and_wait(
        master.server_addr(),
        &alice_kp,
        &master_pub,
        "alice-worker",
        1,
        "alice-self",
    )
    .await;

    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let http = reqwest::Client::new();
    let unauth = http
        .get(format!("http://{dash_addr}/api/credits"))
        .send()
        .await
        .expect("unauth request");
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let authed = http
        .get(format!("http://{dash_addr}/api/credits"))
        .bearer_auth("observer-token")
        .send()
        .await
        .expect("authed request");
    assert_eq!(authed.status(), StatusCode::OK);
    let snap: CreditsSnapshot = authed.json().await.expect("parse CreditsSnapshot");

    assert!(snap.display_only);
    assert!(!snap.window_truncated);
    assert_eq!(snap.total_records, 3);
    assert_eq!(snap.retained_records, 3);
    assert_eq!(snap.archived_records, 0);
    assert_zero_sum_all_windows(&snap);

    let by_id: HashMap<&str, _> = snap
        .participants
        .iter()
        .map(|p| (p.participant.as_str(), p))
        .collect();
    let alice = by_id[alice_id.as_str()];
    let bob = by_id[bob_id.as_str()];

    assert_eq!(alice.display_name.as_deref(), Some("alice"));
    assert_eq!(bob.display_name.as_deref(), Some("bob"));

    assert_eq!(alice.all_time.contributed.tasks, 2);
    assert_eq!(
        alice.all_time.contributed.core_ms,
        r2.execution_time_ms.saturating_mul(2) + r3.execution_time_ms
    );
    assert_eq!(alice.all_time.used.tasks, 2);
    assert_eq!(
        alice.all_time.used.core_ms,
        r1.execution_time_ms + r3.execution_time_ms
    );

    assert_eq!(bob.all_time.contributed.tasks, 1);
    assert_eq!(bob.all_time.contributed.core_ms, r1.execution_time_ms);
    assert_eq!(bob.all_time.used.tasks, 1);
    assert_eq!(
        bob.all_time.used.core_ms,
        r2.execution_time_ms.saturating_mul(2)
    );

    // Stop workers & master cleanly, then restart master on the same `--state-dir`.
    w_alice.stop().await;
    w_bob.stop().await;
    let _ = master.shutdown();
    drop(master);

    let restarted = spawn_master_with_retry(cfg).await;
    let dash_addr_2 = restarted.dashboard_addr().expect("restarted dash addr");
    let snap_after: CreditsSnapshot = http
        .get(format!("http://{dash_addr_2}/api/credits"))
        .bearer_auth("observer-token")
        .send()
        .await
        .expect("credits after restart")
        .json()
        .await
        .expect("parse after restart");

    assert_eq!(snap_after.total_records, 3);
    assert_eq!(snap_after.participants.len(), 2);
    assert_zero_sum_all_windows(&snap_after);
    let by_id_after: HashMap<&str, _> = snap_after
        .participants
        .iter()
        .map(|p| (p.participant.as_str(), p))
        .collect();
    assert_eq!(
        by_id_after[alice_id.as_str()].all_time,
        alice.all_time,
        "alice all_time totals must survive restart"
    );
    assert_eq!(
        by_id_after[bob_id.as_str()].all_time,
        bob.all_time,
        "bob all_time totals must survive restart"
    );

    let _ = restarted.shutdown();
}

#[tokio::test]
async fn test_net10_rollover_compaction_and_rejection_not_booked() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");
    let alice_sb = temp.path().join("alice-sb");
    let bob_sb = temp.path().join("bob-sb");

    let master_kp = NodeKeypair::generate().expect("master kp");
    let master_pub = *master_kp.public_key();
    let alice_kp = NodeKeypair::generate().expect("alice kp");
    let bob_kp = NodeKeypair::generate().expect("bob kp");
    let alice_id = alice_kp.public_key().member_id();
    let bob_id = bob_kp.public_key().member_id();

    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_state_dir(&state_dir)
        .with_auth_mode(AuthMode::Members)
        .with_master_noise_key(master_kp.clone())
        .with_members(make_members(alice_kp.public_key(), bob_kp.public_key()))
        .with_max_credit_records(3);

    let master = MasterServer::spawn(cfg.clone())
        .await
        .expect("spawn master");
    let w_alice = V3TestWorker::spawn(
        master.server_addr(),
        "alice-worker",
        1,
        alice_kp.clone(),
        master_pub,
        &alice_sb,
    );
    let w_bob = V3TestWorker::spawn(
        master.server_addr(),
        "bob-worker",
        1,
        bob_kp.clone(),
        master_pub,
        &bob_sb,
    );
    wait_for_two_workers(&master).await;

    let mut total_core_ms_bob_contrib = 0u64;
    let mut total_core_ms_alice_contrib = 0u64;
    for i in 0..5 {
        if i % 2 == 0 {
            let r = submit_v3_and_wait(
                master.server_addr(),
                &alice_kp,
                &master_pub,
                "bob-worker",
                1,
                &format!("roll-{i}"),
            )
            .await;
            total_core_ms_bob_contrib += r.execution_time_ms;
        } else {
            let r = submit_v3_and_wait(
                master.server_addr(),
                &bob_kp,
                &master_pub,
                "alice-worker",
                1,
                &format!("roll-{i}"),
            )
            .await;
            total_core_ms_alice_contrib += r.execution_time_ms;
        }
    }

    let snap = master.credits().snapshot();
    assert!(
        snap.retained_records <= 3,
        "retained records {} exceeded bound 3",
        snap.retained_records
    );
    assert!(
        snap.archived_records >= 2,
        "expected archived_records >= 2, got {}",
        snap.archived_records
    );
    assert_eq!(snap.total_records, 5);
    assert!(snap.window_truncated);
    assert_zero_sum_all_windows(&snap);

    w_alice.stop().await;
    w_bob.stop().await;
    let _ = master.shutdown();
    drop(master);

    let restarted = spawn_master_with_retry(cfg).await;
    let snap2 = restarted.credits().snapshot();
    assert_eq!(snap2.total_records, 5);
    assert_zero_sum_all_windows(&snap2);
    let by_id: HashMap<&str, _> = snap2
        .participants
        .iter()
        .map(|p| (p.participant.as_str(), p))
        .collect();
    assert_eq!(
        by_id[bob_id.as_str()].all_time.contributed.core_ms,
        total_core_ms_bob_contrib
    );
    assert_eq!(
        by_id[alice_id.as_str()].all_time.contributed.core_ms,
        total_core_ms_alice_contrib
    );
    let _ = restarted.shutdown();

    // Verify a worker rejection that ends the task terminally (`max_retries = 0`) never credits the
    // refusing worker.
    let rej_kp = NodeKeypair::generate().expect("rej kp");
    let rej_master_kp = NodeKeypair::generate().expect("rej master kp");
    let rej_master_pub = *rej_master_kp.public_key();
    let rej_master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_auth_mode(AuthMode::Members)
            .with_master_noise_key(rej_master_kp)
            .with_members(MemberStore::from_entries(vec![MemberEntry::new(
                "rej-member",
                *rej_kp.public_key(),
                vec![MemberRole::Worker, MemberRole::Client],
            )])),
    )
    .await
    .expect("spawn rejection master");

    let stream = TcpStream::connect(rej_master.server_addr())
        .await
        .expect("connect mock worker");
    let mut wt = MessageTransport::new(stream);
    let wsk = v3_client_handshake(&mut wt, &rej_kp, &rej_master_pub, HandshakePurpose::Member)
        .await
        .expect("worker handshake");
    wt.enable_encryption(&wsk, EndpointRole::Client);

    let worker_id = Uuid::new_v4();
    wt.send_msg(&WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities::new("mock-refuser", 1, 1024, false, false, None),
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: ProtocolCapabilities::all_supported(),
    })
    .await
    .expect("register mock worker");
    match wt.recv_msg::<MasterMessage>().await.expect("reg ack") {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("expected RegisterAck, got {other:?}"),
    }

    let task = Task::new(
        TaskSpec::command("echo", vec!["never-runs".into()]),
        TaskRequirements::generic(1, 30).with_max_retries(0),
    );
    let task_id = rej_master.submit_task(task).await.expect("submit task");

    match wt.recv_msg::<MasterMessage>().await.expect("assign") {
        Some(MasterMessage::AssignTask { task }) => assert_eq!(task.id, task_id),
        other => panic!("expected AssignTask, got {other:?}"),
    }
    wt.send_msg(&WorkerMessage::TaskResult {
        worker_id,
        task_id,
        exit_code: 1,
        stdout: Vec::new().into(),
        stderr: Vec::new().into(),
        execution_time_ms: 0,
        is_gpu_executed: false,
        device_name: None,
        error: Some("rejected by worker: capabilities insufficient".into()),
        asset_bytes: None,
        artifacts: Vec::new(),
        outputs: Vec::new(),
    })
    .await
    .expect("send rejection result");

    let final_res = rej_master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .expect("wait rejected task");
    assert_eq!(final_res.exit_code, 1);
    let info = rej_master.get_task_info(task_id).await.expect("task info");
    assert_eq!(info.state, TaskState::Failed);
    assert_eq!(
        rej_master.credits().snapshot().total_records,
        0,
        "worker rejection must never book contribution credit"
    );

    let _ = rej_master.shutdown();
}
