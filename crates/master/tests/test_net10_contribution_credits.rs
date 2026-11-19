//! NET-10 Option B integration tests: two enrolled v3 participants on loopback, exact Q96
//! `ran_for_others` == `used_from_others` conservation (with `self_run` separated), Observer vs
//! Admin redaction on `GET /api/credits`, restart persistence and fail-open corruption quarantine
//! in `--state-dir`, low-water rollover compaction, and late/stale/failover/cancel/rejection/
//! duration-clamping edge cases.

use std::collections::HashMap;
use std::fs;
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
use rusty_grid_master::credits::{CreditsSnapshot, CREDITS_JOURNAL_FILE, CREDITS_SNAPSHOT_FILE};
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

async fn wait_for_credits_total(master: &MasterHandle, expected: u64) -> CreditsSnapshot {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snap = master.credits().snapshot();
        if snap.total_records >= expected {
            return snap;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for credits total_records >= {expected}, got {}",
            snap.total_records
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
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

async fn submit_v3_async(
    addr: SocketAddr,
    client_kp: &NodeKeypair,
    master_pub: &PublicKey,
    task: Task,
) -> rusty_grid_core::task::TaskId {
    let stream = TcpStream::connect(addr).await.expect("connect v3 client");
    let mut t = MessageTransport::new(stream);
    let sk = v3_client_handshake(&mut t, client_kp, master_pub, HandshakePurpose::Member)
        .await
        .expect("v3 client handshake");
    t.enable_encryption(&sk, EndpointRole::Client);

    t.send_msg(&ClientMessage::SubmitTask { task, wait: false })
        .await
        .expect("send SubmitTask");
    match t
        .recv_msg::<ClientResponse>()
        .await
        .expect("recv ClientResponse")
    {
        Some(ClientResponse::TaskSubmitted { task_id }) => task_id,
        other => panic!("expected TaskSubmitted, got {other:?}"),
    }
}

async fn register_v3_mock_worker(
    addr: SocketAddr,
    worker_kp: &NodeKeypair,
    master_pub: &PublicKey,
    worker_id: Uuid,
    name: &str,
    features: ProtocolCapabilities,
) -> MessageTransport<TcpStream> {
    let stream = TcpStream::connect(addr).await.expect("connect mock worker");
    let mut wt = MessageTransport::new(stream);
    let wsk = v3_client_handshake(&mut wt, worker_kp, master_pub, HandshakePurpose::Member)
        .await
        .expect("worker handshake");
    wt.enable_encryption(&wsk, EndpointRole::Client);

    wt.send_msg(&WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities::new(name, 4, 2048, false, false, None),
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features,
    })
    .await
    .expect("register mock worker");
    match wt.recv_msg::<MasterMessage>().await.expect("reg ack") {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("expected RegisterAck, got {other:?}"),
    }
    wt
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
        .with_auth_config(
            AuthConfig::new()
                .with_token("observer-token", "observer", &[AuthScope::Observer])
                .with_token("admin-token", "admin", &[AuthScope::Admin]),
        );

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

    let _ = wait_for_credits_total(&master, 3).await;

    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let http = reqwest::Client::new();
    let unauth = http
        .get(format!("http://{dash_addr}/api/credits"))
        .send()
        .await
        .expect("unauth request");
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    // Observer view must NOT expose raw member_ids or machine names (design 0094 D9(c)).
    let obs_res = http
        .get(format!("http://{dash_addr}/api/credits"))
        .bearer_auth("observer-token")
        .send()
        .await
        .expect("observer request");
    assert_eq!(obs_res.status(), StatusCode::OK);
    let obs_text = obs_res.text().await.expect("observer body");
    assert!(!obs_text.contains(&alice_id), "observer leaked alice_id");
    assert!(!obs_text.contains(&bob_id), "observer leaked bob_id");
    assert!(
        !obs_text.contains("\"alice\"") && !obs_text.contains("\"bob\""),
        "observer leaked member names: {obs_text}"
    );
    let obs_snap: CreditsSnapshot = serde_json::from_str(&obs_text).expect("parse observer");
    assert!(obs_snap.display_only);
    assert!(obs_snap.conservation_holds());
    assert_eq!(obs_snap.total_records, 3);
    assert!(obs_snap
        .participants
        .iter()
        .all(|p| p.member_ids.is_empty()));

    // Admin view receives member names and `member_ids`.
    let adm_res = http
        .get(format!("http://{dash_addr}/api/credits"))
        .bearer_auth("admin-token")
        .send()
        .await
        .expect("admin request");
    assert_eq!(adm_res.status(), StatusCode::OK);
    let snap: CreditsSnapshot = adm_res.json().await.expect("parse CreditsSnapshot");
    assert!(snap.display_only);
    assert!(!snap.window_truncated);
    assert_eq!(snap.total_records, 3);
    assert_eq!(snap.retained_records, 3);
    assert_eq!(snap.archived_records, 0);
    assert!(snap.conservation_holds());

    let by_label: HashMap<&str, _> = snap
        .participants
        .iter()
        .map(|p| (p.label.as_str(), p))
        .collect();
    let alice = by_label["alice"];
    let bob = by_label["bob"];
    assert_eq!(alice.member_ids, vec![alice_id.clone()]);
    assert_eq!(bob.member_ids, vec![bob_id.clone()]);

    assert_eq!(alice.all_time.ran_for_others.tasks, 1);
    assert_eq!(
        alice.all_time.ran_for_others.core_ms,
        u128::from(r2.execution_time_ms) * 2
    );
    assert_eq!(alice.all_time.used_from_others.tasks, 1);
    assert_eq!(
        alice.all_time.used_from_others.core_ms,
        u128::from(r1.execution_time_ms)
    );
    assert_eq!(alice.all_time.self_run.tasks, 1);
    assert_eq!(
        alice.all_time.self_run.core_ms,
        u128::from(r3.execution_time_ms)
    );

    assert_eq!(bob.all_time.ran_for_others.tasks, 1);
    assert_eq!(
        bob.all_time.ran_for_others.core_ms,
        u128::from(r1.execution_time_ms)
    );
    assert_eq!(bob.all_time.used_from_others.tasks, 1);
    assert_eq!(
        bob.all_time.used_from_others.core_ms,
        u128::from(r2.execution_time_ms) * 2
    );
    assert_eq!(bob.all_time.self_run.tasks, 0);

    w_alice.stop().await;
    w_bob.stop().await;
    let _ = master.shutdown();
    drop(master);

    let restarted = spawn_master_with_retry(cfg).await;
    let dash_addr_2 = restarted.dashboard_addr().expect("restarted dash addr");
    let snap_after: CreditsSnapshot = http
        .get(format!("http://{dash_addr_2}/api/credits"))
        .bearer_auth("admin-token")
        .send()
        .await
        .expect("credits after restart")
        .json()
        .await
        .expect("parse after restart");

    assert_eq!(snap_after.total_records, 3);
    assert_eq!(snap_after.participants.len(), 2);
    assert!(snap_after.conservation_holds());
    let by_label_after: HashMap<&str, _> = snap_after
        .participants
        .iter()
        .map(|p| (p.label.as_str(), p))
        .collect();
    assert_eq!(
        by_label_after["alice"].all_time, alice.all_time,
        "alice all_time totals must survive restart"
    );
    assert_eq!(
        by_label_after["bob"].all_time, bob.all_time,
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
        .with_max_credit_records(4);

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

    let mut total_core_ms_bob_contrib = 0u128;
    let mut total_core_ms_alice_contrib = 0u128;
    for i in 0..8 {
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
            total_core_ms_bob_contrib += u128::from(r.execution_time_ms);
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
            total_core_ms_alice_contrib += u128::from(r.execution_time_ms);
        }
    }

    let _ = wait_for_credits_total(&master, 8).await;
    master
        .credits()
        .flush_async()
        .await
        .expect("flush credits writer before inspecting compaction bounds");
    let snap = master.credits().snapshot();
    assert!(snap.retained_records <= 4);
    assert!(snap.archived_records >= 4);
    assert!(snap.window_truncated);
    assert!(snap.conservation_holds());
    assert!(
        master.credits().compaction_count() <= 4,
        "low-water compaction should trigger sparingly, got {}",
        master.credits().compaction_count()
    );

    w_alice.stop().await;
    w_bob.stop().await;
    let _ = master.shutdown();
    drop(master);

    let restarted = spawn_master_with_retry(cfg).await;
    let snap2 = restarted.credits().snapshot();
    assert_eq!(snap2.total_records, 8);
    assert!(snap2.conservation_holds());
    let by_label: HashMap<&str, _> = snap2
        .participants
        .iter()
        .map(|p| (p.label.as_str(), p))
        .collect();
    assert_eq!(
        by_label[bob_id.as_str()].all_time.ran_for_others.core_ms,
        total_core_ms_bob_contrib
    );
    assert_eq!(
        by_label[alice_id.as_str()].all_time.ran_for_others.core_ms,
        total_core_ms_alice_contrib
    );
    let _ = restarted.shutdown();

    // Verify a worker rejection that ends the task terminally (`max_retries = 0`) never credits.
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

    let worker_id = Uuid::new_v4();
    let mut wt = register_v3_mock_worker(
        rej_master.server_addr(),
        &rej_kp,
        &rej_master_pub,
        worker_id,
        "mock-refuser",
        ProtocolCapabilities::all_supported(),
    )
    .await;

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
    let _ = rej_master.credits().flush_async().await;
    assert_eq!(rej_master.credits().snapshot().total_records, 0);

    let _ = rej_master.shutdown();
}

#[tokio::test]
async fn test_net10_late_stale_failover_cancel_and_clamping() {
    let master_kp = NodeKeypair::generate().expect("master kp");
    let master_pub = *master_kp.public_key();
    let alice_kp = NodeKeypair::generate().expect("alice kp");
    let bob_kp = NodeKeypair::generate().expect("bob kp");
    let alice_id = alice_kp.public_key().member_id();
    let bob_id = bob_kp.public_key().member_id();

    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_auth_mode(AuthMode::Members)
            .with_master_noise_key(master_kp)
            .with_members(make_members(alice_kp.public_key(), bob_kp.public_key())),
    )
    .await
    .expect("spawn master");

    // 1) Failover: Worker 1 (bob) receives task 1 submitted by alice, disconnects without result.
    let w1_id = Uuid::new_v4();
    let mut wt1 = register_v3_mock_worker(
        master.server_addr(),
        &bob_kp,
        &master_pub,
        w1_id,
        "w1-bob",
        ProtocolCapabilities::empty(),
    )
    .await;

    let t1 = Task::new(
        TaskSpec::command("echo", vec!["failover".into()]),
        TaskRequirements::generic(2, 30),
    );
    let t1_id = submit_v3_async(master.server_addr(), &alice_kp, &master_pub, t1).await;
    match wt1.recv_msg::<MasterMessage>().await.expect("assign t1") {
        Some(MasterMessage::AssignTask { task }) => assert_eq!(task.id, t1_id),
        other => panic!("expected AssignTask t1, got {other:?}"),
    }
    drop(wt1);

    // Worker 2 (alice) connects and completes failed-over t1 cleanly.
    let w2_id = Uuid::new_v4();
    let mut wt2 = register_v3_mock_worker(
        master.server_addr(),
        &alice_kp,
        &master_pub,
        w2_id,
        "w2-alice",
        ProtocolCapabilities::empty(),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            wt2.recv_msg::<MasterMessage>(),
        )
        .await
        {
            Ok(Ok(Some(MasterMessage::AssignTask { task }))) => {
                assert_eq!(task.id, t1_id);
                break;
            }
            Ok(Ok(Some(_))) => continue,
            other => panic!("timed out/failed waiting for failover AssignTask: {other:?}"),
        }
    }
    wt2.send_msg(&WorkerMessage::TaskResult {
        worker_id: w2_id,
        task_id: t1_id,
        exit_code: 0,
        stdout: Vec::new().into(),
        stderr: Vec::new().into(),
        execution_time_ms: 250,
        is_gpu_executed: false,
        device_name: None,
        error: None,
        asset_bytes: None,
        artifacts: Vec::new(),
        outputs: Vec::new(),
    })
    .await
    .expect("send failover result");
    let _ = master
        .wait_task(t1_id, Some(Duration::from_secs(5)))
        .await
        .expect("t1 complete");

    let snap_after_failover = wait_for_credits_total(&master, 1).await;
    assert_eq!(snap_after_failover.total_records, 1);
    assert!(snap_after_failover.conservation_holds());
    let by_label: HashMap<&str, _> = snap_after_failover
        .participants
        .iter()
        .map(|p| (p.label.as_str(), p))
        .collect();
    // Both runner (w2-alice) and submitter (alice) are alice -> self_run only; w1 (bob) got 0!
    assert_eq!(by_label[alice_id.as_str()].all_time.self_run.core_ms, 500);
    assert!(!by_label.contains_key(bob_id.as_str()));

    // 2) Stale/late duplicate results must never add extra credit records.
    wt2.send_msg(&WorkerMessage::TaskResult {
        worker_id: w2_id,
        task_id: t1_id,
        exit_code: 0,
        stdout: Vec::new().into(),
        stderr: Vec::new().into(),
        execution_time_ms: 999,
        is_gpu_executed: false,
        device_name: None,
        error: None,
        asset_bytes: None,
        artifacts: Vec::new(),
        outputs: Vec::new(),
    })
    .await
    .expect("send late duplicate result");
    wt2.send_msg(&WorkerMessage::TaskResult {
        worker_id: w1_id,
        task_id: t1_id,
        exit_code: 0,
        stdout: Vec::new().into(),
        stderr: Vec::new().into(),
        execution_time_ms: 999,
        is_gpu_executed: false,
        device_name: None,
        error: None,
        asset_bytes: None,
        artifacts: Vec::new(),
        outputs: Vec::new(),
    })
    .await
    .expect("send stale worker result");

    // 3) Cancelled queued task and cancelled running task (even if worker sends late result)
    // must never book credit.
    let t_queued = Task::new(
        TaskSpec::command("echo", vec!["cancel-q".into()]),
        TaskRequirements::generic(1, 30),
    )
    .with_tags(vec!["require:worker=missing-worker".into()]);
    let t_queued_id = submit_v3_async(master.server_addr(), &bob_kp, &master_pub, t_queued).await;
    master
        .cancel_task(t_queued_id)
        .await
        .expect("cancel queued");

    let t_running = Task::new(
        TaskSpec::command("echo", vec!["cancel-run".into()]),
        TaskRequirements::generic(1, 30),
    );
    let t_running_id = submit_v3_async(master.server_addr(), &bob_kp, &master_pub, t_running).await;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), wt2.recv_msg::<MasterMessage>()).await {
            Ok(Ok(Some(MasterMessage::AssignTask { task }))) => {
                assert_eq!(task.id, t_running_id);
                break;
            }
            Ok(Ok(Some(_))) => continue,
            other => panic!("failed receiving AssignTask for t_running: {other:?}"),
        }
    }
    master
        .cancel_task(t_running_id)
        .await
        .expect("cancel running");
    wt2.send_msg(&WorkerMessage::TaskResult {
        worker_id: w2_id,
        task_id: t_running_id,
        exit_code: 0,
        stdout: Vec::new().into(),
        stderr: Vec::new().into(),
        execution_time_ms: 400,
        is_gpu_executed: false,
        device_name: None,
        error: None,
        asset_bytes: None,
        artifacts: Vec::new(),
        outputs: Vec::new(),
    })
    .await
    .expect("send late result after cancel");

    // 4) Non-GPU task with inflated worker execution_time_ms + is_gpu_executed=true gets clamped
    // and receives 0 GPU-ms.
    let t_clamp = Task::new(
        TaskSpec::command("echo", vec!["clamp".into()]),
        TaskRequirements::generic(2, 5),
    );
    let t_clamp_id = submit_v3_async(master.server_addr(), &bob_kp, &master_pub, t_clamp).await;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), wt2.recv_msg::<MasterMessage>()).await {
            Ok(Ok(Some(MasterMessage::AssignTask { task }))) => {
                assert_eq!(task.id, t_clamp_id);
                break;
            }
            Ok(Ok(Some(_))) => continue,
            other => panic!("failed receiving AssignTask for t_clamp: {other:?}"),
        }
    }
    wt2.send_msg(&WorkerMessage::TaskResult {
        worker_id: w2_id,
        task_id: t_clamp_id,
        exit_code: 0,
        stdout: Vec::new().into(),
        stderr: Vec::new().into(),
        execution_time_ms: 10_000_000_000,
        is_gpu_executed: true,
        device_name: Some("fake-gpu".into()),
        error: None,
        asset_bytes: None,
        artifacts: Vec::new(),
        outputs: Vec::new(),
    })
    .await
    .expect("send inflated result");
    let _ = master
        .wait_task(t_clamp_id, Some(Duration::from_secs(5)))
        .await
        .expect("t_clamp complete");

    let final_snap = wait_for_credits_total(&master, 2).await;
    assert_eq!(final_snap.total_records, 2);
    assert!(final_snap.conservation_holds());
    let final_by_label: HashMap<&str, _> = final_snap
        .participants
        .iter()
        .map(|p| (p.label.as_str(), p))
        .collect();
    let alice_row = final_by_label[alice_id.as_str()];
    assert_eq!(alice_row.all_time.ran_for_others.tasks, 1);
    assert_eq!(alice_row.all_time.ran_for_others.gpu_ms, 0);
    assert!(
        alice_row.all_time.ran_for_others.core_ms <= 10_000,
        "clamped core_ms ({}) must not exceed timeout*cores (10000)",
        alice_row.all_time.ran_for_others.core_ms
    );
    let bob_row = final_by_label[bob_id.as_str()];
    assert_eq!(bob_row.all_time.used_from_others.tasks, 1);
    assert_eq!(
        bob_row.all_time.used_from_others.core_ms,
        alice_row.all_time.ran_for_others.core_ms
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_net10_corrupt_credits_files_do_not_stop_master_startup() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");
    fs::create_dir_all(&state_dir).expect("mkdir state");
    fs::write(state_dir.join(CREDITS_SNAPSHOT_FILE), b"{corrupt-snapshot").expect("corrupt snap");
    fs::write(
        state_dir.join(CREDITS_JOURNAL_FILE),
        b"{\"seq\":1,\"blake3\":\"deadbeef\",\"payload\":{\"corrupt\":1}}\n{\"seq\":2,\"blake3\":\"deadbeef\",\"payload\":{\"ok\":2}}\n",
    )
    .expect("corrupt jrn");

    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_state_dir(&state_dir),
    )
    .await
    .expect("master must still start cleanly with corrupt credits files");
    assert_eq!(master.credits().snapshot().total_records, 0);
    let _ = master.shutdown();

    let quarantined: Vec<String> = fs::read_dir(&state_dir)
        .expect("readdir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".corrupt."))
        .collect();
    assert!(
        !quarantined.is_empty(),
        "corrupt credit files should be moved aside"
    );
}
