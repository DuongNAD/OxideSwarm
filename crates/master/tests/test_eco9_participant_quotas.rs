//! End-to-end loopback tests for ECO-9 (per-participant quotas, participant fair share, priority
//! bands, persistence) and ENROLL-1c step 2 (`RevokeMember` cancels active submitted tasks).

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use uuid::Uuid;

use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
use rusty_grid_core::noise_auth::{
    v3_client_handshake, HandshakePurpose, MemberEntry, MemberRole, NodeKeypair, PublicKey,
};
use rusty_grid_core::placement::VERIFY_REPLICAS_TAG_PREFIX;
use rusty_grid_core::protocol::{
    ClientMessage, ClientResponse, EndpointRole, MasterMessage, ProtocolCapabilities,
    WorkerMessage, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::{Bytes, Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_core::{GridError, MessageTransport, WorkerCapabilities};
use rusty_grid_master::auth::{AuthConfig, AuthScope};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::quotas::{ParticipantQuota, PriorityBand, QuotaPolicyConfig};
use rusty_grid_master::server::{
    AuthMode, MasterHandle, MasterServer, MemberStore, ServerConfig, MEMBERS_FILE_NAME,
};

fn echo_task(label: &str, cores: usize) -> Task {
    Task::new(
        TaskSpec::command("echo", vec![label.to_string()]),
        TaskRequirements::generic(cores, 32),
    )
}

async fn connect_v3(
    addr: SocketAddr,
    kp: &NodeKeypair,
    master_pub: &PublicKey,
) -> MessageTransport<TcpStream> {
    let stream = TcpStream::connect(addr).await.expect("connect master");
    let mut t = MessageTransport::new(stream);
    let sk = v3_client_handshake(&mut t, kp, master_pub, HandshakePurpose::Member)
        .await
        .expect("v3 handshake");
    t.enable_encryption(&sk, EndpointRole::Client);
    t
}

async fn submit_v3(t: &mut MessageTransport<TcpStream>, task: Task) -> Result<TaskId, String> {
    t.send_msg(&ClientMessage::SubmitTask { task, wait: false })
        .await
        .expect("send SubmitTask");
    match t
        .recv_msg::<ClientResponse>()
        .await
        .expect("recv ClientResponse")
    {
        Some(ClientResponse::TaskSubmitted { task_id }) => Ok(task_id),
        Some(ClientResponse::Error { message }) => Err(message),
        other => panic!("unexpected SubmitTask response: {other:?}"),
    }
}

async fn register_fake_worker(
    addr: SocketAddr,
    name: &str,
    cores: usize,
    v3: Option<(&NodeKeypair, &PublicKey)>,
) -> (Uuid, MessageTransport<TcpStream>) {
    let mut t = match v3 {
        Some((kp, master_pub)) => connect_v3(addr, kp, master_pub).await,
        None => MessageTransport::new(TcpStream::connect(addr).await.expect("connect worker")),
    };
    let worker_id = Uuid::new_v4();
    t.send_msg(&WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities::new(name, cores, 16_384, false, false, None),
        protocol_version: PROTOCOL_VERSION_CURRENT,
        features: ProtocolCapabilities::all_supported(),
    })
    .await
    .expect("send Register");
    match tokio::time::timeout(Duration::from_secs(30), t.recv_msg::<MasterMessage>())
        .await
        .expect("RegisterAck timeout")
        .expect("RegisterAck io")
    {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("expected accepted RegisterAck, got {other:?}"),
    }
    (worker_id, t)
}

async fn recv_next_assignment(worker: &mut MessageTransport<TcpStream>, timeout: Duration) -> Task {
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "timed out waiting for AssignTask");
        match tokio::time::timeout(left, worker.recv_msg::<MasterMessage>())
            .await
            .expect("AssignTask timeout")
            .expect("AssignTask io")
        {
            Some(MasterMessage::AssignTask { task }) => return task,
            Some(MasterMessage::HeartbeatAck { .. }) => continue,
            Some(MasterMessage::CancelTask {
                reason: Some(ref r),
                ..
            }) if r.starts_with("oxs:result-ack:") => continue,
            other => panic!("unexpected worker message while awaiting AssignTask: {other:?}"),
        }
    }
}

async fn send_task_success(
    worker: &mut MessageTransport<TcpStream>,
    worker_id: Uuid,
    task_id: TaskId,
) {
    worker
        .send_msg(&WorkerMessage::TaskResult {
            worker_id,
            task_id,
            exit_code: 0,
            stdout: Bytes::from_static(b"ok"),
            stderr: Bytes::new(),
            execution_time_ms: 1,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: vec![],
            outputs: vec![],
        })
        .await
        .expect("send TaskResult");
}

async fn wait_task_state(master: &MasterHandle, id: TaskId, want: TaskState, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if master
            .get_task_info(id)
            .await
            .is_ok_and(|i| i.state == want)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "task {id} did not reach state {want:?} within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

async fn wait_task_queued(master: &MasterHandle, id: TaskId, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if master.get_task_info(id).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "task {id} did not appear in queue within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn http_client() -> reqwest::Client {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::new()
}

#[tokio::test]
async fn running_cap_bounds_concurrency_reports_wait_reason_and_never_starves_others() {
    let ka = NodeKeypair::generate().unwrap();
    let kb = NodeKeypair::generate().unwrap();
    let kw = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![
        MemberEntry::new("alice-node", *ka.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("bob-node", *kb.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("worker-node", *kw.public_key(), vec![MemberRole::Worker]),
    ]);
    members
        .set_member_participant(&ka.member_id(), Some("alice"))
        .unwrap();
    members
        .set_member_participant(&kb.member_id(), Some("bob"))
        .unwrap();

    let mut overrides = BTreeMap::new();
    overrides.insert(
        "alice".to_string(),
        ParticipantQuota {
            max_running_tasks: Some(2),
            ..Default::default()
        },
    );
    let auth_cfg = AuthConfig::new().with_token("adm", "admin", &[AuthScope::Admin]);
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_auth_mode(AuthMode::Both)
        .with_members(members)
        .with_dashboard_port(0)
        .with_auth_config(auth_cfg)
        .with_quota_policy(QuotaPolicyConfig {
            default_quota: ParticipantQuota::default(),
            participant_overrides: overrides,
        });

    let master = MasterServer::spawn(cfg).await.unwrap();
    let master_pub = master.master_noise_public_key().unwrap();
    let addr = master.server_addr();
    let dash = format!("http://{}", master.dashboard_addr().unwrap());

    let mut ca = connect_v3(addr, &ka, &master_pub).await;
    let mut cb = connect_v3(addr, &kb, &master_pub).await;

    let mut a_ids = HashSet::new();
    for i in 0..6 {
        let tid = submit_v3(&mut ca, echo_task(&format!("a-{i}"), 1))
            .await
            .unwrap();
        a_ids.insert(tid);
    }
    let mut b_ids = HashSet::new();
    for i in 0..2 {
        let tid = submit_v3(&mut cb, echo_task(&format!("b-{i}"), 1))
            .await
            .unwrap();
        b_ids.insert(tid);
    }

    let (wid, mut worker) = register_fake_worker(addr, "w-6", 6, Some((&kw, &master_pub))).await;
    let mut held_a = Vec::new();
    let mut held_b = Vec::new();
    // All 4 runnable tasks (2 from capped alice, 2 from uncapped bob) must arrive even while
    // 4 of alice's tasks remain queued behind the cap.
    for _ in 0..4 {
        let t = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
        if a_ids.contains(&t.id) {
            held_a.push(t.id);
        } else if b_ids.contains(&t.id) {
            held_b.push(t.id);
        } else {
            panic!("unexpected task {}", t.id);
        }
    }
    assert_eq!(held_a.len(), 2, "alice concurrency must be capped at 2");
    assert_eq!(held_b.len(), 2, "uncapped bob must not starve behind alice");

    // Inspect /api/quotas for alice's live wait_reason while held_a is active.
    let http = http_client();
    let quotas: serde_json::Value = http
        .get(format!("{dash}/api/quotas"))
        .bearer_auth("adm")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let alice_row = quotas["participants"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["participant"] == "alice")
        .expect("alice quota row");
    assert_eq!(alice_row["usage"]["running_tasks"], 2);
    assert_eq!(alice_row["usage"]["queued_tasks"], 4);
    assert!(
        alice_row["wait_reason"]
            .as_str()
            .is_some_and(|s| s.contains("2/2")),
        "expected running wait reason, got: {alice_row}"
    );

    // Finish bob and drain alice's 6 tasks cleanly while verifying concurrency <= 2.
    for tid in held_b {
        send_task_success(&mut worker, wid, tid).await;
    }
    let mut completed_a = 0usize;
    while completed_a < 6 {
        let done = held_a.remove(0);
        send_task_success(&mut worker, wid, done).await;
        completed_a += 1;
        wait_task_state(&master, done, TaskState::Completed, Duration::from_secs(30)).await;
        if completed_a + held_a.len() < 6 {
            let next = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
            assert!(a_ids.contains(&next.id));
            held_a.push(next.id);
            assert!(held_a.len() <= 2);
        }
    }
    let _ = master.shutdown();
}

#[tokio::test]
async fn slot_cap_bounds_concurrent_cores_and_rejects_oversized_task() {
    let ka = NodeKeypair::generate().unwrap();
    let kb = NodeKeypair::generate().unwrap();
    let kw = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![
        MemberEntry::new("slot-a", *ka.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("ctrl-b", *kb.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("worker-slot", *kw.public_key(), vec![MemberRole::Worker]),
    ]);
    members
        .set_member_participant(&ka.member_id(), Some("alice"))
        .unwrap();
    members
        .set_member_participant(&kb.member_id(), Some("bob"))
        .unwrap();

    let mut overrides = BTreeMap::new();
    overrides.insert(
        "alice".to_string(),
        ParticipantQuota {
            max_slots: Some(4),
            ..Default::default()
        },
    );
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_auth_mode(AuthMode::Both)
        .with_members(members)
        .with_quota_policy(QuotaPolicyConfig {
            default_quota: ParticipantQuota::default(),
            participant_overrides: overrides,
        });

    let master = MasterServer::spawn(cfg).await.unwrap();
    let master_pub = master.master_noise_public_key().unwrap();
    let addr = master.server_addr();

    let mut ca = connect_v3(addr, &ka, &master_pub).await;
    let mut cb = connect_v3(addr, &kb, &master_pub).await;

    // 5-core task exceeds alice's max_slots=4 and must be refused immediately at submission.
    let err = submit_v3(&mut ca, echo_task("too-wide", 5))
        .await
        .expect_err("5-core task must fail max_slots=4");
    assert!(
        err.contains("max_slots") || err.contains("Quota"),
        "unexpected error: {err}"
    );

    // Three 2-core tasks from alice (total 6 slots) + one 1-core control task from bob.
    let mut a_ids = Vec::new();
    for i in 0..3 {
        a_ids.push(
            submit_v3(&mut ca, echo_task(&format!("a2-{i}"), 2))
                .await
                .unwrap(),
        );
    }
    let b_id = submit_v3(&mut cb, echo_task("b1", 1)).await.unwrap();

    let (wid, mut worker) =
        register_fake_worker(addr, "w-slot-6", 6, Some((&kw, &master_pub))).await;
    let mut first_wave = Vec::new();
    for _ in 0..3 {
        first_wave.push(recv_next_assignment(&mut worker, Duration::from_secs(30)).await);
    }
    let wave_a: Vec<TaskId> = first_wave
        .iter()
        .filter(|t| a_ids.contains(&t.id))
        .map(|t| t.id)
        .collect();
    assert_eq!(wave_a.len(), 2, "at most 2x2=4 slots from alice may run");
    assert!(
        first_wave.iter().any(|t| t.id == b_id),
        "bob's 1-core control task must run while alice's 3rd 2-core task waits"
    );

    // Finish one of alice's 2-core tasks -> 3rd 2-core task immediately starts.
    send_task_success(&mut worker, wid, wave_a[0]).await;
    let third = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    assert!(a_ids.contains(&third.id));
    send_task_success(&mut worker, wid, wave_a[1]).await;
    send_task_success(&mut worker, wid, b_id).await;
    send_task_success(&mut worker, wid, third.id).await;
    wait_task_state(
        &master,
        third.id,
        TaskState::Completed,
        Duration::from_secs(30),
    )
    .await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn fair_share_one_connection_vs_twenty_connections_splits_equally() {
    let ka = NodeKeypair::generate().unwrap();
    let kb = NodeKeypair::generate().unwrap();
    let kw = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![
        MemberEntry::new("one-conn", *ka.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("many-conns", *kb.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("worker-fair", *kw.public_key(), vec![MemberRole::Worker]),
    ]);
    members
        .set_member_participant(&ka.member_id(), Some("alpha"))
        .unwrap();
    members
        .set_member_participant(&kb.member_id(), Some("beta"))
        .unwrap();

    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_auth_mode(AuthMode::Both)
        .with_members(members);
    let master = MasterServer::spawn(cfg).await.unwrap();
    let master_pub = master.master_noise_public_key().unwrap();
    let addr = master.server_addr();

    let mut ca = connect_v3(addr, &ka, &master_pub).await;
    let mut a_ids = HashSet::new();
    for i in 0..8 {
        a_ids.insert(
            submit_v3(&mut ca, echo_task(&format!("alpha-{i}"), 1))
                .await
                .unwrap(),
        );
    }

    let mut b_ids = HashSet::new();
    for i in 0..20 {
        let mut cb = connect_v3(addr, &kb, &master_pub).await;
        b_ids.insert(
            submit_v3(&mut cb, echo_task(&format!("beta-{i}"), 1))
                .await
                .unwrap(),
        );
    }

    // 4-core worker takes 4 tasks in its first wave -> must be 2 alpha + 2 beta (1:1).
    let (wid, mut worker) =
        register_fake_worker(addr, "w-fair-4", 4, Some((&kw, &master_pub))).await;
    let mut got_a = 0usize;
    let mut got_b = 0usize;
    let mut assigned = Vec::new();
    for _ in 0..4 {
        let t = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
        if a_ids.contains(&t.id) {
            got_a += 1;
        } else if b_ids.contains(&t.id) {
            got_b += 1;
        } else {
            panic!("unexpected task {}", t.id);
        }
        assigned.push(t.id);
    }
    assert_eq!(
        (got_a, got_b),
        (2, 2),
        "1-connection alpha vs 20-connection beta must split 4 slots 2:2"
    );
    for id in assigned {
        send_task_success(&mut worker, wid, id).await;
    }
    let _ = master.shutdown();
}

#[tokio::test]
async fn multi_machine_grouping_and_live_relabeling() {
    let a1 = NodeKeypair::generate().unwrap();
    let a2 = NodeKeypair::generate().unwrap();
    let b_ctrl = NodeKeypair::generate().unwrap();
    let kw = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![
        MemberEntry::new("alice-laptop", *a1.public_key(), vec![MemberRole::Client]),
        MemberEntry::new(
            "alice-workstation",
            *a2.public_key(),
            vec![MemberRole::Client],
        ),
        MemberEntry::new("bob-ctrl", *b_ctrl.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("worker-group", *kw.public_key(), vec![MemberRole::Worker]),
    ]);
    let auth_cfg = AuthConfig::new().with_token("adm", "admin", &[AuthScope::Admin]);

    let mut overrides = BTreeMap::new();
    overrides.insert(
        "team-alice".to_string(),
        ParticipantQuota {
            max_running_tasks: Some(2),
            ..Default::default()
        },
    );
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_auth_mode(AuthMode::Both)
        .with_members(members)
        .with_dashboard_port(0)
        .with_auth_config(auth_cfg)
        .with_quota_policy(QuotaPolicyConfig {
            default_quota: ParticipantQuota::default(),
            participant_overrides: overrides,
        });

    let master = MasterServer::spawn(cfg).await.unwrap();
    let master_pub = master.master_noise_public_key().unwrap();
    let addr = master.server_addr();
    let dash = format!("http://{}", master.dashboard_addr().unwrap());
    let http = http_client();

    // Assign both `alice-laptop` and `alice-workstation` to `"team-alice"` via HTTP Admin API.
    for mid in [a1.member_id(), a2.member_id()] {
        let r = http
            .post(format!("{dash}/api/members/{mid}/participant"))
            .bearer_auth("adm")
            .json(&serde_json::json!({"participant": "team-alice"}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), reqwest::StatusCode::OK);
    }

    let mut c1 = connect_v3(addr, &a1, &master_pub).await;
    let mut c2 = connect_v3(addr, &a2, &master_pub).await;
    let mut cb = connect_v3(addr, &b_ctrl, &master_pub).await;

    let t1 = submit_v3(&mut c1, echo_task("m1-1", 1)).await.unwrap();
    let t2 = submit_v3(&mut c1, echo_task("m1-2", 1)).await.unwrap();
    let t3 = submit_v3(&mut c2, echo_task("m2-held", 1)).await.unwrap();
    let tb = submit_v3(&mut cb, echo_task("b-ctrl", 1)).await.unwrap();

    // Worker (6 cores) gets t1, t2 (team-alice at 2/2) + tb; t3 stays queued under team-alice!
    let (wid, mut worker) =
        register_fake_worker(addr, "w-group-6", 6, Some((&kw, &master_pub))).await;
    let mut first_three = HashSet::new();
    for _ in 0..3 {
        first_three.insert(
            recv_next_assignment(&mut worker, Duration::from_secs(30))
                .await
                .id,
        );
    }
    assert!(first_three.contains(&t1) && first_three.contains(&t2) && first_three.contains(&tb));
    assert!(
        !first_three.contains(&t3),
        "a2's task shares team-alice cap of 2"
    );

    // Re-label a2 to `"team-bob"` live: `t3` must immediately become eligible and dispatch!
    let relabel = http
        .post(format!("{dash}/api/members/{}/participant", a2.member_id()))
        .bearer_auth("adm")
        .json(&serde_json::json!({"participant": "team-bob"}))
        .send()
        .await
        .unwrap();
    assert_eq!(relabel.status(), reqwest::StatusCode::OK);

    let rekeyed = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    assert_eq!(
        rekeyed.id, t3,
        "live re-labeling must dispatch queued task t3"
    );

    for id in [t1, t2, tb, t3] {
        send_task_success(&mut worker, wid, id).await;
    }
    wait_task_state(&master, t3, TaskState::Completed, Duration::from_secs(30)).await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn queued_cap_and_priority_bands_without_starvation() {
    let kh = NodeKeypair::generate().unwrap();
    let kn = NodeKeypair::generate().unwrap();
    let kl = NodeKeypair::generate().unwrap();
    let kw = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![
        MemberEntry::new("high-cli", *kh.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("norm-cli", *kn.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("low-cli", *kl.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("worker-prio", *kw.public_key(), vec![MemberRole::Worker]),
    ]);
    members
        .set_member_participant(&kh.member_id(), Some("p-high"))
        .unwrap();
    members
        .set_member_participant(&kn.member_id(), Some("p-norm"))
        .unwrap();
    members
        .set_member_participant(&kl.member_id(), Some("p-low"))
        .unwrap();

    let mut overrides = BTreeMap::new();
    overrides.insert(
        "p-high".to_string(),
        ParticipantQuota {
            max_running_tasks: Some(1),
            max_queued_tasks: Some(2),
            priority_band: Some(PriorityBand::High),
            ..Default::default()
        },
    );
    overrides.insert(
        "p-norm".to_string(),
        ParticipantQuota {
            priority_band: Some(PriorityBand::Normal),
            ..Default::default()
        },
    );
    overrides.insert(
        "p-low".to_string(),
        ParticipantQuota {
            priority_band: Some(PriorityBand::Low),
            ..Default::default()
        },
    );
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_auth_mode(AuthMode::Both)
        .with_members(members)
        .with_quota_policy(QuotaPolicyConfig {
            default_quota: ParticipantQuota::default(),
            participant_overrides: overrides,
        });

    let master = MasterServer::spawn(cfg).await.unwrap();
    let master_pub = master.master_noise_public_key().unwrap();
    let addr = master.server_addr();

    let mut ch = connect_v3(addr, &kh, &master_pub).await;
    let mut cn = connect_v3(addr, &kn, &master_pub).await;
    let mut cl = connect_v3(addr, &kl, &master_pub).await;

    // Submit Low first, then Normal, then High (testing priority ordering High > Normal > Low).
    let l1 = submit_v3(&mut cl, echo_task("low-1", 1)).await.unwrap();
    let n1 = submit_v3(&mut cn, echo_task("norm-1", 1)).await.unwrap();
    let h1 = submit_v3(&mut ch, echo_task("high-1", 1)).await.unwrap();
    let h2 = submit_v3(&mut ch, echo_task("high-2", 1)).await.unwrap();

    // 3rd queued task for `p-high` exceeds max_queued_tasks=2 and must be rejected.
    let err = submit_v3(&mut ch, echo_task("high-3", 1))
        .await
        .expect_err("3rd queued task exceeds max_queued_tasks=2");
    assert!(
        err.contains("max_queued_tasks") || err.contains("Quota"),
        "unexpected error: {err}"
    );

    let (wid, mut worker) =
        register_fake_worker(addr, "w-prio-4", 4, Some((&kw, &master_pub))).await;
    // Order of first 3 assignments must be [h1 (High), n1 (Normal), l1 (Low)] while h2 waits for
    // `p-high` max_running_tasks=1.
    let t_first = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    let t_second = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    let t_third = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    assert_eq!(t_first.id, h1, "High band must dispatch first");
    assert_eq!(t_second.id, n1, "Normal band must dispatch second");
    assert_eq!(
        t_third.id, l1,
        "Low band must dispatch without starvation while High is running-capped"
    );

    send_task_success(&mut worker, wid, h1).await;
    let t_fourth = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    assert_eq!(t_fourth.id, h2);
    for id in [n1, l1, h2] {
        send_task_success(&mut worker, wid, id).await;
    }
    wait_task_state(&master, h2, TaskState::Completed, Duration::from_secs(30)).await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn state_dir_persistence_preserves_labels_quotas_and_submitted_by_member() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&state_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    let ka = NodeKeypair::generate().unwrap();
    let kb = NodeKeypair::generate().unwrap();
    let kw = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![
        MemberEntry::new("alice-persist", *ka.public_key(), vec![MemberRole::Client]),
        MemberEntry::new(
            "bob-ctrl-persist",
            *kb.public_key(),
            vec![MemberRole::Client],
        ),
        MemberEntry::new("worker-persist", *kw.public_key(), vec![MemberRole::Worker]),
    ])
    .with_save_path(state_dir.join(MEMBERS_FILE_NAME));
    // Trigger initial write of members.json so Restart 2 loads the enrolled keys.
    members
        .set_member_participant(&ka.member_id(), Some("initial-tag"))
        .unwrap();

    let auth_cfg = AuthConfig::new().with_token("adm", "admin", &[AuthScope::Admin]);

    let (t1, t2, t3, master_pub) = {
        let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_state_dir(&state_dir)
            .with_auth_mode(AuthMode::Members)
            .with_members(members)
            .with_dashboard_port(0)
            .with_auth_config(auth_cfg.clone());
        let master = MasterServer::spawn(cfg).await.unwrap();
        let dash = format!("http://{}", master.dashboard_addr().unwrap());
        let master_pub = master.master_noise_public_key().unwrap();
        let http = http_client();

        // Admin sets participant label and quota override via HTTP (persists to members.json).
        assert_eq!(
            http.post(format!("{dash}/api/members/{}/participant", ka.member_id()))
                .bearer_auth("adm")
                .json(&serde_json::json!({"participant": "team-persist"}))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        assert_eq!(
            http.post(format!("{dash}/api/quotas/participants/team-persist"))
                .bearer_auth("adm")
                .json(&serde_json::json!({"max_running_tasks": 1}))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );

        let mut ca = connect_v3(master.server_addr(), &ka, &master_pub).await;
        let t1 = submit_v3(&mut ca, echo_task("persist-1", 1)).await.unwrap();
        let t2 = submit_v3(&mut ca, echo_task("persist-2", 1)).await.unwrap();
        let t3 = submit_v3(&mut ca, echo_task("persist-3", 1)).await.unwrap();
        wait_task_queued(&master, t3, Duration::from_secs(30)).await;

        master.shutdown().unwrap();
        drop(master);
        (t1, t2, t3, master_pub)
    };

    // Wait cleanly for Master 1 to release `store.lock` before opening Master 2 on the same dir.
    let deadline = Instant::now() + Duration::from_secs(30);
    let master2 = loop {
        let cfg2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_state_dir(&state_dir)
            .with_auth_mode(AuthMode::Members)
            .with_dashboard_port(0)
            .with_auth_config(auth_cfg.clone());
        match MasterServer::spawn(cfg2).await {
            Ok(h) => break h,
            Err(GridError::Storage(_)) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(e) => panic!("failed to restart master on state_dir: {e}"),
        }
    };
    assert_eq!(master2.master_noise_public_key().unwrap(), master_pub);

    let dash2 = format!("http://{}", master2.dashboard_addr().unwrap());
    let http = http_client();

    // Verify recovered participant quota, label, and queued count on `/api/quotas` after restart.
    let q_dto: serde_json::Value = http
        .get(format!("{dash2}/api/quotas"))
        .bearer_auth("adm")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let persist_row = q_dto["participants"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["participant"] == "team-persist")
        .expect("team-persist must survive restart");
    assert_eq!(persist_row["effective_quota"]["max_running_tasks"], 1);
    assert_eq!(persist_row["usage"]["queued_tasks"], 3);

    // Submit a control task for bob-ctrl-persist and connect a 4-core worker on Master 2:
    // ONLY 1 of `team-persist`'s recovered tasks must run concurrently while bob's control runs.
    let mut cb2 = connect_v3(master2.server_addr(), &kb, &master_pub).await;
    let b_ctrl = submit_v3(&mut cb2, echo_task("persist-ctrl", 1))
        .await
        .unwrap();
    let (wid, mut worker) = register_fake_worker(
        master2.server_addr(),
        "w-persist",
        4,
        Some((&kw, &master_pub)),
    )
    .await;

    let first = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    let second = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    let alice_set: HashSet<TaskId> = [t1, t2, t3].into_iter().collect();
    let got_ids: HashSet<TaskId> = [first.id, second.id].into_iter().collect();
    assert!(
        got_ids.contains(&b_ctrl),
        "bob control task must run on free slots"
    );
    let first_alice = if first.id == b_ctrl {
        second.id
    } else {
        first.id
    };
    assert!(
        alice_set.contains(&first_alice),
        "only one recovered team-persist task may run concurrently"
    );
    send_task_success(&mut worker, wid, b_ctrl).await;
    send_task_success(&mut worker, wid, first_alice).await;

    let next_a = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    assert!(alice_set.contains(&next_a.id));
    send_task_success(&mut worker, wid, next_a.id).await;
    let last_a = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    assert!(alice_set.contains(&last_a.id));
    send_task_success(&mut worker, wid, last_a.id).await;
    wait_task_state(
        &master2,
        last_a.id,
        TaskState::Completed,
        Duration::from_secs(30),
    )
    .await;
    let _ = master2.shutdown();
}

#[tokio::test]
async fn enroll_1c_step2_revoke_member_cancels_running_and_queued_tasks() {
    let kadm = NodeKeypair::generate().unwrap();
    let ka = NodeKeypair::generate().unwrap();
    let kb = NodeKeypair::generate().unwrap();
    let kw = NodeKeypair::generate().unwrap();

    let members = MemberStore::from_entries(vec![
        MemberEntry::new("admin", *kadm.public_key(), vec![MemberRole::Admin]),
        MemberEntry::new("revokee-a", *ka.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("keeper-b", *kb.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("worker-w", *kw.public_key(), vec![MemberRole::Worker]),
    ]);
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_auth_mode(AuthMode::Members)
        .with_members(members);
    let master = MasterServer::spawn(cfg).await.unwrap();
    let master_pub = master.master_noise_public_key().unwrap();
    let addr = master.server_addr();

    // 1-core worker so only `a_run` can occupy it while `a_queued` and `b_ctrl` wait in queue.
    let (wid, mut worker) =
        register_fake_worker(addr, "w-revoke", 1, Some((&kw, &master_pub))).await;

    let mut ca = connect_v3(addr, &ka, &master_pub).await;
    let a_run = submit_v3(&mut ca, echo_task("a-running", 1)).await.unwrap();
    let assigned = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    assert_eq!(assigned.id, a_run);

    let a_queued = submit_v3(&mut ca, echo_task("a-queued", 1)).await.unwrap();
    wait_task_queued(&master, a_queued, Duration::from_secs(30)).await;

    let mut cb = connect_v3(addr, &kb, &master_pub).await;
    let b_ctrl = submit_v3(&mut cb, echo_task("b-control", 1)).await.unwrap();
    wait_task_queued(&master, b_ctrl, Duration::from_secs(30)).await;

    // Admin revokes `ka` -> Step 2 must cancel BOTH `a_run` and `a_queued`, notify worker with
    // `CancelTask(a_run)`, and allow `b_ctrl` to run and complete on `kw`.
    let mut cadm = connect_v3(addr, &kadm, &master_pub).await;
    cadm.send_msg(&ClientMessage::RevokeMember {
        target: ka.member_id(),
    })
    .await
    .unwrap();
    match cadm.recv_msg::<ClientResponse>().await.unwrap() {
        Some(ClientResponse::MemberRevoked { member_id, .. }) => {
            assert_eq!(member_id, ka.member_id());
        }
        other => panic!("expected MemberRevoked, got {other:?}"),
    }

    let mut saw_cancel_a_run = false;
    let mut saw_assign_b_ctrl = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !(saw_cancel_a_run && saw_assign_b_ctrl) {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(
            !left.is_zero(),
            "timed out waiting for CancelTask and AssignTask(b_ctrl)"
        );
        match tokio::time::timeout(left, worker.recv_msg::<MasterMessage>())
            .await
            .expect("worker recv timeout")
            .expect("worker recv io")
        {
            Some(MasterMessage::CancelTask { task_id, .. }) => {
                assert_eq!(task_id, a_run, "must only cancel a_run");
                saw_cancel_a_run = true;
            }
            Some(MasterMessage::AssignTask { task }) => {
                assert_ne!(task.id, a_queued, "cancelled a_queued must never dispatch");
                assert_eq!(task.id, b_ctrl);
                saw_assign_b_ctrl = true;
            }
            Some(MasterMessage::HeartbeatAck { .. }) => {}
            other => panic!("unexpected worker msg after revoke: {other:?}"),
        }
    }

    wait_task_state(
        &master,
        a_run,
        TaskState::Cancelled,
        Duration::from_secs(30),
    )
    .await;
    wait_task_state(
        &master,
        a_queued,
        TaskState::Cancelled,
        Duration::from_secs(30),
    )
    .await;

    send_task_success(&mut worker, wid, b_ctrl).await;
    wait_task_state(
        &master,
        b_ctrl,
        TaskState::Completed,
        Duration::from_secs(30),
    )
    .await;

    let _ = master.shutdown();
}

#[tokio::test]
async fn mapreduce_enforces_participant_quotas_and_reports_in_api() {
    let ka = NodeKeypair::generate().unwrap();
    let kw = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![
        MemberEntry::new("mr-alice", *ka.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("mr-worker", *kw.public_key(), vec![MemberRole::Worker]),
    ]);
    members
        .set_member_participant(&ka.member_id(), Some("alice"))
        .unwrap();

    let mut overrides = BTreeMap::new();
    overrides.insert(
        "alice".to_string(),
        ParticipantQuota {
            max_running_tasks: Some(1),
            ..Default::default()
        },
    );
    let auth_cfg = AuthConfig::new().with_token("adm", "admin", &[AuthScope::Admin]);
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_auth_mode(AuthMode::Members)
        .with_members(members)
        .with_dashboard_port(0)
        .with_auth_config(auth_cfg)
        .with_quota_policy(QuotaPolicyConfig {
            default_quota: ParticipantQuota::default(),
            participant_overrides: overrides,
        });
    let master = MasterServer::spawn(cfg).await.unwrap();
    let master_pub = master.master_noise_public_key().unwrap();
    let addr = master.server_addr();
    let dash = format!("http://{}", master.dashboard_addr().unwrap());

    let (wid, mut worker) = register_fake_worker(addr, "w-mr-4", 4, Some((&kw, &master_pub))).await;

    let mut ca = connect_v3(addr, &ka, &master_pub).await;
    let job = MapReduceJobSpec::new(
        "mr-quota-test",
        vec!["one".into(), "two".into(), "three".into()],
        MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        3,
        1,
        30,
    );
    let mr_handle = tokio::spawn(async move {
        ca.send_msg(&ClientMessage::SubmitMapReduce { job })
            .await
            .unwrap();
        ca.recv_msg::<ClientResponse>().await.unwrap()
    });

    // Receive the first mapper; the other 2 mappers must wait queued under alice max_running=1.
    let m1 = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    let http = http_client();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let q_dto: serde_json::Value = http
            .get(format!("{dash}/api/quotas"))
            .bearer_auth("adm")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let row = q_dto["participants"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["participant"] == "alice");
        if let Some(r) = row {
            if r["usage"]["running_tasks"] == 1 && r["usage"]["queued_tasks"] == 2 {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for MapReduce quota usage on /api/quotas"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Complete all 3 mappers sequentially; prove each starts only after the previous finishes.
    for (idx, task) in [Some(m1), None, None].into_iter().enumerate() {
        let current = match task {
            Some(t) => t,
            None => recv_next_assignment(&mut worker, Duration::from_secs(30)).await,
        };
        assert!(
            tokio::time::timeout(
                Duration::from_millis(120),
                worker.recv_msg::<MasterMessage>()
            )
            .await
            .is_err(),
            "MapReduce exceeded max_running_tasks=1 on mapper wave {idx}"
        );
        worker
            .send_msg(&WorkerMessage::TaskResult {
                worker_id: wid,
                task_id: current.id,
                exit_code: 0,
                stdout: Bytes::from_static(b"word\t1\n"),
                stderr: Bytes::new(),
                execution_time_ms: 1,
                is_gpu_executed: false,
                device_name: None,
                error: None,
                asset_bytes: None,
                artifacts: vec![],
                outputs: vec![],
            })
            .await
            .unwrap();
    }

    match tokio::time::timeout(Duration::from_secs(30), mr_handle)
        .await
        .expect("MapReduce completion timeout")
        .expect("MapReduce task join")
    {
        Some(ClientResponse::MapReduceCompleted { result }) => {
            assert_eq!(result.status, "Completed");
            assert_eq!(result.map_tasks_completed, 3);
            assert_eq!(
                result.output.get("word").and_then(|v| v.as_f64()),
                Some(3.0)
            );
        }
        other => panic!("expected MapReduceCompleted, got {other:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn lowering_max_slots_fails_stranded_queued_task_unfulfillable_and_wakes_waiter() {
    let ka = NodeKeypair::generate().unwrap();
    let kw = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![
        MemberEntry::new("alice-slots", *ka.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("w-slots", *kw.public_key(), vec![MemberRole::Worker]),
    ]);
    members
        .set_member_participant(&ka.member_id(), Some("alice"))
        .unwrap();

    let mut overrides = BTreeMap::new();
    overrides.insert(
        "alice".to_string(),
        ParticipantQuota {
            max_running_tasks: Some(1),
            max_slots: Some(4),
            ..Default::default()
        },
    );
    let auth_cfg = AuthConfig::new().with_token("adm", "admin", &[AuthScope::Admin]);
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_auth_mode(AuthMode::Members)
        .with_members(members)
        .with_dashboard_port(0)
        .with_auth_config(auth_cfg)
        .with_quota_policy(QuotaPolicyConfig {
            default_quota: ParticipantQuota::default(),
            participant_overrides: overrides,
        });
    let master = MasterServer::spawn(cfg).await.unwrap();
    let master_pub = master.master_noise_public_key().unwrap();
    let addr = master.server_addr();
    let dash = format!("http://{}", master.dashboard_addr().unwrap());

    let (wid, mut worker) =
        register_fake_worker(addr, "w-strand-4", 4, Some((&kw, &master_pub))).await;

    // Hold alice's running cap at 1/1 with a 1-core task while queuing a 4-core `wait: true` task.
    let mut ca1 = connect_v3(addr, &ka, &master_pub).await;
    let hold_id = submit_v3(&mut ca1, echo_task("hold-1", 1)).await.unwrap();
    let assigned_hold = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    assert_eq!(assigned_hold.id, hold_id);

    let wide_task = echo_task("wide-4", 4);
    let wide_id = wide_task.id;
    let mut ca2 = connect_v3(addr, &ka, &master_pub).await;
    let waiter_handle = tokio::spawn(async move {
        ca2.send_msg(&ClientMessage::SubmitTask {
            task: wide_task,
            wait: true,
        })
        .await
        .unwrap();
        ca2.recv_msg::<ClientResponse>().await.unwrap()
    });
    wait_task_queued(&master, wide_id, Duration::from_secs(30)).await;

    // Lower alice's max_slots from 4 to 2 via HTTP Admin API -> stranded `wide_id` must fail as
    // Unfulfillable with reason naming `max_slots=2` and wake the `wait: true` client.
    let http = http_client();
    let resp = http
        .post(format!("{dash}/api/quotas/participants/alice"))
        .bearer_auth("adm")
        .json(&serde_json::json!({
            "max_running_tasks": 1,
            "max_slots": 2
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    match tokio::time::timeout(Duration::from_secs(30), waiter_handle)
        .await
        .expect("stranded task waiter timed out")
        .expect("waiter join")
    {
        Some(ClientResponse::TaskCompleted { task_id, result }) => {
            assert_eq!(task_id, wide_id);
            assert!(!result.is_success());
            let err = result.error.unwrap_or_default();
            assert!(err.contains("max_slots=2"), "unexpected error: {err}");
        }
        other => panic!("expected failed TaskCompleted for stranded task, got {other:?}"),
    }
    wait_task_state(&master, wide_id, TaskState::Failed, Duration::from_secs(30)).await;

    send_task_success(&mut worker, wid, hold_id).await;
    wait_task_state(
        &master,
        hold_id,
        TaskState::Completed,
        Duration::from_secs(30),
    )
    .await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn unbanded_http_and_master_handle_priority_is_clamped_below_members() {
    let ka = NodeKeypair::generate().unwrap();
    let kw = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![
        MemberEntry::new("high-alice", *ka.public_key(), vec![MemberRole::Client]),
        MemberEntry::new("w-clamp", *kw.public_key(), vec![MemberRole::Worker]),
    ]);
    members
        .set_member_participant(&ka.member_id(), Some("alice"))
        .unwrap();

    let mut overrides = BTreeMap::new();
    overrides.insert(
        "alice".to_string(),
        ParticipantQuota {
            priority_band: Some(PriorityBand::High),
            ..Default::default()
        },
    );
    let auth_cfg = AuthConfig::new().with_token("sub", "submitter", &[AuthScope::Submitter]);
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_auth_mode(AuthMode::Both)
        .with_members(members)
        .with_dashboard_port(0)
        .with_auth_config(auth_cfg)
        .with_quota_policy(QuotaPolicyConfig {
            default_quota: ParticipantQuota::default(),
            participant_overrides: overrides,
        });
    let master = MasterServer::spawn(cfg).await.unwrap();
    let master_pub = master.master_noise_public_key().unwrap();
    let addr = master.server_addr();
    let dash = format!("http://{}", master.dashboard_addr().unwrap());

    // Submit unbanded HTTP and MasterHandle tasks with priority=999 BEFORE alice's High-band task:
    // both unbanded tasks must clamp to Normal (50), so alice's High (100) still runs first.
    let http = http_client();
    let http_resp: serde_json::Value = http
        .post(format!("{dash}/api/tasks"))
        .bearer_auth("sub")
        .json(&serde_json::json!({"command": "echo http-unbanded", "priority": 999}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let http_id: TaskId = http_resp["task_id"].as_str().unwrap().parse().unwrap();
    let mh_id = master
        .submit_task_with_priority(echo_task("mh-unbanded", 1), 999)
        .await
        .unwrap();
    assert_eq!(
        master.get_task_info(http_id).await.unwrap().priority,
        PriorityBand::Normal.default_priority()
    );
    assert_eq!(
        master.get_task_info(mh_id).await.unwrap().priority,
        PriorityBand::Normal.default_priority()
    );

    let mut ca = connect_v3(addr, &ka, &master_pub).await;
    let high_id = submit_v3(&mut ca, echo_task("alice-high", 1))
        .await
        .unwrap();

    let (wid, mut worker) =
        register_fake_worker(addr, "w-clamp-1", 1, Some((&kw, &master_pub))).await;
    let first = recv_next_assignment(&mut worker, Duration::from_secs(30)).await;
    assert_eq!(
        first.id, high_id,
        "High-band member task must dispatch before clamped unbanded HTTP/MasterHandle tasks"
    );
    send_task_success(&mut worker, wid, first.id).await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn verify_replicas_exceeding_max_queued_tasks_is_refused_upfront() {
    let ka = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![MemberEntry::new(
        "verify-alice",
        *ka.public_key(),
        vec![MemberRole::Client],
    )]);
    members
        .set_member_participant(&ka.member_id(), Some("alice"))
        .unwrap();

    let mut overrides = BTreeMap::new();
    overrides.insert(
        "alice".to_string(),
        ParticipantQuota {
            max_queued_tasks: Some(2),
            ..Default::default()
        },
    );
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_auth_mode(AuthMode::Members)
        .with_members(members)
        .with_quota_policy(QuotaPolicyConfig {
            default_quota: ParticipantQuota::default(),
            participant_overrides: overrides,
        });
    let master = MasterServer::spawn(cfg).await.unwrap();
    let master_pub = master.master_noise_public_key().unwrap();
    let addr = master.server_addr();

    let mut ca = connect_v3(addr, &ka, &master_pub).await;
    let mut bad_verify = echo_task("verify-3", 1);
    bad_verify
        .tags
        .push(format!("{VERIFY_REPLICAS_TAG_PREFIX}3"));
    let err = submit_v3(&mut ca, bad_verify)
        .await
        .expect_err("--verify 3 must be refused up front under max_queued_tasks=2");
    assert!(
        err.contains("max_queued_tasks") && err.contains("--verify 3"),
        "unexpected error: {err}"
    );
    assert_eq!(master.queue().stats().await.total, 0);

    let mut ok_verify = echo_task("verify-2", 1);
    ok_verify
        .tags
        .push(format!("{VERIFY_REPLICAS_TAG_PREFIX}2"));
    let ok_id = submit_v3(&mut ca, ok_verify)
        .await
        .expect("--verify 2 fits max_queued_tasks=2");
    wait_task_queued(&master, ok_id, Duration::from_secs(30)).await;
    let _ = master.shutdown();
}
