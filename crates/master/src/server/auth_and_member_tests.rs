use super::client_and_members::wait_for_pending_enrollment;
use super::*;
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::noise_auth::NodeKeypair;
use rusty_grid_core::protocol::{
    ClientMessage, ClientResponse, EndpointRole, InboundMessage, ProtocolCapabilities,
    PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::Task;
use rusty_grid_core::{MasterMessage, MessageTransport, WorkerCapabilities, WorkerMessage};
use tokio::net::TcpStream;

use crate::queue::TaskState;

#[tokio::test]
async fn test_v3_auth_role_check_and_members_only_mode() {
    use super::members::{MemberEntry, MemberRole};
    use rusty_grid_core::cluster_auth::client_handshake;
    use rusty_grid_core::noise_auth::{v3_client_handshake, HandshakePurpose};
    use rusty_grid_core::WorkerCapabilities;

    let master_kp = NodeKeypair::generate().unwrap();
    let worker_kp = NodeKeypair::generate().unwrap();
    let client_only_kp = NodeKeypair::generate().unwrap();

    let members = Arc::new(MemberStore::from_entries(vec![
        MemberEntry::new(
            "worker-a",
            *worker_kp.public_key(),
            vec![MemberRole::Worker],
        ),
        MemberEntry::new(
            "client-only",
            *client_only_kp.public_key(),
            vec![MemberRole::Client],
        ),
    ]));
    let secret = ClusterSecret::from_hex("0123456789abcdef0123456789abcdef").unwrap();

    // 1. v3 peer without the `worker` role cannot Register.
    let ctx_both = MasterAuthContext {
        auth_mode: AuthMode::Both,
        master_noise_key: Some(master_kp.clone()),
        master_noise_key_path: None,
        local_node_key_path: None,
        members: Arc::clone(&members),
        p2p_ticket: None,
        ..Default::default()
    };
    let (c_stream, s_stream) = tokio::io::duplex(16 * 1024);
    let client_pub = *master_kp.public_key();
    let client_kp = client_only_kp.clone();
    let client_task = tokio::spawn(async move {
        let mut t = MessageTransport::new(c_stream);
        let sk = v3_client_handshake(&mut t, &client_kp, &client_pub, HandshakePurpose::Member)
            .await
            .unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        let reg = WorkerMessage::Register {
            worker_id: Uuid::new_v4(),
            capabilities: WorkerCapabilities::default(),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        };
        t.send_msg(&reg).await.unwrap();
        t.recv_msg::<MasterMessage>().await.unwrap()
    });

    let mut st = MessageTransport::new(s_stream);
    let server_res = authenticate_and_read_first_message(
        &mut st,
        "127.0.0.1:50000",
        Some(&secret),
        &ctx_both,
        Duration::from_secs(5),
        "connection",
    )
    .await;
    let err = server_res.unwrap_err().to_string();
    assert!(
        err.contains("member does not have the worker role"),
        "expected missing worker role error, got: {err}"
    );
    let ack = client_task.await.unwrap();
    match ack {
        Some(MasterMessage::RegisterAck {
            accepted: false,
            message: Some(msg),
            ..
        }) => assert!(msg.contains("worker role"), "got: {msg}"),
        other => panic!("expected rejected RegisterAck, got: {other:?}"),
    }

    // 1b. v3 peer with `worker` role is allowed read-only `ListWorkers`, but refused on
    //     `CancelTask` (requires `client`), while `client`-only is refused on `ListMembers`
    //     (requires `admin`).
    let (c_stream, s_stream) = tokio::io::duplex(16 * 1024);
    let client_pub = *master_kp.public_key();
    let worker_only = worker_kp.clone();
    let worker_ro_task = tokio::spawn(async move {
        let mut t = MessageTransport::new(c_stream);
        let sk = v3_client_handshake(&mut t, &worker_only, &client_pub, HandshakePurpose::Member)
            .await
            .unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&ClientMessage::ListWorkers).await.unwrap();
    });
    let mut st = MessageTransport::new(s_stream);
    let (first_msg, _, v3_peer) = authenticate_and_read_first_message(
        &mut st,
        "127.0.0.1:50002",
        Some(&secret),
        &ctx_both,
        Duration::from_secs(5),
        "connection",
    )
    .await
    .expect("worker-only member must be allowed read-only ListWorkers");
    assert!(matches!(
        first_msg,
        InboundMessage::Client(ClientMessage::ListWorkers)
    ));
    assert!(v3_peer.is_some());
    worker_ro_task.await.unwrap();

    let (c_stream, s_stream) = tokio::io::duplex(16 * 1024);
    let client_pub = *master_kp.public_key();
    let worker_only = worker_kp.clone();
    let worker_as_client_task = tokio::spawn(async move {
        let mut t = MessageTransport::new(c_stream);
        let sk = v3_client_handshake(&mut t, &worker_only, &client_pub, HandshakePurpose::Member)
            .await
            .unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&ClientMessage::CancelTask {
            task_id: TaskId::new(),
        })
        .await
        .unwrap();
        t.recv_msg::<ClientResponse>().await.unwrap()
    });

    let mut st = MessageTransport::new(s_stream);
    let server_res = authenticate_and_read_first_message(
        &mut st,
        "127.0.0.1:50003",
        Some(&secret),
        &ctx_both,
        Duration::from_secs(5),
        "connection",
    )
    .await;
    let err = server_res.unwrap_err().to_string();
    assert!(
        err.contains("member does not have the client role"),
        "expected missing client role error, got: {err}"
    );
    let resp = worker_as_client_task.await.unwrap();
    match resp {
        Some(ClientResponse::Error { message }) => {
            assert!(message.contains("client role"), "got: {message}");
        }
        other => panic!("expected ClientResponse::Error, got: {other:?}"),
    }

    let (c_stream, s_stream) = tokio::io::duplex(16 * 1024);
    let client_pub = *master_kp.public_key();
    let client_only = client_only_kp.clone();
    let client_as_admin_task = tokio::spawn(async move {
        let mut t = MessageTransport::new(c_stream);
        let sk = v3_client_handshake(&mut t, &client_only, &client_pub, HandshakePurpose::Member)
            .await
            .unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&ClientMessage::ListMembers).await.unwrap();
        t.recv_msg::<ClientResponse>().await.unwrap()
    });

    let mut st = MessageTransport::new(s_stream);
    let server_res = authenticate_and_read_first_message(
        &mut st,
        "127.0.0.1:50004",
        Some(&secret),
        &ctx_both,
        Duration::from_secs(5),
        "connection",
    )
    .await;
    let err = server_res.unwrap_err().to_string();
    assert!(
        err.contains("member does not have the admin role"),
        "expected missing admin role error, got: {err}"
    );
    let resp = client_as_admin_task.await.unwrap();
    match resp {
        Some(ClientResponse::Error { message }) => {
            assert!(message.contains("admin role"), "got: {message}");
        }
        other => panic!("expected ClientResponse::Error, got: {other:?}"),
    }

    // 2. With `--auth members`, a v2 secret peer is refused with REJECT_MEMBERS_ONLY.
    let ctx_members = MasterAuthContext {
        auth_mode: AuthMode::Members,
        master_noise_key: Some(master_kp.clone()),
        master_noise_key_path: None,
        local_node_key_path: None,
        members: Arc::clone(&members),
        p2p_ticket: None,
        ..Default::default()
    };
    let (c_stream, s_stream) = tokio::io::duplex(16 * 1024);
    let secret_c = secret.clone();
    let v2_client = tokio::spawn(async move {
        let mut t = MessageTransport::new(c_stream);
        client_handshake(&mut t, &secret_c).await
    });
    let mut st = MessageTransport::new(s_stream);
    let v2_server = authenticate_and_read_first_message(
        &mut st,
        "127.0.0.1:50001",
        Some(&secret),
        &ctx_members,
        Duration::from_secs(5),
        "connection",
    )
    .await;
    assert!(v2_server.is_err());
    let v2_client_err = v2_client.await.unwrap().unwrap_err().to_string();
    assert!(
        v2_client_err.contains("this master admits enrolled machines only; join with an invite"),
        "expected members-only rejection reason, got: {v2_client_err}"
    );
}

#[tokio::test]
async fn test_v3_member_70_unnamed_worker_restarts_and_v2_offline_worker_ownership() {
    use rusty_grid_core::cluster_auth::client_handshake;
    use rusty_grid_core::noise_auth::{v3_client_handshake, HandshakePurpose};
    use rusty_grid_core::WorkerCapabilities;

    let secret = ClusterSecret::generate().unwrap();
    let master_kp = NodeKeypair::generate().unwrap();
    let member_kp = NodeKeypair::generate().unwrap();
    let members = MemberStore::from_entries(vec![super::members::MemberEntry::new(
        "worker-box",
        *member_kp.public_key(),
        vec![super::members::MemberRole::Worker],
    )]);
    let caps = WorkerCapabilities {
        cpu_cores: 2,
        ram_mb: 2048,
        ..Default::default()
    };
    let mut config = ServerConfig::default()
        .with_cluster_secret(secret.clone())
        .with_auth_mode(AuthMode::Both)
        .with_ephemeral_key(true)
        .with_master_noise_key(master_kp.clone())
        .with_members(members);
    config.bind_addr = "127.0.0.1:0".parse().unwrap();
    #[cfg(feature = "p2p")]
    {
        config.enable_p2p = false;
    }
    config.enable_discovery = false;
    #[cfg(feature = "dashboard")]
    {
        config.enable_web_ui = false;
    }
    let handle = MasterServer::spawn(config).await.unwrap();
    let addr = handle.server_addr;
    let master_pub = *master_kp.public_key();

    // N1: 70 sequential worker registrations with a fresh UUID each time (simulating 70
    // unnamed worker restarts) must all succeed; non-live worker_id claims are LRU-evicted.
    for run in 0..70 {
        let wid = Uuid::new_v4();
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut t = MessageTransport::new(stream);
        let sk = v3_client_handshake(&mut t, &member_kp, &master_pub, HandshakePurpose::Member)
            .await
            .unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&WorkerMessage::Register {
            worker_id: wid,
            capabilities: caps.clone(),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
        let ack = t.recv_msg::<MasterMessage>().await.unwrap().unwrap();
        match ack {
            MasterMessage::RegisterAck {
                accepted, message, ..
            } => {
                assert!(accepted, "run {run} must be accepted: {message:?}");
            }
            other => panic!("expected RegisterAck on run {run}, got: {other:?}"),
        }
        drop(t);
        // Wait until the server marks the session Disconnected before the next restart.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while handle.registry().live_worker_ids().await.contains(&wid) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "session for run {run} did not disconnect in time"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    assert_eq!(
        handle
            .members()
            .workers_owned_by(&member_kp.member_id())
            .len(),
        64
    );

    // N7: A v2 worker registers and then disconnects (temporarily offline); the v3 member
    // must NOT be able to claim the offline v2 worker's ID and lock the v2 worker out.
    let v2_wid = Uuid::new_v4();
    {
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut t = MessageTransport::new(stream);
        let sk = client_handshake(&mut t, &secret).await.unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&WorkerMessage::Register {
            worker_id: v2_wid,
            capabilities: caps.clone(),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
        match t.recv_msg::<MasterMessage>().await.unwrap().unwrap() {
            MasterMessage::RegisterAck { accepted, .. } => assert!(accepted),
            other => panic!("expected RegisterAck, got: {other:?}"),
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while handle.registry().live_worker_ids().await.contains(&v2_wid) {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // v3 member attempts to register `v2_wid` while the v2 worker is Disconnected -> refused!
    {
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut t = MessageTransport::new(stream);
        let sk = v3_client_handshake(&mut t, &member_kp, &master_pub, HandshakePurpose::Member)
            .await
            .unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&WorkerMessage::Register {
            worker_id: v2_wid,
            capabilities: caps.clone(),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
        match t.recv_msg::<MasterMessage>().await.unwrap().unwrap() {
            MasterMessage::RegisterAck {
                accepted, message, ..
            } => {
                assert!(
                    !accepted,
                    "v3 member must not take over an offline v2 worker's ID"
                );
                assert!(
                    message.unwrap_or_default().contains("owned by another"),
                    "expected ownership refusal"
                );
            }
            other => panic!("expected RegisterAck refusal, got: {other:?}"),
        }
    }
    // The v2 worker reconnects with `v2_wid` -> accepted!
    {
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut t = MessageTransport::new(stream);
        let sk = client_handshake(&mut t, &secret).await.unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&WorkerMessage::Register {
            worker_id: v2_wid,
            capabilities: caps,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
        match t.recv_msg::<MasterMessage>().await.unwrap().unwrap() {
            MasterMessage::RegisterAck { accepted, .. } => {
                assert!(accepted, "v2 worker must be able to reconnect with its ID");
            }
            other => panic!("expected RegisterAck, got: {other:?}"),
        }
    }

    let _ = handle.shutdown();
}

#[tokio::test]
async fn test_n2_unauthenticated_member_admin_refused_and_restart_with_secret_and_n6_key_mismatch()
{
    use rusty_grid_core::cluster_auth::client_handshake;
    use rusty_grid_core::noise_auth::MemberRole;

    let dir = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    // Phase 1: Start a loopback master with NO secret and no non-host members (`requires_auth` is false).
    let mut cfg1 = ServerConfig::default()
        .with_state_dir(dir.path().to_path_buf())
        .with_ephemeral_key(false);
    cfg1.bind_addr = "127.0.0.1:0".parse().unwrap();
    #[cfg(feature = "p2p")]
    {
        cfg1.enable_p2p = false;
    }
    cfg1.enable_discovery = false;
    #[cfg(feature = "dashboard")]
    {
        cfg1.enable_web_ui = false;
    }
    let handle1 = MasterServer::spawn(cfg1).await.unwrap();
    let addr1 = handle1.server_addr;

    // `mallory` connects without v2 or v3 authentication and attempts `CreateInvite` and `ListMembers`.
    for msg in [
        ClientMessage::CreateInvite {
            roles: vec![MemberRole::Admin],
            max_uses: 1,
            expires_in_secs: 3600,
            require_approval: false,
        },
        ClientMessage::ListMembers,
    ] {
        let stream = TcpStream::connect(addr1).await.unwrap();
        let mut t = MessageTransport::new(stream);
        t.send_msg(&msg).await.unwrap();
        let resp = t.recv_msg::<ClientResponse>().await.unwrap().unwrap();
        match resp {
            ClientResponse::Error { message } => {
                assert!(
                    message.contains("Authentication failed"),
                    "expected Authentication failed for unauthenticated member-admin, got: {message}"
                );
            }
            other => panic!("expected ClientResponse::Error for {msg:?}, got: {other:?}"),
        }
    }
    assert!(
        handle1.members().is_empty(),
        "no members or invites may be created by an unauthenticated peer"
    );
    let _ = handle1.shutdown();

    // Phase 2: Restart the master WITH a cluster secret on the same state_dir.
    let secret = ClusterSecret::generate().unwrap();
    let mut cfg2 = ServerConfig::default()
        .with_state_dir(dir.path().to_path_buf())
        .with_cluster_secret(secret.clone())
        .with_ephemeral_key(false);
    cfg2.bind_addr = "127.0.0.1:0".parse().unwrap();
    #[cfg(feature = "p2p")]
    {
        cfg2.enable_p2p = false;
    }
    cfg2.enable_discovery = false;
    #[cfg(feature = "dashboard")]
    {
        cfg2.enable_web_ui = false;
    }
    let handle2 = MasterServer::spawn(cfg2).await.unwrap();
    let addr2 = handle2.server_addr;

    // Unauthenticated peer is rejected at the handshake gate.
    {
        let stream = TcpStream::connect(addr2).await.unwrap();
        let mut t = MessageTransport::new(stream);
        let _ = t.send_msg(&ClientMessage::ListMembers).await;
        let res = t.recv_msg::<ClientResponse>().await;
        assert!(
            !matches!(res, Ok(Some(ClientResponse::MembersList { .. }))),
            "unauthenticated peer must not list members after restart with secret"
        );
    }
    // v2-authenticated admin can list members and create an invite.
    {
        let stream = TcpStream::connect(addr2).await.unwrap();
        let mut t = MessageTransport::new(stream);
        let sk = client_handshake(&mut t, &secret).await.unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&ClientMessage::ListMembers).await.unwrap();
        match t.recv_msg::<ClientResponse>().await.unwrap().unwrap() {
            ClientResponse::MembersList { members } => assert!(members.is_empty()),
            other => panic!("expected MembersList, got: {other:?}"),
        }
    }
    let _ = handle2.shutdown();

    // N6: ensure_v3_key_persisted detects a mismatch when master_noise.key on disk differs
    // from the in-memory master_noise_key.
    let disk_kp = NodeKeypair::generate().unwrap();
    let mem_kp = NodeKeypair::generate().unwrap();
    let key_path = dir.path().join("mismatch_master_noise.key");
    disk_kp.save_file(&key_path, false).unwrap();
    let mismatch_ctx = MasterAuthContext {
        auth_mode: AuthMode::Both,
        master_noise_key: Some(mem_kp),
        master_noise_key_path: Some(key_path),
        local_node_key_path: None,
        ephemeral_without_state_dir: false,
        members: Arc::new(MemberStore::new()),
        p2p_ticket: None,
        ..Default::default()
    };
    let err = mismatch_ctx
        .ensure_v3_key_persisted()
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("different public key"),
        "expected key mismatch error, got: {err}"
    );
}

#[tokio::test]
async fn test_round3_item6_v2_reclaims_squatted_worker_id_and_survives_restart_and_item7_prune() {
    use rusty_grid_core::cluster_auth::client_handshake;
    use rusty_grid_core::noise_auth::{
        compute_enroll_proof, v3_client_handshake, HandshakePurpose, MemberRole,
    };

    let dir = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let secret = ClusterSecret::generate().unwrap();
    let master_kp = NodeKeypair::generate().unwrap();
    let master_pub = *master_kp.public_key();
    let member_kp = NodeKeypair::generate().unwrap();
    let members = MemberStore::load_from_dir(Some(dir.path())).unwrap();
    let inv = members
        .create_invite(vec![MemberRole::Worker], 1, 3600, false)
        .unwrap();
    let h = [0x22u8; 32];
    let proof = compute_enroll_proof(&inv.invite_secret.0, &h);
    members
        .handle_enroll_request(
            *member_kp.public_key(),
            &h,
            "1111-2222-3333-4444",
            "127.0.0.1:44000",
            &inv.invite_id,
            &proof,
            "w-member".into(),
            "linux".into(),
            "x86_64".into(),
        )
        .unwrap();

    let make_cfg = |members: MemberStore| {
        let mut cfg = ServerConfig::default()
            .with_state_dir(dir.path().to_path_buf())
            .with_cluster_secret(secret.clone())
            .with_auth_mode(AuthMode::Both)
            .with_ephemeral_key(false)
            .with_master_noise_key(master_kp.clone())
            .with_members(members);
        cfg.bind_addr = "127.0.0.1:0".parse().unwrap();
        #[cfg(feature = "p2p")]
        {
            cfg.enable_p2p = false;
        }
        cfg.enable_discovery = false;
        #[cfg(feature = "dashboard")]
        {
            cfg.enable_web_ui = false;
        }
        cfg
    };

    let handle = MasterServer::spawn(make_cfg(members)).await.unwrap();
    let addr = handle.server_addr;
    let v2_wid = Uuid::new_v4();
    let caps = WorkerCapabilities::new("node", 2, 1024, false, false, None);

    // 1. v3 member registers `v2_wid` first (squatting) and then disconnects.
    {
        let s = TcpStream::connect(addr).await.unwrap();
        let mut t = MessageTransport::new(s);
        let sk = v3_client_handshake(&mut t, &member_kp, &master_pub, HandshakePurpose::Member)
            .await
            .unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&WorkerMessage::Register {
            worker_id: v2_wid,
            capabilities: caps.clone(),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
        assert!(matches!(
            t.recv_msg::<MasterMessage>().await.unwrap(),
            Some(MasterMessage::RegisterAck { accepted: true, .. })
        ));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while handle.registry().live_worker_ids().await.contains(&v2_wid) {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // 2. While v3 member is still live on another session, v2 cannot hijack a live v3 session;
    // once the squatted v3 session has disconnected, the v2 cluster-secret worker reclaims `v2_wid`.
    {
        let s = TcpStream::connect(addr).await.unwrap();
        let mut t = MessageTransport::new(s);
        let sk = client_handshake(&mut t, &secret).await.unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&WorkerMessage::Register {
            worker_id: v2_wid,
            capabilities: caps.clone(),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
        assert!(
            matches!(
                t.recv_msg::<MasterMessage>().await.unwrap(),
                Some(MasterMessage::RegisterAck { accepted: true, .. })
            ),
            "v2 cluster-secret worker must reclaim a non-live squatted worker_id"
        );
    }
    let _ = handle.shutdown();

    // 3. Master restarts from disk; v2 worker's ownership of `v2_wid` was persisted in
    // `v2_worker_seq`, so v3 member is refused BEFORE v2 reconnects, and v2 reconnects cleanly.
    let reloaded_members = MemberStore::load_from_dir(Some(dir.path())).unwrap();
    let handle2 = MasterServer::spawn(make_cfg(reloaded_members))
        .await
        .unwrap();
    let addr2 = handle2.server_addr;

    // v3 member tries to re-squat `v2_wid` right after master restart -> refused!
    {
        let s = TcpStream::connect(addr2).await.unwrap();
        let mut t = MessageTransport::new(s);
        let sk = v3_client_handshake(&mut t, &member_kp, &master_pub, HandshakePurpose::Member)
            .await
            .unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&WorkerMessage::Register {
            worker_id: v2_wid,
            capabilities: caps.clone(),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
        assert!(
            matches!(
                t.recv_msg::<MasterMessage>().await.unwrap(),
                Some(MasterMessage::RegisterAck {
                    accepted: false,
                    ..
                })
            ),
            "v3 member must not reclaim a persisted v2 worker_id after master restart"
        );
    }

    // v2 worker reconnects after master restart -> accepted!
    {
        let s = TcpStream::connect(addr2).await.unwrap();
        let mut t = MessageTransport::new(s);
        let sk = client_handshake(&mut t, &secret).await.unwrap();
        t.enable_encryption(&sk, EndpointRole::Client);
        t.send_msg(&WorkerMessage::Register {
            worker_id: v2_wid,
            capabilities: caps.clone(),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
        assert!(
            matches!(
                t.recv_msg::<MasterMessage>().await.unwrap(),
                Some(MasterMessage::RegisterAck { accepted: true, .. })
            ),
            "v2 worker must reconnect after master restart"
        );
    }

    // Item 7: remove_worker removes from owner_member_ids as well.
    assert!(handle2
        .registry()
        .check_worker_owner(&v2_wid, None)
        .await
        .is_ok());
    handle2.registry().remove_worker(&v2_wid).await;
    assert!(
        handle2
            .registry()
            .check_worker_owner(&v2_wid, Some("any-member"))
            .await
            .is_ok(),
        "remove_worker must also remove the worker_id from in-memory owner_member_ids"
    );

    let _ = handle2.shutdown();
}

#[tokio::test]
async fn test_round4_n3_server_disconnect_race_and_nit3_confirm_timeout_message() {
    use rusty_grid_core::cluster_auth::client_handshake;
    use rusty_grid_core::noise_auth::{
        compute_enroll_proof, derive_pairing_code, v3_client_handshake_with_hash, HandshakePurpose,
        MemberRole, RedactedString,
    };
    use rusty_grid_core::protocol::EndpointRole;

    let dir = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let secret = ClusterSecret::generate().unwrap();
    let master_kp = NodeKeypair::generate().unwrap();
    let master_pub = *master_kp.public_key();
    let members = MemberStore::load_from_dir(Some(dir.path())).unwrap();
    let inv = members
        .create_invite(vec![MemberRole::Worker], 1, 3600, true)
        .unwrap();

    let mut cfg = ServerConfig::default()
        .with_state_dir(dir.path().to_path_buf())
        .with_cluster_secret(secret.clone())
        .with_auth_mode(AuthMode::Both)
        .with_ephemeral_key(false)
        .with_master_noise_key(master_kp.clone())
        .with_members(members.clone());
    cfg.bind_addr = "127.0.0.1:0".parse().unwrap();
    #[cfg(feature = "p2p")]
    {
        cfg.enable_p2p = false;
    }
    cfg.enable_discovery = false;
    #[cfg(feature = "dashboard")]
    {
        cfg.enable_web_ui = false;
    }

    let handle = MasterServer::spawn(cfg).await.unwrap();
    let addr = handle.server_addr;

    // 1. N3 TCP race: joiner connects, sends Enroll, receives EnrollPending, and drops its
    //    TCP stream concurrently with admin sending ApproveEnrollment over TCP.
    for i in 0..15u32 {
        let joiner_kp = NodeKeypair::generate().unwrap();
        let s = TcpStream::connect(addr).await.unwrap();
        let mut jt = MessageTransport::new(s);
        let (sk, hh) = v3_client_handshake_with_hash(
            &mut jt,
            &joiner_kp,
            &master_pub,
            HandshakePurpose::Enroll,
        )
        .await
        .unwrap();
        jt.enable_encryption(&sk, EndpointRole::Client);
        let code = derive_pairing_code(&hh).as_str().to_string();
        let proof = compute_enroll_proof(&inv.invite_secret.0, &hh);
        jt.send_msg(&ClientMessage::Enroll {
            invite_id: inv.invite_id.clone(),
            proof,
            name: format!("tcp-race-{i}"),
            os: "linux".into(),
            arch: "x86_64".into(),
        })
        .await
        .unwrap();
        let pid = match jt.recv_msg::<ClientResponse>().await.unwrap().unwrap() {
            ClientResponse::EnrollPending { pending_id } => pending_id,
            other => panic!("expected EnrollPending, got: {other:?}"),
        };

        let admin_s = TcpStream::connect(addr).await.unwrap();
        let mut admin_t = MessageTransport::new(admin_s);
        let ask = client_handshake(&mut admin_t, &secret).await.unwrap();
        admin_t.enable_encryption(&ask, EndpointRole::Client);

        let approve_fut = async move {
            admin_t
                .send_msg(&ClientMessage::ApproveEnrollment {
                    pending_id: pid,
                    pairing_code: RedactedString(code),
                })
                .await
                .unwrap();
            let _ = admin_t.recv_msg::<ClientResponse>().await;
        };
        let drop_fut = async move {
            tokio::task::yield_now().await;
            drop(jt);
        };
        tokio::join!(approve_fut, drop_fut);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            if !members.contains(joiner_kp.public_key())
                && members.unconfirmed_tracking_counts() == (0, 0)
                && members.list_invites().first().map(|x| x.uses_remaining) == Some(1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !members.contains(joiner_kp.public_key()),
            "iteration {i}: disconnected joiner stayed enrolled"
        );
        assert_eq!(members.list_invites()[0].uses_remaining, 1, "iteration {i}");
        assert_eq!(
            members.unconfirmed_tracking_counts(),
            (0, 0),
            "iteration {i}"
        );
    }

    let _ = handle.shutdown();

    // 2. NIT 3: verify distinct timeout message ("enrollment confirmation timed out") vs
    //    client-cancelled message ("enrollment cancelled by joining machine").
    let mem_nit3 = MemberStore::new();
    let inv_nit3 = mem_nit3
        .create_invite(vec![MemberRole::Worker], 1, 3600, true)
        .unwrap();
    let j_nit3 = NodeKeypair::generate().unwrap();
    let h_nit3 = [0x99u8; 32];
    let p_nit3 = compute_enroll_proof(&inv_nit3.invite_secret.0, &h_nit3);
    let (pid_nit3, rx_nit3) = match mem_nit3
        .handle_enroll_request(
            *j_nit3.public_key(),
            &h_nit3,
            "1111-2222-3333-4444",
            "127.0.0.1:50099",
            &inv_nit3.invite_id,
            &p_nit3,
            "nit3-box".into(),
            "linux".into(),
            "x86_64".into(),
        )
        .unwrap()
    {
        members::EnrollOutcome::Pending {
            pending_id,
            decision_rx,
        } => (pending_id, decision_rx),
        _ => panic!("expected Pending"),
    };
    let auth_ctx_nit3 = MasterAuthContext {
        members: Arc::new(mem_nit3.clone()),
        enroll_confirm_timeout: Duration::from_millis(80),
        ..Default::default()
    };
    let reg_nit3 = WorkerRegistry::new();
    let q_nit3 = TaskQueue::new();
    let sched_nit3 = tokio::sync::Notify::new();
    let waiters_nit3: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
    let (c_dup, s_dup) = tokio::io::duplex(16 * 1024);
    let mut c_trans = MessageTransport::new(c_dup);
    let mut s_trans = MessageTransport::new(s_dup);

    let j_nit3_mid = j_nit3.member_id();
    let srv_fut = wait_for_pending_enrollment(
        &mut s_trans,
        &auth_ctx_nit3,
        &reg_nit3,
        &q_nit3,
        &sched_nit3,
        &waiters_nit3,
        #[cfg(feature = "dashboard")]
        None,
        &j_nit3_mid,
        pid_nit3.clone(),
        rx_nit3,
    );
    let cli_fut = async {
        assert!(matches!(
            c_trans.recv_msg::<ClientResponse>().await.unwrap(),
            Some(ClientResponse::EnrollPending { .. })
        ));
        mem_nit3
            .approve_pending(&pid_nit3, derive_pairing_code(&h_nit3).as_str())
            .unwrap();
        assert!(matches!(
            c_trans.recv_msg::<ClientResponse>().await.unwrap(),
            Some(ClientResponse::Enrolled { .. })
        ));
        // Do not send Hello; wait for the 80 ms confirmation timeout to fire!
        match c_trans.recv_msg::<ClientResponse>().await.unwrap() {
            Some(ClientResponse::EnrollRejected { message }) => {
                assert_eq!(message, "enrollment confirmation timed out");
            }
            other => panic!("expected EnrollRejected timeout message, got: {other:?}"),
        }
    };
    tokio::join!(srv_fut, cli_fut);
    assert!(!mem_nit3.contains(j_nit3.public_key()));
    assert_eq!(mem_nit3.list_invites()[0].uses_remaining, 1);
    assert_eq!(mem_nit3.unconfirmed_tracking_counts(), (0, 0));
}

#[tokio::test]
async fn test_r6_m1_release_worker_wire_error_and_s1_shutdown_flush() {
    use rusty_grid_core::cluster_auth::client_handshake;
    use rusty_grid_core::noise_auth::{
        compute_enroll_proof, derive_pairing_code, v3_client_handshake_with_hash, HandshakePurpose,
        MemberRole, RedactedString,
    };
    use rusty_grid_core::protocol::{EndpointRole, ProtocolCapabilities, PROTOCOL_VERSION_CURRENT};

    let dir = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let secret = ClusterSecret::generate().unwrap();
    let master_kp = NodeKeypair::generate().unwrap();
    let master_pub = *master_kp.public_key();
    let members = MemberStore::load_from_dir(Some(dir.path()))
        .unwrap()
        .with_auth_mode(AuthMode::Both);
    let inv = members
        .create_invite(vec![MemberRole::Worker], 1, 3600, true)
        .unwrap();

    let mut cfg = ServerConfig::default()
        .with_state_dir(dir.path().to_path_buf())
        .with_cluster_secret(secret.clone())
        .with_auth_mode(AuthMode::Both)
        .with_ephemeral_key(false)
        .with_master_noise_key(master_kp)
        .with_members(members.clone());
    cfg.bind_addr = "127.0.0.1:0".parse().unwrap();
    #[cfg(feature = "p2p")]
    {
        cfg.enable_p2p = false;
    }
    cfg.enable_discovery = false;
    #[cfg(feature = "dashboard")]
    {
        cfg.enable_web_ui = false;
    }

    let handle = MasterServer::spawn(cfg).await.unwrap();
    let addr = handle.server_addr;

    // 1. Register a v2 worker, then disconnect it cleanly so its `worker_id` is non-live.
    let v2_wid = Uuid::new_v4();
    {
        let ws = TcpStream::connect(addr).await.unwrap();
        let mut wt = MessageTransport::new(ws);
        let wsk = client_handshake(&mut wt, &secret).await.unwrap();
        wt.enable_encryption(&wsk, EndpointRole::Client);
        wt.send_msg(&WorkerMessage::Register {
            worker_id: v2_wid,
            capabilities: WorkerCapabilities::new("v2-m1-box", 2, 4096, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::empty(),
        })
        .await
        .unwrap();
        assert!(matches!(
            wt.recv_msg::<MasterMessage>().await.unwrap(),
            Some(MasterMessage::RegisterAck { accepted: true, .. })
        ));
        wt.send_msg(&WorkerMessage::Disconnecting {
            worker_id: v2_wid,
            reason: "stop before release-worker test".into(),
        })
        .await
        .unwrap();
    }
    let wait_disc = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < wait_disc {
        if handle.registry().list_all_workers().await.iter().all(|w| {
            w.worker_id != v2_wid || w.status == crate::registry::WorkerStatus::Disconnected
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Point `save_path` under a regular file so the `members.json` write fails (`M1`).
    let blocker = dir.path().join("not-a-dir");
    std::fs::write(&blocker, b"blocker").unwrap();
    members.set_save_path(Some(blocker.join("members.json")));

    {
        let admin_s = TcpStream::connect(addr).await.unwrap();
        let mut admin_t = MessageTransport::new(admin_s);
        let ask = client_handshake(&mut admin_t, &secret).await.unwrap();
        admin_t.enable_encryption(&ask, EndpointRole::Client);
        admin_t
            .send_msg(&ClientMessage::ReleaseWorkerId { worker_id: v2_wid })
            .await
            .unwrap();
        match admin_t.recv_msg::<ClientResponse>().await.unwrap() {
            Some(ClientResponse::Error { .. }) => {}
            other => panic!(
                "M1: ReleaseWorkerId must return ClientResponse::Error on failed write, got: {other:?}"
            ),
        }
    }
    assert!(
        handle
            .registry()
            .check_worker_owner(&v2_wid, Some("0123456789abcdef"))
            .await
            .is_err(),
        "M1: failed ReleaseWorkerId must retain v2 claim in WorkerRegistry"
    );
    assert!(
        members.claim_worker_id(v2_wid, "0123456789abcdef").is_err(),
        "M1: failed ReleaseWorkerId must retain v2_worker_seq claim in MemberStore"
    );

    // Restore valid `save_path` for `S1` enrollment + approval, then break `save_path` for
    // the post-approval rollback write and verify `handle.shutdown()` flushes the rollback.
    let valid_members_path = dir.path().join("members.json");
    members.set_save_path(Some(valid_members_path.clone()));

    let joiner_kp = NodeKeypair::generate().unwrap();
    let js = TcpStream::connect(addr).await.unwrap();
    let mut jt = MessageTransport::new(js);
    let (jsk, jhh) =
        v3_client_handshake_with_hash(&mut jt, &joiner_kp, &master_pub, HandshakePurpose::Enroll)
            .await
            .unwrap();
    jt.enable_encryption(&jsk, EndpointRole::Client);
    let code = derive_pairing_code(&jhh).as_str().to_string();
    let proof = compute_enroll_proof(&inv.invite_secret.0, &jhh);
    jt.send_msg(&ClientMessage::Enroll {
        invite_id: inv.invite_id.clone(),
        proof,
        name: "s1-shutdown-box".into(),
        os: "linux".into(),
        arch: "x86_64".into(),
    })
    .await
    .unwrap();
    let pid = match jt.recv_msg::<ClientResponse>().await.unwrap().unwrap() {
        ClientResponse::EnrollPending { pending_id } => pending_id,
        other => panic!("expected EnrollPending, got: {other:?}"),
    };

    {
        let admin_s = TcpStream::connect(addr).await.unwrap();
        let mut admin_t = MessageTransport::new(admin_s);
        let ask = client_handshake(&mut admin_t, &secret).await.unwrap();
        admin_t.enable_encryption(&ask, EndpointRole::Client);
        admin_t
            .send_msg(&ClientMessage::ApproveEnrollment {
                pending_id: pid,
                pairing_code: RedactedString(code),
            })
            .await
            .unwrap();
        assert!(matches!(
            admin_t.recv_msg::<ClientResponse>().await.unwrap(),
            Some(ClientResponse::EnrollmentApproved { .. })
        ));
    }
    assert!(matches!(
        jt.recv_msg::<ClientResponse>().await.unwrap(),
        Some(ClientResponse::Enrolled { .. })
    ));

    // Break `save_path` before dropping `jt` (simulating joiner declining `[y/N]` while disk
    // cannot be written).
    members.set_save_path(Some(blocker.join("members.json")));
    drop(jt);

    let wait_rb = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < wait_rb {
        if !members.contains(joiner_kp.public_key()) && members.is_dirty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!members.contains(joiner_kp.public_key()));
    assert!(members.is_dirty());
    // Disk still has `joiner_kp` before retry/shutdown flush.
    assert!(MemberStore::load_from_dir(Some(dir.path()))
        .unwrap()
        .contains(joiner_kp.public_key()));

    // Restore valid path while the server is still running -> `spawn_member_store_resync`
    // periodically flushes the dirty rollback without waiting for shutdown.
    members.set_save_path(Some(valid_members_path.clone()));
    let wait_resync = tokio::time::Instant::now() + Duration::from_secs(4);
    while tokio::time::Instant::now() < wait_resync {
        if !members.is_dirty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        !members.is_dirty(),
        "S1: periodic resync task must clear dirty flag once save_path recovers"
    );
    let reloaded_periodic = MemberStore::load_from_dir(Some(dir.path())).unwrap();
    assert!(
        !reloaded_periodic.contains(joiner_kp.public_key()),
        "S1: periodic resync must remove declined unconfirmed member from members.json"
    );

    // Now repeat with a second declined enrollment and verify `handle.shutdown()` flushes
    // synchronously before any periodic tick needs to run.
    let joiner_kp2 = NodeKeypair::generate().unwrap();
    let js2 = TcpStream::connect(addr).await.unwrap();
    let mut jt2 = MessageTransport::new(js2);
    let (jsk2, jhh2) =
        v3_client_handshake_with_hash(&mut jt2, &joiner_kp2, &master_pub, HandshakePurpose::Enroll)
            .await
            .unwrap();
    jt2.enable_encryption(&jsk2, EndpointRole::Client);
    let code2 = derive_pairing_code(&jhh2).as_str().to_string();
    let proof2 = compute_enroll_proof(&inv.invite_secret.0, &jhh2);
    jt2.send_msg(&ClientMessage::Enroll {
        invite_id: inv.invite_id.clone(),
        proof: proof2,
        name: "s1-shutdown-box-2".into(),
        os: "linux".into(),
        arch: "x86_64".into(),
    })
    .await
    .unwrap();
    let pid2 = match jt2.recv_msg::<ClientResponse>().await.unwrap().unwrap() {
        ClientResponse::EnrollPending { pending_id } => pending_id,
        other => panic!("expected EnrollPending, got: {other:?}"),
    };
    {
        let admin_s = TcpStream::connect(addr).await.unwrap();
        let mut admin_t = MessageTransport::new(admin_s);
        let ask = client_handshake(&mut admin_t, &secret).await.unwrap();
        admin_t.enable_encryption(&ask, EndpointRole::Client);
        admin_t
            .send_msg(&ClientMessage::ApproveEnrollment {
                pending_id: pid2,
                pairing_code: RedactedString(code2),
            })
            .await
            .unwrap();
        assert!(matches!(
            admin_t.recv_msg::<ClientResponse>().await.unwrap(),
            Some(ClientResponse::EnrollmentApproved { .. })
        ));
    }
    assert!(matches!(
        jt2.recv_msg::<ClientResponse>().await.unwrap(),
        Some(ClientResponse::Enrolled { .. })
    ));
    members.set_save_path(Some(blocker.join("members.json")));
    drop(jt2);
    let wait_rb2 = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < wait_rb2 {
        if !members.contains(joiner_kp2.public_key()) && members.is_dirty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(members.is_dirty());
    assert!(MemberStore::load_from_dir(Some(dir.path()))
        .unwrap()
        .contains(joiner_kp2.public_key()));

    members.set_save_path(Some(valid_members_path));
    handle.shutdown().unwrap();
    assert!(!members.is_dirty());

    let reloaded = MemberStore::load_from_dir(Some(dir.path())).unwrap();
    assert!(
        !reloaded.contains(joiner_kp2.public_key()),
        "S1: shutdown flush must remove declined unconfirmed member from members.json"
    );
    assert_eq!(
        reloaded
            .list_invites()
            .into_iter()
            .find(|i| i.invite_id == inv.invite_id)
            .expect("1-use invite must not be pruned after retried rollback")
            .uses_remaining,
        1
    );
}

#[tokio::test]
async fn server_speed_book_skips_checkpoint_resumed_attempts_but_records_checkpoint_emitters() {
    use rusty_grid_core::task::{Bytes, TaskRequirements, TaskSpec};

    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let addr = master.server_addr();

    let stream = TcpStream::connect(addr).await.unwrap();
    let mut worker = MessageTransport::new(stream);
    let worker_id = Uuid::new_v4();
    worker
        .send_msg(&WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities::new("cp-speed-worker", 2, 8192, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    match worker.recv_msg::<MasterMessage>().await.unwrap() {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("expected RegisterAck, got {other:?}"),
    }

    // 1. A normal attempt that emits a mid-run checkpoint and completes MUST still record in SpeedBook.
    let spec = TaskSpec::command("echo", vec!["cp-emit-and-resume".into()]);
    let task_key = crate::speed::TaskKey::of(&spec);
    let emit_task = Task::new(spec.clone(), TaskRequirements::generic(1, 60));
    let emit_id = emit_task.id;
    master.submit_task(emit_task).await.unwrap();

    match tokio::time::timeout(Duration::from_secs(2), worker.recv_msg::<MasterMessage>())
        .await
        .expect("worker must receive AssignTask")
        .unwrap()
    {
        Some(MasterMessage::AssignTask { task }) => assert_eq!(task.id, emit_id),
        other => panic!("expected AssignTask for fresh task, got {other:?}"),
    }
    worker
        .send_msg(&WorkerMessage::Checkpoint {
            task_id: emit_id,
            sequence: 1,
            delta_state: Bytes::from_static(b"mid-run-cp"),
        })
        .await
        .unwrap();
    let mut got_cp = false;
    for _ in 0..100 {
        if master.get_checkpoint(&emit_id).await.is_some() {
            got_cp = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(got_cp, "master must record mid-run checkpoint");

    worker
        .send_msg(&WorkerMessage::TaskResult {
            worker_id,
            task_id: emit_id,
            exit_code: 0,
            stdout: Bytes::from_static(b"ok"),
            stderr: Bytes::new(),
            execution_time_ms: 120,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: vec![],
            outputs: vec![],
        })
        .await
        .unwrap();
    let mut emit_done = false;
    for _ in 0..100 {
        if master.get_task_info(emit_id).await.unwrap().state == TaskState::Completed {
            emit_done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(emit_done, "emit task must complete");

    let sb = master
        .registry
        .speed_book()
        .expect("scheduler must attach SpeedBook to registry");
    {
        let guard = sb.lock().unwrap_or_else(|e| e.into_inner());
        let after_emit = guard
            .get(&worker_id, task_key)
            .expect("checkpoint-emitting normal task must be recorded in SpeedBook");
        assert_eq!(after_emit.samples, 1);
        assert!((after_emit.avg_ms - 120.0).abs() < 1e-9);
    }

    // 2. A checkpoint-resumed attempt (`AssignTaskWithCheckpoint`) MUST NOT record into SpeedBook,
    //    neither polluting `task_key`'s existing average nor creating a new entry for `resume_key`.
    let mut resumed_same_key = Task::new(spec, TaskRequirements::generic(1, 60));
    resumed_same_key.latest_checkpoint = Some(CheckpointData::new(2, b"prev".to_vec()));
    let resumed_same_id = resumed_same_key.id;
    master.submit_task(resumed_same_key).await.unwrap();

    match tokio::time::timeout(Duration::from_secs(2), worker.recv_msg::<MasterMessage>())
        .await
        .expect("worker must receive AssignTaskWithCheckpoint")
        .unwrap()
    {
        Some(MasterMessage::AssignTaskWithCheckpoint { task, .. }) => {
            assert_eq!(task.id, resumed_same_id)
        }
        other => panic!("expected AssignTaskWithCheckpoint, got {other:?}"),
    }
    worker
        .send_msg(&WorkerMessage::TaskResult {
            worker_id,
            task_id: resumed_same_id,
            exit_code: 0,
            stdout: Bytes::from_static(b"ok"),
            stderr: Bytes::new(),
            execution_time_ms: 5,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: vec![],
            outputs: vec![],
        })
        .await
        .unwrap();

    let fresh_resume_spec = TaskSpec::command("echo", vec!["resume-only".into()]);
    let fresh_resume_key = crate::speed::TaskKey::of(&fresh_resume_spec);
    let mut fresh_resume = Task::new(fresh_resume_spec, TaskRequirements::generic(1, 60));
    fresh_resume.latest_checkpoint = Some(CheckpointData::new(1, b"cp".to_vec()));
    let fresh_resume_id = fresh_resume.id;
    master.submit_task(fresh_resume).await.unwrap();

    match tokio::time::timeout(Duration::from_secs(2), worker.recv_msg::<MasterMessage>())
        .await
        .expect("worker must receive second AssignTaskWithCheckpoint")
        .unwrap()
    {
        Some(MasterMessage::AssignTaskWithCheckpoint { task, .. }) => {
            assert_eq!(task.id, fresh_resume_id)
        }
        other => panic!("expected AssignTaskWithCheckpoint, got {other:?}"),
    }
    worker
        .send_msg(&WorkerMessage::TaskResult {
            worker_id,
            task_id: fresh_resume_id,
            exit_code: 0,
            stdout: Bytes::from_static(b"ok"),
            stderr: Bytes::new(),
            execution_time_ms: 5,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: vec![],
            outputs: vec![],
        })
        .await
        .unwrap();

    let mut both_done = false;
    for _ in 0..100 {
        if master.get_task_info(resumed_same_id).await.unwrap().state == TaskState::Completed
            && master.get_task_info(fresh_resume_id).await.unwrap().state == TaskState::Completed
        {
            both_done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(both_done, "resumed tasks must complete");

    let guard = sb.lock().unwrap_or_else(|e| e.into_inner());
    let after_resume = guard
        .get(&worker_id, task_key)
        .expect("prior sample must still exist");
    assert_eq!(after_resume.samples, 1);
    assert!((after_resume.avg_ms - 120.0).abs() < 1e-9);
    assert!(
        guard.get(&worker_id, fresh_resume_key).is_none(),
        "resumed-only task key must not enter SpeedBook"
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_multi_client_fair_share_prevents_head_of_line_starvation() {
    use rusty_grid_core::task::{Bytes, TaskRequirements, TaskSpec};
    use std::collections::HashSet;

    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let addr = master.server_addr();

    let mut client_a = MessageTransport::new(TcpStream::connect(addr).await.unwrap());
    let mut client_b = MessageTransport::new(TcpStream::connect(addr).await.unwrap());

    let mut a_ids = HashSet::new();
    for i in 0..10 {
        let task = Task::new(
            TaskSpec::command("echo", vec![format!("a-{i}")]),
            TaskRequirements::generic(1, 60),
        );
        a_ids.insert(task.id);
        client_a
            .send_msg(&ClientMessage::SubmitTask { task, wait: false })
            .await
            .unwrap();
        match client_a.recv_msg::<ClientResponse>().await.unwrap() {
            Some(ClientResponse::TaskSubmitted { .. }) => {}
            other => panic!("expected TaskSubmitted for client_a, got {other:?}"),
        }
    }

    let mut b_ids = HashSet::new();
    for i in 0..2 {
        let task = Task::new(
            TaskSpec::command("echo", vec![format!("b-{i}")]),
            TaskRequirements::generic(1, 60),
        );
        b_ids.insert(task.id);
        client_b
            .send_msg(&ClientMessage::SubmitTask { task, wait: false })
            .await
            .unwrap();
        match client_b.recv_msg::<ClientResponse>().await.unwrap() {
            Some(ClientResponse::TaskSubmitted { .. }) => {}
            other => panic!("expected TaskSubmitted for client_b, got {other:?}"),
        }
    }

    let mut worker = MessageTransport::new(TcpStream::connect(addr).await.unwrap());
    let worker_id = Uuid::new_v4();
    worker
        .send_msg(&WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities::new("fair-worker", 2, 8192, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    match worker.recv_msg::<MasterMessage>().await.unwrap() {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("expected RegisterAck, got {other:?}"),
    }

    let mut assigned_from_a = Vec::new();
    let mut assigned_from_b = Vec::new();
    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(2), worker.recv_msg::<MasterMessage>())
            .await
            .expect("worker must receive initial 2 tasks")
            .unwrap()
        {
            Some(MasterMessage::AssignTask { task }) => {
                if a_ids.contains(&task.id) {
                    assigned_from_a.push(task.id);
                } else if b_ids.contains(&task.id) {
                    assigned_from_b.push(task.id);
                } else {
                    panic!("unexpected task assigned: {}", task.id);
                }
            }
            other => panic!("expected AssignTask, got {other:?}"),
        }
    }
    assert_eq!(
        assigned_from_a.len(),
        1,
        "first 2 assignments must interleave client A"
    );
    assert_eq!(
        assigned_from_b.len(),
        1,
        "first 2 assignments must interleave client B instead of starving behind A"
    );

    let b_done = assigned_from_b[0];
    worker
        .send_msg(&WorkerMessage::TaskResult {
            worker_id,
            task_id: b_done,
            exit_code: 0,
            stdout: Bytes::from_static(b"ok"),
            stderr: Bytes::new(),
            execution_time_ms: 5,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: vec![],
            outputs: vec![],
        })
        .await
        .unwrap();

    let mut b_completed = false;
    for _ in 0..100 {
        client_b
            .send_msg(&ClientMessage::GetTaskStatus { task_id: b_done })
            .await
            .unwrap();
        match client_b.recv_msg::<ClientResponse>().await.unwrap() {
            Some(ClientResponse::TaskStatusInfo {
                status: TaskStatus::Completed,
                ..
            }) => {
                b_completed = true;
                break;
            }
            Some(ClientResponse::TaskStatusInfo { .. }) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            other => panic!("unexpected GetTaskStatus reply: {other:?}"),
        }
    }
    assert!(b_completed, "client B task must complete cleanly");

    let _ = master.shutdown();
}
