use super::tasks_and_blobs::apply_task_result;
use super::*;
use rusty_grid_core::blob_transfer::{BlobReply, BlobRequest};
use rusty_grid_core::protocol::{
    ClientMessage, ClientResponse, InboundMessage, ProtocolCapabilities, ProtocolError,
    PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::Task;
use rusty_grid_core::{MasterMessage, MessageTransport, WorkerCapabilities, WorkerMessage};
use tokio::net::TcpStream;

use crate::queue::TaskState;

#[cfg(feature = "p2p")]
#[tokio::test]
async fn test_p2p_handshake_refusals_delivered_over_real_iroh() {
    use iroh::endpoint::presets::Minimal;
    use iroh::endpoint::{QuicTransportConfig, RelayMode, VarInt};
    use rusty_grid_core::arbitrage::{MasterLeaseTicket, LEASE_VALIDITY_DURATION_MS};
    use rusty_grid_core::cluster_auth::{
        client_handshake, ClusterSecret, AUTH_VERSION, REJECT_NO_SECRET, WIRE_FORMAT_AUTH,
    };
    use rusty_grid_core::protocol::EndpointRole;
    use rusty_grid_core::transport::{BiStream, GRID_ALPN, STREAM_CONTROL, STREAM_DATA};

    let test_secret = ClusterSecret::generate().unwrap();
    let signing_secret = test_secret.clone();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let claim_ticket = MasterLeaseTicket::issue(
        Uuid::new_v4(),
        Uuid::new_v4(),
        10,
        9500,
        now_ms,
        Duration::from_millis(LEASE_VALIDITY_DURATION_MS),
        &signing_secret,
    );

    for secret in [None, Some(test_secret.clone())] {
        let mut cfg =
            ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_pending_handshakes(1);
        cfg.cluster_secret = secret.clone();
        let mut server = MasterServer::bind(cfg, WorkerRegistry::new())
            .await
            .unwrap();
        let master_ep = iroh::Endpoint::builder(Minimal)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![GRID_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let master_addr = master_ep.addr();
        server.p2p_endpoint = Some(master_ep.clone());

        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let server_task = tokio::spawn(server.run(stop_rx));

        let client_ep = iroh::Endpoint::builder(Minimal)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![GRID_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();

        let open_raw_ctrl = || async {
            let conn = client_ep
                .connect(master_addr.clone(), GRID_ALPN)
                .await
                .expect("connect to master over loopback iroh");
            let (mut ctrl_send, ctrl_recv) = conn.open_bi().await.expect("open ctrl bi");
            ctrl_send.write_all(&[STREAM_CONTROL]).await.unwrap();
            let (mut data_send, data_recv) = conn.open_bi().await.expect("open data bi");
            data_send.write_all(&[STREAM_DATA]).await.unwrap();
            let transport =
                MessageTransport::new(GridStream::P2p(BiStream::new(ctrl_recv, ctrl_send)));
            (transport, (conn, data_send, data_recv))
        };

        let open_ctrl = || async {
            let (mut transport, keep) = open_raw_ctrl().await;
            if let Some(ref sec) = secret {
                let key = client_handshake(&mut transport, sec)
                    .await
                    .expect("client handshake over iroh");
                transport.enable_encryption(&key, EndpointRole::Client);
            }
            (transport, keep)
        };

        for half_close in [false, true] {
            for iter in 0..8 {
                // 1. ClaimMasterRole refusal on the P2P handshake path.
                let (mut ctrl, keep) = open_ctrl().await;
                ctrl.send_msg(&WorkerMessage::ClaimMasterRole {
                    ticket: claim_ticket.clone(),
                })
                .await
                .unwrap();
                if half_close {
                    tokio::io::AsyncWriteExt::shutdown(ctrl.get_mut())
                        .await
                        .unwrap();
                }
                let claim_reply = ctrl
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "iter {iter} (secret={}, half_close={half_close}): ClaimMasterRole refusal read failed: {e}",
                            secret.is_some()
                        )
                    })
                    .unwrap_or_else(|| {
                        panic!(
                            "iter {iter} (secret={}, half_close={half_close}): ClaimMasterRole refusal stream closed before frame arrived",
                            secret.is_some()
                        )
                    });
                match claim_reply {
                    MasterMessage::RoleHandoverAck {
                        accepted: false,
                        ref reason,
                        ref task_queue_snapshot,
                        ref connected_workers,
                    } => {
                        assert!(reason.is_some());
                        assert!(task_queue_snapshot.is_empty());
                        assert!(connected_workers.is_empty());
                    }
                    other => panic!("expected refused RoleHandoverAck, got {other:?}"),
                }
                assert!(
                    ctrl.recv_msg::<MasterMessage>().await.unwrap().is_none(),
                    "master must finish the control send stream after ClaimMasterRole refusal"
                );
                keep.0.close(0u32.into(), b"done");

                // 2. RegisterAck { accepted: false } refusal (cpu_cores == 0) on the P2P handshake path.
                let (mut ctrl_reg, keep_reg) = open_ctrl().await;
                ctrl_reg
                    .send_msg(&WorkerMessage::Register {
                        worker_id: Uuid::new_v4(),
                        capabilities: WorkerCapabilities::new(
                            "zero-core-worker",
                            0,
                            2048,
                            false,
                            false,
                            None,
                        ),
                        protocol_version: PROTOCOL_VERSION_CURRENT,
                        features: ProtocolCapabilities::all_supported(),
                    })
                    .await
                    .unwrap();
                if half_close {
                    tokio::io::AsyncWriteExt::shutdown(ctrl_reg.get_mut())
                        .await
                        .unwrap();
                }
                let reg_reply = ctrl_reg
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "iter {iter} (secret={}, half_close={half_close}): RegisterAck (cpu_cores=0) refusal read failed: {e}",
                            secret.is_some()
                        )
                    })
                    .unwrap_or_else(|| {
                        panic!(
                            "iter {iter} (secret={}, half_close={half_close}): RegisterAck (cpu_cores=0) refusal stream closed before frame arrived",
                            secret.is_some()
                        )
                    });
                match reg_reply {
                    MasterMessage::RegisterAck {
                        accepted: false,
                        ref message,
                        ..
                    } => {
                        assert!(
                            message
                                .as_deref()
                                .unwrap_or_default()
                                .contains("cpu_cores must be > 0"),
                            "unexpected refusal message: {message:?}"
                        );
                    }
                    other => panic!("expected refused RegisterAck, got {other:?}"),
                }
                assert!(
                    ctrl_reg
                        .recv_msg::<MasterMessage>()
                        .await
                        .unwrap()
                        .is_none(),
                    "master must finish the control send stream after cpu_cores=0 refusal"
                );
                keep_reg.0.close(0u32.into(), b"done");

                // 3. RegisterAck { accepted: false } refusal (incompatible protocol_version) on P2P handshake path.
                let (mut ctrl_ver, keep_ver) = open_ctrl().await;
                ctrl_ver
                    .send_msg(&WorkerMessage::Register {
                        worker_id: Uuid::new_v4(),
                        capabilities: WorkerCapabilities::new(
                            "old-proto-worker",
                            4,
                            2048,
                            false,
                            false,
                            None,
                        ),
                        protocol_version: 0,
                        features: ProtocolCapabilities::all_supported(),
                    })
                    .await
                    .unwrap();
                if half_close {
                    tokio::io::AsyncWriteExt::shutdown(ctrl_ver.get_mut())
                        .await
                        .unwrap();
                }
                let ver_reply = ctrl_ver
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "iter {iter} (secret={}, half_close={half_close}): RegisterAck (protocol_version=0) refusal read failed: {e}",
                            secret.is_some()
                        )
                    })
                    .unwrap_or_else(|| {
                        panic!(
                            "iter {iter} (secret={}, half_close={half_close}): RegisterAck (protocol_version=0) refusal stream closed before frame arrived",
                            secret.is_some()
                        )
                    });
                match ver_reply {
                    MasterMessage::RegisterAck {
                        accepted: false,
                        ref message,
                        ..
                    } => {
                        assert!(
                            message
                                .as_deref()
                                .unwrap_or_default()
                                .contains("Incompatible protocol version"),
                            "unexpected refusal message: {message:?}"
                        );
                    }
                    other => panic!("expected refused RegisterAck, got {other:?}"),
                }
                assert!(
                    ctrl_ver
                        .recv_msg::<MasterMessage>()
                        .await
                        .unwrap()
                        .is_none(),
                    "master must finish the control send stream after protocol_version=0 refusal"
                );
                keep_ver.0.close(0u32.into(), b"done");

                // 4. RegisterAck { accepted: false } refusal (unexpected first WorkerMessage) on P2P handshake path.
                let (mut ctrl_other, keep_other) = open_ctrl().await;
                ctrl_other
                    .send_msg(&WorkerMessage::Heartbeat {
                        worker_id: Uuid::new_v4(),
                        timestamp: 1,
                        active_tasks: 0,
                        cpu_usage_pct: 0.0,
                        ram_available_mb: 1024,
                    })
                    .await
                    .unwrap();
                if half_close {
                    tokio::io::AsyncWriteExt::shutdown(ctrl_other.get_mut())
                        .await
                        .unwrap();
                }
                let other_reply = ctrl_other
                    .recv_msg::<MasterMessage>()
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "iter {iter} (secret={}, half_close={half_close}): RegisterAck (unexpected Heartbeat) refusal read failed: {e}",
                            secret.is_some()
                        )
                    })
                    .unwrap_or_else(|| {
                        panic!(
                            "iter {iter} (secret={}, half_close={half_close}): RegisterAck (unexpected Heartbeat) refusal stream closed before frame arrived",
                            secret.is_some()
                        )
                    });
                match other_reply {
                    MasterMessage::RegisterAck {
                        accepted: false,
                        ref message,
                        ..
                    } => {
                        assert!(
                            message
                                .as_deref()
                                .unwrap_or_default()
                                .contains("Expected Register message, received Heartbeat"),
                            "unexpected refusal message: {message:?}"
                        );
                    }
                    other => panic!("expected refused RegisterAck, got {other:?}"),
                }
                assert!(
                    ctrl_other
                        .recv_msg::<MasterMessage>()
                        .await
                        .unwrap()
                        .is_none(),
                    "master must finish the control send stream after unexpected Heartbeat refusal"
                );
                keep_other.0.close(0u32.into(), b"done");

                // 5. Cluster-auth refusal on the P2P handshake path (8 of 8 in both normal and half-close modes):
                //    - When `secret` is `None`: a client sending an auth HELLO receives `REJECT_NO_SECRET`.
                //    - When `secret` is `Some`: an unauthenticated client sending `Register` receives
                //      `REJECT_AUTH_REQUIRED` (`InvalidFormatTag(WIRE_FORMAT_AUTH)` -> `GridError::AuthRequired`).
                let (mut ctrl_auth, keep_auth) = open_raw_ctrl().await;
                if secret.is_none() {
                    if !half_close {
                        let err = client_handshake(&mut ctrl_auth, &test_secret)
                            .await
                            .expect_err("master without secret must reject client_handshake");
                        match err {
                            GridError::AuthFailed(ref msg)
                                if msg.contains("no cluster secret configured") => {}
                            other => panic!(
                                "iter {iter} (half_close={half_close}): expected AuthFailed(no cluster secret configured), got {other:?}"
                            ),
                        }
                    } else {
                        let mut hello_frame =
                            vec![0, 0, 0, 35, WIRE_FORMAT_AUTH, 0x01, AUTH_VERSION];
                        hello_frame.extend_from_slice(&[7u8; 32]);
                        tokio::io::AsyncWriteExt::write_all(ctrl_auth.get_mut(), &hello_frame)
                            .await
                            .unwrap();
                        tokio::io::AsyncWriteExt::shutdown(ctrl_auth.get_mut())
                            .await
                            .unwrap();
                        let reject_frame = ctrl_auth
                            .recv_raw_frame_max(1024)
                            .await
                            .unwrap()
                            .expect("REJECT_NO_SECRET frame must arrive after half-close");
                        assert_eq!(reject_frame[0], WIRE_FORMAT_AUTH);
                        assert_eq!(reject_frame[1], 0x05);
                        assert_eq!(&reject_frame[2..], REJECT_NO_SECRET.as_bytes());
                    }
                    assert!(
                        ctrl_auth.recv_raw_frame_max(1024).await.unwrap().is_none(),
                        "master must finish the control send stream after REJECT_NO_SECRET"
                    );
                } else {
                    ctrl_auth
                        .send_msg(&WorkerMessage::Register {
                            worker_id: Uuid::new_v4(),
                            capabilities: WorkerCapabilities::new(
                                "unauth-worker",
                                2,
                                2048,
                                false,
                                false,
                                None,
                            ),
                            protocol_version: PROTOCOL_VERSION_CURRENT,
                            features: ProtocolCapabilities::all_supported(),
                        })
                        .await
                        .unwrap();
                    if half_close {
                        tokio::io::AsyncWriteExt::shutdown(ctrl_auth.get_mut())
                            .await
                            .unwrap();
                    }
                    let err = ctrl_auth
                        .recv_msg::<MasterMessage>()
                        .await
                        .expect_err("unauthenticated Register on secret master must be rejected");
                    assert!(
                        matches!(err, ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH)),
                        "iter {iter} (half_close={half_close}): expected InvalidFormatTag(WIRE_FORMAT_AUTH), got {err:?}"
                    );
                    let grid_err: GridError = err.into();
                    assert!(matches!(grid_err, GridError::AuthRequired(_)));
                    assert!(
                        ctrl_auth.recv_raw_frame_max(1024).await.unwrap().is_none(),
                        "master must finish the control send stream after REJECT_AUTH_REQUIRED"
                    );
                }
                keep_auth.0.close(0u32.into(), b"done");
            }
        }

        // 6. Silent peer check + `max_pending_handshakes(1)` release during auth-refusal drain:
        //    - While a first peer that failed cluster auth is still open inside
        //      `drain_refusal_stream` (holding its connection open without closing `conn`),
        //      its `handshake_permit` must already be dropped so a second peer is admitted
        //      under `max_pending_handshakes(1)`.
        //    - When a refused peer reads the refusal frame and stays silent without closing
        //      `conn`, the master keeps `conn` open for `REFUSAL_DRAIN_TIMEOUT` (`1 s`) and
        //      closes it between 0.9 s and 3.0 s.
        {
            let (mut ctrl_auth_drain, keep_auth_drain) = open_raw_ctrl().await;
            if secret.is_none() {
                let err = client_handshake(&mut ctrl_auth_drain, &test_secret)
                    .await
                    .expect_err("master without secret must reject client_handshake");
                assert!(matches!(err, GridError::AuthFailed(_)));
            } else {
                ctrl_auth_drain
                    .send_msg(&WorkerMessage::Register {
                        worker_id: Uuid::new_v4(),
                        capabilities: WorkerCapabilities::new(
                            "unauth-drain-holder",
                            2,
                            2048,
                            false,
                            false,
                            None,
                        ),
                        protocol_version: PROTOCOL_VERSION_CURRENT,
                        features: ProtocolCapabilities::all_supported(),
                    })
                    .await
                    .unwrap();
                let err = ctrl_auth_drain
                    .recv_msg::<MasterMessage>()
                    .await
                    .expect_err("unauthenticated Register must be rejected");
                assert!(matches!(
                    err,
                    ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH)
                ));
            }
            // `keep_auth_drain.0` is intentionally kept open so the master task is parked inside
            // `drain_refusal_stream` waiting up to 1 s for `conn.closed()`. With
            // `max_pending_handshakes(1)`, a second peer connecting right now is admitted only
            // if `drop(handshake_permit)` happened before `drain_refusal_stream`.
            let (mut ctrl_silent, keep_silent) = open_ctrl().await;
            let started = tokio::time::Instant::now();
            ctrl_silent
                .send_msg(&WorkerMessage::ClaimMasterRole {
                    ticket: claim_ticket.clone(),
                })
                .await
                .unwrap();
            let reply = ctrl_silent
                .recv_msg::<MasterMessage>()
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                reply,
                MasterMessage::RoleHandoverAck {
                    accepted: false,
                    ..
                }
            ));
            keep_auth_drain.0.close(0u32.into(), b"done");
            tokio::time::timeout(Duration::from_secs(5), keep_silent.0.closed())
                .await
                .expect("silent refused peer must be closed before 5 s timeout");
            let elapsed = started.elapsed();
            assert!(
                elapsed >= Duration::from_millis(900) && elapsed <= Duration::from_secs(3),
                "silent refused peer must be closed after ~1 s (0.9..=3.0 s), took {elapsed:?}"
            );
        }

        let _ = stop_tx.send(true);
        client_ep.close().await;
        master_ep.close().await;
        let _ = server_task.await;
    }

    // 7. Zero flow-control credit peer with `max_connections = 1`:
    //    A peer whose QUIC stream receive window is 1 byte (smaller than the refusal frame)
    //    causes `transport.send_msg` on the master to block on flow-control credit. The single
    //    1 s timeout around `send_msg`, `shutdown`, and `conn.closed()` aborts the stalled
    //    refusal within 3.0 s and frees the single connection slot.
    {
        let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_connections(1);
        let mut server = MasterServer::bind(cfg, WorkerRegistry::new())
            .await
            .unwrap();
        let master_ep = iroh::Endpoint::builder(Minimal)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![GRID_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let master_addr = master_ep.addr();
        server.p2p_endpoint = Some(master_ep.clone());

        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let server_task = tokio::spawn(server.run(stop_rx));

        let tiny_window_quic = QuicTransportConfig::builder()
            .stream_receive_window(VarInt::from_u32(1))
            .build();
        let stalled_ep = iroh::Endpoint::builder(Minimal)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![GRID_ALPN.to_vec()])
            .transport_config(tiny_window_quic)
            .bind()
            .await
            .unwrap();

        let stalled_conn = stalled_ep
            .connect(master_addr.clone(), GRID_ALPN)
            .await
            .unwrap();
        let (mut c_send, c_recv) = stalled_conn.open_bi().await.unwrap();
        c_send.write_all(&[STREAM_CONTROL]).await.unwrap();
        let (mut d_send, _d_recv) = stalled_conn.open_bi().await.unwrap();
        d_send.write_all(&[STREAM_DATA]).await.unwrap();
        let mut stalled_ctrl =
            MessageTransport::new(GridStream::P2p(BiStream::new(c_recv, c_send)));
        let started = tokio::time::Instant::now();
        stalled_ctrl
            .send_msg(&WorkerMessage::ClaimMasterRole {
                ticket: claim_ticket.clone(),
            })
            .await
            .unwrap();
        // Do not read from `stalled_ctrl`; its 1-byte stream receive window blocks `send_msg`.
        tokio::time::timeout(Duration::from_secs(5), stalled_conn.closed())
            .await
            .expect("peer granting no flow-control credit must be closed before 5 s timeout");
        let elapsed = started.elapsed();
        assert!(
            elapsed <= Duration::from_secs(3),
            "peer granting no flow-control credit must be closed within 3.0 s, took {elapsed:?}"
        );

        // Now `max_connections = 1` slot is free again: a normal peer must be admitted.
        let normal_ep = iroh::Endpoint::builder(Minimal)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![GRID_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let normal_conn = normal_ep.connect(master_addr, GRID_ALPN).await.unwrap();
        let (mut nc_send, nc_recv) = normal_conn.open_bi().await.unwrap();
        nc_send.write_all(&[STREAM_CONTROL]).await.unwrap();
        let (mut nd_send, _nd_recv) = normal_conn.open_bi().await.unwrap();
        nd_send.write_all(&[STREAM_DATA]).await.unwrap();
        let mut normal_ctrl =
            MessageTransport::new(GridStream::P2p(BiStream::new(nc_recv, nc_send)));
        normal_ctrl
            .send_msg(&WorkerMessage::ClaimMasterRole {
                ticket: claim_ticket,
            })
            .await
            .unwrap();
        let reply = normal_ctrl
            .recv_msg::<MasterMessage>()
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            reply,
            MasterMessage::RoleHandoverAck {
                accepted: false,
                ..
            }
        ));
        normal_conn.close(0u32.into(), b"done");

        let _ = stop_tx.send(true);
        stalled_ep.close().await;
        normal_ep.close().await;
        master_ep.close().await;
        let _ = server_task.await;
    }
}

#[test]
fn test_master_spawn_warns_when_p2p_minimal_preset_is_enabled_and_ignores_env() {
    #[derive(Default)]
    struct WarnVisitor(String);
    impl tracing::field::Visit for WarnVisitor {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                self.0.push_str(value);
            }
        }
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                use std::fmt::Write;
                let _ = write!(&mut self.0, "{value:?}");
            }
        }
    }
    struct WarnCollector {
        warnings: Arc<std::sync::Mutex<Vec<String>>>,
        thread_id: std::thread::ThreadId,
    }
    impl tracing::Subscriber for WarnCollector {
        fn register_callsite(
            &self,
            metadata: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            if *metadata.level() <= tracing::Level::WARN {
                tracing::subscriber::Interest::always()
            } else {
                tracing::subscriber::Interest::never()
            }
        }
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() <= tracing::Level::WARN
        }
        fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
            Some(tracing::metadata::LevelFilter::WARN)
        }
        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == tracing::Level::WARN
                && std::thread::current().id() == self.thread_id
            {
                let mut v = WarnVisitor::default();
                event.record(&mut v);
                self.warnings.lock().unwrap().push(v.0);
            }
        }
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    let warnings: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let thread_id = std::thread::current().id();
    let dispatch = tracing::Dispatch::new(WarnCollector {
        warnings: warnings.clone(),
        thread_id,
    });
    let _second_dispatch = tracing::Dispatch::new(WarnCollector {
        warnings: warnings.clone(),
        thread_id,
    });

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tracing::dispatcher::with_default(&dispatch, || {
        rt.block_on(async {
            tracing::callsite::rebuild_interest_cache();
            let default_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap());
            assert!(!default_cfg.p2p_minimal_preset);
            let m1 = MasterServer::spawn(default_cfg).await.expect("spawn m1");
            assert!(
                !warnings
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|w| w.contains("P2P minimal preset is enabled (p2p_minimal_preset=true)")),
                "without with_p2p_minimal_preset(true), MasterServer::spawn must not emit the minimal preset warning"
            );
            let _ = m1.shutdown();

            warnings.lock().unwrap().clear();
            tracing::callsite::rebuild_interest_cache();
            let p2p_off_min_cfg =
                ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_p2p_minimal_preset(true);
            let m_off = MasterServer::spawn(p2p_off_min_cfg)
                .await
                .expect("spawn m_off");
            assert!(
                !warnings
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|w| w
                        .contains("P2P minimal preset is enabled (p2p_minimal_preset=true)")),
                "when P2P is off, MasterServer::spawn must not warn about P2P minimal preset"
            );
            let _ = m_off.shutdown();

            warnings.lock().unwrap().clear();
            tracing::callsite::rebuild_interest_cache();
            let min_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
                .with_p2p(true)
                .with_p2p_minimal_preset(true)
                .with_ephemeral_key(true);
            let m2 = MasterServer::spawn(min_cfg).await.expect("spawn m2");
            assert!(
                warnings
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|w| w.contains("P2P minimal preset is enabled (p2p_minimal_preset=true)")),
                "with_p2p_minimal_preset(true) must emit a tracing::warn! on MasterServer::spawn, got: {:?}",
                *warnings.lock().unwrap()
            );
            let _ = m2.shutdown();
        });
    });

    let server_source = concat!(
        include_str!("../server.rs"),
        "\n",
        include_str!("tasks_and_blobs.rs"),
        "\n",
        include_str!("client_and_members.rs"),
        "\n",
        include_str!("handshakes.rs"),
    )
    .split("#[cfg(test)]")
    .next()
    .unwrap();
    assert!(
        !server_source
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .any(|l| l.contains("RUSTY_GRID_P2P_MINIMAL")),
        "crates/master/src/server.rs library code must not read RUSTY_GRID_P2P_MINIMAL from the environment"
    );
}

#[tokio::test]
async fn test_client_and_worker_protocol_version_mismatch_refused_on_both_sides() {
    fn stamp_frame_version<M: serde::Serialize>(
        msg: &M,
        version: u32,
        codec: WireCodec,
    ) -> Vec<u8> {
        let mut raw = rusty_grid_core::protocol::serialize_message(msg, codec)
            .unwrap()
            .to_vec();
        match codec {
            WireCodec::Bincode => {
                let len = raw.len();
                raw[len - 4..].copy_from_slice(&version.to_le_bytes());
            }
            WireCodec::Json => {
                let mut val: serde_json::Value = serde_json::from_slice(&raw[1..]).unwrap();
                val["protocol_version"] = serde_json::Value::from(version);
                raw.truncate(1);
                serde_json::to_writer(&mut raw, &val).unwrap();
            }
        }
        raw
    }

    let server = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("spawn master");
    let addr = server.server_addr;

    // 1. Worker with lower (2, current - 1) or higher (current + 1) protocol_version in both
    // Bincode and JSON is refused by Master with a clear RegisterAck message.
    for codec in [WireCodec::Bincode, WireCodec::Json] {
        for bad_worker_ver in [
            2u32,
            PROTOCOL_VERSION_CURRENT - 1,
            PROTOCOL_VERSION_CURRENT + 1,
        ] {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut transport = MessageTransport::with_codec(stream, codec);
            transport
                .send_msg(&WorkerMessage::Register {
                    worker_id: Uuid::new_v4(),
                    capabilities: WorkerCapabilities::new(
                        "skew-worker",
                        4,
                        4096,
                        false,
                        false,
                        None,
                    ),
                    protocol_version: bad_worker_ver,
                    features: ProtocolCapabilities::all_supported(),
                })
                .await
                .unwrap();
            let ack = transport
                .recv_msg::<MasterMessage>()
                .await
                .unwrap()
                .expect("RegisterAck");
            match ack {
                MasterMessage::RegisterAck {
                    accepted,
                    protocol_version,
                    message,
                    ..
                } => {
                    assert!(!accepted);
                    assert_eq!(protocol_version, PROTOCOL_VERSION_CURRENT);
                    let msg = message.unwrap_or_default();
                    assert!(
                        msg.contains(&format!("Incompatible protocol version {bad_worker_ver}"))
                            && msg.contains(&PROTOCOL_VERSION_CURRENT.to_string()),
                        "expected version mismatch message for {codec:?}, got: {msg}"
                    );
                }
                other => panic!("expected RegisterAck, got {other:?}"),
            }
        }
    }

    // 2. Client with lower (2, current - 1) or higher (current + 1) protocol_version in both
    // Bincode and JSON receives a clear ClientResponse::Error from the Master, and a client
    // reading a mismatched ClientResponse from a fake Master also refuses with
    // GridError::IncompatibleVersion.
    for codec in [WireCodec::Bincode, WireCodec::Json] {
        for bad_client_ver in [
            2u32,
            PROTOCOL_VERSION_CURRENT - 1,
            PROTOCOL_VERSION_CURRENT + 1,
        ] {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut transport = MessageTransport::with_codec(stream, codec);
            let raw_frame =
                stamp_frame_version(&ClientMessage::ClusterStatus, bad_client_ver, codec);
            transport.send_raw_frame(raw_frame.into()).await.unwrap();

            let reply = transport
                .recv_msg::<ClientResponse>()
                .await
                .expect("client must decode Master's Error response cleanly")
                .expect("ClientResponse::Error frame");
            match reply {
                ClientResponse::Error { message } => {
                    assert!(
                        message
                            .contains(&format!("Incompatible protocol version {bad_client_ver}"))
                            && message.contains(&PROTOCOL_VERSION_CURRENT.to_string()),
                        "expected clear version error from Master, got: {message}"
                    );
                }
                other => panic!("expected ClientResponse::Error, got {other:?}"),
            }
        }

        // Client side: receiving a non-Error ClientResponse with an incompatible protocol_version
        // fails with GridError::IncompatibleVersion naming both versions (without a
        // "Registration handshake failed:" prefix).
        for bad_master_ver in [PROTOCOL_VERSION_CURRENT - 1, PROTOCOL_VERSION_CURRENT + 2] {
            let (client_io, master_io) = tokio::io::duplex(4096);
            tokio::spawn(async move {
                let mut fake_master = MessageTransport::with_codec(master_io, codec);
                let _ = fake_master
                    .recv_msg::<InboundMessage>()
                    .await
                    .unwrap()
                    .unwrap();
                let bad_resp = stamp_frame_version(
                    &ClientResponse::WorkerList {
                        workers: Vec::new(),
                    },
                    bad_master_ver,
                    codec,
                );
                fake_master.send_raw_frame(bad_resp.into()).await.unwrap();
            });
            let mut client_transport = MessageTransport::with_codec(client_io, codec);
            client_transport
                .send_msg(&ClientMessage::ListWorkers)
                .await
                .unwrap();
            let err: GridError = client_transport
                .recv_msg::<ClientResponse>()
                .await
                .expect_err("client must refuse incompatible Master ClientResponse")
                .into();
            assert!(
                !err.to_string().contains("Registration handshake failed:"),
                "client refusal must not carry worker handshake prefix: {err}"
            );
            match &err {
                GridError::IncompatibleVersion(msg) => {
                    assert!(
                        msg.contains(&format!("Incompatible protocol version {bad_master_ver}"))
                            && msg.contains(&PROTOCOL_VERSION_CURRENT.to_string()),
                        "expected clear client-side version refusal, got: {msg}"
                    );
                }
                other => panic!("expected GridError::IncompatibleVersion, got {other:?}"),
            }
        }
    }

    let _ = server.shutdown();
}

#[tokio::test]
async fn test_concurrent_submissions_share_group_commit_and_recover_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let handle = MasterServer::spawn(config).await.unwrap();

    let total_tasks = 1_000usize;
    let start = std::time::Instant::now();
    let mut join_set = tokio::task::JoinSet::new();
    for i in 0..total_tasks {
        let h = handle.clone();
        join_set.spawn(async move {
            let task = Task::new(
                rusty_grid_core::TaskSpec::command("echo", vec![format!("task-{i}")]),
                rusty_grid_core::TaskRequirements::generic(1, 10),
            );
            h.submit_task(task).await.unwrap()
        });
    }
    let mut submitted_ids = Vec::with_capacity(total_tasks);
    while let Some(res) = join_set.join_next().await {
        submitted_ids.push(res.unwrap());
    }
    let elapsed = start.elapsed();
    let syncs = handle.queue().store_sync_count().await.unwrap_or(0);
    // TEST-28: no wall-clock bound here (it flaked on slow CI runners). Batching is proven by the
    // fsync counter below, which does not depend on runner speed; the hang bound is generous.
    assert!(
        elapsed < Duration::from_secs(60),
        "1,000 concurrent durable submissions took {elapsed:?}"
    );
    assert!(
        syncs > 0 && syncs < 200,
        "expected group commit to batch 1,000 submissions into < 200 syncs, got {syncs}"
    );

    handle.shutdown().unwrap();

    // Reopen MasterServer on the same state_dir and verify all 1,000 tasks are recovered.
    let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let handle2 = MasterServer::spawn(config2).await.unwrap();
    let stats = handle2.queue_stats().await.unwrap();
    assert_eq!(stats.total, total_tasks);
    assert_eq!(stats.queued, total_tasks);
    for id in submitted_ids.iter().take(16) {
        assert_eq!(handle2.queue().get_state(id).await, Some(TaskState::Queued));
    }
    handle2.shutdown().unwrap();
}

#[tokio::test]
async fn test_state_dir_repins_input_and_output_blobs_across_restart() {
    use rusty_grid_core::blob::{BlobFile, BlobId, TaskInput};
    use rusty_grid_core::blob_transfer::{upload_blob, BlobChannel, BlobError};

    struct StoreConn {
        store: Arc<BlobStore>,
        owner: u64,
    }
    impl BlobChannel for StoreConn {
        async fn call(&self, request: BlobRequest) -> Result<BlobReply, BlobError> {
            Ok(self.store.handle(self.owner, request).await)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let blob_dir = state_dir.join("blobs");

    let input_bytes = vec![b'I'; 1024];
    let output_bytes = vec![b'O'; 1024];
    let unpinned_bytes = vec![b'U'; 512];
    let filler_bytes = vec![b'F'; 1536];
    let input_id = BlobId::of(&input_bytes);
    let output_id = BlobId::of(&output_bytes);
    let unpinned_id = BlobId::of(&unpinned_bytes);
    let filler_id = BlobId::of(&filler_bytes);

    let (queued_task_id, completed_task_id) = {
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir)
            .with_blob_store(&blob_dir, 2560);
        let handle = MasterServer::spawn(config).await.unwrap();
        let bstore = handle.blob_store.as_ref().unwrap();
        let conn = StoreConn {
            store: Arc::clone(bstore),
            owner: bstore.new_owner(),
        };

        upload_blob(&conn, input_id, input_bytes.len() as u64, &input_bytes[..])
            .await
            .unwrap();
        upload_blob(
            &conn,
            output_id,
            output_bytes.len() as u64,
            &output_bytes[..],
        )
        .await
        .unwrap();
        upload_blob(
            &conn,
            unpinned_id,
            unpinned_bytes.len() as u64,
            &unpinned_bytes[..],
        )
        .await
        .unwrap();

        let mut queued_task = Task::new(
            rusty_grid_core::TaskSpec::command("echo", vec!["in".into()]),
            rusty_grid_core::TaskRequirements::generic(1, 10),
        );
        queued_task.inputs.push(TaskInput::file(
            "input.bin",
            input_id,
            input_bytes.len() as u64,
            false,
        ));
        let queued_task_id = handle.submit_task(queued_task).await.unwrap();

        let completed_task = Task::new(
            rusty_grid_core::TaskSpec::command("echo", vec!["out".into()]),
            rusty_grid_core::TaskRequirements::generic(1, 10),
        );
        let completed_task_id = handle.submit_task(completed_task).await.unwrap();
        let worker_id = Uuid::new_v4();
        handle
            .queue()
            .schedule_tasks_batch_in_sessions(&[(completed_task_id, worker_id, 1)])
            .await;
        let mut result = TaskResult::success(worker_id, completed_task_id, "ok", 5, false);
        result.outputs.push(BlobFile {
            path: "output.bin".into(),
            id: output_id,
            size: output_bytes.len() as u64,
            executable: false,
        });
        let pin = bstore.check_and_pin_outputs(&[output_id]).unwrap();
        handle
            .queue()
            .record_result_from(worker_id, Some(1), result)
            .await
            .unwrap();
        pin.commit_terminal(completed_task_id).await;

        handle.shutdown().unwrap();
        (queued_task_id, completed_task_id)
    };

    // Restart master on the same state_dir with the same 2560-byte blob cap.
    // Because `repin_recovered_blobs` re-pins `input_id` and `output_id` (2048 B total),
    // attempting to upload a 1536-byte filler blob must fail to evict either pinned blob.
    let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir)
        .with_blob_store(&blob_dir, 2560);
    let handle2 = MasterServer::spawn(config2).await.unwrap();
    let bstore2 = handle2.blob_store.as_ref().unwrap();
    let conn2 = StoreConn {
        store: Arc::clone(bstore2),
        owner: bstore2.new_owner(),
    };

    assert_eq!(bstore2.size_of(&input_id), Some(input_bytes.len() as u64));
    assert_eq!(bstore2.size_of(&output_id), Some(output_bytes.len() as u64));
    assert_eq!(
        bstore2.size_of(&unpinned_id),
        Some(unpinned_bytes.len() as u64)
    );
    assert_eq!(bstore2.stats().pinned_blobs, 2);

    upload_blob(
        &conn2,
        filler_id,
        filler_bytes.len() as u64,
        &filler_bytes[..],
    )
    .await
    .unwrap();

    assert_eq!(
        bstore2.size_of(&unpinned_id),
        None,
        "unpinned blob should be evicted first when uploading filler blob"
    );
    assert_eq!(
        bstore2.size_of(&input_id),
        Some(input_bytes.len() as u64),
        "pinned recovered input blob must survive LRU eviction"
    );
    assert_eq!(
        bstore2.size_of(&output_id),
        Some(output_bytes.len() as u64),
        "pinned recovered output blob must survive LRU eviction"
    );
    assert_eq!(
        handle2.queue().get_state(&queued_task_id).await,
        Some(TaskState::Queued)
    );
    assert_eq!(
        handle2.queue().get_state(&completed_task_id).await,
        Some(TaskState::Completed)
    );
    handle2.shutdown().unwrap();
}

#[tokio::test]
async fn test_storage_failure_on_result_releases_worker_slot_unpins_blobs_and_wakes_waiters() {
    use rusty_grid_core::blob::{BlobId, TaskInput};
    use rusty_grid_core::blob_transfer::{upload_blob, BlobChannel, BlobError};

    struct StoreConn {
        store: Arc<BlobStore>,
        owner: u64,
    }
    impl BlobChannel for StoreConn {
        async fn call(&self, request: BlobRequest) -> Result<BlobReply, BlobError> {
            Ok(self.store.handle(self.owner, request).await)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let blob_dir = state_dir.join("blobs");
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir)
        .with_blob_store(&blob_dir, 64 * 1024);
    let handle = MasterServer::spawn(config).await.unwrap();
    let bstore = handle.blob_store.as_ref().unwrap();
    let conn = StoreConn {
        store: Arc::clone(bstore),
        owner: bstore.new_owner(),
    };

    let input_bytes = vec![b'X'; 512];
    let input_id = BlobId::of(&input_bytes);
    upload_blob(&conn, input_id, input_bytes.len() as u64, &input_bytes[..])
        .await
        .unwrap();

    let worker_id = Uuid::new_v4();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let session_id = handle
        .registry()
        .register(
            worker_id,
            WorkerCapabilities::new("slot-worker", 4, 4096, false, false, None),
            "127.0.0.1:9999".parse().unwrap(),
            tx,
            None,
        )
        .await
        .unwrap();

    let mut task = Task::new(
        rusty_grid_core::TaskSpec::command("echo", vec!["hi".into()]),
        rusty_grid_core::TaskRequirements::generic(1, 10),
    );
    task.inputs.push(TaskInput::file(
        "input.bin",
        input_id,
        input_bytes.len() as u64,
        false,
    ));
    let task_id = handle.submit_task(task).await.unwrap();
    assert_eq!(bstore.stats().pinned_blobs, 1);

    let (waiter_tx, waiter_rx) = tokio::sync::oneshot::channel();
    handle
        .waiters
        .write()
        .await
        .entry(task_id)
        .or_default()
        .push(waiter_tx);

    handle
        .queue()
        .schedule_tasks_batch_in_sessions(&[(task_id, worker_id, session_id)])
        .await;
    handle
        .registry()
        .increment_active_tasks(&worker_id)
        .await
        .unwrap();
    assert_eq!(
        handle
            .registry()
            .get_worker(worker_id)
            .await
            .unwrap()
            .active_tasks,
        1
    );

    // Inject storage sync failure before the result arrives.
    handle.queue().inject_store_sync_failure(1).await;

    apply_task_result(
        handle.registry(),
        handle.queue(),
        handle.scheduler_notify(),
        &handle.waiters,
        worker_id,
        Some(session_id),
        TaskResult::success(worker_id, task_id, "done", 3, false),
        handle.blob_store.as_ref(),
        #[cfg(feature = "dashboard")]
        None,
    )
    .await;

    let waited = tokio::time::timeout(Duration::from_secs(1), waiter_rx)
        .await
        .expect("waiter must be woken even when TaskStore sync fails")
        .expect("waiter channel must receive result");
    assert_eq!(waited.exit_code, 0);
    assert_eq!(
        handle
            .registry()
            .get_worker(worker_id)
            .await
            .unwrap()
            .active_tasks,
        0,
        "worker slot must be released even when TaskStore sync fails"
    );
    assert_eq!(
        bstore.stats().pinned_blobs,
        0,
        "task input blobs must be unpinned even when TaskStore sync fails"
    );
    handle.shutdown().unwrap();
}

#[tokio::test]
async fn test_second_master_refused_on_locked_state_dir_leaves_blobs_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let config1 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let handle1 = MasterServer::spawn(config1).await.unwrap();

    // Simulate an in-flight upload file inside `<state_dir>/blobs/tmp/` on the running master.
    let staged_tmp = state_dir.join("blobs").join("tmp").join("upload-1.part");
    std::fs::create_dir_all(staged_tmp.parent().unwrap()).unwrap();
    std::fs::write(&staged_tmp, b"in-flight-upload-bytes").unwrap();
    assert!(staged_tmp.exists());

    // Starting a second master on the same `--state-dir` must fail on `store.lock` BEFORE
    // `BlobStore::open` runs and purges `blobs/tmp/`.
    let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let Err(err) = MasterServer::spawn(config2).await else {
        panic!("expected second MasterServer::spawn on locked state_dir to fail");
    };
    assert!(
        err.to_string().contains("store.lock"),
        "expected store.lock refusal on locked state_dir, got {err:?}"
    );
    assert!(
        staged_tmp.exists(),
        "second master refused on locked state_dir must not purge running master's blobs/tmp/ files"
    );
    assert_eq!(
        std::fs::read(&staged_tmp).unwrap(),
        b"in-flight-upload-bytes"
    );

    handle1.shutdown().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_worker_session_50_results_batch_in_group_commit() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let handle = MasterServer::spawn(config).await.unwrap();

    let worker_id = Uuid::new_v4();
    let (tx, _rx) = tokio::sync::mpsc::channel(128);
    let session_id = handle
        .registry()
        .register(
            worker_id,
            WorkerCapabilities::new("batch-worker", 64, 65536, false, false, None),
            "127.0.0.1:9998".parse().unwrap(),
            tx,
            None,
        )
        .await
        .unwrap();

    let count = 50usize;
    let mut task_ids = Vec::with_capacity(count);
    let mut waiter_rxs = Vec::with_capacity(count);
    for i in 0..count {
        let t = Task::new(
            rusty_grid_core::TaskSpec::command("echo", vec![format!("r-{i}")]),
            rusty_grid_core::TaskRequirements::generic(1, 10),
        );
        let tid = handle.submit_task(t).await.unwrap();
        let (wtx, wrx) = tokio::sync::oneshot::channel();
        handle
            .waiters
            .write()
            .await
            .entry(tid)
            .or_default()
            .push(wtx);
        task_ids.push(tid);
        waiter_rxs.push(wrx);
    }

    let assign: Vec<_> = task_ids
        .iter()
        .map(|tid| (*tid, worker_id, session_id))
        .collect();
    handle
        .queue()
        .schedule_tasks_batch_in_sessions(&assign)
        .await;

    let syncs_before = handle.queue().store_sync_count().await.unwrap();
    let start = std::time::Instant::now();
    for tid in &task_ids {
        handle_session_task_message(
            WorkerMessage::TaskResult {
                worker_id,
                task_id: *tid,
                exit_code: 0,
                stdout: "ok".into(),
                stderr: String::new().into(),
                execution_time_ms: 1,
                is_gpu_executed: false,
                device_name: None,
                error: None,
                asset_bytes: None,
                artifacts: Vec::new(),
                outputs: Vec::new(),
            },
            handle.registry(),
            handle.queue(),
            handle.scheduler_notify(),
            &handle.waiters,
            worker_id,
            session_id,
            handle.blob_store.as_ref(),
            #[cfg(feature = "dashboard")]
            None,
        )
        .await;
    }

    for wrx in waiter_rxs {
        let res = tokio::time::timeout(Duration::from_secs(2), wrx)
            .await
            .expect("waiter timed out")
            .expect("waiter dropped");
        assert_eq!(res.exit_code, 0);
    }
    let elapsed = start.elapsed();
    let syncs_after = handle.queue().store_sync_count().await.unwrap();
    let sync_delta = syncs_after.saturating_sub(syncs_before);

    assert!(
        sync_delta > 0 && sync_delta < 20,
        "expected >=50 sequential session results to coalesce into < 20 syncs, got {sync_delta} (elapsed {elapsed:?})"
    );

    handle.shutdown().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_submit_after_shutdown_returns_err_and_clean_shutdown_does_not_charge_running_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state_part_a");
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let handle = MasterServer::spawn(config).await.unwrap();
    handle.shutdown().unwrap();

    let late_task = Task::new(
        rusty_grid_core::TaskSpec::command("echo", vec!["late".into()]),
        rusty_grid_core::TaskRequirements::generic(1, 10),
    );
    let late_id = late_task.id;
    let res = handle.submit_task(late_task).await;
    assert!(
        matches!(res, Err(GridError::Storage(_))),
        "submit_task after shutdown() must return Err(GridError::Storage), got {res:?}"
    );

    let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let handle2 = MasterServer::spawn(config2).await.unwrap();
    assert_eq!(handle2.queue().get_state(&late_id).await, None);
    handle2.shutdown().unwrap();

    // Part B: clean shutdown while a worker is executing a task must never charge failover_count.
    for round in 0..4 {
        let round_dir = dir.path().join(format!("state_round_{round}"));
        let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&round_dir)
            .with_session_resume_hold(Duration::ZERO);
        let h = MasterServer::spawn(cfg).await.unwrap();
        let addr = h.server_addr().to_string();
        let (wtx, wrx) = tokio::sync::watch::channel(false);
        let mut w = rusty_grid_worker::client::WorkerClient::for_testing(
            addr,
            Some(format!("clean-shutdown-{round}")),
            Some(1),
            false,
        );
        let wh = tokio::spawn(async move {
            let _ = w.run(wrx).await;
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while h.list_workers().await.map(|v| v.len()).unwrap_or(0) < 1
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut req = rusty_grid_core::TaskRequirements::generic(1, 16);
        req.max_retries = Some(2);
        let running_task = Task::new(
            rusty_grid_core::TaskSpec::BuiltinTest {
                test_name: "long".into(),
                iterations: 4,
                duration_ms: 5_000,
                should_fail: false,
                require_gpu: false,
            },
            req,
        );
        let id = h.submit_task(running_task).await.unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while h.queue().get_state(&id).await != Some(TaskState::Running)
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        h.shutdown().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = wtx.send(true);
        wh.abort();

        let cfg_reopen = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&round_dir);
        let h2 = MasterServer::spawn(cfg_reopen).await.unwrap();
        let info = h2.queue().get_task(&id).await.unwrap();
        assert_eq!(
            info.failover_count, 0,
            "round {round}: clean master shutdown must not charge failover_count"
        );
        assert_eq!(
            info.retry_count, 0,
            "round {round}: clean master shutdown must not charge retry_count"
        );
        assert!(
            h2.queue()
                .get_retry_history(&id)
                .await
                .unwrap_or_default()
                .is_empty(),
            "round {round}: clean master shutdown must not append a RetryAttempt"
        );
        h2.shutdown().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rvb_submit_in_flight_during_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let sd = dir.path().join("s");
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&sd);
    let h = MasterServer::spawn(cfg).await.unwrap();
    let mut ids = Vec::new();
    let mut js = Vec::new();
    for i in 0..200 {
        let t = Task::new(
            rusty_grid_core::TaskSpec::command("echo", vec![format!("f{i}")]),
            rusty_grid_core::TaskRequirements::generic(1, 10),
        );
        ids.push(t.id);
        let h2 = h.clone();
        js.push(tokio::spawn(async move { h2.submit_task(t).await }));
    }
    tokio::time::sleep(Duration::from_millis(1)).await;
    h.shutdown().unwrap();
    let mut oks = std::collections::HashSet::new();
    for (i, j) in js.into_iter().enumerate() {
        if j.await.unwrap().is_ok() {
            oks.insert(ids[i]);
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let cfg2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&sd);
    let h2 = MasterServer::spawn(cfg2).await.unwrap();
    let mut lost = 0;
    for id in &oks {
        if h2.queue().get_state(id).await.is_none() {
            lost += 1;
        }
    }
    h2.shutdown().unwrap();
    assert_eq!(lost, 0, "acknowledged submissions must survive shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rvb_hundred_results_one_session() {
    let dir = tempfile::tempdir().unwrap();
    let sd = dir.path().join("s");
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&sd);
    let h = MasterServer::spawn(cfg).await.unwrap();
    let w = Uuid::new_v4();
    let (tx, _rx) = tokio::sync::mpsc::channel(512);
    let sid = h
        .registry()
        .register(
            w,
            WorkerCapabilities::new("w", 128, 65536, false, false, None),
            "127.0.0.1:9997".parse().unwrap(),
            tx,
            None,
        )
        .await
        .unwrap();
    let mut ids = Vec::new();
    let mut rxs = Vec::new();
    for i in 0..100 {
        let t = Task::new(
            rusty_grid_core::TaskSpec::command("echo", vec![format!("r{i}")]),
            rusty_grid_core::TaskRequirements::generic(1, 10),
        );
        let id = h.submit_task(t).await.unwrap();
        let (a, b) = tokio::sync::oneshot::channel();
        h.waiters.write().await.entry(id).or_default().push(a);
        ids.push(id);
        rxs.push(b);
    }
    let assign: Vec<_> = ids.iter().map(|i| (*i, w, sid)).collect();
    h.queue().schedule_tasks_batch_in_sessions(&assign).await;
    let s0 = h.queue().store_sync_count().await.unwrap();
    for id in &ids {
        handle_session_task_message(
            WorkerMessage::TaskResult {
                worker_id: w,
                task_id: *id,
                exit_code: 0,
                stdout: "ok".into(),
                stderr: String::new().into(),
                execution_time_ms: 1,
                is_gpu_executed: false,
                device_name: None,
                error: None,
                asset_bytes: None,
                artifacts: Vec::new(),
                outputs: Vec::new(),
            },
            h.registry(),
            h.queue(),
            h.scheduler_notify(),
            &h.waiters,
            w,
            sid,
            h.blob_store.as_ref(),
            #[cfg(feature = "dashboard")]
            None,
        )
        .await;
    }
    for rx in rxs {
        let res = tokio::time::timeout(Duration::from_secs(3), rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(res.exit_code, 0);
    }
    let s1 = h.queue().store_sync_count().await.unwrap();
    assert!(
        s1 - s0 > 0 && s1 - s0 < 20,
        "100 results should coalesce into < 20 syncs, got {}",
        s1 - s0
    );
    h.shutdown().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rvb_result_after_shutdown_detach_is_not_durable() {
    let dir = tempfile::tempdir().unwrap();
    let sd = dir.path().join("s");
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&sd);
    let h = MasterServer::spawn(cfg).await.unwrap();
    let t = Task::new(
        rusty_grid_core::TaskSpec::command("echo", vec!["r".into()]),
        rusty_grid_core::TaskRequirements::generic(1, 10),
    );
    let id = h.submit_task(t).await.unwrap();
    let w = Uuid::new_v4();
    h.queue()
        .schedule_tasks_batch_in_sessions(&[(id, w, 1)])
        .await;
    h.shutdown().unwrap();

    let o = h
        .queue()
        .record_result_from(
            w,
            Some(1),
            rusty_grid_core::TaskResult::success(w, id, "o", 1, false),
        )
        .await;
    assert!(
        matches!(o, Err(GridError::Storage(_))),
        "result arriving after shutdown() detaches the store must be refused, got {o:?}"
    );
    assert_eq!(
        h.queue().get_state(&id).await,
        Some(TaskState::Scheduled),
        "refused post-shutdown result must not transition in-memory state to Completed"
    );

    let cfg2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&sd);
    let h2 = MasterServer::spawn(cfg2).await.unwrap();
    assert_eq!(h2.queue().get_state(&id).await, Some(TaskState::Queued));
    h2.shutdown().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_failed_store_is_reported_in_cluster_status_and_dashboard() {
    let dir = tempfile::tempdir().unwrap();
    let sd = dir.path().join("state");
    let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
        .with_auth_config(crate::auth::AuthConfig::dev())
        .with_discovery(false)
        .with_state_dir(&sd);
    let h = MasterServer::spawn(cfg).await.unwrap();
    let addr = h.server_addr();

    // Trigger a real store sync failure via fault injection + submit.
    h.queue().inject_store_sync_failure(1).await;
    let bad_task = Task::new(
        rusty_grid_core::TaskSpec::command("echo", vec!["fail-sync".into()]),
        rusty_grid_core::TaskRequirements::generic(1, 10),
    );
    let submit_res = h.submit_task(bad_task).await;
    assert!(matches!(submit_res, Err(GridError::Storage(_))));

    // 1. QueueStats and TaskQueue::store_error report the failure reason.
    let q_stats = h.queue().stats().await;
    assert!(
        q_stats
            .store_error
            .as_deref()
            .is_some_and(|s| s.contains("injected sync failure")),
        "expected store_error in QueueStats, got {:?}",
        q_stats.store_error
    );

    // 2. ClientMessage::ClusterStatus carries store_error.
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);
    transport
        .send_msg(&ClientMessage::ClusterStatus)
        .await
        .unwrap();
    match transport.recv_msg::<ClientResponse>().await.unwrap() {
        Some(ClientResponse::ClusterStatus { store_error, .. }) => {
            assert!(
                store_error
                    .as_deref()
                    .is_some_and(|s| s.contains("injected sync failure")),
                "expected store_error in ClientResponse::ClusterStatus, got {store_error:?}"
            );
        }
        other => panic!("expected ClusterStatus, got {other:?}"),
    }

    // 3. Dashboard /api/status carries store_error on both ClusterStatusDto and its tasks QueueStats,
    // and GET / serves dashboard.html with the storeErrorBanner alert.
    let dash_addr = h
        .dashboard_addr()
        .expect("dashboard must be enabled in this test");
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let status_dto: crate::dashboard::dto::ClusterStatusDto =
        reqwest::get(format!("http://{dash_addr}/api/status"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert!(
        status_dto
            .store_error
            .as_deref()
            .is_some_and(|s| s.contains("injected sync failure")),
        "expected store_error in ClusterStatusDto, got {:?}",
        status_dto.store_error
    );
    assert_eq!(status_dto.tasks.store_error, status_dto.store_error);

    let html = reqwest::get(format!("http://{dash_addr}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        html.contains("id=\"storeErrorBanner\"")
            && html.contains("Task store failed: durability is off until restart"),
        "dashboard.html must contain #storeErrorBanner for store_error visibility"
    );

    h.shutdown().unwrap();
}

#[tokio::test]
async fn test_admit_blob_task_pins_workspace_tree_manifest_and_files() {
    use rusty_grid_core::blob::{BlobFile, BlobId, TreeManifest};
    use rusty_grid_core::task::WorkspaceRef;

    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(&crate::blob_store::BlobStoreConfig::new(
        dir.path(),
        64 * 1024 * 1024,
    ))
    .unwrap();

    let file_bytes = b"fn main() {}\n";
    let file_id = store.put(file_bytes).await.unwrap();
    let manifest = TreeManifest::new(vec![BlobFile {
        path: "src/main.rs".into(),
        id: file_id,
        size: file_bytes.len() as u64,
        executable: false,
    }])
    .unwrap();
    let manifest_bytes = manifest.encode();
    let manifest_id = store.put(&manifest_bytes).await.unwrap();

    let task = Task::new(
        rusty_grid_core::TaskSpec::new_command("cargo", vec!["check".into()]),
        rusty_grid_core::TaskRequirements::generic(1, 30),
    )
    .with_workspace(
        WorkspaceRef::new("oxide-ws").with_tree(manifest_id, manifest_bytes.len() as u64),
    );

    admit_blob_task(Some(&store), &task).await.unwrap();
    assert_eq!(store.pinned_tasks().len(), 1);
    store.unpin_task(&task.id).await;
    assert_eq!(store.pinned_tasks().len(), 0);

    // Missing workspace manifest blob is rejected.
    let missing_manifest_id = BlobId::of(b"not-uploaded");
    let bad_task = Task::new(
        rusty_grid_core::TaskSpec::new_command("cargo", vec!["check".into()]),
        rusty_grid_core::TaskRequirements::generic(1, 30),
    )
    .with_workspace(WorkspaceRef::new("oxide-ws").with_tree(missing_manifest_id, 12));
    let err = admit_blob_task(Some(&store), &bad_task).await.unwrap_err();
    assert!(err.contains("is not on the master"), "got: {err}");
}

#[cfg(feature = "p2p")]
#[tokio::test]
async fn test_master_bind_refuses_invalid_relay_url_and_minimal_conflict() {
    for bad_url in [
        "http://relay.example.com",
        "https://alice:secret@relay.example.com",
        "not-a-valid-url",
    ] {
        let mut cfg = ServerConfig::default()
            .with_ephemeral_key(true)
            .with_p2p_relay_url(bad_url);
        cfg.enable_p2p = true;
        let res = MasterServer::bind(cfg, WorkerRegistry::new()).await;
        let err = match res {
            Ok(_) => panic!("expected bind to fail for '{bad_url}'"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("Invalid relay URL"),
            "expected invalid relay URL error for '{bad_url}', got: {err}"
        );
        assert!(!err.contains("secret"), "must not echo password: {err}");
    }

    let conflict_cfg = ServerConfig::default()
        .with_p2p(true)
        .with_ephemeral_key(true)
        .with_p2p_minimal_preset(true)
        .with_p2p_relay_url("https://relay.example.com");
    let err = match MasterServer::bind(conflict_cfg, WorkerRegistry::new()).await {
        Ok(_) => panic!("expected bind to fail when minimal preset + relay URL are combined"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("minimal") && err.contains("relay"),
        "expected minimal+relay conflict error, got: {err}"
    );
}
