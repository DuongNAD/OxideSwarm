//! The master's QUIC (iroh) listener under misbehaving peers, over loopback with relays disabled on
//! the client side:
//!
//! * a peer that completes the QUIC handshake but never opens a stream is dropped once the
//!   handshake timeout passes (it used to hold its connection for the whole QUIC idle timeout);
//! * the connection budget applies to QUIC peers too: with a budget of one, a second concurrent peer
//!   is refused while the first is still pending, and gets in once the first is gone;
//! * real workers connecting over QUIC release their pre-authentication slot once registered, so a
//!   budget of one still lets any number of workers join, one after another.

use std::time::{Duration, Instant};

use iroh::endpoint::presets::N0;
use iroh::endpoint::RelayMode;

use rusty_grid_core::transport::{parse_p2p_ticket, GRID_ALPN, STREAM_CONTROL};
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use tokio::sync::watch;

async fn spawn_p2p_master(config: ServerConfig, handshake_timeout_secs: u64) -> MasterHandle {
    MasterServer::spawn(
        config
            .with_p2p(true)
            .with_ephemeral_key(true)
            .with_web_ui(false)
            .with_discovery(false)
            .with_handshake_timeout(handshake_timeout_secs),
    )
    .await
    .expect("spawn p2p master")
}

async fn dial(master: &MasterHandle) -> Result<iroh::endpoint::Connection, String> {
    let ticket = master.p2p_ticket().expect("p2p ticket");
    let addr = parse_p2p_ticket(ticket).expect("parse ticket");
    let client = iroh::Endpoint::builder(N0)
        .alpns(vec![GRID_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind client endpoint");
    let conn = client
        .connect(addr, GRID_ALPN)
        .await
        .map_err(|e| e.to_string())?;
    // Keep the client endpoint alive as long as the connection.
    std::mem::forget(client);
    Ok(conn)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_a_quic_peer_that_opens_no_stream_is_dropped_after_the_handshake_timeout() {
    let master = spawn_p2p_master(ServerConfig::new("127.0.0.1:0".parse().unwrap()), 1).await;
    let conn = dial(&master).await.expect("QUIC handshake succeeds");

    // It never opens a stream. The master must give up on it after about a second.
    let started = Instant::now();
    tokio::time::timeout(Duration::from_secs(6), conn.closed())
        .await
        .expect("the master must close a QUIC connection that never opens a stream");
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "closed after {:?}",
        started.elapsed()
    );
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_quic_peers_count_against_the_connection_budget_and_free_it_when_gone() {
    // A long handshake timeout, so that only the budget (not a timeout) can turn the second peer away
    // quickly.
    let master = spawn_p2p_master(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_connections(1),
        20,
    )
    .await;

    // The first peer connects and sits on its slot (it opens a control stream and then says nothing).
    let first = dial(&master).await.expect("first peer");
    let (mut send, _recv) = first.open_bi().await.expect("open stream");
    send.write_all(&[STREAM_CONTROL]).await.expect("tag");

    // A second concurrent peer is refused straight away: its connection attempt fails, or the
    // connection is closed at once (well before the 20 s handshake timeout could do it).
    let started = Instant::now();
    let refused = match dial(&master).await {
        Err(_) => true,
        Ok(conn) => tokio::time::timeout(Duration::from_millis(1500), conn.closed())
            .await
            .is_ok(),
    };
    assert!(
        refused && started.elapsed() < Duration::from_secs(5),
        "a second QUIC peer must be refused at once while the budget of one is used (took {:?})",
        started.elapsed()
    );

    // Once the first peer is gone the slot is free again: a new peer stays connected.
    first.close(0u32.into(), b"bye");
    let start = Instant::now();
    loop {
        if let Ok(conn) = dial(&master).await {
            let closed_quickly = tokio::time::timeout(Duration::from_millis(1500), conn.closed())
                .await
                .is_ok();
            if !closed_quickly {
                break;
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "a closed QUIC peer must give its slot back"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_workers_over_quic_release_the_pre_auth_slot_once_registered() {
    let master = spawn_p2p_master(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_pending_handshakes(1),
        20,
    )
    .await;
    let ticket = master.p2p_ticket().expect("ticket").to_string();

    let mut shutdowns = Vec::new();
    for i in 0..3 {
        let sandbox = tempfile::tempdir().unwrap();
        let config = WorkerConfig::new("auto")
            .with_name(format!("quic-worker-{i}"))
            .with_cores(1)
            .with_sandbox_base_dir(sandbox.path())
            .with_p2p_ticket(&ticket);
        let mut worker = WorkerClient::new(config);
        let (tx, rx) = watch::channel(false);
        tokio::spawn(async move {
            let _keep = sandbox;
            let _ = worker.run(rx).await;
        });
        shutdowns.push(tx);

        let start = Instant::now();
        loop {
            let connected = master
                .list_workers()
                .await
                .map(|w| w.len())
                .unwrap_or_default();
            if connected > i {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "worker {i} never registered: a registered QUIC worker must not hold the pre-auth slot"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    for tx in shutdowns {
        let _ = tx.send(true);
    }
    let _ = master.shutdown();
}
