//! End-to-end integration tests for Subnet-Isolated Discovery Fallback and Rendezvous Registry.
//!
//! Validates:
//! 1. Direct authenticated HTTP candidate probing (`GET /api/discovery/probe?nonce=<hex>`).
//! 2. Cryptographic HMAC verification and rejection of tampered nonces/signatures/wrong secrets.
//! 3. Rendezvous lease registration (`POST /api/rendezvous/register`) and discovery (`GET /api/rendezvous/master`).
//! 4. Full tiered discovery fallback: secret holders seamlessly find the master via HTTP probing or
//!    rendezvous when UDP broadcast is disabled or isolated across subnets.

use std::time::Duration;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::discovery::{
    discover_master_rendezvous, discover_master_tiered, probe_http_candidate_with_secret,
    register_rendezvous_lease, RendezvousLease,
};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};

fn make_secret(seed: u8) -> ClusterSecret {
    ClusterSecret::from_bytes(&[seed; 32]).expect("valid 32-byte secret")
}

#[tokio::test]
async fn test_http_discovery_probe_hmac_verification_and_tamper_rejection() {
    let secret = make_secret(0x11);
    let wrong_secret = make_secret(0x22);

    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(secret.clone());

    let master: MasterHandle = MasterServer::spawn_with_config(
        server_config,
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawns");

    let dash_addr = master
        .dashboard_addr()
        .expect("master has dashboard address bound");
    let dash_port = dash_addr.port();

    // 1. Valid probe with matching cluster secret
    let beacon = probe_http_candidate_with_secret(
        "127.0.0.1",
        dash_port,
        Duration::from_secs(3),
        Some(&secret),
    )
    .await
    .expect("authenticated probe must succeed");

    assert_eq!(beacon.cluster_addr, master.server_addr().to_string());
    assert!(beacon.mac.is_some());

    // 2. Probe with wrong cluster secret must fail verification
    let wrong_result = probe_http_candidate_with_secret(
        "127.0.0.1",
        dash_port,
        Duration::from_secs(3),
        Some(&wrong_secret),
    )
    .await;
    assert!(
        wrong_result.is_none(),
        "probe with mismatched cluster secret must fail verification"
    );
}

#[tokio::test]
async fn test_rendezvous_registry_and_discovery_fallback() {
    let secret = make_secret(0x33);
    let wrong_secret = make_secret(0x44);

    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(secret.clone());

    let master: MasterHandle = MasterServer::spawn_with_config(
        server_config,
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawns");

    let dash_addr = master
        .dashboard_addr()
        .expect("master has dashboard address bound");
    let rendezvous_url = format!("http://127.0.0.1:{}", dash_addr.port());

    // 1. Querying before registration yields None
    let pre_query =
        discover_master_rendezvous(&rendezvous_url, Some(&secret), Duration::from_secs(3)).await;
    assert!(
        pre_query.is_none(),
        "unregistered rendezvous must return None"
    );

    // 2. Create signed lease for master
    let mut lease = RendezvousLease::new(
        master.server_addr().to_string(),
        format!("http://127.0.0.1:{}", dash_addr.port()),
        "test-master",
        None,
        1,
        60,
    );
    lease.sign(&secret);

    // 3. Register valid lease
    let reg_result = register_rendezvous_lease(&rendezvous_url, &lease).await;
    assert!(reg_result.is_ok(), "lease registration must succeed");

    // 4. Query rendezvous with valid secret
    let discovered =
        discover_master_rendezvous(&rendezvous_url, Some(&secret), Duration::from_secs(3)).await;
    assert!(discovered.is_some(), "rendezvous discovery must succeed");
    let beacon = discovered.unwrap();
    assert_eq!(beacon.cluster_addr, master.server_addr().to_string());

    // 5. Query rendezvous with wrong secret must fail verification
    let wrong_discovery =
        discover_master_rendezvous(&rendezvous_url, Some(&wrong_secret), Duration::from_secs(3))
            .await;
    assert!(
        wrong_discovery.is_none(),
        "rendezvous discovery with wrong secret must reject lease"
    );

    // 6. Register tampered lease must be rejected
    let mut tampered_lease = lease.clone();
    tampered_lease.cluster_addr = "127.0.0.1:9999".to_string(); // Tamper cluster_addr without re-signing
    let tampered_result = register_rendezvous_lease(&rendezvous_url, &tampered_lease).await;
    assert!(
        tampered_result.is_err(),
        "registration of tampered lease must be rejected"
    );
}

#[tokio::test]
async fn test_tiered_discovery_fallback_without_udp_broadcast() {
    let secret = make_secret(0x55);

    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(secret.clone());

    let master: MasterHandle = MasterServer::spawn_with_config(
        server_config,
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawns");

    let dash_addr = master
        .dashboard_addr()
        .expect("master has dashboard address bound");
    // Call discover_master_tiered with custom web_ui_port fallback
    let discovered = discover_master_tiered(
        Duration::from_millis(100), // Short UDP timeout (UDP broadcast isn't listening)
        master.server_addr().port(),
        Some(&secret),
        dash_addr.port(),
    )
    .await;

    assert!(
        discovered.is_some(),
        "tiered discovery must fall back to candidate probe"
    );
    let beacon = discovered.unwrap();
    assert_eq!(beacon.cluster_addr, master.server_addr().to_string());
    assert!(beacon.mac.is_some());
}
