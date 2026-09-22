//! Integration Test Suite for Milestone 4 (R4: Dashboard & CLI Telemetry Visualization).
//!
//! Validates:
//! 1. WorkerUiInfo telemetry serialization/deserialization: interconnect_type, is_relayed, rtt_ms.
//! 2. ClusterStatusDto and DashboardStatus serialization with p2p_ticket.
//! 3. Web UI /api/status HTTP endpoint returning genuine telemetry fields.
//! 4. Dashboard HTML containing Link Badges, 1-Click Copy Ticket button, degradation alert banner,
//!    and dynamic SVG topology connector line styling.
//! 5. Full MasterServer spawn with Web UI and P2P ticket exposure.

use std::sync::Arc;
use std::time::Duration;
use reqwest::StatusCode;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_master::dashboard::dto::ClusterStatusDto;
use rusty_grid_master::queue::TaskQueue;
use rusty_grid_master::registry::WorkerRegistry;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_master::web_ui::{create_web_ui_router, DashboardStatus, WorkerUiInfo};

/// 1. Verifies that WorkerUiInfo includes and correctly serializes/deserializes
///    interconnect_type, is_relayed, and rtt_ms.
#[test]
fn test_m4_worker_ui_info_schema_and_serialization() {
    let direct_worker = WorkerUiInfo {
        id: "w-direct-001".to_string(),
        name: "macbook-m3-direct".to_string(),
        status: "Connected".to_string(),
        role: "ORCH".to_string(),
        role_description: "Orchestrator Node".to_string(),
        active_tasks: 1,
        cpu_cores: 12,
        ram_mb: 32768,
        cpu_usage_pct: 15.2,
        ram_available_mb: 24576,
        has_gpu: true,
        gpu_device_name: Some("Apple M3 Max".to_string()),
        battery_pct: Some(92),
        is_charging: Some(true),
        thermal_throttled: false,
        last_heartbeat_secs_ago: 1,
        interconnect_type: "Direct P2P (QUIC)".to_string(),
        is_relayed: false,
        rtt_ms: Some(14.8),
    };

    let json_direct = serde_json::to_string(&direct_worker).expect("serialize direct worker");
    assert!(json_direct.contains("\"interconnect_type\":\"Direct P2P (QUIC)\""));
    assert!(json_direct.contains("\"is_relayed\":false"));
    assert!(json_direct.contains("\"rtt_ms\":14.8"));

    let de_direct: WorkerUiInfo = serde_json::from_str(&json_direct).expect("deserialize direct worker");
    assert_eq!(de_direct.interconnect_type, "Direct P2P (QUIC)");
    assert!(!de_direct.is_relayed);
    assert_eq!(de_direct.rtt_ms, Some(14.8));

    let relay_worker = WorkerUiInfo {
        id: "w-relay-002".to_string(),
        name: "phone-s24-relay".to_string(),
        status: "Busy".to_string(),
        role: "WORKER".to_string(),
        role_description: "Mobile Worker".to_string(),
        active_tasks: 3,
        cpu_cores: 8,
        ram_mb: 8192,
        cpu_usage_pct: 78.4,
        ram_available_mb: 2048,
        has_gpu: false,
        gpu_device_name: None,
        battery_pct: Some(45),
        is_charging: Some(false),
        thermal_throttled: true,
        last_heartbeat_secs_ago: 2,
        interconnect_type: "Relay (DERP)".to_string(),
        is_relayed: true,
        rtt_ms: Some(182.5),
    };

    let json_relay = serde_json::to_string(&relay_worker).expect("serialize relay worker");
    assert!(json_relay.contains("\"interconnect_type\":\"Relay (DERP)\""));
    assert!(json_relay.contains("\"is_relayed\":true"));
    assert!(json_relay.contains("\"rtt_ms\":182.5"));

    let de_relay: WorkerUiInfo = serde_json::from_str(&json_relay).expect("deserialize relay worker");
    assert_eq!(de_relay.interconnect_type, "Relay (DERP)");
    assert!(de_relay.is_relayed);
    assert_eq!(de_relay.rtt_ms, Some(182.5));
}

/// 2. Verifies that ClusterStatusDto and DashboardStatus serialize and deserialize p2p_ticket.
#[test]
fn test_m4_cluster_status_dto_p2p_ticket() {
    let raw_with_ticket = r#"{
        "cluster_health": "Healthy",
        "master": {
            "host": "192.168.1.144:8088",
            "role": "MASTER",
            "description": "Cluster Coordinator"
        },
        "tasks": {
            "total": 0,
            "queued": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "active_list": []
        },
        "workers": [],
        "p2p_ticket": "ticket-master-test-sec12345"
    }"#;

    let parsed_with: DashboardStatus = serde_json::from_str(raw_with_ticket).expect("parse with ticket");
    assert_eq!(
        parsed_with.p2p_ticket.as_deref(),
        Some("ticket-master-test-sec12345")
    );

    let raw_without_ticket = r#"{
        "cluster_health": "Healthy",
        "master": {
            "host": "127.0.0.1:8088",
            "role": "MASTER",
            "description": "Cluster Coordinator"
        },
        "tasks": {
            "total": 0,
            "queued": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "active_list": []
        },
        "workers": []
    }"#;

    let parsed_without: DashboardStatus = serde_json::from_str(raw_without_ticket).expect("parse without ticket");
    assert_eq!(parsed_without.p2p_ticket, None);

    let dto: ClusterStatusDto = serde_json::from_str(r#"{
        "version": "0.1.0",
        "uptime_secs": 120,
        "master_addr": "127.0.0.1:8088",
        "dashboard_addr": null,
        "tasks": { "total": 0, "queued": 0, "scheduled": 0, "running": 0, "retrying": 0, "completed": 0, "failed": 0, "cancelled": 0 },
        "workers": { "total": 0, "connected": 0, "busy": 0, "disconnected": 0 },
        "p2p_ticket": "iroh-ticket-cluster-test"
    }"#).expect("parse ClusterStatusDto with ticket");
    assert_eq!(dto.p2p_ticket.as_deref(), Some("iroh-ticket-cluster-test"));
}

/// 3. Spawns Web UI HTTP router on an ephemeral port, registers heterogeneous workers with
/// different interconnect paths (Direct P2P, DERP Relay, TCP/LAN), and asserts that /api/status
/// populates all telemetry fields accurately.
#[tokio::test]
async fn test_m4_api_status_endpoint_returns_telemetry() {
    let registry = WorkerRegistry::new();
    let queue = TaskQueue::new();
    let scheduler_notify = Arc::new(tokio::sync::Notify::new());

    // Register Direct P2P Worker
    let caps_direct = WorkerCapabilities::new("galaxy-s24-phone", 8, 16384, false, false, None)
        .with_tags(vec!["link:Direct P2P (QUIC):18ms".to_string()]);
    let (tx1, _rx1) = tokio::sync::mpsc::channel(16);
    registry
        .register(
            Uuid::new_v4(),
            caps_direct,
            "127.0.0.1:9001".parse().unwrap(),
            tx1,
            None,
        )
        .await
        .unwrap();

    // Register DERP Relay Worker
    let caps_relay = WorkerCapabilities::new("macbook-relay-node", 10, 32768, false, false, None)
        .with_tags(vec!["link:Relay (DERP):168ms".to_string()]);
    let (tx2, _rx2) = tokio::sync::mpsc::channel(16);
    registry
        .register(
            Uuid::new_v4(),
            caps_relay,
            "127.0.0.1:9002".parse().unwrap(),
            tx2,
            None,
        )
        .await
        .unwrap();

    // Register Standard TCP/LAN Worker
    let caps_tcp = WorkerCapabilities::new("lan-desktop-pc", 4, 8192, false, false, None)
        .with_tags(vec!["link:TCP/LAN".to_string()]);
    let (tx3, _rx3) = tokio::sync::mpsc::channel(16);
    registry
        .register(
            Uuid::new_v4(),
            caps_tcp,
            "127.0.0.1:9003".parse().unwrap(),
            tx3,
            None,
        )
        .await
        .unwrap();

    let ticket = Some("iroh-p2p-test-ticket-987654321".to_string());
    let router = create_web_ui_router(
        registry.clone(),
        queue.clone(),
        scheduler_notify.clone(),
        None,
        ticket.clone(),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral listener");
    let bound_addr = listener.local_addr().expect("local addr");

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let server = axum::serve(listener, router);
        let _ = server
            .with_graceful_shutdown(async move {
                while shutdown_rx.changed().await.is_ok() {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            })
            .await;
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    let base_url = format!("http://{}", bound_addr);

    // Query /api/status
    let resp = client
        .get(format!("{}/api/status", base_url))
        .send()
        .await
        .expect("GET /api/status");

    assert_eq!(resp.status(), StatusCode::OK);
    let status: DashboardStatus = resp.json().await.expect("parse DashboardStatus");

    // 1. Verify p2p_ticket is returned
    assert_eq!(
        status.p2p_ticket.as_deref(),
        Some("iroh-p2p-test-ticket-987654321")
    );

    // 2. Verify worker list telemetry
    assert_eq!(status.workers.len(), 3);

    let direct_res = status
        .workers
        .iter()
        .find(|w| w.name == "galaxy-s24-phone")
        .expect("found direct worker");
    assert_eq!(direct_res.interconnect_type, "Direct P2P (QUIC)");
    assert!(!direct_res.is_relayed);
    assert_eq!(direct_res.rtt_ms, Some(18.0));

    let relay_res = status
        .workers
        .iter()
        .find(|w| w.name == "macbook-relay-node")
        .expect("found relay worker");
    assert_eq!(relay_res.interconnect_type, "Relay (DERP)");
    assert!(relay_res.is_relayed);
    assert_eq!(relay_res.rtt_ms, Some(168.0));

    let tcp_res = status
        .workers
        .iter()
        .find(|w| w.name == "lan-desktop-pc")
        .expect("found tcp worker");
    assert_eq!(tcp_res.interconnect_type, "TCP/LAN");
    assert!(!tcp_res.is_relayed);

    let _ = shutdown_tx.send(true);
}

/// 4. Verifies that the Dashboard HTML includes Link Badges, 1-Click "Copy Ticket" button,
/// visual degradation alert banner, and dynamic SVG topology connector styling.
#[tokio::test]
async fn test_m4_dashboard_html_visual_elements() {
    let registry = WorkerRegistry::new();
    let queue = TaskQueue::new();
    let scheduler_notify = Arc::new(tokio::sync::Notify::new());

    let router = create_web_ui_router(
        registry,
        queue,
        scheduler_notify,
        None,
        Some("ticket-html-test-456".to_string()),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral listener");
    let bound_addr = listener.local_addr().expect("local addr");

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let server = axum::serve(listener, router);
        let _ = server
            .with_graceful_shutdown(async move {
                while shutdown_rx.changed().await.is_ok() {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            })
            .await;
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    let resp = client
        .get(format!("http://{}/", bound_addr))
        .send()
        .await
        .expect("GET /");

    assert_eq!(resp.status(), StatusCode::OK);
    let html = resp.text().await.expect("body text");

    // 1. Link Badges styling & elements
    assert!(html.contains(".link-badge"), "CSS must contain .link-badge");
    assert!(html.contains(".link-direct"), "CSS must contain .link-direct");
    assert!(html.contains(".link-relay"), "CSS must contain .link-relay");
    assert!(html.contains(".link-tcp"), "CSS must contain .link-tcp");
    assert!(html.contains("Direct P2P"), "Must contain Direct P2P badge text");
    assert!(html.contains("DERP Relay"), "Must contain DERP Relay badge text");

    // 2. 1-Click Copy Ticket button
    assert!(
        html.contains("id=\"copyTicketBtn\""),
        "Top actions must contain #copyTicketBtn"
    );
    assert!(
        html.contains("copyP2pTicket()"),
        "Button must invoke copyP2pTicket()"
    );
    assert!(
        html.contains("window.currentP2pTicket"),
        "syncData must store window.currentP2pTicket"
    );

    // 3. Degradation Alert Banner
    assert!(
        html.contains("id=\"degradationAlertBanner\""),
        "Must contain #degradationAlertBanner element"
    );
    assert!(
        html.contains(".degradation-banner"),
        "CSS must contain .degradation-banner"
    );

    // 4. Dynamic SVG Topology styling
    assert!(
        html.contains("getTransportStyle"),
        "Topology rendering must use getTransportStyle"
    );
    assert!(
        html.contains("glowGreen"),
        "SVG defs must define glowGreen filter"
    );

    // 5. Device Detail Modal interconnect & RTT fields
    assert!(
        html.contains("id=\"mLink\""),
        "Device modal must contain #mLink"
    );
    assert!(
        html.contains("id=\"mRtt\""),
        "Device modal must contain #mRtt"
    );

    let _ = shutdown_tx.send(true);
}

/// 5. Full end-to-end MasterServer spawn with Web UI enabled and P2P ticket exposure.
#[tokio::test]
async fn test_m4_master_server_full_spawn_with_p2p_and_web_ui() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let ticket_path = temp_dir.path().join("p2p_ticket.txt");

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_p2p(true)
        .with_p2p_ticket_file(&ticket_path)
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap());

    let master = MasterServer::spawn(config).await.expect("spawn MasterServer");
    assert!(master.server_addr().port() > 0);

    // Ensure MasterHandle p2p_ticket is populated
    #[cfg(feature = "p2p")]
    {
        let ticket = master.p2p_ticket();
        assert!(ticket.is_some(), "MasterHandle must expose active p2p_ticket");
        assert!(!ticket.unwrap().is_empty(), "p2p_ticket must not be empty");
    }

    let _ = master.shutdown();
}
