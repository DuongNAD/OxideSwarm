//! Integration tests for OxideSwarm Android Bridge: P2P Ticket, Mobile Telemetry & Coordinator.

use std::time::Duration;
use uuid::Uuid;

use oxideworker::{
    get_master_status_impl, get_master_ticket_impl, get_master_worker_count_impl,
    get_telemetry_impl, get_worker_status_impl, is_master_running_impl, is_running_impl,
    start_master_impl, start_worker_impl, stop_master_impl, stop_worker_impl,
    update_telemetry_impl,
};
use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[test]
fn test_p2p_ticket_and_worker_config() {
    let _guard = TEST_LOCK.blocking_lock();
    stop_worker_impl();

    let fake_p2p_ticket = "{\"id\":\"fake_node_id\",\"addrs\":[{\"type\":\"relay\",\"url\":\"https://relay.n0.iroh.link./\"}]}".to_string();
    let started = start_worker_impl(
        "127.0.0.1:54321".to_string(),
        Some(fake_p2p_ticket.clone()),
        "AndroidWorker-P2P-Test".to_string(),
        4,
        4096,
        false,
        5,
    );
    assert!(started, "Worker should start in-process");

    let status_json = get_worker_status_impl();
    assert!(
        status_json.contains("fake_node_id"),
        "Status JSON should include configured P2P ticket: {status_json}"
    );
    assert!(
        status_json.contains("status"),
        "Status JSON should contain worker status"
    );

    // Stop worker gracefully
    assert!(stop_worker_impl());
    assert!(!is_running_impl());
}

#[test]
fn test_telemetry_injection_roundtrip() {
    let _guard = TEST_LOCK.blocking_lock();

    // 1. Initial nominal telemetry (AC charging, cool)
    assert!(update_telemetry_impl(95, true, false));
    let t1 = get_telemetry_impl();
    assert_eq!(t1.battery_pct, Some(95));
    assert_eq!(t1.is_charging, Some(true));
    assert!(!t1.thermal_throttled);

    // 2. Battery drops to 12%, discharging, thermal throttling active
    assert!(update_telemetry_impl(12, false, true));
    let t2 = get_telemetry_impl();
    assert_eq!(t2.battery_pct, Some(12));
    assert_eq!(t2.is_charging, Some(false));
    assert!(t2.thermal_throttled);

    // 3. Worker status JSON reflects injected telemetry
    let status_json = get_worker_status_impl();
    assert!(
        status_json.contains("\"battery_pct\":12"),
        "Status JSON must reflect battery percentage: {status_json}"
    );
    assert!(
        status_json.contains("\"is_charging\":false"),
        "Status JSON must reflect charging state: {status_json}"
    );
    assert!(
        status_json.contains("\"thermal_throttled\":true"),
        "Status JSON must reflect thermal throttling: {status_json}"
    );
}

#[tokio::test]
async fn test_mobile_coordinator_and_worker_lifecycle() {
    let _guard = TEST_LOCK.lock().await;

    // Ensure clean initial state
    stop_master_impl();
    stop_worker_impl();

    // Start mobile coordinator on dynamic port (0) with P2P enabled
    let ticket = start_master_impl(0, 0, true).expect("Mobile coordinator should start");
    assert!(!ticket.is_empty(), "P2P ticket should be returned");
    assert!(ticket.contains("relay.n0.iroh.link"));
    assert!(is_master_running_impl());

    let master_status = get_master_status_impl();
    assert!(master_status.contains("\"running\":true"));
    assert!(master_status.contains("\"p2p_enabled\":true"));

    let ticket_queried = get_master_ticket_impl();
    assert_eq!(ticket_queried, Some(ticket.clone()));

    // Stop master coordinator
    assert!(stop_master_impl());
    assert!(!is_master_running_impl());
}

#[tokio::test]
async fn test_mobile_coordinator_registration_and_pre_sleep_evacuation() {
    let _guard = TEST_LOCK.lock().await;

    stop_master_impl();

    // Start master on dynamic port without P2P (direct LAN mode for TCP test)
    let lan_addr = start_master_impl(0, 0, false).expect("Mobile coordinator should start");
    assert!(is_master_running_impl());
    assert_eq!(get_master_worker_count_impl(), 0);

    // Connect client
    let stream = tokio::net::TcpStream::connect(&lan_addr)
        .await
        .expect("Client should connect to mobile coordinator");
    let mut transport = MessageTransport::new(stream);

    // 1. Handshake: Register
    let worker_id = Uuid::new_v4();
    let caps = WorkerCapabilities::new("android-test-node", 4, 2048, false, false, None);
    let reg_msg = WorkerMessage::Register {
        worker_id,
        capabilities: caps,
    };
    transport
        .send_msg(&reg_msg)
        .await
        .expect("Send Register should succeed");

    let ack_msg = transport
        .recv_msg::<MasterMessage>()
        .await
        .expect("Receive RegisterAck should succeed")
        .expect("Message should not be EOF");

    match ack_msg {
        MasterMessage::RegisterAck {
            accepted,
            worker_id: ack_id,
            ..
        } => {
            assert!(accepted);
            assert_eq!(ack_id, worker_id);
        }
        other => panic!("Expected RegisterAck, got: {other:?}"),
    }

    // Allow registration accounting to settle
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(get_master_worker_count_impl(), 1);

    // 2. Heartbeat Ping-Pong
    let hb_msg = WorkerMessage::Heartbeat {
        worker_id,
        timestamp: 123456789,
        active_tasks: 0,
        cpu_usage_pct: 15.0,
        ram_available_mb: 2048,
    };
    transport
        .send_msg(&hb_msg)
        .await
        .expect("Send Heartbeat should succeed");

    let hb_ack = transport
        .recv_msg::<MasterMessage>()
        .await
        .expect("Receive HeartbeatAck should succeed")
        .expect("Message should not be EOF");

    match hb_ack {
        MasterMessage::HeartbeatAck { timestamp } => {
            assert_eq!(timestamp, 123456789);
        }
        other => panic!("Expected HeartbeatAck, got: {other:?}"),
    }

    // 3. Zero-Latency Pre-Sleep Evacuation
    let disconnect_msg = WorkerMessage::Disconnecting {
        worker_id,
        reason: "BATTERY_CRITICAL".to_string(),
    };
    transport
        .send_msg(&disconnect_msg)
        .await
        .expect("Send Disconnecting should succeed");

    let shutdown_ack = transport
        .recv_msg::<MasterMessage>()
        .await
        .expect("Receive Shutdown should succeed")
        .expect("Message should not be EOF");

    match shutdown_ack {
        MasterMessage::Shutdown { reason, .. } => {
            assert!(reason.contains("pre-sleep evacuation"));
        }
        other => panic!("Expected Shutdown ack, got: {other:?}"),
    }

    // Worker count should immediately drop to 0 without waiting for heartbeat timeout
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(get_master_worker_count_impl(), 0);

    // Clean stop
    assert!(stop_master_impl());
    assert!(!is_master_running_impl());
}
