//! Adversarial Challenge Test Harness for OxideSwarm Android Bridge (`oxideworker`).
//!
//! Specifically stress-tests:
//! 1. Boundary telemetry inputs (0%, 100%, negative values, extreme out-of-bounds, rapid toggling, concurrent access).
//! 2. P2P ticket parsing with empty strings, whitespace, malformed JSON, LAN strings, and testing compatibility with real `iroh::EndpointAddr`.
//! 3. Rapid sequential worker start/stop cycles and double-start/stop idempotency.

use std::time::Duration;

use oxideworker::{
    get_telemetry_impl, get_worker_status_impl, is_running_impl, set_sandbox_dir_impl,
    start_worker_impl, stop_worker_impl, update_telemetry_detailed_impl, update_telemetry_impl,
};
use rusty_grid_master::server::{MasterServer, ServerConfig};

static ADVERSARIAL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ==============================================================================
// 1. Boundary Telemetry Testing
// ==============================================================================

#[test]
fn test_adversarial_telemetry_boundaries() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    // 1. Nominal boundary: 0% battery
    assert!(update_telemetry_impl(0, false, false));
    let t_zero = get_telemetry_impl();
    assert_eq!(
        t_zero.battery_pct,
        Some(0),
        "0% battery must be accepted as Some(0)"
    );
    assert_eq!(t_zero.is_charging, Some(false));
    assert!(!t_zero.thermal_throttled);

    // 2. Nominal boundary: 100% battery
    assert!(update_telemetry_impl(100, true, false));
    let t_full = get_telemetry_impl();
    assert_eq!(
        t_full.battery_pct,
        Some(100),
        "100% battery must be accepted as Some(100)"
    );
    assert_eq!(t_full.is_charging, Some(true));
    assert!(!t_full.thermal_throttled);

    // 3. Negative out-of-bounds: -1, -50, i32::MIN
    for neg_val in [-1, -50, i32::MIN] {
        assert!(update_telemetry_impl(neg_val, false, true));
        let t_neg = get_telemetry_impl();
        assert_eq!(
            t_neg.battery_pct, None,
            "Negative battery level {neg_val} must sanitize to None"
        );
        assert!(t_neg.thermal_throttled);
    }

    // 4. Positive out-of-bounds: 101, 200, i32::MAX
    for pos_val in [101, 200, i32::MAX] {
        assert!(update_telemetry_impl(pos_val, true, false));
        let t_pos = get_telemetry_impl();
        assert_eq!(
            t_pos.battery_pct, None,
            "Excessive battery level {pos_val} must sanitize to None"
        );
    }

    // 5. Rapid thermal throttling and charging state toggling
    for i in 0..100 {
        let is_charging = i % 2 == 0;
        let thermal_throttled = i % 3 == 0;
        let pct = i % 101;
        assert!(update_telemetry_impl(pct, is_charging, thermal_throttled));
        let cur = get_telemetry_impl();
        assert_eq!(cur.battery_pct, Some(pct as u8));
        assert_eq!(cur.is_charging, Some(is_charging));
        assert_eq!(cur.thermal_throttled, thermal_throttled);
    }

    // 6. Adversarial edge cases for update_telemetry_detailed_impl
    // 6a. Extreme temperature values, NaN, +/-Infinity
    let temp_edge_cases = [
        (
            Some(f32::NAN),
            None,
            "NaN temperature must sanitize to None",
        ),
        (
            Some(f32::INFINITY),
            None,
            "Positive infinity must sanitize to None",
        ),
        (
            Some(f32::NEG_INFINITY),
            None,
            "Negative infinity must sanitize to None",
        ),
        (
            Some(-50.001),
            None,
            "Below -50.0C lower bound must sanitize to None",
        ),
        (
            Some(-1000.0),
            None,
            "Extreme negative temperature must sanitize to None",
        ),
        (
            Some(120.001),
            None,
            "Above 120.0C upper bound must sanitize to None",
        ),
        (
            Some(1000.0),
            None,
            "Extreme positive temperature must sanitize to None",
        ),
        (
            Some(-50.0),
            Some(-50.0),
            "-50.0C exact lower bound must be accepted",
        ),
        (
            Some(120.0),
            Some(120.0),
            "120.0C exact upper bound must be accepted",
        ),
        (Some(0.0), Some(0.0), "0.0C nominal must be accepted"),
        (
            Some(37.5),
            Some(37.5),
            "37.5C normal operating temperature must be accepted",
        ),
        (None, None, "None temperature must remain None"),
    ];

    for (input_temp, expected_temp, desc) in temp_edge_cases {
        assert!(update_telemetry_detailed_impl(
            75,
            true,
            false,
            input_temp,
            Some("wifi".to_string())
        ));
        let cur = get_telemetry_impl();
        assert_eq!(cur.battery_temperature, expected_temp, "{desc}");
        assert_eq!(
            cur.battery_temp_c(),
            expected_temp,
            "{desc} (via battery_temp_c)"
        );
    }

    // 6b. Empty, whitespace, sentinel ("unknown", "none"), and valid network types
    let net_edge_cases = [
        (
            Some("".to_string()),
            None,
            "Empty string network type must sanitize to None",
        ),
        (
            Some("   ".to_string()),
            None,
            "Whitespace string network type must sanitize to None",
        ),
        (
            Some(" \t\r\n ".to_string()),
            None,
            "Whitespace with tabs/newlines must sanitize to None",
        ),
        (
            Some("unknown".to_string()),
            None,
            "'unknown' sentinel must sanitize to None",
        ),
        (
            Some("UNKNOWN".to_string()),
            None,
            "'UNKNOWN' uppercase sentinel must sanitize to None",
        ),
        (
            Some("  Unknown  ".to_string()),
            None,
            "Padded 'Unknown' sentinel must sanitize to None",
        ),
        (
            Some("none".to_string()),
            None,
            "'none' sentinel must sanitize to None",
        ),
        (
            Some("NONE".to_string()),
            None,
            "'NONE' uppercase sentinel must sanitize to None",
        ),
        (
            Some("  None  ".to_string()),
            None,
            "Padded 'None' sentinel must sanitize to None",
        ),
        (
            Some("wifi".to_string()),
            Some("wifi".to_string()),
            "'wifi' must be accepted",
        ),
        (
            Some("  WIFI  ".to_string()),
            Some("wifi".to_string()),
            "'  WIFI  ' must be trimmed and lowercased",
        ),
        (
            Some("CELLULAR".to_string()),
            Some("cellular".to_string()),
            "'CELLULAR' must be lowercased",
        ),
        (
            Some("Ethernet".to_string()),
            Some("ethernet".to_string()),
            "'Ethernet' must be lowercased",
        ),
        (
            Some("5G-NR".to_string()),
            Some("5g-nr".to_string()),
            "'5G-NR' must be lowercased",
        ),
        (None, None, "None network type must remain None"),
    ];

    for (input_net, expected_net, desc) in net_edge_cases {
        assert!(update_telemetry_detailed_impl(
            50,
            false,
            false,
            Some(35.0),
            input_net
        ));
        let cur = get_telemetry_impl();
        assert_eq!(cur.network_type, expected_net, "{desc}");
    }

    // 6c. JSON output serialization under edge cases
    assert!(update_telemetry_detailed_impl(
        -1,
        false,
        true,
        Some(f32::NAN),
        Some("  ".to_string())
    ));
    let status_str = get_worker_status_impl();
    let status_val: serde_json::Value =
        serde_json::from_str(&status_str).expect("Worker status must always parse as valid JSON");
    assert_eq!(
        status_val["telemetry"]["battery_pct"],
        serde_json::Value::Null
    );
    assert_eq!(
        status_val["telemetry"]["battery_temperature"],
        serde_json::Value::Null
    );
    assert_eq!(
        status_val["telemetry"]["network_type"],
        serde_json::Value::Null
    );
    assert_eq!(status_val["telemetry"]["thermal_throttled"], true);
}

#[test]
fn test_adversarial_telemetry_concurrent_flood() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    // Concurrently flood update_telemetry_impl and get_worker_status_impl from 16 threads
    let threads: Vec<_> = (0..16)
        .map(|tid| {
            std::thread::spawn(move || {
                for i in 0..200 {
                    let pct = (tid * 10 + i) % 101;
                    let charging = (i + tid) % 2 == 0;
                    let throttled = (i + tid) % 3 == 0;
                    update_telemetry_impl(pct, charging, throttled);
                    let _ = get_worker_status_impl();
                    let _ = get_telemetry_impl();
                }
            })
        })
        .collect();

    for handle in threads {
        handle
            .join()
            .expect("Concurrent telemetry thread should not panic");
    }

    // After flood, verify state is healthy and queryable
    let status = get_worker_status_impl();
    assert!(status.contains("telemetry"));
    assert!(status.contains("battery_pct"));
}

// ==============================================================================
// 2. P2P Ticket Parsing Edge Cases & Master Ticket Validation
// ==============================================================================

#[test]
fn test_adversarial_p2p_ticket_parsing_edge_cases() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    // 1. Empty string
    let res_empty = rusty_grid_core::transport::parse_p2p_ticket("");
    assert!(res_empty.is_err(), "Empty ticket string must fail to parse");

    // 2. Whitespace only
    let res_ws = rusty_grid_core::transport::parse_p2p_ticket("   \t\r\n   ");
    assert!(
        res_ws.is_err(),
        "Whitespace ticket string must fail to parse"
    );

    // 3. Plain IP/port LAN address (not a P2P ticket)
    let res_lan = rusty_grid_core::transport::parse_p2p_ticket("127.0.0.1:8088");
    assert!(
        res_lan.is_err(),
        "Raw LAN address must not parse as P2P ticket"
    );

    // 4. Malformed JSON
    let res_malformed = rusty_grid_core::transport::parse_p2p_ticket("{\"id\": 1234, incomplete");
    assert!(res_malformed.is_err(), "Malformed JSON must fail to parse");

    // 5. Random binary/ASCII garbage
    let res_garbage = rusty_grid_core::transport::parse_p2p_ticket("!@#$%^&*()_+-=~`[]{}|;:,.<>?");
    assert!(res_garbage.is_err(), "Garbage string must fail to parse");
}

#[tokio::test]
async fn test_adversarial_master_generated_ticket_compatibility() {
    let _guard = ADVERSARIAL_TEST_LOCK.lock().await;

    let master_cfg = ServerConfig::from_addr("127.0.0.1:0")
        .expect("valid addr")
        .with_p2p(true)
        .with_p2p_minimal_preset(true)
        .with_ephemeral_key(true)
        .with_web_ui(false);
    let master = MasterServer::spawn(master_cfg)
        .await
        .expect("Master should start with P2P enabled");
    let ticket = master
        .p2p_ticket()
        .expect("Master should expose P2P ticket")
        .to_string();
    assert!(!ticket.is_empty(), "Master ticket must not be empty");

    let parse_result = rusty_grid_core::transport::parse_p2p_ticket(&ticket);
    assert!(
        parse_result.is_ok(),
        "Master generated P2P ticket must successfully parse into iroh::EndpointAddr: {:?}",
        parse_result.err()
    );

    let _ = master.shutdown();
}

#[test]
fn test_adversarial_worker_p2p_ticket_sanitization_and_lan_fallback() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    stop_worker_impl();
    let tmp = tempfile::tempdir().expect("tempdir");
    assert!(set_sandbox_dir_impl(
        tmp.path().to_string_lossy().into_owned()
    ));

    // 1. Whitespace ticket should be sanitized and filtered to None
    let started = start_worker_impl(
        "127.0.0.1:59999".to_string(),
        Some("   \t  ".to_string()),
        "SanitizeTest".to_string(),
        2,
        1024,
        false,
        5,
    );
    assert!(started, "Worker should accept sanitized config");
    let status_json = get_worker_status_impl();
    assert!(status_json.contains("status"));

    assert!(stop_worker_impl());
    assert!(!is_running_impl());

    // 2. Completely malformed ticket
    let started_malformed = start_worker_impl(
        "127.0.0.1:59998".to_string(),
        Some("INVALID_P2P_TICKET_PAYLOAD_NOT_JSON".to_string()),
        "MalformedTest".to_string(),
        2,
        1024,
        false,
        5,
    );
    assert!(
        started_malformed,
        "Worker should initialize in-process supervisor"
    );

    // Give supervisor loop brief moment to attempt connection and fail gracefully with backoff
    std::thread::sleep(Duration::from_millis(50));

    // Worker supervisor must cleanly shut down without panic or hang
    assert!(
        stop_worker_impl(),
        "Worker with malformed ticket must stop cleanly"
    );
    assert!(!is_running_impl());
}

// ==============================================================================
// 3. Rapid Sequential Worker Start/Stop Cycles & Idempotency
// ==============================================================================

#[test]
fn test_adversarial_worker_rapid_start_stop_cycles() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    stop_worker_impl();
    let tmp = tempfile::tempdir().expect("tempdir");
    assert!(set_sandbox_dir_impl(
        tmp.path().to_string_lossy().into_owned()
    ));

    // 15 rapid start and stop cycles
    for i in 0..15 {
        let started = start_worker_impl(
            "127.0.0.1:58888".to_string(),
            None,
            format!("RapidWorker-{i}"),
            2,
            1024,
            false,
            5,
        );
        assert!(started, "Cycle {i}: Worker failed to start");
        assert!(is_running_impl(), "Cycle {i}: Worker must report running");

        let stopped = stop_worker_impl();
        assert!(stopped, "Cycle {i}: stop_worker_impl must succeed");
        assert!(!is_running_impl(), "Cycle {i}: Worker must report stopped");
    }
}

#[test]
fn test_adversarial_double_start_and_double_stop_idempotency() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    stop_worker_impl();
    let tmp = tempfile::tempdir().expect("tempdir");
    assert!(set_sandbox_dir_impl(
        tmp.path().to_string_lossy().into_owned()
    ));

    // 1. Double start worker
    assert!(start_worker_impl(
        "127.0.0.1:57777".to_string(),
        None,
        "IdempotentWorker".to_string(),
        2,
        1024,
        false,
        5
    ));
    // Second start should return true without spawning duplicate thread
    assert!(start_worker_impl(
        "127.0.0.1:57777".to_string(),
        None,
        "IdempotentWorker".to_string(),
        2,
        1024,
        false,
        5
    ));
    assert!(is_running_impl());

    // 2. Double stop worker
    assert!(stop_worker_impl(), "First stop_worker should return true");
    assert!(
        stop_worker_impl(),
        "Second stop_worker should be idempotent true"
    );
    assert!(!is_running_impl());
}

#[tokio::test]
async fn test_adversarial_worker_connect_via_master_p2p_ticket() {
    let _guard = ADVERSARIAL_TEST_LOCK.lock().await;
    std::env::set_var("RUSTY_GRID_P2P_MINIMAL", "1");

    stop_worker_impl();
    let tmp = tempfile::tempdir().expect("tempdir");
    assert!(set_sandbox_dir_impl(
        tmp.path().to_string_lossy().into_owned()
    ));

    let master_cfg = ServerConfig::from_addr("127.0.0.1:0")
        .expect("valid addr")
        .with_p2p(true)
        .with_p2p_minimal_preset(true)
        .with_ephemeral_key(true)
        .with_web_ui(false);
    let master = MasterServer::spawn(master_cfg)
        .await
        .expect("Master should start");
    let ticket = master
        .p2p_ticket()
        .expect("Master should expose P2P ticket")
        .to_string();

    // Start worker using this ticket
    let started = start_worker_impl(
        "127.0.0.1:0".to_string(),
        Some(ticket),
        "P2PWorker".to_string(),
        2,
        1024,
        false,
        2,
    );
    assert!(started, "Worker supervisor should spawn");

    // Settle connection. Setting up the P2P connection can take several seconds (address
    // discovery, and iroh's public relays when no direct path works), and 5 s was not enough on
    // CI, so poll for up to 30 s.
    let mut connected = false;
    for _ in 0..600 {
        if master.list_workers().await.map(|w| w.len()).unwrap_or(0) >= 1 {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let worker_count = master.list_workers().await.map(|w| w.len()).unwrap_or(0);
    assert!(
        connected,
        "Worker must successfully connect and register via master genuine P2P ticket, count: {worker_count}"
    );

    assert!(stop_worker_impl());
    let _ = master.shutdown();
}
