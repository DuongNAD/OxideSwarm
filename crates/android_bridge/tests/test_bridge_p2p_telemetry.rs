//! Integration tests for OxideSwarm Android Bridge: P2P Ticket & Mobile Telemetry.

use oxideworker::{
    get_telemetry_impl, get_worker_status_impl, is_running_impl, set_sandbox_dir_impl,
    start_worker_impl, stop_worker_impl, update_telemetry_impl,
};

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[test]
fn test_p2p_ticket_and_worker_config() {
    let _guard = TEST_LOCK.blocking_lock();
    struct MinimalPresetGuard;
    impl Drop for MinimalPresetGuard {
        fn drop(&mut self) {
            oxideworker::set_p2p_minimal_preset_for_testing(false);
        }
    }
    oxideworker::set_p2p_minimal_preset_for_testing(true);
    let _minimal_preset_guard = MinimalPresetGuard;
    stop_worker_impl();
    let tmp = tempfile::tempdir().expect("tempdir");
    assert!(set_sandbox_dir_impl(
        tmp.path().to_string_lossy().into_owned()
    ));

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
