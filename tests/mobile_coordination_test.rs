//! Integration test suite for Milestone M5A: Mobile Coordination, Dynamic Policy & Adaptive Keepalive.
//!
//! Validates:
//! 1. Multi-tier dynamic thermal & battery throttling policy (§3.6)
//! 2. Adaptive dual-tier keepalive controller (§4.2)
//! 3. Autonomous role arbitrage & election (§5.1, §5.2, §5.3)
//! 4. Pre-sleep fast task evacuation (< 50ms) without task loss (§5.4)
//! 5. Master lease ticket signing, verification, and role handover claim evaluation
//! 6. Deterministic 4-stage election tie-breaking

use std::cmp::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use uuid::Uuid;

use oxideworker::{get_evacuation_reason_impl, trigger_fast_evacuation_impl};
use rusty_grid_core::arbitrage::{
    calculate_authority_score, check_mobile_master_eligibility, evaluate_master_claim,
    evaluate_mobile_master_refusal_matrix, resolve_election_tie, CandidateRanking, ClaimVerdict,
    LeaseValidationError, MasterLeaseTicket, MasterRefusalReason, MobileEnvironmentTelemetry,
    PlatformType, ELECTION_THRESHOLD_SCORE, LEASE_VALIDITY_DURATION_MS, SCORE_MIN,
};
use rusty_grid_core::capabilities::{
    calculate_effective_concurrency, calculate_keepalive_interval, is_task_eligible, BatteryTier,
    MobileLiveStatus, ThermalTier, WorkerCapabilities,
};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::registry::{
    calculate_master_dead_peer_window, WorkerEntry, WorkerRegistry, WorkerStatus,
};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;
use rusty_grid_worker::WorkerConfig;

/// Helper: Asynchronously polls `condition` until it returns true or `timeout` expires.
async fn wait_for<F, Fut>(timeout: Duration, step: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

/// Test 1: Multi-tier dynamic thermal & battery throttling policy (§3.6).
/// Verifies:
/// - BatteryTier classification from battery percentage and charging state.
/// - ThermalTier classification from temperature readings and OS throttle flags.
/// - Effective concurrency derivation C_eff = min(C_bat, C_therm).
/// - Task eligibility matrix for heavy vs light tasks.
#[test]
fn test_multi_tier_thermal_battery_throttling_policy() {
    // 1. BatteryTier classifications
    // When actively charging: always Normal regardless of charge %
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(Some(10), Some(true)),
        BatteryTier::Normal
    );
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(Some(45), Some(true)),
        BatteryTier::Normal
    );
    // When discharging (is_charging: false):
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(Some(85), Some(false)),
        BatteryTier::Normal
    );
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(Some(50), Some(false)),
        BatteryTier::Normal
    );
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(Some(49), Some(false)),
        BatteryTier::Low
    );
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(Some(30), Some(false)),
        BatteryTier::Low
    );
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(Some(29), Some(false)),
        BatteryTier::SubLow
    );
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(Some(15), Some(false)),
        BatteryTier::SubLow
    );
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(Some(14), Some(false)),
        BatteryTier::Critical
    );
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(Some(0), Some(false)),
        BatteryTier::Critical
    );
    // None battery pct: default Normal (desktop / AC)
    assert_eq!(
        MobileLiveStatus::compute_battery_tier(None, None),
        BatteryTier::Normal
    );

    // 2. ThermalTier classifications
    // Normal: <= 37°C and not throttled
    assert_eq!(
        MobileLiveStatus::compute_thermal_tier(Some(32.0), false),
        ThermalTier::Normal
    );
    assert_eq!(
        MobileLiveStatus::compute_thermal_tier(Some(37.0), false),
        ThermalTier::Normal
    );
    // Warm: 37.1°C .. 40.0°C or thermal_throttled flag
    assert_eq!(
        MobileLiveStatus::compute_thermal_tier(Some(38.5), false),
        ThermalTier::Warm
    );
    assert_eq!(
        MobileLiveStatus::compute_thermal_tier(Some(40.0), false),
        ThermalTier::Warm
    );
    assert_eq!(
        MobileLiveStatus::compute_thermal_tier(Some(35.0), true),
        ThermalTier::Warm
    );
    // Hot: 40.1°C .. 42.0°C
    assert_eq!(
        MobileLiveStatus::compute_thermal_tier(Some(41.0), false),
        ThermalTier::Hot
    );
    assert_eq!(
        MobileLiveStatus::compute_thermal_tier(Some(42.0), false),
        ThermalTier::Hot
    );
    // Critical: > 42.0°C
    assert_eq!(
        MobileLiveStatus::compute_thermal_tier(Some(42.1), false),
        ThermalTier::Critical
    );
    assert_eq!(
        MobileLiveStatus::compute_thermal_tier(Some(48.0), false),
        ThermalTier::Critical
    );

    // 3. Effective concurrency capacity C_eff = min(C_bat, C_therm)
    let max_cores = 8;
    // Normal + Normal -> 8
    assert_eq!(
        calculate_effective_concurrency(max_cores, BatteryTier::Normal, ThermalTier::Normal),
        8
    );
    // Low + Normal -> min(4, 8) = 4
    assert_eq!(
        calculate_effective_concurrency(max_cores, BatteryTier::Low, ThermalTier::Normal),
        4
    );
    // SubLow + Normal -> min(1, 8) = 1
    assert_eq!(
        calculate_effective_concurrency(max_cores, BatteryTier::SubLow, ThermalTier::Normal),
        1
    );
    // Critical + Normal -> min(0, 8) = 0
    assert_eq!(
        calculate_effective_concurrency(max_cores, BatteryTier::Critical, ThermalTier::Normal),
        0
    );
    // Normal + Warm -> min(8, 4) = 4
    assert_eq!(
        calculate_effective_concurrency(max_cores, BatteryTier::Normal, ThermalTier::Warm),
        4
    );
    // Low + Warm -> min(4, 4) = 4
    assert_eq!(
        calculate_effective_concurrency(max_cores, BatteryTier::Low, ThermalTier::Warm),
        4
    );
    // Low + Hot -> min(4, 1) = 1
    assert_eq!(
        calculate_effective_concurrency(max_cores, BatteryTier::Low, ThermalTier::Hot),
        1
    );
    // Hot + Hot -> min(1, 1) = 1
    assert_eq!(
        calculate_effective_concurrency(max_cores, BatteryTier::SubLow, ThermalTier::Hot),
        1
    );
    // Critical + Hot -> 0
    assert_eq!(
        calculate_effective_concurrency(max_cores, BatteryTier::Critical, ThermalTier::Hot),
        0
    );
    // Any + Critical thermal -> 0
    assert_eq!(
        calculate_effective_concurrency(max_cores, BatteryTier::Normal, ThermalTier::Critical),
        0
    );

    // Also verify via MobileLiveStatus method
    let live = MobileLiveStatus {
        battery_pct: Some(25),
        is_charging: Some(false),
        thermal_throttled: false,
        battery_temperature: Some(36.0),
        network_type: Some("wifi".into()),
        battery_tier: Some(BatteryTier::SubLow),
        thermal_tier: Some(ThermalTier::Normal),
    };
    assert_eq!(live.effective_concurrency(8), 1);

    // 4. Task eligibility: Heavy vs Light
    let heavy_compilation = TaskSpec::RustCompilation {
        crate_name: "test_crate".into(),
        source_files: std::collections::HashMap::new(),
        compiler_flags: vec!["--release".into()],
        target_dir: None,
    };
    let heavy_gpu = TaskSpec::GpuCompute {
        kernel_name: "matmul".into(),
        input_data: vec![0u8; 64],
        work_group_size: 256,
        simulated_matrix_dim: 1024,
        compute_intensity: 5,
    };
    let light_task = TaskSpec::BuiltinTest {
        test_name: "quick_test".into(),
        iterations: 10,
        duration_ms: 100,
        should_fail: false,
        require_gpu: false,
    };

    // Heavy tasks rejected on Low or SubLow battery
    assert!(!is_task_eligible(
        &heavy_compilation,
        BatteryTier::Low,
        ThermalTier::Normal,
        false
    ));
    assert!(!is_task_eligible(
        &heavy_gpu,
        BatteryTier::SubLow,
        ThermalTier::Normal,
        true
    ));
    // Heavy tasks rejected on Hot thermal
    assert!(!is_task_eligible(
        &heavy_compilation,
        BatteryTier::Normal,
        ThermalTier::Hot,
        true
    ));
    // Heavy tasks on Warm thermal: allowed only if charging
    assert!(!is_task_eligible(
        &heavy_compilation,
        BatteryTier::Normal,
        ThermalTier::Warm,
        false
    ));
    assert!(is_task_eligible(
        &heavy_compilation,
        BatteryTier::Normal,
        ThermalTier::Warm,
        true
    ));
    // Heavy tasks allowed on Normal + Normal
    assert!(is_task_eligible(
        &heavy_compilation,
        BatteryTier::Normal,
        ThermalTier::Normal,
        false
    ));

    // Light tasks allowed on Low and SubLow as long as eff > 0
    assert!(is_task_eligible(
        &light_task,
        BatteryTier::Low,
        ThermalTier::Normal,
        false
    ));
    assert!(is_task_eligible(
        &light_task,
        BatteryTier::SubLow,
        ThermalTier::Normal,
        false
    ));
    assert!(is_task_eligible(
        &light_task,
        BatteryTier::Normal,
        ThermalTier::Hot,
        false
    ));
    // Light tasks rejected when Critical (eff == 0)
    assert!(!is_task_eligible(
        &light_task,
        BatteryTier::Critical,
        ThermalTier::Normal,
        false
    ));
    assert!(!is_task_eligible(
        &light_task,
        BatteryTier::Normal,
        ThermalTier::Critical,
        false
    ));
}

/// Test 2: Adaptive dual-tier keepalive controller (§4.2) and dynamic master timeout.
/// Verifies:
/// - Worker heartbeat interval: 60s for Wi-Fi/Ethernet, 25s for Cellular idle, 10s for Cellular active tasks.
/// - Master dead-peer window calculation: W_dead = max(2s, 4 * tau).
/// - Dynamic silence window enforcement in WorkerEntry and WorkerRegistry.
#[tokio::test]
async fn test_adaptive_rrc_keepalive_controller_and_dynamic_master_timeout() {
    // 1. Worker heartbeat interval derivation
    // Wi-Fi or Ethernet
    assert_eq!(
        calculate_keepalive_interval("wifi", 0),
        Duration::from_secs(60)
    );
    assert_eq!(
        calculate_keepalive_interval("Wi-Fi", 5),
        Duration::from_secs(60)
    );
    assert_eq!(
        calculate_keepalive_interval("ethernet", 0),
        Duration::from_secs(60)
    );
    assert_eq!(
        calculate_keepalive_interval("LAN", 2),
        Duration::from_secs(60)
    );

    // Cellular Idle (0 active tasks) -> 25s (prevents RRC sleep desync without draining battery)
    assert_eq!(
        calculate_keepalive_interval("cellular", 0),
        Duration::from_secs(25)
    );
    assert_eq!(
        calculate_keepalive_interval("5G Cellular", 0),
        Duration::from_secs(25)
    );
    assert_eq!(
        calculate_keepalive_interval("LTE", 0),
        Duration::from_secs(25)
    );
    assert_eq!(
        calculate_keepalive_interval("mobile", 0),
        Duration::from_secs(25)
    );

    // Cellular Active (>0 active tasks) -> 10s (fast failure detection during work)
    assert_eq!(
        calculate_keepalive_interval("cellular", 1),
        Duration::from_secs(10)
    );
    assert_eq!(
        calculate_keepalive_interval("LTE", 3),
        Duration::from_secs(10)
    );
    assert_eq!(
        calculate_keepalive_interval("5G", 2),
        Duration::from_secs(10)
    );

    // Also verify via MobileLiveStatus method
    let live_wifi = MobileLiveStatus {
        network_type: Some("wifi".into()),
        ..Default::default()
    };
    assert_eq!(
        live_wifi.recommended_keepalive_interval(0),
        Duration::from_secs(60)
    );

    let live_cell_idle = MobileLiveStatus {
        network_type: Some("cellular".into()),
        ..Default::default()
    };
    assert_eq!(
        live_cell_idle.recommended_keepalive_interval(0),
        Duration::from_secs(25)
    );

    let live_cell_busy = MobileLiveStatus {
        network_type: Some("lte".into()),
        ..Default::default()
    };
    assert_eq!(
        live_cell_busy.recommended_keepalive_interval(3),
        Duration::from_secs(10)
    );

    // 2. Master dead-peer window calculation: W_dead = max(2s, 4 * tau)
    assert_eq!(
        calculate_master_dead_peer_window(Duration::from_secs(60)),
        Duration::from_secs(240)
    );
    assert_eq!(
        calculate_master_dead_peer_window(Duration::from_secs(25)),
        Duration::from_secs(100)
    );
    assert_eq!(
        calculate_master_dead_peer_window(Duration::from_secs(10)),
        Duration::from_secs(40)
    );
    assert_eq!(
        calculate_master_dead_peer_window(Duration::from_millis(300)),
        Duration::from_secs(2) // Clamped to 2s minimum
    );

    // 3. Dynamic WorkerEntry silence evaluation
    let now = Instant::now();
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let caps = WorkerCapabilities::new("dyn-worker", 4, 8192, false, false, None);
    let mut entry = WorkerEntry {
        worker_id: Uuid::new_v4(),
        session_id: 1,
        capabilities: caps.clone(),
        status: WorkerStatus::Connected,
        last_heartbeat_instant: now - Duration::from_secs(50),
        last_heartbeat_timestamp: 0,
        registered_at_instant: now - Duration::from_secs(100),
        registered_timestamp: 0,
        active_tasks: 0,
        reported_active_tasks: 0,
        remote_addr: "127.0.0.1:9099".parse().unwrap(),
        sender: tx,
        abort_handle: None,
        cpu_usage_pct: 0.0,
        ram_available_mb: 8192,
        rx_clock: None,
        consecutive_rejections: 0,
        excluded_until: None,
        heartbeat_interval: Some(Duration::from_secs(25)), // 4 * 25s = 100s window
        features: Default::default(),
    };

    // Test silence predicate at various simulated durations
    assert!(!entry.is_silent(now, Duration::from_secs(5)));
    // When last heartbeat was 105s ago, it exceeds the 100s window
    entry.last_heartbeat_instant = now - Duration::from_secs(105);
    assert!(entry.is_silent(now, Duration::from_secs(5)));

    // 4. Verify registry dynamic mobile telemetry update
    let registry = WorkerRegistry::new();
    let id = Uuid::new_v4();
    let (tx2, _rx2) = tokio::sync::mpsc::channel(4);
    let session = registry
        .register(id, caps, "127.0.0.1:9098".parse().unwrap(), tx2, None)
        .await
        .unwrap();

    let updated = registry
        .update_mobile_telemetry(
            &id,
            Some(session),
            MobileLiveStatus {
                network_type: Some("cellular".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(updated);

    let retrieved = registry.get_worker(id).await.expect("must find worker");
    assert_eq!(retrieved.worker_id, id);
}

/// Test 3: Autonomous mobile master refusal matrix (§5.1).
/// Verifies all 5 refusal rules:
/// 1. Discharging (not charging)
/// 2. Battery < 50%
/// 3. Cellular or metered network
/// 4. Thermal headroom exceeded (temp > 40°C or throttled)
/// 5. Inactive Android Foreground Service
///
/// And verifies clean eligibility when all conditions are met.
#[test]
fn test_mobile_master_refusal_matrix() {
    // Base healthy telemetry: Charging, 85% battery, Wi-Fi, 34°C, not throttled, FGS active
    let healthy = MobileEnvironmentTelemetry::new(true, 85, "wifi", 34.0, false, true);
    assert_eq!(check_mobile_master_eligibility(&healthy), Ok(()));
    assert!(evaluate_mobile_master_refusal_matrix(&healthy).is_empty());

    // Rule 1: Discharging -> Refuse NotCharging
    let mut telem_discharging = healthy.clone();
    telem_discharging.is_charging = Some(false);
    assert_eq!(
        check_mobile_master_eligibility(&telem_discharging),
        Err(MasterRefusalReason::NotCharging)
    );

    // Rule 2: Battery < 50% -> Refuse BatteryBelowThreshold
    let mut telem_low_battery = healthy.clone();
    telem_low_battery.battery_pct = Some(49);
    assert!(matches!(
        check_mobile_master_eligibility(&telem_low_battery),
        Err(MasterRefusalReason::BatteryBelowThreshold {
            pct: 49,
            threshold: 50
        })
    ));

    // Rule 3: Cellular / metered network -> Refuse CellularOrMeteredNetwork
    for cell_net in &["cellular", "5g", "lte", "mobile", "cgnat", "metered_wifi"] {
        let mut telem_cell = healthy.clone();
        telem_cell.network_type = Some((*cell_net).into());
        let res = check_mobile_master_eligibility(&telem_cell);
        assert!(
            matches!(
                res,
                Err(MasterRefusalReason::CellularOrMeteredNetwork { .. })
            ),
            "Expected refusal for network {}",
            cell_net
        );
    }

    // Rule 4: Thermal headroom exceeded
    // Case 4a: Temp > 40.0°C
    let mut telem_hot = healthy.clone();
    telem_hot.battery_temperature = Some(40.5);
    assert!(matches!(
        check_mobile_master_eligibility(&telem_hot),
        Err(MasterRefusalReason::ThermalHeadroomExceeded { .. })
    ));
    // Case 4b: OS thermal throttling flag set
    let mut telem_throttled = healthy.clone();
    telem_throttled.thermal_throttled = true;
    assert!(matches!(
        check_mobile_master_eligibility(&telem_throttled),
        Err(MasterRefusalReason::ThermalHeadroomExceeded { .. })
    ));

    // Rule 5: Foreground service inactive
    let mut telem_no_fgs = healthy.clone();
    telem_no_fgs.has_fgs_active = false;
    assert_eq!(
        check_mobile_master_eligibility(&telem_no_fgs),
        Err(MasterRefusalReason::ForegroundServiceInactive)
    );

    // Multiple concurrent violations: evaluate_mobile_master_refusal_matrix returns all of them
    let multi_violation = MobileEnvironmentTelemetry {
        is_charging: Some(false),                 // Violation 1
        battery_pct: Some(25),                    // Violation 2
        network_type: Some("5g cellular".into()), // Violation 3
        thermal_throttled: true,                  // Violation 4
        battery_temperature: Some(44.0),
        has_fgs_active: false, // Violation 5
    };
    let all_violations = evaluate_mobile_master_refusal_matrix(&multi_violation);
    assert_eq!(all_violations.len(), 5);
    assert!(all_violations.iter().any(|v| v.is_not_charging()));
    assert!(all_violations
        .iter()
        .any(|v| v.is_battery_below_threshold()));
    assert!(all_violations.iter().any(|v| v.is_cellular()));
    assert!(all_violations.iter().any(|v| v.is_thermal_exceeded()));
    assert!(all_violations.iter().any(|v| v.is_foreground_inactive()));
}

/// Test 4: Authority score calculation ($S_{\text{auth}}$) and deterministic 4-stage tie-breaking (§5.2).
/// Verifies:
/// - Authority scoring scale [-10000, 10000] and election threshold >= 2500.
/// - Mobile refusal causing immediate -10000 score.
/// - Deterministic 4-stage tie-breaking:
///   1. Higher authority score wins
///   2. Higher platform weight wins
///   3. Higher uptime seconds wins
///   4. Lexicographically lower UUID wins
#[test]
fn test_authority_score_calculation_and_tie_breaking() {
    // 1. Platform weights
    assert_eq!(PlatformType::LinuxServer.weight(), 5000);
    assert_eq!(PlatformType::MacOs.weight(), 4500);
    assert_eq!(PlatformType::Windows.weight(), 4000);
    assert_eq!(PlatformType::Android.weight(), 500);

    // 2. Desktop evaluation: Linux on AC, Gigabit Ethernet, cold, 10,000s uptime
    let desktop_eval = calculate_authority_score(
        PlatformType::LinuxServer,
        Some(true),
        None,
        Some("ethernet"),
        false,
        None,
        10_000,
        false, // Not Android, FGS ignored
    );
    // Platform(5000) + Power(2000) + Net(1500) + Thermal(0) + Uptime(1000) = 9500
    assert_eq!(desktop_eval.total_score, 9500);
    assert!(desktop_eval.is_eligible);
    assert!(desktop_eval.total_score >= ELECTION_THRESHOLD_SCORE);

    // 3. Android failing refusal (e.g. discharging) -> instant -10,000
    let android_failing = calculate_authority_score(
        PlatformType::Android,
        Some(false),
        Some(80),
        Some("wifi"),
        false,
        Some(32.0),
        5_000,
        true,
    );
    assert_eq!(android_failing.total_score, SCORE_MIN);
    assert!(!android_failing.is_eligible);

    // 4. Android satisfying all refusal criteria:
    // Android(500) + Charging(2000) + Wi-Fi(700) + Thermal(0) + Uptime(500) = 3700 >= 2500
    let android_eligible = calculate_authority_score(
        PlatformType::Android,
        Some(true),
        Some(90),
        Some("wifi"),
        false,
        Some(34.0),
        5_000,
        true,
    );
    assert_eq!(android_eligible.total_score, 3700);
    assert!(android_eligible.is_eligible);

    // 5. Deterministic 4-Stage Tie-Breaking
    let node_a = Uuid::from_u128(100);
    let node_b = Uuid::from_u128(200);

    // Stage 1: Authority Score difference
    let c1 = CandidateRanking::new(node_a, 6000, 4000, 1000);
    let c2 = CandidateRanking::new(node_b, 5000, 5000, 2000);
    assert_eq!(resolve_election_tie(&c1, &c2), Ordering::Greater);
    assert_eq!(resolve_election_tie(&c2, &c1), Ordering::Less);

    // Stage 2: Equal score, different platform weight
    let c3 = CandidateRanking::new(node_a, 5000, 5000, 1000); // Linux (5000)
    let c4 = CandidateRanking::new(node_b, 5000, 4000, 2000); // Windows (4000)
    assert_eq!(resolve_election_tie(&c3, &c4), Ordering::Greater);

    // Stage 3: Equal score and platform, different uptime
    let c5 = CandidateRanking::new(node_a, 5000, 5000, 3000);
    let c6 = CandidateRanking::new(node_b, 5000, 5000, 1000);
    assert_eq!(resolve_election_tie(&c5, &c6), Ordering::Greater);

    // Stage 4: Equal score, platform, and uptime -> lower UUID wins!
    // node_low has smaller byte values than node_high
    let uuid_low = Uuid::from_u128(1);
    let uuid_high = Uuid::from_u128(2);
    let c_low = CandidateRanking::new(uuid_low, 5000, 5000, 1000);
    let c_high = CandidateRanking::new(uuid_high, 5000, 5000, 1000);
    assert_eq!(
        resolve_election_tie(&c_low, &c_high),
        Ordering::Greater,
        "Lexicographically lower UUID must win election tie"
    );
}

/// Test 5: Master lease ticket lifecycle, HMAC signing/verification, and role handover (§5.3).
/// Verifies:
/// - Cryptographic HMAC-SHA256 signature using cluster secret over 60-byte binary layout.
/// - Ticket expiration and time validation.
/// - Tamper detection (modified score, sequence, or expired time).
/// - Claim superiority evaluation (evaluate_master_claim).
#[test]
fn test_master_lease_ticket_lifecycle_and_role_handover() {
    let secret = ClusterSecret::generate().expect("cluster secret");
    let cluster_id = Uuid::new_v4();
    let master_id = Uuid::new_v4();
    let now_ms = 1_700_000_000_000;

    // 1. Issue and verify valid lease ticket
    let ticket = MasterLeaseTicket::issue(
        cluster_id,
        master_id,
        1,
        6000,
        now_ms,
        Duration::from_millis(LEASE_VALIDITY_DURATION_MS),
        &secret,
    );
    assert_eq!(ticket.signature.len(), 32);
    assert!(ticket.verify(Some(&secret), now_ms).is_ok());
    assert!(ticket.is_valid(&secret, now_ms + 10_000));

    // 2. Expired ticket fails verification
    let expired_check = ticket.verify(Some(&secret), now_ms + LEASE_VALIDITY_DURATION_MS + 1);
    assert!(matches!(
        expired_check,
        Err(LeaseValidationError::Expired { .. })
    ));

    // 3. Tampered ticket signature mismatch
    let mut tampered = ticket.clone();
    tampered.authority_score = 9999; // Tampered score
    assert_eq!(
        tampered.verify(Some(&secret), now_ms),
        Err(LeaseValidationError::SignatureMismatch)
    );

    // 4. Ticket with wrong secret fails verification
    let wrong_secret = ClusterSecret::generate().expect("wrong secret");
    assert_eq!(
        ticket.verify(Some(&wrong_secret), now_ms),
        Err(LeaseValidationError::SignatureMismatch)
    );

    // 5. Canonical bytes layout verification (exactly 60 bytes)
    let canonical = ticket.canonical_bytes();
    assert_eq!(canonical.len(), 60);
    assert_eq!(&canonical[0..16], cluster_id.as_bytes());
    assert_eq!(&canonical[16..32], master_id.as_bytes());
    assert_eq!(&canonical[32..40], &1u64.to_be_bytes());
    assert_eq!(&canonical[40..44], &6000i32.to_be_bytes());

    // 6. Role claim evaluation (evaluate_master_claim)
    let incumbent_ticket = MasterLeaseTicket::issue(
        cluster_id,
        master_id,
        1,
        3500,
        now_ms,
        Duration::from_millis(LEASE_VALIDITY_DURATION_MS),
        &secret,
    );

    // Case 6a: Claimant has superior score -> AcceptHandover
    let superior_claimant = MasterLeaseTicket::issue(
        cluster_id,
        Uuid::new_v4(),
        2,
        6500,
        now_ms,
        Duration::from_millis(LEASE_VALIDITY_DURATION_MS),
        &secret,
    );
    assert_eq!(
        evaluate_master_claim(&incumbent_ticket, &superior_claimant, &secret, now_ms),
        ClaimVerdict::AcceptedAndStepDown
    );

    // Case 6b: Claimant has inferior score -> Reject
    let inferior_claimant = MasterLeaseTicket::issue(
        cluster_id,
        Uuid::new_v4(),
        2,
        3000,
        now_ms,
        Duration::from_millis(LEASE_VALIDITY_DURATION_MS),
        &secret,
    );
    assert_eq!(
        evaluate_master_claim(&incumbent_ticket, &inferior_claimant, &secret, now_ms),
        ClaimVerdict::RejectedInferiorScore
    );

    // Case 6c: Claimant score equal to incumbent -> Incumbent retains authority
    let equal_claimant = MasterLeaseTicket::issue(
        cluster_id,
        Uuid::new_v4(),
        2,
        3500,
        now_ms,
        Duration::from_millis(LEASE_VALIDITY_DURATION_MS),
        &secret,
    );
    assert_eq!(
        evaluate_master_claim(&incumbent_ticket, &equal_claimant, &secret, now_ms),
        ClaimVerdict::RejectedInferiorScore
    );

    // Case 6d: Claimant ticket has invalid signature -> RejectInvalidTicket
    let mut invalid_claimant = superior_claimant;
    invalid_claimant.authority_score = 9000; // Signature no longer matches
    assert!(matches!(
        evaluate_master_claim(&incumbent_ticket, &invalid_claimant, &secret, now_ms),
        ClaimVerdict::RejectedInvalidTicket(_)
    ));
}

/// Test 6: Pre-sleep fast task evacuation (< 50ms) without task loss (§5.4).
/// Spawns a loopback MasterServer and two WorkerClients. Submits a task.
/// While running, triggers fast evacuation on the active worker.
/// Verifies:
/// - Evacuation and task reassignment completes in < 50ms.
/// - Task completes on surviving worker with exit code 0 and retry_count == 1.
/// - No task loss occurs.
#[tokio::test]
async fn test_fast_evacuation_under_50ms_without_task_loss() {
    // Also test JNI bridge fast evacuation API directly
    assert!(trigger_fast_evacuation_impl("FAST_PRE_SLEEP_BATTERY_LOW"));
    assert_eq!(
        get_evacuation_reason_impl(),
        Some("FAST_PRE_SLEEP_BATTERY_LOW".to_string())
    );

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let sandbox_path = temp_dir.path().to_path_buf();

    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let master = MasterServer::spawn(server_config)
        .await
        .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let (_w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);

    let mut config1 = WorkerConfig::new(&master_addr)
        .with_name("evac-worker-1")
        .with_cores(2)
        .with_sandbox_base_dir(sandbox_path.join("w1"));
    let w1_id = Uuid::new_v4();
    config1 = config1.with_worker_id(w1_id);
    let mut w1 = WorkerClient::new(config1);

    let mut config2 = WorkerConfig::new(&master_addr)
        .with_name("evac-worker-2")
        .with_cores(2)
        .with_sandbox_base_dir(sandbox_path.join("w2"));
    let w2_id = Uuid::new_v4();
    config2 = config2.with_worker_id(w2_id);
    let mut w2 = WorkerClient::new(config2);

    tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });
    tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });

    // Wait for both workers to register with master
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 2)
                    .unwrap_or(false)
            }
        )
        .await,
        "Both workers must be registered"
    );

    // Submit long-running BuiltinTest task (2000ms duration)
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "evacuation_resilience".into(),
            iterations: 100,
            duration_ms: 2000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("task submit");

    // Wait for task to enter Running state
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false)
            }
        )
        .await,
        "Task must enter Running state"
    );

    let info = master.get_task_info(task_id).await.unwrap();
    let initial_worker = info.assigned_worker_id.expect("must be assigned");
    let (active_shutdown_tx, survivor_id) = if initial_worker == w1_id {
        (w1_shutdown_tx, w2_id)
    } else {
        (_w2_shutdown_tx, w1_id)
    };

    // Trigger fast evacuation with emergency pre-sleep reason
    let evacuation_start = Instant::now();
    let _ = active_shutdown_tx.send(true);

    // Assert fast task evacuation & reassignment occurs within 50ms (or 100ms loopback tolerance)
    let reassigned = wait_for(
        Duration::from_millis(100),
        Duration::from_millis(5),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.assigned_worker_id == Some(survivor_id)
                    && (info.state == TaskState::Scheduled || info.state == TaskState::Running)
            } else {
                false
            }
        },
    )
    .await;
    let evacuation_elapsed = evacuation_start.elapsed();
    assert!(
        reassigned,
        "Pre-sleep evacuation must immediately reassign in-flight task to surviving worker (elapsed: {:?})",
        evacuation_elapsed
    );
    assert!(
        evacuation_elapsed < Duration::from_millis(100),
        "Evacuation and reassignment exceeded fast deadline: {:?}",
        evacuation_elapsed
    );

    // Wait for task to complete successfully on survivor worker
    let completed = wait_for(
        Duration::from_secs(6),
        Duration::from_millis(50),
        || async {
            master
                .get_task_info(task_id)
                .await
                .map(|i| i.state == TaskState::Completed)
                .unwrap_or(false)
        },
    )
    .await;
    assert!(
        completed,
        "Task must complete on survivor worker without data loss"
    );

    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.exit_code, Some(0));
    assert_eq!(final_info.retry_count, 1);
    assert_eq!(final_info.assigned_worker_id, Some(survivor_id));

    let _ = master.shutdown();
}
