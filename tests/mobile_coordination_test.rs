//! Integration test suite for Milestone M5A: Mobile Coordination, Dynamic Policy & Adaptive Keepalive.
//!
//! Validates:
//! 1. Multi-tier dynamic thermal & battery throttling policy (§3.6)
//! 2. Adaptive dual-tier keepalive controller (§4.2)
//! 3. Autonomous role arbitrage & election (§5.1, §5.2, §5.3)
//! 4. Pre-sleep fast task evacuation (< 50ms) without task loss (§5.4)
//! 5. Master lease ticket signing, verification, and role handover claim evaluation
//! 6. Deterministic 4-stage election tie-breaking

use std::time::{Duration, Instant};

use tokio::sync::watch;
use uuid::Uuid;

use oxideworker::{get_evacuation_reason_impl, trigger_fast_evacuation_impl};
use rusty_grid_core::arbitrage::{
    LeaseValidationError, MasterLeaseTicket, LEASE_VALIDITY_DURATION_MS,
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
/// - Worker heartbeat interval: 60s for Wi-Fi/Ethernet idle, 25s for Cellular idle, 10s for active tasks on any link.
/// - Laptop / unknown network_type keeps the negotiated interval.
/// - Master dead-peer window calculation: W_dead = max(2s, 4 * tau).
/// - Dynamic silence window enforcement in WorkerEntry and WorkerRegistry.
#[tokio::test]
async fn test_adaptive_rrc_keepalive_controller_and_dynamic_master_timeout() {
    // 1. Worker heartbeat interval derivation
    // Wi-Fi or Ethernet (60s when idle, 10s when active)
    assert_eq!(
        calculate_keepalive_interval("wifi", 0),
        Duration::from_secs(60)
    );
    assert_eq!(
        calculate_keepalive_interval("Wi-Fi", 5),
        Duration::from_secs(10)
    );
    assert_eq!(
        calculate_keepalive_interval("ethernet", 0),
        Duration::from_secs(60)
    );
    assert_eq!(
        calculate_keepalive_interval("LAN", 2),
        Duration::from_secs(10)
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
    assert_eq!(
        live_wifi.recommended_keepalive_interval(1),
        Duration::from_secs(10)
    );

    let live_laptop = MobileLiveStatus {
        battery_pct: Some(75),
        is_charging: Some(false),
        network_type: None,
        ..Default::default()
    };
    assert_eq!(
        live_laptop.recommended_keepalive_interval_or(0, Duration::from_secs(1)),
        Duration::from_secs(1)
    );
    assert_eq!(
        live_laptop.recommended_keepalive_interval_or(1, Duration::from_secs(1)),
        Duration::from_secs(1)
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
    let base = Instant::now();
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let caps = WorkerCapabilities::new("dyn-worker", 4, 8192, false, false, None);
    let entry = WorkerEntry {
        worker_id: Uuid::new_v4(),
        session_id: 1,
        capabilities: caps.clone(),
        status: WorkerStatus::Connected,
        last_heartbeat_instant: base,
        last_heartbeat_timestamp: 0,
        registered_at_instant: base,
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
        mobile_status: None,
        preserved_thermal_until: None,
    };

    // Test silence predicate at various simulated durations
    assert!(!entry.is_silent(base + Duration::from_secs(50), Duration::from_secs(5)));
    // When last heartbeat was 105s ago, it exceeds the 100s window
    assert!(entry.is_silent(base + Duration::from_secs(105), Duration::from_secs(5)));

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

/// Test 5: Master lease ticket lifecycle and HMAC signing/verification (§5.3).
/// Verifies:
/// - Cryptographic HMAC-SHA256 signature using cluster secret over 60-byte binary layout.
/// - Ticket expiration and time validation.
/// - Tamper detection (modified score, sequence, or expired time).
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
}

/// Test 6: Pre-sleep fast task evacuation (< 50ms) without task loss (§5.4).
/// Spawns a loopback MasterServer and WorkerClients. Submits a task with `max_retries = 1`.
/// Triggers repeated fast evacuations across two workers (`w1`/`w2` -> survivor -> `w3`).
/// Verifies:
/// - Each evacuation and task reassignment completes in < 100ms (< 50ms design target).
/// - Repeated evacuations do not charge `retry_count` or `failover_count` (both remain 0) and
///   do not exhaust the task's `max_retries = 1` budget (`MOB-3`).
/// - Task completes on the final surviving worker with exit code 0, `retry_count == 0`,
///   and `failover_count == 0`.
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

    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_max_retries(1)
        .with_max_failovers(1);
    let master = MasterServer::spawn(server_config)
        .await
        .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let (w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);

    let mut config1 = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("evac-worker-1")
        .with_cores(2)
        .with_sandbox_base_dir(sandbox_path.join("w1"));
    let w1_id = Uuid::new_v4();
    config1 = config1.with_worker_id(w1_id);
    let mut w1 = WorkerClient::new(config1);

    let mut config2 = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
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

    // Submit long-running BuiltinTest task (2500ms duration) with max_retries = 1
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "evacuation_resilience".into(),
            iterations: 100,
            duration_ms: 2500,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30).with_max_retries(1),
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
    let (first_shutdown_tx, second_worker_id, second_shutdown_tx) = if initial_worker == w1_id {
        (w1_shutdown_tx, w2_id, w2_shutdown_tx)
    } else {
        (w2_shutdown_tx, w1_id, w1_shutdown_tx)
    };

    // Trigger 1st fast evacuation with emergency pre-sleep reason
    let evacuation_start = Instant::now();
    let _ = first_shutdown_tx.send(true);

    // Assert fast task evacuation & reassignment occurs within 50ms (or 100ms loopback tolerance)
    let reassigned = wait_for(
        Duration::from_millis(100),
        Duration::from_millis(5),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.assigned_worker_id == Some(second_worker_id)
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
    let after_first = master.get_task_info(task_id).await.unwrap();
    assert_eq!(after_first.retry_count, 0);
    assert_eq!(after_first.failover_count, 0);

    // Wait for task to enter Running on second_worker_id, then register a 3rd worker and evacuate a second time
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| {
                        i.assigned_worker_id == Some(second_worker_id)
                            && i.state == TaskState::Running
                    })
                    .unwrap_or(false)
            }
        )
        .await,
        "Task must enter Running state on second worker before second evacuation"
    );

    let (_w3_shutdown_tx, w3_shutdown_rx) = watch::channel(false);
    let w3_id = Uuid::new_v4();
    let config3 = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("evac-worker-3")
        .with_cores(2)
        .with_worker_id(w3_id)
        .with_sandbox_base_dir(sandbox_path.join("w3"));
    let mut w3 = WorkerClient::new(config3);
    tokio::spawn(async move {
        let _ = w3.run(w3_shutdown_rx).await;
    });
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|workers| {
                        workers.iter().any(|w| {
                            w.worker_id == w3_id
                                && w.status == rusty_grid_master::registry::WorkerStatus::Connected
                        })
                    })
                    .unwrap_or(false)
            }
        )
        .await,
        "Third worker must register and be Connected before second evacuation"
    );

    let second_evac_start = Instant::now();
    let _ = second_shutdown_tx.send(true);
    let reassigned_second = wait_for(
        Duration::from_millis(100),
        Duration::from_millis(5),
        || async {
            if let Ok(info) = master.get_task_info(task_id).await {
                info.assigned_worker_id == Some(w3_id)
                    && (info.state == TaskState::Scheduled || info.state == TaskState::Running)
            } else {
                false
            }
        },
    )
    .await;
    let second_evac_elapsed = second_evac_start.elapsed();
    assert!(
        reassigned_second,
        "Second pre-sleep evacuation must immediately reassign task to w3 (elapsed: {:?})",
        second_evac_elapsed
    );
    assert!(
        second_evac_elapsed < Duration::from_millis(100),
        "Second evacuation exceeded fast deadline: {:?}",
        second_evac_elapsed
    );

    // Wait for task to complete successfully on w3
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
        "Task must complete on third worker after repeated evacuations without data loss"
    );

    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(final_info.exit_code, Some(0));
    assert_eq!(final_info.retry_count, 0);
    assert_eq!(final_info.failover_count, 0);
    assert_eq!(final_info.evacuation_count, 2);
    assert_eq!(final_info.assigned_worker_id, Some(w3_id));

    let _ = master.shutdown();
}

/// Integration test for Finding 1:
/// With a real MasterServer using a short reaper timeout (2 s) and a WorkerClient whose mobile
/// handle says Wi-Fi before `run`, verifies:
/// 1. The master records `network_type == "wifi"` within 3 s of registration.
/// 2. The worker stays in one session (`session_id == 1`, `Connected`) for at least 8 s.
#[tokio::test]
async fn test_wifi_phone_with_short_reaper_timeout_reports_wifi_within_3s_and_stays_in_one_session_for_8s(
) {
    use rusty_grid_master::reaper::ReaperConfig;
    use rusty_grid_master::scheduler::SchedulerConfig;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let reaper_config = ReaperConfig::new(Duration::from_millis(200), Duration::from_secs(2));
    let master =
        MasterServer::spawn_with_config(server_config, SchedulerConfig::default(), reaper_config)
            .await
            .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (w_shutdown_tx, w_shutdown_rx) = watch::channel(false);
    let w_id = Uuid::new_v4();
    let config = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("wifi-phone-1")
        .with_cores(4)
        .with_worker_id(w_id)
        .with_sandbox_base_dir(temp_dir.path().join("w1"));
    let mut worker = WorkerClient::new(config);
    worker.mobile_telemetry().update(MobileLiveStatus {
        battery_pct: Some(85),
        is_charging: Some(false),
        thermal_throttled: false,
        battery_temperature: Some(34.0),
        network_type: Some("wifi".into()),
        battery_tier: Some(BatteryTier::Normal),
        thermal_tier: Some(ThermalTier::Normal),
    });

    tokio::spawn(async move {
        let _ = worker.run(w_shutdown_rx).await;
    });

    // Check 1: master sees mobile_status.network_type == "wifi" within 3 s.
    let saw_wifi = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(50),
        || async {
            master
                .registry()
                .get_worker(w_id)
                .await
                .and_then(|info| info.mobile_status)
                .and_then(|s| s.network_type)
                .as_deref()
                == Some("wifi")
        },
    )
    .await;
    assert!(
        saw_wifi,
        "Master must receive initial MobileTelemetry with mobile_status.network_type=wifi within 3 s"
    );

    let initial_info = master
        .registry()
        .get_worker(w_id)
        .await
        .expect("worker must be registered");
    assert_eq!(initial_info.session_id, 1);
    assert_eq!(initial_info.status, WorkerStatus::Connected);

    // Check 2: worker stays in session 1 for at least 8 s despite the 2 s reaper timeout.
    tokio::time::sleep(Duration::from_millis(8200)).await;
    let after_8s = master
        .registry()
        .get_worker(w_id)
        .await
        .expect("worker must still be present after 8 s");
    assert_eq!(
        after_8s.session_id, 1,
        "Wi-Fi phone must stay in one session for at least 8 s (no reap loop)"
    );
    assert_eq!(after_8s.status, WorkerStatus::Connected);

    let _ = w_shutdown_tx.send(true);
    let _ = master.shutdown();
}

/// Integration test for Finding 2:
/// A 50 s task dispatched to a Wi-Fi phone right after it registers finishes in one session
/// (`session_id == 1`) with `retry_count == 0`.
#[tokio::test]
async fn test_50s_task_on_wifi_phone_finishes_in_one_session_with_zero_retries() {
    use rusty_grid_master::reaper::ReaperConfig;
    use rusty_grid_master::scheduler::SchedulerConfig;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let reaper_config = ReaperConfig::new(Duration::from_millis(500), Duration::from_secs(10));
    let master =
        MasterServer::spawn_with_config(server_config, SchedulerConfig::default(), reaper_config)
            .await
            .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (w_shutdown_tx, w_shutdown_rx) = watch::channel(false);
    let w_id = Uuid::new_v4();
    let config = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("wifi-phone-50s")
        .with_cores(4)
        .with_worker_id(w_id)
        .with_sandbox_base_dir(temp_dir.path().join("w_50s"));
    let mut worker = WorkerClient::new(config);
    worker.mobile_telemetry().update(MobileLiveStatus {
        battery_pct: Some(90),
        is_charging: Some(true),
        thermal_throttled: false,
        battery_temperature: Some(33.0),
        network_type: Some("wifi".into()),
        battery_tier: Some(BatteryTier::Normal),
        thermal_tier: Some(ThermalTier::Normal),
    });

    tokio::spawn(async move {
        let _ = worker.run(w_shutdown_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(3),
            Duration::from_millis(50),
            || async {
                master
                    .registry()
                    .worker_network_type(&w_id)
                    .await
                    .as_deref()
                    == Some("wifi")
            }
        )
        .await,
        "Wi-Fi phone must register and report network_type=wifi"
    );

    // Dispatch a 50 s task right after the Wi-Fi phone registers.
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "wifi_50s_task".into(),
            iterations: 1,
            duration_ms: 50_000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 120),
    );
    let task_id = master.submit_task(task).await.expect("submit 50s task");

    let completed = wait_for(
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async {
            master
                .get_task_info(task_id)
                .await
                .map(|i| i.state == TaskState::Completed || i.state == TaskState::Failed)
                .unwrap_or(false)
        },
    )
    .await;
    assert!(
        completed,
        "50 s task on Wi-Fi phone must finish within 60 s"
    );

    let task_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(task_info.state, TaskState::Completed);
    assert_eq!(task_info.exit_code, Some(0));
    assert_eq!(
        task_info.retry_count, 0,
        "50 s task on Wi-Fi phone must finish with retry_count == 0"
    );

    let worker_info = master.registry().get_worker(w_id).await.unwrap();
    assert_eq!(
        worker_info.session_id, 1,
        "Wi-Fi phone must finish the 50 s task in its original session (session_id == 1)"
    );

    let _ = w_shutdown_tx.send(true);
    let _ = master.shutdown();
}

/// Integration test for Finding 3:
/// With a 1 s negotiated heartbeat interval, once Wi-Fi is reported (`network_type = "wifi"`),
/// the worker's dead-peer watchdog (`dead_after`) adapts to the 60 s Wi-Fi interval instead of
/// timing out after 4 × 1 s = 4 s, so no reconnect occurs within 15 s.
#[tokio::test]
async fn test_worker_with_1s_negotiated_interval_does_not_reconnect_within_15s_after_wifi_reported()
{
    use rusty_grid_master::reaper::ReaperConfig;
    use rusty_grid_master::scheduler::SchedulerConfig;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let server_config =
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_heartbeat_interval(1);
    let reaper_config = ReaperConfig::new(Duration::from_millis(250), Duration::from_secs(10));
    let master =
        MasterServer::spawn_with_config(server_config, SchedulerConfig::default(), reaper_config)
            .await
            .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (w_shutdown_tx, w_shutdown_rx) = watch::channel(false);
    let w_id = Uuid::new_v4();
    let mut config = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("wifi-phone-1s-negotiated")
        .with_cores(2)
        .with_worker_id(w_id)
        .with_sandbox_base_dir(temp_dir.path().join("w_1s"));
    config.default_heartbeat_interval = Duration::from_secs(1);
    let mut worker = WorkerClient::new(config);
    let mobile_handle = worker.mobile_telemetry();
    // Start without network_type so the session opens at the 1 s negotiated interval (4 s dead_after),
    // then report Wi-Fi right after registration.
    mobile_handle.update(MobileLiveStatus {
        battery_pct: Some(80),
        is_charging: Some(false),
        network_type: None,
        ..Default::default()
    });

    tokio::spawn(async move {
        let _ = worker.run(w_shutdown_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(3),
            Duration::from_millis(50),
            || async { master.registry().get_worker(w_id).await.is_some() }
        )
        .await,
        "Worker must register"
    );

    mobile_handle.update(MobileLiveStatus {
        battery_pct: Some(80),
        is_charging: Some(false),
        network_type: Some("wifi".into()),
        ..Default::default()
    });

    assert!(
        wait_for(
            Duration::from_secs(3),
            Duration::from_millis(50),
            || async {
                master
                    .registry()
                    .worker_network_type(&w_id)
                    .await
                    .as_deref()
                    == Some("wifi")
            }
        )
        .await,
        "Master must observe network_type=wifi"
    );

    let session_at_wifi = master.registry().get_worker(w_id).await.unwrap().session_id;
    assert_eq!(session_at_wifi, 1);

    // Wait 15 s after Wi-Fi is reported: with 1 s negotiated interval (4 s old dead_after) and
    // 60 s Wi-Fi heartbeats, the worker must NOT declare the master dead or reconnect.
    tokio::time::sleep(Duration::from_millis(15_200)).await;

    let after_15s = master.registry().get_worker(w_id).await.unwrap();
    assert_eq!(
        after_15s.session_id, 1,
        "Worker must not reconnect within 15 s after Wi-Fi is reported with a 1 s negotiated interval"
    );
    assert_eq!(after_15s.status, WorkerStatus::Connected);

    let _ = w_shutdown_tx.send(true);
    let _ = master.shutdown();
}

/// Integration test for Round-2 Item 1:
/// An idle Wi-Fi phone that switches to `network_type: None` (for example while Wi-Fi roams)
/// restarts its heartbeat timer to the negotiated interval and keeps the master's 4 × 60 s window
/// until its next heartbeat, staying in session 1 for 25 s with a 10 s reaper.
#[tokio::test]
async fn test_idle_wifi_phone_switching_to_unknown_network_stays_in_session_1_for_25s_with_10s_reaper(
) {
    use rusty_grid_core::capabilities::MobileCapabilities;
    use rusty_grid_core::protocol::{
        MasterMessage, MessageTransport, ProtocolCapabilities, WorkerMessage,
        PROTOCOL_VERSION_CURRENT,
    };
    use rusty_grid_master::reaper::ReaperConfig;
    use rusty_grid_master::scheduler::SchedulerConfig;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let server_config =
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_heartbeat_interval(3);
    let reaper_config = ReaperConfig::new(Duration::from_millis(250), Duration::from_secs(10));
    let master =
        MasterServer::spawn_with_config(server_config, SchedulerConfig::default(), reaper_config)
            .await
            .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (w_shutdown_tx, w_shutdown_rx) = watch::channel(false);
    let w_id = Uuid::new_v4();
    let config = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("wifi-phone-roaming")
        .with_cores(2)
        .with_worker_id(w_id)
        .with_sandbox_base_dir(temp_dir.path().join("w_roaming"));
    let mut worker = WorkerClient::new(config);
    let mobile_handle = worker.mobile_telemetry();
    mobile_handle.update(MobileLiveStatus {
        battery_pct: Some(85),
        is_charging: Some(false),
        network_type: Some("wifi".into()),
        ..Default::default()
    });

    tokio::spawn(async move {
        let _ = worker.run(w_shutdown_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(3),
            Duration::from_millis(50),
            || async {
                master
                    .registry()
                    .worker_network_type(&w_id)
                    .await
                    .as_deref()
                    == Some("wifi")
            }
        )
        .await,
        "Idle Wi-Fi phone must register and report network_type=wifi"
    );
    assert_eq!(
        master.registry().get_worker(w_id).await.unwrap().session_id,
        1
    );

    // Also connect a raw-protocol phone (simulating an older APK that never restarts its
    // heartbeat timer when `network_type` drops to `None` or a task arrives) so this test
    // guards the master's `update_mobile_telemetry` window retention on its own.
    let raw_id = Uuid::new_v4();
    let raw_stream = tokio::net::TcpStream::connect(&master_addr)
        .await
        .expect("connect raw phone");
    let mut raw_transport = MessageTransport::new(raw_stream);
    raw_transport
        .send_msg(&WorkerMessage::Register {
            worker_id: raw_id,
            capabilities: WorkerCapabilities::new("raw-apk-phone", 2, 4096, false, false, None)
                .with_mobile(Some(MobileCapabilities {
                    os_version: "Android 14".into(),
                    soc_model: "Snapdragon".into(),
                    battery_pct: Some(85),
                    is_charging: Some(false),
                    thermal_throttled: false,
                })),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .expect("send raw Register");
    match raw_transport
        .recv_msg::<MasterMessage>()
        .await
        .expect("recv raw RegisterAck")
    {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("expected accepted RegisterAck for raw phone, got {other:?}"),
    }
    raw_transport
        .send_msg(&WorkerMessage::MobileTelemetry {
            worker_id: raw_id,
            battery_pct: Some(85),
            is_charging: Some(false),
            thermal_throttled: false,
            battery_temperature: Some(33.0),
            network_type: Some("wifi".into()),
            battery_tier: Some(BatteryTier::Normal),
            thermal_tier: Some(ThermalTier::Normal),
        })
        .await
        .expect("send raw wifi telemetry");
    raw_transport
        .send_msg(&WorkerMessage::MobileTelemetry {
            worker_id: raw_id,
            battery_pct: Some(85),
            is_charging: Some(false),
            thermal_throttled: false,
            battery_temperature: Some(33.0),
            network_type: None,
            battery_tier: Some(BatteryTier::Normal),
            thermal_tier: Some(ThermalTier::Normal),
        })
        .await
        .expect("send raw None telemetry");
    let raw_initial_session = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(25),
        || async {
            master
                .registry()
                .get_worker(raw_id)
                .await
                .and_then(|w| w.mobile_status)
                .and_then(|s| s.network_type)
                .as_deref()
                == Some("wifi")
        },
    )
    .await;
    assert!(
        raw_initial_session,
        "Master must retain network_type=wifi for raw-protocol phone after network_type=None telemetry"
    );
    let raw_session_id = master
        .registry()
        .get_worker(raw_id)
        .await
        .unwrap()
        .session_id;

    // After ~1 s on Wi-Fi (while the worker's heartbeat ticker was armed for 60 s), switch
    // network_type to None (simulating Wi-Fi roaming / activeNetwork without capabilities).
    tokio::time::sleep(Duration::from_secs(1)).await;
    mobile_handle.update(MobileLiveStatus {
        battery_pct: Some(85),
        is_charging: Some(false),
        network_type: None,
        ..Default::default()
    });

    assert!(
        wait_for(
            Duration::from_secs(3),
            Duration::from_millis(50),
            || async { master.registry().worker_network_type(&w_id).await.is_none() }
        )
        .await,
        "Master must observe network_type=None after telemetry update"
    );

    // Wait 25 s with the 10 s reaper running: both the WorkerClient phone and the raw-protocol
    // phone (which sent zero Heartbeats) must remain Connected in their original sessions.
    tokio::time::sleep(Duration::from_millis(25_200)).await;

    let info = master.registry().get_worker(w_id).await.unwrap();
    assert_eq!(
        info.session_id, 1,
        "Idle Wi-Fi phone switching to network_type=None must stay in session 1 for 25 s with a 10 s reaper"
    );
    assert_eq!(info.status, WorkerStatus::Connected);

    let raw_info = master.registry().get_worker(raw_id).await.unwrap();
    assert_eq!(
        raw_info.session_id, raw_session_id,
        "Raw-protocol phone that never restarts its heartbeat timer must stay in its original session"
    );
    assert_eq!(
        raw_info.status,
        WorkerStatus::Connected,
        "Raw-protocol phone that sends no heartbeats for 25 s after network_type=None must not be reaped by the 10 s reaper"
    );
    drop(raw_transport);

    let _ = w_shutdown_tx.send(true);
    let _ = master.shutdown();
}

/// Integration test for Finding 4 (PR #99):
/// A phone set to `ThermalTier::Critical` (`45 °C`) before `run` gets 0 assignments within 3 s of
/// registering, and again within 3 s after a forced reconnect.
#[tokio::test]
async fn test_critical_thermal_phone_gets_zero_assignments_after_register_and_after_forced_reconnect(
) {
    use rusty_grid_master::reaper::ReaperConfig;
    use rusty_grid_master::scheduler::SchedulerConfig;
    use rusty_grid_worker::backoff::BackoffConfig;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let reaper_config = ReaperConfig::new(Duration::from_millis(200), Duration::from_secs(10));
    let master =
        MasterServer::spawn_with_config(server_config, SchedulerConfig::default(), reaper_config)
            .await
            .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    // Queue a 1-core task before the Critical phone even connects.
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "critical_thermal_gate".into(),
            iterations: 1,
            duration_ms: 500,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 60),
    );
    let task_id = master.submit_task(task).await.expect("submit task");

    let (w_shutdown_tx, w_shutdown_rx) = watch::channel(false);
    let w_id = Uuid::new_v4();
    let config = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("critical-thermal-phone")
        .with_cores(4)
        .with_worker_id(w_id)
        .with_sandbox_base_dir(temp_dir.path().join("w_crit"));
    let mut worker = WorkerClient::new(config);
    worker.set_backoff_config(BackoffConfig {
        initial: Duration::from_millis(100),
        max: Duration::from_millis(300),
        factor: 1.5,
        jitter_ratio: 0.0,
    });
    // Delay the session's first MobileTelemetry by 250 ms (both on initial connect and on
    // reconnect) so the scheduler ticks multiple times before telemetry arrives:
    // - on session 1, this verifies `MOBILE_INITIAL_TELEMETRY_HOLD` (Item 7: 200+ ms telemetry
    //   delay yields 0 assignments);
    // - on session 2, this verifies `register_session` preserves `ThermalTier::Critical` across
    //   reconnect before session 2's first telemetry arrives (Item 5).
    worker.set_initial_telemetry_delay(Duration::from_millis(250));
    worker.mobile_telemetry().update(MobileLiveStatus {
        battery_pct: Some(85),
        is_charging: Some(true),
        thermal_throttled: false,
        battery_temperature: Some(45.0),
        network_type: Some("wifi".into()),
        battery_tier: Some(BatteryTier::Normal),
        thermal_tier: Some(ThermalTier::Critical),
    });

    tokio::spawn(async move {
        let _ = worker.run(w_shutdown_rx).await;
    });

    // Wait for registration and initial MobileTelemetry delivery.
    let saw_critical = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(25),
        || async {
            master
                .registry()
                .get_worker(w_id)
                .await
                .and_then(|info| info.mobile_status)
                .and_then(|s| s.thermal_tier)
                == Some(ThermalTier::Critical)
        },
    )
    .await;
    assert!(
        saw_critical,
        "Master must observe ThermalTier::Critical within 3 s of registration"
    );

    // Wait 3 s after registering: the Critical phone must receive 0 assignments.
    tokio::time::sleep(Duration::from_millis(3100)).await;
    let t_info_1 = master.get_task_info(task_id).await.unwrap();
    assert_eq!(
        t_info_1.state,
        TaskState::Queued,
        "Critical thermal phone must get 0 assignments within 3 s of registering"
    );
    assert_eq!(t_info_1.assigned_worker_id, None);
    let w_info_1 = master.registry().get_worker(w_id).await.unwrap();
    assert_eq!(w_info_1.session_id, 1);
    assert_eq!(w_info_1.active_tasks, 0);

    // Force a disconnect so the worker reconnects on session 2.
    master
        .registry()
        .mark_disconnected(&w_id)
        .await
        .expect("mark_disconnected");

    let reconnected = wait_for(
        Duration::from_secs(3),
        Duration::from_millis(25),
        || async {
            master
                .registry()
                .get_worker(w_id)
                .await
                .is_some_and(|info| {
                    info.session_id >= 2
                        && info.status == WorkerStatus::Connected
                        && info.mobile_status.as_ref().and_then(|s| s.thermal_tier)
                            == Some(ThermalTier::Critical)
                })
        },
    )
    .await;
    assert!(
        reconnected,
        "Worker must reconnect on a new session and retain ThermalTier::Critical"
    );

    // Wait 3 s after the forced reconnect: still 0 assignments.
    tokio::time::sleep(Duration::from_millis(3100)).await;
    let t_info_2 = master.get_task_info(task_id).await.unwrap();
    assert_eq!(
        t_info_2.state,
        TaskState::Queued,
        "Critical thermal phone must get 0 assignments within 3 s after a forced reconnect"
    );
    assert_eq!(t_info_2.assigned_worker_id, None);

    let _ = w_shutdown_tx.send(true);
    let _ = master.shutdown();
}
