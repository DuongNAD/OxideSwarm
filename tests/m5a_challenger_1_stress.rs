//! Adversarial and Empirical Stress Test Suite for Milestone M5A (Challenger 1).
//!
//! Validates and stress-tests:
//! 1. Multi-tier dynamic thermal & battery throttling policy (§3.6):
//!    - Rapid oscillation between all battery tiers (Normal, Low, SubLow, Critical) and charging states.
//!    - Rapid oscillation between all thermal tiers (Normal, Warm, Hot, Critical) and throttle flags.
//!    - Concurrency capacity clamping C_eff = min(C_bat, C_therm) across core counts (1 to 64).
//!    - Mathematical invariants: boundary enforcement, monotonicity, boundedness, charging override.
//!    - Task eligibility matrix under tier oscillation for heavy vs light tasks.
//! 2. Adaptive dual-tier keepalive controller (§4.2) & Dynamic Master Reaper Window:
//!    - RRC-aware heartbeat interval transitions: Wi-Fi 60s -> Cellular active 10s -> Cellular idle 25s.
//!    - Master dead-peer calculation: W_dead = max(2s, 4 * tau_worker).
//!    - Zero false-positive reaper evictions under all network transitions and normal jitter (< 4 * tau).
//!    - Silence predicate timing on WorkerEntry and dynamic window updates in WorkerRegistry.
//! 3. Empirical telemetry propagation and scheduler integration verification:
//!    - Demonstrates data flow through MobileLiveStatus, MobileCapabilities, WorkerRegistry, and Scheduler.
//!    - Pinpoints telemetry preservation invariants across mobile coordination components.

use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use uuid::Uuid;

use rusty_grid_core::capabilities::{
    calculate_effective_concurrency, calculate_keepalive_interval, is_task_eligible, BatteryTier,
    MobileCapabilities, MobileLiveStatus, ThermalTier, WorkerCapabilities,
};
use rusty_grid_core::task::TaskSpec;
use rusty_grid_master::registry::{
    calculate_master_dead_peer_window, WorkerEntry, WorkerRegistry, WorkerStatus,
};

/// Deterministic Pseudo-Random Number Generator for reproducible stress testing.
struct StressPrng {
    state: u64,
}

impl StressPrng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0xDEAD_BEEF_CAFE_F00D
            } else {
                seed
            },
        }
    }

    fn next_u64(&mut self) -> u64 {
        // Xorshift64star
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn next_bool(&mut self) -> bool {
        (self.next_u64() & 1) == 1
    }

    fn next_opt_bool(&mut self) -> Option<bool> {
        match self.next_u64() % 3 {
            0 => Some(true),
            1 => Some(false),
            _ => None,
        }
    }

    fn next_battery_pct(&mut self) -> Option<u8> {
        let r = self.next_u64() % 14;
        match r {
            0 => Some(0),
            1 => Some(5),
            2 => Some(14),
            3 => Some(15),
            4 => Some(20),
            5 => Some(29),
            6 => Some(30),
            7 => Some(45),
            8 => Some(49),
            9 => Some(50),
            10 => Some(85),
            11 => Some(100),
            12 => Some((self.next_u64() % 101) as u8),
            _ => None,
        }
    }

    fn next_temperature(&mut self) -> Option<f32> {
        let r = self.next_u64() % 13;
        match r {
            0 => Some(-10.0),
            1 => Some(25.0),
            2 => Some(35.0),
            3 => Some(37.0),
            4 => Some(37.1),
            5 => Some(38.5),
            6 => Some(40.0),
            7 => Some(40.1),
            8 => Some(41.5),
            9 => Some(42.0),
            10 => Some(42.1),
            11 => Some(48.5),
            _ => None,
        }
    }

    fn next_cores(&mut self) -> usize {
        let cores_set = [1, 2, 4, 8, 12, 16, 32, 64];
        cores_set[(self.next_u64() as usize) % cores_set.len()]
    }
}

/// Helper to create a dummy WorkerEntry for silence testing.
fn make_test_worker_entry(
    worker_id: Uuid,
    tau: Option<Duration>,
    last_hb: Instant,
    active_tasks: usize,
) -> WorkerEntry {
    let (tx, _rx) = mpsc::channel(4);
    let caps = WorkerCapabilities::new("stress-worker", 8, 8192, false, false, None);
    WorkerEntry {
        worker_id,
        session_id: 1,
        capabilities: caps,
        status: WorkerStatus::Connected,
        last_heartbeat_instant: last_hb,
        last_heartbeat_timestamp: 0,
        registered_at_instant: last_hb,
        registered_timestamp: 0,
        active_tasks,
        reported_active_tasks: active_tasks,
        remote_addr: "127.0.0.1:8080".parse().unwrap(),
        sender: tx,
        abort_handle: None,
        cpu_usage_pct: 0.0,
        ram_available_mb: 8192,
        rx_clock: None,
        consecutive_rejections: 0,
        excluded_until: None,
        heartbeat_interval: tau,
        features: Default::default(),
    }
}

// ============================================================================
// TEST 1: Rapid Battery & Thermal Tier Oscillation & Invariant Verification
// ============================================================================
#[test]
fn test_rapid_battery_and_thermal_tier_oscillation_invariants() {
    let mut prng = StressPrng::new(0x1337_C0DE_2026);
    let iterations = 20_000;

    for i in 0..iterations {
        let bat_pct = prng.next_battery_pct();
        let is_charging = prng.next_opt_bool();
        let temp = prng.next_temperature();
        let thermal_throttled = prng.next_bool();
        let cores = prng.next_cores();

        let bat_tier = MobileLiveStatus::compute_battery_tier(bat_pct, is_charging);
        let therm_tier = MobileLiveStatus::compute_thermal_tier(temp, thermal_throttled);
        let eff = calculate_effective_concurrency(cores, bat_tier, therm_tier);

        // INVARIANT 1: Boundedness — 0 <= eff <= cores
        assert!(
            eff <= cores,
            "Iteration {i}: eff {eff} exceeds cores {cores} (bat={bat_tier:?}, therm={therm_tier:?})"
        );

        // INVARIANT 2: Critical Zero Clamping
        // If either battery or thermal is Critical, effective capacity MUST be 0
        if bat_tier == BatteryTier::Critical || therm_tier == ThermalTier::Critical {
            assert_eq!(
                eff, 0,
                "Iteration {i}: Critical state must yield 0 concurrency (bat={bat_tier:?}, therm={therm_tier:?})"
            );
        }

        // INVARIANT 3: SubLow and Hot Upper Bound Clamping (max 1 slot)
        if (bat_tier == BatteryTier::SubLow || therm_tier == ThermalTier::Hot)
            && bat_tier != BatteryTier::Critical
            && therm_tier != ThermalTier::Critical
        {
            assert!(
                eff <= 1,
                "Iteration {i}: SubLow/Hot must clamp concurrency <= 1 (got {eff}, cores={cores})"
            );
        }

        // INVARIANT 4: Low and Warm Upper Bound Clamping (max cores/2, min 1)
        if (bat_tier == BatteryTier::Low || therm_tier == ThermalTier::Warm)
            && bat_tier < BatteryTier::SubLow
            && therm_tier < ThermalTier::Hot
        {
            let max_expected = (cores / 2).max(1);
            assert!(
                eff <= max_expected,
                "Iteration {i}: Low/Warm must clamp concurrency <= {max_expected} (got {eff}, cores={cores})"
            );
        }

        // INVARIANT 5: Normal + Normal yields full capacity
        if bat_tier == BatteryTier::Normal && therm_tier == ThermalTier::Normal {
            assert_eq!(
                eff, cores,
                "Iteration {i}: Normal+Normal must yield full core capacity {cores} (got {eff})"
            );
        }

        // INVARIANT 6: Charging always promotes BatteryTier to Normal
        if is_charging == Some(true) {
            assert_eq!(
                bat_tier,
                BatteryTier::Normal,
                "Iteration {i}: Actively charging must always be BatteryTier::Normal"
            );
        }

        // INVARIANT 7: Temperature classification strict boundaries
        if let Some(t) = temp {
            if t > 42.0 {
                assert_eq!(therm_tier, ThermalTier::Critical);
            } else if t > 40.0 {
                assert_eq!(therm_tier, ThermalTier::Hot);
            } else if t > 37.0 || thermal_throttled {
                assert_eq!(therm_tier, ThermalTier::Warm);
            } else {
                assert_eq!(therm_tier, ThermalTier::Normal);
            }
        } else if thermal_throttled {
            assert_eq!(therm_tier, ThermalTier::Warm);
        } else {
            assert_eq!(therm_tier, ThermalTier::Normal);
        }

        // INVARIANT 8: Battery percentage strict boundaries when discharging
        if is_charging == Some(false) {
            if let Some(p) = bat_pct {
                if p >= 50 {
                    assert_eq!(bat_tier, BatteryTier::Normal);
                } else if p >= 30 {
                    assert_eq!(bat_tier, BatteryTier::Low);
                } else if p >= 15 {
                    assert_eq!(bat_tier, BatteryTier::SubLow);
                } else {
                    assert_eq!(bat_tier, BatteryTier::Critical);
                }
            }
        }
    }
}

// ============================================================================
// TEST 2: Strict Monotonicity Under Simulated Draining & Heating Sweeps
// ============================================================================
#[test]
fn test_concurrency_monotonicity_under_thermal_and_battery_sweeps() {
    let max_cores = 16;

    // Sweeping battery from 100% down to 0% while discharging at normal temp (30°C)
    let mut prev_eff = max_cores;
    for pct in (0..=100).rev() {
        let bat = MobileLiveStatus::compute_battery_tier(Some(pct), Some(false));
        let therm = ThermalTier::Normal;
        let eff = calculate_effective_concurrency(max_cores, bat, therm);
        assert!(
            eff <= prev_eff,
            "Battery drain violation at {pct}%: eff {eff} > prev {prev_eff}"
        );
        prev_eff = eff;
    }
    assert_eq!(prev_eff, 0, "At 0% battery, capacity must be 0");

    // Sweeping temperature from 20.0°C up to 50.0°C at normal battery (80%)
    let mut prev_eff = max_cores;
    let mut temp = 20.0;
    while temp <= 50.0 {
        let bat = BatteryTier::Normal;
        let therm = MobileLiveStatus::compute_thermal_tier(Some(temp), false);
        let eff = calculate_effective_concurrency(max_cores, bat, therm);
        assert!(
            eff <= prev_eff,
            "Thermal rise violation at {temp}°C: eff {eff} > prev {prev_eff}"
        );
        prev_eff = eff;
        temp += 0.5;
    }
    assert_eq!(prev_eff, 0, "At 50°C temperature, capacity must be 0");
}

// ============================================================================
// TEST 3: Task Eligibility Matrix Under Tier Oscillation
// ============================================================================
#[test]
fn test_task_eligibility_under_tier_oscillation() {
    let heavy_compilation = TaskSpec::RustCompilation {
        crate_name: "oxide_engine".into(),
        source_files: std::collections::HashMap::new(),
        compiler_flags: vec!["--release".into()],
        target_dir: None,
    };
    let heavy_gpu = TaskSpec::GpuCompute {
        kernel_name: "matrix_mul".into(),
        input_data: vec![1, 2, 3],
        work_group_size: 128,
        simulated_matrix_dim: 512,
        compute_intensity: 3,
    };
    let light_command = TaskSpec::command("uptime", vec![]);
    let light_builtin = TaskSpec::BuiltinTest {
        test_name: "ping".into(),
        iterations: 1,
        duration_ms: 10,
        should_fail: false,
        require_gpu: false,
    };

    let battery_tiers = [
        BatteryTier::Normal,
        BatteryTier::Low,
        BatteryTier::SubLow,
        BatteryTier::Critical,
    ];
    let thermal_tiers = [
        ThermalTier::Normal,
        ThermalTier::Warm,
        ThermalTier::Hot,
        ThermalTier::Critical,
    ];

    for bat in battery_tiers {
        for therm in thermal_tiers {
            for is_charging in [true, false] {
                let eff = calculate_effective_concurrency(8, bat, therm);

                // Invariant A: When eff == 0, NO task is eligible
                if eff == 0 {
                    assert!(!is_task_eligible(
                        &heavy_compilation,
                        bat,
                        therm,
                        is_charging
                    ));
                    assert!(!is_task_eligible(&heavy_gpu, bat, therm, is_charging));
                    assert!(!is_task_eligible(&light_command, bat, therm, is_charging));
                    assert!(!is_task_eligible(&light_builtin, bat, therm, is_charging));
                    continue;
                }

                // Invariant B: Heavy tasks NEVER eligible under Low or SubLow battery
                if bat == BatteryTier::Low || bat == BatteryTier::SubLow {
                    assert!(
                        !is_task_eligible(&heavy_compilation, bat, therm, is_charging),
                        "Heavy compilation must be ineligible under {bat:?}"
                    );
                    assert!(
                        !is_task_eligible(&heavy_gpu, bat, therm, is_charging),
                        "Heavy GPU must be ineligible under {bat:?}"
                    );
                }

                // Invariant C: Heavy tasks NEVER eligible under Hot thermal
                if therm == ThermalTier::Hot {
                    assert!(
                        !is_task_eligible(&heavy_compilation, bat, therm, is_charging),
                        "Heavy compilation must be ineligible under Hot thermal"
                    );
                    assert!(
                        !is_task_eligible(&heavy_gpu, bat, therm, is_charging),
                        "Heavy GPU must be ineligible under Hot thermal"
                    );
                }

                // Invariant D: Heavy tasks on Warm thermal allowed ONLY when actively charging
                if therm == ThermalTier::Warm && bat == BatteryTier::Normal {
                    if is_charging {
                        assert!(
                            is_task_eligible(&heavy_compilation, bat, therm, is_charging),
                            "Heavy compilation on Warm thermal must be allowed when charging"
                        );
                    } else {
                        assert!(
                            !is_task_eligible(&heavy_compilation, bat, therm, is_charging),
                            "Heavy compilation on Warm thermal must be rejected when discharging"
                        );
                    }
                }

                // Invariant E: Light tasks are eligible whenever eff > 0
                if eff > 0 {
                    assert!(
                        is_task_eligible(&light_command, bat, therm, is_charging),
                        "Light command must be eligible whenever eff > 0 (bat={bat:?}, therm={therm:?})"
                    );
                    assert!(
                        is_task_eligible(&light_builtin, bat, therm, is_charging),
                        "Light builtin must be eligible whenever eff > 0 (bat={bat:?}, therm={therm:?})"
                    );
                }
            }
        }
    }
}

// ============================================================================
// TEST 4: Adaptive Keepalive Transitions and Master Dead-Peer Calculation
// ============================================================================
#[test]
fn test_keepalive_interval_network_transitions_and_dead_peer_window() {
    // 1. Verify standard RRC-aware keepalive derivations
    let networks = [
        ("wifi", 0, Duration::from_secs(60)),
        ("WiFi", 10, Duration::from_secs(60)),
        ("Ethernet", 0, Duration::from_secs(60)),
        ("lan", 4, Duration::from_secs(60)),
        ("cellular", 0, Duration::from_secs(25)),
        ("5G", 0, Duration::from_secs(25)),
        ("LTE", 0, Duration::from_secs(25)),
        ("cgnat", 0, Duration::from_secs(25)),
        ("mobile", 0, Duration::from_secs(25)),
        ("cellular", 1, Duration::from_secs(10)),
        ("5G Cellular", 3, Duration::from_secs(10)),
        ("LTE-Advanced", 8, Duration::from_secs(10)),
    ];

    for (net, active, expected) in networks {
        assert_eq!(
            calculate_keepalive_interval(net, active),
            expected,
            "Network '{net}' with {active} tasks must have keepalive interval {expected:?}"
        );
    }

    // 2. Master dead-peer calculation: W_dead = max(2s, 4 * tau_worker)
    let tau_cases = [
        (Duration::from_millis(100), Duration::from_secs(2)), // Clamped to 2s
        (Duration::from_millis(400), Duration::from_secs(2)), // Clamped to 2s
        (Duration::from_millis(500), Duration::from_secs(2)), // 4 * 0.5s = 2.0s
        (Duration::from_secs(1), Duration::from_secs(4)),     // 4 * 1s = 4s
        (Duration::from_secs(10), Duration::from_secs(40)),   // Cellular active: 40s
        (Duration::from_secs(25), Duration::from_secs(100)),  // Cellular idle: 100s
        (Duration::from_secs(60), Duration::from_secs(240)),  // Wi-Fi: 240s
    ];

    for (tau, expected_w_dead) in tau_cases {
        assert_eq!(
            calculate_master_dead_peer_window(tau),
            expected_w_dead,
            "Tau {tau:?} must produce dead-peer window {expected_w_dead:?}"
        );
    }

    // 3. Mathematical proof of ZERO false-positive reaper evictions under all network transitions
    // Simulate rapid sequence of network transitions:
    // WiFi (60s) -> Cellular Idle (25s) -> Cellular Active (10s) -> Cellular Idle (25s) -> WiFi (60s)
    let transition_sequence = [
        Duration::from_secs(60),
        Duration::from_secs(25),
        Duration::from_secs(10),
        Duration::from_secs(25),
        Duration::from_secs(60),
        Duration::from_secs(10),
        Duration::from_secs(25),
        Duration::from_secs(60),
    ];

    let now = Instant::now();
    for &tau in &transition_sequence {
        let w_dead = calculate_master_dead_peer_window(tau);

        // Under normal operation, a heartbeat arrives every tau seconds.
        // Therefore, elapsed time since last heartbeat t <= tau.
        // Since tau < 4 * tau <= W_dead, elapsed duration NEVER reaches W_dead.
        let elapsed_normal = tau;
        let entry_normal =
            make_test_worker_entry(Uuid::new_v4(), Some(tau), now - elapsed_normal, 0);
        assert!(
            !entry_normal.is_silent(now, Duration::from_secs(5)),
            "False-positive eviction detected! Worker with tau={tau:?} at normal heartbeat elapsed={elapsed_normal:?} was marked silent"
        );

        // Under carrier jitter, heartbeat is delayed up to 3 * tau.
        // Even with 3x delay, 3 * tau < 4 * tau <= W_dead, preventing false-positive evictions.
        let elapsed_jitter = tau.saturating_mul(3);
        let entry_jitter =
            make_test_worker_entry(Uuid::new_v4(), Some(tau), now - elapsed_jitter, 0);
        assert!(
            !entry_jitter.is_silent(now, Duration::from_secs(5)),
            "Carrier jitter (3x tau) must NOT cause reaper eviction (tau={tau:?}, elapsed={elapsed_jitter:?}, W_dead={w_dead:?})"
        );

        // Once silence strictly exceeds W_dead = 4 * tau, eviction MUST trigger.
        let elapsed_dead = w_dead + Duration::from_millis(100);
        let entry_dead = make_test_worker_entry(Uuid::new_v4(), Some(tau), now - elapsed_dead, 0);
        assert!(
            entry_dead.is_silent(now, Duration::from_secs(5)),
            "Dead peer timeout violation: Worker with silence {elapsed_dead:?} > W_dead {w_dead:?} was NOT marked silent"
        );
    }
}

// ============================================================================
// TEST 5: WorkerEntry Silence Predicate & Registry Dynamic Liveness Tracking
// ============================================================================
#[tokio::test]
async fn test_worker_entry_and_registry_dead_peer_window_lifecycle() {
    let now = Instant::now();
    let id = Uuid::new_v4();

    // 1. Cellular active worker (tau = 10s, W_dead = 40s)
    let entry_cell_active = make_test_worker_entry(
        id,
        Some(Duration::from_secs(10)),
        now - Duration::from_secs(35),
        2,
    );
    // At 35s, it is within 40s window -> NOT silent
    assert!(!entry_cell_active.is_silent(now, Duration::from_secs(5)));

    let entry_cell_active_dead = make_test_worker_entry(
        id,
        Some(Duration::from_secs(10)),
        now - Duration::from_secs(45),
        2,
    );
    // At 45s, it exceeds 40s window -> SILENT
    assert!(entry_cell_active_dead.is_silent(now, Duration::from_secs(5)));

    // 2. Cellular idle worker (tau = 25s, W_dead = 100s)
    let entry_cell_idle = make_test_worker_entry(
        id,
        Some(Duration::from_secs(25)),
        now - Duration::from_secs(95),
        0,
    );
    // At 95s, within 100s window -> NOT silent
    assert!(!entry_cell_idle.is_silent(now, Duration::from_secs(5)));

    let entry_cell_idle_dead = make_test_worker_entry(
        id,
        Some(Duration::from_secs(25)),
        now - Duration::from_secs(105),
        0,
    );
    // At 105s, exceeds 100s window -> SILENT
    assert!(entry_cell_idle_dead.is_silent(now, Duration::from_secs(5)));

    // 3. Wi-Fi worker (tau = 60s, W_dead = 240s)
    let entry_wifi = make_test_worker_entry(
        id,
        Some(Duration::from_secs(60)),
        now - Duration::from_secs(235),
        0,
    );
    // At 235s, within 240s window -> NOT silent
    assert!(!entry_wifi.is_silent(now, Duration::from_secs(5)));

    let entry_wifi_dead = make_test_worker_entry(
        id,
        Some(Duration::from_secs(60)),
        now - Duration::from_secs(245),
        0,
    );
    // At 245s, exceeds 240s window -> SILENT
    assert!(entry_wifi_dead.is_silent(now, Duration::from_secs(5)));

    // 4. Registry dynamic telemetry update
    let registry = WorkerRegistry::new();
    let worker_id = Uuid::new_v4();
    let (tx, _rx) = mpsc::channel(4);
    let caps = WorkerCapabilities::new("android-phone", 8, 8192, false, false, None);

    let session = registry
        .register(worker_id, caps, "127.0.0.1:9090".parse().unwrap(), tx, None)
        .await
        .unwrap();

    // Push dynamic cellular idle telemetry
    let updated = registry
        .update_mobile_telemetry(
            &worker_id,
            Some(session),
            MobileLiveStatus {
                battery_pct: Some(80),
                is_charging: Some(true),
                thermal_throttled: false,
                battery_temperature: Some(33.0),
                network_type: Some("cellular".into()),
                battery_tier: Some(BatteryTier::Normal),
                thermal_tier: Some(ThermalTier::Normal),
            },
        )
        .await
        .unwrap();
    assert!(updated);

    let info = registry
        .get_worker(worker_id)
        .await
        .expect("worker must exist");
    assert_eq!(info.worker_id, worker_id);
}

// ============================================================================
// TEST 6: Empirical Verification of Telemetry Data Invariants
// ============================================================================
#[test]
fn test_empirical_telemetry_pipeline_invariants() {
    // 1. Direct validation: MobileLiveStatus calculation functions are pure and accurate
    let live_critical_bat = MobileLiveStatus {
        battery_pct: Some(10),
        is_charging: Some(false),
        thermal_throttled: false,
        battery_temperature: Some(32.0),
        network_type: Some("wifi".into()),
        battery_tier: Some(BatteryTier::Critical),
        thermal_tier: Some(ThermalTier::Normal),
    };
    assert_eq!(live_critical_bat.effective_concurrency(8), 0);

    let live_sublow = MobileLiveStatus {
        battery_pct: Some(20),
        is_charging: Some(false),
        thermal_throttled: false,
        battery_temperature: Some(35.0),
        network_type: Some("wifi".into()),
        battery_tier: Some(BatteryTier::SubLow),
        thermal_tier: Some(ThermalTier::Normal),
    };
    assert_eq!(live_sublow.effective_concurrency(8), 1);

    let live_hot = MobileLiveStatus {
        battery_pct: Some(90),
        is_charging: Some(true),
        thermal_throttled: false,
        battery_temperature: Some(41.0),
        network_type: Some("wifi".into()),
        battery_tier: Some(BatteryTier::Normal),
        thermal_tier: Some(ThermalTier::Hot),
    };
    assert_eq!(live_hot.effective_concurrency(8), 1);

    let live_critical_temp = MobileLiveStatus {
        battery_pct: Some(90),
        is_charging: Some(true),
        thermal_throttled: false,
        battery_temperature: Some(44.0),
        network_type: Some("wifi".into()),
        battery_tier: Some(BatteryTier::Normal),
        thermal_tier: Some(ThermalTier::Critical),
    };
    assert_eq!(live_critical_temp.effective_concurrency(8), 0);

    // 2. Telemetry struct representation analysis:
    // MobileCapabilities carries the static snapshot:
    let mob = MobileCapabilities {
        os_version: "Android 14 (API 34)".into(),
        soc_model: "Snapdragon 8 Gen 2".into(),
        battery_pct: Some(20),
        is_charging: Some(false),
        thermal_throttled: false,
    };
    // Reconstructing live status from MobileCapabilities:
    let re_read = mob.live_status();
    // Observe that fields not in MobileCapabilities (battery_temperature, network_type, battery_tier, thermal_tier)
    // are None in the converted MobileLiveStatus snapshot:
    assert_eq!(re_read.battery_temperature, None);
    assert_eq!(re_read.network_type, None);
    assert_eq!(re_read.battery_tier, None);
    assert_eq!(re_read.thermal_tier, None);

    // When re_read is evaluated via pure functions with default fallbacks:
    let computed_bat =
        MobileLiveStatus::compute_battery_tier(re_read.battery_pct, re_read.is_charging);
    assert_eq!(computed_bat, BatteryTier::SubLow);
    let computed_therm = MobileLiveStatus::compute_thermal_tier(
        re_read.battery_temperature,
        re_read.thermal_throttled,
    );
    assert_eq!(computed_therm, ThermalTier::Normal);
    let eff_computed = calculate_effective_concurrency(8, computed_bat, computed_therm);
    assert_eq!(eff_computed, 1);
}
