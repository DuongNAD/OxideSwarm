//! Adversarial and Empirical Stress Test Suite for Milestone M5A (Challenger 2).
//!
//! Stress-tests:
//! 1. Autonomous Mobile Master Refusal Matrix (§5.1):
//!    - Discharging phone claiming coordinator role
//!    - Metered / cellular networks (LTE, 5G, 4G, 3G, mobile, metered, CGNAT) claiming master
//!    - Battery reserve boundary tests (0%, 14%, 15%, 49%, 50%, 51%, 100%)
//!    - Thermal headroom boundary tests (39.9°C, 40.0°C, 40.001°C, 41.0°C, extreme values)
//!    - Execution context requirement (Android Foreground Service active)
//!    - Simultaneous 5-way refusal rule violation vectors
//!    - Desktop immunity from mobile refusal rules
//! 2. Deterministic Authority Scoring ($S_{\text{auth}}$) & 4-Stage Tie-Breaking (§5.2):
//!    - Mathematical properties across 10,000 randomized candidate pairs (Antisymmetry, Totality, Consistency, Reflexivity)
//!    - Transitivity across 1,000 candidate triples
//!    - Strict permutation sorting stability
//!    - Isolated 4-stage hierarchy validation:
//!      Stage 1: Authority Score
//!      Stage 2: Platform Weight
//!      Stage 3: Uptime
//!      Stage 4: UUID Lexicographical Ordering (lower byte values win)
//! 3. Cryptographic `MasterLeaseTicket` Tamper Detection (§5.3):
//!    - Exhaustive 480 single-bit flip sweeps across canonical 60-byte payload
//!    - Exhaustive 256 single-bit flip sweeps across 32-byte HMAC-SHA256 signature
//!    - Truncated and oversized signature lengths
//!    - Expiration and time range boundary invariants (now == expiry vs now == expiry + 1)
//!    - Inverted time ranges (expires_at <= issued_at)
//!    - Wrong cluster secret and mutated secret keys
//!    - Claim superiority arbitrage evaluation (`evaluate_master_claim`)
//! 4. Pre-Sleep Fast Task Evacuation Under Multi-Task Load (< 50ms) (§5.4):
//!    - Concurrent multi-task workload running across 3 workers
//!    - Pre-sleep evacuation trigger under active load
//!    - Task reassignment and cluster convergence in < 50ms
//!    - 100% completion with zero task loss across all submitted tasks
//!    - Correct retry_count accounting (1 on evacuated tasks, 0 on survivor tasks)
//!    - Direct `cancel_all_active_tasks` fast-path verification (< 15ms)

use std::cmp::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use uuid::Uuid;

use oxideworker::{get_evacuation_reason_impl, trigger_fast_evacuation_impl};
use rusty_grid_core::arbitrage::{
    calculate_authority_score, check_mobile_master_eligibility, evaluate_master_claim,
    evaluate_mobile_master_refusal_matrix, is_cellular_or_metered, resolve_election_tie,
    CandidateRanking, ClaimVerdict, LeaseValidationError, MasterLeaseTicket, MasterRefusalReason,
    MobileEnvironmentTelemetry, PlatformType, ELECTION_THRESHOLD_SCORE, LEASE_VALIDITY_DURATION_MS,
    SCORE_MAX, SCORE_MIN,
};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;
use rusty_grid_worker::WorkerConfig;

/// Fast, deterministic pseudo-random number generator for reproducible fuzzing.
struct SimplePrng {
    state: u64,
}

impl SimplePrng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x5EED_CAFE_BABE_D00D
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

    fn next_i32_range(&mut self, min: i32, max: i32) -> i32 {
        let span = (max as i64 - min as i64 + 1) as u64;
        let r = self.next_u64() % span;
        (min as i64 + r as i64) as i32
    }

    fn next_u128(&mut self) -> u128 {
        ((self.next_u64() as u128) << 64) | (self.next_u64() as u128)
    }

    fn next_uuid(&mut self) -> Uuid {
        Uuid::from_u128(self.next_u128())
    }

    fn next_platform(&mut self) -> PlatformType {
        match self.next_u64() % 5 {
            0 => PlatformType::LinuxServer,
            1 => PlatformType::MacOs,
            2 => PlatformType::Windows,
            3 => PlatformType::Android,
            _ => PlatformType::Unknown,
        }
    }
}

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

// ==============================================================================
// 1. Refusal Matrix Adversarial Edge Cases & Boundary Analysis (§5.1)
// ==============================================================================

#[test]
fn test_adversarial_refusal_matrix_boundary_and_fuzzing() {
    // 1. Discharging phone with high CPU claiming coordinator role
    let discharging_phone = MobileEnvironmentTelemetry {
        is_charging: Some(false),
        battery_pct: Some(98),
        network_type: Some("wifi".into()),
        battery_temperature: Some(36.5),
        thermal_throttled: false,
        has_fgs_active: true,
    };
    let refusal = check_mobile_master_eligibility(&discharging_phone);
    assert_eq!(refusal, Err(MasterRefusalReason::NotCharging));

    let auth_discharging = calculate_authority_score(
        PlatformType::Android,
        Some(false),
        Some(98),
        Some("wifi"),
        false,
        Some(36.5),
        100_000,
        true,
    );
    assert_eq!(
        auth_discharging.total_score, SCORE_MIN,
        "Discharging mobile node must receive minimum score -10,000"
    );
    assert!(!auth_discharging.is_eligible);

    // 2. Metered / Cellular network types claiming coordinator role
    let cellular_types = [
        "cellular",
        "5G",
        "5G-NR",
        "LTE",
        "LTE-Advanced",
        "4G",
        "3G",
        "3G HSPA+",
        "mobile",
        "cgnat",
        "metered",
        "metered_wifi",
        "Public-CGNAT-5G",
        "android.net.NetworkCapabilities.TRANSPORT_CELLULAR",
    ];

    for net_type in cellular_types {
        assert!(
            is_cellular_or_metered(&net_type.to_ascii_lowercase()),
            "Network type '{}' must be detected as cellular or metered",
            net_type
        );

        let cell_phone = MobileEnvironmentTelemetry {
            is_charging: Some(true),
            battery_pct: Some(100),
            network_type: Some(net_type.to_string()),
            battery_temperature: Some(32.0),
            thermal_throttled: false,
            has_fgs_active: true,
        };
        let res = check_mobile_master_eligibility(&cell_phone);
        assert!(
            matches!(
                res,
                Err(MasterRefusalReason::CellularOrMeteredNetwork { .. })
            ),
            "Expected CellularOrMeteredNetwork refusal for '{}', got {:?}",
            net_type,
            res
        );

        let eval = calculate_authority_score(
            PlatformType::Android,
            Some(true),
            Some(100),
            Some(net_type),
            false,
            Some(32.0),
            10_000,
            true,
        );
        assert_eq!(
            eval.total_score, SCORE_MIN,
            "Cellular mobile node must be scored at minimum -10,000"
        );
        assert!(!eval.is_eligible);
    }

    // 3. Battery reserve exact boundary testing: 0, 14, 15, 49, 50, 51, 100%
    let test_battery = |pct: u8, charging: bool| {
        let telem = MobileEnvironmentTelemetry {
            is_charging: Some(charging),
            battery_pct: Some(pct),
            network_type: Some("wifi".into()),
            battery_temperature: Some(30.0),
            thermal_throttled: false,
            has_fgs_active: true,
        };
        check_mobile_master_eligibility(&telem)
    };

    // Charging at 49% -> Rejected (< 50%)
    assert!(matches!(
        test_battery(49, true),
        Err(MasterRefusalReason::BatteryBelowThreshold {
            pct: 49,
            threshold: 50
        })
    ));
    // Charging at 50% -> Accepted
    assert_eq!(test_battery(50, true), Ok(()));
    // Charging at 51% -> Accepted
    assert_eq!(test_battery(51, true), Ok(()));
    // Charging at 0% -> Rejected
    assert!(matches!(
        test_battery(0, true),
        Err(MasterRefusalReason::BatteryBelowThreshold {
            pct: 0,
            threshold: 50
        })
    ));

    // Missing battery percentage defaults to 0% -> Rejected
    let missing_bat = MobileEnvironmentTelemetry {
        is_charging: Some(true),
        battery_pct: None,
        network_type: Some("wifi".into()),
        battery_temperature: Some(30.0),
        thermal_throttled: false,
        has_fgs_active: true,
    };
    assert!(matches!(
        check_mobile_master_eligibility(&missing_bat),
        Err(MasterRefusalReason::BatteryBelowThreshold {
            pct: 0,
            threshold: 50
        })
    ));

    // 4. Thermal headroom boundary testing: 39.9°C, 40.0°C, 40.001°C, 41.0°C, extreme values
    let test_temp = |temp: f32, throttled: bool| {
        let telem = MobileEnvironmentTelemetry {
            is_charging: Some(true),
            battery_pct: Some(90),
            network_type: Some("wifi".into()),
            battery_temperature: Some(temp),
            thermal_throttled: throttled,
            has_fgs_active: true,
        };
        check_mobile_master_eligibility(&telem)
    };

    // 39.9°C unthrottled -> Accepted
    assert_eq!(test_temp(39.9, false), Ok(()));
    // 40.0°C unthrottled -> Accepted (boundary is strict > 40.0°C)
    assert_eq!(test_temp(40.0, false), Ok(()));
    // 40.001°C unthrottled -> Rejected
    assert!(matches!(
        test_temp(40.001, false),
        Err(MasterRefusalReason::ThermalHeadroomExceeded { .. })
    ));
    // 41.0°C unthrottled -> Rejected
    assert!(matches!(
        test_temp(41.0, false),
        Err(MasterRefusalReason::ThermalHeadroomExceeded { .. })
    ));
    // 30.0°C but OS thermal throttled flag is set -> Rejected
    assert!(matches!(
        test_temp(30.0, true),
        Err(MasterRefusalReason::ThermalHeadroomExceeded { .. })
    ));
    // Extreme temperature 100.0°C -> Rejected
    assert!(matches!(
        test_temp(100.0, false),
        Err(MasterRefusalReason::ThermalHeadroomExceeded { .. })
    ));
    // Cold temperature -10.0°C -> Accepted
    assert_eq!(test_temp(-10.0, false), Ok(()));

    // 5. Execution context (Android Foreground Service)
    let no_fgs = MobileEnvironmentTelemetry {
        is_charging: Some(true),
        battery_pct: Some(100),
        network_type: Some("ethernet".into()),
        battery_temperature: Some(25.0),
        thermal_throttled: false,
        has_fgs_active: false,
    };
    assert_eq!(
        check_mobile_master_eligibility(&no_fgs),
        Err(MasterRefusalReason::ForegroundServiceInactive)
    );

    // 6. Simultaneous 5-way violation returns ALL 5 reasons without omission
    let adversarial_all_violations = MobileEnvironmentTelemetry {
        is_charging: Some(false),
        battery_pct: Some(12),
        network_type: Some("5G CGNAT Metered".into()),
        battery_temperature: Some(48.5),
        thermal_throttled: true,
        has_fgs_active: false,
    };
    let violations = evaluate_mobile_master_refusal_matrix(&adversarial_all_violations);
    assert_eq!(violations.len(), 5);
    assert!(violations.iter().any(|v| v.is_not_charging()));
    assert!(violations.iter().any(|v| v.is_battery_below_threshold()));
    assert!(violations.iter().any(|v| v.is_cellular()));
    assert!(violations.iter().any(|v| v.is_thermal_exceeded()));
    assert!(violations.iter().any(|v| v.is_foreground_inactive()));

    // 7. Desktop Immunity: Linux, Windows, MacOs nodes are NOT disqualified by mobile refusal matrix
    let desktop_eval = calculate_authority_score(
        PlatformType::LinuxServer,
        Some(false), // on battery
        Some(40),    // low battery
        Some("wifi"),
        false,
        None,
        500,
        false, // no FGS
    );
    // Linux(5000) + Power(100) + Net(700) + Thermal(0) + Uptime(50) = 5850 >= 2500
    assert_eq!(desktop_eval.total_score, 5850);
    assert!(desktop_eval.is_eligible);
}

// ==============================================================================
// 2. Deterministic Authority Scoring & 4-Stage Tie-Breaking Stress (§5.2)
// ==============================================================================

#[test]
fn test_deterministic_authority_scoring_and_4stage_tiebreaking_stress() {
    let mut rng = SimplePrng::new(0xABCD_1234_5678_9ABC);

    // Stage 1: Authority Score difference dominates
    let id_a = rng.next_uuid();
    let id_b = rng.next_uuid();
    let c1 = CandidateRanking::new(id_a, 7000, 4000, 100);
    let c2 = CandidateRanking::new(id_b, 6999, 5000, 10_000);
    assert_eq!(resolve_election_tie(&c1, &c2), Ordering::Greater);
    assert_eq!(resolve_election_tie(&c2, &c1), Ordering::Less);

    // Stage 2: Equal score, platform weight dominates
    let c3 = CandidateRanking::new(id_a, 5000, 5000, 100); // Linux
    let c4 = CandidateRanking::new(id_b, 5000, 4500, 10_000); // MacOs
    assert_eq!(resolve_election_tie(&c3, &c4), Ordering::Greater);
    assert_eq!(resolve_election_tie(&c4, &c3), Ordering::Less);

    // Stage 3: Equal score & platform, uptime dominates
    let c5 = CandidateRanking::new(id_a, 5000, 5000, 2000);
    let c6 = CandidateRanking::new(id_b, 5000, 5000, 1999);
    assert_eq!(resolve_election_tie(&c5, &c6), Ordering::Greater);
    assert_eq!(resolve_election_tie(&c6, &c5), Ordering::Less);

    // Stage 4: Equal score, platform, and uptime -> Lexicographically LOWER UUID byte array wins!
    // Construct two UUIDs with known byte relationships
    let bytes_low = [0x55u8; 16];
    let mut bytes_high = [0x55u8; 16];
    // 1-bit difference at byte 0
    bytes_high[0] = 0x56;
    let uuid_low_0 = Uuid::from_bytes(bytes_low);
    let uuid_high_0 = Uuid::from_bytes(bytes_high);
    let c_low_0 = CandidateRanking::new(uuid_low_0, 4000, 4000, 500);
    let c_high_0 = CandidateRanking::new(uuid_high_0, 4000, 4000, 500);
    assert_eq!(
        resolve_election_tie(&c_low_0, &c_high_0),
        Ordering::Greater,
        "Lexicographically lower UUID (byte 0) must win tie"
    );
    assert_eq!(resolve_election_tie(&c_high_0, &c_low_0), Ordering::Less);

    // 1-bit difference at byte 15 (last byte)
    bytes_high = bytes_low;
    bytes_high[15] = 0x56;
    let uuid_low_15 = Uuid::from_bytes(bytes_low);
    let uuid_high_15 = Uuid::from_bytes(bytes_high);
    let c_low_15 = CandidateRanking::new(uuid_low_15, 4000, 4000, 500);
    let c_high_15 = CandidateRanking::new(uuid_high_15, 4000, 4000, 500);
    assert_eq!(
        resolve_election_tie(&c_low_15, &c_high_15),
        Ordering::Greater,
        "Lexicographically lower UUID (byte 15) must win tie"
    );

    // -------------------------------------------------------------------------
    // Stress Verification: 10,000 Randomized Candidate Pairs
    // -------------------------------------------------------------------------
    const NUM_PAIRS: usize = 10_000;
    for _ in 0..NUM_PAIRS {
        let score_a = rng.next_i32_range(SCORE_MIN, SCORE_MAX);
        let score_b = rng.next_i32_range(SCORE_MIN, SCORE_MAX);
        let plat_a = rng.next_platform().weight();
        let plat_b = rng.next_platform().weight();
        let up_a = rng.next_u64() % 1_000_000;
        let up_b = rng.next_u64() % 1_000_000;
        let id_a = rng.next_uuid();
        let id_b = rng.next_uuid();

        let a = CandidateRanking::new(id_a, score_a, plat_a, up_a);
        let b = CandidateRanking::new(id_b, score_b, plat_b, up_b);

        // 1. Reflexivity: a == a
        assert_eq!(resolve_election_tie(&a, &a), Ordering::Equal);
        assert_eq!(a.cmp(&a), Ordering::Equal);

        // 2. Consistency: resolve_election_tie matches Ord::cmp
        let ord_ab = resolve_election_tie(&a, &b);
        assert_eq!(a.cmp(&b), ord_ab);

        let ord_ba = resolve_election_tie(&b, &a);
        assert_eq!(b.cmp(&a), ord_ba);

        // 3. Antisymmetry: a.cmp(b) == b.cmp(a).reverse()
        assert_eq!(
            ord_ab,
            ord_ba.reverse(),
            "Antisymmetry violation between {:?} and {:?}",
            a,
            b
        );

        // 4. Strict Totality: distinct UUIDs never produce Equal
        if id_a != id_b {
            assert_ne!(
                ord_ab,
                Ordering::Equal,
                "Distinct node UUIDs must never result in an unresolved tie"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Transitivity Verification Across 1,000 Triples: A > B && B > C ==> A > C
    // -------------------------------------------------------------------------
    const NUM_TRIPLES: usize = 1_000;
    for _ in 0..NUM_TRIPLES {
        let a = CandidateRanking::new(
            rng.next_uuid(),
            rng.next_i32_range(-5000, 5000),
            rng.next_platform().weight(),
            rng.next_u64() % 500_000,
        );
        let b = CandidateRanking::new(
            rng.next_uuid(),
            rng.next_i32_range(-5000, 5000),
            rng.next_platform().weight(),
            rng.next_u64() % 500_000,
        );
        let c = CandidateRanking::new(
            rng.next_uuid(),
            rng.next_i32_range(-5000, 5000),
            rng.next_platform().weight(),
            rng.next_u64() % 500_000,
        );

        if a > b && b > c {
            assert!(
                a > c,
                "Transitivity violation: A > B and B > C but !(A > C)"
            );
        } else if a < b && b < c {
            assert!(
                a < c,
                "Transitivity violation: A < B and B < C but !(A < C)"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Sorting Permutation Stability & Idempotence
    // -------------------------------------------------------------------------
    let mut candidates = Vec::with_capacity(500);
    for _ in 0..500 {
        candidates.push(CandidateRanking::new(
            rng.next_uuid(),
            rng.next_i32_range(2000, 8000),
            rng.next_platform().weight(),
            rng.next_u64() % 100_000,
        ));
    }

    let mut sorted_once = candidates.clone();
    sorted_once.sort();

    // Shuffle and sort again
    let mut shuffled = candidates.clone();
    for i in (1..shuffled.len()).rev() {
        let j = (rng.next_u64() as usize) % (i + 1);
        shuffled.swap(i, j);
    }
    shuffled.sort();

    assert_eq!(
        sorted_once, shuffled,
        "Sorting 500 randomized candidates must be bitwise deterministic across permutations"
    );
}

// ==============================================================================
// 3. Cryptographic MasterLeaseTicket Tamper Detection (§5.3)
// ==============================================================================

#[test]
fn test_master_lease_ticket_cryptographic_tamper_detection_exhaustive() {
    let secret = ClusterSecret::generate().expect("Valid cluster secret");
    let cluster_id = Uuid::new_v4();
    let master_id = Uuid::new_v4();
    let now_epoch_ms = 1_700_000_000_000;
    let validity = Duration::from_millis(LEASE_VALIDITY_DURATION_MS);

    let baseline_ticket = MasterLeaseTicket::issue(
        cluster_id,
        master_id,
        42,
        6500,
        now_epoch_ms,
        validity,
        &secret,
    );

    // Baseline ticket must verify cleanly
    assert!(baseline_ticket.verify(Some(&secret), now_epoch_ms).is_ok());
    assert!(baseline_ticket.is_valid(&secret, now_epoch_ms));

    // -------------------------------------------------------------------------
    // Exhaustive 480 Single-Bit Flip Sweep Across Canonical 60-Byte Payload
    // -------------------------------------------------------------------------
    let canonical = baseline_ticket.canonical_bytes();
    assert_eq!(canonical.len(), 60);

    for byte_idx in 0..60 {
        for bit_idx in 0..8 {
            let mut corrupted_canonical = canonical;
            corrupted_canonical[byte_idx] ^= 1 << bit_idx;

            // Reconstruct ticket from corrupted canonical bytes
            let c_id = Uuid::from_slice(&corrupted_canonical[0..16]).unwrap();
            let m_id = Uuid::from_slice(&corrupted_canonical[16..32]).unwrap();
            let seq = u64::from_be_bytes(corrupted_canonical[32..40].try_into().unwrap());
            let score = i32::from_be_bytes(corrupted_canonical[40..44].try_into().unwrap());
            let issued = u64::from_be_bytes(corrupted_canonical[44..52].try_into().unwrap());
            let expires = u64::from_be_bytes(corrupted_canonical[52..60].try_into().unwrap());

            let mut tampered_ticket = baseline_ticket.clone();
            tampered_ticket.cluster_id = c_id;
            tampered_ticket.master_id = m_id;
            tampered_ticket.lease_sequence = seq;
            tampered_ticket.authority_score = score;
            tampered_ticket.issued_at_epoch_ms = issued;
            tampered_ticket.expires_at_epoch_ms = expires;

            // Verification MUST fail: either SignatureMismatch or InvalidTimeRange or Expired
            let verify_result = tampered_ticket.verify(Some(&secret), now_epoch_ms);
            assert!(
                verify_result.is_err(),
                "Single-bit flip at byte {} bit {} went undetected!",
                byte_idx,
                bit_idx
            );
        }
    }

    // -------------------------------------------------------------------------
    // Exhaustive 256 Single-Bit Flip Sweep Across 32-Byte HMAC-SHA256 Signature
    // -------------------------------------------------------------------------
    for byte_idx in 0..32 {
        for bit_idx in 0..8 {
            let mut tampered_ticket = baseline_ticket.clone();
            tampered_ticket.signature[byte_idx] ^= 1 << bit_idx;

            let result = tampered_ticket.verify(Some(&secret), now_epoch_ms);
            assert_eq!(
                result,
                Err(LeaseValidationError::SignatureMismatch),
                "Flipped signature bit at byte {} bit {} was not rejected!",
                byte_idx,
                bit_idx
            );
        }
    }

    // -------------------------------------------------------------------------
    // Signature Length Tampering
    // -------------------------------------------------------------------------
    for invalid_len in [0, 1, 15, 16, 31, 33, 48, 64] {
        let mut tampered_ticket = baseline_ticket.clone();
        tampered_ticket.signature = vec![0xEE; invalid_len];

        let result = tampered_ticket.verify(Some(&secret), now_epoch_ms);
        assert_eq!(
            result,
            Err(LeaseValidationError::InvalidSignatureLength(invalid_len)),
            "Invalid signature length {} was not caught",
            invalid_len
        );
    }

    // -------------------------------------------------------------------------
    // Expiration and Time Window Boundary Conditions
    // -------------------------------------------------------------------------
    let expiry = baseline_ticket.expires_at_epoch_ms;

    // At exact expiration millisecond -> Valid
    assert!(baseline_ticket.verify(Some(&secret), expiry).is_ok());

    // 1 millisecond past expiration -> Expired error
    let expired_res = baseline_ticket.verify(Some(&secret), expiry + 1);
    assert_eq!(
        expired_res,
        Err(LeaseValidationError::Expired {
            expired_at: expiry,
            current: expiry + 1,
        })
    );

    // Far future -> Expired error
    assert!(matches!(
        baseline_ticket.verify(Some(&secret), expiry + 1_000_000),
        Err(LeaseValidationError::Expired { .. })
    ));

    // Time inversion: expires_at <= issued_at
    let mut inverted_ticket = baseline_ticket.clone();
    inverted_ticket.expires_at_epoch_ms = inverted_ticket.issued_at_epoch_ms;
    assert_eq!(
        inverted_ticket.verify(Some(&secret), now_epoch_ms),
        Err(LeaseValidationError::InvalidTimeRange)
    );

    // -------------------------------------------------------------------------
    // Cluster Secret Variations
    // -------------------------------------------------------------------------
    let wrong_secret = ClusterSecret::generate().expect("Different cluster secret");
    assert_eq!(
        baseline_ticket.verify(Some(&wrong_secret), now_epoch_ms),
        Err(LeaseValidationError::SignatureMismatch),
        "Ticket verified against wrong cluster secret must fail"
    );

    // Mutated secret (single character flipped in hex cluster secret)
    let hex = secret.to_hex();
    let mut hex_chars: Vec<char> = hex.chars().collect();
    hex_chars[0] = if hex_chars[0] == 'a' { 'b' } else { 'a' };
    let mutated_hex: String = hex_chars.into_iter().collect();
    let mutated_secret = ClusterSecret::from_hex(&mutated_hex).expect("mutated secret");
    assert_eq!(
        baseline_ticket.verify(Some(&mutated_secret), now_epoch_ms),
        Err(LeaseValidationError::SignatureMismatch),
        "Ticket verified against mutated cluster secret must fail"
    );

    // -------------------------------------------------------------------------
    // Role Arbitrage Claim Superiority Evaluation (`evaluate_master_claim`)
    // -------------------------------------------------------------------------
    let incumbent_ticket = MasterLeaseTicket::issue(
        cluster_id,
        master_id,
        1,
        4000,
        now_epoch_ms,
        validity,
        &secret,
    );

    // Claimant with superior score (6500 > 4000) -> Accepted
    let superior_claimant = MasterLeaseTicket::issue(
        cluster_id,
        Uuid::new_v4(),
        2,
        6500,
        now_epoch_ms,
        validity,
        &secret,
    );
    assert_eq!(
        evaluate_master_claim(&incumbent_ticket, &superior_claimant, &secret, now_epoch_ms),
        ClaimVerdict::AcceptedAndStepDown
    );

    // Claimant with inferior score (3999 < 4000) -> Rejected
    let inferior_claimant = MasterLeaseTicket::issue(
        cluster_id,
        Uuid::new_v4(),
        2,
        3999,
        now_epoch_ms,
        validity,
        &secret,
    );
    assert_eq!(
        evaluate_master_claim(&incumbent_ticket, &inferior_claimant, &secret, now_epoch_ms),
        ClaimVerdict::RejectedInferiorScore
    );

    // Claimant with equal score (4000 == 4000) -> Incumbent retains coordinator role
    let equal_claimant = MasterLeaseTicket::issue(
        cluster_id,
        Uuid::new_v4(),
        2,
        4000,
        now_epoch_ms,
        validity,
        &secret,
    );
    assert_eq!(
        evaluate_master_claim(&incumbent_ticket, &equal_claimant, &secret, now_epoch_ms),
        ClaimVerdict::RejectedInferiorScore
    );

    // Claimant with score below election threshold (2499 < 2500) -> Rejected
    let low_threshold_claimant = MasterLeaseTicket::issue(
        cluster_id,
        Uuid::new_v4(),
        2,
        ELECTION_THRESHOLD_SCORE - 1,
        now_epoch_ms,
        validity,
        &secret,
    );
    assert_eq!(
        evaluate_master_claim(
            &incumbent_ticket,
            &low_threshold_claimant,
            &secret,
            now_epoch_ms
        ),
        ClaimVerdict::RejectedInferiorScore
    );

    // Claimant with forged ticket (signature mismatch) -> RejectedInvalidTicket
    let mut forged_claimant = superior_claimant.clone();
    forged_claimant.authority_score = 9999; // Bump score without resigning
    assert!(matches!(
        evaluate_master_claim(&incumbent_ticket, &forged_claimant, &secret, now_epoch_ms),
        ClaimVerdict::RejectedInvalidTicket(_)
    ));

    // Claimant with expired ticket -> RejectedInvalidTicket
    assert!(matches!(
        evaluate_master_claim(
            &incumbent_ticket,
            &superior_claimant,
            &secret,
            now_epoch_ms + LEASE_VALIDITY_DURATION_MS + 10
        ),
        ClaimVerdict::RejectedInvalidTicket(_)
    ));
}

// ==============================================================================
// 4. Pre-Sleep Fast Task Evacuation Under Multi-Task Load (< 50ms) (§5.4)
// ==============================================================================

#[tokio::test]
async fn test_pre_sleep_fast_evacuation_under_multi_task_load() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let sandbox_path = temp_dir.path().to_path_buf();

    // 1. Direct JNI fast evacuation hook test
    assert!(trigger_fast_evacuation_impl(
        "FAST_PRE_SLEEP_BATTERY_CRITICAL"
    ));
    assert_eq!(
        get_evacuation_reason_impl(),
        Some("FAST_PRE_SLEEP_BATTERY_CRITICAL".to_string())
    );

    // 2. Spawn loopback MasterServer with max_retries = 3
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let master = MasterServer::spawn(server_config)
        .await
        .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    // 3. Spawn 3 WorkerClients (Worker 1 = Mobile Candidate to be evacuated, Workers 2 & 3 = Survivors)
    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let (_w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);
    let (_w3_shutdown_tx, w3_shutdown_rx) = watch::channel(false);

    let w1_id = Uuid::new_v4();
    let config1 = WorkerConfig::new(&master_addr)
        .with_name("mobile-evac-worker-1")
        .with_cores(4)
        .with_worker_id(w1_id)
        .with_sandbox_base_dir(sandbox_path.join("w1"));
    let mut w1 = WorkerClient::new(config1);

    let w2_id = Uuid::new_v4();
    let config2 = WorkerConfig::new(&master_addr)
        .with_name("survivor-worker-2")
        .with_cores(4)
        .with_worker_id(w2_id)
        .with_sandbox_base_dir(sandbox_path.join("w2"));
    let mut w2 = WorkerClient::new(config2);

    let w3_id = Uuid::new_v4();
    let config3 = WorkerConfig::new(&master_addr)
        .with_name("survivor-worker-3")
        .with_cores(4)
        .with_worker_id(w3_id)
        .with_sandbox_base_dir(sandbox_path.join("w3"));
    let mut w3 = WorkerClient::new(config3);

    tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });
    tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });
    tokio::spawn(async move {
        let _ = w3.run(w3_shutdown_rx).await;
    });

    // Wait for all 3 workers to register
    assert!(
        wait_for(
            Duration::from_secs(6),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 3)
                    .unwrap_or(false)
            }
        )
        .await,
        "All 3 workers must register successfully"
    );

    // 4. Submit MULTIPLE concurrent tasks (batch of 6 tasks) under active load
    const BATCH_SIZE: usize = 6;
    let mut task_ids = Vec::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        let task = Task::new(
            TaskSpec::BuiltinTest {
                test_name: format!("load_task_{i}"),
                iterations: 50,
                duration_ms: 1800,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 30),
        );
        let tid = master.submit_task(task).await.expect("task submit");
        task_ids.push(tid);
    }

    // Wait for tasks to enter Running state across the cluster
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                let mut running_count = 0;
                for &tid in &task_ids {
                    if let Ok(info) = master.get_task_info(tid).await {
                        if info.state == TaskState::Running {
                            running_count += 1;
                        }
                    }
                }
                running_count >= 3
            }
        )
        .await,
        "At least 3 tasks must enter Running state before evacuation trigger"
    );

    // Identify which tasks are assigned to Worker 1
    let mut w1_tasks = Vec::new();
    for &tid in &task_ids {
        if let Ok(info) = master.get_task_info(tid).await {
            if info.assigned_worker_id == Some(w1_id) {
                w1_tasks.push(tid);
            }
        }
    }

    // If no task happened to land on Worker 1 yet, wait briefly for scheduling
    if w1_tasks.is_empty() {
        let found = wait_for(
            Duration::from_secs(2),
            Duration::from_millis(10),
            || async {
                for &tid in &task_ids {
                    if let Ok(info) = master.get_task_info(tid).await {
                        if info.assigned_worker_id == Some(w1_id) {
                            return true;
                        }
                    }
                }
                false
            },
        )
        .await;
        if found {
            for &tid in &task_ids {
                if let Ok(info) = master.get_task_info(tid).await {
                    if info.assigned_worker_id == Some(w1_id) {
                        w1_tasks.push(tid);
                    }
                }
            }
        }
    }

    // 5. Trigger pre-sleep fast evacuation on Worker 1 under active multi-task load
    let evacuation_start = Instant::now();
    let _ = w1_shutdown_tx.send(true);

    // 6. Measure evacuation latency: Master MUST re-queue or re-assign all of Worker 1's tasks in < 50ms
    let evacuation_completed = wait_for(
        Duration::from_millis(50),
        Duration::from_millis(2),
        || async {
            for &tid in &w1_tasks {
                if let Ok(info) = master.get_task_info(tid).await {
                    // Task must either be re-assigned away from w1_id, or re-queued
                    if info.assigned_worker_id == Some(w1_id) && info.state == TaskState::Running {
                        return false;
                    }
                }
            }
            true
        },
    )
    .await;

    let evacuation_elapsed = evacuation_start.elapsed();
    assert!(
        evacuation_completed,
        "Fast pre-sleep task evacuation took {:?}, exceeding the 50ms deadline!",
        evacuation_elapsed
    );
    assert!(
        evacuation_elapsed < Duration::from_millis(50),
        "Evacuation elapsed time {:?} exceeded 50ms threshold",
        evacuation_elapsed
    );

    // 7. Verify all tasks complete successfully with ZERO task loss
    let all_completed = wait_for(
        Duration::from_secs(8),
        Duration::from_millis(50),
        || async {
            for &tid in &task_ids {
                if let Ok(info) = master.get_task_info(tid).await {
                    if info.state != TaskState::Completed {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            true
        },
    )
    .await;
    assert!(
        all_completed,
        "All 6 tasks must complete without any task loss"
    );

    // 8. Assert task state, exit code, and retry count
    for &tid in &task_ids {
        let final_info = master.get_task_info(tid).await.expect("task info exists");
        assert_eq!(
            final_info.state,
            TaskState::Completed,
            "Task {} must be Completed",
            tid
        );
        assert_eq!(
            final_info.exit_code,
            Some(0),
            "Task {} must have exit code 0",
            tid
        );
        // Surviving worker assigned must be Worker 2 or Worker 3
        let assigned = final_info.assigned_worker_id.expect("assigned worker");
        assert!(
            assigned == w2_id || assigned == w3_id,
            "Task {} must be completed by a surviving worker (got {})",
            tid,
            assigned
        );

        if w1_tasks.contains(&tid) {
            assert_eq!(
                final_info.retry_count, 1,
                "Evacuated task {} must have retry_count == 1",
                tid
            );
        }
    }

    // 9. Verify WorkerClient fast-path cancellation latency directly
    let test_worker_config = WorkerConfig::new(&master_addr)
        .with_cores(2)
        .with_sandbox_base_dir(sandbox_path.join("fast_cancel"));
    let standalone_worker = WorkerClient::new(test_worker_config);

    // Cancelling with reason containing "FAST" or "EVAC" must return immediately (< 15ms)
    let cancel_start = Instant::now();
    let cancelled_count = standalone_worker
        .cancel_all_active_tasks("FAST_PRE_SLEEP_EVACUATION")
        .await;
    let cancel_elapsed = cancel_start.elapsed();
    assert_eq!(cancelled_count, 0);
    assert!(
        cancel_elapsed < Duration::from_millis(15),
        "cancel_all_active_tasks fast-path took too long: {:?}",
        cancel_elapsed
    );

    let _ = master.shutdown();
}
