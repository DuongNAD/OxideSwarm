//! Autonomous role arbitrage, mobile master refusal matrix, authority scoring ($S_{\text{auth}}$),
//! and cryptographic master lease tickets (§5.1, §5.2, §5.3).

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::time::Duration;
use uuid::Uuid;

use crate::cluster_auth::ClusterSecret;

/// Minimum battery percentage required for a mobile device to act as coordinator.
pub const MIN_MASTER_BATTERY_PCT: u8 = 50;

/// Maximum battery temperature (°C) before coordinator role is refused.
pub const MAX_MASTER_BATTERY_TEMP_C: f32 = 40.0;

/// Minimum authority score required to be eligible for coordinator role election.
pub const ELECTION_THRESHOLD_SCORE: i32 = 2500;

/// Minimum boundary for authority scoring scale.
pub const SCORE_MIN: i32 = -10000;

/// Maximum boundary for authority scoring scale.
pub const SCORE_MAX: i32 = 10000;

/// Master lease ticket validity duration in milliseconds (30 seconds).
pub const LEASE_VALIDITY_DURATION_MS: u64 = 30_000;

/// Master lease ticket renewal interval in milliseconds (10 seconds).
pub const LEASE_RENEWAL_INTERVAL_MS: u64 = 10_000;

/// Domain separation label for lease HMAC generation.
pub const LEASE_HMAC_LABEL: &[u8] = b"oxs1-master-lease";

/// Specific violation reason preventing a mobile node from assuming or holding the Master coordinator role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MasterRefusalReason {
    /// Device is discharging / running on battery power alone.
    NotCharging,
    /// Battery level is below the minimum threshold required for coordinator buffer.
    BatteryBelowThreshold { pct: u8, threshold: u8 },
    /// Network interface is cellular (LTE, 5G, 4G, 3G) or metered.
    CellularOrMeteredNetwork { network_type: String },
    /// Device is experiencing thermal throttling or temperature exceeds safety limit.
    ThermalHeadroomExceeded {
        temp_bits: Option<u32>,
        throttled: bool,
    },
    /// Android Foreground Service is not actively elevated.
    ForegroundServiceInactive,
}

impl MasterRefusalReason {
    pub fn thermal_exceeded(temp_c: Option<f32>, throttled: bool) -> Self {
        Self::ThermalHeadroomExceeded {
            temp_bits: temp_c.map(|t| t.to_bits()),
            throttled,
        }
    }

    pub fn is_not_charging(&self) -> bool {
        matches!(self, Self::NotCharging)
    }

    pub fn is_battery_below_threshold(&self) -> bool {
        matches!(self, Self::BatteryBelowThreshold { .. })
    }

    pub fn is_cellular(&self) -> bool {
        matches!(self, Self::CellularOrMeteredNetwork { .. })
    }

    pub fn is_thermal_exceeded(&self) -> bool {
        matches!(self, Self::ThermalHeadroomExceeded { .. })
    }

    pub fn is_foreground_inactive(&self) -> bool {
        matches!(self, Self::ForegroundServiceInactive)
    }
}

impl std::fmt::Display for MasterRefusalReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotCharging => write!(f, "Device is running on battery (not charging)"),
            Self::BatteryBelowThreshold { pct, threshold } => {
                write!(
                    f,
                    "Battery level {pct}% is below minimum {threshold}% required for coordinator"
                )
            }
            Self::CellularOrMeteredNetwork { network_type } => {
                write!(
                    f,
                    "Network interface '{network_type}' is cellular or metered"
                )
            }
            Self::ThermalHeadroomExceeded {
                temp_bits,
                throttled,
            } => {
                let temp_str = temp_bits
                    .map(|b| format!("{:.1}°C", f32::from_bits(b)))
                    .unwrap_or_else(|| "unknown".into());
                write!(
                    f,
                    "Thermal limit exceeded (temp: {temp_str}, throttled: {throttled})"
                )
            }
            Self::ForegroundServiceInactive => {
                write!(
                    f,
                    "Android Foreground Service (FGS) is not active; Doze mode would suspend master"
                )
            }
        }
    }
}

/// Telemetry snapshot used to evaluate mobile master refusal rules.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MobileEnvironmentTelemetry {
    pub is_charging: Option<bool>,
    pub battery_pct: Option<u8>,
    pub network_type: Option<String>,
    pub thermal_throttled: bool,
    pub battery_temperature: Option<f32>,
    pub has_fgs_active: bool,
}

impl MobileEnvironmentTelemetry {
    pub fn new(
        is_charging: bool,
        battery_pct: u8,
        network_type: impl Into<String>,
        battery_temperature: f32,
        thermal_throttled: bool,
        has_fgs_active: bool,
    ) -> Self {
        Self {
            is_charging: Some(is_charging),
            battery_pct: Some(battery_pct),
            network_type: Some(network_type.into()),
            battery_temperature: Some(battery_temperature),
            thermal_throttled,
            has_fgs_active,
        }
    }
}

/// Evaluates all 5 mobile refusal rules, returning a vector of all active violations.
pub fn evaluate_mobile_master_refusal_matrix(
    telemetry: &MobileEnvironmentTelemetry,
) -> Vec<MasterRefusalReason> {
    let mut reasons = Vec::new();

    // Rule 1: Power source must be actively charging
    if telemetry.is_charging != Some(true) {
        reasons.push(MasterRefusalReason::NotCharging);
    }

    // Rule 2: Battery reserve must be at least 50%
    let pct = telemetry.battery_pct.unwrap_or(0);
    if pct < MIN_MASTER_BATTERY_PCT {
        reasons.push(MasterRefusalReason::BatteryBelowThreshold {
            pct,
            threshold: MIN_MASTER_BATTERY_PCT,
        });
    }

    // Rule 3: Network interface must not be cellular or metered
    if let Some(ref net) = telemetry.network_type {
        let net_lower = net.to_ascii_lowercase();
        if is_cellular_or_metered(&net_lower) {
            reasons.push(MasterRefusalReason::CellularOrMeteredNetwork {
                network_type: net.clone(),
            });
        }
    }

    // Rule 4: Thermal headroom (must not be throttled and temp <= 40.0°C)
    let temp_over = telemetry
        .battery_temperature
        .is_some_and(|t| t > MAX_MASTER_BATTERY_TEMP_C);
    if telemetry.thermal_throttled || temp_over {
        reasons.push(MasterRefusalReason::thermal_exceeded(
            telemetry.battery_temperature,
            telemetry.thermal_throttled,
        ));
    }

    // Rule 5: Execution context must have active Android Foreground Service
    if !telemetry.has_fgs_active {
        reasons.push(MasterRefusalReason::ForegroundServiceInactive);
    }

    reasons
}

/// Helper identifying cellular or metered network descriptors.
pub fn is_cellular_or_metered(net_lower: &str) -> bool {
    net_lower.contains("cell")
        || net_lower.contains("mobile")
        || net_lower.contains("lte")
        || net_lower.contains("5g")
        || net_lower.contains("4g")
        || net_lower.contains("3g")
        || net_lower.contains("cgnat")
        || net_lower.contains("metered")
}

/// Returns Ok(()) if the mobile telemetry satisfies coordinator prerequisites, or the first refusal reason.
pub fn check_mobile_master_eligibility(
    telemetry: &MobileEnvironmentTelemetry,
) -> Result<(), MasterRefusalReason> {
    let reasons = evaluate_mobile_master_refusal_matrix(telemetry);
    if let Some(first) = reasons.into_iter().next() {
        Err(first)
    } else {
        Ok(())
    }
}

/// Alias for `check_mobile_master_eligibility`.
pub fn evaluate_mobile_master_refusal(
    profile: &MobileEnvironmentTelemetry,
) -> Result<(), MasterRefusalReason> {
    check_mobile_master_eligibility(profile)
}

/// Classification of node platform for authority scoring ($P_{\text{platform}}$).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PlatformType {
    LinuxServer = 5000,
    MacOs = 4500,
    Windows = 4000,
    Android = 500,
    Unknown = 0,
}

impl PlatformType {
    pub fn detect() -> Self {
        if cfg!(target_os = "android") || std::path::Path::new("/system/build.prop").exists() {
            PlatformType::Android
        } else if cfg!(target_os = "linux") {
            PlatformType::LinuxServer
        } else if cfg!(target_os = "macos") {
            PlatformType::MacOs
        } else if cfg!(target_os = "windows") {
            PlatformType::Windows
        } else {
            PlatformType::Unknown
        }
    }

    pub fn weight(&self) -> i32 {
        *self as i32
    }
}

/// Evaluation breakdown of a node's Authority Score ($S_{\text{auth}}$).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityEvaluation {
    pub total_score: i32,
    pub platform_weight: i32,
    pub power_weight: i32,
    pub net_weight: i32,
    pub thermal_weight: i32,
    pub uptime_weight: i32,
    pub uptime_seconds: u64,
    pub is_eligible: bool,
}

/// Computes deterministic authority score according to §5.2.
#[allow(clippy::too_many_arguments)]
pub fn calculate_authority_score(
    platform: PlatformType,
    is_charging: Option<bool>,
    battery_pct: Option<u8>,
    network_type: Option<&str>,
    thermal_throttled: bool,
    battery_temperature: Option<f32>,
    uptime_seconds: u64,
    has_fgs_active: bool,
) -> AuthorityEvaluation {
    // If mobile, evaluate refusal matrix first. Any violation immediately forces score = -10,000.
    if platform == PlatformType::Android {
        let telem = MobileEnvironmentTelemetry {
            is_charging,
            battery_pct,
            network_type: network_type.map(str::to_string),
            thermal_throttled,
            battery_temperature,
            has_fgs_active,
        };
        if check_mobile_master_eligibility(&telem).is_err() {
            return AuthorityEvaluation {
                total_score: SCORE_MIN,
                platform_weight: platform.weight(),
                power_weight: -3000,
                net_weight: -2000,
                thermal_weight: -5000,
                uptime_weight: 0,
                uptime_seconds,
                is_eligible: false,
            };
        }
    }

    // 1. Platform Weight
    let platform_weight = platform.weight();

    // 2. Power Weight
    let power_weight = match (is_charging, battery_pct) {
        (Some(true), _) | (None, None) => 2000, // AC power or standard desktop
        (Some(false), Some(pct)) | (None, Some(pct)) => {
            if pct > 80 {
                500
            } else if pct >= 30 {
                100
            } else {
                -3000
            }
        }
        (Some(false), None) => 0,
    };

    // 3. Network Weight
    let net_weight = if let Some(net) = network_type {
        let net_lower = net.to_ascii_lowercase();
        if is_cellular_or_metered(&net_lower) {
            -2000
        } else if net_lower.contains("ethernet")
            || net_lower.contains("eth")
            || net_lower.contains("lan")
            || net_lower.contains("gigabit")
        {
            1500
        } else if net_lower.contains("5g")
            || net_lower.contains("wifi6")
            || net_lower.contains("wifi 6")
            || net_lower.contains("5ghz")
        {
            1000
        } else if net_lower.contains("wifi") || net_lower.contains("wlan") {
            700
        } else {
            0
        }
    } else if platform != PlatformType::Android {
        1500
    } else {
        0
    };

    // 4. Thermal Weight
    let thermal_weight = if thermal_throttled || battery_temperature.is_some_and(|t| t > 40.0) {
        -5000
    } else if battery_temperature.is_some_and(|t| t > 37.0) {
        -1500
    } else {
        0
    };

    // 5. Uptime Weight
    let uptime_weight = ((uptime_seconds / 10).min(1000)) as i32;

    let raw_sum = platform_weight + power_weight + net_weight + thermal_weight + uptime_weight;
    let total_score = raw_sum.clamp(SCORE_MIN, SCORE_MAX);
    let is_eligible = total_score >= ELECTION_THRESHOLD_SCORE;

    AuthorityEvaluation {
        total_score,
        platform_weight,
        power_weight,
        net_weight,
        thermal_weight,
        uptime_weight,
        uptime_seconds,
        is_eligible,
    }
}

/// Candidate descriptor used to deterministically order candidate nodes in leader elections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateRanking {
    pub node_id: Uuid,
    pub authority_score: i32,
    pub platform_weight: i32,
    pub uptime_seconds: u64,
}

impl CandidateRanking {
    pub fn new(
        node_id: Uuid,
        authority_score: i32,
        platform_weight: i32,
        uptime_seconds: u64,
    ) -> Self {
        Self {
            node_id,
            authority_score,
            platform_weight,
            uptime_seconds,
        }
    }
}

impl Ord for CandidateRanking {
    fn cmp(&self, other: &Self) -> Ordering {
        self.authority_score
            .cmp(&other.authority_score)
            .then_with(|| self.platform_weight.cmp(&other.platform_weight))
            .then_with(|| self.uptime_seconds.cmp(&other.uptime_seconds))
            // Lexicographically lower UUID byte array wins tie
            .then_with(|| other.node_id.as_bytes().cmp(self.node_id.as_bytes()))
    }
}

impl PartialOrd for CandidateRanking {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Resolves tie between two candidates according to the 4-stage algorithm.
pub fn resolve_election_tie(a: &CandidateRanking, b: &CandidateRanking) -> Ordering {
    a.cmp(b)
}

/// Cryptographically signed, time-bounded ticket proving coordinator authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MasterLeaseTicket {
    /// Target cluster identifier.
    pub cluster_id: Uuid,
    /// Node UUID of the designated master coordinator.
    pub master_id: Uuid,
    /// Monotonically increasing lease sequence number.
    pub lease_sequence: u64,
    /// Authority score calculated at ticket generation.
    pub authority_score: i32,
    /// Creation timestamp in milliseconds since Unix epoch.
    pub issued_at_epoch_ms: u64,
    /// Expiration timestamp in milliseconds since Unix epoch.
    pub expires_at_epoch_ms: u64,
    /// 32-byte HMAC-SHA256 signature generated with the shared cluster secret.
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LeaseValidationError {
    #[error("Master lease ticket expired at epoch {expired_at}ms, current time is {current}ms")]
    Expired { expired_at: u64, current: u64 },
    #[error("Invalid lease time range (expires_at <= issued_at)")]
    InvalidTimeRange,
    #[error("Invalid signature length: expected 32 bytes, got {0}")]
    InvalidSignatureLength(usize),
    #[error("Lease ticket signature mismatch against cluster secret")]
    SignatureMismatch,
    #[error("Authority score {0} is below minimum election threshold (2500)")]
    ScoreBelowThreshold(i32),
}

impl MasterLeaseTicket {
    /// Constructs a fresh unsigned lease ticket valid for 30 seconds.
    pub fn new(
        cluster_id: Uuid,
        master_id: Uuid,
        lease_sequence: u64,
        authority_score: i32,
        now_epoch_ms: u64,
    ) -> Self {
        Self {
            cluster_id,
            master_id,
            lease_sequence,
            authority_score,
            issued_at_epoch_ms: now_epoch_ms,
            expires_at_epoch_ms: now_epoch_ms + LEASE_VALIDITY_DURATION_MS,
            signature: Vec::new(),
        }
    }

    /// Issues and signs a fresh lease ticket with custom validity.
    pub fn issue(
        cluster_id: Uuid,
        master_id: Uuid,
        lease_sequence: u64,
        authority_score: i32,
        now_epoch_ms: u64,
        validity: Duration,
        secret: &ClusterSecret,
    ) -> Self {
        let mut ticket = Self {
            cluster_id,
            master_id,
            lease_sequence,
            authority_score,
            issued_at_epoch_ms: now_epoch_ms,
            expires_at_epoch_ms: now_epoch_ms + validity.as_millis() as u64,
            signature: Vec::new(),
        };
        ticket.sign_with_secret(secret);
        ticket
    }

    /// Serializes canonical 60-byte binary payload for HMAC computation.
    /// Layout: [cluster_id: 16B][master_id: 16B][seq: 8B BE][score: 4B BE][issued: 8B BE][expires: 8B BE]
    pub fn canonical_bytes(&self) -> [u8; 60] {
        let mut buf = [0u8; 60];
        buf[0..16].copy_from_slice(self.cluster_id.as_bytes());
        buf[16..32].copy_from_slice(self.master_id.as_bytes());
        buf[32..40].copy_from_slice(&self.lease_sequence.to_be_bytes());
        buf[40..44].copy_from_slice(&self.authority_score.to_be_bytes());
        buf[44..52].copy_from_slice(&self.issued_at_epoch_ms.to_be_bytes());
        buf[52..60].copy_from_slice(&self.expires_at_epoch_ms.to_be_bytes());
        buf
    }

    /// Signs this ticket with the cluster secret using HMAC-SHA256.
    pub fn sign_with_secret(&mut self, secret: &ClusterSecret) {
        let canonical = self.canonical_bytes();
        let tag = secret.mac(&[LEASE_HMAC_LABEL, &canonical]);
        self.signature = tag.to_vec();
    }

    /// Validates ticket expiry, time window, and cryptographic HMAC signature.
    pub fn verify(
        &self,
        secret: Option<&ClusterSecret>,
        current_epoch_ms: u64,
    ) -> Result<(), LeaseValidationError> {
        // 1. Time validity checks
        if current_epoch_ms > self.expires_at_epoch_ms {
            return Err(LeaseValidationError::Expired {
                expired_at: self.expires_at_epoch_ms,
                current: current_epoch_ms,
            });
        }
        if self.expires_at_epoch_ms <= self.issued_at_epoch_ms {
            return Err(LeaseValidationError::InvalidTimeRange);
        }

        // 2. Cryptographic signature verification
        if let Some(sec) = secret {
            if self.signature.len() != 32 {
                return Err(LeaseValidationError::InvalidSignatureLength(
                    self.signature.len(),
                ));
            }
            let canonical = self.canonical_bytes();
            if !sec.verify_mac(&[LEASE_HMAC_LABEL, &canonical], &self.signature) {
                return Err(LeaseValidationError::SignatureMismatch);
            }
        }

        Ok(())
    }

    /// Convenience predicate returning true if ticket is valid and unexpired.
    pub fn is_valid(&self, secret: &ClusterSecret, current_epoch_ms: u64) -> bool {
        self.verify(Some(secret), current_epoch_ms).is_ok()
    }
}

/// Handover claim evaluation outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimVerdict {
    AcceptedAndStepDown,
    RejectedInferiorScore,
    RejectedInvalidTicket(String),
}

/// Evaluates coordinator role claim between incumbent and claimant tickets.
pub fn evaluate_master_claim(
    incumbent_ticket: &MasterLeaseTicket,
    claimant_ticket: &MasterLeaseTicket,
    secret: &ClusterSecret,
    now_epoch_ms: u64,
) -> ClaimVerdict {
    if let Err(e) = claimant_ticket.verify(Some(secret), now_epoch_ms) {
        return ClaimVerdict::RejectedInvalidTicket(e.to_string());
    }
    if claimant_ticket.authority_score < ELECTION_THRESHOLD_SCORE {
        return ClaimVerdict::RejectedInferiorScore;
    }
    if claimant_ticket.authority_score <= incumbent_ticket.authority_score {
        return ClaimVerdict::RejectedInferiorScore;
    }
    ClaimVerdict::AcceptedAndStepDown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_refusal_matrix_all_5_rules() {
        // 1. Discharging -> Refuse NotCharging
        let telem1 = MobileEnvironmentTelemetry {
            is_charging: Some(false),
            battery_pct: Some(80),
            network_type: Some("wifi".into()),
            thermal_throttled: false,
            battery_temperature: Some(30.0),
            has_fgs_active: true,
        };
        assert_eq!(
            check_mobile_master_eligibility(&telem1),
            Err(MasterRefusalReason::NotCharging)
        );

        // 2. Battery < 50% while charging -> Refuse BatteryBelowThreshold
        let telem2 = MobileEnvironmentTelemetry {
            is_charging: Some(true),
            battery_pct: Some(49),
            network_type: Some("wifi".into()),
            thermal_throttled: false,
            battery_temperature: Some(30.0),
            has_fgs_active: true,
        };
        assert!(matches!(
            check_mobile_master_eligibility(&telem2),
            Err(MasterRefusalReason::BatteryBelowThreshold {
                pct: 49,
                threshold: 50
            })
        ));

        // 3. Cellular -> Refuse CellularOrMeteredNetwork
        let telem3 = MobileEnvironmentTelemetry {
            is_charging: Some(true),
            battery_pct: Some(90),
            network_type: Some("5g cellular".into()),
            thermal_throttled: false,
            battery_temperature: Some(30.0),
            has_fgs_active: true,
        };
        assert!(matches!(
            check_mobile_master_eligibility(&telem3),
            Err(MasterRefusalReason::CellularOrMeteredNetwork { .. })
        ));

        // 4. Overheated (41°C) -> Refuse ThermalHeadroomExceeded
        let telem4 = MobileEnvironmentTelemetry {
            is_charging: Some(true),
            battery_pct: Some(90),
            network_type: Some("wifi".into()),
            thermal_throttled: false,
            battery_temperature: Some(41.0),
            has_fgs_active: true,
        };
        assert!(matches!(
            check_mobile_master_eligibility(&telem4),
            Err(MasterRefusalReason::ThermalHeadroomExceeded { .. })
        ));

        // 5. Inactive FGS -> Refuse ForegroundServiceInactive
        let telem5 = MobileEnvironmentTelemetry {
            is_charging: Some(true),
            battery_pct: Some(90),
            network_type: Some("wifi".into()),
            thermal_throttled: false,
            battery_temperature: Some(30.0),
            has_fgs_active: false,
        };
        assert_eq!(
            check_mobile_master_eligibility(&telem5),
            Err(MasterRefusalReason::ForegroundServiceInactive)
        );

        // Satisfies all 5 -> Qualified!
        let telem_ok = MobileEnvironmentTelemetry {
            is_charging: Some(true),
            battery_pct: Some(90),
            network_type: Some("wifi".into()),
            thermal_throttled: false,
            battery_temperature: Some(30.0),
            has_fgs_active: true,
        };
        assert_eq!(check_mobile_master_eligibility(&telem_ok), Ok(()));
    }

    #[test]
    fn test_authority_scoring_calculation() {
        // Plugged-in mobile phone on Wi-Fi: 500 + 2000 + 700 + 0 + 300 = 3500 (Eligible!)
        let eval_mob = calculate_authority_score(
            PlatformType::Android,
            Some(true),
            Some(85),
            Some("wifi"),
            false,
            Some(32.0),
            3000,
            true,
        );
        assert_eq!(eval_mob.total_score, 3500);
        assert!(eval_mob.is_eligible);

        // Linux workstation on Ethernet: 5000 + 2000 + 1500 + 0 + 500 = 9000 (Eligible!)
        let eval_linux = calculate_authority_score(
            PlatformType::LinuxServer,
            None,
            None,
            Some("ethernet"),
            false,
            None,
            5000,
            false,
        );
        assert_eq!(eval_linux.total_score, 9000);
        assert!(eval_linux.is_eligible);
    }

    #[test]
    fn test_deterministic_4_stage_tie_breaking() {
        let uuid_a = Uuid::from_bytes([0x01; 16]);
        let uuid_b = Uuid::from_bytes([0x02; 16]);

        // Stage 1: Higher score wins
        let c1 = CandidateRanking::new(uuid_a, 9000, 4000, 100);
        let c2 = CandidateRanking::new(uuid_b, 8500, 5000, 500);
        assert!(c1 > c2);

        // Stage 2: Equal score -> Higher platform wins
        let c3 = CandidateRanking::new(uuid_a, 8000, 5000, 100);
        let c4 = CandidateRanking::new(uuid_b, 8000, 4500, 500);
        assert!(c3 > c4);

        // Stage 3: Equal score & platform -> Higher uptime wins
        let c5 = CandidateRanking::new(uuid_a, 8000, 5000, 600);
        let c6 = CandidateRanking::new(uuid_b, 8000, 5000, 300);
        assert!(c5 > c6);

        // Stage 4: Equal score, platform & uptime -> Lexicographically lower UUID wins
        let c7 = CandidateRanking::new(uuid_a, 8000, 5000, 500);
        let c8 = CandidateRanking::new(uuid_b, 8000, 5000, 500);
        assert!(uuid_a < uuid_b);
        assert!(c7 > c8, "Lower UUID byte array must rank higher");
    }

    #[test]
    fn test_master_lease_ticket_signing_and_verification() {
        let secret = ClusterSecret::generate().unwrap();
        let cluster_id = Uuid::new_v4();
        let master_id = Uuid::new_v4();
        let now = 1_000_000;

        let mut ticket = MasterLeaseTicket::new(cluster_id, master_id, 1, 8500, now);
        ticket.sign_with_secret(&secret);

        // Valid signature & unexpired
        assert!(ticket.verify(Some(&secret), now + 10_000).is_ok());

        // Expired ticket rejected
        assert!(matches!(
            ticket.verify(Some(&secret), now + 35_000),
            Err(LeaseValidationError::Expired { .. })
        ));

        // Tampered payload rejected
        let mut tampered = ticket.clone();
        tampered.authority_score = 9999;
        assert_eq!(
            tampered.verify(Some(&secret), now + 5_000),
            Err(LeaseValidationError::SignatureMismatch)
        );
    }
}
