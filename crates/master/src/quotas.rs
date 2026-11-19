//! Per-participant labels, quota policies, priority bands, and fair-share resolution (`ECO-9`,
//! [design 0097](../../docs/design/0097-per-participant-quotas.md)).
//!
//! Quotas share a circle grid between mutually trusted members (`design 0050 D6`): they are not
//! a cryptographic boundary against a malicious participant holding an arbitrary key.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use rusty_grid_core::error::{GridError, GridResult};
use serde::{Deserialize, Serialize};

use crate::server::MemberStore;

/// Reserved participant identity for v2 secret peers and unauthenticated local/dashboard hosts.
pub const ORGANISER_PARTICIPANT: &str = "organiser";
/// Prefix used when an enrolled v3 member has no explicit participant label (`member:<member_id>`).
/// Reserved so no admin-supplied participant label can impersonate an unlabeled member.
pub const MEMBER_PARTICIPANT_PREFIX: &str = "member:";
/// Maximum ASCII length of a participant label.
pub const MAX_PARTICIPANT_LABEL_CHARS: usize = 64;

const TRUST_BOUNDARY_NOTE: &str = "Quotas share the grid cooperatively between people who trust each other and are not a security boundary against a hostile participant (design 0050 D6).";

/// Scheduling priority tier assigned to a participant (`ECO-9`, design 0097 D2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriorityBand {
    Low,
    Normal,
    High,
}

impl PriorityBand {
    /// Default queue priority assigned when a submission carries no explicit priority.
    pub const fn default_priority(self) -> u32 {
        match self {
            PriorityBand::Low => 0,
            PriorityBand::Normal => 50,
            PriorityBand::High => 100,
        }
    }

    /// Inclusive ceiling enforced when a caller supplies an explicit numeric priority.
    pub const fn max_priority(self) -> u32 {
        match self {
            PriorityBand::Low => 33,
            PriorityBand::Normal => 66,
            PriorityBand::High => 100,
        }
    }
}

impl FromStr for PriorityBand {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "low" => Ok(PriorityBand::Low),
            "normal" => Ok(PriorityBand::Normal),
            "high" => Ok(PriorityBand::High),
            other => Err(format!(
                "invalid priority band '{other}' (expected low, normal, or high)"
            )),
        }
    }
}

impl fmt::Display for PriorityBand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PriorityBand::Low => write!(f, "low"),
            PriorityBand::Normal => write!(f, "normal"),
            PriorityBand::High => write!(f, "high"),
        }
    }
}

/// Validates and canonicalizes an admin-assigned participant label (`1..=64` chars of
/// `[a-z0-9._@-]`, normalized lowercase ASCII, refusing empty or `"member:*"`).
pub fn validate_participant_label(raw: &str) -> Result<String, String> {
    if raw
        .chars()
        .any(rusty_grid_core::noise_auth::is_control_or_format_char)
    {
        return Err("participant label cannot contain control or format characters".into());
    }
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err("participant label cannot be empty".into());
    }
    if normalized.len() > MAX_PARTICIPANT_LABEL_CHARS {
        return Err(format!(
            "participant label cannot exceed {MAX_PARTICIPANT_LABEL_CHARS} characters"
        ));
    }
    if normalized.starts_with(MEMBER_PARTICIPANT_PREFIX) {
        return Err(format!(
            "participant label '{normalized}' cannot start with reserved prefix '{MEMBER_PARTICIPANT_PREFIX}'"
        ));
    }
    if !normalized.bytes().all(|b| {
        b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'@' | b'-')
    }) {
        return Err(format!(
            "participant label '{normalized}' may only contain ASCII letters, digits, '.', '_', '-', or '@'"
        ));
    }
    Ok(normalized)
}

/// Validates a quota target string accepted in CLI flags, config files, and HTTP quota endpoints:
/// either `"organiser"`, a valid participant label, or an unlabeled fallback `"member:<hex>"`.
pub fn validate_quota_target(raw: &str) -> Result<String, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    if let Some(hex) = normalized.strip_prefix(MEMBER_PARTICIPANT_PREFIX) {
        if hex.is_empty()
            || hex.len() > MAX_PARTICIPANT_LABEL_CHARS
            || !hex.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(format!(
                "invalid member participant target '{normalized}' (expected member:<hex member_id>)"
            ));
        }
        return Ok(normalized);
    }
    validate_participant_label(&normalized)
}

/// Configurable per-participant limits and scheduling weights (`ECO-9`, design 0097 D2).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantQuota {
    /// Maximum concurrently active (`Scheduled`, `Running`, or held) tasks for this participant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_running_tasks: Option<usize>,
    /// Maximum CPU-core slots (`max(1, cpu_cores)`) held simultaneously by this participant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_slots: Option<usize>,
    /// Maximum waiting (`Submitted`, `Queued`, `Retrying`) tasks allowed in the queue at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_queued_tasks: Option<usize>,
    /// Scheduling priority band (assigns default priority and enforces maximum priority).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority_band: Option<PriorityBand>,
    /// Weighted round-robin weight inside `FairShareQueues` (`>= 1`, default `1`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fair_share_weight: Option<u32>,
}

impl ParticipantQuota {
    /// Ensures every specified numeric ceiling is at least 1.
    pub fn validate(&self, context: &str) -> Result<(), String> {
        if self.max_running_tasks == Some(0) {
            return Err(format!(
                "{context}: max_running_tasks must be at least 1 (got 0)"
            ));
        }
        if self.max_slots == Some(0) {
            return Err(format!("{context}: max_slots must be at least 1 (got 0)"));
        }
        if self.max_queued_tasks == Some(0) {
            return Err(format!(
                "{context}: max_queued_tasks must be at least 1 (got 0)"
            ));
        }
        if self.fair_share_weight == Some(0) {
            return Err(format!(
                "{context}: fair_share_weight must be at least 1 (got 0)"
            ));
        }
        Ok(())
    }

    /// True when no limit, band, or custom weight is configured.
    pub fn is_empty(&self) -> bool {
        self.max_running_tasks.is_none()
            && self.max_slots.is_none()
            && self.max_queued_tasks.is_none()
            && self.priority_band.is_none()
            && self.fair_share_weight.is_none()
    }

    /// Layers `self` (higher precedence) over `fallback` (lower precedence) field by field.
    pub fn merge_with_default(&self, fallback: &Self) -> Self {
        Self {
            max_running_tasks: self.max_running_tasks.or(fallback.max_running_tasks),
            max_slots: self.max_slots.or(fallback.max_slots),
            max_queued_tasks: self.max_queued_tasks.or(fallback.max_queued_tasks),
            priority_band: self.priority_band.or(fallback.priority_band),
            fair_share_weight: self.fair_share_weight.or(fallback.fair_share_weight),
        }
    }
}

/// Parses `<PARTICIPANT>:<KEY>=<VAL>[,...]` (e.g. `alice:running=2,slots=4,queued=8,priority=high,weight=2`
/// or `member:abcd1234:running=1`).
pub fn parse_participant_quota_spec(raw: &str) -> Result<(String, ParticipantQuota), String> {
    let trimmed = raw.trim();
    let (target_raw, spec_raw) = trimmed.rsplit_once(':').ok_or_else(|| {
        format!("invalid participant quota '{trimmed}' (expected <participant>:running=N,slots=N,queued=N,priority=low|normal|high,weight=N)")
    })?;
    let target = validate_quota_target(target_raw)?;
    let spec = spec_raw.trim();
    if spec.is_empty() {
        return Err(format!(
            "empty quota specification for participant '{target}'"
        ));
    }

    let mut quota = ParticipantQuota::default();
    for part in spec.split(',') {
        let item = part.trim();
        if item.is_empty() {
            continue;
        }
        let (key, val) = item.split_once('=').ok_or_else(|| {
            format!("invalid quota entry '{item}' for '{target}' (expected key=value)")
        })?;
        let key = key.trim().to_ascii_lowercase();
        let val = val.trim();
        match key.as_str() {
            "running" | "max_running" | "max_running_tasks" => {
                let n: usize = val.parse().map_err(|_| {
                    format!(
                        "invalid {key}='{val}' for participant '{target}' (expected integer >= 1)"
                    )
                })?;
                quota.max_running_tasks = Some(n);
            }
            "slots" | "max_slots" => {
                let n: usize = val.parse().map_err(|_| {
                    format!(
                        "invalid {key}='{val}' for participant '{target}' (expected integer >= 1)"
                    )
                })?;
                quota.max_slots = Some(n);
            }
            "queued" | "max_queued" | "max_queued_tasks" => {
                let n: usize = val.parse().map_err(|_| {
                    format!(
                        "invalid {key}='{val}' for participant '{target}' (expected integer >= 1)"
                    )
                })?;
                quota.max_queued_tasks = Some(n);
            }
            "priority" | "priority_band" => {
                quota.priority_band = Some(val.parse()?);
            }
            "weight" | "fair_share_weight" => {
                let w: u32 = val.parse().map_err(|_| {
                    format!(
                        "invalid {key}='{val}' for participant '{target}' (expected integer >= 1)"
                    )
                })?;
                quota.fair_share_weight = Some(w);
            }
            other => {
                return Err(format!(
                    "unknown quota key '{other}' for '{target}' (expected running, slots, queued, priority, or weight)"
                ));
            }
        }
    }
    quota.validate(&format!("participant '{target}'"))?;
    if quota.is_empty() {
        return Err(format!("quota specification for '{target}' set no fields"));
    }
    Ok((target, quota))
}

/// Static CLI/TOML baseline quota policy passed into `ServerConfig`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaPolicyConfig {
    pub default_quota: ParticipantQuota,
    pub participant_overrides: BTreeMap<String, ParticipantQuota>,
}

impl QuotaPolicyConfig {
    /// Validates the default quota and every named participant override.
    pub fn validate(&self) -> Result<(), String> {
        self.default_quota.validate("default_participant_quota")?;
        for (participant, quota) in &self.participant_overrides {
            validate_quota_target(participant)?;
            quota.validate(&format!("participant '{participant}'"))?;
        }
        Ok(())
    }
}

/// Current resource usage of one participant across queue states.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantUsageCounts {
    pub running_tasks: usize,
    pub running_slots: usize,
    pub queued_tasks: usize,
}

/// Per-participant status row serialized in `GET /api/quotas`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantQuotaStatusDto {
    pub participant: String,
    pub effective_quota: ParticipantQuota,
    pub usage: ParticipantUsageCounts,
    pub fair_share_weight: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub member_ids: Vec<String>,
}

/// Admin-only member -> participant label assignment row returned in `GET /api/quotas`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberParticipantDto {
    pub member_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant: Option<String>,
}

/// Dashboard `GET /api/quotas` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotasSnapshotDto {
    pub trust_note: String,
    pub default_quota: ParticipantQuota,
    pub participants: Vec<ParticipantQuotaStatusDto>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<MemberParticipantDto>,
}

/// Coordinates participant resolution, effective limits, priority enforcement, and live wait reasons.
pub struct QuotaController {
    members: Arc<MemberStore>,
    policy: QuotaPolicyConfig,
    wait_reasons: RwLock<HashMap<String, String>>,
}

impl QuotaController {
    pub fn new(members: Arc<MemberStore>, policy: QuotaPolicyConfig) -> Self {
        Self {
            members,
            policy,
            wait_reasons: RwLock::new(HashMap::new()),
        }
    }

    pub fn members(&self) -> &Arc<MemberStore> {
        &self.members
    }

    /// Resolves `member_id` (or `None` for v2 / local-host peers) to its canonical participant key:
    /// `"organiser"`, `<assigned_label>`, or `"member:<member_id>"`.
    pub fn participant_for_member(&self, member_id: Option<&str>) -> String {
        let Some(mid) = member_id.map(str::trim).filter(|s| !s.is_empty()) else {
            return ORGANISER_PARTICIPANT.to_string();
        };
        if let Some(label) = self.members.participant_for_member(mid) {
            return label;
        }
        format!("{MEMBER_PARTICIPANT_PREFIX}{}", mid.to_ascii_lowercase())
    }

    /// Effective cluster default quota (`members.json` default layered over CLI/TOML default).
    pub fn effective_default_quota(&self) -> ParticipantQuota {
        match self.members.default_participant_quota() {
            Some(ref persisted) => persisted.merge_with_default(&self.policy.default_quota),
            None => self.policy.default_quota.clone(),
        }
    }

    /// Effective quota for `participant` following precedence:
    /// `members.json` participant override > CLI/TOML override > `members.json` default > CLI/TOML default.
    pub fn effective_quota(&self, participant: &str) -> ParticipantQuota {
        let base = self.effective_default_quota();
        let after_cli = match self.policy.participant_overrides.get(participant) {
            Some(cli) => cli.merge_with_default(&base),
            None => base,
        };
        match self.members.quota_overrides().get(participant) {
            Some(persisted) => persisted.merge_with_default(&after_cli),
            None => after_cli,
        }
    }

    /// True if any default or override specifies a `PriorityBand`.
    pub fn any_priority_band_configured(&self) -> bool {
        self.policy.default_quota.priority_band.is_some()
            || self
                .policy
                .participant_overrides
                .values()
                .any(|q| q.priority_band.is_some())
            || self
                .members
                .default_participant_quota()
                .is_some_and(|q| q.priority_band.is_some())
            || self
                .members
                .quota_overrides()
                .values()
                .any(|q| q.priority_band.is_some())
    }

    /// Resolves and checks task priority against the participant's configured `PriorityBand`.
    pub fn resolve_submission_priority(
        &self,
        participant: &str,
        requested: Option<u32>,
    ) -> GridResult<u32> {
        let quota = self.effective_quota(participant);
        if let Some(band) = quota.priority_band {
            if let Some(req) = requested {
                let max = band.max_priority();
                if req > max {
                    return Err(GridError::QuotaExceeded {
                        participant: participant.to_string(),
                        limit_kind: "priority_band".to_string(),
                        detail: format!(
                            "requested priority {req} exceeds configured '{band}' band ceiling {max}"
                        ),
                    });
                }
                return Ok(req);
            }
            return Ok(band.default_priority());
        }
        Ok(requested.unwrap_or(0))
    }

    /// Replaces the current scheduler wait-reason map (`participant -> reason`).
    pub fn set_wait_reasons(&self, reasons: HashMap<String, String>) {
        *self.wait_reasons.write().unwrap_or_else(|e| e.into_inner()) = reasons;
    }

    /// Returns a snapshot of the current per-participant wait reasons.
    pub fn wait_reasons(&self) -> HashMap<String, String> {
        self.wait_reasons
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Assembles `GET /api/quotas`, redacting raw `member_ids` and machine names when
    /// `!is_admin` (`design 0094 D9(c)`).
    pub fn snapshot_dto(
        &self,
        live_usage: &BTreeMap<String, ParticipantUsageCounts>,
        is_admin: bool,
    ) -> QuotasSnapshotDto {
        let member_list = self.members.list_members();
        let label_map = self.members.member_participants();
        let mem_overrides = self.members.quota_overrides();
        let wait_map = self.wait_reasons();

        let mut participants_set: BTreeSet<String> = BTreeSet::new();
        let mut by_participant_members: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for m in &member_list {
            let p = self.participant_for_member(Some(&m.id));
            participants_set.insert(p.clone());
            by_participant_members
                .entry(p)
                .or_default()
                .push(m.id.clone());
        }
        for k in live_usage.keys() {
            participants_set.insert(k.clone());
        }
        for k in self.policy.participant_overrides.keys() {
            participants_set.insert(k.clone());
        }
        for k in mem_overrides.keys() {
            participants_set.insert(k.clone());
        }

        // Deterministic anonymization for unlabeled `member:<id>` keys when serving non-Admin Observers.
        let mut anon_map: HashMap<String, String> = HashMap::new();
        if !is_admin {
            let mut next = 1usize;
            for p in &participants_set {
                if p.starts_with(MEMBER_PARTICIPANT_PREFIX) {
                    anon_map.insert(p.clone(), format!("participant-{next}"));
                    next += 1;
                }
            }
        }

        let mut rows = Vec::with_capacity(participants_set.len());
        for p in participants_set {
            let effective = self.effective_quota(&p);
            let weight = effective.fair_share_weight.unwrap_or(1).max(1);
            let usage = live_usage.get(&p).copied().unwrap_or_default();
            let wait_reason = wait_map.get(&p).cloned();
            let display_participant = if !is_admin {
                anon_map.get(&p).cloned().unwrap_or_else(|| p.clone())
            } else {
                p.clone()
            };
            let member_ids = if is_admin {
                by_participant_members.remove(&p).unwrap_or_default()
            } else {
                Vec::new()
            };
            rows.push(ParticipantQuotaStatusDto {
                participant: display_participant,
                effective_quota: effective,
                usage,
                fair_share_weight: weight,
                wait_reason,
                member_ids,
            });
        }

        let members = if is_admin {
            member_list
                .into_iter()
                .map(|m| {
                    let participant = label_map.get(&m.id).cloned();
                    MemberParticipantDto {
                        member_id: m.id,
                        name: m.name,
                        participant,
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        QuotasSnapshotDto {
            trust_note: TRUST_BOUNDARY_NOTE.to_string(),
            default_quota: self.effective_default_quota(),
            participants: rows,
            members,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::noise_auth::{MemberEntry, MemberRole, NodeKeypair};

    #[test]
    fn label_validation_enforces_charset_length_and_reserved_prefix() {
        assert_eq!(
            validate_participant_label("  Alice_Lab-1@grid  ").unwrap(),
            "alice_lab-1@grid"
        );
        assert_eq!(
            validate_participant_label("organiser").unwrap(),
            "organiser"
        );
        assert!(validate_participant_label("").is_err());
        assert!(validate_participant_label("   ").is_err());
        assert!(validate_participant_label("member:deadbeef").is_err());
        assert!(validate_participant_label("bad space").is_err());
        assert!(validate_participant_label("bad/slash").is_err());
        assert!(validate_participant_label(&"a".repeat(65)).is_err());

        assert_eq!(
            validate_quota_target("member:ABCD1234").unwrap(),
            "member:abcd1234"
        );
        assert!(validate_quota_target("member:not-hex").is_err());
    }

    #[test]
    fn spec_parser_and_priority_band_rules_work_with_member_colons() {
        let (p1, q1) =
            parse_participant_quota_spec("alice:running=2,slots=4,queued=8,priority=high,weight=3")
                .unwrap();
        assert_eq!(p1, "alice");
        assert_eq!(q1.max_running_tasks, Some(2));
        assert_eq!(q1.max_slots, Some(4));
        assert_eq!(q1.max_queued_tasks, Some(8));
        assert_eq!(q1.priority_band, Some(PriorityBand::High));
        assert_eq!(q1.fair_share_weight, Some(3));

        let (p2, q2) =
            parse_participant_quota_spec("member:deadbeef1234:running=1,priority=low").unwrap();
        assert_eq!(p2, "member:deadbeef1234");
        assert_eq!(q2.max_running_tasks, Some(1));
        assert_eq!(q2.priority_band, Some(PriorityBand::Low));

        assert!(parse_participant_quota_spec("alice:running=0").is_err());
        assert!(parse_participant_quota_spec("alice:slots=0").is_err());
        assert!(parse_participant_quota_spec("alice:queued=0").is_err());
        assert!(parse_participant_quota_spec("alice:weight=0").is_err());
        assert!(parse_participant_quota_spec("alice:unknown=1").is_err());
    }

    #[test]
    fn precedence_priority_band_and_observer_redaction() {
        let kp_a = NodeKeypair::generate().unwrap();
        let kp_b = NodeKeypair::generate().unwrap();
        let aid = kp_a.public_key().member_id();
        let bid = kp_b.public_key().member_id();
        let members = Arc::new(MemberStore::from_entries(vec![
            MemberEntry::new("alice-box", *kp_a.public_key(), vec![MemberRole::Client]),
            MemberEntry::new("bob-box", *kp_b.public_key(), vec![MemberRole::Client]),
        ]));
        members
            .set_member_participant(&aid, Some("team-a"))
            .unwrap();
        members
            .set_default_participant_quota(Some(ParticipantQuota {
                max_running_tasks: Some(4),
                max_queued_tasks: Some(10),
                ..Default::default()
            }))
            .unwrap();
        members
            .set_participant_quota(
                "team-a",
                Some(ParticipantQuota {
                    max_running_tasks: Some(2),
                    priority_band: Some(PriorityBand::Low),
                    ..Default::default()
                }),
            )
            .unwrap();

        let policy = QuotaPolicyConfig {
            default_quota: ParticipantQuota {
                max_slots: Some(8),
                ..Default::default()
            },
            participant_overrides: [(
                "team-a".to_string(),
                ParticipantQuota {
                    max_running_tasks: Some(9), // Overridden by members.json override `2`
                    fair_share_weight: Some(3),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
        };

        let ctrl = QuotaController::new(members, policy);
        assert_eq!(ctrl.participant_for_member(None), ORGANISER_PARTICIPANT);
        assert_eq!(ctrl.participant_for_member(Some(&aid)), "team-a");
        assert_eq!(
            ctrl.participant_for_member(Some(&bid)),
            format!("member:{bid}")
        );

        let eff = ctrl.effective_quota("team-a");
        assert_eq!(eff.max_running_tasks, Some(2));
        assert_eq!(eff.max_queued_tasks, Some(10));
        assert_eq!(eff.max_slots, Some(8));
        assert_eq!(eff.fair_share_weight, Some(3));
        assert_eq!(eff.priority_band, Some(PriorityBand::Low));

        // Priority band check: Low defaults to 0, rejects > 33.
        assert_eq!(ctrl.resolve_submission_priority("team-a", None).unwrap(), 0);
        assert_eq!(
            ctrl.resolve_submission_priority("team-a", Some(20))
                .unwrap(),
            20
        );
        let err = ctrl
            .resolve_submission_priority("team-a", Some(34))
            .unwrap_err();
        assert_eq!(err.error_code(), "QUOTA_EXCEEDED");

        // Observer snapshot redacts member_ids & machine names; Admin sees full details.
        let obs = ctrl.snapshot_dto(&BTreeMap::new(), false);
        let obs_json = serde_json::to_string(&obs).unwrap();
        assert!(!obs_json.contains(&bid));
        assert!(!obs_json.contains(&aid));
        assert!(!obs_json.contains("alice-box") && !obs_json.contains("bob-box"));
        assert!(obs.members.is_empty());

        let adm = ctrl.snapshot_dto(&BTreeMap::new(), true);
        assert_eq!(adm.members.len(), 2);
        let team_a_row = adm
            .participants
            .iter()
            .find(|r| r.participant == "team-a")
            .unwrap();
        assert_eq!(team_a_row.member_ids, vec![aid]);
    }
}
