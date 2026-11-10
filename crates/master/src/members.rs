//! Enrolled member store (`members.json`), invites, pending enrollments, worker-ID ownership, and
//! master authentication mode (`--auth`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rusty_grid_core::noise_auth::{
    default_oxideswarm_dir, derive_pairing_code, verify_enroll_proof, verify_pairing_code, KEY_LEN,
};
pub use rusty_grid_core::noise_auth::{
    InviteSummary, MemberEntry, MemberRole, PendingEnrollmentInfo, PublicKey, RedactedBytes32,
    RedactedString,
};
use rusty_grid_core::{GridError, GridResult};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use uuid::Uuid;

/// File name of the enrolled member list inside `--state-dir` or `~/.oxideswarm/`.
pub const MEMBERS_FILE_NAME: &str = "members.json";

/// Supported schema version for `members.json`.
pub const MEMBERS_FILE_VERSION: u32 = 1;

/// Maximum number of concurrent pending enrollment requests (design 0051 D4).
pub const MAX_PENDING_ENROLLMENTS: usize = 32;

/// Time-to-live (10 minutes) for a pending enrollment request (design 0051 D4).
pub const PENDING_ENROLLMENT_TTL_SECS: u64 = 600;

/// Maximum number of claimed worker IDs per enrolled member (design 0051 D5).
pub const MAX_WORKERS_PER_MEMBER: usize = 64;

/// Maximum length in Unicode scalar values of an enrolled member's name.
pub const MAX_MEMBER_NAME_CHARS: usize = 64;

/// `via_invite` tag used when the master auto-enrolls the host's own `node.key` as `admin`.
pub const LOCAL_HOST_INVITE_TAG: &str = "local-host";

/// Generic wire rejection message returned to any invalid/expired/revoked/unauthenticated `ENROLL`
/// request (design 0051 D4 item 7).
pub const GENERIC_ENROLL_REJECT_MESSAGE: &str =
    "enrollment rejected: invalid, expired, or revoked invite";

/// Master authentication mode (`--auth secret|members|both`, `RUSTY_GRID_AUTH`, `[master].auth`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// Admit only OXS-AUTH v2 (shared secret) peers.
    Secret,
    /// Admit only OXS-AUTH v3 (Noise IK enrolled member) peers; v2 peers are refused with
    /// [`rusty_grid_core::cluster_auth::REJECT_MEMBERS_ONLY`].
    Members,
    /// Admit both OXS-AUTH v2 (shared secret) and OXS-AUTH v3 (enrolled member) peers.
    #[default]
    Both,
}

impl AuthMode {
    /// Canonical lowercase CLI/config name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Secret => "secret",
            Self::Members => "members",
            Self::Both => "both",
        }
    }

    /// Returns `true` if OXS-AUTH v2 (shared secret) peers may be admitted.
    pub const fn allows_secret(self) -> bool {
        matches!(self, Self::Secret | Self::Both)
    }

    /// Returns `true` if OXS-AUTH v3 (enrolled member) peers may be admitted.
    pub const fn allows_members(self) -> bool {
        matches!(self, Self::Members | Self::Both)
    }
}

impl fmt::Display for AuthMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AuthMode {
    type Err = GridError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "secret" => Ok(Self::Secret),
            "members" => Ok(Self::Members),
            "both" => Ok(Self::Both),
            other => Err(GridError::Config(format!(
                "invalid auth mode '{other}' (expected 'secret', 'members', or 'both')"
            ))),
        }
    }
}

/// Persisted invite record in `members.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteRecord {
    pub invite_id: String,
    pub invite_secret: RedactedBytes32,
    pub roles: Vec<MemberRole>,
    pub uses_remaining: u32,
    pub max_uses: u32,
    pub expires_at: u64,
    pub require_approval: bool,
    #[serde(default)]
    pub revoked: bool,
}

impl InviteRecord {
    pub fn to_summary(&self) -> InviteSummary {
        InviteSummary {
            invite_id: self.invite_id.clone(),
            roles: self.roles.clone(),
            uses_remaining: self.uses_remaining,
            max_uses: self.max_uses,
            expires_at: self.expires_at,
            require_approval: self.require_approval,
            revoked: self.revoked,
        }
    }
}

/// On-disk JSON format for `members.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembersFile {
    /// Schema version (must be `1`).
    pub version: u32,
    /// Enrolled machines.
    #[serde(default)]
    pub members: Vec<MemberEntry>,
    /// Issued invites.
    #[serde(default)]
    pub invites: Vec<InviteRecord>,
    /// Worker IDs owned by each member (`worker_id -> member_id`, design 0051 D5).
    #[serde(default)]
    pub worker_owners: BTreeMap<Uuid, String>,
    /// Monotonic LRU sequence numbers for claimed worker IDs (`worker_id -> seq`), persisted so
    /// LRU eviction order survives master restarts.
    #[serde(default)]
    pub worker_last_seq: BTreeMap<Uuid, u64>,
    /// Worker IDs registered by cluster-secret (v2) workers (`worker_id -> seq`), persisted so
    /// v3 members cannot squat a v2 worker's ID across master restarts (design 0051 D5).
    #[serde(default)]
    pub v2_worker_seq: BTreeMap<Uuid, u64>,
    /// Public keys of revoked members that are permanently refused re-enrollment (design 0051 G4).
    #[serde(default)]
    pub revoked_keys: Vec<PublicKey>,
}

impl Default for MembersFile {
    fn default() -> Self {
        Self {
            version: MEMBERS_FILE_VERSION,
            members: Vec::new(),
            invites: Vec::new(),
            worker_owners: BTreeMap::new(),
            worker_last_seq: BTreeMap::new(),
            v2_worker_seq: BTreeMap::new(),
            revoked_keys: Vec::new(),
        }
    }
}

/// Snapshot needed to roll back an approved enrollment if the joining machine subsequently
/// declines the `[y/N]` pairing confirmation or disconnects before confirming.
#[derive(Debug, Clone)]
pub struct ApprovedEnrollmentRollback {
    pub approval_id: u64,
    pub public_key: PublicKey,
    pub member_id: String,
    pub expected_generation: u64,
    pub invite_id: String,
}

#[derive(Debug, Clone)]
struct UnconfirmedApprovalOp {
    approval_id: u64,
    name: String,
    roles: Vec<MemberRole>,
    invite_id: String,
    approved_at: u64,
}

#[derive(Debug, Clone)]
struct UnconfirmedMemberTracker {
    baseline: Option<MemberEntry>,
    ops: Vec<UnconfirmedApprovalOp>,
    expected_generation: u64,
}

fn apply_unconfirmed_op_to_entry(
    entry: &mut Option<MemberEntry>,
    public_key: PublicKey,
    op: &UnconfirmedApprovalOp,
) {
    let id = public_key.member_id();
    match entry {
        Some(existing) => {
            existing.id = id;
            if existing.name.trim().is_empty() && !op.name.trim().is_empty() {
                existing.name = op.name.clone();
            }
            for role in &op.roles {
                if !existing.roles.contains(role) {
                    existing.roles.push(*role);
                }
            }
            existing.last_seen = Some(op.approved_at);
            if existing.via_invite.is_none() {
                existing.via_invite = Some(op.invite_id.clone());
            }
        }
        None => {
            *entry = Some(MemberEntry {
                id,
                name: op.name.clone(),
                public_key,
                roles: op.roles.clone(),
                enrolled_at: Some(op.approved_at),
                last_seen: Some(op.approved_at),
                via_invite: Some(op.invite_id.clone()),
            });
        }
    }
}

/// Decision delivered to a waiting `ENROLL` connection when an admin approves or rejects it.
#[derive(Debug, Clone)]
pub enum PendingDecision {
    Approved {
        member_id: String,
        roles: Vec<MemberRole>,
        rollback: Box<ApprovedEnrollmentRollback>,
    },
    Rejected {
        message: String,
    },
}

/// Result of handling an incoming `ClientMessage::Enroll`.
pub enum EnrollOutcome {
    /// `--no-approval` invite: enrolled immediately and took one use.
    ImmediateApproved {
        member_id: String,
        roles: Vec<MemberRole>,
    },
    /// Queued for admin approval; the connection must stay open while waiting on `decision_rx`.
    Pending {
        pending_id: String,
        decision_rx: oneshot::Receiver<PendingDecision>,
    },
    /// Refused; `log_reason` is logged by the master and `wire_message` is sent in `EnrollRejected`.
    Rejected {
        log_reason: String,
        wire_message: String,
    },
}

struct PendingEnrollmentEntry {
    info: PendingEnrollmentInfo,
    pairing_code: rusty_grid_core::noise_auth::RedactedString,
    decision_tx: oneshot::Sender<PendingDecision>,
}

#[derive(Clone, Default)]
struct PersistentMemberState {
    by_pubkey: HashMap<PublicKey, MemberEntry>,
    invites: Vec<InviteRecord>,
    worker_owners: BTreeMap<Uuid, String>,
    worker_last_seq: BTreeMap<Uuid, u64>,
    v2_worker_seq: BTreeMap<Uuid, u64>,
    next_worker_seq: u64,
    revoked_keys: HashSet<PublicKey>,
}

impl PersistentMemberState {
    fn to_members_file(&self) -> MembersFile {
        let mut members: Vec<MemberEntry> = self.by_pubkey.values().cloned().collect();
        members.sort_by(|a, b| a.id.cmp(&b.id));
        let mut revoked_keys: Vec<PublicKey> = self.revoked_keys.iter().copied().collect();
        revoked_keys.sort_by_key(PublicKey::to_hex);
        MembersFile {
            version: MEMBERS_FILE_VERSION,
            members,
            invites: self.invites.clone(),
            worker_owners: self.worker_owners.clone(),
            worker_last_seq: self.worker_last_seq.clone(),
            v2_worker_seq: self.v2_worker_seq.clone(),
            revoked_keys,
        }
    }

    fn upsert_member(
        &mut self,
        public_key: PublicKey,
        name: String,
        roles: Vec<MemberRole>,
        via_invite: Option<String>,
        now: u64,
    ) -> MemberEntry {
        let id = public_key.member_id();
        match self.by_pubkey.get_mut(&public_key) {
            Some(existing) => {
                existing.id = id;
                if existing.name.trim().is_empty() && !name.trim().is_empty() {
                    existing.name = name;
                }
                for role in roles {
                    if !existing.roles.contains(&role) {
                        existing.roles.push(role);
                    }
                }
                existing.last_seen = Some(now);
                if existing.via_invite.is_none() && via_invite.is_some() {
                    existing.via_invite = via_invite;
                }
                existing.clone()
            }
            None => {
                let entry = MemberEntry {
                    id,
                    name,
                    public_key,
                    roles,
                    enrolled_at: Some(now),
                    last_seen: Some(now),
                    via_invite,
                };
                self.by_pubkey.insert(public_key, entry.clone());
                entry
            }
        }
    }

    fn prune_inactive_invites(&mut self, now: u64) {
        self.invites
            .retain(|inv| !inv.revoked && inv.uses_remaining > 0 && now < inv.expires_at);
    }
}

#[derive(Default)]
struct MemberStoreInner {
    state: PersistentMemberState,
    pending: HashMap<String, PendingEnrollmentEntry>,
    next_approval_id: u64,
    member_generations: HashMap<PublicKey, u64>,
    unconfirmed_approvals: HashMap<PublicKey, UnconfirmedMemberTracker>,
    active_approval_invites: HashMap<u64, String>,
    save_path: Option<PathBuf>,
}

/// Thread-safe enrolled member, invite, pending-enrollment, and worker-ownership store.
#[derive(Clone, Default)]
pub struct MemberStore {
    inner: Arc<RwLock<MemberStoreInner>>,
}

impl fmt::Debug for MemberStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("MemberStore")
            .field("members", &guard.state.by_pubkey.len())
            .field("invites", &guard.state.invites.len())
            .field("pending", &guard.pending.len())
            .finish()
    }
}

pub fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Strips control and Unicode format characters (`Cf`, BiDi overrides/isolates), trims
/// whitespace, and bounds the string to `max_chars` Unicode scalar values.
pub fn sanitize_display_field(raw: &str, max_chars: usize) -> String {
    raw.chars()
        .filter(|c| !rusty_grid_core::noise_auth::is_control_or_format_char(*c))
        .collect::<String>()
        .trim()
        .chars()
        .take(max_chars)
        .collect()
}

/// Validates a member name (`1..=MAX_MEMBER_NAME_CHARS` characters after trimming, no control or
/// Unicode format characters).
pub fn validate_member_name(raw: &str) -> GridResult<String> {
    if raw
        .chars()
        .any(rusty_grid_core::noise_auth::is_control_or_format_char)
    {
        return Err(GridError::Config(
            "member name cannot contain control or format characters".into(),
        ));
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(GridError::Config("member name cannot be empty".into()));
    }
    if trimmed.chars().count() > MAX_MEMBER_NAME_CHARS {
        return Err(GridError::Config(format!(
            "member name cannot exceed {MAX_MEMBER_NAME_CHARS} characters"
        )));
    }
    Ok(trimmed.to_string())
}

fn random_hex(num_bytes: usize) -> GridResult<String> {
    use ring::rand::{SecureRandom, SystemRandom};
    let mut buf = vec![0u8; num_bytes];
    SystemRandom::new()
        .fill(&mut buf)
        .map_err(|_| GridError::Config("OS CSPRNG failed".into()))?;
    let mut out = String::with_capacity(num_bytes * 2);
    for b in buf {
        out.push_str(&format!("{b:02x}"));
    }
    Ok(out)
}

fn random_bytes_32() -> GridResult<[u8; KEY_LEN]> {
    use ring::rand::{SecureRandom, SystemRandom};
    let mut buf = [0u8; KEY_LEN];
    SystemRandom::new()
        .fill(&mut buf)
        .map_err(|_| GridError::Config("OS CSPRNG failed".into()))?;
    Ok(buf)
}

#[cfg(unix)]
fn check_members_file_permissions(path: &Path) -> GridResult<()> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path).map_err(|e| {
        GridError::Config(format!(
            "failed to stat members file {}: {e}",
            path.display()
        ))
    })?;
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        let msg = format!(
            "refusing to load members file {}: permissions {:03o} allow group/others access (fix with: chmod 600 {})",
            path.display(),
            mode,
            path.display()
        );
        tracing::warn!("{msg}");
        return Err(GridError::Config(msg));
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let dir_meta = std::fs::metadata(parent).map_err(|e| {
            GridError::Config(format!(
                "failed to stat members directory {}: {e}",
                parent.display()
            ))
        })?;
        let dir_mode = dir_meta.permissions().mode() & 0o777;
        if dir_mode & 0o022 != 0 {
            let msg = format!(
                "refusing to load members file {}: parent directory {} has permissions {:03o} allowing group/others write access (fix with: chmod 700 {})",
                path.display(),
                parent.display(),
                dir_mode,
                parent.display()
            );
            tracing::warn!("{msg}");
            return Err(GridError::Config(msg));
        }
    }
    Ok(())
}

#[cfg(windows)]
fn check_members_file_permissions(path: &Path) -> GridResult<()> {
    use rusty_grid_core::private_file::{
        current_user_sid, open_acl_warning, open_grants, read_dacl,
    };
    let user_sid = current_user_sid().map_err(|e| {
        GridError::Config(format!(
            "failed to query current user SID while checking {}: {e}",
            path.display()
        ))
    })?;
    let user_name = std::env::var("USERNAME").unwrap_or_else(|_| "%USERNAME%".to_string());
    let (aces, _) = read_dacl(path).map_err(|e| {
        GridError::Config(format!(
            "failed to read DACL of members file {}: {e}",
            path.display()
        ))
    })?;
    let offenders = open_grants(&aces, &user_sid);
    if let Some(msg) = open_acl_warning(path, "members file", &offenders, &user_name) {
        tracing::warn!("{msg}");
        return Err(GridError::Config(msg));
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        if let Ok((dir_aces, _)) = read_dacl(parent) {
            let dir_offenders = open_grants(&dir_aces, &user_sid);
            if let Some(msg) =
                open_acl_warning(parent, "members directory", &dir_offenders, &user_name)
            {
                tracing::warn!("{msg}");
                return Err(GridError::Config(msg));
            }
        }
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn check_members_file_permissions(_path: &Path) -> GridResult<()> {
    Ok(())
}

fn write_members_file_atomic(path: &Path, file: &MembersFile) -> GridResult<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let existed = parent.exists();
        #[cfg(windows)]
        let created = rusty_grid_core::private_file::create_private_dir_all(parent);
        #[cfg(not(windows))]
        let created = std::fs::create_dir_all(parent);
        created.map_err(|e| {
            GridError::Config(format!(
                "cannot create members directory '{}': {e}",
                parent.display()
            ))
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if !existed {
                let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
            }
        }
        #[cfg(not(unix))]
        let _ = existed;
    }

    let json = serde_json::to_string_pretty(file)
        .map_err(|e| GridError::Config(format!("failed to serialize members.json: {e}")))?;

    #[cfg(windows)]
    {
        rusty_grid_core::private_file::write_private_replace(path, json.as_bytes()).map_err(|e| {
            GridError::Config(format!(
                "cannot write members file '{}': {e}",
                path.display()
            ))
        })
    }
    #[cfg(not(windows))]
    {
        use std::io::Write;
        let tmp = path.with_file_name(format!(".{MEMBERS_FILE_NAME}.{}.tmp", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let res = opts
            .open(&tmp)
            .and_then(|mut f| {
                f.write_all(json.as_bytes())?;
                f.sync_all()
            })
            .and_then(|()| std::fs::rename(&tmp, path));
        if let Err(e) = res {
            let _ = std::fs::remove_file(&tmp);
            return Err(GridError::Config(format!(
                "cannot write members file '{}': {e}",
                path.display()
            )));
        }
        Ok(())
    }
}

impl MemberStoreInner {
    fn mutate_and_persist<T, F>(&mut self, f: F) -> GridResult<T>
    where
        F: FnOnce(&mut PersistentMemberState) -> GridResult<T>,
    {
        let mut staged = self.state.clone();
        let out = f(&mut staged)?;
        if let Some(ref path) = self.save_path {
            write_members_file_atomic(path, &staged.to_members_file())?;
        }
        self.state = staged;
        Ok(out)
    }

    fn prune_pending(&mut self, now: u64) {
        self.pending
            .retain(|_, entry| now < entry.info.expires_at && !entry.decision_tx.is_closed());
    }
}

/// Resolves a target identifier by exact ID or unique hex ID prefix (`>= 4` hex characters).
/// Never matches by display name so an attacker cannot shadow an ID prefix with a crafted name.
fn resolve_id_or_prefix<'a, I>(target: &str, ids: I, kind: &str) -> GridResult<String>
where
    I: IntoIterator<Item = &'a str>,
{
    let q = target.trim();
    if q.is_empty() {
        return Err(GridError::Config(format!("{kind} target cannot be empty")));
    }
    let q_clean = q.replace('-', "").to_ascii_lowercase();
    let all: Vec<&'a str> = ids.into_iter().collect();

    // 1. Exact ID match.
    for id in &all {
        if id.eq_ignore_ascii_case(&q_clean) || id.eq_ignore_ascii_case(q) {
            return Ok((*id).to_string());
        }
    }
    // 2. Unique ID prefix of at least 4 hex characters.
    if q_clean.len() >= 4 && q_clean.chars().all(|c| c.is_ascii_hexdigit()) {
        let prefix_matches: Vec<&str> = all
            .iter()
            .filter(|id| id.to_ascii_lowercase().starts_with(&q_clean))
            .copied()
            .collect();
        if prefix_matches.len() == 1 {
            return Ok(prefix_matches[0].to_string());
        }
        if prefix_matches.len() > 1 {
            return Err(GridError::Config(format!(
                "ambiguous {kind} ID prefix '{q}'; matches {} entries",
                prefix_matches.len()
            )));
        }
    }
    Err(GridError::Config(format!("no {kind} matching ID '{q}'")))
}

/// Resolves a target identifier for `members rename` by exact ID, unique hex ID prefix (`>= 4`
/// hex chars), or finally unique exact `name`.
fn resolve_id_prefix_or_name<'a, I>(target: &str, items: I, kind: &str) -> GridResult<String>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let q = target.trim();
    if q.is_empty() {
        return Err(GridError::Config(format!("{kind} target cannot be empty")));
    }
    let all: Vec<(&'a str, &'a str)> = items.into_iter().collect();
    if let Ok(id) = resolve_id_or_prefix(q, all.iter().map(|(id, _)| *id), kind) {
        return Ok(id);
    }
    let name_matches: Vec<&str> = all
        .iter()
        .filter(|(_, name)| *name == q)
        .map(|(id, _)| *id)
        .collect();
    if name_matches.len() == 1 {
        return Ok(name_matches[0].to_string());
    }
    if name_matches.len() > 1 {
        return Err(GridError::Config(format!(
            "ambiguous {kind} name '{q}'; specify the ID instead"
        )));
    }
    Err(GridError::Config(format!("no {kind} matching '{q}'")))
}

impl MemberStore {
    /// Creates an empty in-memory member store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Configures the on-disk path where mutations will be atomically persisted.
    pub fn with_save_path(self, path: impl Into<PathBuf>) -> Self {
        {
            let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
            guard.save_path = Some(path.into());
        }
        self
    }

    /// Builds an in-memory store from an iterator of [`MemberEntry`] items.
    pub fn from_entries(entries: impl IntoIterator<Item = MemberEntry>) -> Self {
        let mut by_pubkey = HashMap::new();
        for mut entry in entries {
            entry.id = entry.public_key.member_id();
            entry.name = sanitize_display_field(&entry.name, MAX_MEMBER_NAME_CHARS);
            if entry.name.is_empty() {
                entry.name = entry.id.clone();
            }
            by_pubkey.insert(entry.public_key, entry);
        }
        Self {
            inner: Arc::new(RwLock::new(MemberStoreInner {
                state: PersistentMemberState {
                    by_pubkey,
                    ..Default::default()
                },
                ..Default::default()
            })),
        }
    }

    /// Validates and builds a store from `entries`, refusing mismatched member IDs or duplicate
    /// public keys.
    pub fn try_from_entries(
        entries: impl IntoIterator<Item = MemberEntry>,
        source_path: &Path,
    ) -> GridResult<Self> {
        let mut by_pubkey = HashMap::new();
        for mut entry in entries {
            let expected_id = entry.public_key.member_id();
            let raw_id = entry.id.trim();
            if !raw_id.is_empty() && !raw_id.eq_ignore_ascii_case(&expected_id) {
                return Err(GridError::Config(format!(
                    "invalid members file {}: member '{}' has id '{}' which does not match its public_key id '{expected_id}'",
                    source_path.display(),
                    sanitize_display_field(&entry.name, MAX_MEMBER_NAME_CHARS),
                    raw_id
                )));
            }
            entry.id = expected_id.clone();
            entry.name = sanitize_display_field(&entry.name, MAX_MEMBER_NAME_CHARS);
            if entry.name.is_empty() {
                entry.name = expected_id.clone();
            }
            if by_pubkey.contains_key(&entry.public_key) {
                return Err(GridError::Config(format!(
                    "invalid members file {}: duplicate public_key '{}' (member id '{expected_id}')",
                    source_path.display(),
                    entry.public_key.to_hex()
                )));
            }
            by_pubkey.insert(entry.public_key, entry);
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(MemberStoreInner {
                state: PersistentMemberState {
                    by_pubkey,
                    ..Default::default()
                },
                ..Default::default()
            })),
        })
    }

    /// Resolves the path to `members.json` inside `state_dir` or `~/.oxideswarm/members.json`.
    pub fn resolve_path(state_dir: Option<&Path>) -> Option<PathBuf> {
        let dir = match state_dir {
            Some(d) => d.to_path_buf(),
            None => default_oxideswarm_dir()?,
        };
        Some(dir.join(MEMBERS_FILE_NAME))
    }

    /// Loads `members.json` from `path`. If the file does not exist, returns an empty store
    /// configured to save future mutations to `path`.
    pub fn load_file(path: &Path) -> GridResult<Self> {
        let store = match std::fs::read_to_string(path) {
            Ok(raw) => {
                check_members_file_permissions(path)?;
                Self::from_json_str(&raw, path)?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::new(),
            Err(e) => {
                return Err(GridError::Config(format!(
                    "failed to read members file {}: {e}",
                    path.display()
                )))
            }
        };
        Ok(store.with_save_path(path.to_path_buf()))
    }

    /// Loads `members.json` from `state_dir` (or `~/.oxideswarm/members.json` when `None`).
    /// If `members.json` does not exist, returns an empty store.
    pub fn load_from_dir(state_dir: Option<&Path>) -> GridResult<Self> {
        let Some(path) = Self::resolve_path(state_dir) else {
            return Ok(Self::new());
        };
        Self::load_file(&path)
    }

    /// Parses a `members.json` payload.
    pub fn from_json_str(raw: &str, source_path: &Path) -> GridResult<Self> {
        let parsed: MembersFile = serde_json::from_str(raw).map_err(|e| {
            GridError::Config(format!(
                "invalid members file {}: {e}",
                source_path.display()
            ))
        })?;
        if parsed.version != MEMBERS_FILE_VERSION {
            return Err(GridError::Config(format!(
                "unsupported members file version {} in {} (expected {})",
                parsed.version,
                source_path.display(),
                MEMBERS_FILE_VERSION
            )));
        }
        let store = Self::try_from_entries(parsed.members, source_path)?;
        {
            let mut guard = store.inner.write().unwrap_or_else(|e| e.into_inner());
            guard.state.invites = parsed.invites;
            guard.state.prune_inactive_invites(now_unix_secs());
            let mut seq = parsed
                .worker_last_seq
                .values()
                .chain(parsed.v2_worker_seq.values())
                .copied()
                .max()
                .map_or(0, |m| m.wrapping_add(1));
            let mut worker_last_seq = BTreeMap::new();
            for wid in parsed.worker_owners.keys() {
                if let Some(&saved_seq) = parsed.worker_last_seq.get(wid) {
                    worker_last_seq.insert(*wid, saved_seq);
                } else {
                    worker_last_seq.insert(*wid, seq);
                    seq = seq.wrapping_add(1);
                }
            }
            guard.state.worker_last_seq = worker_last_seq;
            guard.state.v2_worker_seq = parsed.v2_worker_seq;
            guard.state.next_worker_seq = seq;
            guard.state.worker_owners = parsed.worker_owners;
            guard.state.revoked_keys = parsed.revoked_keys.into_iter().collect();
        }
        Ok(store)
    }

    /// Number of enrolled members in the store.
    pub fn len(&self) -> usize {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .state
            .by_pubkey
            .len()
    }

    /// Returns `true` if the store holds no members.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if this store has a disk persistence path configured.
    pub fn has_save_path(&self) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .save_path
            .is_some()
    }

    /// Returns `true` if there is any member other than the auto-enrolled local host admin.
    pub fn has_non_host_members(&self) -> bool {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard
            .state
            .by_pubkey
            .values()
            .any(|m| m.via_invite.as_deref() != Some(LOCAL_HOST_INVITE_TAG))
    }

    /// Returns `true` if `pubkey` belongs to an enrolled (and non-revoked) member.
    pub fn contains(&self, pubkey: &PublicKey) -> bool {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard.state.by_pubkey.contains_key(pubkey) && !guard.state.revoked_keys.contains(pubkey)
    }

    /// Looks up a member entry by public key.
    pub fn get(&self, pubkey: &PublicKey) -> Option<MemberEntry> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        if guard.state.revoked_keys.contains(pubkey) {
            return None;
        }
        guard.state.by_pubkey.get(pubkey).cloned()
    }

    /// Updates `last_seen` for `pubkey` in memory.
    pub fn touch_last_seen(&self, pubkey: &PublicKey) {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = guard.state.by_pubkey.get_mut(pubkey) {
            entry.last_seen = Some(now_unix_secs());
        }
    }

    /// Ensures the master host's own `node.key` is enrolled with the `admin` role (design 0051 D6).
    /// When `persist` is `false`, the save path is cleared so nothing is written to disk.
    pub fn ensure_local_host_admin(
        &self,
        host_pubkey: PublicKey,
        host_name: impl Into<String>,
        persist: bool,
    ) -> GridResult<()> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if !persist {
            guard.save_path = None;
        }
        if guard.state.revoked_keys.contains(&host_pubkey) {
            tracing::warn!(
                member_id = %host_pubkey.member_id(),
                "Skipping auto-enrollment of local host node.key because its public key is in revoked_keys"
            );
            return Ok(());
        }
        let now = now_unix_secs();
        let name = sanitize_display_field(&host_name.into(), MAX_MEMBER_NAME_CHARS);
        let name = if name.is_empty() {
            LOCAL_HOST_INVITE_TAG.to_string()
        } else {
            name
        };
        let needs_update = match guard.state.by_pubkey.get(&host_pubkey) {
            Some(entry) => !entry.roles.contains(&MemberRole::Admin),
            None => true,
        };
        if !needs_update {
            return Ok(());
        }
        guard.mutate_and_persist(|staged| {
            match staged.by_pubkey.get_mut(&host_pubkey) {
                Some(entry) => {
                    if !entry.roles.contains(&MemberRole::Admin) {
                        entry.roles.push(MemberRole::Admin);
                    }
                }
                None => {
                    let entry = MemberEntry {
                        id: host_pubkey.member_id(),
                        name,
                        public_key: host_pubkey,
                        roles: vec![MemberRole::Admin],
                        enrolled_at: Some(now),
                        last_seen: Some(now),
                        via_invite: Some(LOCAL_HOST_INVITE_TAG.to_string()),
                    };
                    staged.by_pubkey.insert(host_pubkey, entry);
                }
            }
            Ok(())
        })?;
        let next_gen = guard
            .member_generations
            .get(&host_pubkey)
            .copied()
            .unwrap_or(0)
            .wrapping_add(1);
        guard.member_generations.insert(host_pubkey, next_gen);
        guard.unconfirmed_approvals.remove(&host_pubkey);
        Ok(())
    }

    /// Creates and persists a new invite (design 0051 D3).
    pub fn create_invite(
        &self,
        roles: Vec<MemberRole>,
        max_uses: u32,
        expires_in_secs: u64,
        require_approval: bool,
    ) -> GridResult<InviteRecord> {
        if roles.is_empty() {
            return Err(GridError::Config(
                "invite must grant at least one role (worker, client, or admin)".into(),
            ));
        }
        if max_uses == 0 {
            return Err(GridError::Config("invite --uses must be at least 1".into()));
        }
        if expires_in_secs == 0 {
            return Err(GridError::Config(
                "invite --expires must be greater than 0 seconds".into(),
            ));
        }
        if roles.contains(&MemberRole::Admin) && (!require_approval || max_uses != 1) {
            return Err(GridError::Config(
                "an invite that grants the admin role must require approval and have --uses 1"
                    .into(),
            ));
        }
        let invite_id = random_hex(16)?;
        let invite_secret = RedactedBytes32(random_bytes_32()?);
        let now = now_unix_secs();
        let expires_at = now.saturating_add(expires_in_secs);
        let record = InviteRecord {
            invite_id,
            invite_secret,
            roles,
            uses_remaining: max_uses,
            max_uses,
            expires_at,
            require_approval,
            revoked: false,
        };
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        guard.mutate_and_persist(|staged| {
            staged.prune_inactive_invites(now);
            staged.invites.push(record.clone());
            Ok(())
        })?;
        Ok(record)
    }

    /// Lists all invites (design 0051 D3).
    pub fn list_invites(&self) -> Vec<InviteSummary> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard
            .state
            .invites
            .iter()
            .map(InviteRecord::to_summary)
            .collect()
    }

    /// Revokes an invite by ID or unique hex prefix (>= 4 chars) and rejects any pending
    /// enrollment requests waiting on that invite.
    pub fn revoke_invite(&self, target: &str) -> GridResult<String> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let id = resolve_id_or_prefix(
            target,
            guard.state.invites.iter().map(|inv| inv.invite_id.as_str()),
            "invite",
        )?;
        guard.mutate_and_persist(|staged| {
            let inv = staged
                .invites
                .iter_mut()
                .find(|i| i.invite_id == id)
                .ok_or_else(|| GridError::Config(format!("unknown invite '{target}'")))?;
            inv.revoked = true;
            Ok(())
        })?;
        let dropped_pending_ids: Vec<String> = guard
            .pending
            .iter()
            .filter(|(_, entry)| entry.info.invite_id == id)
            .map(|(pid, _)| pid.clone())
            .collect();
        for pid in dropped_pending_ids {
            if let Some(entry) = guard.pending.remove(&pid) {
                let _ = entry.decision_tx.send(PendingDecision::Rejected {
                    message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
                });
            }
        }
        Ok(id)
    }

    /// Processes an incoming `ClientMessage::Enroll` over a v3 `enroll` session (design 0051 D4).
    ///
    /// Note that a use is taken **at approval**, not when a pending request is registered.
    #[allow(clippy::too_many_arguments)]
    pub fn handle_enroll_request(
        &self,
        peer_public_key: PublicKey,
        handshake_hash: &[u8; KEY_LEN],
        master_fingerprint: &str,
        remote_addr: &str,
        invite_id: &str,
        proof: &RedactedBytes32,
        name: String,
        os: String,
        arch: String,
    ) -> GridResult<EnrollOutcome> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let now = now_unix_secs();
        guard.prune_pending(now);

        if guard.state.revoked_keys.contains(&peer_public_key) {
            return Ok(EnrollOutcome::Rejected {
                log_reason: format!(
                    "public key of candidate {} has been revoked",
                    peer_public_key.member_id()
                ),
                wire_message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
        }

        let Some(inv_idx) = guard
            .state
            .invites
            .iter()
            .position(|i| i.invite_id == invite_id)
        else {
            return Ok(EnrollOutcome::Rejected {
                log_reason: format!("unknown invite_id '{invite_id}'"),
                wire_message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
        };
        let invite = &guard.state.invites[inv_idx];
        if invite.revoked {
            return Ok(EnrollOutcome::Rejected {
                log_reason: format!("invite '{}' has been revoked", invite.invite_id),
                wire_message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
        }
        if now >= invite.expires_at {
            return Ok(EnrollOutcome::Rejected {
                log_reason: format!("invite '{}' has expired", invite.invite_id),
                wire_message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
        }
        if invite.uses_remaining == 0 {
            return Ok(EnrollOutcome::Rejected {
                log_reason: format!("invite '{}' has no remaining uses", invite.invite_id),
                wire_message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
        }
        if !verify_enroll_proof(&invite.invite_secret.0, handshake_hash, proof) {
            return Ok(EnrollOutcome::Rejected {
                log_reason: format!(
                    "invalid enrollment proof for invite '{}' from candidate {}",
                    invite.invite_id,
                    peer_public_key.member_id()
                ),
                wire_message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
        }

        let clean_name = if name.trim().is_empty() {
            peer_public_key.member_id()
        } else {
            match validate_member_name(&name) {
                Ok(valid) => valid,
                Err(e) => {
                    return Ok(EnrollOutcome::Rejected {
                        log_reason: format!(
                            "invalid enrollment name from candidate {}: {e}",
                            peer_public_key.member_id()
                        ),
                        wire_message: format!("enrollment rejected: {e}"),
                    });
                }
            }
        };
        let clean_os = sanitize_display_field(&os, 32);
        let clean_arch = sanitize_display_field(&arch, 32);

        if !invite.require_approval {
            let roles = invite.roles.clone();
            let inv_id = invite.invite_id.clone();
            let entry = guard.mutate_and_persist(|staged| {
                staged.invites[inv_idx].uses_remaining -= 1;
                Ok(staged.upsert_member(
                    peer_public_key,
                    clean_name,
                    roles.clone(),
                    Some(inv_id),
                    now,
                ))
            })?;
            let next_gen = guard
                .member_generations
                .get(&peer_public_key)
                .copied()
                .unwrap_or(0)
                .wrapping_add(1);
            guard.member_generations.insert(peer_public_key, next_gen);
            guard.unconfirmed_approvals.remove(&peer_public_key);
            return Ok(EnrollOutcome::ImmediateApproved {
                member_id: entry.id,
                roles: entry.roles,
            });
        }

        if guard
            .pending
            .values()
            .any(|e| e.info.public_key == peer_public_key)
        {
            return Ok(EnrollOutcome::Rejected {
                log_reason: format!(
                    "candidate {} already has a live pending enrollment request",
                    peer_public_key.member_id()
                ),
                wire_message: "an enrollment request for this machine is already pending approval"
                    .to_string(),
            });
        }
        if guard.pending.len() >= MAX_PENDING_ENROLLMENTS {
            return Ok(EnrollOutcome::Rejected {
                log_reason: format!("pending enrollment limit ({MAX_PENDING_ENROLLMENTS}) reached"),
                wire_message: "too many pending enrollment requests; try again later".to_string(),
            });
        }

        let member_id = peer_public_key.member_id();
        let pending_id = random_hex(8)?;
        let pairing_code = derive_pairing_code(handshake_hash);
        let info = PendingEnrollmentInfo {
            pending_id: pending_id.clone(),
            member_id,
            public_key: peer_public_key,
            name: clean_name,
            os: clean_os,
            arch: clean_arch,
            roles: invite.roles.clone(),
            invite_id: invite.invite_id.clone(),
            master_fingerprint: sanitize_display_field(master_fingerprint, 64),
            remote_addr: sanitize_display_field(remote_addr, 128),
            requested_at: now,
            expires_at: now.saturating_add(PENDING_ENROLLMENT_TTL_SECS),
        };
        let (decision_tx, decision_rx) = oneshot::channel();
        guard.pending.insert(
            pending_id.clone(),
            PendingEnrollmentEntry {
                info,
                pairing_code,
                decision_tx,
            },
        );
        Ok(EnrollOutcome::Pending {
            pending_id,
            decision_rx,
        })
    }

    /// Removes a pending enrollment request (called when its live connection closes or times out).
    pub fn remove_pending(&self, pending_id: &str) {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        guard.pending.remove(pending_id);
    }

    /// Lists active pending enrollment requests.
    pub fn list_pending(&self) -> Vec<PendingEnrollmentInfo> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let now = now_unix_secs();
        guard.prune_pending(now);
        let mut list: Vec<PendingEnrollmentInfo> =
            guard.pending.values().map(|e| e.info.clone()).collect();
        list.sort_by(|a, b| {
            a.requested_at
                .cmp(&b.requested_at)
                .then(a.pending_id.cmp(&b.pending_id))
        });
        list
    }

    /// Approves a pending enrollment request by exact ID or unique hex ID prefix (`>= 4` chars)
    /// after verifying the 6-digit pairing check in constant time (design 0051 D4/D5).
    /// Consumes one use from the invite at approval time and writes `members.json` before
    /// mutating in-memory state.
    pub fn approve_pending(&self, target: &str, pairing_code: &str) -> GridResult<MemberEntry> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let now = now_unix_secs();
        guard.prune_pending(now);

        let id = resolve_id_or_prefix(
            target,
            guard.pending.values().map(|e| e.info.pending_id.as_str()),
            "pending enrollment",
        )?;
        let expected_code_matches = guard
            .pending
            .get(&id)
            .is_some_and(|e| verify_pairing_code(e.pairing_code.as_str(), pairing_code));
        if !expected_code_matches {
            return Err(GridError::Config(format!(
                "pairing code mismatch for pending enrollment '{id}'; verify the 6-digit code and master key fingerprint shown on the joining machine"
            )));
        }

        let pending_info = guard
            .pending
            .get(&id)
            .map(|e| e.info.clone())
            .ok_or_else(|| GridError::Config(format!("no pending enrollment '{target}'")))?;

        if guard.state.revoked_keys.contains(&pending_info.public_key) {
            if let Some(pending) = guard.pending.remove(&id) {
                let _ = pending.decision_tx.send(PendingDecision::Rejected {
                    message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
                });
            }
            return Err(GridError::Config(
                "cannot approve: this machine's public key has been revoked".into(),
            ));
        }

        let inv_idx = guard
            .state
            .invites
            .iter()
            .position(|i| i.invite_id == pending_info.invite_id);
        let Some(inv_idx) = inv_idx else {
            if let Some(pending) = guard.pending.remove(&id) {
                let _ = pending.decision_tx.send(PendingDecision::Rejected {
                    message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
                });
            }
            return Err(GridError::Config(
                "cannot approve: the invite for this request no longer exists".into(),
            ));
        };
        if guard.state.invites[inv_idx].revoked {
            if let Some(pending) = guard.pending.remove(&id) {
                let _ = pending.decision_tx.send(PendingDecision::Rejected {
                    message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
                });
            }
            return Err(GridError::Config(
                "cannot approve: the invite for this request has been revoked".into(),
            ));
        };
        if now >= guard.state.invites[inv_idx].expires_at {
            if let Some(pending) = guard.pending.remove(&id) {
                let _ = pending.decision_tx.send(PendingDecision::Rejected {
                    message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
                });
            }
            return Err(GridError::Config(
                "cannot approve: the invite for this request has expired".into(),
            ));
        }
        if guard.state.invites[inv_idx].uses_remaining == 0 {
            if let Some(pending) = guard.pending.remove(&id) {
                let _ = pending.decision_tx.send(PendingDecision::Rejected {
                    message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
                });
            }
            return Err(GridError::Config(
                "cannot approve: the invite for this request has no remaining uses".into(),
            ));
        }

        let gen_before = guard
            .member_generations
            .get(&pending_info.public_key)
            .copied()
            .unwrap_or(0);
        let mut tracker = match guard.unconfirmed_approvals.get(&pending_info.public_key) {
            Some(existing) if existing.expected_generation == gen_before => existing.clone(),
            _ => UnconfirmedMemberTracker {
                baseline: guard.state.by_pubkey.get(&pending_info.public_key).cloned(),
                ops: Vec::new(),
                expected_generation: gen_before,
            },
        };
        let approval_id = guard.next_approval_id.wrapping_add(1);
        guard.next_approval_id = approval_id;
        tracker.ops.push(UnconfirmedApprovalOp {
            approval_id,
            name: pending_info.name.clone(),
            roles: pending_info.roles.clone(),
            invite_id: pending_info.invite_id.clone(),
            approved_at: now,
        });

        let entry = guard.mutate_and_persist(|staged| {
            staged.invites[inv_idx].uses_remaining -= 1;
            let mut rebuilt = tracker.baseline.clone();
            for op in &tracker.ops {
                apply_unconfirmed_op_to_entry(&mut rebuilt, pending_info.public_key, op);
            }
            let entry = rebuilt.expect("tracker has at least one approval op");
            staged.by_pubkey.insert(pending_info.public_key, entry.clone());
            Ok(entry)
        })?;
        let new_gen = gen_before.wrapping_add(1);
        guard
            .member_generations
            .insert(pending_info.public_key, new_gen);
        tracker.expected_generation = new_gen;
        guard
            .unconfirmed_approvals
            .insert(pending_info.public_key, tracker);
        guard
            .active_approval_invites
            .insert(approval_id, pending_info.invite_id.clone());

        let rollback = ApprovedEnrollmentRollback {
            approval_id,
            public_key: pending_info.public_key,
            member_id: entry.id.clone(),
            expected_generation: new_gen,
            invite_id: pending_info.invite_id,
        };
        if let Some(pending) = guard.pending.remove(&id) {
            let _ = pending.decision_tx.send(PendingDecision::Approved {
                member_id: entry.id.clone(),
                roles: entry.roles.clone(),
                rollback: Box::new(rollback),
            });
        }
        Ok(entry)
    }

    /// Marks an approved enrollment as confirmed by the joining machine (`ClientMessage::Hello`)
    /// so its member state and invite consumption become permanent.
    pub fn confirm_approved_enrollment(&self, rollback: &ApprovedEnrollmentRollback) {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        guard.active_approval_invites.remove(&rollback.approval_id);
        let current_gen = guard
            .member_generations
            .get(&rollback.public_key)
            .copied()
            .unwrap_or(0);
        let mut remove_tracker = false;
        if let Some(tracker) = guard.unconfirmed_approvals.get_mut(&rollback.public_key) {
            if tracker.expected_generation == current_gen {
                if let Some(pos) = tracker
                    .ops
                    .iter()
                    .position(|op| op.approval_id == rollback.approval_id)
                {
                    let op = tracker.ops.remove(pos);
                    apply_unconfirmed_op_to_entry(&mut tracker.baseline, rollback.public_key, &op);
                }
                if tracker.ops.is_empty() {
                    remove_tracker = true;
                }
            } else {
                remove_tracker = true;
            }
        }
        if remove_tracker {
            guard.unconfirmed_approvals.remove(&rollback.public_key);
        }
    }

    /// Rolls back an approved enrollment when the joining machine subsequently declines the
    /// `[y/N]` confirmation prompt or disconnects before confirming.
    ///
    /// Uses compare-and-swap against the member's generation and unconfirmed-approval tracker:
    /// - Never re-inserts a key that is in `revoked_keys`.
    /// - If the member was modified externally (for example revoked or re-enrolled) since the
    ///   approval, leaves the member entry untouched and only restores the invite use if this
    ///   approval consumed it.
    /// - Returns the list of `worker_id` claims released if the rollback removed the member or its
    ///   `worker` role.
    pub fn rollback_approved_enrollment(
        &self,
        rollback: ApprovedEnrollmentRollback,
    ) -> GridResult<Vec<Uuid>> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let invite_to_restore = guard.active_approval_invites.remove(&rollback.approval_id);
        let current_gen = guard
            .member_generations
            .get(&rollback.public_key)
            .copied()
            .unwrap_or(0);
        let is_revoked = guard.state.revoked_keys.contains(&rollback.public_key);

        let updated_tracker = if !is_revoked {
            guard
                .unconfirmed_approvals
                .get(&rollback.public_key)
                .filter(|t| {
                    t.expected_generation == current_gen
                        && t.ops.iter().any(|op| op.approval_id == rollback.approval_id)
                })
                .cloned()
        } else {
            None
        };

        if let Some(mut tracker) = updated_tracker {
            tracker
                .ops
                .retain(|op| op.approval_id != rollback.approval_id);
            let mut rebuilt = tracker.baseline.clone();
            for op in &tracker.ops {
                apply_unconfirmed_op_to_entry(&mut rebuilt, rollback.public_key, op);
            }
            let released_workers = guard.mutate_and_persist(|staged| {
                let mut released = Vec::new();
                if !staged.revoked_keys.contains(&rollback.public_key) {
                    match rebuilt {
                        Some(prev) => {
                            let has_worker = prev.has_worker_role();
                            staged.by_pubkey.insert(rollback.public_key, prev);
                            if !has_worker {
                                staged.worker_owners.retain(|wid, owner| {
                                    if *owner == rollback.member_id {
                                        released.push(*wid);
                                        false
                                    } else {
                                        true
                                    }
                                });
                                for wid in &released {
                                    staged.worker_last_seq.remove(wid);
                                }
                            }
                        }
                        None => {
                            staged.by_pubkey.remove(&rollback.public_key);
                            staged.worker_owners.retain(|wid, owner| {
                                if *owner == rollback.member_id {
                                    released.push(*wid);
                                    false
                                } else {
                                    true
                                }
                            });
                            for wid in &released {
                                staged.worker_last_seq.remove(wid);
                            }
                        }
                    }
                }
                if let Some(ref inv_id) = invite_to_restore {
                    if let Some(inv) = staged.invites.iter_mut().find(|i| i.invite_id == *inv_id) {
                        inv.uses_remaining = inv.uses_remaining.saturating_add(1).min(inv.max_uses);
                    }
                }
                Ok(released)
            })?;
            let new_gen = current_gen.wrapping_add(1);
            guard
                .member_generations
                .insert(rollback.public_key, new_gen);
            if tracker.ops.is_empty() {
                guard.unconfirmed_approvals.remove(&rollback.public_key);
            } else {
                tracker.expected_generation = new_gen;
                guard
                    .unconfirmed_approvals
                    .insert(rollback.public_key, tracker);
            }
            return Ok(released_workers);
        }

        guard.unconfirmed_approvals.remove(&rollback.public_key);
        if let Some(inv_id) = invite_to_restore {
            guard.mutate_and_persist(|staged| {
                if let Some(inv) = staged.invites.iter_mut().find(|i| i.invite_id == inv_id) {
                    inv.uses_remaining = inv.uses_remaining.saturating_add(1).min(inv.max_uses);
                }
                Ok(())
            })?;
        }
        Ok(Vec::new())
    }

    /// Rejects a pending enrollment request by exact ID or unique hex ID prefix (`>= 4` chars)
    /// without consuming an invite use.
    pub fn reject_pending(&self, target: &str) -> GridResult<String> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let now = now_unix_secs();
        guard.prune_pending(now);

        let id = resolve_id_or_prefix(
            target,
            guard.pending.values().map(|e| e.info.pending_id.as_str()),
            "pending enrollment",
        )?;
        let pending = guard
            .pending
            .remove(&id)
            .ok_or_else(|| GridError::Config(format!("no pending enrollment '{target}'")))?;
        let _ = pending.decision_tx.send(PendingDecision::Rejected {
            message: "enrollment request was rejected by the cluster administrator".to_string(),
        });
        Ok(id)
    }

    /// Lists all enrolled members sorted by member ID.
    pub fn list_members(&self) -> Vec<MemberEntry> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let mut list: Vec<MemberEntry> = guard.state.by_pubkey.values().cloned().collect();
        list.sort_by(|a, b| a.id.cmp(&b.id));
        list
    }

    /// Renames an enrolled member by ID, unique hex prefix, or current name.
    pub fn rename_member(&self, target: &str, new_name: &str) -> GridResult<MemberEntry> {
        let clean = validate_member_name(new_name)?;
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let id = resolve_id_prefix_or_name(
            target,
            guard
                .state
                .by_pubkey
                .values()
                .map(|m| (m.id.as_str(), m.name.as_str())),
            "member",
        )?;
        let pubkey = *guard
            .state
            .by_pubkey
            .iter()
            .find(|(_, m)| m.id == id)
            .map(|(k, _)| k)
            .ok_or_else(|| GridError::Config(format!("no member '{target}'")))?;
        let updated = guard.mutate_and_persist(|staged| {
            let entry = staged
                .by_pubkey
                .get_mut(&pubkey)
                .ok_or_else(|| GridError::Config(format!("no member '{target}'")))?;
            entry.name = clean;
            Ok(entry.clone())
        })?;
        let next_gen = guard
            .member_generations
            .get(&pubkey)
            .copied()
            .unwrap_or(0)
            .wrapping_add(1);
        guard.member_generations.insert(pubkey, next_gen);
        guard.unconfirmed_approvals.remove(&pubkey);
        Ok(updated)
    }

    /// Revokes an enrolled member by exact ID or unique hex ID prefix (`>= 4` chars), records its
    /// public key in `revoked_keys`, and releases its owned worker IDs.
    /// Returns `(revoked_entry, owned_worker_ids)`.
    pub fn revoke_member(&self, target: &str) -> GridResult<(MemberEntry, Vec<Uuid>)> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let id = resolve_id_or_prefix(
            target,
            guard.state.by_pubkey.values().map(|m| m.id.as_str()),
            "member",
        )?;
        let pubkey = *guard
            .state
            .by_pubkey
            .iter()
            .find(|(_, m)| m.id == id)
            .map(|(k, _)| k)
            .ok_or_else(|| GridError::Config(format!("no member '{target}'")))?;
        let (removed, owned_workers) = guard.mutate_and_persist(|staged| {
            let removed = staged
                .by_pubkey
                .remove(&pubkey)
                .ok_or_else(|| GridError::Config(format!("no member '{target}'")))?;
            staged.revoked_keys.insert(pubkey);
            let mut owned_workers = Vec::new();
            staged.worker_owners.retain(|wid, owner| {
                if *owner == removed.id {
                    owned_workers.push(*wid);
                    false
                } else {
                    true
                }
            });
            for wid in &owned_workers {
                staged.worker_last_seq.remove(wid);
            }
            Ok((removed, owned_workers))
        })?;
        let next_gen = guard
            .member_generations
            .get(&pubkey)
            .copied()
            .unwrap_or(0)
            .wrapping_add(1);
        guard.member_generations.insert(pubkey, next_gen);
        guard.unconfirmed_approvals.remove(&pubkey);
        let dropped_pending_ids: Vec<String> = guard
            .pending
            .iter()
            .filter(|(_, entry)| entry.info.public_key == pubkey)
            .map(|(pid, _)| pid.clone())
            .collect();
        for pid in dropped_pending_ids {
            if let Some(entry) = guard.pending.remove(&pid) {
                let _ = entry.decision_tx.send(PendingDecision::Rejected {
                    message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
                });
            }
        }
        Ok((removed, owned_workers))
    }

    /// Read-only check verifying that `worker_id` is not already owned by another member or by a
    /// cluster-secret (v2) worker.
    pub fn check_worker_id_owner(
        &self,
        worker_id: Uuid,
        member_id: Option<&str>,
    ) -> GridResult<()> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        if let Some(mid) = member_id {
            if guard.state.v2_worker_seq.contains_key(&worker_id) {
                return Err(GridError::AuthFailed(format!(
                    "worker_id {worker_id} is owned by another peer (v2 peer); member {mid} cannot register it"
                )));
            }
        }
        if let Some(existing_owner) = guard.state.worker_owners.get(&worker_id) {
            if member_id != Some(existing_owner.as_str()) {
                return Err(GridError::AuthFailed(format!(
                    "worker_id {worker_id} is owned by another member ({existing_owner}); {} cannot register it",
                    member_id.unwrap_or("unauthenticated/v2 peer")
                )));
            }
        }
        Ok(())
    }

    /// Read-only check verifying that `worker_id` is not owned by another member and that
    /// `member_id` (if a v3 member) has not exceeded [`MAX_WORKERS_PER_MEMBER`].
    pub fn check_worker_id_claim(
        &self,
        worker_id: Uuid,
        member_id: Option<&str>,
    ) -> GridResult<()> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        if let Some(mid) = member_id {
            if guard.state.v2_worker_seq.contains_key(&worker_id) {
                return Err(GridError::AuthFailed(format!(
                    "worker_id {worker_id} is owned by another peer (v2 peer); member {mid} cannot register it"
                )));
            }
        }
        if let Some(existing_owner) = guard.state.worker_owners.get(&worker_id) {
            if member_id != Some(existing_owner.as_str()) {
                return Err(GridError::AuthFailed(format!(
                    "worker_id {worker_id} is owned by another member ({existing_owner}); {} cannot register it",
                    member_id.unwrap_or("unauthenticated/v2 peer")
                )));
            }
            return Ok(());
        }
        if let Some(mid) = member_id {
            let owned_count = guard
                .state
                .worker_owners
                .values()
                .filter(|owner| owner.as_str() == mid)
                .count();
            if owned_count >= MAX_WORKERS_PER_MEMBER {
                return Err(GridError::AuthFailed(format!(
                    "member {mid} has reached the maximum of {MAX_WORKERS_PER_MEMBER} claimed worker IDs"
                )));
            }
        }
        Ok(())
    }

    /// Records or verifies ownership of `worker_id` by a non-v3 peer (`is_v2_secret = true` when
    /// authenticated with the cluster secret, `false` when unauthenticated on loopback).
    ///
    /// A cluster-secret (v2) worker reclaims `worker_id` if it was held by a v3 member claim that
    /// is not currently live (`!is_live(&worker_id)`), and records `worker_id` in `v2_worker_seq`
    /// so v3 members cannot squat it across master restarts (design 0051 D5).
    pub fn claim_v2_worker_id<F>(
        &self,
        worker_id: Uuid,
        is_v2_secret: bool,
        is_live: F,
    ) -> GridResult<()>
    where
        F: Fn(&Uuid) -> bool,
    {
        const MAX_PERSISTED_V2_WORKERS: usize = 1024;
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(existing_owner) = guard.state.worker_owners.get(&worker_id).cloned() {
            if !is_v2_secret || is_live(&worker_id) {
                return Err(GridError::AuthFailed(format!(
                    "worker_id {worker_id} is owned by another member ({existing_owner}); {} cannot register it",
                    if is_v2_secret { "v2 peer" } else { "unauthenticated peer" }
                )));
            }
        }
        if !is_v2_secret {
            return Ok(());
        }
        let mut v2_to_evict = Vec::new();
        if !guard.state.v2_worker_seq.contains_key(&worker_id)
            && guard.state.v2_worker_seq.len() >= MAX_PERSISTED_V2_WORKERS
        {
            let needed = (guard.state.v2_worker_seq.len() + 1)
                .saturating_sub(MAX_PERSISTED_V2_WORKERS);
            let mut evictable: Vec<(u64, Uuid)> = guard
                .state
                .v2_worker_seq
                .iter()
                .filter(|(wid, _)| !is_live(wid))
                .map(|(wid, seq)| (*seq, *wid))
                .collect();
            evictable.sort_by_key(|(seq, wid)| (*seq, *wid));
            v2_to_evict.extend(evictable.into_iter().take(needed).map(|(_, wid)| wid));
        }
        guard.mutate_and_persist(|staged| {
            staged.worker_owners.remove(&worker_id);
            staged.worker_last_seq.remove(&worker_id);
            for wid in &v2_to_evict {
                staged.v2_worker_seq.remove(wid);
            }
            let seq = staged.next_worker_seq;
            staged.next_worker_seq = seq.wrapping_add(1);
            staged.v2_worker_seq.insert(worker_id, seq);
            Ok(())
        })
    }

    /// Records or verifies ownership of `worker_id` by `member_id` (design 0051 D5), treating all
    /// existing claims as live.
    pub fn claim_worker_id(&self, worker_id: Uuid, member_id: &str) -> GridResult<()> {
        self.claim_worker_id_evicting(worker_id, member_id, |_| true)
            .map(|_| ())
    }

    /// Records or verifies ownership of `worker_id` by `member_id` (design 0051 D5).
    ///
    /// When `member_id` already holds [`MAX_WORKERS_PER_MEMBER`] claims, any non-live claims
    /// (`!is_live(wid)`) are evicted in least-recently-used order so restarting an unnamed worker
    /// on a joined machine never permanently locks the member out once 64 past worker IDs have
    /// accumulated. If all 64 claimed worker IDs currently have live sessions, the claim is
    /// refused. Returns the list of evicted non-live worker IDs.
    pub fn claim_worker_id_evicting<F>(
        &self,
        worker_id: Uuid,
        member_id: &str,
        is_live: F,
    ) -> GridResult<Vec<Uuid>>
    where
        F: Fn(&Uuid) -> bool,
    {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if guard.state.v2_worker_seq.contains_key(&worker_id) {
            return Err(GridError::AuthFailed(format!(
                "worker_id {worker_id} is owned by another peer (v2 peer); member {member_id} cannot register it"
            )));
        }
        if let Some(existing_owner) = guard.state.worker_owners.get(&worker_id) {
            if existing_owner != member_id {
                return Err(GridError::AuthFailed(format!(
                    "worker_id {worker_id} is owned by another member ({existing_owner}); member {member_id} cannot register it"
                )));
            }
            guard.mutate_and_persist(|staged| {
                let seq = staged.next_worker_seq;
                staged.next_worker_seq = seq.wrapping_add(1);
                staged.worker_last_seq.insert(worker_id, seq);
                Ok(())
            })?;
            return Ok(Vec::new());
        }
        let owned: Vec<Uuid> = guard
            .state
            .worker_owners
            .iter()
            .filter(|(_, owner)| owner.as_str() == member_id)
            .map(|(wid, _)| *wid)
            .collect();
        let mut to_evict = Vec::new();
        if owned.len() >= MAX_WORKERS_PER_MEMBER {
            let needed = (owned.len() + 1).saturating_sub(MAX_WORKERS_PER_MEMBER);
            let mut evictable: Vec<(u64, Uuid)> = owned
                .into_iter()
                .filter(|wid| !is_live(wid))
                .map(|wid| {
                    (
                        guard.state.worker_last_seq.get(&wid).copied().unwrap_or(0),
                        wid,
                    )
                })
                .collect();
            if evictable.len() < needed {
                return Err(GridError::AuthFailed(format!(
                    "member {member_id} has reached the maximum of {MAX_WORKERS_PER_MEMBER} claimed worker IDs"
                )));
            }
            evictable.sort_by_key(|(seq, wid)| (*seq, *wid));
            to_evict.extend(evictable.into_iter().take(needed).map(|(_, wid)| wid));
        }
        let evicted = to_evict.clone();
        guard.mutate_and_persist(|staged| {
            for wid in &to_evict {
                staged.worker_owners.remove(wid);
                staged.worker_last_seq.remove(wid);
            }
            let seq = staged.next_worker_seq;
            staged.next_worker_seq = seq.wrapping_add(1);
            staged.worker_last_seq.insert(worker_id, seq);
            staged
                .worker_owners
                .insert(worker_id, member_id.to_string());
            Ok(())
        })?;
        Ok(evicted)
    }

    /// Returns the worker IDs currently owned by `member_id`.
    pub fn workers_owned_by(&self, member_id: &str) -> Vec<Uuid> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard
            .state
            .worker_owners
            .iter()
            .filter(|(_, owner)| owner.as_str() == member_id)
            .map(|(wid, _)| *wid)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::noise_auth::{compute_enroll_proof, NodeKeypair};

    #[test]
    fn auth_mode_parse_and_display_round_trip() {
        assert_eq!("secret".parse::<AuthMode>().unwrap(), AuthMode::Secret);
        assert_eq!("members".parse::<AuthMode>().unwrap(), AuthMode::Members);
        assert_eq!("both".parse::<AuthMode>().unwrap(), AuthMode::Both);
        assert_eq!("BOTH".parse::<AuthMode>().unwrap(), AuthMode::Both);
        assert!("unknown".parse::<AuthMode>().is_err());
        assert_eq!(AuthMode::default(), AuthMode::Both);
    }

    #[test]
    fn parse_members_json_and_check_roles() {
        let kp_worker = NodeKeypair::generate().unwrap();
        let kp_admin = NodeKeypair::generate().unwrap();
        let json = format!(
            r#"{{
                "version": 1,
                "members": [
                    {{
                        "id": "{}",
                        "name": "worker-a",
                        "public_key": "{}",
                        "roles": ["worker", "client"]
                    }},
                    {{
                        "id": "{}",
                        "name": "laptop-admin",
                        "public_key": "{}",
                        "roles": ["admin"]
                    }}
                ]
            }}"#,
            kp_worker.member_id(),
            kp_worker.public_key().to_hex(),
            kp_admin.member_id(),
            kp_admin.public_key().to_hex(),
        );
        let store = MemberStore::from_json_str(&json, Path::new("members.json")).unwrap();
        assert_eq!(store.len(), 2);

        let w = store.get(kp_worker.public_key()).unwrap();
        assert!(w.has_worker_role());
        assert!(w.has_client_role());

        let a = store.get(kp_admin.public_key()).unwrap();
        assert!(!a.has_worker_role());
        assert!(a.has_client_role());
        assert!(a.has_admin_role());
    }

    #[test]
    fn reject_unsupported_members_version() {
        let json = r#"{"version": 2, "members": []}"#;
        let err = MemberStore::from_json_str(json, Path::new("members.json")).unwrap_err();
        assert!(err.to_string().contains("version 2"));
    }

    #[test]
    fn missing_members_file_returns_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemberStore::load_from_dir(Some(dir.path())).unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn omitted_roles_defaults_to_worker_only() {
        let kp = NodeKeypair::generate().unwrap();
        let json = format!(
            r#"{{
                "version": 1,
                "members": [
                    {{
                        "name": "worker-default",
                        "public_key": "{}"
                    }}
                ]
            }}"#,
            kp.public_key().to_hex(),
        );
        let store = MemberStore::from_json_str(&json, Path::new("members.json")).unwrap();
        let entry = store.get(kp.public_key()).unwrap();
        assert_eq!(entry.roles, vec![MemberRole::Worker]);
        assert!(entry.has_worker_role());
        assert!(!entry.has_client_role());
        assert_eq!(entry.id, kp.member_id());
    }

    #[test]
    fn reject_entry_with_mismatched_id() {
        let kp = NodeKeypair::generate().unwrap();
        let json = format!(
            r#"{{
                "version": 1,
                "members": [
                    {{
                        "id": "0000000000000000",
                        "name": "bad-id",
                        "public_key": "{}"
                    }}
                ]
            }}"#,
            kp.public_key().to_hex(),
        );
        let err = MemberStore::from_json_str(&json, Path::new("members.json"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("does not match its public_key id"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn reject_duplicate_public_keys() {
        let kp = NodeKeypair::generate().unwrap();
        let json = format!(
            r#"{{
                "version": 1,
                "members": [
                    {{
                        "id": "{id}",
                        "name": "first",
                        "public_key": "{pk}",
                        "roles": ["worker"]
                    }},
                    {{
                        "id": "{id}",
                        "name": "second",
                        "public_key": "{pk}",
                        "roles": ["admin"]
                    }}
                ]
            }}"#,
            id = kp.member_id(),
            pk = kp.public_key().to_hex(),
        );
        let err = MemberStore::from_json_str(&json, Path::new("members.json"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("duplicate public_key"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn invite_use_is_consumed_at_approval_and_not_at_request_or_rejection() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemberStore::load_from_dir(Some(dir.path())).unwrap();
        let invite = store
            .create_invite(vec![MemberRole::Worker], 1, 3600, true)
            .unwrap();
        assert_eq!(store.list_invites()[0].uses_remaining, 1);

        let cand1 = NodeKeypair::generate().unwrap();
        let cand2 = NodeKeypair::generate().unwrap();
        let hash1 = [0x11u8; KEY_LEN];
        let hash2 = [0x22u8; KEY_LEN];
        let proof1 = compute_enroll_proof(&invite.invite_secret.0, &hash1);
        let proof2 = compute_enroll_proof(&invite.invite_secret.0, &hash2);

        // Two pending requests on a single-use invite both succeed because uses are not consumed
        // until approval.
        let out1 = store
            .handle_enroll_request(
                *cand1.public_key(),
                &hash1,
                "1111-2222-3333-4444",
                "127.0.0.1:50001",
                &invite.invite_id,
                &proof1,
                "box-1".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap();
        let out2 = store
            .handle_enroll_request(
                *cand2.public_key(),
                &hash2,
                "1111-2222-3333-4444",
                "127.0.0.1:50002",
                &invite.invite_id,
                &proof2,
                "box-2".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap();
        let (pid1, mut rx1) = match out1 {
            EnrollOutcome::Pending {
                pending_id,
                decision_rx,
            } => (pending_id, decision_rx),
            _ => panic!("expected Pending"),
        };
        let (pid2, mut rx2) = match out2 {
            EnrollOutcome::Pending {
                pending_id,
                decision_rx,
            } => (pending_id, decision_rx),
            _ => panic!("expected Pending"),
        };
        assert_eq!(store.list_invites()[0].uses_remaining, 1);

        // Rejecting the first takes no use.
        store.reject_pending(&pid1[..6]).unwrap();
        assert!(matches!(
            rx1.try_recv().unwrap(),
            PendingDecision::Rejected { .. }
        ));
        assert_eq!(store.list_invites()[0].uses_remaining, 1);

        // Approving with a wrong pairing code is refused, keeps the request pending, and takes no use.
        let code2 = derive_pairing_code(&hash2);
        let wrong_code = if code2.as_str() == "000000" {
            "999999"
        } else {
            "000000"
        };
        let wrong_err = store
            .approve_pending(&pid2, wrong_code)
            .unwrap_err()
            .to_string();
        assert!(wrong_err.contains("pairing code mismatch"));
        assert_eq!(store.list_pending().len(), 1);
        assert_eq!(store.list_invites()[0].uses_remaining, 1);

        // Approving by name is refused (approve accepts only ID or unique hex ID prefix).
        assert!(store.approve_pending("box-2", code2.as_str()).is_err());

        // Approving the second by its pending ID prefix with the matching pairing code takes the
        // single use and enrolls box-2.
        let enrolled = store.approve_pending(&pid2[..6], code2.as_str()).unwrap();
        assert_eq!(enrolled.id, cand2.member_id());
        assert!(matches!(
            rx2.try_recv().unwrap(),
            PendingDecision::Approved { .. }
        ));
        assert_eq!(store.list_invites()[0].uses_remaining, 0);
        assert!(store.contains(cand2.public_key()));
        assert!(!store.contains(cand1.public_key()));
    }

    #[test]
    fn worker_id_ownership_prevents_takeover_and_enforces_per_member_bound() {
        let store = MemberStore::new();
        let wid = Uuid::new_v4();
        store.check_worker_id_claim(wid, Some("member_a")).unwrap();
        store.claim_worker_id(wid, "member_a").unwrap();
        store.claim_worker_id(wid, "member_a").unwrap();
        let err = store.claim_worker_id(wid, "member_b").unwrap_err();
        assert!(err.to_string().contains("owned by another member"));
        assert!(store.check_worker_id_claim(wid, Some("member_b")).is_err());
        assert!(store.check_worker_id_claim(wid, None).is_err());

        for _ in 1..MAX_WORKERS_PER_MEMBER {
            store.claim_worker_id(Uuid::new_v4(), "member_a").unwrap();
        }
        let extra = Uuid::new_v4();
        assert!(store
            .check_worker_id_claim(extra, Some("member_a"))
            .is_err());
        let bound_err = store.claim_worker_id(extra, "member_a").unwrap_err();
        assert!(bound_err.to_string().contains("maximum"));
    }

    #[test]
    fn master_side_expired_revoked_wrong_proof_32_cap_and_10_min_ttl() {
        let store = MemberStore::new();
        let inv = store
            .create_invite(vec![MemberRole::Worker], 5, 3600, true)
            .unwrap();
        let cand = NodeKeypair::generate().unwrap();
        let hash = [0x33u8; KEY_LEN];
        let valid_proof = compute_enroll_proof(&inv.invite_secret.0, &hash);

        // 1. Wrong proof is refused on the master with the generic wire message.
        let bad_proof = RedactedBytes32([0xffu8; KEY_LEN]);
        match store
            .handle_enroll_request(
                *cand.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:40000",
                &inv.invite_id,
                &bad_proof,
                "node-bad".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap()
        {
            EnrollOutcome::Rejected {
                log_reason,
                wire_message,
            } => {
                assert!(log_reason.contains("invalid enrollment proof"));
                assert_eq!(wire_message, GENERIC_ENROLL_REJECT_MESSAGE);
            }
            _ => panic!("expected wrong proof to be rejected"),
        }

        // 2. Expired invite is refused on the master.
        let exp_inv = store
            .create_invite(vec![MemberRole::Worker], 1, 60, true)
            .unwrap();
        {
            let mut guard = store.inner.write().unwrap();
            guard
                .state
                .invites
                .iter_mut()
                .find(|i| i.invite_id == exp_inv.invite_id)
                .unwrap()
                .expires_at = 1;
        }
        let exp_proof = compute_enroll_proof(&exp_inv.invite_secret.0, &hash);
        match store
            .handle_enroll_request(
                *cand.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:40001",
                &exp_inv.invite_id,
                &exp_proof,
                "node-exp".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap()
        {
            EnrollOutcome::Rejected {
                log_reason,
                wire_message,
            } => {
                assert!(log_reason.contains("expired"));
                assert_eq!(wire_message, GENERIC_ENROLL_REJECT_MESSAGE);
            }
            _ => panic!("expected expired invite to be rejected"),
        }

        // 3. Revoking an invite drops its pending requests and refuses subsequent requests.
        let rev_inv = store
            .create_invite(vec![MemberRole::Worker], 2, 3600, true)
            .unwrap();
        let rev_proof = compute_enroll_proof(&rev_inv.invite_secret.0, &hash);
        let mut rx_pending = match store
            .handle_enroll_request(
                *cand.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:40002",
                &rev_inv.invite_id,
                &rev_proof,
                "node-rev".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap()
        {
            EnrollOutcome::Pending { decision_rx, .. } => decision_rx,
            _ => panic!("expected Pending"),
        };
        store.revoke_invite(&rev_inv.invite_id[..8]).unwrap();
        assert!(
            matches!(rx_pending.try_recv(), Ok(PendingDecision::Rejected { .. })),
            "revoking invite must drop its pending requests"
        );
        assert!(store.list_pending().is_empty());

        match store
            .handle_enroll_request(
                *cand.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:40003",
                &rev_inv.invite_id,
                &rev_proof,
                "node-rev-2".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap()
        {
            EnrollOutcome::Rejected {
                log_reason,
                wire_message,
            } => {
                assert!(log_reason.contains("revoked"));
                assert_eq!(wire_message, GENERIC_ENROLL_REJECT_MESSAGE);
            }
            _ => panic!("expected revoked invite to be rejected"),
        }

        // 4. Duplicate request from the same key while one is live is refused without cancelling the first.
        let (first_pid, _first_rx) = match store
            .handle_enroll_request(
                *cand.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:40004",
                &inv.invite_id,
                &valid_proof,
                "node-dup-1".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap()
        {
            EnrollOutcome::Pending {
                pending_id,
                decision_rx,
            } => (pending_id, decision_rx),
            _ => panic!("expected first request Pending"),
        };
        match store
            .handle_enroll_request(
                *cand.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:40005",
                &inv.invite_id,
                &valid_proof,
                "node-dup-2".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap()
        {
            EnrollOutcome::Rejected { log_reason, .. } => {
                assert!(log_reason.contains("already has a live pending enrollment request"));
            }
            _ => panic!("expected second concurrent request from same key to be rejected"),
        }
        store.remove_pending(&first_pid);

        // 5. 32-request cap (MAX_PENDING_ENROLLMENTS) is enforced, and 10-minute TTL expiry prunes old entries.
        let mut receivers = Vec::new();
        for idx in 0..MAX_PENDING_ENROLLMENTS {
            let kp = NodeKeypair::generate().unwrap();
            match store
                .handle_enroll_request(
                    *kp.public_key(),
                    &hash,
                    "1111-2222-3333-4444",
                    "127.0.0.1:41000",
                    &inv.invite_id,
                    &valid_proof,
                    format!("cap-{idx}"),
                    "linux".into(),
                    "x86_64".into(),
                )
                .unwrap()
            {
                EnrollOutcome::Pending { decision_rx, .. } => receivers.push(decision_rx),
                _ => panic!("expected Pending at {idx}"),
            }
        }
        assert_eq!(store.list_pending().len(), MAX_PENDING_ENROLLMENTS);
        let overflow_kp = NodeKeypair::generate().unwrap();
        match store
            .handle_enroll_request(
                *overflow_kp.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:41999",
                &inv.invite_id,
                &valid_proof,
                "overflow".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap()
        {
            EnrollOutcome::Rejected { log_reason, .. } => {
                assert!(log_reason.contains("pending enrollment limit"));
            }
            _ => panic!("expected 33rd pending request to be rejected"),
        }

        // Backdate one pending request past the 10-minute TTL and verify it is pruned so the next request succeeds.
        {
            let mut guard = store.inner.write().unwrap();
            let first_entry = guard.pending.values_mut().next().unwrap();
            first_entry.info.expires_at = now_unix_secs().saturating_sub(1);
        }
        assert_eq!(store.list_pending().len(), MAX_PENDING_ENROLLMENTS - 1);
        assert!(matches!(
            store
                .handle_enroll_request(
                    *overflow_kp.public_key(),
                    &hash,
                    "1111-2222-3333-4444",
                    "127.0.0.1:41999",
                    &inv.invite_id,
                    &valid_proof,
                    "overflow-after-ttl".into(),
                    "linux".into(),
                    "x86_64".into(),
                )
                .unwrap(),
            EnrollOutcome::Pending { .. }
        ));
    }

    #[test]
    fn admin_invite_rule_revoked_keys_role_merge_and_id_only_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemberStore::load_from_dir(Some(dir.path())).unwrap();

        // Item 6: admin invite requires approval and uses == 1.
        assert!(store
            .create_invite(vec![MemberRole::Admin], 1, 3600, false)
            .is_err());
        assert!(store
            .create_invite(vec![MemberRole::Admin], 2, 3600, true)
            .is_err());
        let admin_inv = store
            .create_invite(vec![MemberRole::Admin], 1, 3600, true)
            .unwrap();

        // Item 8: enrolling again merges roles instead of overwriting existing roles.
        let host_kp = NodeKeypair::generate().unwrap();
        store
            .ensure_local_host_admin(*host_kp.public_key(), "local-host", true)
            .unwrap();
        let worker_inv = store
            .create_invite(vec![MemberRole::Worker], 2, 3600, false)
            .unwrap();
        let hash = [0x44u8; KEY_LEN];
        let proof = compute_enroll_proof(&worker_inv.invite_secret.0, &hash);
        match store
            .handle_enroll_request(
                *host_kp.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:42000",
                &worker_inv.invite_id,
                &proof,
                "local-host".into(),
                "macos".into(),
                "aarch64".into(),
            )
            .unwrap()
        {
            EnrollOutcome::ImmediateApproved { roles, .. } => {
                assert!(roles.contains(&MemberRole::Admin));
                assert!(roles.contains(&MemberRole::Worker));
            }
            _ => panic!("expected ImmediateApproved"),
        }

        // Item 10: a request named after another request's ID prefix cannot hijack `approve_pending`.
        let cand_real = NodeKeypair::generate().unwrap();
        let cand_spoof = NodeKeypair::generate().unwrap();
        let admin_proof = compute_enroll_proof(&admin_inv.invite_secret.0, &hash);
        let (real_pid, _real_rx) = match store
            .handle_enroll_request(
                *cand_real.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:42001",
                &admin_inv.invite_id,
                &admin_proof,
                "honest-admin".into(),
                "linux\x1b[31m".into(),
                "x86_64\n".into(),
            )
            .unwrap()
        {
            EnrollOutcome::Pending {
                pending_id,
                decision_rx,
            } => (pending_id, decision_rx),
            _ => panic!("expected Pending"),
        };
        let prefix = &real_pid[..6];
        let (_spoof_pid, _spoof_rx) = match store
            .handle_enroll_request(
                *cand_spoof.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:42002",
                &admin_inv.invite_id,
                &admin_proof,
                prefix.to_string(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap()
        {
            EnrollOutcome::Pending {
                pending_id,
                decision_rx,
            } => (pending_id, decision_rx),
            _ => panic!("expected Pending"),
        };
        let code = derive_pairing_code(&hash);
        let approved = store.approve_pending(prefix, code.as_str()).unwrap();
        assert_eq!(
            approved.id,
            cand_real.member_id(),
            "ID prefix must resolve to the request with that ID, never to a request named after that prefix"
        );
        // Control characters in os/arch were stripped when stored.
        assert!(validate_member_name("bad\x1bname").is_err());
        assert!(validate_member_name(&"a".repeat(65)).is_err());

        // Item 7: revoking a member records its public key in `revoked_keys` (persisted across reload)
        // and refuses re-enrollment of that key.
        store.revoke_member(&cand_real.member_id()[..6]).unwrap();
        let reloaded = MemberStore::load_from_dir(Some(dir.path())).unwrap();
        let proof2 = compute_enroll_proof(&worker_inv.invite_secret.0, &hash);
        match reloaded
            .handle_enroll_request(
                *cand_real.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:42003",
                &worker_inv.invite_id,
                &proof2,
                "honest-admin".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap()
        {
            EnrollOutcome::Rejected {
                log_reason,
                wire_message,
            } => {
                assert!(log_reason.contains("revoked"));
                assert_eq!(wire_message, GENERIC_ENROLL_REJECT_MESSAGE);
            }
            _ => panic!("expected revoked key to be refused re-enrollment"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn reject_group_or_world_accessible_members_file_or_writable_directory() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = dir.path().join("members.json");
        let kp = NodeKeypair::generate().unwrap();
        let json = format!(
            r#"{{"version": 1, "members": [{{"name": "w", "public_key": "{}"}}]}}"#,
            kp.public_key().to_hex()
        );
        std::fs::write(&path, &json).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

        // Private file + private directory succeeds.
        assert_eq!(MemberStore::load_file(&path).unwrap().len(), 1);

        // Group-readable (0640) members.json is refused (0o077 check).
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let err = MemberStore::load_file(&path).unwrap_err().to_string();
        assert!(
            err.contains("allow group/others access"),
            "unexpected error for 0640: {err}"
        );

        // Group-writable (0620) members.json is refused.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o620)).unwrap();
        let err = MemberStore::load_file(&path).unwrap_err().to_string();
        assert!(
            err.contains("allow group/others access"),
            "unexpected error for 0620: {err}"
        );

        // Restore 0600 on file, make parent directory world-writable: refused.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        let err = MemberStore::load_file(&path).unwrap_err().to_string();
        assert!(
            err.contains("parent directory") && err.contains("allowing group/others write access"),
            "unexpected error: {err}"
        );
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn claim_worker_id_evicting_drops_oldest_non_live_claims_and_caps_live_claims_at_64() {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let store = MemberStore::load_from_dir(Some(dir.path())).unwrap();
        let member_id = "8f14e45fceea167a";

        // 70 consecutive unnamed worker runs (only the current run's worker_id is live):
        // all 70 succeed, and the store retains the most recent 64 worker IDs.
        let mut history = Vec::new();
        for _ in 0..70 {
            let wid = Uuid::new_v4();
            let evicted = store
                .claim_worker_id_evicting(wid, member_id, |_existing| false)
                .expect("non-live claims must be LRU-evicted past 64");
            if history.len() >= MAX_WORKERS_PER_MEMBER {
                assert_eq!(evicted.len(), 1);
                assert_eq!(
                    evicted[0],
                    history[history.len() - MAX_WORKERS_PER_MEMBER],
                    "oldest non-live worker_id must be evicted first"
                );
            } else {
                assert!(evicted.is_empty());
            }
            history.push(wid);
        }
        let owned = store.workers_owned_by(member_id);
        assert_eq!(owned.len(), MAX_WORKERS_PER_MEMBER);
        for wid in &history[..6] {
            assert!(!owned.contains(wid), "evicted worker_id {wid} must be gone");
        }
        for wid in &history[6..] {
            assert!(
                owned.contains(wid),
                "recent worker_id {wid} must still be owned"
            );
        }

        // If all 64 currently claimed worker IDs are live, a 65th distinct worker_id is refused.
        let live_65th = Uuid::new_v4();
        let err = store
            .claim_worker_id_evicting(live_65th, member_id, |_existing| true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("maximum of 64"), "got: {err}");

        // Item 8: Re-registering the oldest remaining claim (`history[6]`) bumps its persisted
        // `worker_last_seq`; after reloading `MemberStore` from disk, evicting one more non-live
        // claim drops `history[7]` (now oldest) rather than `history[6]` (which has the smallest
        // UUID on average or was originally registered earliest).
        let refreshed = history[6];
        store
            .claim_worker_id_evicting(refreshed, member_id, |_existing| false)
            .expect("re-claiming owned worker_id must succeed and refresh LRU seq");
        let reloaded = MemberStore::load_from_dir(Some(dir.path())).unwrap();
        let post_reload_wid = Uuid::new_v4();
        let evicted_after_reload = reloaded
            .claim_worker_id_evicting(post_reload_wid, member_id, |_existing| false)
            .expect("LRU eviction after reload must succeed");
        assert_eq!(
            evicted_after_reload,
            vec![history[7]],
            "persisted worker_last_seq must preserve LRU order across master restart"
        );
        assert!(reloaded.workers_owned_by(member_id).contains(&refreshed));
    }

    #[test]
    fn test_n5_revoked_host_key_not_re_enrolled_n8_preserve_name_and_via_invite_n9_cf_format_chars()
    {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let store = MemberStore::load_from_dir(Some(dir.path())).unwrap();
        let host_kp = NodeKeypair::generate().unwrap();
        store
            .ensure_local_host_admin(*host_kp.public_key(), "local-host", true)
            .unwrap();
        assert!(store.contains(host_kp.public_key()));

        // N5: once revoked, ensure_local_host_admin must not re-add the revoked key.
        store.revoke_member(&host_kp.member_id()).unwrap();
        assert!(!store.contains(host_kp.public_key()));
        store
            .ensure_local_host_admin(*host_kp.public_key(), "local-host", true)
            .unwrap();
        assert!(
            !store.contains(host_kp.public_key()),
            "revoked local-host key must not be re-enrolled by ensure_local_host_admin"
        );

        // N8: re-enrolling an existing member to add a role preserves its existing name and
        // original via_invite.
        let inv1 = store
            .create_invite(vec![MemberRole::Worker], 5, 3600, false)
            .unwrap();
        let inv2 = store
            .create_invite(vec![MemberRole::Client], 5, 3600, false)
            .unwrap();
        let member_kp = NodeKeypair::generate().unwrap();
        let hash = [0x33u8; KEY_LEN];
        let proof1 = compute_enroll_proof(&inv1.invite_secret.0, &hash);
        store
            .handle_enroll_request(
                *member_kp.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:43000",
                &inv1.invite_id,
                &proof1,
                "initial-box".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap();
        store
            .rename_member(&member_kp.member_id(), "curated-admin-name")
            .unwrap();
        let proof2 = compute_enroll_proof(&inv2.invite_secret.0, &hash);
        store
            .handle_enroll_request(
                *member_kp.public_key(),
                &hash,
                "1111-2222-3333-4444",
                "127.0.0.1:43001",
                &inv2.invite_id,
                &proof2,
                "overwriting-hostname".into(),
                "linux".into(),
                "x86_64".into(),
            )
            .unwrap();
        let updated = store.get(member_kp.public_key()).unwrap();
        assert_eq!(
            updated.name, "curated-admin-name",
            "re-enrollment must preserve existing non-empty member name"
        );
        assert_eq!(
            updated.via_invite.as_deref(),
            Some(inv1.invite_id.as_str()),
            "re-enrollment must preserve original via_invite"
        );
        assert!(updated.has_worker_role() && updated.has_client_role());

        // N9: Unicode format characters (Cf: BiDi overrides/isolates, zero-width space) are
        // rejected by validate_member_name and stripped by sanitize_display_field.
        for bad in ["node\u{202e}spoof", "node\u{2066}iso", "node\u{200b}zw"] {
            assert!(
                validate_member_name(bad).is_err(),
                "expected validate_member_name to reject {bad:?}"
            );
        }
        assert_eq!(
            sanitize_display_field("linux\u{202e}\u{2066}\u{200b}-x64", 32),
            "linux-x64"
        );
    }

    #[test]
    fn test_rollback_approved_enrollment_cas_never_restores_revoked_or_double_approved_member() {
        let enroll_pending = |store: &MemberStore,
                              kp: &NodeKeypair,
                              hash: [u8; KEY_LEN],
                              invite_id: &str,
                              secret: &[u8; 32]| {
            let proof = compute_enroll_proof(secret, &hash);
            match store
                .handle_enroll_request(
                    *kp.public_key(),
                    &hash,
                    "1111-2222-3333-4444",
                    "127.0.0.1:50001",
                    invite_id,
                    &proof,
                    "box".into(),
                    "linux".into(),
                    "x86_64".into(),
                )
                .unwrap()
            {
                EnrollOutcome::Pending {
                    pending_id,
                    decision_rx,
                } => (pending_id, decision_rx),
                _ => panic!("expected Pending"),
            }
        };

        let approve = |store: &MemberStore,
                       pid: &str,
                       hash: [u8; KEY_LEN],
                       rx: &mut oneshot::Receiver<PendingDecision>| {
            store
                .approve_pending(pid, derive_pairing_code(&hash).as_str())
                .unwrap();
            match rx.try_recv().unwrap() {
                PendingDecision::Approved { rollback, .. } => *rollback,
                _ => panic!("expected Approved"),
            }
        };

        // Scenario A: member M re-enrolls, admin approves, admin revokes M, then M declines /
        // disconnects -> M must NOT be restored to members.
        let m = NodeKeypair::generate().unwrap();
        let store = MemberStore::from_entries(vec![MemberEntry::new(
            "m-box",
            *m.public_key(),
            vec![MemberRole::Worker],
        )]);
        let inv = store
            .create_invite(vec![MemberRole::Client], 1, 3600, true)
            .unwrap();
        let hash = [0x41u8; KEY_LEN];
        let (pid, mut rx) = enroll_pending(&store, &m, hash, &inv.invite_id, &inv.invite_secret.0);
        let rb = approve(&store, &pid, hash, &mut rx);
        store.revoke_member(&m.member_id()).unwrap();
        assert!(!store.contains(m.public_key()));
        store.rollback_approved_enrollment(rb).unwrap();
        assert!(
            !store.contains(m.public_key()),
            "rollback must never restore a revoked member"
        );
        assert!(store.get(m.public_key()).is_none());
        assert_eq!(store.list_invites()[0].uses_remaining, 1);

        // Scenario B: fresh joiner J approved twice across two single-use invites, then declines
        // both -> J is not a member and both invites have uses_remaining == 1.
        let store = MemberStore::new();
        let j = NodeKeypair::generate().unwrap();
        let inv1 = store
            .create_invite(vec![MemberRole::Worker], 1, 3600, true)
            .unwrap();
        let inv2 = store
            .create_invite(vec![MemberRole::Worker], 1, 3600, true)
            .unwrap();
        let h1 = [0x51u8; KEY_LEN];
        let h2 = [0x52u8; KEY_LEN];
        let (p1, mut r1) = enroll_pending(&store, &j, h1, &inv1.invite_id, &inv1.invite_secret.0);
        let rb1 = approve(&store, &p1, h1, &mut r1);
        let (p2, mut r2) = enroll_pending(&store, &j, h2, &inv2.invite_id, &inv2.invite_secret.0);
        let rb2 = approve(&store, &p2, h2, &mut r2);
        store.rollback_approved_enrollment(rb1).unwrap();
        store.rollback_approved_enrollment(rb2).unwrap();
        let uses: Vec<u32> = store
            .list_invites()
            .iter()
            .map(|i| i.uses_remaining)
            .collect();
        assert!(
            !store.contains(j.public_key()),
            "fresh joiner declining both approvals must not remain a member"
        );
        assert_eq!(uses, vec![1, 1]);
    }
}
