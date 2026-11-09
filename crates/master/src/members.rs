//! Enrolled member store (`members.json`), invites, pending enrollments, worker-ID ownership, and
//! master authentication mode (`--auth`).

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rusty_grid_core::noise_auth::{
    default_oxideswarm_dir, derive_pairing_code, verify_enroll_proof, KEY_LEN,
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
}

impl Default for MembersFile {
    fn default() -> Self {
        Self {
            version: MEMBERS_FILE_VERSION,
            members: Vec::new(),
            invites: Vec::new(),
            worker_owners: BTreeMap::new(),
        }
    }
}

/// Decision delivered to a waiting `ENROLL` connection when an admin approves or rejects it.
#[derive(Debug, Clone)]
pub enum PendingDecision {
    Approved {
        member_id: String,
        roles: Vec<MemberRole>,
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
    decision_tx: oneshot::Sender<PendingDecision>,
}

#[derive(Default)]
struct MemberStoreInner {
    by_pubkey: HashMap<PublicKey, MemberEntry>,
    invites: Vec<InviteRecord>,
    worker_owners: BTreeMap<Uuid, String>,
    pending: HashMap<String, PendingEnrollmentEntry>,
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
            .field("members", &guard.by_pubkey.len())
            .field("invites", &guard.invites.len())
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
    if mode & 0o022 != 0 {
        let msg = format!(
            "refusing to load members file {}: permissions {:03o} allow group/others write access (fix with: chmod 600 {})",
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
    fn to_members_file(&self) -> MembersFile {
        let mut members: Vec<MemberEntry> = self.by_pubkey.values().cloned().collect();
        members.sort_by(|a, b| a.id.cmp(&b.id));
        MembersFile {
            version: MEMBERS_FILE_VERSION,
            members,
            invites: self.invites.clone(),
            worker_owners: self.worker_owners.clone(),
        }
    }

    fn persist_if_configured(&self) -> GridResult<()> {
        if let Some(ref path) = self.save_path {
            write_members_file_atomic(path, &self.to_members_file())?;
        }
        Ok(())
    }

    fn prune_pending(&mut self, now: u64) {
        self.pending
            .retain(|_, entry| now < entry.info.expires_at && !entry.decision_tx.is_closed());
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
        let entry = match self.by_pubkey.get_mut(&public_key) {
            Some(existing) => {
                existing.id = id.clone();
                if !name.trim().is_empty() {
                    existing.name = name;
                }
                existing.roles = roles;
                existing.last_seen = Some(now);
                if via_invite.is_some() {
                    existing.via_invite = via_invite;
                }
                existing.clone()
            }
            None => {
                let entry = MemberEntry {
                    id: id.clone(),
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
        };
        entry
    }
}

/// Resolves a target identifier (`id`, prefix >= 4 chars, or exact `name`) over items `(id, name)`.
fn resolve_id_or_name<'a, I>(target: &str, items: I, kind: &str) -> GridResult<String>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let q = target.trim();
    if q.is_empty() {
        return Err(GridError::Config(format!("{kind} target cannot be empty")));
    }
    let q_clean = q.replace('-', "").to_ascii_lowercase();
    let all: Vec<(&'a str, &'a str)> = items.into_iter().collect();

    // 1. Exact ID match.
    for (id, _) in &all {
        if id.eq_ignore_ascii_case(&q_clean) || id.eq_ignore_ascii_case(q) {
            return Ok((*id).to_string());
        }
    }
    // 2. Exact name match (if unique).
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
    // 3. Unique ID prefix of at least 4 hex characters.
    if q_clean.len() >= 4 {
        let prefix_matches: Vec<&str> = all
            .iter()
            .filter(|(id, _)| id.to_ascii_lowercase().starts_with(&q_clean))
            .map(|(id, _)| *id)
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
            by_pubkey.insert(entry.public_key, entry);
        }
        Self {
            inner: Arc::new(RwLock::new(MemberStoreInner {
                by_pubkey,
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
                    entry.name,
                    raw_id
                )));
            }
            entry.id = expected_id.clone();
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
                by_pubkey,
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
            guard.invites = parsed.invites;
            guard.worker_owners = parsed.worker_owners;
        }
        Ok(store)
    }

    /// Number of enrolled members in the store.
    pub fn len(&self) -> usize {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .by_pubkey
            .len()
    }

    /// Returns `true` if the store holds no members.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if there is any member other than the auto-enrolled local host admin.
    pub fn has_non_host_members(&self) -> bool {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard
            .by_pubkey
            .values()
            .any(|m| m.via_invite.as_deref() != Some(LOCAL_HOST_INVITE_TAG))
    }

    /// Returns `true` if `pubkey` belongs to an enrolled member.
    pub fn contains(&self, pubkey: &PublicKey) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .by_pubkey
            .contains_key(pubkey)
    }

    /// Looks up a member entry by public key.
    pub fn get(&self, pubkey: &PublicKey) -> Option<MemberEntry> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .by_pubkey
            .get(pubkey)
            .cloned()
    }

    /// Updates `last_seen` for `pubkey` in memory.
    pub fn touch_last_seen(&self, pubkey: &PublicKey) {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = guard.by_pubkey.get_mut(pubkey) {
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
        let now = now_unix_secs();
        let changed = match guard.by_pubkey.get_mut(&host_pubkey) {
            Some(entry) => {
                if !entry.roles.contains(&MemberRole::Admin) {
                    entry.roles.push(MemberRole::Admin);
                    true
                } else {
                    false
                }
            }
            None => {
                let entry = MemberEntry {
                    id: host_pubkey.member_id(),
                    name: host_name.into(),
                    public_key: host_pubkey,
                    roles: vec![MemberRole::Admin],
                    enrolled_at: Some(now),
                    last_seen: Some(now),
                    via_invite: Some(LOCAL_HOST_INVITE_TAG.to_string()),
                };
                guard.by_pubkey.insert(host_pubkey, entry);
                true
            }
        };
        if changed && persist {
            guard.persist_if_configured()?;
        }
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
        let invite_id = random_hex(16)?;
        let invite_secret = RedactedBytes32(random_bytes_32()?);
        let expires_at = now_unix_secs().saturating_add(expires_in_secs);
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
        guard.invites.push(record.clone());
        guard.persist_if_configured()?;
        Ok(record)
    }

    /// Lists all invites (design 0051 D3).
    pub fn list_invites(&self) -> Vec<InviteSummary> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard.invites.iter().map(InviteRecord::to_summary).collect()
    }

    /// Revokes an invite by ID or unique prefix (>= 4 chars).
    pub fn revoke_invite(&self, target: &str) -> GridResult<String> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let id = resolve_id_or_name(
            target,
            guard.invites.iter().map(|inv| (inv.invite_id.as_str(), "")),
            "invite",
        )?;
        let inv = guard
            .invites
            .iter_mut()
            .find(|i| i.invite_id == id)
            .ok_or_else(|| GridError::Config(format!("unknown invite '{target}'")))?;
        inv.revoked = true;
        guard.persist_if_configured()?;
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
        invite_id: &str,
        proof: &RedactedBytes32,
        name: String,
        os: String,
        arch: String,
    ) -> GridResult<EnrollOutcome> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let now = now_unix_secs();
        guard.prune_pending(now);

        let Some(inv_idx) = guard.invites.iter().position(|i| i.invite_id == invite_id) else {
            return Ok(EnrollOutcome::Rejected {
                log_reason: format!("unknown invite_id '{invite_id}'"),
                wire_message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
        };
        let invite = &guard.invites[inv_idx];
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
            name.trim().to_string()
        };

        if !invite.require_approval {
            let roles = invite.roles.clone();
            let inv_id = invite.invite_id.clone();
            guard.invites[inv_idx].uses_remaining -= 1;
            let entry = guard.upsert_member(
                peer_public_key,
                clean_name,
                roles.clone(),
                Some(inv_id),
                now,
            );
            guard.persist_if_configured()?;
            return Ok(EnrollOutcome::ImmediateApproved {
                member_id: entry.id,
                roles,
            });
        }

        let member_id = peer_public_key.member_id();
        if !guard.pending.contains_key(&member_id) && guard.pending.len() >= MAX_PENDING_ENROLLMENTS
        {
            return Ok(EnrollOutcome::Rejected {
                log_reason: format!("pending enrollment limit ({MAX_PENDING_ENROLLMENTS}) reached"),
                wire_message: "too many pending enrollment requests; try again later".to_string(),
            });
        }

        let pairing_code = derive_pairing_code(handshake_hash);
        let info = PendingEnrollmentInfo {
            pending_id: member_id.clone(),
            member_id: member_id.clone(),
            public_key: peer_public_key,
            name: clean_name,
            os,
            arch,
            roles: invite.roles.clone(),
            invite_id: invite.invite_id.clone(),
            pairing_code,
            requested_at: now,
            expires_at: now.saturating_add(PENDING_ENROLLMENT_TTL_SECS),
        };
        let (decision_tx, decision_rx) = oneshot::channel();
        guard.pending.insert(
            member_id.clone(),
            PendingEnrollmentEntry { info, decision_tx },
        );
        Ok(EnrollOutcome::Pending {
            pending_id: member_id,
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

    /// Approves a pending enrollment request by ID, prefix, or name (design 0051 D4/D5).
    /// Consumes one use from the invite at approval time.
    pub fn approve_pending(&self, target: &str) -> GridResult<MemberEntry> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let now = now_unix_secs();
        guard.prune_pending(now);

        let id = resolve_id_or_name(
            target,
            guard
                .pending
                .values()
                .map(|e| (e.info.pending_id.as_str(), e.info.name.as_str())),
            "pending enrollment",
        )?;
        let pending = guard
            .pending
            .remove(&id)
            .ok_or_else(|| GridError::Config(format!("no pending enrollment '{target}'")))?;

        let inv_idx = guard
            .invites
            .iter()
            .position(|i| i.invite_id == pending.info.invite_id);
        let Some(inv_idx) = inv_idx else {
            let _ = pending.decision_tx.send(PendingDecision::Rejected {
                message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
            return Err(GridError::Config(
                "cannot approve: the invite for this request no longer exists".into(),
            ));
        };
        if guard.invites[inv_idx].revoked {
            let _ = pending.decision_tx.send(PendingDecision::Rejected {
                message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
            return Err(GridError::Config(
                "cannot approve: the invite for this request has been revoked".into(),
            ));
        }
        if now >= guard.invites[inv_idx].expires_at {
            let _ = pending.decision_tx.send(PendingDecision::Rejected {
                message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
            return Err(GridError::Config(
                "cannot approve: the invite for this request has expired".into(),
            ));
        }
        if guard.invites[inv_idx].uses_remaining == 0 {
            let _ = pending.decision_tx.send(PendingDecision::Rejected {
                message: GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
            });
            return Err(GridError::Config(
                "cannot approve: the invite for this request has no remaining uses".into(),
            ));
        }

        guard.invites[inv_idx].uses_remaining -= 1;
        let entry = guard.upsert_member(
            pending.info.public_key,
            pending.info.name.clone(),
            pending.info.roles.clone(),
            Some(pending.info.invite_id.clone()),
            now,
        );
        guard.persist_if_configured()?;
        let _ = pending.decision_tx.send(PendingDecision::Approved {
            member_id: entry.id.clone(),
            roles: entry.roles.clone(),
        });
        Ok(entry)
    }

    /// Rejects a pending enrollment request by ID, prefix, or name without consuming an invite use.
    pub fn reject_pending(&self, target: &str) -> GridResult<String> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let now = now_unix_secs();
        guard.prune_pending(now);

        let id = resolve_id_or_name(
            target,
            guard
                .pending
                .values()
                .map(|e| (e.info.pending_id.as_str(), e.info.name.as_str())),
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
        let mut list: Vec<MemberEntry> = guard.by_pubkey.values().cloned().collect();
        list.sort_by(|a, b| a.id.cmp(&b.id));
        list
    }

    /// Renames an enrolled member by ID, prefix, or current name.
    pub fn rename_member(&self, target: &str, new_name: &str) -> GridResult<MemberEntry> {
        let clean = new_name.trim();
        if clean.is_empty() {
            return Err(GridError::Config("new member name cannot be empty".into()));
        }
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let id = resolve_id_or_name(
            target,
            guard
                .by_pubkey
                .values()
                .map(|m| (m.id.as_str(), m.name.as_str())),
            "member",
        )?;
        let pubkey = *guard
            .by_pubkey
            .iter()
            .find(|(_, m)| m.id == id)
            .map(|(k, _)| k)
            .ok_or_else(|| GridError::Config(format!("no member '{target}'")))?;
        let updated = {
            let entry = guard.by_pubkey.get_mut(&pubkey).unwrap();
            entry.name = clean.to_string();
            entry.clone()
        };
        guard.persist_if_configured()?;
        Ok(updated)
    }

    /// Revokes an enrolled member by ID, prefix, or name and releases its owned worker IDs.
    /// Returns `(revoked_entry, owned_worker_ids)`.
    pub fn revoke_member(&self, target: &str) -> GridResult<(MemberEntry, Vec<Uuid>)> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let id = resolve_id_or_name(
            target,
            guard
                .by_pubkey
                .values()
                .map(|m| (m.id.as_str(), m.name.as_str())),
            "member",
        )?;
        let pubkey = *guard
            .by_pubkey
            .iter()
            .find(|(_, m)| m.id == id)
            .map(|(k, _)| k)
            .ok_or_else(|| GridError::Config(format!("no member '{target}'")))?;
        let removed = guard.by_pubkey.remove(&pubkey).unwrap();
        let mut owned_workers = Vec::new();
        guard.worker_owners.retain(|wid, owner| {
            if *owner == removed.id {
                owned_workers.push(*wid);
                false
            } else {
                true
            }
        });
        guard.persist_if_configured()?;
        Ok((removed, owned_workers))
    }

    /// Records or verifies ownership of `worker_id` by `member_id` (design 0051 D5).
    ///
    /// The first `Register` of `worker_id` from a v3 member records `member_id` as its owner.
    /// A `Register` with a `worker_id` owned by another member is refused.
    pub fn claim_worker_id(&self, worker_id: Uuid, member_id: &str) -> GridResult<()> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(existing_owner) = guard.worker_owners.get(&worker_id) {
            if existing_owner != member_id {
                return Err(GridError::AuthFailed(format!(
                    "worker_id {worker_id} is owned by another member ({existing_owner}); member {member_id} cannot register it"
                )));
            }
            return Ok(());
        }
        guard.worker_owners.insert(worker_id, member_id.to_string());
        guard.persist_if_configured()?;
        Ok(())
    }

    /// Returns the worker IDs currently owned by `member_id`.
    pub fn workers_owned_by(&self, member_id: &str) -> Vec<Uuid> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard
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

        // Approving the second takes the single use and enrolls box-2.
        let enrolled = store.approve_pending("box-2").unwrap();
        assert_eq!(enrolled.id, pid2);
        assert!(matches!(
            rx2.try_recv().unwrap(),
            PendingDecision::Approved { .. }
        ));
        assert_eq!(store.list_invites()[0].uses_remaining, 0);
        assert!(store.contains(cand2.public_key()));
        assert!(!store.contains(cand1.public_key()));
    }

    #[test]
    fn worker_id_ownership_prevents_takeover_by_another_member() {
        let store = MemberStore::new();
        let wid = Uuid::new_v4();
        store.claim_worker_id(wid, "member_a").unwrap();
        store.claim_worker_id(wid, "member_a").unwrap();
        let err = store.claim_worker_id(wid, "member_b").unwrap_err();
        assert!(err.to_string().contains("owned by another member"));
    }

    #[cfg(unix)]
    #[test]
    fn reject_group_or_world_writable_members_file_or_directory() {
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

        // Group-writable members.json is refused.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o620)).unwrap();
        let err = MemberStore::load_file(&path).unwrap_err().to_string();
        assert!(
            err.contains("allow group/others write access"),
            "unexpected error: {err}"
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
}
