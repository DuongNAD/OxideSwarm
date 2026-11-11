//! Enrolled member store (`members.json`) and master authentication mode (`--auth`).
//!
//! In `ENROLL-1a` (`docs/design/0051-machine-identity-and-invites.md`), the master reads
//! `members.json` once at start-up from `--state-dir` (or `~/.oxideswarm/members.json` when
//! `--state-dir` is not set). Missing `members.json` is treated as an empty member list.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use rusty_grid_core::noise_auth::{default_oxideswarm_dir, PublicKey};
use rusty_grid_core::{GridError, GridResult};
use serde::{Deserialize, Serialize};

/// File name of the enrolled member list inside `--state-dir` or `~/.oxideswarm/`.
pub const MEMBERS_FILE_NAME: &str = "members.json";

/// Supported schema version for `members.json`.
pub const MEMBERS_FILE_VERSION: u32 = 1;

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

/// Role granted to an enrolled member (`docs/design/0051-machine-identity-and-invites.md` D3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberRole {
    /// May register as a worker (`WorkerMessage::Register`) and execute tasks.
    Worker,
    /// May submit tasks, query status, and read/write blobs.
    Client,
    /// May manage invites and members; implies [`MemberRole::Client`], does **not** imply
    /// [`MemberRole::Worker`].
    Admin,
}

fn default_member_roles() -> Vec<MemberRole> {
    vec![MemberRole::Worker]
}

/// A single enrolled machine entry in `members.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberEntry {
    /// Member ID (first 16 hex chars of `SHA-256(public_key)` by convention).
    #[serde(default)]
    pub id: String,
    /// Human-readable machine name.
    #[serde(default)]
    pub name: String,
    /// Static 32-byte X25519 public key (serialized as 64 hex chars).
    pub public_key: PublicKey,
    /// Granted roles for this machine.
    #[serde(default = "default_member_roles")]
    pub roles: Vec<MemberRole>,
    /// Optional Unix timestamp when the machine was enrolled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enrolled_at: Option<u64>,
    /// Optional invite ID used for enrollment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enrolled_via: Option<String>,
    /// Optional Unix timestamp when the machine was last seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<u64>,
}

impl MemberEntry {
    /// Creates a new member entry with canonical `id = public_key.member_id()`.
    pub fn new(name: impl Into<String>, public_key: PublicKey, roles: Vec<MemberRole>) -> Self {
        Self {
            id: public_key.member_id(),
            name: name.into(),
            public_key,
            roles,
            enrolled_at: None,
            enrolled_via: None,
            last_seen: None,
        }
    }

    /// Canonical member ID derived from `self.public_key`.
    pub fn effective_id(&self) -> String {
        self.public_key.member_id()
    }

    /// Returns `true` if this member is permitted to register as a worker.
    ///
    /// Per design 0051 D3, `admin` implies `client` but does **not** imply `worker`.
    pub fn has_worker_role(&self) -> bool {
        self.roles.contains(&MemberRole::Worker)
    }

    /// Returns `true` if this member has client permissions (`client` or `admin`).
    pub fn has_client_role(&self) -> bool {
        self.roles.contains(&MemberRole::Client) || self.roles.contains(&MemberRole::Admin)
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
}

impl Default for MembersFile {
    fn default() -> Self {
        Self {
            version: MEMBERS_FILE_VERSION,
            members: Vec::new(),
        }
    }
}

/// In-memory lookup table of enrolled members keyed by their X25519 [`PublicKey`].
#[derive(Debug, Clone, Default)]
pub struct MemberStore {
    by_pubkey: HashMap<PublicKey, MemberEntry>,
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
    let (aces, _) = read_dacl(path).map_err(|e| {
        GridError::Config(format!(
            "failed to read DACL of members file {}: {e}",
            path.display()
        ))
    })?;
    let offenders = open_grants(&aces, &user_sid);
    if let Some(msg) = open_acl_warning(path, "members file", &offenders, &user_sid) {
        tracing::warn!("{msg}");
        return Err(GridError::Config(msg));
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        if let Ok((dir_aces, _)) = read_dacl(parent) {
            let dir_offenders = open_grants(&dir_aces, &user_sid);
            if let Some(msg) =
                open_acl_warning(parent, "members directory", &dir_offenders, &user_sid)
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

impl MemberStore {
    /// Creates an empty member store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a store from an iterator of [`MemberEntry`] items.
    pub fn from_entries(entries: impl IntoIterator<Item = MemberEntry>) -> Self {
        let mut by_pubkey = HashMap::new();
        for mut entry in entries {
            entry.id = entry.public_key.member_id();
            by_pubkey.insert(entry.public_key, entry);
        }
        Self { by_pubkey }
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
        Ok(Self { by_pubkey })
    }

    /// Resolves the path to `members.json` inside `state_dir` or `~/.oxideswarm/members.json`.
    pub fn resolve_path(state_dir: Option<&Path>) -> Option<PathBuf> {
        let dir = match state_dir {
            Some(d) => d.to_path_buf(),
            None => default_oxideswarm_dir()?,
        };
        Some(dir.join(MEMBERS_FILE_NAME))
    }

    /// Loads `members.json` from `path`. If the file does not exist, returns an empty store.
    pub fn load_file(path: &Path) -> GridResult<Self> {
        match std::fs::read_to_string(path) {
            Ok(raw) => {
                check_members_file_permissions(path)?;
                Self::from_json_str(&raw, path)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::new()),
            Err(e) => Err(GridError::Config(format!(
                "failed to read members file {}: {e}",
                path.display()
            ))),
        }
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
        Self::try_from_entries(parsed.members, source_path)
    }

    /// Number of enrolled members in the store.
    pub fn len(&self) -> usize {
        self.by_pubkey.len()
    }

    /// Returns `true` if the store holds no members.
    pub fn is_empty(&self) -> bool {
        self.by_pubkey.is_empty()
    }

    /// Returns `true` if `pubkey` belongs to an enrolled member.
    pub fn contains(&self, pubkey: &PublicKey) -> bool {
        self.by_pubkey.contains_key(pubkey)
    }

    /// Looks up a member entry by public key.
    pub fn get(&self, pubkey: &PublicKey) -> Option<&MemberEntry> {
        self.by_pubkey.get(pubkey)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::noise_auth::NodeKeypair;

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
