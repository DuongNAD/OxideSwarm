//! Shared repository plan parser, validator, and compact team snapshot (design 0111, TEAM-1).
//!
//! Reads `.oxideswarm/plan.toml`, checks dependencies, cycles, overlapping `owned_paths`, and
//! profile placement tags (`oxideswarm plan check`), and renders a snapshot of at most 30 lines
//! combining the base plan with git lease refs `refs/oxideswarm/lease/<task-id>` (`oxideswarm
//! plan status` and MCP tool `plan_snapshot`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};

use rusty_grid_core::placement::{
    normalize_arch, normalize_os, validate_tool_name, validate_user_tag, IsolationLevel,
};

use crate::shard::HARD_MAX_SHARDS;

pub mod lease;

/// Default path of the shared team plan relative to the repository root.
pub const DEFAULT_PLAN_PATH: &str = ".oxideswarm/plan.toml";
/// Hard upper bound on rendered snapshot lines (design 0111, D2).
pub const MAX_SNAPSHOT_LINES: usize = 30;
/// Git ref prefix for task claims and path leases (design 0111, D3).
pub const LEASE_REF_PREFIX: &str = "refs/oxideswarm/lease/";
/// Maximum characters rendered per snapshot line to keep compact agent context bounded.
const MAX_SNAPSHOT_LINE_CHARS: usize = 160;
/// Maximum recent merges shown on the snapshot footer line.
const MAX_RECENT_MERGES: usize = 5;

const KNOWN_OS: &[&str] = &["linux", "macos", "windows", "android"];
const KNOWN_ARCH: &[&str] = &["x86_64", "aarch64", "x86", "arm"];

/// CLI arguments for `oxideswarm plan`.
#[derive(Args, Debug, Clone)]
pub struct PlanArgs {
    #[command(subcommand)]
    pub command: PlanSubcommand,
}

/// Subcommands under `oxideswarm plan`.
#[derive(Subcommand, Debug, Clone)]
pub enum PlanSubcommand {
    /// Validate `.oxideswarm/plan.toml` (dependencies, cycles, overlapping owned_paths, profile tags)
    Check {
        /// Path to the plan TOML file (default: `.oxideswarm/plan.toml`)
        #[arg(long, value_name = "PATH")]
        plan: Option<PathBuf>,

        /// Print machine-readable JSON diagnostics
        #[arg(long)]
        json: bool,
    },

    /// Print the compact (at most 30 lines) snapshot of the active wave and leases
    Status {
        /// Path to the plan TOML file (default: `.oxideswarm/plan.toml`)
        #[arg(long, value_name = "PATH")]
        plan: Option<PathBuf>,

        /// Git repository directory (default: current directory)
        #[arg(long, value_name = "DIR")]
        repo: Option<PathBuf>,

        /// Override base branch/ref name from `[project].base`
        #[arg(long, value_name = "REF")]
        base: Option<String>,

        /// Print structured JSON snapshot alongside line metrics
        #[arg(long)]
        json: bool,
    },

    /// Claim a task lease (`refs/oxideswarm/lease/<task-id>`) and create an isolated worktree
    Claim {
        /// Task id in `.oxideswarm/plan.toml`
        #[arg(value_name = "TASK_ID")]
        task_id: String,

        /// Claiming agent identifier stored in the lease record
        #[arg(long, value_name = "AGENT_ID")]
        holder: String,

        /// Optional human owner override for the lease record
        #[arg(long, value_name = "PERSON")]
        person: Option<String>,

        /// Git remote hosting `refs/oxideswarm/lease/*` (default: `origin`)
        #[arg(long, value_name = "REMOTE", default_value = lease::DEFAULT_REMOTE)]
        remote: String,

        /// Override base branch/ref name from `[project].base`
        #[arg(long, value_name = "REF")]
        base: Option<String>,

        /// Path to the plan TOML file (default: `.oxideswarm/plan.toml`)
        #[arg(long, value_name = "PATH")]
        plan: Option<PathBuf>,

        /// Git repository directory (default: current directory)
        #[arg(long, value_name = "DIR")]
        repo: Option<PathBuf>,

        /// Branch name to create for the worktree (default: `team/<task-id>`)
        #[arg(long, value_name = "BRANCH")]
        branch: Option<String>,

        /// Parent directory under which `wt-<task-id>` is created (default: parent of repo)
        #[arg(long, value_name = "DIR")]
        worktree_root: Option<PathBuf>,

        /// Claim the lease ref without creating a git worktree
        #[arg(long)]
        no_worktree: bool,

        /// Lease TTL in minutes (default: 45, max: 1440)
        #[arg(long, value_name = "MINS", default_value_t = lease::DEFAULT_LEASE_TTL_MINS)]
        ttl_minutes: i64,

        /// Print machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Renew an active task lease via a strictly fast-forward push
    Renew {
        /// Task id whose lease should be extended
        #[arg(value_name = "TASK_ID")]
        task_id: String,

        /// Holder agent identifier (must match the current lease holder)
        #[arg(long, value_name = "AGENT_ID")]
        holder: String,

        /// Git remote hosting `refs/oxideswarm/lease/*` (default: `origin`)
        #[arg(long, value_name = "REMOTE", default_value = lease::DEFAULT_REMOTE)]
        remote: String,

        /// Git repository directory (default: current directory)
        #[arg(long, value_name = "DIR")]
        repo: Option<PathBuf>,

        /// Expected current remote lease commit OID for compare-and-swap check
        #[arg(long, value_name = "OID")]
        expected_oid: Option<String>,

        /// Extended lease TTL in minutes from now (default: 45, max: 1440)
        #[arg(long, value_name = "MINS", default_value_t = lease::DEFAULT_LEASE_TTL_MINS)]
        ttl_minutes: i64,

        /// Print machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Release a task lease by deleting `refs/oxideswarm/lease/<task-id>` on the remote
    Release {
        /// Task id whose lease ref should be deleted
        #[arg(value_name = "TASK_ID")]
        task_id: String,

        /// Expected current lease holder (required unless `--force` is passed)
        #[arg(long, value_name = "AGENT_ID")]
        holder: Option<String>,

        /// Git remote hosting `refs/oxideswarm/lease/*` (default: `origin`)
        #[arg(long, value_name = "REMOTE", default_value = lease::DEFAULT_REMOTE)]
        remote: String,

        /// Git repository directory (default: current directory)
        #[arg(long, value_name = "DIR")]
        repo: Option<PathBuf>,

        /// Release even if `holder` is omitted or held by another agent
        #[arg(long)]
        force: bool,

        /// Print machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Verify that every path changed in `<range>` lies within the task's `owned_paths`
    CheckDiff {
        /// Git diff range such as `origin/master..HEAD`
        #[arg(value_name = "RANGE")]
        range: String,

        /// Task id in `.oxideswarm/plan.toml` (inferred from `team/<id>` branch if omitted)
        #[arg(long, value_name = "TASK_ID")]
        task: Option<String>,

        /// Path to the plan TOML file (default: `.oxideswarm/plan.toml`)
        #[arg(long, value_name = "PATH")]
        plan: Option<PathBuf>,

        /// Git repository directory (default: current directory)
        #[arg(long, value_name = "DIR")]
        repo: Option<PathBuf>,

        /// Override base branch/ref name when loading `.oxideswarm/plan.toml` from git
        #[arg(long, value_name = "REF")]
        base: Option<String>,

        /// Print machine-readable JSON diagnostics
        #[arg(long)]
        json: bool,
    },
}

/// Root document parsed from `.oxideswarm/plan.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanFile {
    pub project: ProjectConfig,
    #[serde(default, rename = "profile")]
    pub profiles: BTreeMap<String, VerifyProfile>,
    #[serde(default, rename = "task")]
    pub tasks: Vec<PlanTask>,
}

/// Repository-level settings in `[project]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    pub base: String,
    #[serde(default)]
    pub contracts: Vec<String>,
}

/// Named verification profile in `[profile.<name>]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyProfile {
    pub command: Vec<String>,
    #[serde(default)]
    pub matrix: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub require: Vec<String>,
    #[serde(default)]
    pub shards: Option<usize>,
}

/// Allowed persistent task statuses in `.oxideswarm/plan.toml` (`claimed` and `blocked` live in
/// git lease refs rather than in the plan file per design 0111 D1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Open,
    Review,
    Done,
    Dropped,
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Review => "review",
            Self::Done => "done",
            Self::Dropped => "dropped",
        }
    }

    pub fn is_active(self) -> bool {
        matches!(self, Self::Open | Self::Review)
    }
}

/// One work item in `[[task]]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanTask {
    pub id: String,
    pub title: String,
    pub status: TaskStatus,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub owned_paths: Vec<String>,
    #[serde(default)]
    pub verify: Vec<String>,
    #[serde(default)]
    pub hours: Option<u32>,
    #[serde(default)]
    pub person: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
}

/// Lease record stored as TOML in git ref commit `refs/oxideswarm/lease/<task-id>` (design 0111 D3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub task_id: String,
    pub holder: String,
    #[serde(default)]
    pub person: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub owned_paths: Vec<String>,
    pub expires_at: String,
    #[serde(default)]
    pub blocked: bool,
    #[serde(default)]
    pub verify_attempts: u32,
    #[serde(default)]
    pub verify_failures: u32,
    #[serde(default)]
    pub last_failure: Option<String>,
    #[serde(default)]
    pub last_fingerprint: Option<String>,
}

impl LeaseRecord {
    pub fn expires_at_utc(&self) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(&self.expires_at)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    }

    pub fn is_expired_at(&self, now_utc: DateTime<Utc>) -> bool {
        match self.expires_at_utc() {
            Some(exp) => exp <= now_utc,
            None => true,
        }
    }
}

/// Category of validation failure reported by [`validate_plan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanDiagnosticKind {
    InvalidProject,
    InvalidProfile,
    UnknownProfileTag,
    InvalidTask,
    DuplicateTaskId,
    UnknownDependency,
    UnknownProfile,
    DependencyCycle,
    InvalidOwnedPath,
    OverlappingOwnedPaths,
}

/// One structured validation error from `oxideswarm plan check`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDiagnostic {
    pub kind: PlanDiagnosticKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    pub message: String,
}

impl PlanDiagnostic {
    fn new(kind: PlanDiagnosticKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            task_id: None,
            profile: None,
            message: message.into(),
        }
    }

    fn for_task(
        kind: PlanDiagnosticKind,
        task_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            task_id: Some(task_id.into()),
            profile: None,
            message: message.into(),
        }
    }

    fn for_profile(
        kind: PlanDiagnosticKind,
        profile: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            task_id: None,
            profile: Some(profile.into()),
            message: message.into(),
        }
    }
}

/// Contract path and its short git object hash for the snapshot header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractStatus {
    pub path: String,
    pub short_sha: String,
}

/// Effective task entry included in [`PlanSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTaskEntry {
    pub id: String,
    pub state: String,
    pub touches_contract: bool,
    pub summary: String,
}

/// Structured output of `oxideswarm plan status` and MCP `plan_snapshot`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanSnapshot {
    pub text: String,
    pub line_count: usize,
    pub base: String,
    pub base_sha: String,
    pub contracts: Vec<ContractStatus>,
    pub tasks: Vec<SnapshotTaskEntry>,
    pub active_leases: Vec<LeaseRecord>,
    pub recent_merges: Vec<String>,
}

/// Parses `.oxideswarm/plan.toml` text into [`PlanFile`].
pub fn parse_plan(toml_str: &str) -> Result<PlanFile, String> {
    toml::from_str::<PlanFile>(toml_str).map_err(|e| format!("invalid plan TOML: {e}"))
}

/// Reads and parses a plan TOML file from disk.
pub fn load_plan(path: &Path) -> Result<PlanFile, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read plan file '{}': {e}", path.display()))?;
    parse_plan(&text)
}

/// Resolves the plan path from an optional override or `<repo_dir>/.oxideswarm/plan.toml`.
pub fn resolve_plan_path(repo_dir: &Path, explicit: Option<&Path>) -> PathBuf {
    match explicit {
        Some(p) if p.is_absolute() => p.to_path_buf(),
        Some(p) => repo_dir.join(p),
        None => repo_dir.join(DEFAULT_PLAN_PATH),
    }
}

/// Normalizes a repository-relative path or glob pattern and rejects empty, absolute, drive-letter,
/// NUL-containing, or `..`-escaping paths.
pub fn normalize_repo_path(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("path pattern cannot be empty".to_string());
    }
    if trimmed.contains('\0') {
        return Err(format!("path pattern '{raw}' contains NUL"));
    }
    let slash = trimmed.replace('\\', "/");
    if slash.starts_with('/') {
        return Err(format!(
            "path pattern '{raw}' must be repository-relative, not absolute"
        ));
    }
    let bytes = slash.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return Err(format!(
            "path pattern '{raw}' must not include a Windows drive prefix"
        ));
    }

    let mut segments = Vec::new();
    for seg in slash.split('/') {
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." {
            return Err(format!(
                "path pattern '{raw}' must not contain '..' parent traversal"
            ));
        }
        segments.push(seg);
    }
    if segments.is_empty() {
        return Err(format!(
            "path pattern '{raw}' resolves to an empty repository path"
        ));
    }
    Ok(segments.join("/"))
}

/// True when two single-segment glob patterns (`*` matching any chars, `?` matching one char) can
/// match at least one common non-empty segment string.
fn single_segments_overlap(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    let ac: Vec<char> = a.chars().collect();
    let bc: Vec<char> = b.chars().collect();
    let (n, m) = (ac.len(), bc.len());
    // dp[i][j] = there exists some string matching ac[..i] and bc[..j].
    let mut dp = vec![vec![false; m + 1]; n + 1];
    dp[0][0] = true;
    for i in 0..=n {
        for j in 0..=m {
            if !dp[i][j] {
                continue;
            }
            if i < n && ac[i] == '*' {
                dp[i + 1][j] = true;
                if j < m {
                    dp[i][j + 1] = true;
                }
            }
            if j < m && bc[j] == '*' {
                dp[i][j + 1] = true;
                if i < n {
                    dp[i + 1][j] = true;
                }
            }
            if i < n && j < m {
                let compat = ac[i] == bc[j] || ac[i] == '?' || bc[j] == '?';
                if compat {
                    dp[i + 1][j + 1] = true;
                }
            }
        }
    }
    dp[n][m]
}

fn segment_has_wildcard(seg: &str) -> bool {
    seg.contains('*') || seg.contains('?')
}

fn segment_sequences_overlap(a: &[&str], b: &[&str]) -> bool {
    let (n, m) = (a.len(), b.len());
    let mut dp = vec![vec![false; m + 1]; n + 1];
    dp[0][0] = true;

    for i in 0..=n {
        for j in 0..=m {
            if !dp[i][j] {
                continue;
            }
            if i < n && a[i] == "**" {
                dp[i + 1][j] = true;
                if j < m {
                    dp[i][j + 1] = true;
                }
            }
            if j < m && b[j] == "**" {
                dp[i][j + 1] = true;
                if i < n {
                    dp[i + 1][j] = true;
                }
            }
            if i < n && j < m && a[i] != "**" && b[j] != "**" && single_segments_overlap(a[i], b[j])
            {
                dp[i + 1][j + 1] = true;
            }
        }
    }

    if dp[n][m] {
        return true;
    }

    // Prefix-based conservative rule (design 0111 D3.1): a literal path (or literal directory
    // prefix with no glob wildcards) overlaps any path below it (`a` overlaps `a/b/c.rs` and
    // `a/**`). Filename globs such as `crates/*.rs` remain single-level unless `**` is used.
    let prefix_covers = |prefix: &[&str], other: &[&str]| -> bool {
        if prefix.is_empty()
            || prefix.len() > other.len()
            || prefix.iter().any(|s| segment_has_wildcard(s))
        {
            return false;
        }
        prefix
            .iter()
            .zip(other.iter())
            .all(|(p, o)| *o == "**" || single_segments_overlap(p, o))
    };

    prefix_covers(a, b) || prefix_covers(b, a)
}

/// Returns `true` when two repository path/glob patterns may overlap (prefix-based and
/// conservative per design 0111 D3.1: `a/**` or directory prefix `a` overlaps `a/b.rs`).
pub fn paths_overlap(a: &str, b: &str) -> bool {
    let Ok(na) = normalize_repo_path(a) else {
        return false;
    };
    let Ok(nb) = normalize_repo_path(b) else {
        return false;
    };
    let sa: Vec<&str> = na.split('/').collect();
    let sb: Vec<&str> = nb.split('/').collect();
    segment_sequences_overlap(&sa, &sb)
}

/// Returns `true` when a concrete repository path `path` matches `pattern`.
pub fn path_matches_pattern(path: &str, pattern: &str) -> bool {
    let Ok(np) = normalize_repo_path(path) else {
        return false;
    };
    let Ok(npat) = normalize_repo_path(pattern) else {
        return false;
    };
    let sp: Vec<&str> = np.split('/').collect();
    let spat: Vec<&str> = npat.split('/').collect();
    if sp.iter().any(|s| segment_has_wildcard(s)) {
        return false;
    }
    segment_sequences_overlap(&sp, &spat)
}

pub(crate) fn validate_task_id(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > 64 {
        return Err(format!(
            "task id '{id}' must be between 1 and 64 ASCII characters"
        ));
    }
    let mut chars = id.chars();
    let first = chars.next().expect("non-empty checked");
    if !first.is_ascii_alphanumeric() {
        return Err(format!(
            "task id '{id}' must start with an ASCII letter or digit"
        ));
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')) {
        return Err(format!("task id '{id}' may contain only [A-Za-z0-9._-]"));
    }
    Ok(())
}

fn validate_profile_require_tag(raw: &str) -> Result<(), String> {
    let tag = raw.trim();
    if tag.is_empty() {
        return Err("profile require entry cannot be empty".to_string());
    }
    if let Some((prefix, val)) = tag.split_once(':') {
        if val.is_empty() {
            return Err(format!("profile tag '{tag}' has an empty value"));
        }
        match prefix {
            "os" => {
                let norm = normalize_os(val);
                if !KNOWN_OS.contains(&norm.as_str()) {
                    return Err(format!(
                        "profile tag '{tag}': unknown os '{val}' (expected one of: {})",
                        KNOWN_OS.join(", ")
                    ));
                }
            }
            "arch" => {
                let norm = normalize_arch(val);
                if !KNOWN_ARCH.contains(&norm.as_str()) {
                    return Err(format!(
                        "profile tag '{tag}': unknown arch '{val}' (expected one of: {})",
                        KNOWN_ARCH.join(", ")
                    ));
                }
            }
            "iso" => {
                IsolationLevel::parse_required(val)
                    .map_err(|e| format!("profile tag '{tag}': {e}"))?;
            }
            "tool" => {
                validate_tool_name(val).map_err(|e| format!("profile tag '{tag}': {e}"))?;
            }
            "tag" => {
                validate_user_tag(val).map_err(|e| format!("profile tag '{tag}': {e}"))?;
            }
            "worker" => {
                if val.chars().any(|c| c.is_whitespace() || c.is_control()) {
                    return Err(format!(
                        "profile tag '{tag}': worker value must not contain whitespace or control characters"
                    ));
                }
            }
            "feat" => {
                if val != "workspaces" {
                    return Err(format!(
                        "profile tag '{tag}': unknown feature tag '{val}' (known: feat:workspaces)"
                    ));
                }
            }
            "opt" => {
                if val != "repeatable" {
                    return Err(format!(
                        "profile tag '{tag}': unknown opt tag '{val}' (known: opt:repeatable)"
                    ));
                }
            }
            other => {
                return Err(format!(
                    "unknown profile tag prefix '{other}:' in '{tag}' (known: os, arch, iso, tool, tag, worker, feat, opt, or bare user tag)"
                ));
            }
        }
        Ok(())
    } else {
        validate_user_tag(tag).map_err(|e| format!("profile tag '{tag}': {e}"))
    }
}

fn validate_matrix_entry(key: &str, values: &[String]) -> Result<(), String> {
    if values.is_empty() {
        return Err(format!(
            "matrix axis '{key}' must contain at least one value"
        ));
    }
    let mut seen = HashSet::new();
    for val in values {
        if val.contains(',') || val.contains('=') || val.contains('\0') {
            return Err(format!(
                "matrix axis '{key}' value '{val}' must not contain ',', '=', or NUL"
            ));
        }
        if let Some(var) = key.strip_prefix("env:") {
            if var.is_empty()
                || var.trim() != var
                || var
                    .chars()
                    .any(|c| c.is_whitespace() || c == '=' || c == ',' || c == '\0')
            {
                return Err(format!(
                    "invalid matrix env axis '{key}' (expected env:<VAR>)"
                ));
            }
            if val.is_empty() {
                return Err(format!("matrix axis '{key}' has an empty value"));
            }
            if !seen.insert(val.clone()) {
                return Err(format!("duplicate value '{val}' in matrix axis '{key}'"));
            }
            continue;
        }
        let norm_val = match key {
            "os" => {
                let norm = normalize_os(val.trim());
                if !KNOWN_OS.contains(&norm.as_str()) {
                    return Err(format!("matrix axis 'os': unknown os '{val}'"));
                }
                norm
            }
            "arch" => {
                let norm = normalize_arch(val.trim());
                if !KNOWN_ARCH.contains(&norm.as_str()) {
                    return Err(format!("matrix axis 'arch': unknown arch '{val}'"));
                }
                norm
            }
            "iso" => IsolationLevel::parse_required(val.trim())
                .map(|l| l.to_string())
                .map_err(|e| format!("matrix axis 'iso': {e}"))?,
            "tool" => {
                validate_tool_name(val.trim()).map_err(|e| format!("matrix axis 'tool': {e}"))?
            }
            "tag" => {
                let t = val.trim();
                validate_user_tag(t).map_err(|e| format!("matrix axis 'tag': {e}"))?;
                t.to_string()
            }
            "worker" => {
                let t = val.trim();
                if t.is_empty() || t.chars().any(|c| c.is_whitespace() || c.is_control()) {
                    return Err(format!(
                        "matrix axis 'worker' contains invalid value '{val}'"
                    ));
                }
                t.to_ascii_lowercase()
            }
            other => {
                return Err(format!(
                    "unknown matrix axis '{other}' (supported: os, arch, worker, iso, tool, tag, env:<VAR>)"
                ));
            }
        };
        if !seen.insert(norm_val) {
            return Err(format!("duplicate value '{val}' in matrix axis '{key}'"));
        }
    }
    Ok(())
}

fn validate_profiles(plan: &PlanFile, diags: &mut Vec<PlanDiagnostic>) {
    for (name, profile) in &plan.profiles {
        if name.trim().is_empty() {
            diags.push(PlanDiagnostic::new(
                PlanDiagnosticKind::InvalidProfile,
                "profile name cannot be empty",
            ));
        }
        if profile.command.is_empty() || profile.command[0].trim().is_empty() {
            diags.push(PlanDiagnostic::for_profile(
                PlanDiagnosticKind::InvalidProfile,
                name,
                format!("profile '{name}' must have a non-empty command"),
            ));
        }
        if let Some(shards) = profile.shards {
            if shards == 0 || shards > HARD_MAX_SHARDS {
                diags.push(PlanDiagnostic::for_profile(
                    PlanDiagnosticKind::InvalidProfile,
                    name,
                    format!(
                        "profile '{name}' shards ({shards}) must be between 1 and {HARD_MAX_SHARDS}"
                    ),
                ));
            }
        }
        for tag in &profile.require {
            if let Err(msg) = validate_profile_require_tag(tag) {
                diags.push(PlanDiagnostic::for_profile(
                    PlanDiagnosticKind::UnknownProfileTag,
                    name,
                    format!("profile '{name}': {msg}"),
                ));
            }
        }
        for (axis, vals) in &profile.matrix {
            if let Err(msg) = validate_matrix_entry(axis, vals) {
                diags.push(PlanDiagnostic::for_profile(
                    PlanDiagnosticKind::UnknownProfileTag,
                    name,
                    format!("profile '{name}': {msg}"),
                ));
            }
        }
    }
}

fn detect_dependency_cycles(
    tasks: &[PlanTask],
    task_index: &HashMap<&str, usize>,
    diags: &mut Vec<PlanDiagnostic>,
) {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum State {
        Unvisited,
        Visiting,
        Visited,
    }

    fn dfs(
        idx: usize,
        tasks: &[PlanTask],
        task_index: &HashMap<&str, usize>,
        state: &mut [State],
        stack: &mut Vec<usize>,
        reported_cycles: &mut HashSet<Vec<String>>,
        diags: &mut Vec<PlanDiagnostic>,
    ) {
        state[idx] = State::Visiting;
        stack.push(idx);
        for dep in &tasks[idx].depends_on {
            let Some(&dep_idx) = task_index.get(dep.as_str()) else {
                continue;
            };
            match state[dep_idx] {
                State::Unvisited => {
                    dfs(
                        dep_idx,
                        tasks,
                        task_index,
                        state,
                        stack,
                        reported_cycles,
                        diags,
                    );
                }
                State::Visiting => {
                    if let Some(pos) = stack.iter().position(|&s| s == dep_idx) {
                        let mut cycle_ids: Vec<String> =
                            stack[pos..].iter().map(|&i| tasks[i].id.clone()).collect();
                        let mut canonical = cycle_ids.clone();
                        canonical.sort();
                        if reported_cycles.insert(canonical) {
                            cycle_ids.push(tasks[dep_idx].id.clone());
                            diags.push(PlanDiagnostic::for_task(
                                PlanDiagnosticKind::DependencyCycle,
                                &tasks[idx].id,
                                format!("dependency cycle: {}", cycle_ids.join(" -> ")),
                            ));
                        }
                    }
                }
                State::Visited => {}
            }
        }
        stack.pop();
        state[idx] = State::Visited;
    }

    let mut state = vec![State::Unvisited; tasks.len()];
    let mut stack = Vec::new();
    let mut reported_cycles = HashSet::new();
    for i in 0..tasks.len() {
        if state[i] == State::Unvisited {
            dfs(
                i,
                tasks,
                task_index,
                &mut state,
                &mut stack,
                &mut reported_cycles,
                diags,
            );
        }
    }
}

/// Computes `reach[i][j] = true` when task `i` transitively depends on task `j`.
fn dependency_reachability(
    tasks: &[PlanTask],
    task_index: &HashMap<&str, usize>,
) -> Vec<Vec<bool>> {
    let n = tasks.len();
    let mut reach = vec![vec![false; n]; n];
    for (i, t) in tasks.iter().enumerate() {
        let mut stack: Vec<usize> = t
            .depends_on
            .iter()
            .filter_map(|d| task_index.get(d.as_str()).copied())
            .collect();
        while let Some(u) = stack.pop() {
            if !reach[i][u] {
                reach[i][u] = true;
                for d in &tasks[u].depends_on {
                    if let Some(&v) = task_index.get(d.as_str()) {
                        if !reach[i][v] {
                            stack.push(v);
                        }
                    }
                }
            }
        }
    }
    reach
}

/// Validates a parsed [`PlanFile`] and returns every diagnostic found (`oxideswarm plan check`).
pub fn validate_plan(plan: &PlanFile) -> Vec<PlanDiagnostic> {
    let mut diags = Vec::new();

    let base = plan.project.base.trim();
    if base.is_empty() || base.chars().any(|c| c.is_whitespace() || c.is_control()) {
        diags.push(PlanDiagnostic::new(
            PlanDiagnosticKind::InvalidProject,
            format!(
                "[project].base ('{}') must be a non-empty git branch/ref name without whitespace",
                plan.project.base
            ),
        ));
    }
    for contract in &plan.project.contracts {
        if let Err(msg) = normalize_repo_path(contract) {
            diags.push(PlanDiagnostic::new(
                PlanDiagnosticKind::InvalidProject,
                format!("[project].contracts: {msg}"),
            ));
        }
    }

    validate_profiles(plan, &mut diags);

    let mut task_index: HashMap<&str, usize> = HashMap::new();
    for (idx, task) in plan.tasks.iter().enumerate() {
        if let Err(msg) = validate_task_id(&task.id) {
            diags.push(PlanDiagnostic::for_task(
                PlanDiagnosticKind::InvalidTask,
                &task.id,
                msg,
            ));
        }
        if let Some(prev_idx) = task_index.insert(task.id.as_str(), idx) {
            diags.push(PlanDiagnostic::for_task(
                PlanDiagnosticKind::DuplicateTaskId,
                &task.id,
                format!(
                    "duplicate task id '{}' (first defined at task index {prev_idx})",
                    task.id
                ),
            ));
        }
        if task.title.trim().is_empty() {
            diags.push(PlanDiagnostic::for_task(
                PlanDiagnosticKind::InvalidTask,
                &task.id,
                format!("task '{}' title cannot be empty", task.id),
            ));
        }
        if matches!(task.hours, Some(0)) {
            diags.push(PlanDiagnostic::for_task(
                PlanDiagnosticKind::InvalidTask,
                &task.id,
                format!("task '{}' hours must be at least 1", task.id),
            ));
        }
        for profile_name in &task.verify {
            if !plan.profiles.contains_key(profile_name) {
                diags.push(PlanDiagnostic::for_task(
                    PlanDiagnosticKind::UnknownProfile,
                    &task.id,
                    format!(
                        "task '{}' references unknown verify profile '{profile_name}'",
                        task.id
                    ),
                ));
            }
        }
        for path in &task.owned_paths {
            if let Err(msg) = normalize_repo_path(path) {
                diags.push(PlanDiagnostic::for_task(
                    PlanDiagnosticKind::InvalidOwnedPath,
                    &task.id,
                    format!("task '{}' owned_paths: {msg}", task.id),
                ));
            }
        }
    }

    for task in &plan.tasks {
        for dep in &task.depends_on {
            if !task_index.contains_key(dep.as_str()) {
                diags.push(PlanDiagnostic::for_task(
                    PlanDiagnosticKind::UnknownDependency,
                    &task.id,
                    format!("task '{}' depends on unknown task '{dep}'", task.id),
                ));
            }
        }
    }

    detect_dependency_cycles(&plan.tasks, &task_index, &mut diags);

    // Check overlapping owned_paths between concurrent active (`open` / `review`) tasks. Tasks
    // ordered by `depends_on` cannot be claimed at the same time (design 0111 G3), so a
    // dependency-ordered pair is permitted to touch the same integration file.
    let reach = dependency_reachability(&plan.tasks, &task_index);
    for (i, a) in plan.tasks.iter().enumerate() {
        if !a.status.is_active() {
            continue;
        }
        for (j, b) in plan.tasks.iter().enumerate().skip(i + 1) {
            if !b.status.is_active() {
                continue;
            }
            if reach[i][j] || reach[j][i] {
                continue;
            }
            for pa in &a.owned_paths {
                for pb in &b.owned_paths {
                    if paths_overlap(pa, pb) {
                        diags.push(PlanDiagnostic::for_task(
                            PlanDiagnosticKind::OverlappingOwnedPaths,
                            &a.id,
                            format!(
                                "open tasks '{}' ({pa}) and '{}' ({pb}) have overlapping owned_paths without a dependency edge",
                                a.id, b.id
                            ),
                        ));
                    }
                }
            }
        }
    }

    diags
}

fn run_git(repo_dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(repo_dir)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).into_owned())
}

pub(crate) fn resolve_git_base_ref(repo_dir: &Path, base: &str) -> Option<String> {
    [
        base.to_string(),
        format!("origin/{base}"),
        "HEAD".to_string(),
    ]
    .into_iter()
    .find(|candidate| run_git(repo_dir, &["rev-parse", "--verify", "--quiet", candidate]).is_some())
}

pub(crate) fn load_plan_for_snapshot(
    repo_dir: &Path,
    plan_path: Option<&Path>,
    base_ref: Option<&str>,
) -> Result<PlanFile, String> {
    let path = resolve_plan_path(repo_dir, plan_path);
    if path.exists() {
        return load_plan(&path);
    }
    if plan_path.is_none() {
        if let Some(git_ref) = base_ref {
            let spec = format!("{git_ref}:{DEFAULT_PLAN_PATH}");
            if let Some(text) = run_git(repo_dir, &["show", &spec]) {
                return parse_plan(&text);
            }
        }
    }
    Err(format!(
        "cannot find plan file at '{}'{}",
        path.display(),
        base_ref
            .map(|r| format!(" or '{r}:{DEFAULT_PLAN_PATH}'"))
            .unwrap_or_default()
    ))
}

fn read_lease_refs(repo_dir: &Path) -> BTreeMap<String, LeaseRecord> {
    let mut leases = BTreeMap::new();
    // Separate commits with RS (0x1e) and refname/body with US (0x1f) so multiline bodies parse intact.
    let fmt = "%(refname)%1f%(contents)%1e";
    let Some(raw) = run_git(
        repo_dir,
        &["for-each-ref", "--format", fmt, LEASE_REF_PREFIX],
    ) else {
        return leases;
    };
    for rec in raw.split('\x1e') {
        let trimmed = rec.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((refname, body)) = rec.split_once('\x1f') else {
            continue;
        };
        let Some(ref_task_id) = refname.trim().strip_prefix(LEASE_REF_PREFIX) else {
            continue;
        };
        if let Ok(mut lease) = toml::from_str::<LeaseRecord>(body) {
            if lease.task_id.is_empty() {
                lease.task_id = ref_task_id.to_string();
            }
            leases.insert(ref_task_id.to_string(), lease);
        }
    }
    leases
}

/// Strips newlines and control characters and caps width so one task never consumes multiple lines.
fn sanitize_inline(raw: &str, max_chars: usize) -> String {
    let mut cleaned = String::new();
    let mut prev_space = false;
    for ch in raw.chars() {
        let mapped = if ch.is_control() { ' ' } else { ch };
        if mapped.is_whitespace() {
            if !prev_space && !cleaned.is_empty() {
                cleaned.push(' ');
            }
            prev_space = true;
        } else {
            cleaned.push(mapped);
            prev_space = false;
        }
    }
    let trimmed = cleaned.trim();
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }
    let mut out: String = trimmed.chars().take(max_chars.saturating_sub(3)).collect();
    out.push_str("...");
    out
}

fn truncate_line(line: String, max_chars: usize) -> String {
    if line.chars().count() <= max_chars {
        return line;
    }
    let mut out: String = line.chars().take(max_chars.saturating_sub(3)).collect();
    out.push_str("...");
    out
}

fn contract_display_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn format_lease_until(lease: &LeaseRecord) -> String {
    match lease.expires_at_utc() {
        Some(dt) => dt.format("%H:%MZ").to_string(),
        None => sanitize_inline(&lease.expires_at, 20),
    }
}

fn render_snapshot_lines(
    base: &str,
    base_sha: &str,
    contracts: &[ContractStatus],
    entries: &[SnapshotTaskEntry],
    recent_merges: &[String],
) -> Vec<String> {
    let mut header = format!(
        "base {}@{}",
        sanitize_inline(base, 40),
        sanitize_inline(base_sha, 16)
    );
    if !contracts.is_empty() {
        let mut parts = Vec::with_capacity(contracts.len());
        for c in contracts {
            parts.push(format!(
                "{}@{}",
                sanitize_inline(contract_display_name(&c.path), 32),
                sanitize_inline(&c.short_sha, 12)
            ));
        }
        header.push_str("  contracts: ");
        header.push_str(&parts.join("  "));
    }

    let footer = if recent_merges.is_empty() {
        None
    } else {
        let formatted: Vec<String> = recent_merges
            .iter()
            .take(MAX_RECENT_MERGES)
            .map(|m| sanitize_inline(m, 48))
            .collect();
        Some(format!("recent merges: {}", formatted.join(", ")))
    };

    let reserve_footer = usize::from(footer.is_some());
    let task_budget = MAX_SNAPSHOT_LINES.saturating_sub(1 + reserve_footer);

    let priority = |state: &str| -> u8 {
        match state {
            "blocked" => 0,
            "claimed" => 1,
            "review" => 2,
            "open" => 3,
            "done" => 4,
            _ => 5,
        }
    };

    let mut ordered: Vec<(usize, &SnapshotTaskEntry)> = entries.iter().enumerate().collect();
    ordered.sort_by_key(|(orig_idx, entry)| (priority(&entry.state), *orig_idx));

    let id_width = entries
        .iter()
        .map(|e| e.id.len())
        .max()
        .unwrap_or(4)
        .clamp(4, 16);

    let format_row = |e: &SnapshotTaskEntry| -> String {
        let id = sanitize_inline(&e.id, 24);
        let state = sanitize_inline(&e.state, 10);
        let contract_tag = if e.touches_contract && e.state != "done" && e.state != "dropped" {
            "[contract] "
        } else {
            ""
        };
        let line = format!(
            "{id:<id_width$}  {state:<7}  {contract_tag}{}",
            sanitize_inline(&e.summary, 110)
        );
        truncate_line(line, MAX_SNAPSHOT_LINE_CHARS)
    };

    let mut lines = Vec::with_capacity(MAX_SNAPSHOT_LINES);
    lines.push(truncate_line(header, MAX_SNAPSHOT_LINE_CHARS));

    if ordered.len() <= task_budget {
        for (_, e) in ordered {
            lines.push(format_row(e));
        }
    } else if task_budget > 0 {
        let shown = task_budget - 1;
        for (_, e) in ordered.iter().take(shown) {
            lines.push(format_row(e));
        }
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for (_, e) in ordered.iter().skip(shown) {
            *counts.entry(e.state.as_str()).or_insert(0) += 1;
        }
        let breakdown: Vec<String> = counts
            .into_iter()
            .map(|(k, v)| format!("{v} {k}"))
            .collect();
        let hidden = ordered.len() - shown;
        lines.push(truncate_line(
            format!("... +{hidden} more ({})", breakdown.join(", ")),
            MAX_SNAPSHOT_LINE_CHARS,
        ));
    }

    if let Some(f) = footer {
        if lines.len() < MAX_SNAPSHOT_LINES {
            lines.push(truncate_line(f, MAX_SNAPSHOT_LINE_CHARS));
        }
    }

    debug_assert!(lines.len() <= MAX_SNAPSHOT_LINES);
    lines
}

/// Builds a compact [`PlanSnapshot`] (at most 30 lines) from `.oxideswarm/plan.toml`, git refs
/// (`refs/oxideswarm/lease/<task>`), contract blob hashes, and recent base commits.
pub fn build_snapshot(
    repo_dir: &Path,
    plan_path: Option<&Path>,
    base_override: Option<&str>,
    now_utc: DateTime<Utc>,
) -> Result<PlanSnapshot, String> {
    let candidate_base = base_override.unwrap_or("HEAD");
    let resolved_ref = resolve_git_base_ref(repo_dir, candidate_base);
    let plan = load_plan_for_snapshot(repo_dir, plan_path, resolved_ref.as_deref())?;

    let base_name = base_override
        .unwrap_or(plan.project.base.as_str())
        .to_string();
    let effective_ref = resolve_git_base_ref(repo_dir, &base_name);
    let base_sha = effective_ref
        .as_deref()
        .and_then(|r| run_git(repo_dir, &["rev-parse", "--short=8", r]))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    let mut contracts = Vec::with_capacity(plan.project.contracts.len());
    for cpath in &plan.project.contracts {
        let normalized = normalize_repo_path(cpath).unwrap_or_else(|_| cpath.clone());
        let sha_from_ref = effective_ref
            .as_deref()
            .and_then(|r| {
                let spec = format!("{r}:{normalized}");
                run_git(repo_dir, &["rev-parse", "--short=4", &spec])
            })
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let short_sha = sha_from_ref
            .or_else(|| {
                let full = repo_dir.join(&normalized);
                if full.is_file() {
                    run_git(repo_dir, &["hash-object", full.to_string_lossy().as_ref()])
                        .map(|h| h.trim().chars().take(4).collect())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "----".to_string());
        contracts.push(ContractStatus {
            path: normalized,
            short_sha,
        });
    }

    let raw_leases = read_lease_refs(repo_dir);
    let active_leases: Vec<LeaseRecord> = raw_leases
        .values()
        .filter(|l| l.blocked || !l.is_expired_at(now_utc))
        .cloned()
        .collect();

    let status_by_id: HashMap<&str, TaskStatus> = plan
        .tasks
        .iter()
        .map(|t| (t.id.as_str(), t.status))
        .collect();

    let mut entries = Vec::with_capacity(plan.tasks.len());
    for task in &plan.tasks {
        let touches_contract = task.owned_paths.iter().any(|op| {
            plan.project
                .contracts
                .iter()
                .any(|cp| paths_overlap(op, cp))
        });
        let lease = raw_leases
            .get(&task.id)
            .filter(|l| l.blocked || !l.is_expired_at(now_utc));

        let (state, summary) = if let Some(l) = lease {
            if l.blocked {
                let who = l
                    .person
                    .as_deref()
                    .or(task.person.as_deref())
                    .map(|p| format!("  -> person {p}"))
                    .unwrap_or_default();
                let fail_note = l
                    .last_failure
                    .as_deref()
                    .map(|f| format!(": {f}"))
                    .unwrap_or_default();
                (
                    "blocked".to_string(),
                    format!(
                        "{} ({} x same failure){who}{fail_note}",
                        l.holder,
                        l.verify_failures.max(1)
                    ),
                )
            } else {
                let until = format_lease_until(l);
                let verify_part = if l.verify_attempts > 0 || l.last_failure.is_some() {
                    let f = l
                        .last_failure
                        .as_deref()
                        .map(|m| format!(" fail: {m}"))
                        .unwrap_or_default();
                    format!("  verify {}/{}{f}", l.verify_failures, l.verify_attempts)
                } else {
                    String::new()
                };
                (
                    "claimed".to_string(),
                    format!("{} (lease until {until}){verify_part}", l.holder),
                )
            }
        } else {
            match task.status {
                TaskStatus::Open => {
                    let unmet: Vec<&str> = task
                        .depends_on
                        .iter()
                        .map(String::as_str)
                        .filter(|dep| status_by_id.get(dep) != Some(&TaskStatus::Done))
                        .collect();
                    if !unmet.is_empty() {
                        ("open".to_string(), format!("needs {}", unmet.join(", ")))
                    } else {
                        ("open".to_string(), task.title.clone())
                    }
                }
                TaskStatus::Review | TaskStatus::Done | TaskStatus::Dropped => {
                    (task.status.as_str().to_string(), task.title.clone())
                }
            }
        };

        entries.push(SnapshotTaskEntry {
            id: task.id.clone(),
            state,
            touches_contract,
            summary,
        });
    }

    let recent_merges: Vec<String> = effective_ref
        .as_deref()
        .and_then(|r| run_git(repo_dir, &["log", "-n", "5", "--format=%h %s", r]))
        .map(|out| {
            out.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default();

    let lines = render_snapshot_lines(&base_name, &base_sha, &contracts, &entries, &recent_merges);
    let line_count = lines.len();
    let text = lines.join("\n");

    Ok(PlanSnapshot {
        text,
        line_count,
        base: base_name,
        base_sha,
        contracts,
        tasks: entries,
        active_leases,
        recent_merges,
    })
}

/// Entrypoint for `oxideswarm plan ...`.
pub fn run_plan_command(args: PlanArgs) -> ExitCode {
    match args.command {
        PlanSubcommand::Check { plan, json } => run_plan_check_cli(plan.as_deref(), json),
        PlanSubcommand::Status {
            plan,
            repo,
            base,
            json,
        } => run_plan_status_cli(plan.as_deref(), repo.as_deref(), base.as_deref(), json),
        PlanSubcommand::Claim {
            task_id,
            holder,
            person,
            remote,
            base,
            plan,
            repo,
            branch,
            worktree_root,
            no_worktree,
            ttl_minutes,
            json,
        } => {
            let opts = lease::ClaimOptions {
                task_id,
                holder,
                person,
                remote,
                base,
                plan_path: plan,
                branch,
                worktree_root,
                create_worktree: !no_worktree,
                ttl_minutes,
            };
            lease::run_plan_claim_cli(&opts, repo.as_deref(), json)
        }
        PlanSubcommand::Renew {
            task_id,
            holder,
            remote,
            repo,
            expected_oid,
            ttl_minutes,
            json,
        } => {
            let opts = lease::RenewOptions {
                task_id,
                holder,
                remote,
                expected_oid,
                ttl_minutes,
                blocked: None,
                verify_attempts: None,
                verify_failures: None,
                last_failure: None,
                last_fingerprint: None,
            };
            lease::run_plan_renew_cli(&opts, repo.as_deref(), json)
        }
        PlanSubcommand::Release {
            task_id,
            holder,
            remote,
            repo,
            force,
            json,
        } => {
            let opts = lease::ReleaseOptions {
                task_id,
                holder,
                remote,
                force,
            };
            lease::run_plan_release_cli(&opts, repo.as_deref(), json)
        }
        PlanSubcommand::CheckDiff {
            range,
            task,
            plan,
            repo,
            base,
            json,
        } => lease::run_plan_check_diff_cli(
            &range,
            task.as_deref(),
            plan.as_deref(),
            repo.as_deref(),
            base.as_deref(),
            json,
        ),
    }
}

fn run_plan_check_cli(plan_override: Option<&Path>, json: bool) -> ExitCode {
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: cannot read current directory: {e}");
            return ExitCode::FAILURE;
        }
    };
    let path = resolve_plan_path(&cwd, plan_override);
    let plan = match load_plan(&path) {
        Ok(p) => p,
        Err(e) => {
            if json {
                let body = serde_json::json!({
                    "ok": false,
                    "path": path.display().to_string(),
                    "diagnostics": [{ "kind": "invalid_project", "message": e }],
                });
                println!(
                    "{}",
                    serde_json::to_string_pretty(&body).unwrap_or_default()
                );
            } else {
                eprintln!("error: {e}");
            }
            return ExitCode::FAILURE;
        }
    };

    let diags = validate_plan(&plan);
    let ok = diags.is_empty();
    if json {
        let body = serde_json::json!({
            "ok": ok,
            "path": path.display().to_string(),
            "tasks": plan.tasks.len(),
            "profiles": plan.profiles.len(),
            "diagnostics": diags,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );
    } else if ok {
        println!(
            "plan check OK: {} task(s), {} profile(s) ({})",
            plan.tasks.len(),
            plan.profiles.len(),
            path.display()
        );
    } else {
        for d in &diags {
            eprintln!("error: {}", d.message);
        }
        eprintln!("plan check FAILED: {} issue(s)", diags.len());
    }
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn run_plan_status_cli(
    plan_override: Option<&Path>,
    repo_override: Option<&Path>,
    base_override: Option<&str>,
    json: bool,
) -> ExitCode {
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: cannot read current directory: {e}");
            return ExitCode::FAILURE;
        }
    };
    let repo_dir = repo_override
        .map(|p| {
            if p.is_absolute() {
                p.to_path_buf()
            } else {
                cwd.join(p)
            }
        })
        .unwrap_or(cwd);

    match build_snapshot(&repo_dir, plan_override, base_override, Utc::now()) {
        Ok(snapshot) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&snapshot).unwrap_or_default()
                );
            } else {
                println!("{}", snapshot.text);
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample_valid_plan() -> &'static str {
        r#"
[project]
base = "master"
contracts = ["crates/core/src/protocol.rs", "docs/reference.md"]

[profile.unit]
command = ["cargo", "test", "--locked", "--lib"]
matrix = { os = ["macos", "windows", "linux"] }
require = ["tool:cargo"]

[profile.lint]
command = ["cargo", "clippy", "--workspace", "--all-targets", "--locked", "--", "-D", "warnings"]
require = ["iso:fs", "fast"]
shards = 4

[[task]]
id = "T-9"
title = "Expose status metrics cleanly"
status = "done"
owned_paths = ["crates/master/src/metrics.rs"]
verify = ["unit"]
hours = 1
person = "owner"

[[task]]
id = "T-12"
title = "Count cancelled tasks apart from failed ones"
status = "open"
depends_on = ["T-9"]
owned_paths = ["crates/master/src/queue.rs", "crates/cli/src/cli_app/tasks.rs"]
verify = ["unit"]
hours = 2
person = "owner"
notes = "Keep JSON field names of design 0040."

[[task]]
id = "T-13"
title = "Document cancel count fields"
status = "open"
depends_on = ["T-12"]
owned_paths = ["docs/reference.md"]
verify = ["lint"]
hours = 1
"#
    }

    #[test]
    fn t1_parses_and_validates_clean_plan() {
        let plan = parse_plan(sample_valid_plan()).expect("valid plan parses");
        assert_eq!(plan.project.base, "master");
        assert_eq!(plan.profiles.len(), 2);
        assert_eq!(plan.tasks.len(), 3);
        let diags = validate_plan(&plan);
        assert!(diags.is_empty(), "unexpected diagnostics: {diags:?}");
    }

    #[test]
    fn t1_rejects_lease_only_statuses_and_unknown_fields() {
        for bad_status in ["claimed", "blocked"] {
            let src = format!(
                r#"
[project]
base = "master"

[[task]]
id = "T-1"
title = "Bad status"
status = "{bad_status}"
"#
            );
            assert!(
                parse_plan(&src).is_err(),
                "status '{bad_status}' must belong only to git lease refs"
            );
        }

        let unknown_field = r#"
[project]
base = "master"
extra = true
"#;
        assert!(parse_plan(unknown_field).is_err());
    }

    #[test]
    fn t1_detects_unknown_dependencies_cycles_and_unknown_profiles() {
        let src = r#"
[project]
base = "master"

[profile.unit]
command = ["cargo", "test"]

[[task]]
id = "A"
title = "Task A"
status = "open"
depends_on = ["C", "MISSING"]
verify = ["unit", "missing_profile"]

[[task]]
id = "B"
title = "Task B"
status = "open"
depends_on = ["A"]

[[task]]
id = "C"
title = "Task C"
status = "open"
depends_on = ["B"]

[[task]]
id = "SELF"
title = "Self cycle"
status = "open"
depends_on = ["SELF"]
"#;
        let plan = parse_plan(src).unwrap();
        let diags = validate_plan(&plan);
        assert!(
            diags
                .iter()
                .any(|d| d.kind == PlanDiagnosticKind::UnknownDependency
                    && d.message.contains("MISSING")),
            "{diags:?}"
        );
        assert!(
            diags
                .iter()
                .any(|d| d.kind == PlanDiagnosticKind::UnknownProfile
                    && d.message.contains("missing_profile")),
            "{diags:?}"
        );
        assert!(
            diags.iter().any(|d| {
                d.kind == PlanDiagnosticKind::DependencyCycle
                    && d.message.contains("A")
                    && d.message.contains("B")
                    && d.message.contains("C")
            }),
            "{diags:?}"
        );
        assert!(
            diags.iter().any(|d| {
                d.kind == PlanDiagnosticKind::DependencyCycle
                    && d.task_id.as_deref() == Some("SELF")
            }),
            "{diags:?}"
        );
    }

    #[test]
    fn t1_path_overlap_prefix_globs_and_non_overlapping_cases() {
        // Exact, directory prefixes, and ** globs
        assert!(paths_overlap(
            "crates/cli/src/plan.rs",
            "crates/cli/src/plan.rs"
        ));
        assert!(paths_overlap("crates/cli", "crates/cli/src/plan.rs"));
        assert!(paths_overlap("crates/cli/", "crates/cli/src/plan.rs"));
        assert!(paths_overlap("crates/cli/**", "crates/cli/src/mcp/plan.rs"));
        assert!(paths_overlap("crates/cli/**", "crates/cli"));
        assert!(paths_overlap("**/*.rs", "crates/cli/src/plan.rs"));
        assert!(paths_overlap("**/Cargo.toml", "Cargo.toml"));
        assert!(paths_overlap(
            "crates/cli/src/*.rs",
            "crates/cli/src/plan.rs"
        ));
        assert!(paths_overlap(
            "crates/cli/src/plan*.rs",
            "crates/cli/src/*_test.rs"
        ));
        assert!(paths_overlap(".\\crates\\cli", "crates/cli/src/lib.rs"));

        // Non-overlapping sibling paths and extensions
        assert!(!paths_overlap(
            "crates/cli/src/plan.rs",
            "crates/cli/src/plan_extra.rs"
        ));
        assert!(!paths_overlap("crates/cli/**", "crates/master/**"));
        assert!(!paths_overlap("crates/cli/src/*.rs", "crates/cli/src/*.md"));
        assert!(!paths_overlap(
            "crates/cli/src/*.rs",
            "crates/cli/src/sub/mod.rs"
        ));
        assert!(!paths_overlap("**/*.rs", "docs/reference.md"));

        assert!(path_matches_pattern(
            "crates/cli/src/plan.rs",
            "crates/cli/**"
        ));
        assert!(!path_matches_pattern("docs/BACKLOG.md", "crates/cli/**"));

        assert!(normalize_repo_path("../outside").is_err());
        assert!(normalize_repo_path("/etc/passwd").is_err());
        assert!(normalize_repo_path("C:\\secret").is_err());
        assert!(normalize_repo_path("").is_err());
    }

    #[test]
    fn t1_validates_overlapping_owned_paths_by_concurrency_and_status() {
        // Concurrent open tasks sharing prefix/glob => error
        let concurrent_overlap = r#"
[project]
base = "master"

[[task]]
id = "T-1"
title = "First"
status = "open"
owned_paths = ["crates/cli/src/**"]

[[task]]
id = "T-2"
title = "Second"
status = "review"
owned_paths = ["crates/cli/src/plan.rs"]
"#;
        let diags = validate_plan(&parse_plan(concurrent_overlap).unwrap());
        assert!(
            diags
                .iter()
                .any(|d| d.kind == PlanDiagnosticKind::OverlappingOwnedPaths),
            "{diags:?}"
        );

        // Dependency-ordered open tasks (like TEAM-3 -> TEAM-1 touching mcp.rs) and done tasks => OK
        let ordered = r#"
[project]
base = "master"

[[task]]
id = "T-0"
title = "Already merged"
status = "done"
owned_paths = ["crates/cli/src/mcp.rs"]

[[task]]
id = "T-1"
title = "First wave task"
status = "open"
owned_paths = ["crates/cli/src/plan.rs", "crates/cli/src/mcp.rs"]

[[task]]
id = "T-3"
title = "Depends on T-1"
status = "open"
depends_on = ["T-1"]
owned_paths = ["crates/cli/src/verify_gate.rs", "crates/cli/src/mcp.rs"]
"#;
        let diags = validate_plan(&parse_plan(ordered).unwrap());
        assert!(diags.is_empty(), "unexpected diagnostics: {diags:?}");
    }

    #[test]
    fn t1_validates_profile_tags_and_matrix_rejects_unknown() {
        let valid_tags = r#"
[project]
base = "master"

[profile.ok]
command = ["cargo", "test", "--locked"]
matrix = { os = ["macos", "windows", "linux"], arch = ["x86_64", "aarch64"], "env:FOO" = ["1"] }
require = ["os:mac", "arch:arm64", "iso:fs+net", "tool:cargo-nextest@0.9.99", "tag:fast", "gpu-box", "feat:workspaces", "opt:repeatable", "worker:win-1"]
shards = 8
"#;
        let ok_diags = validate_plan(&parse_plan(valid_tags).unwrap());
        assert!(ok_diags.is_empty(), "unexpected diagnostics: {ok_diags:?}");

        let bad_tags = r#"
[project]
base = "master"

[profile.bad]
command = ["   "]
matrix = { bogus = ["1"], os = ["plan9"] }
require = ["os:plan9", "iso:off", "link:direct", "bogus:val", "tool:"]
shards = 999
"#;
        let bad_diags = validate_plan(&parse_plan(bad_tags).unwrap());
        assert!(
            bad_diags
                .iter()
                .filter(|d| d.kind == PlanDiagnosticKind::UnknownProfileTag)
                .count()
                >= 6,
            "expected UnknownProfileTag diagnostics, got: {bad_diags:?}"
        );
        assert!(
            bad_diags
                .iter()
                .filter(|d| d.kind == PlanDiagnosticKind::InvalidProfile)
                .count()
                >= 2,
            "expected InvalidProfile diagnostics (command & shards), got: {bad_diags:?}"
        );
    }

    #[test]
    fn t1_snapshot_renders_leases_contracts_sanitization_and_caps_at_30_lines() {
        let now = Utc.with_ymd_and_hms(2026, 11, 26, 6, 0, 0).unwrap();
        let active_lease = LeaseRecord {
            task_id: "T-12".to_string(),
            holder: "agent-win".to_string(),
            person: Some("owner".to_string()),
            branch: Some("team/T-12".to_string()),
            owned_paths: vec!["crates/master/src/queue.rs".to_string()],
            expires_at: "2026-11-26T06:40:00Z".to_string(),
            blocked: false,
            verify_attempts: 3,
            verify_failures: 2,
            last_failure: Some("test queue::tests::cancel_counts".to_string()),
            last_fingerprint: Some("deadbeef".to_string()),
        };
        assert!(!active_lease.is_expired_at(now));

        let expired_lease = LeaseRecord {
            expires_at: "2026-11-26T05:30:00Z".to_string(),
            ..active_lease.clone()
        };
        assert!(expired_lease.is_expired_at(now));

        // 40-task stress plan with newline injection attempt in a task summary must strictly stay <= 30 lines.
        let mut entries = Vec::with_capacity(40);
        for i in 0..40 {
            entries.push(SnapshotTaskEntry {
                id: format!("T-{i}"),
                state: match i % 5 {
                    0 => "blocked",
                    1 => "claimed",
                    2 => "open",
                    3 => "review",
                    _ => "done",
                }
                .to_string(),
                touches_contract: i == 2,
                summary: if i == 1 {
                    "line1\nINJECTED\r\nFAKE LINE".to_string()
                } else {
                    format!("Task number {i}")
                },
            });
        }
        let lines = render_snapshot_lines(
            "master",
            "fc95e4c9",
            &[ContractStatus {
                path: "crates/core/src/protocol.rs".to_string(),
                short_sha: "3b1e".to_string(),
            }],
            &entries,
            &["abcd1234 merged #470".to_string()],
        );
        assert_eq!(
            lines.len(),
            MAX_SNAPSHOT_LINES,
            "got {} lines: {lines:?}",
            lines.len()
        );
        assert!(lines[0].starts_with("base master@fc95e4c9  contracts: protocol.rs@3b1e"));
        assert!(
            lines.iter().any(|l| l.contains("+13 more")),
            "expected +13 more summary line in {lines:?}"
        );
        assert!(lines.iter().any(|l| l.contains("[contract]")));
        assert!(lines.last().unwrap().starts_with("recent merges:"));
        assert!(
            !lines.join("\n").contains("line1\n"),
            "newlines in untrusted fields must be collapsed"
        );
    }

    #[test]
    fn t1_oxideswarm_first_wave_plan_fits_within_30_lines() {
        let wave_plan = r#"
[project]
base = "master"
contracts = ["crates/core/src/protocol.rs", "docs/reference.md"]

[profile.unit]
command = ["cargo", "test", "--locked", "--lib"]
matrix = { os = ["macos", "windows", "linux"] }
require = ["tool:cargo"]

[[task]]
id = "TEAM-1"
title = "Shared plan file, plan check, plan status and MCP plan_snapshot"
status = "open"
owned_paths = ["crates/cli/src/plan.rs", "crates/cli/src/mcp/plan.rs", "crates/cli/src/mcp.rs", "docs/reference.md"]
verify = ["unit"]
hours = 2

[[task]]
id = "TEAM-2"
title = "Task claims and path leases as git refs"
status = "open"
depends_on = ["TEAM-1"]
owned_paths = ["crates/cli/src/plan/lease.rs"]
verify = ["unit"]
hours = 2

[[task]]
id = "TEAM-3"
title = "Verify gate with compact failure JSON"
status = "open"
depends_on = ["TEAM-1"]
owned_paths = ["crates/cli/src/verify_gate.rs", "crates/cli/src/mcp.rs"]
verify = ["unit"]
hours = 2

[[task]]
id = "TEAM-4"
title = "Circuit breaker and notice to the person"
status = "open"
depends_on = ["TEAM-2", "TEAM-3"]
owned_paths = ["crates/cli/src/plan/lease.rs", "scripts/agent_bridge.py"]
verify = ["unit"]
hours = 1

[[task]]
id = "TEAM-5"
title = "Merge flow pull request table and re-verify"
status = "open"
depends_on = ["TEAM-2", "TEAM-3"]
owned_paths = [".oxideswarm/plan.toml"]
verify = ["unit"]
hours = 2
"#;
        let parsed = parse_plan(wave_plan).unwrap();
        let diags = validate_plan(&parsed);
        assert!(diags.is_empty(), "first wave diagnostics: {diags:?}");

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".oxideswarm")).unwrap();
        std::fs::write(dir.path().join(DEFAULT_PLAN_PATH), wave_plan).unwrap();
        let snap = build_snapshot(dir.path(), None, None, Utc::now()).unwrap();
        assert!(
            snap.line_count <= MAX_SNAPSHOT_LINES,
            "first wave snapshot exceeded {MAX_SNAPSHOT_LINES} lines: {}",
            snap.line_count
        );
        assert!(snap.text.contains("TEAM-1"));
        assert!(snap.text.contains("needs TEAM-1"));
        assert!(snap.text.contains("[contract]"));
    }

    #[test]
    fn t1_build_snapshot_reads_git_base_contracts_and_lease_refs() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();

        let git = |args: &[&str]| {
            let out = Command::new("git")
                .args(args)
                .current_dir(repo)
                .env("GIT_AUTHOR_NAME", "Test")
                .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
                .env("GIT_COMMITTER_NAME", "Test")
                .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
                .output()
                .expect("git must be installed");
            assert!(
                out.status.success(),
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };

        git(&["init", "-b", "master"]);
        std::fs::create_dir_all(repo.join(".oxideswarm")).unwrap();
        std::fs::create_dir_all(repo.join("docs")).unwrap();
        std::fs::write(repo.join("docs/reference.md"), "# Ref\n").unwrap();
        std::fs::write(
            repo.join(DEFAULT_PLAN_PATH),
            r#"
[project]
base = "master"
contracts = ["docs/reference.md"]

[[task]]
id = "T-1"
title = "Done item"
status = "done"

[[task]]
id = "T-2"
title = "Active leased item"
status = "open"
depends_on = ["T-1"]
owned_paths = ["crates/cli/src/plan.rs"]

[[task]]
id = "T-3"
title = "Blocked item"
status = "open"
person = "alice"
owned_paths = ["docs/other.md"]
"#,
        )
        .unwrap();
        git(&["add", "."]);
        git(&["commit", "-m", "init wave #10"]);

        let tree_sha = git(&["rev-parse", "HEAD^{tree}"]);
        let lease_t2 = toml::to_string(&LeaseRecord {
            task_id: "T-2".to_string(),
            holder: "agent-mac-ag".to_string(),
            person: Some("alice".to_string()),
            branch: Some("team/T-2".to_string()),
            owned_paths: vec!["crates/cli/src/plan.rs".to_string()],
            expires_at: "2099-01-01T06:40:00Z".to_string(),
            blocked: false,
            verify_attempts: 2,
            verify_failures: 1,
            last_failure: Some("test plan::tests::fail".to_string()),
            last_fingerprint: None,
        })
        .unwrap();
        let c2 = git(&["commit-tree", &tree_sha, "-m", &lease_t2]);
        git(&["update-ref", "refs/oxideswarm/lease/T-2", &c2]);

        let lease_t3 = toml::to_string(&LeaseRecord {
            task_id: "T-3".to_string(),
            holder: "agent-win".to_string(),
            person: None,
            branch: Some("team/T-3".to_string()),
            owned_paths: vec!["docs/other.md".to_string()],
            expires_at: "2099-01-01T07:00:00Z".to_string(),
            blocked: true,
            verify_attempts: 3,
            verify_failures: 3,
            last_failure: Some("exit 101".to_string()),
            last_fingerprint: Some("fp1".to_string()),
        })
        .unwrap();
        let c3 = git(&["commit-tree", &tree_sha, "-m", &lease_t3]);
        git(&["update-ref", "refs/oxideswarm/lease/T-3", &c3]);

        let now = Utc.with_ymd_and_hms(2026, 11, 26, 6, 0, 0).unwrap();
        let snap = build_snapshot(repo, None, None, now).unwrap();
        assert_ne!(snap.base_sha, "unknown");
        assert_eq!(snap.contracts.len(), 1);
        assert_ne!(snap.contracts[0].short_sha, "----");
        assert!(
            snap.text.contains("contracts: reference.md@"),
            "{}",
            snap.text
        );
        assert!(
            snap.text.contains("agent-mac-ag (lease until 06:40Z)"),
            "{}",
            snap.text
        );
        assert!(
            snap.text.contains("agent-win (3 x same failure)"),
            "{}",
            snap.text
        );
        assert!(snap.text.contains("-> person alice"), "{}", snap.text);
        assert!(snap.text.contains("recent merges:"), "{}", snap.text);
        assert!(snap.line_count <= MAX_SNAPSHOT_LINES);
    }
}
