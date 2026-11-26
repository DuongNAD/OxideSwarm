//! Git-ref task claims, fast-forward renewals, expiry takeovers, worktrees, and owned-path
//! diff checks (design 0111, D3 / TEAM-2).
//!
//! Leases live at `refs/oxideswarm/lease/<task-id>` on the configured git remote and store a
//! commit whose message encodes [`LeaseRecord`] as TOML with an empty tree. Create-only
//! `git push` claims atomically across concurrent callers; renewals fast-forward from the local
//! parent commit; expired leases are replaced via `git push --force-with-lease`.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::{
    load_plan_for_snapshot, path_matches_pattern, paths_overlap, resolve_git_base_ref,
    validate_task_id, LeaseRecord, PlanFile, PlanTask, TaskStatus, DEFAULT_PLAN_PATH,
    LEASE_REF_PREFIX,
};

/// Default lease lifetime in minutes (design 0111, D3.3).
pub const DEFAULT_LEASE_TTL_MINS: i64 = 45;
/// Maximum allowed lease TTL (24 hours) to keep abandoned claims bounded.
pub const MAX_LEASE_TTL_MINS: i64 = 1440;
/// Default git remote used for lease refs and base refs.
pub const DEFAULT_REMOTE: &str = "origin";
/// Default branch prefix created for claimed tasks (`team/<task-id>`).
pub const DEFAULT_BRANCH_PREFIX: &str = "team/";

const LEASE_AUTHOR_EMAIL: &str = "oxideswarm-lease@local";

/// Remote or local lease ref snapshot paired with its commit OID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRefEntry {
    pub task_id: String,
    pub oid: String,
    pub record: LeaseRecord,
}

/// Options for claiming a task lease (`oxideswarm plan claim` / MCP `task_claim`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimOptions {
    pub task_id: String,
    pub holder: String,
    pub person: Option<String>,
    pub remote: String,
    pub base: Option<String>,
    pub plan_path: Option<PathBuf>,
    pub branch: Option<String>,
    pub worktree_root: Option<PathBuf>,
    pub create_worktree: bool,
    pub ttl_minutes: i64,
}

/// Result returned by [`claim_task_at`] / [`claim_task`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimResult {
    pub task_id: String,
    pub holder: String,
    pub refname: String,
    pub oid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub taken_over_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<PathBuf>,
    pub lease: LeaseRecord,
}

/// Options for renewing an existing lease (`oxideswarm plan renew` / MCP `task_renew`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewOptions {
    pub task_id: String,
    pub holder: String,
    pub remote: String,
    pub expected_oid: Option<String>,
    pub ttl_minutes: i64,
    pub blocked: Option<bool>,
    pub verify_attempts: Option<u32>,
    pub verify_failures: Option<u32>,
    pub last_failure: Option<Option<String>>,
    pub last_fingerprint: Option<Option<String>>,
}

/// Result returned by [`renew_task_at`] / [`renew_task`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewResult {
    pub task_id: String,
    pub holder: String,
    pub refname: String,
    pub old_oid: String,
    pub new_oid: String,
    pub lease: LeaseRecord,
}

/// Options for releasing a lease (`oxideswarm plan release` / MCP `task_release`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseOptions {
    pub task_id: String,
    pub holder: Option<String>,
    pub remote: String,
    pub force: bool,
}

/// Result returned by [`release_task`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseResult {
    pub task_id: String,
    pub refname: String,
    pub deleted_oid: String,
}

/// Structured result of `oxideswarm plan check-diff <range>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffCheckResult {
    pub ok: bool,
    pub task_id: String,
    pub range: String,
    pub changed_files: Vec<String>,
    pub owned_paths: Vec<String>,
    pub violations: Vec<String>,
}

/// Validates a lease holder identifier (agent id).
pub fn validate_holder(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 128 {
        return Err("holder must be between 1 and 128 characters".to_string());
    }
    if trimmed.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(format!(
            "holder '{raw}' must not contain whitespace or control characters"
        ));
    }
    Ok(trimmed.to_string())
}

/// Validates a git token (`base`, `branch`, `remote`, `expected_oid`) so it cannot inject CLI
/// flags or control characters into `git` invocations.
pub fn validate_git_token(raw: &str, field: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 256 {
        return Err(format!("{field} must be between 1 and 256 characters"));
    }
    if trimmed.starts_with('-') {
        return Err(format!("{field} ('{raw}') must not start with '-'"));
    }
    if trimmed.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(format!(
            "{field} ('{raw}') must not contain whitespace or control characters"
        ));
    }
    if trimmed.contains("..") || trimmed.contains('\\') {
        return Err(format!(
            "{field} ('{raw}') must not contain '..' or backslashes"
        ));
    }
    Ok(trimmed.to_string())
}

/// Validates a git remote name used inside tracking namespace `refs/oxideswarm/remotes/<remote>/`.
pub fn validate_remote_name(raw: &str) -> Result<String, String> {
    let token = validate_git_token(raw, "remote")?;
    if token.contains('/') || token.contains(':') || token.ends_with(".lock") {
        return Err(format!(
            "remote ('{raw}') must be a configured git remote name without '/', ':', or '.lock'"
        ));
    }
    Ok(token)
}

/// Validates a branch name suitable for `git worktree add -b <branch>`.
pub fn validate_branch_name(raw: &str) -> Result<String, String> {
    let token = validate_git_token(raw, "branch")?;
    if token.starts_with('/')
        || token.ends_with('/')
        || token.contains("//")
        || token.contains(':')
        || token.ends_with(".lock")
        || token.contains("@{")
        || token
            .chars()
            .any(|c| matches!(c, '~' | '^' | '?' | '*' | '['))
    {
        return Err(format!("branch ('{raw}') is not a valid git branch name"));
    }
    Ok(token)
}

/// Validates a `git diff` revision/range argument without rejecting valid `A..B` / `A...B` ranges.
pub fn validate_diff_range(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 512 {
        return Err("diff range must be between 1 and 512 characters".to_string());
    }
    if trimmed.starts_with('-') {
        return Err(format!("diff range ('{raw}') must not start with '-'"));
    }
    if trimmed.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(format!(
            "diff range ('{raw}') must not contain whitespace or control characters"
        ));
    }
    Ok(trimmed.to_string())
}

/// Validates requested lease TTL in minutes.
pub fn validate_ttl_minutes(ttl_minutes: i64) -> Result<i64, String> {
    if !(1..=MAX_LEASE_TTL_MINS).contains(&ttl_minutes) {
        return Err(format!(
            "ttl_minutes ({ttl_minutes}) must be between 1 and {MAX_LEASE_TTL_MINS}"
        ));
    }
    Ok(ttl_minutes)
}

fn validate_hex_oid(raw: &str, field: &str) -> Result<String, String> {
    let token = validate_git_token(raw, field)?;
    if !matches!(token.len(), 40 | 64) || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "{field} ('{raw}') must be a full 40- or 64-character hex git OID"
        ));
    }
    Ok(token.to_ascii_lowercase())
}

/// Canonical git ref path for `task_id`: `refs/oxideswarm/lease/<task-id>`.
pub fn lease_refname(task_id: &str) -> String {
    format!("{LEASE_REF_PREFIX}{task_id}")
}

fn remote_lease_prefix(remote: &str) -> String {
    format!("refs/oxideswarm/remotes/{remote}/lease/")
}

fn run_git_checked(repo_dir: &Path, args: &[&str]) -> Result<String, String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(repo_dir)
        .output()
        .map_err(|e| format!("failed to run git {:?}: {e}", args))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        return Err(format!("git {} failed: {detail}", args.join(" ")));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn run_git_with_stdin(
    repo_dir: &Path,
    args: &[&str],
    stdin_bytes: &[u8],
    extra_env: &[(&str, &str)],
) -> Result<String, String> {
    let mut cmd = Command::new("git");
    cmd.args(args)
        .current_dir(repo_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn git {:?}: {e}", args))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(stdin_bytes)
            .map_err(|e| format!("failed writing stdin to git {:?}: {e}", args))?;
    }
    let out = child
        .wait_with_output()
        .map_err(|e| format!("failed waiting for git {:?}: {e}", args))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        return Err(format!("git {} failed: {detail}", args.join(" ")));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn resolve_local_ref_oid(repo_dir: &Path, refname: &str) -> Result<Option<String>, String> {
    let out = Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", refname])
        .current_dir(repo_dir)
        .output()
        .map_err(|e| format!("failed to run git rev-parse: {e}"))?;
    if out.status.success() {
        let oid = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if oid.is_empty() {
            Ok(None)
        } else {
            Ok(Some(oid))
        }
    } else {
        Ok(None)
    }
}

fn delete_local_ref_if_present(repo_dir: &Path, refname: &str) {
    let _ = Command::new("git")
        .args(["update-ref", "-d", refname])
        .current_dir(repo_dir)
        .output();
}

fn read_lease_ref_prefix(
    repo_dir: &Path,
    prefix: &str,
) -> Result<BTreeMap<String, LeaseRefEntry>, String> {
    let fmt = "%(refname)%1f%(objectname)%1f%(contents)%1e";
    let raw = run_git_checked(repo_dir, &["for-each-ref", "--format", fmt, prefix])?;
    let mut map = BTreeMap::new();
    for rec in raw.split('\x1e') {
        if rec.trim().is_empty() {
            continue;
        }
        let mut parts = rec.splitn(3, '\x1f');
        let (Some(refname), Some(oid), Some(body)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let Some(task_id) = refname.trim().strip_prefix(prefix) else {
            continue;
        };
        let oid = oid.trim().to_string();
        if oid.is_empty() {
            continue;
        }
        if let Ok(mut record) = toml::from_str::<LeaseRecord>(body) {
            if record.task_id.is_empty() {
                record.task_id = task_id.to_string();
            }
            map.insert(
                task_id.to_string(),
                LeaseRefEntry {
                    task_id: task_id.to_string(),
                    oid,
                    record,
                },
            );
        }
    }
    Ok(map)
}

/// Fetches `refs/oxideswarm/lease/*` from `remote` into a tracking namespace
/// (`refs/oxideswarm/remotes/<remote>/lease/*`) without clobbering the caller's own local
/// `refs/oxideswarm/lease/<task-id>` parent pointers.
pub fn sync_remote_leases(
    repo_dir: &Path,
    remote: &str,
) -> Result<BTreeMap<String, LeaseRefEntry>, String> {
    let remote = validate_remote_name(remote)?;
    let pattern = format!("{LEASE_REF_PREFIX}*");
    let listed = run_git_checked(repo_dir, &["ls-remote", &remote, &pattern])?;
    let track_prefix = remote_lease_prefix(&remote);

    if listed.trim().is_empty() {
        // Git exits 128 on `git fetch <remote> refs/oxideswarm/lease/*:...` when nothing matches
        // on the remote, so prune stale local tracking refs explicitly instead of invoking fetch.
        let existing_tracking = run_git_checked(
            repo_dir,
            &["for-each-ref", "--format", "%(refname)", &track_prefix],
        )?;
        for refname in existing_tracking
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
        {
            delete_local_ref_if_present(repo_dir, refname);
        }
        return Ok(BTreeMap::new());
    }

    let refspec = format!("+{LEASE_REF_PREFIX}*:{track_prefix}*");
    run_git_checked(
        repo_dir,
        &["fetch", "--prune", "--no-tags", &remote, &refspec],
    )?;
    read_lease_ref_prefix(repo_dir, &track_prefix)
}

fn create_lease_commit(
    repo_dir: &Path,
    record: &LeaseRecord,
    parent_oid: Option<&str>,
) -> Result<String, String> {
    let empty_tree = run_git_with_stdin(repo_dir, &["mktree"], b"", &[])?;
    let body = toml::to_string_pretty(record)
        .map_err(|e| format!("failed to serialize lease record: {e}"))?;
    let mut args = vec!["commit-tree", empty_tree.as_str()];
    if let Some(p) = parent_oid {
        args.push("-p");
        args.push(p);
    }
    let env = [
        ("GIT_AUTHOR_NAME", record.holder.as_str()),
        ("GIT_AUTHOR_EMAIL", LEASE_AUTHOR_EMAIL),
        ("GIT_COMMITTER_NAME", record.holder.as_str()),
        ("GIT_COMMITTER_EMAIL", LEASE_AUTHOR_EMAIL),
    ];
    run_git_with_stdin(repo_dir, &args, body.as_bytes(), &env)
}

fn load_plan_for_lease_ops(
    repo_dir: &Path,
    remote: &str,
    plan_path: Option<&Path>,
    base_override: Option<&str>,
) -> Result<PlanFile, String> {
    if plan_path.is_some() {
        return load_plan_for_snapshot(repo_dir, plan_path, None);
    }
    if let Some(base) = base_override {
        let base = validate_git_token(base, "base")?;
        let remote_ref = format!("{remote}/{base}");
        let _ = Command::new("git")
            .args(["fetch", "--no-tags", remote, &base])
            .current_dir(repo_dir)
            .output();
        if let Some(resolved) = resolve_git_base_ref(repo_dir, &remote_ref)
            .or_else(|| resolve_git_base_ref(repo_dir, &base))
        {
            if let Ok(plan) = load_plan_for_snapshot(repo_dir, None, Some(&resolved)) {
                return Ok(plan);
            }
        }
    }

    // Read base branch name from remote HEAD/master or working tree first, then refresh from remote.
    let head_hint = resolve_git_base_ref(repo_dir, &format!("{remote}/HEAD"))
        .or_else(|| resolve_git_base_ref(repo_dir, &format!("{remote}/master")))
        .or_else(|| resolve_git_base_ref(repo_dir, "HEAD"));
    if let Ok(initial) = load_plan_for_snapshot(repo_dir, None, head_hint.as_deref()) {
        let base = initial.project.base.clone();
        if validate_git_token(&base, "project.base").is_ok() {
            let _ = Command::new("git")
                .args(["fetch", "--no-tags", remote, &base])
                .current_dir(repo_dir)
                .output();
            let remote_ref = format!("{remote}/{base}");
            if let Some(resolved) = resolve_git_base_ref(repo_dir, &remote_ref)
                .or_else(|| resolve_git_base_ref(repo_dir, &base))
            {
                let spec = format!("{resolved}:{DEFAULT_PLAN_PATH}");
                if let Ok(text) = run_git_checked(repo_dir, &["show", &spec]) {
                    return super::parse_plan(&text);
                }
            }
        }
        return Ok(initial);
    }

    Err(format!(
        "cannot find plan file at '{}' or on remote '{remote}'",
        repo_dir.join(DEFAULT_PLAN_PATH).display()
    ))
}

fn find_task<'a>(plan: &'a PlanFile, task_id: &str) -> Result<&'a PlanTask, String> {
    plan.tasks
        .iter()
        .find(|t| t.id == task_id)
        .ok_or_else(|| format!("task '{task_id}' was not found in the plan"))
}

fn default_worktree_path(repo_dir: &Path, worktree_root: Option<&Path>, task_id: &str) -> PathBuf {
    let dir_name = format!("wt-{task_id}");
    match worktree_root {
        Some(root) => root.join(dir_name),
        None => repo_dir
            .parent()
            .unwrap_or(repo_dir)
            .to_path_buf()
            .join(dir_name),
    }
}

fn rollback_pushed_lease(repo_dir: &Path, remote: &str, refname: &str, pushed_oid: &str) {
    let force_arg = format!("--force-with-lease={refname}:{pushed_oid}");
    let delete_spec = format!(":{refname}");
    let _ = Command::new("git")
        .args(["push", &force_arg, remote, &delete_spec])
        .current_dir(repo_dir)
        .output();
    delete_local_ref_if_present(repo_dir, refname);
    let track = format!(
        "{}{}",
        remote_lease_prefix(remote),
        refname.strip_prefix(LEASE_REF_PREFIX).unwrap_or(refname)
    );
    delete_local_ref_if_present(repo_dir, &track);
}

/// Claims `opts.task_id` on `opts.remote` using a create-only push (or `--force-with-lease` on an
/// expired lease), optionally creating an isolated worktree on `team/<task-id>`.
pub fn claim_task_at(
    repo_dir: &Path,
    opts: &ClaimOptions,
    now_utc: DateTime<Utc>,
) -> Result<ClaimResult, String> {
    validate_task_id(&opts.task_id)?;
    let holder = validate_holder(&opts.holder)?;
    let remote = validate_remote_name(&opts.remote)?;
    let ttl_minutes = validate_ttl_minutes(opts.ttl_minutes)?;
    if let Some(b) = &opts.branch {
        validate_branch_name(b)?;
    }
    if let Some(base) = &opts.base {
        validate_git_token(base, "base")?;
    }

    let remote_leases = sync_remote_leases(repo_dir, &remote)?;
    let plan = load_plan_for_lease_ops(
        repo_dir,
        &remote,
        opts.plan_path.as_deref(),
        opts.base.as_deref(),
    )?;
    let task = find_task(&plan, &opts.task_id)?;

    if task.status != TaskStatus::Open {
        return Err(format!(
            "cannot claim task '{}': plan status is '{}' (expected 'open')",
            task.id,
            task.status.as_str()
        ));
    }

    for dep_id in &task.depends_on {
        let dep = find_task(&plan, dep_id).map_err(|_| {
            format!(
                "cannot claim task '{}': dependency '{dep_id}' is missing from the plan",
                task.id
            )
        })?;
        if dep.status != TaskStatus::Done {
            return Err(format!(
                "cannot claim task '{}': dependency '{dep_id}' has status '{}' (expected 'done')",
                task.id,
                dep.status.as_str()
            ));
        }
    }

    for path in &task.owned_paths {
        super::normalize_repo_path(path)
            .map_err(|e| format!("task '{}' has invalid owned_paths entry: {e}", task.id))?;
    }

    let mut takeover_from: Option<(String, String)> = None;
    if let Some(existing) = remote_leases.get(&task.id) {
        if existing.record.blocked {
            return Err(format!(
                "cannot claim task '{}': lease is blocked by circuit breaker (holder '{}')",
                task.id, existing.record.holder
            ));
        }
        if !existing.record.is_expired_at(now_utc) {
            return Err(format!(
                "task '{}' already has an active lease held by '{}' until {}",
                task.id, existing.record.holder, existing.record.expires_at
            ));
        }
        takeover_from = Some((existing.oid.clone(), existing.record.holder.clone()));
    }

    for (other_id, other) in &remote_leases {
        if other_id == &task.id {
            continue;
        }
        if !other.record.blocked && other.record.is_expired_at(now_utc) {
            continue;
        }
        for mine in &task.owned_paths {
            for theirs in &other.record.owned_paths {
                if paths_overlap(mine, theirs) {
                    return Err(format!(
                        "cannot claim task '{}': owned path '{mine}' overlaps active lease for '{other_id}' ('{theirs}', held by '{}')",
                        task.id, other.record.holder
                    ));
                }
            }
        }
    }

    let branch_name = opts
        .branch
        .clone()
        .unwrap_or_else(|| format!("{DEFAULT_BRANCH_PREFIX}{}", task.id));
    let wt_path = if opts.create_worktree {
        Some(default_worktree_path(
            repo_dir,
            opts.worktree_root.as_deref(),
            &task.id,
        ))
    } else {
        None
    };

    let expires_at = (now_utc + Duration::minutes(ttl_minutes)).to_rfc3339();
    let record = LeaseRecord {
        task_id: task.id.clone(),
        holder: holder.clone(),
        person: opts.person.clone().or_else(|| task.person.clone()),
        branch: Some(branch_name.clone()),
        owned_paths: task.owned_paths.clone(),
        expires_at,
        blocked: false,
        verify_attempts: 0,
        verify_failures: 0,
        last_failure: None,
        last_fingerprint: None,
    };

    let oid = create_lease_commit(repo_dir, &record, None)?;
    let refname = lease_refname(&task.id);
    let push_spec = format!("{oid}:{refname}");

    let taken_over_from = match takeover_from {
        None => {
            // Create-only push: never pass `--force`, so concurrent claims race atomically on the
            // remote ref creation check.
            run_git_checked(repo_dir, &["push", &remote, &push_spec])
                .map_err(|e| format!("lease claim push rejected for '{}': {e}", task.id))?;
            None
        }
        Some((old_oid, old_holder)) => {
            let lease_arg = format!("--force-with-lease={refname}:{old_oid}");
            run_git_checked(repo_dir, &["push", &lease_arg, &remote, &push_spec]).map_err(|e| {
                format!(
                    "expired lease takeover push failed for '{}' (old holder '{old_holder}'): {e}",
                    task.id
                )
            })?;
            Some(old_holder)
        }
    };

    run_git_checked(repo_dir, &["update-ref", &refname, &oid])?;

    if let Some(ref wt_dir) = wt_path {
        if let Some(parent) = wt_dir.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                rollback_pushed_lease(repo_dir, &remote, &refname, &oid);
                return Err(format!(
                    "failed to prepare worktree parent '{}': {e}",
                    parent.display()
                ));
            }
        }
        let base_name = opts.base.as_deref().unwrap_or(plan.project.base.as_str());
        let start_point = resolve_git_base_ref(repo_dir, &format!("{remote}/{base_name}"))
            .or_else(|| resolve_git_base_ref(repo_dir, base_name))
            .unwrap_or_else(|| "HEAD".to_string());
        let wt_str = wt_dir.to_string_lossy().into_owned();
        if let Err(err) = run_git_checked(
            repo_dir,
            &["worktree", "add", &wt_str, "-b", &branch_name, &start_point],
        ) {
            rollback_pushed_lease(repo_dir, &remote, &refname, &oid);
            return Err(format!(
                "{err} (rolled back remote lease '{refname}' at {oid})"
            ));
        }
    }

    Ok(ClaimResult {
        task_id: task.id.clone(),
        holder,
        refname,
        oid,
        taken_over_from,
        branch: Some(branch_name),
        worktree_path: wt_path,
        lease: record,
    })
}

/// Convenience wrapper around [`claim_task_at`] using `Utc::now()`.
pub fn claim_task(repo_dir: &Path, opts: &ClaimOptions) -> Result<ClaimResult, String> {
    claim_task_at(repo_dir, opts, Utc::now())
}

/// Renews an active lease via a strictly fast-forward push built on the holder's parent OID.
pub fn renew_task_at(
    repo_dir: &Path,
    opts: &RenewOptions,
    now_utc: DateTime<Utc>,
) -> Result<RenewResult, String> {
    validate_task_id(&opts.task_id)?;
    let holder = validate_holder(&opts.holder)?;
    let remote = validate_remote_name(&opts.remote)?;
    let ttl_minutes = validate_ttl_minutes(opts.ttl_minutes)?;
    let expected_oid = match &opts.expected_oid {
        Some(raw) => Some(validate_hex_oid(raw, "expected_oid")?),
        None => None,
    };

    let remote_leases = sync_remote_leases(repo_dir, &remote)?;
    let current = remote_leases
        .get(&opts.task_id)
        .ok_or_else(|| format!("no remote lease exists for task '{}'", opts.task_id))?;

    if current.record.holder != holder {
        return Err(format!(
            "cannot renew task '{}': lease is held by '{}', not '{holder}'",
            opts.task_id, current.record.holder
        ));
    }

    if current.record.blocked && !matches!(opts.blocked, Some(false)) {
        return Err(format!(
            "cannot renew task '{}': lease is marked blocked",
            opts.task_id
        ));
    }

    let refname = lease_refname(&opts.task_id);
    let local_oid = resolve_local_ref_oid(repo_dir, &refname)?;
    if let Some(ref exp) = expected_oid {
        if *exp != current.oid {
            return Err(format!(
                "cannot renew task '{}': remote lease moved to {} (expected {exp})",
                opts.task_id, current.oid
            ));
        }
    }
    if let Some(ref local) = local_oid {
        if *local != current.oid {
            return Err(format!(
                "cannot renew task '{}': remote lease moved from local OID {local} to {}",
                opts.task_id, current.oid
            ));
        }
    }

    let parent_oid = expected_oid
        .or(local_oid)
        .unwrap_or_else(|| current.oid.clone());

    let mut next_record = current.record.clone();
    next_record.expires_at = (now_utc + Duration::minutes(ttl_minutes)).to_rfc3339();
    if let Some(b) = opts.blocked {
        next_record.blocked = b;
    }
    if let Some(a) = opts.verify_attempts {
        next_record.verify_attempts = a;
    }
    if let Some(f) = opts.verify_failures {
        next_record.verify_failures = f;
    }
    if let Some(ref lf) = opts.last_failure {
        next_record.last_failure = lf.clone();
    }
    if let Some(ref fp) = opts.last_fingerprint {
        next_record.last_fingerprint = fp.clone();
    }

    let new_oid = create_lease_commit(repo_dir, &next_record, Some(&parent_oid))?;
    let push_spec = format!("{new_oid}:{refname}");
    // Strict fast-forward push (no `--force`): fails automatically if another holder replaced the ref.
    run_git_checked(repo_dir, &["push", &remote, &push_spec]).map_err(|e| {
        format!(
            "lease renew fast-forward rejected for '{}': {e}",
            opts.task_id
        )
    })?;
    run_git_checked(repo_dir, &["update-ref", &refname, &new_oid])?;

    Ok(RenewResult {
        task_id: opts.task_id.clone(),
        holder,
        refname,
        old_oid: parent_oid,
        new_oid,
        lease: next_record,
    })
}

/// Convenience wrapper around [`renew_task_at`] using `Utc::now()`.
pub fn renew_task(repo_dir: &Path, opts: &RenewOptions) -> Result<RenewResult, String> {
    renew_task_at(repo_dir, opts, Utc::now())
}

/// Deletes the remote and local lease ref for `opts.task_id`.
pub fn release_task(repo_dir: &Path, opts: &ReleaseOptions) -> Result<ReleaseResult, String> {
    validate_task_id(&opts.task_id)?;
    let remote = validate_remote_name(&opts.remote)?;
    let holder = match &opts.holder {
        Some(h) => Some(validate_holder(h)?),
        None => None,
    };

    let remote_leases = sync_remote_leases(repo_dir, &remote)?;
    let current = remote_leases
        .get(&opts.task_id)
        .ok_or_else(|| format!("no remote lease exists for task '{}'", opts.task_id))?;

    if !opts.force {
        match holder.as_deref() {
            Some(h) if h == current.record.holder => {}
            Some(h) => {
                return Err(format!(
                    "cannot release task '{}': lease is held by '{}', not '{h}' (pass --force to override)",
                    opts.task_id, current.record.holder
                ));
            }
            None => {
                return Err(format!(
                    "cannot release task '{}' held by '{}' without matching holder or --force",
                    opts.task_id, current.record.holder
                ));
            }
        }
    }

    let refname = lease_refname(&opts.task_id);
    let lease_arg = format!("--force-with-lease={refname}:{}", current.oid);
    let delete_spec = format!(":{refname}");
    run_git_checked(repo_dir, &["push", &lease_arg, &remote, &delete_spec])
        .map_err(|e| format!("failed to delete remote lease ref '{refname}' on '{remote}': {e}"))?;

    delete_local_ref_if_present(repo_dir, &refname);
    let track = format!("{}{}", remote_lease_prefix(&remote), opts.task_id);
    delete_local_ref_if_present(repo_dir, &track);

    Ok(ReleaseResult {
        task_id: opts.task_id.clone(),
        refname,
        deleted_oid: current.oid.clone(),
    })
}

fn infer_task_id_from_branch(repo_dir: &Path) -> Result<String, String> {
    let branch = run_git_checked(repo_dir, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    let trimmed = branch.trim();
    if let Some(rest) = trimmed.strip_prefix(DEFAULT_BRANCH_PREFIX) {
        validate_task_id(rest)?;
        return Ok(rest.to_string());
    }
    Err(format!(
        "cannot infer task id from branch '{trimmed}' (expected '{DEFAULT_BRANCH_PREFIX}<task-id>' or pass --task)"
    ))
}

/// Checks `git diff --name-only -z <range> --` against `task.owned_paths`.
pub fn check_diff(
    repo_dir: &Path,
    range: &str,
    task_id_override: Option<&str>,
    plan_path: Option<&Path>,
    base_override: Option<&str>,
) -> Result<DiffCheckResult, String> {
    let range = validate_diff_range(range)?;
    if let Some(b) = base_override {
        validate_git_token(b, "base")?;
    }
    let task_id = match task_id_override {
        Some(id) => {
            validate_task_id(id)?;
            id.to_string()
        }
        None => infer_task_id_from_branch(repo_dir)?,
    };

    let base_hint = base_override
        .and_then(|b| resolve_git_base_ref(repo_dir, b))
        .or_else(|| resolve_git_base_ref(repo_dir, "HEAD"));
    let plan = load_plan_for_snapshot(repo_dir, plan_path, base_hint.as_deref())?;
    let task = find_task(&plan, &task_id)?;

    let out = Command::new("git")
        .args(["diff", "--name-only", "-z", &range, "--"])
        .current_dir(repo_dir)
        .output()
        .map_err(|e| format!("failed to run git diff: {e}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        return Err(format!("git diff '{range}' failed: {stderr}"));
    }

    let mut changed_files = Vec::new();
    let mut violations = Vec::new();
    for raw_entry in out.stdout.split(|&b| b == 0) {
        if raw_entry.is_empty() {
            continue;
        }
        let path_str = String::from_utf8(raw_entry.to_vec())
            .map_err(|_| "git diff returned a non-UTF-8 file path".to_string())?;
        let allowed = task
            .owned_paths
            .iter()
            .any(|pattern| path_matches_pattern(&path_str, pattern));
        if !allowed {
            violations.push(path_str.clone());
        }
        changed_files.push(path_str);
    }

    Ok(DiffCheckResult {
        ok: violations.is_empty(),
        task_id,
        range,
        changed_files,
        owned_paths: task.owned_paths.clone(),
        violations,
    })
}

fn resolve_cli_repo_dir(repo_override: Option<&Path>) -> Result<PathBuf, String> {
    let cwd = std::env::current_dir().map_err(|e| format!("cannot read current directory: {e}"))?;
    Ok(match repo_override {
        Some(p) if p.is_absolute() => p.to_path_buf(),
        Some(p) => cwd.join(p),
        None => cwd,
    })
}

fn emit_cli_error(json: bool, msg: &str) -> ExitCode {
    if json {
        let body = serde_json::json!({ "ok": false, "error": msg });
        println!(
            "{}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );
    } else {
        eprintln!("error: {msg}");
    }
    ExitCode::FAILURE
}

/// CLI handler for `oxideswarm plan claim`.
pub(crate) fn run_plan_claim_cli(opts: &ClaimOptions, repo: Option<&Path>, json: bool) -> ExitCode {
    let repo_dir = match resolve_cli_repo_dir(repo) {
        Ok(d) => d,
        Err(e) => return emit_cli_error(json, &e),
    };
    match claim_task(&repo_dir, opts) {
        Ok(res) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({ "ok": true, "claim": res }))
                        .unwrap_or_default()
                );
            } else {
                println!(
                    "claimed {} as {} (ref {}, oid {}, expires {})",
                    res.task_id, res.holder, res.refname, res.oid, res.lease.expires_at
                );
                if let Some(wt) = &res.worktree_path {
                    println!(
                        "worktree: {} (branch {})",
                        wt.display(),
                        res.branch.as_deref().unwrap_or("")
                    );
                }
            }
            ExitCode::SUCCESS
        }
        Err(e) => emit_cli_error(json, &e),
    }
}

/// CLI handler for `oxideswarm plan renew`.
pub(crate) fn run_plan_renew_cli(opts: &RenewOptions, repo: Option<&Path>, json: bool) -> ExitCode {
    let repo_dir = match resolve_cli_repo_dir(repo) {
        Ok(d) => d,
        Err(e) => return emit_cli_error(json, &e),
    };
    match renew_task(&repo_dir, opts) {
        Ok(res) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({ "ok": true, "renew": res }))
                        .unwrap_or_default()
                );
            } else {
                println!(
                    "renewed {} ({} -> {}, expires {})",
                    res.task_id, res.old_oid, res.new_oid, res.lease.expires_at
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => emit_cli_error(json, &e),
    }
}

/// CLI handler for `oxideswarm plan release`.
pub(crate) fn run_plan_release_cli(
    opts: &ReleaseOptions,
    repo: Option<&Path>,
    json: bool,
) -> ExitCode {
    let repo_dir = match resolve_cli_repo_dir(repo) {
        Ok(d) => d,
        Err(e) => return emit_cli_error(json, &e),
    };
    match release_task(&repo_dir, opts) {
        Ok(res) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &serde_json::json!({ "ok": true, "release": res })
                    )
                    .unwrap_or_default()
                );
            } else {
                println!(
                    "released {} (deleted ref {} at {})",
                    res.task_id, res.refname, res.deleted_oid
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => emit_cli_error(json, &e),
    }
}

/// CLI handler for `oxideswarm plan check-diff`.
pub(crate) fn run_plan_check_diff_cli(
    range: &str,
    task: Option<&str>,
    plan: Option<&Path>,
    repo: Option<&Path>,
    base: Option<&str>,
    json: bool,
) -> ExitCode {
    let repo_dir = match resolve_cli_repo_dir(repo) {
        Ok(d) => d,
        Err(e) => return emit_cli_error(json, &e),
    };
    match check_diff(&repo_dir, range, task, plan, base) {
        Ok(res) => {
            if json {
                println!("{}", serde_json::to_string_pretty(&res).unwrap_or_default());
            } else if res.ok {
                println!(
                    "plan check-diff OK: {} file(s) in '{}' all within owned_paths of {}",
                    res.changed_files.len(),
                    res.range,
                    res.task_id
                );
            } else {
                for v in &res.violations {
                    eprintln!(
                        "error: path '{v}' is outside owned_paths of task '{}'",
                        res.task_id
                    );
                }
                eprintln!(
                    "plan check-diff FAILED: {} violation(s)",
                    res.violations.len()
                );
            }
            if res.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(e) => emit_cli_error(json, &e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_input_validation_blocks_flag_injection_and_bad_values() {
        assert!(validate_holder("agent-win-2").is_ok());
        assert!(validate_holder("").is_err());
        assert!(validate_holder("bad agent").is_err());
        assert!(validate_holder("bad\nagent").is_err());

        assert!(validate_remote_name("origin").is_ok());
        assert!(validate_remote_name("--upload-pack=evil").is_err());
        assert!(validate_remote_name("bad/remote").is_err());

        assert!(validate_branch_name("team/TEAM-2").is_ok());
        assert!(validate_branch_name("-b").is_err());
        assert!(validate_branch_name("team/../escape").is_err());
        assert!(validate_branch_name("team/foo.lock").is_err());

        assert!(validate_diff_range("origin/master..HEAD").is_ok());
        assert!(validate_diff_range("--output=/tmp/x").is_err());

        assert!(validate_ttl_minutes(45).is_ok());
        assert!(validate_ttl_minutes(0).is_err());
        assert!(validate_ttl_minutes(MAX_LEASE_TTL_MINS + 1).is_err());
    }
}
