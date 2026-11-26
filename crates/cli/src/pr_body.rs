//! Pull request body and verify-table generator (design 0111, D6 / TEAM-5 groundwork).
//!
//! Formats recorded verification outputs (worker, OS, link kind, toolchain, exit result, task id,
//! and verified base commit) into a deterministic GFM Markdown pull request body with stale-base
//! warnings and failure summaries. Compatible with both design 0111 D4.3/D6.1 JSON and TEAM-3
//! `verify_gate::{VerifyGateReport, VerifyRecord}` JSON (where optional fields use `Option<String>`
//! with `skip_serializing_if = "Option::is_none"`).

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::plan::lease::{
    has_git_remote, lookup_task_lease, release_task, validate_remote_name, ReleaseOptions,
    DEFAULT_BRANCH_PREFIX,
};
use crate::plan::{
    load_plan, parse_plan, resolve_plan_path, validate_plan, validate_task_id, PlanTask, TaskStatus,
};
use crate::verify_gate::VERIFY_RECORD_REL_DIR;

/// One failing test or compiler diagnostic summary from a verify cell (design 0111, D4.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyFailureSummary {
    pub kind: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub line: Option<u32>,
    pub message: String,
}

/// Execution summary of one verify profile cell across a target worker/OS (design 0111, D4.3 & D6.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifyCellRecord {
    pub profile: String,
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub arch: Option<String>,
    #[serde(default)]
    pub shard: Option<String>,
    #[serde(default)]
    pub worker: Option<String>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub link: Option<String>,
    #[serde(default)]
    pub toolchain: Option<String>,
    pub exit: i32,
    pub secs: f64,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub failures: Vec<VerifyFailureSummary>,
}

/// Compact verify output produced by the verify gate for one task revision (design 0111, D4.3)
/// or stored in `.oxideswarm/verify/<task>.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifyRecord {
    pub task: String,
    #[serde(default)]
    pub title: Option<String>,
    pub commit: String,
    #[serde(default)]
    pub base_commit: Option<String>,
    #[serde(default)]
    pub base_branch: Option<String>,
    pub ok: bool,
    #[serde(default)]
    pub timestamp_utc: Option<String>,
    #[serde(default)]
    pub first_fingerprint: Option<String>,
    #[serde(default)]
    pub untrusted_data: Option<bool>,
    #[serde(default)]
    pub cells: Vec<VerifyCellRecord>,
    #[serde(default)]
    pub omitted: usize,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Metadata required to render a complete pull request description for a completed task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrBodyOptions {
    pub task_id: String,
    #[serde(default)]
    pub task_title: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub base_branch: Option<String>,
    pub base_commit: String,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub current_base_commit: Option<String>,
}

/// Parses a single JSON-encoded [`VerifyRecord`].
pub fn parse_verify_record(json_str: &str) -> Result<VerifyRecord, String> {
    serde_json::from_str::<VerifyRecord>(json_str)
        .map_err(|e| format!("invalid verify record JSON: {e}"))
}

/// Parses either a JSON array of [`VerifyRecord`]s, or a whitespace-separated stream of one or more
/// JSON [`VerifyRecord`] objects (single object, JSONL, or concatenated pretty-printed files).
pub fn parse_verify_records(input: &str) -> Result<Vec<VerifyRecord>, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.starts_with('[') {
        return serde_json::from_str::<Vec<VerifyRecord>>(trimmed)
            .map_err(|e| format!("invalid verify record array JSON: {e}"));
    }

    let mut out = Vec::new();
    for (idx, item) in serde_json::Deserializer::from_str(trimmed)
        .into_iter::<VerifyRecord>()
        .enumerate()
    {
        let rec = item.map_err(|e| format!("invalid verify record #{}: {e}", idx + 1))?;
        out.push(rec);
    }
    Ok(out)
}

/// Collapses newlines and control characters and escapes Markdown table pipes (`|`) so untrusted
/// output strings never break or inject rows/columns inside a Markdown table cell.
pub fn escape_md_cell(raw: &str) -> String {
    let mut cleaned = String::with_capacity(raw.len());
    let mut prev_space = false;
    for ch in raw.chars() {
        let mapped = if ch.is_control() { ' ' } else { ch };
        if mapped.is_whitespace() {
            if !prev_space && !cleaned.is_empty() {
                cleaned.push(' ');
            }
            prev_space = true;
        } else {
            if mapped == '|' {
                cleaned.push_str("\\|");
            } else {
                cleaned.push(mapped);
            }
            prev_space = false;
        }
    }
    cleaned.trim().to_string()
}

fn escape_code_span(raw: &str) -> String {
    escape_md_cell(raw).replace('`', "'")
}

fn display_opt_or_dash(opt: Option<&str>) -> &str {
    opt.map(str::trim).filter(|s| !s.is_empty()).unwrap_or("-")
}

/// Formats one verify cell's result string (`PASS (...)` or `FAIL (...)`).
pub fn format_cell_result(cell: &VerifyCellRecord) -> String {
    let secs = if cell.secs.is_finite() && cell.secs >= 0.0 {
        cell.secs
    } else {
        0.0
    };
    if cell.exit == 0 && cell.failures.is_empty() {
        format!("PASS (exit 0, {secs:.1}s)")
    } else if cell.failures.is_empty() {
        format!("FAIL (exit {}, {secs:.1}s)", cell.exit)
    } else {
        format!(
            "FAIL (exit {}, {secs:.1}s, {} failure(s))",
            cell.exit,
            cell.failures.len()
        )
    }
}

fn short_sha(raw: &str) -> String {
    raw.trim().chars().take(12).collect()
}

/// Returns `true` when `current_base` is known and differs from `verified_base` (case-insensitive,
/// allowing abbreviated SHA prefix match when at least 7 characters match).
pub fn is_base_commit_stale(verified_base: &str, current_base: Option<&str>) -> bool {
    let vb = verified_base.trim();
    if vb.is_empty() {
        return false;
    }
    let Some(cur) = current_base.map(str::trim).filter(|s| !s.is_empty()) else {
        return false;
    };
    if vb.eq_ignore_ascii_case(cur) {
        return false;
    }
    let min_len = vb.len().min(cur.len());
    if min_len >= 7 {
        let vb_low = vb.to_ascii_lowercase();
        let cur_low = cur.to_ascii_lowercase();
        if vb_low.starts_with(&cur_low) || cur_low.starts_with(&vb_low) {
            return false;
        }
    }
    true
}

/// Returns `true` if `opts.base_commit` or any record's `base_commit` differs from
/// `opts.current_base_commit`.
pub fn check_records_stale(opts: &PrBodyOptions, records: &[VerifyRecord]) -> bool {
    if is_base_commit_stale(&opts.base_commit, opts.current_base_commit.as_deref()) {
        return true;
    }
    records
        .iter()
        .filter_map(|r| r.base_commit.as_deref())
        .any(|b| is_base_commit_stale(b, opts.current_base_commit.as_deref()))
}

fn validate_non_empty_token(raw: &str, field: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(format!("{field} cannot be empty"));
    }
    if trimmed.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(format!(
            "{field} ('{raw}') must not contain whitespace or control characters"
        ));
    }
    Ok(trimmed.to_string())
}

/// Renders the GFM Markdown verification table for `records` (design 0111, D6.1).
pub fn render_verify_table(records: &[VerifyRecord], default_base_commit: Option<&str>) -> String {
    let has_any_cell = records.iter().any(|r| !r.cells.is_empty());
    if !has_any_cell {
        return "_(no verify cells recorded)_".to_string();
    }

    let mut lines = vec![
        "| Profile | Worker | OS | Link | Toolchain | Result | Task ID | Base commit |".to_string(),
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |".to_string(),
    ];

    for rec in records {
        let base_ref = rec
            .base_commit
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| default_base_commit.filter(|s| !s.trim().is_empty()))
            .unwrap_or("unknown");
        let base_cell = escape_code_span(&short_sha(base_ref));

        for cell in &rec.cells {
            let worker_disp = cell
                .worker
                .as_deref()
                .or(cell.worker_id.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("-");
            let task_id_disp = display_opt_or_dash(cell.task_id.as_deref());
            let row = format!(
                "| {} | {} | {} | {} | `{}` | {} | `{}` | `{}` |",
                escape_md_cell(&cell.profile),
                escape_md_cell(worker_disp),
                escape_md_cell(display_opt_or_dash(cell.os.as_deref())),
                escape_md_cell(display_opt_or_dash(cell.link.as_deref())),
                escape_code_span(display_opt_or_dash(cell.toolchain.as_deref())),
                escape_md_cell(&format_cell_result(cell)),
                escape_code_span(task_id_disp),
                base_cell,
            );
            lines.push(row);
        }
    }

    lines.join("\n")
}

/// Renders the complete Markdown pull request body from task metadata and verify records
/// (design 0111, D6).
pub fn render_pr_body(opts: &PrBodyOptions, records: &[VerifyRecord]) -> Result<String, String> {
    let task_id = validate_non_empty_token(&opts.task_id, "task_id")?;
    let base_commit = validate_non_empty_token(&opts.base_commit, "base_commit")?;
    let base_branch = opts
        .base_branch
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("master");

    for rec in records {
        if rec.task.trim() != task_id {
            return Err(format!(
                "verify record task '{}' does not match PR task_id '{task_id}'",
                rec.task
            ));
        }
    }

    let safe_task_id = escape_code_span(&task_id);
    let safe_base_branch = escape_code_span(base_branch);
    let short_verified = short_sha(&base_commit);

    let mut out = String::new();
    out.push_str("## Task\n\n");
    let resolved_title = opts
        .task_title
        .as_deref()
        .or_else(|| records.iter().find_map(|r| r.title.as_deref()))
        .map(escape_md_cell)
        .filter(|t| !t.is_empty());

    match resolved_title {
        Some(title) => out.push_str(&format!("- **Task:** `{safe_task_id}` — {title}\n")),
        None => out.push_str(&format!("- **Task:** `{safe_task_id}`\n")),
    }
    if let Some(branch) = opts
        .branch
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        out.push_str(&format!("- **Branch:** `{}`\n", escape_code_span(branch)));
    }
    out.push_str(&format!(
        "- **Base verified:** `{safe_base_branch}` @ `{}`\n",
        escape_code_span(&short_verified)
    ));

    let stale = check_records_stale(opts, records);
    let cur_base_trimmed = opts
        .current_base_commit
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if stale {
        if let Some(cur_base) = cur_base_trimmed {
            out.push_str(&format!(
                "- **Stale base warning:** current `{safe_base_branch}` is at `{}`, which differs from verified `{}` — re-verify on the latest base before merging.\n",
                escape_code_span(&short_sha(cur_base)),
                escape_code_span(&short_verified)
            ));
        }
    }

    if let Some(summary) = opts
        .summary
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        out.push_str("\n## Summary\n\n");
        out.push_str(summary);
        out.push('\n');
    }

    let total_cells: usize = records.iter().map(|r| r.cells.len()).sum();
    let all_pass = !records.is_empty()
        && total_cells > 0
        && records
            .iter()
            .all(|r| r.ok && r.cells.iter().all(|c| c.exit == 0 && c.failures.is_empty()));
    let overall_status = if total_cells == 0 {
        "UNVERIFIED"
    } else if all_pass {
        "PASS"
    } else {
        "FAIL"
    };

    out.push_str("\n## Verification\n\n");
    if stale {
        out.push_str(&format!(
            "**Status:** `STALE` (verified `{overall_status}`, {total_cells} cell(s))\n\n"
        ));
        if let Some(cur_base) = cur_base_trimmed {
            out.push_str(&format!(
                "> **STALE:** verified base `{}` does not match current `{safe_base_branch}` tip `{}` — re-verify on the latest base before merging.\n\n",
                escape_code_span(&short_verified),
                escape_code_span(&short_sha(cur_base)),
            ));
        }
    } else {
        out.push_str(&format!(
            "**Status:** `{overall_status}` ({total_cells} cell(s))\n\n"
        ));
    }
    out.push_str(&render_verify_table(records, Some(&base_commit)));
    out.push('\n');

    let mut failure_lines = Vec::new();
    let mut omitted_total = 0usize;
    for rec in records {
        omitted_total = omitted_total.saturating_add(rec.omitted);
        for cell in &rec.cells {
            let worker_disp = cell
                .worker
                .as_deref()
                .or(cell.worker_id.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("-");
            for f in &cell.failures {
                let loc = match (&f.file, f.line) {
                    (Some(file), Some(line)) => format!(" ({}:{line})", escape_md_cell(file)),
                    (Some(file), None) => format!(" ({})", escape_md_cell(file)),
                    _ => String::new(),
                };
                let name_disp = display_opt_or_dash(f.name.as_deref());
                failure_lines.push(format!(
                    "- `{}` on `{}` [{}]: `{}`{loc} — {}",
                    escape_code_span(&cell.profile),
                    escape_code_span(worker_disp),
                    escape_md_cell(&f.kind),
                    escape_code_span(name_disp),
                    escape_md_cell(&f.message),
                ));
            }
        }
    }

    if !failure_lines.is_empty() || omitted_total > 0 {
        out.push_str("\n### Failures\n\n");
        for line in failure_lines {
            out.push_str(&line);
            out.push('\n');
        }
        if omitted_total > 0 {
            out.push_str(&format!(
                "- _({omitted_total} additional failure detail(s) omitted to stay within the compact verify budget)_\n"
            ));
        }
    }

    out.push_str(
        "\n> Note: Re-verify against the latest base branch before merging. A person reviews and merges; agents never merge.\n",
    );

    Ok(out)
}

/// Options for generating a PR body from `.oxideswarm/plan.toml` and `.oxideswarm/verify/<id>.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanPrBodyOptions {
    pub task_id: String,
    pub plan_path: Option<PathBuf>,
    pub base: Option<String>,
    pub remote: String,
    pub record_paths: Vec<PathBuf>,
    pub branch: Option<String>,
    pub summary: Option<String>,
    pub require_fresh: bool,
}

/// Output returned by [`build_plan_pr_body`] / `oxideswarm plan pr-body`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPrBodyOutput {
    pub task_id: String,
    pub status: String,
    pub stale: bool,
    pub base_branch: String,
    pub verified_base_commit: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_base_commit: Option<String>,
    pub markdown: String,
}

/// Options for [`mark_task_done`] (`oxideswarm plan done <id>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoneOptions {
    pub task_id: String,
    pub plan_path: Option<PathBuf>,
    pub remote: String,
}

/// Result returned by [`mark_task_done`] / `oxideswarm plan done`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoneResult {
    pub task_id: String,
    pub previous_status: String,
    pub new_status: String,
    pub plan_path: String,
    pub lease_deleted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_oid: Option<String>,
    pub unblocked_dependents: Vec<String>,
}

fn sanitize_record_filename(raw: &str) -> String {
    let mut cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '-'
            }
        })
        .collect();
    if cleaned.len() > 64 {
        cleaned.truncate(64);
    }
    let trimmed = cleaned.trim_matches(|c| c == '.' || c == '-');
    if trimmed.is_empty() {
        "task".to_string()
    } else {
        trimmed.to_string()
    }
}

fn run_git_trimmed(repo_dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(repo_dir)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty()).then_some(s)
}

fn resolve_base_tip_sha(repo_dir: &Path, remote: &str, base_ref: &str) -> Option<String> {
    let trimmed = base_ref.trim();
    if trimmed.is_empty() {
        return None;
    }
    if !trimmed.contains('/') && !remote.is_empty() && has_git_remote(repo_dir, remote) {
        let remote_ref = format!("refs/remotes/{remote}/{trimmed}");
        if let Some(sha) = run_git_trimmed(repo_dir, &["rev-parse", "--verify", &remote_ref]) {
            return Some(sha);
        }
    }
    if let Some(sha) = run_git_trimmed(repo_dir, &["rev-parse", "--verify", trimmed]) {
        return Some(sha);
    }
    if !trimmed.contains('/') {
        let fallback = format!("refs/heads/{trimmed}");
        if let Some(sha) = run_git_trimmed(repo_dir, &["rev-parse", "--verify", &fallback]) {
            return Some(sha);
        }
    }
    None
}

fn read_non_symlink_text(path: &Path, label: &str) -> Result<String, String> {
    let meta = std::fs::symlink_metadata(path)
        .map_err(|e| format!("failed to read {label} '{}': {e}", path.display()))?;
    if meta.file_type().is_symlink() {
        return Err(format!("refusing symlinked {label} '{}'", path.display()));
    }
    if !meta.is_file() {
        return Err(format!(
            "{label} '{}' is not a regular file",
            path.display()
        ));
    }
    std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {label} '{}': {e}", path.display()))
}

fn resolve_record_paths(
    repo_dir: &Path,
    task_id: &str,
    explicit: &[PathBuf],
) -> Result<Vec<PathBuf>, String> {
    if explicit.is_empty() {
        let dot_ox = repo_dir.join(".oxideswarm");
        if let Ok(meta) = std::fs::symlink_metadata(&dot_ox) {
            if meta.file_type().is_symlink() {
                return Err(format!("refusing symlinked dir {}", dot_ox.display()));
            }
        }
        let verify_dir = repo_dir.join(VERIFY_RECORD_REL_DIR);
        if let Ok(meta) = std::fs::symlink_metadata(&verify_dir) {
            if meta.file_type().is_symlink() {
                return Err(format!("refusing symlinked dir {}", verify_dir.display()));
            }
        }
        let safe = sanitize_record_filename(task_id);
        return Ok(vec![verify_dir.join(format!("{safe}.json"))]);
    }
    Ok(explicit
        .iter()
        .map(|p| {
            if p.is_absolute() {
                p.clone()
            } else {
                repo_dir.join(p)
            }
        })
        .collect())
}

fn find_task_in_plan<'a>(
    tasks: &'a [PlanTask],
    task_id: &str,
    plan_path: &Path,
) -> Result<&'a PlanTask, String> {
    tasks
        .iter()
        .find(|t| t.id == task_id)
        .ok_or_else(|| format!("task '{task_id}' not found in {}", plan_path.display()))
}

/// Builds a PR body and staleness assessment from `.oxideswarm/plan.toml` and verify record JSONs
/// (design 0111 D6.1 & D6.2).
pub fn build_plan_pr_body(
    repo_dir: &Path,
    opts: &PlanPrBodyOptions,
) -> Result<PlanPrBodyOutput, String> {
    let task_id = opts.task_id.trim().to_string();
    validate_task_id(&task_id)?;
    let remote = validate_remote_name(&opts.remote)?;
    let plan_path = resolve_plan_path(repo_dir, opts.plan_path.as_deref());
    let plan = load_plan(&plan_path)?;
    let task = find_task_in_plan(&plan.tasks, &task_id, &plan_path)?;

    let record_paths = resolve_record_paths(repo_dir, &task_id, &opts.record_paths)?;
    let mut records = Vec::new();
    for path in &record_paths {
        let raw = read_non_symlink_text(path, "verify record")?;
        let parsed = parse_verify_records(&raw)
            .map_err(|e| format!("failed parsing '{}': {e}", path.display()))?;
        records.extend(parsed);
    }
    if records.is_empty() {
        return Err("no verify records found in input".to_string());
    }

    let base_branch = opts
        .base
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&plan.project.base)
        .to_string();
    let current_base_commit = resolve_base_tip_sha(repo_dir, &remote, &base_branch);

    let verified_base_commit = records
        .iter()
        .find_map(|r| {
            r.base_commit
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
        })
        .or_else(|| {
            let head_commit = records[0].commit.trim();
            current_base_commit
                .as_deref()
                .and_then(|cur| run_git_trimmed(repo_dir, &["merge-base", head_commit, cur]))
        })
        .unwrap_or_else(|| records[0].commit.trim().to_string());

    let branch = opts
        .branch
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            run_git_trimmed(repo_dir, &["rev-parse", "--abbrev-ref", "HEAD"])
                .filter(|b| b != "HEAD")
        })
        .unwrap_or_else(|| format!("{DEFAULT_BRANCH_PREFIX}{task_id}"));

    let render_opts = PrBodyOptions {
        task_id: task.id.clone(),
        task_title: Some(task.title.clone()),
        branch: Some(branch),
        base_branch: Some(base_branch.clone()),
        base_commit: verified_base_commit.clone(),
        summary: opts.summary.clone(),
        current_base_commit: current_base_commit.clone(),
    };
    let markdown = render_pr_body(&render_opts, &records)?;
    let stale = check_records_stale(&render_opts, &records);

    let total_cells: usize = records.iter().map(|r| r.cells.len()).sum();
    let all_pass = !records.is_empty()
        && total_cells > 0
        && records
            .iter()
            .all(|r| r.ok && r.cells.iter().all(|c| c.exit == 0 && c.failures.is_empty()));
    let status = if stale {
        "STALE".to_string()
    } else if total_cells == 0 {
        "UNVERIFIED".to_string()
    } else if all_pass {
        "PASS".to_string()
    } else {
        "FAIL".to_string()
    };

    Ok(PlanPrBodyOutput {
        task_id,
        status,
        stale,
        base_branch,
        verified_base_commit,
        current_base_commit,
        markdown,
    })
}

fn write_plan_atomic(plan_path: &Path, content: &str) -> Result<(), String> {
    let parent = plan_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if let Ok(meta) = std::fs::symlink_metadata(parent) {
        if meta.file_type().is_symlink() {
            return Err(format!("refusing symlinked dir {}", parent.display()));
        }
    }
    let tmp = parent.join(format!(".plan.toml.tmp.{}", Uuid::new_v4()));
    std::fs::write(&tmp, content.as_bytes())
        .map_err(|e| format!("write temp plan '{}': {e}", tmp.display()))?;
    std::fs::rename(&tmp, plan_path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("replace plan '{}': {e}", plan_path.display())
    })
}

/// Sets `task_id`'s status to `done` in `.oxideswarm/plan.toml` (preserving comments/layout via
/// `toml_edit`) and deletes its remote lease ref `refs/oxideswarm/lease/<task_id>` if present
/// (design 0111 D6.3).
pub fn mark_task_done(repo_dir: &Path, opts: &DoneOptions) -> Result<DoneResult, String> {
    let task_id = opts.task_id.trim().to_string();
    validate_task_id(&task_id)?;
    let remote = validate_remote_name(&opts.remote)?;
    let plan_path = resolve_plan_path(repo_dir, opts.plan_path.as_deref());
    let raw = read_non_symlink_text(&plan_path, "plan file")?;

    let current_plan = parse_plan(&raw)?;
    let existing = find_task_in_plan(&current_plan.tasks, &task_id, &plan_path)?;
    let previous_status = existing.status.as_str().to_string();

    let mut doc = raw.parse::<toml_edit::DocumentMut>().map_err(|e| {
        format!(
            "failed parsing '{}' as TOML document: {e}",
            plan_path.display()
        )
    })?;
    let tasks_arr = doc
        .get_mut("task")
        .and_then(|item| item.as_array_of_tables_mut())
        .ok_or_else(|| format!("missing [[task]] array in {}", plan_path.display()))?;

    let mut updated_row = false;
    for tbl in tasks_arr.iter_mut() {
        if tbl.get("id").and_then(|v| v.as_str()) == Some(task_id.as_str()) {
            let mut val = toml_edit::Value::from("done");
            if let Some(decor) = tbl
                .get("status")
                .and_then(|item| item.as_value())
                .map(|existing| existing.decor().clone())
            {
                *val.decor_mut() = decor;
            }
            tbl.insert("status", toml_edit::Item::Value(val));
            updated_row = true;
            break;
        }
    }
    if !updated_row {
        return Err(format!(
            "task '{task_id}' not found in {}",
            plan_path.display()
        ));
    }

    let rendered = doc.to_string();
    let reparsed = parse_plan(&rendered)?;
    let diags = validate_plan(&reparsed);
    if let Some(first) = diags.first() {
        return Err(format!("updated plan fails validation: {}", first.message));
    }

    if previous_status != TaskStatus::Done.as_str() {
        write_plan_atomic(&plan_path, &rendered)?;
    }

    let (lease_deleted, deleted_oid) = if has_git_remote(repo_dir, &remote) {
        if lookup_task_lease(repo_dir, &remote, &task_id)?.is_some() {
            let rel = release_task(
                repo_dir,
                &ReleaseOptions {
                    task_id: task_id.clone(),
                    holder: None,
                    remote: remote.clone(),
                    force: true,
                },
            )?;
            (true, Some(rel.deleted_oid))
        } else {
            (false, None)
        }
    } else {
        (false, None)
    };

    let mut done_ids: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for t in &reparsed.tasks {
        if t.status == TaskStatus::Done {
            done_ids.insert(t.id.as_str());
        }
    }
    let unblocked_dependents: Vec<String> = reparsed
        .tasks
        .iter()
        .filter(|t| t.status == TaskStatus::Open)
        .filter(|t| t.depends_on.iter().any(|d| d == &task_id))
        .filter(|t| t.depends_on.iter().all(|d| done_ids.contains(d.as_str())))
        .map(|t| t.id.clone())
        .collect();

    Ok(DoneResult {
        task_id,
        previous_status,
        new_status: TaskStatus::Done.as_str().to_string(),
        plan_path: plan_path.display().to_string(),
        lease_deleted,
        deleted_oid,
        unblocked_dependents,
    })
}

fn resolve_repo_arg(repo: Option<&Path>) -> Result<PathBuf, String> {
    match repo {
        Some(p) => Ok(p.to_path_buf()),
        None => std::env::current_dir().map_err(|e| format!("cannot read current directory: {e}")),
    }
}

fn emit_pr_body_cli_error(json: bool, msg: &str) -> ExitCode {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({ "ok": false, "error": msg }))
                .unwrap_or_default()
        );
    } else {
        eprintln!("error: {msg}");
    }
    ExitCode::FAILURE
}

/// CLI handler for `oxideswarm plan pr-body`.
pub(crate) fn run_plan_pr_body_cli(
    opts: &PlanPrBodyOptions,
    repo: Option<&Path>,
    json: bool,
) -> ExitCode {
    let repo_dir = match resolve_repo_arg(repo) {
        Ok(d) => d,
        Err(e) => return emit_pr_body_cli_error(json, &e),
    };
    let out = match build_plan_pr_body(&repo_dir, opts) {
        Ok(o) => o,
        Err(e) => return emit_pr_body_cli_error(json, &e),
    };
    let fresh_fail = opts.require_fresh && out.stale;
    if json {
        let mut val = serde_json::to_value(&out).unwrap_or_default();
        if let Some(obj) = val.as_object_mut() {
            obj.insert("ok".to_string(), serde_json::Value::Bool(!fresh_fail));
            if fresh_fail {
                obj.insert(
                    "error".to_string(),
                    serde_json::Value::String(format!(
                        "verify record for '{}' is STALE (verified base {} != current {} tip {})",
                        out.task_id,
                        short_sha(&out.verified_base_commit),
                        out.base_branch,
                        out.current_base_commit
                            .as_deref()
                            .map(short_sha)
                            .unwrap_or_else(|| "unknown".to_string()),
                    )),
                );
            }
        }
        println!("{}", serde_json::to_string_pretty(&val).unwrap_or_default());
    } else {
        print!("{}", out.markdown);
        if fresh_fail {
            eprintln!(
                "error: verify record for '{}' is STALE (verified base {} != current {} tip {})",
                out.task_id,
                short_sha(&out.verified_base_commit),
                out.base_branch,
                out.current_base_commit
                    .as_deref()
                    .map(short_sha)
                    .unwrap_or_else(|| "unknown".to_string()),
            );
        }
    }
    if fresh_fail {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// CLI handler for `oxideswarm plan done`.
pub(crate) fn run_plan_done_cli(opts: &DoneOptions, repo: Option<&Path>, json: bool) -> ExitCode {
    let repo_dir = match resolve_repo_arg(repo) {
        Ok(d) => d,
        Err(e) => return emit_pr_body_cli_error(json, &e),
    };
    match mark_task_done(&repo_dir, opts) {
        Ok(res) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({ "ok": true, "done": res }))
                        .unwrap_or_default()
                );
            } else {
                println!(
                    "marked {} done ({} -> {}); lease_deleted={}; unblocked=[{}]",
                    res.task_id,
                    res.previous_status,
                    res.new_status,
                    res.lease_deleted,
                    res.unblocked_dependents.join(", ")
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => emit_pr_body_cli_error(json, &e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorded_passing_json() -> &'static str {
        r#"{
  "task": "T-12",
  "title": "Count cancelled tasks apart from failed ones in status --json",
  "commit": "abcdef1234567890",
  "base_commit": "fc95e4c9f6ea5a23b1be6e10ac902d79baf92163",
  "base_branch": "master",
  "ok": true,
  "cells": [
    {
      "profile": "unit",
      "os": "macos",
      "worker": "mac-air",
      "link": "direct",
      "toolchain": "rustc 1.96.0 aarch64-apple-darwin",
      "exit": 0,
      "secs": 31.4,
      "task_id": "11111111-1111-1111-1111-111111111111",
      "failures": []
    },
    {
      "profile": "unit",
      "os": "windows",
      "worker": "msi",
      "link": "direct",
      "toolchain": "rustc 1.96.0 x86_64-pc-windows-gnu",
      "exit": 0,
      "secs": 48.2,
      "task_id": "22222222-2222-2222-2222-222222222222",
      "failures": []
    },
    {
      "profile": "clippy",
      "os": "linux",
      "worker": "hetzner-1",
      "link": "relay",
      "toolchain": "rustc 1.96.0 x86_64-unknown-linux-gnu",
      "exit": 0,
      "secs": 19.0,
      "task_id": "33333333-3333-3333-3333-333333333333",
      "failures": []
    }
  ],
  "omitted": 0
}"#
    }

    fn recorded_failing_json() -> &'static str {
        r#"{
  "task": "T-12",
  "commit": "1234567890abcdef",
  "ok": false,
  "cells": [
    {
      "profile": "unit",
      "os": "windows",
      "worker": "msi",
      "link": "direct",
      "toolchain": "rustc 1.96.0 x86_64-pc-windows-gnu",
      "exit": 101,
      "secs": 48.2,
      "task_id": "44444444-4444-4444-4444-444444444444",
      "failures": [
        {
          "kind": "test",
          "name": "queue::tests::cancel_counts",
          "file": "crates/master/src/queue.rs",
          "line": 812,
          "message": "assertion `left == right` failed: left: 2, right: 1\nsecond line | pipe"
        }
      ]
    }
  ],
  "omitted": 2
}"#
    }

    #[test]
    fn test_pr_body_passing_matrix_table_and_stale_base_warning() {
        let records = parse_verify_records(recorded_passing_json()).expect("parse passing json");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cells.len(), 3);

        let body = render_pr_body(
            &PrBodyOptions {
                task_id: "T-12".to_string(),
                task_title: None,
                branch: Some("team/T-12".to_string()),
                base_branch: Some("master".to_string()),
                base_commit: "fc95e4c9f6ea5a23b1be6e10ac902d79baf92163".to_string(),
                summary: Some("Separated cancelled counts in status --json.".to_string()),
                current_base_commit: Some("ddbbf520deadbeef".to_string()),
            },
            &records,
        )
        .expect("render passing PR body");

        assert!(body.contains(
            "**Task:** `T-12` — Count cancelled tasks apart from failed ones in status --json"
        ));
        assert!(body.contains("**Status:** `STALE` (verified `PASS`, 3 cell(s))"));
        assert!(body.contains("> **STALE:** verified base `fc95e4c9f6ea` does not match current `master` tip `ddbbf520dead`"));
        assert!(body.contains(
            "| Profile | Worker | OS | Link | Toolchain | Result | Task ID | Base commit |"
        ));
        assert!(body.contains("| unit | mac-air | macos | direct | `rustc 1.96.0 aarch64-apple-darwin` | PASS (exit 0, 31.4s) | `11111111-1111-1111-1111-111111111111` | `fc95e4c9f6ea` |"));
        assert!(body.contains("| unit | msi | windows | direct | `rustc 1.96.0 x86_64-pc-windows-gnu` | PASS (exit 0, 48.2s) |"));
        assert!(body.contains("| clippy | hetzner-1 | linux | relay |"));
        assert!(body.contains("Stale base warning:"));
        assert!(body.contains("`ddbbf520dead`"));
        assert!(!body.contains("### Failures"));

        // Matching base commit tip -> PASS (not STALE).
        let fresh_body = render_pr_body(
            &PrBodyOptions {
                task_id: "T-12".to_string(),
                task_title: None,
                branch: Some("team/T-12".to_string()),
                base_branch: Some("master".to_string()),
                base_commit: "fc95e4c9f6ea5a23b1be6e10ac902d79baf92163".to_string(),
                summary: None,
                current_base_commit: Some("fc95e4c9f6ea5a23b1be6e10ac902d79baf92163".to_string()),
            },
            &records,
        )
        .expect("render fresh passing PR body");
        assert!(fresh_body.contains("**Status:** `PASS` (3 cell(s))"));
        assert!(!fresh_body.contains("STALE"));
    }

    #[test]
    fn test_pr_body_failing_record_escapes_markdown_and_surfaces_omitted() {
        let rec = parse_verify_record(recorded_failing_json()).expect("parse failing json");
        let body = render_pr_body(
            &PrBodyOptions {
                task_id: "T-12".to_string(),
                task_title: Some("Header | title\ninjection".to_string()),
                branch: Some("team/T-12".to_string()),
                base_branch: None,
                base_commit: "fc95e4c9".to_string(),
                summary: None,
                current_base_commit: Some("fc95e4c9".to_string()),
            },
            &[rec],
        )
        .expect("render failing PR body");

        assert!(body.contains("**Status:** `FAIL` (1 cell(s))"));
        assert!(body.contains("FAIL (exit 101, 48.2s, 1 failure(s))"));
        assert!(body.contains("Header \\| title injection"));
        assert!(body.contains("queue::tests::cancel_counts"));
        assert!(body.contains("crates/master/src/queue.rs:812"));
        assert!(body.contains("second line \\| pipe"));
        assert!(body.contains("2 additional failure detail(s) omitted"));
        assert!(!body.contains("Stale base warning:"));
    }

    #[test]
    fn test_pr_body_accepts_team3_verify_gate_json_with_omitted_optional_fields() {
        // Recorded TEAM-3 `VerifyGateReport` + `.oxideswarm/verify/<task>.json` shape where
        // `Option<String>` keys are omitted when `None` (unplaced cell and command failure).
        let team3_report_json = r#"{
  "task": "TEAM-3",
  "commit": "de04d03b+dirty-1234",
  "ok": false,
  "untrusted_data": true,
  "cells": [
    {
      "profile": "unit",
      "os": "linux",
      "arch": "x86_64",
      "exit": 101,
      "secs": 12.3,
      "failures": [
        {
          "kind": "compile",
          "file": "crates/cli/src/lib.rs",
          "line": 42,
          "message": "mismatched types"
        }
      ]
    },
    {
      "profile": "gpu",
      "exit": 1,
      "secs": 0.0,
      "failures": [
        {
          "kind": "unplaced",
          "message": "no connected worker satisfies gpu:true"
        }
      ]
    }
  ],
  "omitted": 0,
  "warnings": ["relayed link estimated ~1.2s sync"]
}"#;
        let rec = parse_verify_record(team3_report_json).expect("parse TEAM-3 report");
        let body = render_pr_body(
            &PrBodyOptions {
                task_id: "TEAM-3".to_string(),
                task_title: Some("Verify gate".to_string()),
                branch: Some("team/TEAM-3".to_string()),
                base_branch: Some("master".to_string()),
                base_commit: "5e48fb02".to_string(),
                summary: None,
                current_base_commit: None,
            },
            &[rec],
        )
        .expect("render TEAM-3 report");

        assert!(body.contains("**Status:** `FAIL` (2 cell(s))"));
        assert!(body.contains("| unit | - | linux | - | `-` | FAIL (exit 101, 12.3s, 1 failure(s)) | `-` | `5e48fb02` |"));
        assert!(body.contains(
            "| gpu | - | - | - | `-` | FAIL (exit 1, 0.0s, 1 failure(s)) | `-` | `5e48fb02` |"
        ));
        assert!(body.contains(
            "- `unit` on `-` [compile]: `-` (crates/cli/src/lib.rs:42) — mismatched types"
        ));
        assert!(body
            .contains("- `gpu` on `-` [unplaced]: `-` — no connected worker satisfies gpu:true"));
    }

    #[test]
    fn test_pr_body_parses_arrays_jsonl_and_rejects_mismatched_or_empty_inputs() {
        // Array input
        let arr_json = format!("[{}, {}]", recorded_passing_json(), recorded_failing_json());
        let arr = parse_verify_records(&arr_json).expect("parse array");
        assert_eq!(arr.len(), 2);

        // JSONL input
        let single_line_pass = recorded_passing_json().replace('\n', " ");
        let single_line_fail = recorded_failing_json().replace('\n', " ");
        let jsonl = format!("{single_line_pass}\n\n{single_line_fail}\n");
        let parsed_jsonl = parse_verify_records(&jsonl).expect("parse jsonl");
        assert_eq!(parsed_jsonl.len(), 2);

        // Concatenated multi-line pretty-printed `.oxideswarm/verify/*.json` stream
        let pretty_stream = format!("{}\n{}", recorded_passing_json(), recorded_failing_json());
        let parsed_stream = parse_verify_records(&pretty_stream).expect("parse pretty JSON stream");
        assert_eq!(parsed_stream.len(), 2);

        // Invalid JSON rejected
        assert!(parse_verify_records("{bad json").is_err());

        // Mismatched task_id rejected
        let rec = parse_verify_record(recorded_passing_json()).unwrap();
        let mismatch = render_pr_body(
            &PrBodyOptions {
                task_id: "T-99".to_string(),
                task_title: None,
                branch: None,
                base_branch: None,
                base_commit: "fc95e4c9".to_string(),
                summary: None,
                current_base_commit: None,
            },
            &[rec],
        );
        assert!(mismatch.is_err());

        // Empty verify records -> UNVERIFIED (never PASS)
        let unverified = render_pr_body(
            &PrBodyOptions {
                task_id: "T-12".to_string(),
                task_title: None,
                branch: None,
                base_branch: None,
                base_commit: "fc95e4c9".to_string(),
                summary: None,
                current_base_commit: None,
            },
            &[],
        )
        .expect("empty records still render unverified body");
        assert!(unverified.contains("**Status:** `UNVERIFIED` (0 cell(s))"));
        assert!(unverified.contains("_(no verify cells recorded)_"));
    }

    #[test]
    fn test_escape_md_cell_handles_pipes_controls_and_non_finite_secs() {
        assert_eq!(escape_md_cell("a | b\r\nc\t\x07d"), "a \\| b c d");

        let nan_cell = VerifyCellRecord {
            profile: "p|1".to_string(),
            os: Some("linux".to_string()),
            arch: None,
            shard: None,
            worker: Some("w1\nline2".to_string()),
            worker_id: None,
            link: Some("direct".to_string()),
            toolchain: Some("rustc`1.96".to_string()),
            exit: 0,
            secs: f64::NAN,
            task_id: Some("id|1".to_string()),
            failures: Vec::new(),
        };
        assert_eq!(format_cell_result(&nan_cell), "PASS (exit 0, 0.0s)");
        let table = render_verify_table(
            &[VerifyRecord {
                task: "T-1".to_string(),
                title: None,
                commit: "abc".to_string(),
                base_commit: None,
                base_branch: None,
                ok: true,
                timestamp_utc: None,
                first_fingerprint: None,
                untrusted_data: None,
                cells: vec![nan_cell],
                omitted: 0,
                warnings: Vec::new(),
            }],
            Some("deadbeef12345678"),
        );
        assert!(table.contains("| p\\|1 | w1 line2 | linux | direct | `rustc'1.96` | PASS (exit 0, 0.0s) | `id\\|1` | `deadbeef1234` |"));

        // Non-ASCII base_commit char-boundary slicing must never panic.
        assert_eq!(short_sha("éééééééééééérest").chars().count(), 12);
    }
}
