//! Verify gate (`oxideswarm verify --task <id>` and MCP `verify`; design 0111, D4 / TEAM-3).
//!
//! Syncs the caller's worktree (committed or dirty) into a `<repo>-<task>` workspace on matching
//! grid workers, runs the task's configured `[profile.<name>]` commands via matrix/shard fan-out,
//! reduces compiler JSON diagnostics (`--message-format=json`), libtest, and nextest failures into
//! a single JSON report capped at 4096 bytes, prefers direct links over relayed ones, and persists
//! a verification record under `.oxideswarm/verify/<task-id>.json`.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::placement::{
    listed_supports_workspaces, listed_worker_id, normalize_arch, normalize_os, tag_value,
    Placement, ARCH_TAG_PREFIX, LINK_TAG_PREFIX, OS_TAG_PREFIX, REQUIRE_PREFIX, TOOL_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, WorkspaceRef};
use rusty_grid_core::WorkerCapabilities;

use crate::blobs::{scan_workspace_source, upload_inputs, BlobConnection, WorkspaceLimits};
use crate::cli_app::{resolve_relay_url, RELAY_URL_HELP};
use crate::config::{
    resolve_string, resolve_u64, resolve_usize, ConfigFile, SecretSection, CLIENT_MASTER_ENV_VARS,
};
use crate::connect::{
    connect_master, normalize_master_addr, with_client_relay_url, DEFAULT_MASTER_ADDR,
};
use crate::matrix::{
    check_placeability, execute_matrix, expand_cells, listed_as_connected, safe_deadline,
    MatrixAxis, MatrixAxisKind, MatrixCell, MatrixSpec,
};
use crate::plan::lease::{
    lookup_task_lease, notify_person_blocked, record_verify_outcome, validate_holder,
    validate_remote_name, RecordVerifyOptions, VerifyLeaseUpdate, CIRCUIT_BREAKER_THRESHOLD,
    DEFAULT_LEASE_TTL_MINS, DEFAULT_REMOTE,
};
use crate::plan::{
    load_plan, path_matches_pattern, resolve_plan_path, validate_plan, VerifyProfile,
};
use crate::secret_cmd::{client_secret, peer_insecure, SecretOpts};
use crate::shard::{
    shard_slug, spread_shards_swrr, EligibleWorker, HARD_MAX_SHARDS, PREFER_WORKER_PREFIX,
};

/// Hard cap on the compact verify JSON returned to agents (`design 0111` D4.3).
pub const MAX_VERIFY_JSON_BYTES: usize = 4096;

/// Default maximum failures retained per matrix/shard cell (`design 0111` D4.3).
pub const MAX_FAILURES_PER_CELL: usize = 5;

/// Default maximum lines kept in each failure message (`design 0111` D4.3).
pub const MAX_FAILURE_MESSAGE_LINES: usize = 3;

/// Default verify execution timeout per profile in seconds.
pub const DEFAULT_VERIFY_TIMEOUT_SECS: u64 = 1800;

/// Conservative relay throughput measured on 2026-11-26 (~0.12 MB/s) for warning estimates.
const RELAY_ESTIMATE_BYTES_PER_SEC: f64 = 0.12 * 1024.0 * 1024.0;

/// Relative directory under repo root where latest verify records are persisted (D4.4).
pub const VERIFY_RECORD_REL_DIR: &str = ".oxideswarm/verify";

/// Default maximum verifies per agent per rolling hour (`design 0111` D5.3 / TEAM-4).
pub const DEFAULT_MAX_VERIFIES_PER_HOUR: usize = 20;

/// Rolling window duration (seconds) for the per-agent verify rate limit.
pub const VERIFY_RATE_WINDOW_SECS: i64 = 3600;

/// File name under `.oxideswarm/verify/` storing per-agent verify timestamps.
pub const VERIFY_RATE_LIMIT_FILE: &str = "rate_limit.json";

/// Environment variable overriding [`DEFAULT_MAX_VERIFIES_PER_HOUR`].
pub const MAX_VERIFIES_PER_HOUR_ENV: &str = "OXIDESWARM_MAX_VERIFIES_PER_HOUR";

/// Environment variable identifying the Agent Hub root for blocked-task notices.
pub const AGENT_HUB_ENV: &str = "OXIDESWARM_AGENT_HUB";

/// CLI arguments for `oxideswarm verify`.
#[derive(Args, Debug, Clone)]
pub struct VerifyGateArgs {
    #[arg(
        long,
        value_name = "ID",
        help = "Task ID defined in .oxideswarm/plan.toml"
    )]
    pub task: String,

    #[arg(
        long = "profile",
        value_name = "NAME",
        help = "Run only these verify profile(s) from the task (repeatable; defaults to all profiles in task.verify)"
    )]
    pub profiles: Vec<String>,

    #[arg(long, value_name = "PATH", help = "Path to .oxideswarm/plan.toml")]
    pub plan: Option<PathBuf>,

    #[arg(
        long,
        value_name = "DIR",
        help = "Git worktree / repository directory (default: current directory)"
    )]
    pub repo: Option<PathBuf>,

    #[arg(
        long,
        value_name = "REF",
        help = "Base git branch/ref overriding [project].base"
    )]
    pub base: Option<String>,

    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088, auto, or P2P ticket)"
    )]
    pub master: Option<String>,

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

    #[arg(
        long,
        help = "Continue WITHOUT a cluster secret when connecting to a remote or P2P master"
    )]
    pub insecure: bool,

    #[arg(
        long,
        value_name = "SECS",
        default_value_t = DEFAULT_VERIFY_TIMEOUT_SECS,
        help = "Maximum seconds to wait for each profile's verify execution"
    )]
    pub timeout_secs: u64,

    #[arg(
        long,
        value_name = "N",
        help = "Maximum files scanned when syncing the worktree to worker workspaces"
    )]
    pub workspace_max_files: Option<usize>,

    #[arg(
        long,
        value_name = "MB",
        help = "Maximum total MiB scanned when syncing the worktree to worker workspaces"
    )]
    pub workspace_max_mb: Option<u64>,

    #[arg(
        long,
        value_name = "REMOTE",
        default_value = DEFAULT_REMOTE,
        help = "Git remote hosting refs/oxideswarm/lease/* for circuit-breaker updates"
    )]
    pub remote: String,

    #[arg(
        long,
        value_name = "AGENT_ID",
        help = "Calling agent identifier used for hourly rate limiting and lease checks"
    )]
    pub agent: Option<String>,

    #[arg(
        long,
        value_name = "N",
        help = "Maximum verify runs per agent per rolling hour (default 20, or OXIDESWARM_MAX_VERIFIES_PER_HOUR)"
    )]
    pub max_verifies_per_hour: Option<usize>,

    #[arg(
        long,
        value_name = "DIR",
        help = "Agent Hub directory for blocked-task notices (defaults to OXIDESWARM_AGENT_HUB if set)"
    )]
    pub hub_dir: Option<PathBuf>,

    #[arg(long, help = "Print only the compact <= 4 KiB verify JSON report")]
    pub json: bool,
}

/// Structured failure category in a verify cell report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyFailureKind {
    Compile,
    Test,
    Command,
    Unplaced,
}

impl VerifyFailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Compile => "compile",
            Self::Test => "test",
            Self::Command => "command",
            Self::Unplaced => "unplaced",
        }
    }
}

/// One extracted compiler diagnostic, test failure, or command failure (`<= 3` message lines).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyFailure {
    pub kind: VerifyFailureKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    pub message: String,
}

/// Execution summary for one `(profile, matrix cell, shard)` run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifyCellReport {
    pub profile: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub toolchain: Option<String>,
    pub exit: i32,
    pub secs: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<VerifyFailure>,
}

/// Compact verify gate report returned by `oxideswarm verify` and MCP `verify` (`<= 4096` bytes).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifyGateReport {
    pub task: String,
    pub commit: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub blocked: bool,
    pub untrusted_data: bool,
    pub cells: Vec<VerifyCellReport>,
    pub omitted: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Persistent verification record saved under `.oxideswarm/verify/<task>.json` (design 0111 D4.4 / TEAM-4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifyRecord {
    pub task: String,
    pub commit: String,
    pub ok: bool,
    #[serde(default)]
    pub blocked: bool,
    pub timestamp_utc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_fingerprint: Option<String>,
    pub cells: Vec<VerifyCellReport>,
}

/// Computes a stable SHA-256 hex fingerprint for a failure (`design 0111` D5 / TEAM-4).
pub fn failure_fingerprint(failure: &VerifyFailure) -> String {
    let normalized_msg = normalize_for_fingerprint(&failure.message);
    let file_part = failure.file.as_deref().unwrap_or("");
    let payload = format!(
        "{}\0{}\0{}",
        failure.kind.as_str(),
        file_part.trim(),
        normalized_msg
    );
    let digest = ring::digest::digest(&ring::digest::SHA256, payload.as_bytes());
    hex::encode_slice(digest.as_ref())
}

mod hex {
    pub(super) fn encode_slice(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            use std::fmt::Write;
            let _ = write!(&mut out, "{b:02x}");
        }
        out
    }
}

fn normalize_for_fingerprint(text: &str) -> String {
    let mut tokens = Vec::new();
    for raw in text.split_whitespace() {
        let low = raw.to_ascii_lowercase();
        let trimmed = low.trim_matches(|c: char| !c.is_ascii_alphanumeric());
        if trimmed.starts_with("0x")
            && trimmed.len() > 2
            && trimmed[2..].chars().all(|c| c.is_ascii_hexdigit())
        {
            tokens.push(low.replacen(trimmed, "<hex>", 1));
        } else {
            tokens.push(low);
        }
    }
    tokens.join(" ")
}

/// Clamps `text` to at most `max_lines` non-empty trimmed lines and at most `max_chars` characters.
fn clamp_message(text: &str, max_lines: usize, max_chars: usize) -> String {
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim_end)
        .filter(|l| !l.trim().is_empty())
        .collect();
    let take_lines = lines.len().min(max_lines);
    let joined = lines[..take_lines].join("\n");
    truncate_chars(&joined, max_chars)
}

fn truncate_chars(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let mut out: String = s.chars().take(keep).collect();
    out.push_str("...");
    out
}

/// Rewrites `cargo check|build|clippy|test` invocations to include `--message-format=json`
/// before any `--` argument separator when no `--message-format` flag is already present.
pub fn augment_cargo_command(command: &[String]) -> Vec<String> {
    if command.len() < 2 {
        return command.to_vec();
    }
    let bin = Path::new(&command[0])
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(&command[0]);
    if !bin.eq_ignore_ascii_case("cargo") && !bin.eq_ignore_ascii_case("cargo.exe") {
        return command.to_vec();
    }
    let sub = command[1].as_str();
    if !matches!(sub, "check" | "build" | "clippy" | "test") {
        return command.to_vec();
    }
    if command
        .iter()
        .any(|a| a == "--message-format" || a.starts_with("--message-format="))
    {
        return command.to_vec();
    }

    let mut out = Vec::with_capacity(command.len() + 1);
    let mut inserted = false;
    for (idx, arg) in command.iter().enumerate() {
        if !inserted && arg == "--" {
            out.push("--message-format=json".to_string());
            inserted = true;
        }
        out.push(arg.clone());
        if idx == 1 && !inserted && command.len() == 2 {
            out.push("--message-format=json".to_string());
            inserted = true;
        }
    }
    if !inserted {
        out.push("--message-format=json".to_string());
    }
    out
}

/// Parses `cargo --message-format=json` lines for compiler errors.
pub fn parse_cargo_json_failures(output: &str) -> Vec<VerifyFailure> {
    let mut concrete = Vec::new();
    let mut summary = Vec::new();

    for line in output.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('{') {
            continue;
        }
        let Ok(val) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if val.get("reason").and_then(Value::as_str) != Some("compiler-message") {
            continue;
        }
        let Some(msg) = val.get("message") else {
            continue;
        };
        if msg.get("level").and_then(Value::as_str) != Some("error") {
            continue;
        }
        let raw_text = msg
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if raw_text.is_empty() {
            continue;
        }
        let code = msg
            .get("code")
            .and_then(|c| c.get("code"))
            .and_then(Value::as_str)
            .map(str::to_string);

        let primary_span = msg
            .get("spans")
            .and_then(Value::as_array)
            .and_then(|spans| {
                spans
                    .iter()
                    .find(|s| s.get("is_primary").and_then(Value::as_bool) == Some(true))
                    .or_else(|| spans.first())
            });
        let file = primary_span
            .and_then(|s| s.get("file_name"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let line_no = primary_span
            .and_then(|s| s.get("line_start"))
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok());

        let formatted_msg = if let Some(ref c) = code {
            format!("{c}: {raw_text}")
        } else {
            raw_text.to_string()
        };
        let failure = VerifyFailure {
            kind: VerifyFailureKind::Compile,
            name: code,
            file,
            line: line_no,
            message: clamp_message(&formatted_msg, MAX_FAILURE_MESSAGE_LINES, 240),
        };
        if raw_text.starts_with("aborting due to ") || raw_text.starts_with("could not compile ") {
            summary.push(failure);
        } else {
            concrete.push(failure);
        }
    }

    if concrete.is_empty() {
        summary
    } else {
        concrete
    }
}

fn parse_panicked_location(line: &str) -> (Option<String>, Option<u32>, Option<String>) {
    // Handles: "thread '...' panicked at crates/foo/src/lib.rs:42:9:" and optionally message after colon.
    let Some(pos) = line.find("panicked at ") else {
        return (None, None, None);
    };
    let rest = line[pos + "panicked at ".len()..].trim();
    // Format in Rust >= 1.73: `path:line:col:\nmsg` (no quotes). Older: `'msg', path:line:col`.
    let loc_str = if let Some(after_quote) = rest.strip_prefix('\'') {
        if let Some(end_q) = after_quote.find("', ") {
            let msg_inline = after_quote[..end_q].to_string();
            let loc = after_quote[end_q + 3..].trim();
            let (f, l) = split_file_line(loc);
            return (f, l, Some(msg_inline));
        }
        rest
    } else {
        rest.trim_end_matches(':')
    };
    let (file, line_no) = split_file_line(loc_str);
    (file, line_no, None)
}

fn split_file_line(raw: &str) -> (Option<String>, Option<u32>) {
    let trimmed = raw.trim().trim_end_matches(':');
    let parts: Vec<&str> = trimmed.rsplitn(3, ':').collect();
    match parts.as_slice() {
        [col, line, file]
            if col.chars().all(|c| c.is_ascii_digit())
                && !line.is_empty()
                && line.chars().all(|c| c.is_ascii_digit()) =>
        {
            (Some((*file).to_string()), line.parse::<u32>().ok())
        }
        [line, file] if !line.is_empty() && line.chars().all(|c| c.is_ascii_digit()) => {
            (Some((*file).to_string()), line.parse::<u32>().ok())
        }
        _ => (None, None),
    }
}

/// Parses standard `cargo test` / libtest stdout for failed tests and panic details.
pub fn parse_libtest_failures(output: &str) -> Vec<VerifyFailure> {
    let mut failures = Vec::new();
    let lines: Vec<&str> = output.lines().collect();
    let mut i = 0usize;

    while i < lines.len() {
        let trimmed = lines[i].trim();
        if let Some(rest) = trimmed.strip_prefix("---- ") {
            if let Some(test_name) = rest.strip_suffix(" stdout ----") {
                let mut file = None;
                let mut line_no = None;
                let mut msg_lines = Vec::new();
                i += 1;
                while i < lines.len() {
                    let cur = lines[i].trim();
                    if cur.starts_with("---- ") || cur == "failures:" {
                        break;
                    }
                    if cur.starts_with("note: run with `RUST_BACKTRACE") {
                        i += 1;
                        continue;
                    }
                    if cur.contains("panicked at ") {
                        let (f, l, inline_msg) = parse_panicked_location(cur);
                        if file.is_none() {
                            file = f;
                            line_no = l;
                        }
                        if let Some(m) = inline_msg {
                            msg_lines.push(m);
                        } else if msg_lines.is_empty() {
                            // Keep next lines (actual panic message); skip wrapper if actual follows.
                            i += 1;
                            continue;
                        }
                    } else if !cur.is_empty() && msg_lines.len() < MAX_FAILURE_MESSAGE_LINES {
                        msg_lines.push(cur.to_string());
                    }
                    i += 1;
                }
                let message = if msg_lines.is_empty() {
                    format!("test {test_name} failed")
                } else {
                    clamp_message(&msg_lines.join("\n"), MAX_FAILURE_MESSAGE_LINES, 240)
                };
                failures.push(VerifyFailure {
                    kind: VerifyFailureKind::Test,
                    name: Some(test_name.to_string()),
                    file,
                    line: line_no,
                    message,
                });
                continue;
            }
        }
        i += 1;
    }

    if failures.is_empty() {
        for line in &lines {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("test ") {
                if let Some(name) = rest.strip_suffix(" ... FAILED") {
                    failures.push(VerifyFailure {
                        kind: VerifyFailureKind::Test,
                        name: Some(name.trim().to_string()),
                        file: None,
                        line: None,
                        message: format!("test {} failed", name.trim()),
                    });
                }
            }
        }
    }

    failures
}

/// Parses `cargo nextest run` failure lines (`FAIL [ ... ] crate test_name`) and stdout sections.
pub fn parse_nextest_failures(output: &str) -> Vec<VerifyFailure> {
    let mut by_name: BTreeMap<String, VerifyFailure> = BTreeMap::new();
    let lines: Vec<&str> = output.lines().collect();
    let mut i = 0usize;

    while i < lines.len() {
        let trimmed = lines[i].trim();
        if trimmed.starts_with("FAIL [") {
            if let Some(after_bracket) = trimmed.split_once(']').map(|x| x.1.trim()) {
                let test_name = after_bracket
                    .split_whitespace()
                    .last()
                    .unwrap_or(after_bracket)
                    .to_string();
                by_name
                    .entry(test_name.clone())
                    .or_insert_with(|| VerifyFailure {
                        kind: VerifyFailureKind::Test,
                        name: Some(test_name),
                        file: None,
                        line: None,
                        message: trimmed.to_string(),
                    });
            }
        } else if (trimmed.starts_with("--- STDOUT:") || trimmed.starts_with("--- STDERR:"))
            && trimmed.ends_with("---")
        {
            let inner = trimmed
                .trim_start_matches("--- STDOUT:")
                .trim_start_matches("--- STDERR:")
                .trim_end_matches("---")
                .trim();
            let test_name = inner.split_whitespace().last().unwrap_or(inner).to_string();
            let mut file = None;
            let mut line_no = None;
            let mut msg_lines = Vec::new();
            i += 1;
            while i < lines.len() {
                let cur = lines[i].trim();
                if cur.starts_with("--- ") || cur.starts_with("FAIL [") || cur.starts_with("PASS [")
                {
                    break;
                }
                if cur.contains("panicked at ") {
                    let (f, l, inline_msg) = parse_panicked_location(cur);
                    if file.is_none() {
                        file = f;
                        line_no = l;
                    }
                    if let Some(m) = inline_msg {
                        msg_lines.push(m);
                    }
                    i += 1;
                    continue;
                }
                if !cur.is_empty() && msg_lines.len() < MAX_FAILURE_MESSAGE_LINES {
                    msg_lines.push(cur.to_string());
                }
                i += 1;
            }
            let entry = by_name
                .entry(test_name.clone())
                .or_insert_with(|| VerifyFailure {
                    kind: VerifyFailureKind::Test,
                    name: Some(test_name),
                    file: None,
                    line: None,
                    message: String::new(),
                });
            if entry.file.is_none() {
                entry.file = file;
                entry.line = line_no;
            }
            if !msg_lines.is_empty() {
                entry.message =
                    clamp_message(&msg_lines.join("\n"), MAX_FAILURE_MESSAGE_LINES, 240);
            }
            continue;
        }
        i += 1;
    }

    by_name.into_values().collect()
}

/// Extracts structured failures from combined task stdout/stderr/error and applies per-cell caps.
pub fn extract_failures(
    exit_code: i32,
    stdout: &str,
    stderr: &str,
    error: Option<&str>,
) -> (Vec<VerifyFailure>, usize) {
    if exit_code == 0 {
        return (Vec::new(), 0);
    }

    let combined = format!("{stdout}\n{stderr}");
    let mut parsed = parse_cargo_json_failures(&combined);
    let test_failures = parse_libtest_failures(&combined);
    for tf in test_failures {
        if !parsed
            .iter()
            .any(|p| p.kind == tf.kind && p.name == tf.name)
        {
            parsed.push(tf);
        }
    }
    let nextest_failures = parse_nextest_failures(&combined);
    for nf in nextest_failures {
        if !parsed
            .iter()
            .any(|p| p.kind == nf.kind && p.name == nf.name)
        {
            parsed.push(nf);
        }
    }

    if parsed.is_empty() {
        let fallback_src = if !stderr.trim().is_empty() {
            stderr
        } else if !stdout.trim().is_empty() {
            stdout
        } else {
            error.unwrap_or("command failed")
        };
        let lines: Vec<&str> = fallback_src
            .lines()
            .map(str::trim_end)
            .filter(|l| !l.trim().is_empty())
            .collect();
        let tail_start = lines.len().saturating_sub(MAX_FAILURE_MESSAGE_LINES);
        let msg = truncate_chars(&lines[tail_start..].join("\n"), 240);
        parsed.push(VerifyFailure {
            kind: VerifyFailureKind::Command,
            name: None,
            file: None,
            line: None,
            message: if msg.is_empty() {
                format!("exit code {exit_code}")
            } else {
                msg
            },
        });
    }

    let total = parsed.len();
    let kept: Vec<VerifyFailure> = parsed.into_iter().take(MAX_FAILURES_PER_CELL).collect();
    let omitted = total.saturating_sub(kept.len());
    (kept, omitted)
}

/// Enforces `<= MAX_FAILURES_PER_CELL`, `<= 3` message lines, and `serialized_len <= max_bytes`
/// (`design 0111` D4.3), tracking dropped failures/cells in `report.omitted`.
pub fn fit_report_to_budget(report: &mut VerifyGateReport, max_bytes: usize) -> String {
    for cell in &mut report.cells {
        for f in &mut cell.failures {
            f.message = clamp_message(&f.message, MAX_FAILURE_MESSAGE_LINES, 240);
        }
        if cell.failures.len() > MAX_FAILURES_PER_CELL {
            let drop_n = cell.failures.len() - MAX_FAILURES_PER_CELL;
            cell.failures.truncate(MAX_FAILURES_PER_CELL);
            report.omitted = report.omitted.saturating_add(drop_n);
        }
    }

    let serialize = |r: &VerifyGateReport| -> String {
        serde_json::to_string(r).unwrap_or_else(|_| {
            r#"{"task":"","commit":"","ok":false,"untrusted_data":true,"cells":[],"omitted":0}"#
                .to_string()
        })
    };

    let mut json = serialize(report);
    if json.len() <= max_bytes {
        return json;
    }

    // 1. Drop advisory warnings first to preserve failure details.
    while !report.warnings.is_empty() && json.len() > max_bytes {
        report.warnings.pop();
        json = serialize(report);
    }

    // 2. Drop excess failures from cells starting from the tail, keeping >= 1 failure per failing cell if possible.
    while json.len() > max_bytes {
        let mut trimmed = false;
        for cell in report.cells.iter_mut().rev() {
            if cell.failures.len() > 1 {
                cell.failures.pop();
                report.omitted = report.omitted.saturating_add(1);
                trimmed = true;
                break;
            }
        }
        if !trimmed {
            break;
        }
        json = serialize(report);
    }

    // 3. Shorten remaining failure messages progressively.
    for limit in [120usize, 64, 32] {
        if json.len() <= max_bytes {
            break;
        }
        for cell in &mut report.cells {
            for f in &mut cell.failures {
                f.message = clamp_message(&f.message, 1, limit);
            }
        }
        json = serialize(report);
    }

    // 4. Drop remaining passing cells, then failing cells if needed to guarantee <= max_bytes.
    while json.len() > max_bytes && !report.cells.is_empty() {
        if let Some(pos) = report.cells.iter().rposition(|c| c.exit == 0) {
            report.cells.remove(pos);
            report.omitted = report.omitted.saturating_add(1);
        } else if let Some(cell) = report
            .cells
            .iter_mut()
            .rev()
            .find(|c| !c.failures.is_empty())
        {
            cell.failures.pop();
            report.omitted = report.omitted.saturating_add(1);
        } else {
            report.cells.pop();
            report.omitted = report.omitted.saturating_add(1);
        }
        json = serialize(report);
    }

    json
}

/// Computes `HEAD` short SHA plus `+dirty-<sha8>` over uncommitted modifications when dirty.
pub fn compute_worktree_commit(repo_dir: &Path) -> Result<String, String> {
    let head = Command::new("git")
        .arg("-C")
        .arg(repo_dir)
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .map_err(|e| format!("failed to run git rev-parse: {e}"))?;
    if !head.status.success() {
        return Err(format!(
            "git rev-parse HEAD failed: {}",
            String::from_utf8_lossy(&head.stderr).trim()
        ));
    }
    let head_sha = String::from_utf8_lossy(&head.stdout).trim().to_string();

    let status = Command::new("git")
        .arg("-C")
        .arg(repo_dir)
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .output()
        .map_err(|e| format!("failed to run git status: {e}"))?;
    if !status.status.success() {
        return Err(format!(
            "git status failed: {}",
            String::from_utf8_lossy(&status.stderr).trim()
        ));
    }

    let dirty_paths: Vec<String> = parse_porcelain_paths(&String::from_utf8_lossy(&status.stdout))
        .into_iter()
        .filter(|p| !is_verify_record_path(p))
        .collect();
    if dirty_paths.is_empty() {
        return Ok(head_sha);
    }

    let mut hash_buf = Vec::new();
    for rel in &dirty_paths {
        hash_buf.extend_from_slice(rel.as_bytes());
        hash_buf.push(0);
        let full = repo_dir.join(rel);
        if let Ok(bytes) = std::fs::read(&full) {
            hash_buf.extend_from_slice(&bytes);
        }
        hash_buf.push(0xff);
    }
    let digest = ring::digest::digest(&ring::digest::SHA256, &hash_buf);
    let hex_full = hex::encode_slice(digest.as_ref());
    Ok(format!("{head_sha}+dirty-{}", &hex_full[..8]))
}

fn is_verify_record_path(rel: &str) -> bool {
    let norm = rel.replace('\\', "/");
    norm == ".oxideswarm/verify" || norm.starts_with(".oxideswarm/verify/")
}

fn parse_porcelain_paths(raw: &str) -> Vec<String> {
    let mut paths = Vec::new();
    for line in raw.lines() {
        if line.len() < 4 {
            continue;
        }
        let body = &line[3..];
        let target = body
            .split_once(" -> ")
            .map(|(_, new)| new)
            .unwrap_or(body)
            .trim()
            .trim_matches('"');
        if !target.is_empty() {
            paths.push(target.replace('\\', "/"));
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

/// Checks changed paths (`<base>...HEAD` + working tree excluding `.oxideswarm/verify/**`)
/// against `owned_paths` (`design 0111` D3.6). Empty `owned_paths` imposes no restriction.
pub fn check_owned_paths_discipline(
    repo_dir: &Path,
    base_ref: &str,
    owned_paths: &[String],
) -> Result<(), String> {
    if owned_paths.is_empty() {
        return Ok(());
    }
    let mut changed = BTreeSet::new();

    if !base_ref.trim().is_empty() {
        let range = format!("{base_ref}...HEAD");
        if let Ok(out) = Command::new("git")
            .arg("-C")
            .arg(repo_dir)
            .args(["diff", "--name-only", &range])
            .output()
        {
            if out.status.success() {
                for line in String::from_utf8_lossy(&out.stdout).lines() {
                    let p = line.trim().replace('\\', "/");
                    if !p.is_empty() {
                        changed.insert(p);
                    }
                }
            }
        }
    }

    let status = Command::new("git")
        .arg("-C")
        .arg(repo_dir)
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .output()
        .map_err(|e| format!("failed to run git status: {e}"))?;
    if status.status.success() {
        for p in parse_porcelain_paths(&String::from_utf8_lossy(&status.stdout)) {
            changed.insert(p);
        }
    }

    let violations: Vec<String> = changed
        .into_iter()
        .filter(|p| !is_verify_record_path(p))
        .filter(|p| !owned_paths.iter().any(|pat| path_matches_pattern(p, pat)))
        .collect();

    if violations.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "diff touches path(s) outside task owned_paths: {}",
            violations.join(", ")
        ))
    }
}

fn placement_from_require(require: &[String]) -> Result<Placement, String> {
    let tags: Vec<String> = require
        .iter()
        .map(|raw| {
            let trimmed = raw.trim();
            if trimmed.starts_with(REQUIRE_PREFIX) {
                trimmed.to_string()
            } else {
                format!("{REQUIRE_PREFIX}{trimmed}")
            }
        })
        .collect();
    Placement::from_task_tags(&tags)
}

fn extract_link_kind(w: &WorkerCapabilities) -> String {
    let Some(link) = tag_value(&w.tags, LINK_TAG_PREFIX) else {
        return "unknown".to_string();
    };
    let lower = link.to_ascii_lowercase();
    if lower.starts_with("direct") {
        "direct".to_string()
    } else if lower.starts_with("relay") {
        "relay".to_string()
    } else if lower.starts_with("tcp") {
        "tcp".to_string()
    } else {
        link.to_string()
    }
}

fn is_direct_like_link(link: &str) -> bool {
    matches!(link, "direct" | "tcp")
}

fn extract_toolchain(w: &WorkerCapabilities) -> Option<String> {
    let os = tag_value(&w.tags, OS_TAG_PREFIX).map(normalize_os);
    let arch = tag_value(&w.tags, ARCH_TAG_PREFIX).map(normalize_arch);
    let rustc = w.tags.iter().find_map(|t| {
        t.strip_prefix(TOOL_TAG_PREFIX)
            .filter(|v| v.starts_with("rustc@") || *v == "rustc")
    });
    let mut parts = Vec::new();
    if let Some(r) = rustc {
        parts.push(r.replace('@', " "));
    }
    match (arch, os) {
        (Some(a), Some(o)) => parts.push(format!("{a}-{o}")),
        (Some(a), None) => parts.push(a),
        (None, Some(o)) => parts.push(o),
        (None, None) => {}
    }
    (!parts.is_empty()).then(|| parts.join(" "))
}

fn select_link_preference(
    cell: &MatrixCell,
    workers: &[WorkerCapabilities],
    reqs: &TaskRequirements,
    has_workspace: bool,
    sync_bytes: u64,
) -> (Option<Uuid>, Option<String>) {
    let matching: Vec<&WorkerCapabilities> = workers
        .iter()
        .filter(|w| crate::matrix::worker_matches_cell(w, &cell.placement, reqs, has_workspace))
        .collect();
    if matching.is_empty() {
        return (None, None);
    }
    let direct: Vec<&WorkerCapabilities> = matching
        .iter()
        .copied()
        .filter(|w| is_direct_like_link(&extract_link_kind(w)))
        .collect();
    if let Some(best) = direct.first() {
        return (listed_worker_id(best), None);
    }
    if matching.iter().all(|w| extract_link_kind(w) == "relay") && sync_bytes > 0 {
        let est_secs = (sync_bytes as f64 / RELAY_ESTIMATE_BYTES_PER_SEC)
            .ceil()
            .max(1.0);
        return (
            None,
            Some(format!(
                "cell '{}': only relayed worker(s) match; workspace sync ({}) estimated ~{:.0}s at 0.12 MB/s",
                cell.label,
                rusty_grid_worker::staging::human_bytes(sync_bytes),
                est_secs
            )),
        );
    }
    (None, None)
}

fn expand_profile_matrix_cells(
    profile: &VerifyProfile,
    base_placement: &Placement,
) -> Result<Vec<MatrixCell>, String> {
    if profile.matrix.is_empty() {
        return Ok(vec![MatrixCell {
            index: 0,
            assignments: Vec::new(),
            label: "default".to_string(),
            slug: "default".to_string(),
            placement: base_placement.clone(),
            env_overrides: BTreeMap::new(),
        }]);
    }
    let mut axes = Vec::with_capacity(profile.matrix.len());
    for (key, vals) in &profile.matrix {
        let kind = match key.as_str() {
            "os" => MatrixAxisKind::Os,
            "arch" => MatrixAxisKind::Arch,
            "worker" => MatrixAxisKind::Worker,
            "iso" => MatrixAxisKind::Iso,
            "tool" => MatrixAxisKind::Tool,
            "tag" => MatrixAxisKind::Tag,
            other => {
                if let Some(var) = other.strip_prefix("env:") {
                    MatrixAxisKind::Env(var.to_string())
                } else {
                    return Err(format!("unsupported matrix axis '{other}' in profile"));
                }
            }
        };
        axes.push(MatrixAxis {
            key: key.clone(),
            kind,
            values: vals.clone(),
        });
    }
    expand_cells(&MatrixSpec { axes }, base_placement, true)
}

fn shard_matrix_cells(
    cells: Vec<MatrixCell>,
    shards: Option<usize>,
    workers: &[WorkerCapabilities],
    reqs: &TaskRequirements,
    has_workspace: bool,
) -> Vec<(MatrixCell, Option<String>, Option<Uuid>)> {
    let count = shards.unwrap_or(1).clamp(1, HARD_MAX_SHARDS);
    if count <= 1 {
        return cells.into_iter().map(|c| (c, None, None)).collect();
    }

    let eligible: Vec<EligibleWorker> = workers
        .iter()
        .filter(|w| listed_as_connected(w) && (!has_workspace || listed_supports_workspaces(w)))
        .filter(|w| w.cpu_cores >= reqs.cpu_cores && w.ram_mb >= reqs.ram_mb)
        .filter_map(|w| {
            let wid = listed_worker_id(w)?;
            let os = tag_value(&w.tags, OS_TAG_PREFIX)
                .map(normalize_os)
                .unwrap_or_default();
            let arch = tag_value(&w.tags, ARCH_TAG_PREFIX)
                .map(normalize_arch)
                .unwrap_or_default();
            Some(EligibleWorker {
                id: Some(wid),
                name: w.name.clone(),
                target_key: wid.to_string(),
                slots: rusty_grid_core::placement::advertised_slots(w)
                    .unwrap_or(w.cpu_cores)
                    .max(1),
                os,
                arch,
                nextest_version: None,
                has_cargo_nextest: false,
                has_cargo: false,
            })
        })
        .collect();

    let mut out = Vec::with_capacity(cells.len() * count);
    let mut seq = 0usize;
    for cell in cells {
        let targets = spread_shards_swrr(count, &eligible, &cell.placement).unwrap_or_default();
        for shard_idx in 1..=count {
            let slug = shard_slug(shard_idx, count);
            let mut env_overrides = cell.env_overrides.clone();
            env_overrides.insert("OX_SHARD_INDEX".to_string(), shard_idx.to_string());
            env_overrides.insert("OX_SHARD_TOTAL".to_string(), count.to_string());
            let mut sub_assignments = cell.assignments.clone();
            sub_assignments.push(("shard".to_string(), slug.clone()));
            let prefer_wid = targets.get(shard_idx - 1).and_then(|a| a.worker_id);
            out.push((
                MatrixCell {
                    index: seq,
                    assignments: sub_assignments,
                    label: if cell.label == "default" {
                        slug.clone()
                    } else {
                        format!("{},{slug}", cell.label)
                    },
                    slug: format!("{}-{slug}", cell.slug),
                    placement: cell.placement.clone(),
                    env_overrides,
                },
                Some(slug),
                prefer_wid,
            ));
            seq += 1;
        }
    }
    out
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

fn sanitize_workspace_name(raw: &str) -> String {
    let mut cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    cleaned = cleaned
        .trim_start_matches(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit()))
        .trim_end_matches('.')
        .to_string();
    if cleaned.len() > 64 {
        cleaned.truncate(64);
    }
    cleaned = cleaned.trim_end_matches('.').to_string();
    if cleaned.is_empty() {
        return "verify-ws".to_string();
    }
    if rusty_grid_core::task::validate_workspace_name(&cleaned).is_err() {
        cleaned = format!("ws-{cleaned}");
        if cleaned.len() > 64 {
            cleaned.truncate(64);
        }
        cleaned = cleaned.trim_end_matches('.').to_string();
    }
    cleaned
}

fn cell_axis_val(cell: &MatrixCell, key: &str) -> Option<String> {
    cell.assignments
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
}

fn ensure_verify_dir(repo_dir: &Path) -> Result<PathBuf, String> {
    let dot_ox = repo_dir.join(".oxideswarm");
    match std::fs::symlink_metadata(&dot_ox) {
        Ok(m) if m.file_type().is_symlink() => {
            return Err(format!("refusing symlinked dir {}", dot_ox.display()));
        }
        Ok(m) if !m.is_dir() => {
            return Err(format!(
                "path {} exists and is not a directory",
                dot_ox.display()
            ));
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(&dot_ox)
                .map_err(|ce| format!("create dir {}: {ce}", dot_ox.display()))?;
        }
        Err(e) => return Err(format!("inspect dir {}: {e}", dot_ox.display())),
    }
    let dir = dot_ox.join("verify");
    match std::fs::symlink_metadata(&dir) {
        Ok(m) if m.file_type().is_symlink() => {
            return Err(format!("refusing symlinked verify dir {}", dir.display()));
        }
        Ok(m) if !m.is_dir() => {
            return Err(format!(
                "verify path {} exists and is not a directory",
                dir.display()
            ));
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(&dir)
                .map_err(|ce| format!("create verify dir {}: {ce}", dir.display()))?;
        }
        Err(e) => return Err(format!("inspect verify dir {}: {e}", dir.display())),
    }
    Ok(dir)
}

fn write_verify_record(repo_dir: &Path, record: &VerifyRecord) -> Result<(), String> {
    let dir = ensure_verify_dir(repo_dir)?;
    let safe_task = sanitize_record_filename(&record.task);
    let dest = dir.join(format!("{safe_task}.json"));
    if let Ok(meta) = std::fs::symlink_metadata(&dest) {
        if meta.file_type().is_symlink() {
            return Err(format!(
                "refusing symlinked verify record {}",
                dest.display()
            ));
        }
    }
    let tmp = dir.join(format!(".{safe_task}.json.tmp.{}", Uuid::new_v4()));
    let body = serde_json::to_string_pretty(record)
        .map_err(|e| format!("serialize verify record: {e}"))?;
    std::fs::write(&tmp, body).map_err(|e| format!("write temp verify record: {e}"))?;
    std::fs::rename(&tmp, &dest).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("replace verify record {}: {e}", dest.display())
    })
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct VerifyRateLedger {
    #[serde(default)]
    agents: BTreeMap<String, Vec<i64>>,
}

/// Snapshot returned after recording an allowed verify run in the rolling 1-hour ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyRateStatus {
    pub agent_id: String,
    pub used_in_window: usize,
    pub limit: usize,
}

/// Checks and records an agent's verify invocation against the rolling 1-hour cap
/// stored under `.oxideswarm/verify/rate_limit.json` (`design 0111` D5.3 / TEAM-4).
pub fn check_and_record_verify_rate_at(
    repo_dir: &Path,
    agent_id: &str,
    max_per_hour: usize,
    now_utc: DateTime<Utc>,
) -> Result<VerifyRateStatus, String> {
    if max_per_hour == 0 {
        return Err("max_verifies_per_hour must be at least 1".to_string());
    }
    let clean_agent = agent_id.trim();
    let agent = if clean_agent.is_empty() {
        "default"
    } else {
        clean_agent
    };

    let dir = ensure_verify_dir(repo_dir)?;
    let path = dir.join(VERIFY_RATE_LIMIT_FILE);
    let mut ledger = match std::fs::symlink_metadata(&path) {
        Ok(m) if m.file_type().is_symlink() => {
            return Err(format!(
                "refusing symlinked verify rate ledger {}",
                path.display()
            ));
        }
        Ok(_) => {
            let raw = std::fs::read_to_string(&path)
                .map_err(|e| format!("read verify rate ledger {}: {e}", path.display()))?;
            serde_json::from_str::<VerifyRateLedger>(&raw).unwrap_or_default()
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => VerifyRateLedger::default(),
        Err(e) => {
            return Err(format!(
                "inspect verify rate ledger {}: {e}",
                path.display()
            ))
        }
    };

    let now_ts = now_utc.timestamp();
    let cutoff = now_ts - VERIFY_RATE_WINDOW_SECS;
    for timestamps in ledger.agents.values_mut() {
        timestamps.retain(|&ts| ts > cutoff && ts <= now_ts);
    }
    ledger.agents.retain(|_, v| !v.is_empty());

    let entries = ledger.agents.entry(agent.to_string()).or_default();
    if entries.len() >= max_per_hour {
        let oldest = *entries.iter().min().unwrap_or(&now_ts);
        let retry_secs = (oldest + VERIFY_RATE_WINDOW_SECS - now_ts).max(1);
        return Err(format!(
            "verify rate limit exceeded for agent '{agent}' ({}/{} in the last hour); retry in {retry_secs}s",
            entries.len(),
            max_per_hour
        ));
    }
    entries.push(now_ts);
    let used_in_window = entries.len();

    let tmp = dir.join(format!(".rate_limit.json.tmp.{}", Uuid::new_v4()));
    let body = serde_json::to_string_pretty(&ledger)
        .map_err(|e| format!("serialize verify rate ledger: {e}"))?;
    std::fs::write(&tmp, body).map_err(|e| format!("write temp rate ledger: {e}"))?;
    std::fs::rename(&tmp, &path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("replace verify rate ledger {}: {e}", path.display())
    })?;

    Ok(VerifyRateStatus {
        agent_id: agent.to_string(),
        used_in_window,
        limit: max_per_hour,
    })
}

/// Optional circuit-breaker and hourly rate-limit settings for [`run_verify_gate_with_options`].
#[derive(Debug, Clone)]
pub struct VerifyGateOptions {
    pub remote: String,
    pub agent_id: Option<String>,
    pub max_verifies_per_hour: usize,
    pub hub_dir: Option<PathBuf>,
    pub breaker_threshold: u32,
}

impl Default for VerifyGateOptions {
    fn default() -> Self {
        Self {
            remote: DEFAULT_REMOTE.to_string(),
            agent_id: None,
            max_verifies_per_hour: DEFAULT_MAX_VERIFIES_PER_HOUR,
            hub_dir: None,
            breaker_threshold: CIRCUIT_BREAKER_THRESHOLD,
        }
    }
}

/// Resolves the hourly verify cap across CLI/direct override and `OXIDESWARM_MAX_VERIFIES_PER_HOUR`.
pub fn resolve_max_verifies_per_hour(override_val: Option<usize>) -> Result<usize, String> {
    let val = match override_val {
        Some(v) => v,
        None => match std::env::var(MAX_VERIFIES_PER_HOUR_ENV) {
            Ok(raw) if !raw.trim().is_empty() => raw
                .trim()
                .parse::<usize>()
                .map_err(|_| format!("invalid integer '{raw}' for {MAX_VERIFIES_PER_HOUR_ENV}"))?,
            _ => DEFAULT_MAX_VERIFIES_PER_HOUR,
        },
    };
    if val == 0 {
        return Err("max_verifies_per_hour must be at least 1".to_string());
    }
    Ok(val)
}

/// Resolves the Agent Hub root from an explicit override or `OXIDESWARM_AGENT_HUB`.
pub fn resolve_hub_dir(override_dir: Option<PathBuf>) -> Option<PathBuf> {
    if let Some(d) = override_dir {
        return Some(d);
    }
    std::env::var_os(AGENT_HUB_ENV)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn summarize_failure(cell: &VerifyCellReport) -> Option<String> {
    let f = cell.failures.first()?;
    let line = f.message.lines().next().unwrap_or("").trim();
    let mut parts = Vec::new();
    if let Some(ref name) = f.name {
        parts.push(name.clone());
    } else if let Some(ref file) = f.file {
        if let Some(ln) = f.line {
            parts.push(format!("{file}:{ln}"));
        } else {
            parts.push(file.clone());
        }
    }
    if !line.is_empty() {
        parts.push(line.to_string());
    }
    (!parts.is_empty()).then(|| parts.join(": "))
}

#[allow(clippy::too_many_arguments)]
fn finalize_verify_gate(
    repo_dir: &Path,
    options: &VerifyGateOptions,
    task_id: &str,
    mut report: VerifyGateReport,
    first_fingerprint: Option<String>,
    failure_summary: Option<String>,
) -> Result<(VerifyGateReport, String), String> {
    let lease_update = record_verify_outcome(
        repo_dir,
        &RecordVerifyOptions {
            task_id: task_id.to_string(),
            remote: options.remote.clone(),
            holder: options.agent_id.clone(),
            ok: report.ok,
            fingerprint: first_fingerprint.clone(),
            failure_summary,
            threshold: options.breaker_threshold,
            ttl_minutes: DEFAULT_LEASE_TTL_MINS,
        },
    )?;

    if let VerifyLeaseUpdate::Updated(outcome) = lease_update {
        if outcome.newly_blocked {
            report.blocked = true;
            let fp = outcome.last_fingerprint.as_deref().unwrap_or("unknown");
            report.warnings.push(format!(
                "circuit breaker tripped for task '{}' after {} identical failures ({fp}); run `oxideswarm plan unblock {}` to clear",
                outcome.task_id, outcome.verify_failures, outcome.task_id
            ));
            match resolve_hub_dir(options.hub_dir.clone()) {
                Some(hub_dir) => match notify_person_blocked(&hub_dir, &outcome, Utc::now()) {
                    Ok(notice_path) => {
                        report.warnings.push(format!(
                            "notified person '{}' at {}",
                            outcome.person.as_deref().unwrap_or("owner"),
                            notice_path.display()
                        ));
                    }
                    Err(e) => {
                        report
                            .warnings
                            .push(format!("failed sending circuit-breaker person notice: {e}"));
                    }
                },
                None => {
                    report.warnings.push(format!(
                        "circuit breaker blocked '{}', but no hub dir is configured (--hub-dir or {AGENT_HUB_ENV}); person notice skipped",
                        outcome.task_id
                    ));
                }
            }
        }
    }

    let compact_json = fit_report_to_budget(&mut report, MAX_VERIFY_JSON_BYTES);
    write_verify_record(
        repo_dir,
        &VerifyRecord {
            task: report.task.clone(),
            commit: report.commit.clone(),
            ok: report.ok,
            blocked: report.blocked,
            timestamp_utc: Utc::now().to_rfc3339(),
            first_fingerprint,
            cells: report.cells.clone(),
        },
    )?;
    Ok((report, compact_json))
}

/// Executes the verify gate for `--task <id>` in `repo_dir` against `master_addr`.
#[allow(clippy::too_many_arguments)]
pub async fn run_verify_gate(
    repo_dir: &Path,
    plan_override: Option<&Path>,
    task_id: &str,
    profile_filter: &[String],
    base_override: Option<&str>,
    master_addr: &str,
    secret: Option<ClusterSecret>,
    timeout: Duration,
    workspace_limits: WorkspaceLimits,
) -> Result<(VerifyGateReport, String), String> {
    run_verify_gate_with_options(
        repo_dir,
        plan_override,
        task_id,
        profile_filter,
        base_override,
        master_addr,
        secret,
        timeout,
        workspace_limits,
        VerifyGateOptions::default(),
    )
    .await
}

/// Executes the verify gate with explicit circuit-breaker and hourly rate-limit options (TEAM-4).
#[allow(clippy::too_many_arguments)]
pub async fn run_verify_gate_with_options(
    repo_dir: &Path,
    plan_override: Option<&Path>,
    task_id: &str,
    profile_filter: &[String],
    base_override: Option<&str>,
    master_addr: &str,
    secret: Option<ClusterSecret>,
    timeout: Duration,
    workspace_limits: WorkspaceLimits,
    options: VerifyGateOptions,
) -> Result<(VerifyGateReport, String), String> {
    validate_remote_name(&options.remote)?;
    if let Some(ref agent) = options.agent_id {
        validate_holder(agent)?;
    }
    if options.max_verifies_per_hour == 0 {
        return Err("max_verifies_per_hour must be at least 1".to_string());
    }

    let plan_path = resolve_plan_path(repo_dir, plan_override);
    let plan = load_plan(&plan_path)?;
    let diags = validate_plan(&plan);
    if !diags.is_empty() {
        return Err(format!(
            "plan validation failed ({} error(s)): {}",
            diags.len(),
            diags[0].message
        ));
    }

    let task = plan
        .tasks
        .iter()
        .find(|t| t.id == task_id)
        .ok_or_else(|| format!("task '{task_id}' not found in {}", plan_path.display()))?;

    let base_ref = base_override.unwrap_or(&plan.project.base);
    check_owned_paths_discipline(repo_dir, base_ref, &task.owned_paths)?;

    let lease = lookup_task_lease(repo_dir, &options.remote, task_id)?;
    if let Some(ref entry) = lease {
        if entry.record.blocked {
            let fp = entry
                .record
                .last_fingerprint
                .as_deref()
                .unwrap_or("unknown");
            return Err(format!(
                "task '{}' is blocked by the circuit breaker after {} identical failure(s) (fingerprint {fp}); a person must run `oxideswarm plan unblock {}`",
                task.id, entry.record.verify_failures, task.id
            ));
        }
        if let Some(ref agent) = options.agent_id {
            if agent != &entry.record.holder {
                return Err(format!(
                    "cannot verify task '{}': active lease is held by '{}', not '{agent}'",
                    task.id, entry.record.holder
                ));
            }
        }
    }

    let effective_agent = options
        .agent_id
        .clone()
        .or_else(|| lease.as_ref().map(|l| l.record.holder.clone()))
        .or_else(|| {
            std::env::var("OXIDESWARM_AGENT_ID")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "default".to_string());
    check_and_record_verify_rate_at(
        repo_dir,
        &effective_agent,
        options.max_verifies_per_hour,
        Utc::now(),
    )?;

    let commit = compute_worktree_commit(repo_dir)?;

    let selected_profiles: Vec<String> = if profile_filter.is_empty() {
        task.verify.clone()
    } else {
        for p in profile_filter {
            if !plan.profiles.contains_key(p) {
                return Err(format!(
                    "profile '{p}' not defined in {}",
                    plan_path.display()
                ));
            }
        }
        profile_filter.to_vec()
    };

    if selected_profiles.is_empty() {
        let report = VerifyGateReport {
            task: task.id.clone(),
            commit: commit.clone(),
            ok: true,
            blocked: false,
            untrusted_data: true,
            cells: Vec::new(),
            omitted: 0,
            warnings: vec!["task has no verify profiles configured".to_string()],
        };
        return finalize_verify_gate(repo_dir, &options, &task.id, report, None, None);
    }

    let scan = scan_workspace_source(repo_dir, workspace_limits)?;
    let sync_bytes = scan.prepared.bytes;
    let project_name = repo_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("project");
    let ws_name = sanitize_workspace_name(&format!("{project_name}-{}", task.id));
    let workspace_ref = WorkspaceRef::new(ws_name).with_tree(scan.manifest_id, scan.manifest_len);

    let mut transport = connect_master(master_addr, secret.as_ref())
        .await
        .map_err(|e| format!("Failed to connect to Master at {master_addr}: {e}"))?;
    let rpc_timeout = Duration::from_secs(10);
    let workers = match tokio::time::timeout(rpc_timeout, async {
        transport.send_msg(&ClientMessage::ListWorkers).await?;
        transport.recv_msg::<ClientResponse>().await
    })
    .await
    {
        Ok(Ok(Some(ClientResponse::WorkerList { workers }))) => workers,
        Ok(Ok(Some(ClientResponse::Error { message }))) => {
            transport.close().await;
            return Err(format!("Master rejected ListWorkers: {message}"));
        }
        Ok(Ok(other)) => {
            transport.close().await;
            return Err(format!("Unexpected Master ListWorkers response: {other:?}"));
        }
        Ok(Err(e)) => {
            transport.close().await;
            return Err(format!("Failed to list workers: {e}"));
        }
        Err(_) => {
            transport.close().await;
            return Err("Timed out querying Master worker list".to_string());
        }
    };
    transport.close().await;

    let mut warnings = Vec::new();
    let mut seen_warnings = HashSet::new();

    struct ProfileBatch {
        profile_name: String,
        prefer_worker: Option<Uuid>,
        template: Task,
        cells: Vec<MatrixCell>,
        shard_labels: Vec<Option<String>>,
        placeability: Vec<Option<String>>,
    }

    let mut batches: Vec<ProfileBatch> = Vec::new();
    let mut any_placeable = false;

    for prof_name in &selected_profiles {
        let profile = plan
            .profiles
            .get(prof_name)
            .ok_or_else(|| format!("profile '{prof_name}' missing from plan"))?;
        let base_placement = placement_from_require(&profile.require)?;
        let raw_cells = expand_profile_matrix_cells(profile, &base_placement)?;
        let reqs = TaskRequirements::generic(1, timeout.as_secs().max(1)).with_max_retries(0);
        let sharded = shard_matrix_cells(raw_cells, profile.shards, &workers, &reqs, true);

        let cmd_args = augment_cargo_command(&profile.command);
        let mut base_template = Task::new(
            TaskSpec::command(cmd_args[0].clone(), cmd_args[1..].to_vec()),
            reqs.clone(),
        )
        .with_placement(&base_placement);
        base_template.workspace = Some(workspace_ref.clone());

        #[derive(Default)]
        struct GroupedCells {
            cells: Vec<MatrixCell>,
            shard_labels: Vec<Option<String>>,
        }
        let mut grouped: BTreeMap<Option<Uuid>, GroupedCells> = BTreeMap::new();
        for (mut cell, shard_label, shard_pref) in sharded {
            let (link_pref, maybe_warn) =
                select_link_preference(&cell, &workers, &reqs, true, sync_bytes);
            if let Some(w) = maybe_warn {
                if seen_warnings.insert(w.clone()) {
                    warnings.push(w);
                }
            }
            let shard_is_direct = shard_pref.is_some_and(|sid| {
                workers.iter().any(|w| {
                    listed_worker_id(w) == Some(sid) && is_direct_like_link(&extract_link_kind(w))
                })
            });
            let pref = if shard_is_direct {
                shard_pref
            } else {
                link_pref.or(shard_pref)
            };
            let entry = grouped.entry(pref).or_default();
            cell.index = entry.cells.len();
            entry.cells.push(cell);
            entry.shard_labels.push(shard_label);
        }

        for (prefer_worker, group) in grouped {
            let placeability = check_placeability(&group.cells, &workers, &reqs, true);
            if placeability.iter().any(Option::is_none) {
                any_placeable = true;
            }
            let mut template = base_template.clone();
            if let Some(pref_id) = prefer_worker {
                template
                    .tags
                    .push(format!("{PREFER_WORKER_PREFIX}{pref_id}"));
            }
            batches.push(ProfileBatch {
                profile_name: prof_name.clone(),
                prefer_worker,
                template,
                cells: group.cells,
                shard_labels: group.shard_labels,
                placeability,
            });
        }
    }

    if any_placeable {
        let conn = BlobConnection::connect(master_addr, secret.as_ref())
            .await
            .map_err(|e| format!("Failed to connect for workspace upload: {e}"))?;
        let up_res = upload_inputs(&conn, &scan.prepared).await;
        conn.close().await;
        up_res.map_err(|e| format!("Failed uploading workspace blobs: {e}"))?;
    }

    let addr_arc = Arc::new(master_addr.to_string());
    let secret_arc = Arc::new(secret);
    let workers_arc = Arc::new(workers);
    let deadline = safe_deadline(timeout);

    let mut join_set = tokio::task::JoinSet::new();
    for (idx, batch) in batches.into_iter().enumerate() {
        let addr_arc = Arc::clone(&addr_arc);
        let secret_arc = Arc::clone(&secret_arc);
        let workers_arc = Arc::clone(&workers_arc);
        let _pref = batch.prefer_worker;
        join_set.spawn(async move {
            let connect = Arc::new(move || {
                let a = Arc::clone(&addr_arc);
                let s = Arc::clone(&secret_arc);
                async move {
                    connect_master(&a, (*s).as_ref())
                        .await
                        .map_err(|e| e.to_string())
                }
            });
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or_else(|| Duration::from_secs(1));
            let execs = execute_matrix(
                batch.cells,
                &workers_arc,
                batch.placeability,
                &batch.template,
                remaining,
                None,
                None,
                connect,
            )
            .await;
            (idx, batch.profile_name, batch.shard_labels, execs)
        });
    }

    struct BatchOutcome {
        profile_name: String,
        shard_labels: Vec<Option<String>>,
        execs: Vec<crate::matrix::CellExecution>,
    }
    let mut ordered_results: Vec<Option<BatchOutcome>> = Vec::new();
    while let Some(joined) = join_set.join_next().await {
        let (idx, profile_name, shard_labels, execs) =
            joined.map_err(|e| format!("verify batch task panicked: {e}"))?;
        if ordered_results.len() <= idx {
            ordered_results.resize_with(idx + 1, || None);
        }
        ordered_results[idx] = Some(BatchOutcome {
            profile_name,
            shard_labels,
            execs,
        });
    }
    let batch_results = ordered_results.into_iter().flatten().collect::<Vec<_>>();

    let mut report_cells = Vec::new();
    let mut all_ok = true;
    let mut omitted = 0usize;
    let mut first_fingerprint = None;
    let mut first_failure_summary = None;

    for BatchOutcome {
        profile_name,
        shard_labels,
        execs,
    } in batch_results
    {
        for (exec, shard_label) in execs.into_iter().zip(shard_labels) {
            let os_axis = cell_axis_val(&exec.cell, "os").or_else(|| {
                exec.result
                    .as_ref()
                    .and_then(|r| {
                        workers_arc
                            .iter()
                            .find(|w| listed_worker_id(w) == Some(r.worker_id))
                    })
                    .and_then(|w| tag_value(&w.tags, OS_TAG_PREFIX).map(normalize_os))
            });
            let arch_axis = cell_axis_val(&exec.cell, "arch").or_else(|| {
                exec.result
                    .as_ref()
                    .and_then(|r| {
                        workers_arc
                            .iter()
                            .find(|w| listed_worker_id(w) == Some(r.worker_id))
                    })
                    .and_then(|w| tag_value(&w.tags, ARCH_TAG_PREFIX).map(normalize_arch))
            });

            let cell_report = build_cell_report(
                &profile_name,
                os_axis,
                arch_axis,
                shard_label,
                &exec,
                &workers_arc,
                &mut omitted,
            );
            if cell_report.exit != 0 {
                all_ok = false;
                if first_fingerprint.is_none() {
                    first_fingerprint = cell_report.failures.first().map(failure_fingerprint);
                    first_failure_summary = summarize_failure(&cell_report);
                }
            }
            report_cells.push(cell_report);
        }
    }

    let report = VerifyGateReport {
        task: task.id.clone(),
        commit,
        ok: all_ok,
        blocked: false,
        untrusted_data: true,
        cells: report_cells,
        omitted,
        warnings,
    };
    finalize_verify_gate(
        repo_dir,
        &options,
        &task.id,
        report,
        first_fingerprint,
        first_failure_summary,
    )
}

fn build_cell_report(
    profile_name: &str,
    os: Option<String>,
    arch: Option<String>,
    shard: Option<String>,
    exec: &crate::matrix::CellExecution,
    workers: &[WorkerCapabilities],
    omitted: &mut usize,
) -> VerifyCellReport {
    let worker_caps = exec.result.as_ref().and_then(|r| {
        workers
            .iter()
            .find(|w| listed_worker_id(w) == Some(r.worker_id))
    });
    let link = worker_caps.map(extract_link_kind);
    let toolchain = worker_caps.and_then(extract_toolchain);
    let worker_id = exec.result.as_ref().map(|r| r.worker_id.to_string());

    let (exit, secs, failures) = match &exec.result {
        Some(res) => {
            let stdout = res.stdout_str();
            let stderr = res.stderr_str();
            let (mut fails, cell_omitted) =
                extract_failures(res.exit_code, &stdout, &stderr, res.error.as_deref());
            if res.exit_code != 0 && fails.is_empty() {
                fails.push(VerifyFailure {
                    kind: VerifyFailureKind::Command,
                    name: None,
                    file: None,
                    line: None,
                    message: format!("exit code {}", res.exit_code),
                });
            }
            *omitted = omitted.saturating_add(cell_omitted);
            (
                res.exit_code,
                ((res.execution_time_ms as f64) / 1000.0 * 10.0).round() / 10.0,
                fails,
            )
        }
        None => {
            let kind = if exec.status == crate::matrix::CellStatus::Unplaced {
                VerifyFailureKind::Unplaced
            } else {
                VerifyFailureKind::Command
            };
            let msg = exec
                .error
                .as_deref()
                .unwrap_or_else(|| exec.status.as_str());
            (
                1,
                0.0,
                vec![VerifyFailure {
                    kind,
                    name: None,
                    file: None,
                    line: None,
                    message: clamp_message(msg, MAX_FAILURE_MESSAGE_LINES, 240),
                }],
            )
        }
    };

    VerifyCellReport {
        profile: profile_name.to_string(),
        os,
        arch,
        shard,
        worker: exec
            .worker_name
            .clone()
            .or_else(|| worker_caps.map(|w| w.name.clone())),
        worker_id,
        link,
        toolchain,
        exit,
        secs,
        task_id: exec.task_id.map(|t| t.to_string()),
        failures,
    }
}

/// Executes `oxideswarm verify --task <id>` from the CLI.
pub async fn run_verify_command(
    args: VerifyGateArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    if args.timeout_secs == 0 {
        eprintln!("--timeout-secs must be at least 1");
        return ExitCode::FAILURE;
    }
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.submit_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    with_client_relay_url(
        relay_url,
        run_verify_command_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_verify_command_inner(
    args: VerifyGateArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let repo_dir = match args.repo.clone() {
        Some(p) => p,
        None => match std::env::current_dir() {
            Ok(d) => d,
            Err(e) => {
                eprintln!("verify: cannot determine current directory: {e}");
                return ExitCode::FAILURE;
            }
        },
    };

    let submit_cfg = config_file.as_ref().and_then(|c| c.submit.as_ref());
    let raw_master = resolve_string(
        args.master.clone(),
        CLIENT_MASTER_ENV_VARS,
        submit_cfg.and_then(|s| s.target_master()),
        DEFAULT_MASTER_ADDR,
    );
    let master_addr = normalize_master_addr(&raw_master);
    let secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Submit,
        &master_addr,
        peer_insecure(args.insecure, submit_cfg.and_then(|s| s.insecure)),
    ) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let worker_cfg = config_file.as_ref().and_then(|c| c.worker.as_ref());
    let max_files = resolve_usize(
        args.workspace_max_files,
        &["RUSTY_GRID_WORKSPACE_MAX_FILES"],
        None,
        crate::blobs::DEFAULT_WORKSPACE_MAX_FILES,
    );
    let max_mb = resolve_u64(
        args.workspace_max_mb,
        &["RUSTY_GRID_WORKSPACE_MAX_MB"],
        worker_cfg.and_then(|w| w.workspace_limit_mb),
        crate::blobs::DEFAULT_WORKSPACE_MAX_BYTES / (1024 * 1024),
    );
    let workspace_limits = WorkspaceLimits {
        max_files,
        max_bytes: max_mb.saturating_mul(1024 * 1024),
    };

    let max_verifies_per_hour = match resolve_max_verifies_per_hour(args.max_verifies_per_hour) {
        Ok(v) => v,
        Err(e) => {
            if args.json {
                let err_json = serde_json::json!({
                    "task": args.task,
                    "ok": false,
                    "error": e,
                });
                println!("{err_json}");
            } else {
                eprintln!("verify failed: {e}");
            }
            return ExitCode::FAILURE;
        }
    };
    let options = VerifyGateOptions {
        remote: args.remote.clone(),
        agent_id: args.agent.clone(),
        max_verifies_per_hour,
        hub_dir: args.hub_dir.clone(),
        breaker_threshold: CIRCUIT_BREAKER_THRESHOLD,
    };

    match run_verify_gate_with_options(
        &repo_dir,
        args.plan.as_deref(),
        &args.task,
        &args.profiles,
        args.base.as_deref(),
        &master_addr,
        secret,
        Duration::from_secs(args.timeout_secs),
        workspace_limits,
        options,
    )
    .await
    {
        Ok((report, json)) => {
            if args.json {
                println!("{json}");
            } else {
                println!(
                    "verify {} ({}): {}",
                    report.task,
                    report.commit,
                    if report.ok { "PASS" } else { "FAIL" }
                );
                for cell in &report.cells {
                    let who = cell.worker.as_deref().unwrap_or("unplaced");
                    let link = cell.link.as_deref().unwrap_or("-");
                    println!(
                        "  {} [{}] worker={} link={} exit={} ({:.1}s)",
                        cell.profile,
                        cell.os.as_deref().unwrap_or("*"),
                        who,
                        link,
                        cell.exit,
                        cell.secs
                    );
                    for f in &cell.failures {
                        println!("    - {}", f.message.lines().next().unwrap_or(&f.message));
                    }
                }
                if report.omitted > 0 {
                    println!(
                        "  ({} additional failure/cell entries omitted)",
                        report.omitted
                    );
                }
                for w in &report.warnings {
                    eprintln!("warning: {w}");
                }
            }
            if report.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(e) => {
            if args.json {
                let err_json = serde_json::json!({
                    "task": args.task,
                    "ok": false,
                    "error": e,
                });
                println!("{err_json}");
            } else {
                eprintln!("verify failed: {e}");
            }
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cargo_json_compile_errors_with_primary_spans() {
        let sample = concat!(
            r#"{"reason":"compiler-message","message":{"code":{"code":"E0308","explanation":null},"level":"error","message":"mismatched types","spans":[{"file_name":"crates/master/src/queue.rs","line_start":812,"is_primary":true}]}}"#,
            "\n",
            r#"{"reason":"compiler-message","message":{"code":null,"level":"warning","message":"unused variable","spans":[]}}"#,
            "\n",
            r#"{"reason":"compiler-message","message":{"code":null,"level":"error","message":"aborting due to 1 previous error","spans":[]}}"#,
            "\n",
        );
        let (fails, omitted) = extract_failures(101, sample, "", None);
        assert_eq!(omitted, 0);
        assert_eq!(fails.len(), 1);
        assert_eq!(fails[0].kind, VerifyFailureKind::Compile);
        assert_eq!(fails[0].name.as_deref(), Some("E0308"));
        assert_eq!(fails[0].file.as_deref(), Some("crates/master/src/queue.rs"));
        assert_eq!(fails[0].line, Some(812));
        assert!(fails[0].message.contains("mismatched types"));
    }

    #[test]
    fn parses_libtest_and_nextest_and_fallback_failures() {
        let libtest = "\
running 2 tests
test ok_test ... ok
test queue::tests::cancel_counts ... FAILED

failures:

---- queue::tests::cancel_counts stdout ----
thread 'queue::tests::cancel_counts' panicked at crates/master/src/queue.rs:812:5:
assertion `left == right` failed
  left: 2
 right: 1
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
";
        let (fails, _) = extract_failures(101, libtest, "", None);
        assert_eq!(fails.len(), 1);
        assert_eq!(fails[0].kind, VerifyFailureKind::Test);
        assert_eq!(
            fails[0].name.as_deref(),
            Some("queue::tests::cancel_counts")
        );
        assert_eq!(fails[0].file.as_deref(), Some("crates/master/src/queue.rs"));
        assert_eq!(fails[0].line, Some(812));
        assert!(fails[0]
            .message
            .contains("assertion `left == right` failed"));

        let nextest = "\
    FAIL [   0.042s] rusty_grid_cli verify_gate::tests::bad_case
--- STDOUT: rusty_grid_cli verify_gate::tests::bad_case ---
thread 'verify_gate::tests::bad_case' panicked at crates/cli/src/verify_gate.rs:42:9:
boom expected true
";
        let (nf, _) = extract_failures(101, nextest, "", None);
        assert_eq!(nf.len(), 1);
        assert_eq!(nf[0].name.as_deref(), Some("verify_gate::tests::bad_case"));
        assert_eq!(nf[0].file.as_deref(), Some("crates/cli/src/verify_gate.rs"));
        assert_eq!(nf[0].line, Some(42));
        assert!(nf[0].message.contains("boom expected true"));

        let (fb, _) = extract_failures(2, "", "line 1\nline 2\nline 3\nline 4\n", None);
        assert_eq!(fb.len(), 1);
        assert_eq!(fb[0].kind, VerifyFailureKind::Command);
        assert_eq!(fb[0].message, "line 2\nline 3\nline 4");
    }

    #[test]
    fn enforces_4kb_budget_cap_even_on_huge_reports() {
        let mut cells = Vec::new();
        for i in 0..20 {
            let mut failures = Vec::new();
            for j in 0..8 {
                failures.push(VerifyFailure {
                    kind: VerifyFailureKind::Test,
                    name: Some(format!("suite::case_{i}_{j}")),
                    file: Some(format!("crates/master/src/mod_{i}.rs")),
                    line: Some(100 + j as u32),
                    message: format!(
                        "failure line 1\nfailure line 2 with lots of text {}\nfailure line 3\nfailure line 4",
                        "x".repeat(180)
                    ),
                });
            }
            cells.push(VerifyCellReport {
                profile: "unit".into(),
                os: Some("linux".into()),
                arch: Some("x86_64".into()),
                shard: None,
                worker: Some(format!("worker-{i}")),
                worker_id: Some(Uuid::new_v4().to_string()),
                link: Some("direct".into()),
                toolchain: Some("rustc 1.96.0 x86_64-linux".into()),
                exit: 101,
                secs: 12.3,
                task_id: Some(Uuid::new_v4().to_string()),
                failures,
            });
        }
        let mut report = VerifyGateReport {
            task: "T-12".into(),
            commit: "abc123def456".into(),
            ok: false,
            blocked: false,
            untrusted_data: true,
            cells,
            omitted: 0,
            warnings: vec!["relayed worker estimate 99s".into()],
        };
        let encoded = fit_report_to_budget(&mut report, MAX_VERIFY_JSON_BYTES);
        assert!(
            encoded.len() <= MAX_VERIFY_JSON_BYTES,
            "encoded JSON len {} exceeded {}",
            encoded.len(),
            MAX_VERIFY_JSON_BYTES
        );
        assert!(report.omitted > 0);
    }

    #[test]
    fn augment_cargo_command_places_json_flag_before_separator() {
        assert_eq!(
            augment_cargo_command(&[
                "cargo".into(),
                "test".into(),
                "--locked".into(),
                "--".into(),
                "--nocapture".into()
            ]),
            vec![
                "cargo",
                "test",
                "--locked",
                "--message-format=json",
                "--",
                "--nocapture"
            ]
        );
        assert_eq!(
            augment_cargo_command(&["python3".into(), "-m".into(), "unittest".into()]),
            vec!["python3", "-m", "unittest"]
        );
    }

    #[test]
    fn failure_fingerprint_normalizes_hex_addresses_and_whitespace() {
        let a = VerifyFailure {
            kind: VerifyFailureKind::Test,
            name: Some("t".into()),
            file: Some("a.rs".into()),
            line: Some(10),
            message: "Panic at 0x7fff001a:\n  left: 2".into(),
        };
        let b = VerifyFailure {
            kind: VerifyFailureKind::Test,
            name: Some("t".into()),
            file: Some("a.rs".into()),
            line: Some(99),
            message: "panic at 0x1234abcd: left: 2".into(),
        };
        assert_eq!(failure_fingerprint(&a), failure_fingerprint(&b));
    }

    #[test]
    fn owned_paths_discipline_accepts_in_scope_and_refuses_out_of_scope() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        assert!(Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["init", "-b", "master"])
            .output()
            .unwrap()
            .status
            .success());
        Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["config", "user.email", "test@example.com"])
            .output()
            .unwrap();
        Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["config", "user.name", "Test"])
            .output()
            .unwrap();
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(repo.join("src/lib.rs"), "pub fn f() {}\n").unwrap();
        std::fs::write(repo.join("README.md"), "# init\n").unwrap();
        Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["add", "-A"])
            .output()
            .unwrap();
        assert!(Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["commit", "-m", "init"])
            .output()
            .unwrap()
            .status
            .success());

        // In-scope edit passes.
        std::fs::write(repo.join("src/lib.rs"), "pub fn f() -> i32 { 1 }\n").unwrap();
        check_owned_paths_discipline(repo, "master", &["src/**".into()]).unwrap();
        let c1 = compute_worktree_commit(repo).unwrap();
        assert!(c1.contains("+dirty-"), "got {c1}");

        // Out-of-scope edit fails.
        std::fs::write(repo.join("README.md"), "oops").unwrap();
        let err = check_owned_paths_discipline(repo, "master", &["src/**".into()]).unwrap_err();
        assert!(err.contains("README.md"), "{err}");
    }

    #[test]
    fn direct_link_preferred_and_relay_warning_estimated() {
        let cell = MatrixCell {
            index: 0,
            assignments: vec![("os".into(), "linux".into())],
            label: "os=linux".into(),
            slug: "os-linux".into(),
            placement: Placement::from_task_tags(&["require:os=linux".into()]).unwrap(),
            env_overrides: BTreeMap::new(),
        };
        let reqs = TaskRequirements::generic(1, 60);
        let id_relay = Uuid::new_v4();
        let id_direct = Uuid::new_v4();
        let w_relay = WorkerCapabilities::new("far", 4, 8192, false, false, None).with_tags(vec![
            "os:linux".into(),
            "link:Relay (DERP):250.0ms".into(),
            format!("id:{id_relay}"),
        ]);
        let w_direct =
            WorkerCapabilities::new("near", 4, 8192, false, false, None).with_tags(vec![
                "os:linux".into(),
                "link:Direct P2P (QUIC):2.1ms".into(),
                format!("id:{id_direct}"),
            ]);

        let (pref, warn) = select_link_preference(
            &cell,
            &[w_relay.clone(), w_direct],
            &reqs,
            false,
            10_000_000,
        );
        assert_eq!(pref, Some(id_direct));
        assert!(warn.is_none());

        let (pref2, warn2) = select_link_preference(&cell, &[w_relay], &reqs, false, 10_000_000);
        assert!(pref2.is_none());
        assert!(warn2.unwrap().contains("0.12 MB/s"));
    }

    #[test]
    fn verify_rate_limit_enforces_hourly_cap_expires_old_entries_and_refuses_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        let t0 = DateTime::parse_from_rfc3339("2026-11-26T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert!(check_and_record_verify_rate_at(&repo, "agent-a", 0, t0).is_err());

        let s1 = check_and_record_verify_rate_at(&repo, "agent-a", 2, t0).unwrap();
        assert_eq!(s1.used_in_window, 1);
        let s2 = check_and_record_verify_rate_at(
            &repo,
            "agent-a",
            2,
            t0 + chrono::Duration::minutes(10),
        )
        .unwrap();
        assert_eq!(s2.used_in_window, 2);

        // 3rd call inside 1h is rejected without affecting another agent.
        let err = check_and_record_verify_rate_at(
            &repo,
            "agent-a",
            2,
            t0 + chrono::Duration::minutes(20),
        )
        .unwrap_err();
        assert!(err.contains("rate limit exceeded"), "{err}");
        assert!(err.contains("retry in"), "{err}");
        assert!(check_and_record_verify_rate_at(
            &repo,
            "agent-b",
            2,
            t0 + chrono::Duration::minutes(20),
        )
        .is_ok());

        // Oldest entry at t0 expires at t0 + 3600s.
        let s3 = check_and_record_verify_rate_at(
            &repo,
            "agent-a",
            2,
            t0 + chrono::Duration::seconds(3600),
        )
        .unwrap();
        assert_eq!(s3.used_in_window, 2);

        #[cfg(unix)]
        {
            let linked = tmp.path().join("linked_repo");
            std::fs::create_dir_all(linked.join(".oxideswarm/verify")).unwrap();
            let outside = tmp.path().join("outside_rate.json");
            std::fs::write(&outside, "{}").unwrap();
            std::os::unix::fs::symlink(&outside, linked.join(".oxideswarm/verify/rate_limit.json"))
                .unwrap();
            let err = check_and_record_verify_rate_at(&linked, "agent-a", 5, t0).unwrap_err();
            assert!(err.contains("symlink"), "{err}");
        }
    }
}
