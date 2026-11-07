//! Command-line interface, interactive menu, and execution engine for OxideSwarm Workflow Modes (`ox-mode`).

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::Parser;
use serde::{Deserialize, Serialize};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

use rusty_grid_core::capabilities::{MobileCapabilities, WorkerCapabilities};
use rusty_grid_core::cluster_auth::ClusterSecret;
pub use rusty_grid_core::mode::{
    find_workspace_root, get_mode_descriptors, ModeActionDescriptor, ModeDescriptor,
    ModeExecutionResult, WorkflowMode,
};
use rusty_grid_core::protocol::{
    deserialize_message, serialize_message, MasterMessage, ProtocolCapabilities, WireCodec,
    MAX_FRAME_SIZE, PROTOCOL_VERSION_CURRENT, PROTOCOL_VERSION_MIN_COMPATIBLE,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};

use crate::config::{load_config_file, SecretSection};
use crate::secret_cmd::SecretOpts;

// ANSI terminal colors for CLI output
const CLR_RESET: &str = "\x1b[0m";
const CLR_BOLD: &str = "\x1b[1m";
const CLR_CYAN: &str = "\x1b[36m";
const CLR_GREEN: &str = "\x1b[32m";
const CLR_YELLOW: &str = "\x1b[33m";
const CLR_RED: &str = "\x1b[31m";
const CLR_MAGENTA: &str = "\x1b[35m";
const CLR_DIM: &str = "\x1b[2m";

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(
    name = "ox-mode",
    about = "OxideSwarm Workflow Modes & Presets: test, dev, doc, research",
    version
)]
pub struct ModeCliArgs {
    /// Workflow mode: test, dev, doc, research
    #[arg(index = 1)]
    pub mode: Option<String>,

    /// Mode action (e.g. quick, full, lint, flaky, fast, cluster, status, stop, build, verify, export, profile, bench, distributed, report)
    #[arg(index = 2)]
    pub action: Option<String>,

    /// Specific action override flag
    #[arg(long, short = 'a')]
    pub action_opt: Option<String>,

    /// Number of iterations for flaky test detection (default: 5)
    #[arg(long, default_value = "5")]
    pub iterations: usize,

    /// Optional dashboard HTTP port for `dev status`
    #[arg(long, env = "RUSTY_GRID_DASHBOARD_PORT")]
    pub port: Option<u16>,

    /// Format output as JSON
    #[arg(long)]
    pub json: bool,

    /// Open interactive menu
    #[arg(short = 'i', long)]
    pub interactive: bool,
}

// ==============================================================================
// 1. TESTING & QUALITY ASSURANCE ENGINE
// ==============================================================================

/// Sums `passed;` and `failed;` counts across every `test result:` summary line in `cargo test` output.
pub fn parse_cargo_test_counts(stdout: &str) -> (usize, usize) {
    let mut passed_tests = 0;
    let mut failed_tests = 0;
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("test result:") {
            if let Some(passed) = extract_count(trimmed, "passed;") {
                passed_tests += passed;
            }
            if let Some(failed) = extract_count(trimmed, "failed;") {
                failed_tests += failed;
            }
        }
    }
    (passed_tests, failed_tests)
}

/// Runs quick workspace unit tests (`cargo test --workspace --lib`).
pub fn run_quick_tests(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "quick");

    let cmd_res = Command::new("cargo")
        .args(["test", "--workspace", "--lib", "--", "--nocapture"])
        .current_dir(workspace_root)
        .output();

    res.duration_ms = start.elapsed().as_millis() as u64;

    match cmd_res {
        Ok(output) => {
            res.exit_code = output.status.code();
            res.success = output.status.success();
            res.stdout = String::from_utf8_lossy(&output.stdout).to_string();
            res.stderr = String::from_utf8_lossy(&output.stderr).to_string();

            let (passed_tests, failed_tests) = parse_cargo_test_counts(&res.stdout);
            res = res.with_metric("passed_tests", passed_tests);
            res = res.with_metric("failed_tests", failed_tests);
        }
        Err(e) => {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to spawn cargo: {}", e);
        }
    }
    res
}

/// Runs full workspace test suite (`cargo test --workspace`).
pub fn run_full_tests(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "full");

    let cmd_res = Command::new("cargo")
        .args(["test", "--workspace", "--", "--nocapture"])
        .current_dir(workspace_root)
        .output();

    res.duration_ms = start.elapsed().as_millis() as u64;

    match cmd_res {
        Ok(output) => {
            res.exit_code = output.status.code();
            res.success = output.status.success();
            res.stdout = String::from_utf8_lossy(&output.stdout).to_string();
            res.stderr = String::from_utf8_lossy(&output.stderr).to_string();
        }
        Err(e) => {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to execute cargo: {}", e);
        }
    }
    res
}

/// Combines `cargo clippy` and `cargo fmt --check` outcomes into a `ModeExecutionResult`.
/// Both checks must succeed for `res.success` to be `true`.
pub fn evaluate_lint_outcomes(
    clippy_ok: bool,
    clippy_stdout: &str,
    clippy_stderr: &str,
    fmt_ok: bool,
    fmt_stdout: &str,
    fmt_stderr: &str,
    duration_ms: u64,
) -> ModeExecutionResult {
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "lint");
    res.duration_ms = duration_ms;
    res.success = clippy_ok && fmt_ok;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let mut combined_stdout = String::new();
    let mut combined_stderr = String::new();

    combined_stdout.push_str("=== 1. Cargo Clippy Lints ===\n");
    combined_stdout.push_str(clippy_stdout);
    combined_stderr.push_str(clippy_stderr);

    combined_stdout.push_str("\n=== 2. Rustfmt Code Style ===\n");
    if fmt_ok {
        combined_stdout.push_str("All source files conform to standard Rust formatting.\n");
    } else {
        combined_stderr.push_str(fmt_stderr);
        combined_stderr.push_str(fmt_stdout);
    }

    res.stdout = combined_stdout;
    res.stderr = combined_stderr;
    res = res.with_metric("clippy_passed", clippy_ok);
    res = res.with_metric("fmt_passed", fmt_ok);
    res
}

/// Runs clippy lint checks and rustfmt verification.
pub fn run_lints(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();

    let clippy_out = Command::new("cargo")
        .args([
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ])
        .current_dir(workspace_root)
        .output();

    let fmt_out = Command::new("cargo")
        .args(["fmt", "--all", "--", "--check"])
        .current_dir(workspace_root)
        .output();

    let duration_ms = start.elapsed().as_millis() as u64;

    let (clippy_ok, clippy_stdout, clippy_stderr) = match clippy_out {
        Ok(o) => (
            o.status.success(),
            String::from_utf8_lossy(&o.stdout).into_owned(),
            String::from_utf8_lossy(&o.stderr).into_owned(),
        ),
        Err(e) => (
            false,
            String::new(),
            format!("Failed to spawn clippy: {e}\n"),
        ),
    };

    let (fmt_ok, fmt_stdout, fmt_stderr) = match fmt_out {
        Ok(o) => (
            o.status.success(),
            String::from_utf8_lossy(&o.stdout).into_owned(),
            String::from_utf8_lossy(&o.stderr).into_owned(),
        ),
        Err(e) => (
            false,
            String::new(),
            format!("Failed to spawn rustfmt: {e}\n"),
        ),
    };

    evaluate_lint_outcomes(
        clippy_ok,
        &clippy_stdout,
        &clippy_stderr,
        fmt_ok,
        &fmt_stdout,
        &fmt_stderr,
        duration_ms,
    )
}

/// Evaluates flaky test detection iterations using the supplied per-iteration runner `(iteration_1_based) -> (passed, elapsed_ms)`.
/// Every iteration must pass (`pass_count == iters`) for `res.success` to be `true`.
pub fn run_flaky_detection_with<F>(iterations: usize, mut run_one: F) -> ModeExecutionResult
where
    F: FnMut(usize) -> (bool, u64),
{
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "flaky");
    let iters = if iterations == 0 { 5 } else { iterations };

    let mut iteration_results = Vec::new();
    let mut pass_count = 0;
    let mut total_duration_ms = 0u64;

    for i in 1..=iters {
        let (ok, elapsed) = run_one(i);
        total_duration_ms += elapsed;
        if ok {
            pass_count += 1;
        }
        iteration_results.push(serde_json::json!({
            "iteration": i,
            "success": ok,
            "duration_ms": elapsed
        }));
    }

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = pass_count == iters;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let reliability_pct = (pass_count as f64 / iters as f64) * 100.0;
    let avg_duration_ms = total_duration_ms / iters as u64;

    let mut stdout = format!(
        "=== Flaky Test Detection Summary ===\nTotal Iterations: {}\nPassed: {} / {}\nPass Rate: {:.1}%\nAverage Duration: {} ms\n\nIterative Breakdown:\n",
        iters, pass_count, iters, reliability_pct, avg_duration_ms
    );
    for item in &iteration_results {
        stdout.push_str(&format!(
            "  - Run #{}: {} ({} ms)\n",
            item["iteration"],
            if item["success"].as_bool().unwrap_or(false) {
                "[PASS]"
            } else {
                "[FAIL]"
            },
            item["duration_ms"]
        ));
    }

    res.stdout = stdout;
    res = res.with_metric("iterations", iters);
    res = res.with_metric("passed", pass_count);
    res = res.with_metric("reliability_pct", reliability_pct);
    res = res.with_metric("avg_duration_ms", avg_duration_ms);
    res = res.with_metric("runs", iteration_results);

    res
}

/// Runs test iterations to detect flaky non-deterministic tests.
pub fn run_flaky_detection(workspace_root: &Path, iterations: usize) -> ModeExecutionResult {
    run_flaky_detection_with(iterations, |_i| {
        let iter_start = Instant::now();
        let cmd = Command::new("cargo")
            .args(["test", "--workspace", "--lib", "--quiet"])
            .current_dir(workspace_root)
            .output();
        let elapsed = iter_start.elapsed().as_millis() as u64;
        let ok = cmd.as_ref().map(|o| o.status.success()).unwrap_or(false);
        (ok, elapsed)
    })
}

// ==============================================================================
// 2. DEVELOPMENT & LOCAL CLUSTER ENGINE
// ==============================================================================

/// Runs fast incremental compilation check (`cargo check --workspace`).
pub fn run_fast_check(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "fast");

    let cmd_res = Command::new("cargo")
        .args(["check", "--workspace"])
        .current_dir(workspace_root)
        .output();

    res.duration_ms = start.elapsed().as_millis() as u64;

    match cmd_res {
        Ok(output) => {
            res.exit_code = output.status.code();
            res.success = output.status.success();
            res.stdout = String::from_utf8_lossy(&output.stdout).to_string();
            res.stderr = String::from_utf8_lossy(&output.stderr).to_string();
            if res.success && res.stdout.trim().is_empty() {
                res.stdout = format!("Cargo check completed cleanly in {} ms.", res.duration_ms);
            }
        }
        Err(e) => {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to spawn cargo: {}", e);
        }
    }
    res
}

/// Runs a check on watch mode readiness, scanning workspace files and performing fast verification.
pub fn run_watch_check(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "watch");

    let mut file_count = 0usize;
    let mut total_lines = 0usize;

    let scan_dirs = ["crates", "tests", "src"];
    for dir_name in &scan_dirs {
        let dir_path = workspace_root.join(dir_name);
        if dir_path.is_dir() {
            count_rust_files(&dir_path, &mut file_count, &mut total_lines);
        }
    }
    if workspace_root.join("Cargo.toml").exists() {
        file_count += 1;
    }

    let check = run_fast_check(workspace_root);

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = check.success;
    res.exit_code = check.exit_code.or(Some(if res.success { 0 } else { 1 }));

    let stdout = format!(
        "=== Live Reload Watch Mode Status ===\nWatched Files: {} Rust source & config files (~{} lines of code)\nCompiler State: {}\nReady for live changes. Use `ox-mode dev watch` or `./scripts/mode.sh dev watch` to run continuous auto-reload watcher loop.\n\nInitial Fast Check Output:\n{}",
        file_count,
        total_lines,
        if res.success {
            "[HEALTHY - READY FOR RELOAD]"
        } else {
            "[COMPILATION ISSUES DETECTED]"
        },
        check.stdout
    );

    res.stdout = stdout;
    res.stderr = check.stderr;
    res = res.with_metric("watched_files_count", file_count);
    res = res.with_metric("watchable_files", file_count);
    res = res.with_metric("watchable_dirs", scan_dirs.len());
    res = res.with_metric("source_lines_count", total_lines);
    res = res.with_metric("compiler_ready", check.success);

    res
}

fn count_rust_files(dir: &Path, file_count: &mut usize, line_count: &mut usize) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name == "target" || name.starts_with('.') {
                        continue;
                    }
                }
                count_rust_files(&path, file_count, line_count);
            } else if path.is_file() {
                if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                    if ext == "rs" {
                        *file_count += 1;
                        if let Ok(content) = fs::read_to_string(&path) {
                            *line_count += content.lines().count();
                        }
                    }
                }
            }
        }
    }
}

/// Resolves the cluster secret from the standard CLI/env/config/default-file hierarchy
/// and derives the dashboard Admin Bearer token when a secret is present.
fn resolve_dashboard_bearer_token() -> Option<String> {
    let config = load_config_file(None).ok().flatten();
    let opts = SecretOpts::default();
    for section in [
        SecretSection::Status,
        SecretSection::Master,
        SecretSection::Submit,
        SecretSection::Worker,
    ] {
        if let Ok(Some(secret)) = opts.resolve(config.as_ref(), section) {
            return Some(rusty_grid_master::dashboard_admin_token(&secret));
        }
    }
    None
}

fn http_get_local_status_once(
    port: u16,
    bearer_token: Option<&str>,
) -> Result<(u16, bool, String), String> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(750))
        .map_err(|e| format!("connect 127.0.0.1:{port}: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));

    let auth_header = match bearer_token {
        Some(token) if !token.is_empty() => format!("Authorization: Bearer {token}\r\n"),
        _ => String::new(),
    };
    let req = format!(
        "GET /api/status HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: application/json\r\n{auth_header}Connection: close\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("write 127.0.0.1:{port}: {e}"))?;

    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|e| format!("read 127.0.0.1:{port}: {e}"))?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))
        .ok_or_else(|| format!("malformed HTTP response from 127.0.0.1:{port}"))?;
    let status_line = head.lines().next().unwrap_or_default();
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| {
            format!("invalid HTTP status line from 127.0.0.1:{port}: '{status_line}'")
        })?;
    let has_oxideswarm_challenge = head.lines().skip(1).any(|line| {
        if let Some((name, val)) = line.split_once(':') {
            name.trim().eq_ignore_ascii_case("www-authenticate")
                && val.contains("Bearer realm=\"OxideSwarm\"")
        } else {
            false
        }
    });
    Ok((status_code, has_oxideswarm_challenge, body.to_string()))
}

/// Performs a synchronous HTTP/1.1 GET against `127.0.0.1:<port>/api/status`,
/// returning `(http_status_code, response_body)`.
///
/// Never sends the Bearer token on the initial probe; only retries with `Authorization: Bearer`
/// if the server returns `401` with `WWW-Authenticate: Bearer realm="OxideSwarm"`.
fn http_get_local_status(port: u16, bearer_token: Option<&str>) -> Result<(u16, String), String> {
    let (status_code, has_oxideswarm_challenge, body) = http_get_local_status_once(port, None)?;
    if status_code == 401 && has_oxideswarm_challenge {
        if let Some(token) = bearer_token.filter(|t| !t.is_empty()) {
            if let Ok((retry_code, _, retry_body)) = http_get_local_status_once(port, Some(token)) {
                return Ok((retry_code, retry_body));
            }
        }
    }
    Ok((status_code, body))
}

/// Default fallback ports probed by `dev status` when `--port` and `.dev_cluster.json` are absent.
pub const DEFAULT_FALLBACK_STATUS_PORTS: &[u16] = &[8080];

/// Probes the health and status of a running local master node.
/// When `port` is `Some(p)`, probes only `p`; when `None`, checks `.dev_cluster.json` then HTTP port `8080`.
pub fn probe_cluster_health(port: Option<u16>) -> ModeExecutionResult {
    probe_cluster_health_with_root(port, &find_workspace_root())
}

/// Probes the health and status of a running local master node with a specific workspace root.
pub fn probe_cluster_health_with_root(
    port: Option<u16>,
    workspace_root: &Path,
) -> ModeExecutionResult {
    let bearer_token = resolve_dashboard_bearer_token();
    probe_cluster_health_with_bearer_and_fallbacks(
        port,
        workspace_root,
        bearer_token.as_deref(),
        DEFAULT_FALLBACK_STATUS_PORTS,
    )
}

/// Probes the health and status of a running local master node using an explicit `ClusterSecret` (if any).
pub fn probe_cluster_health_with_secret(
    port: Option<u16>,
    workspace_root: &Path,
    secret: Option<&ClusterSecret>,
) -> ModeExecutionResult {
    probe_cluster_health_with_fallback_ports(
        port,
        workspace_root,
        secret,
        DEFAULT_FALLBACK_STATUS_PORTS,
    )
}

/// Probes the health and status of a running local master node with an explicit fallback port list (for testing).
#[doc(hidden)]
pub fn probe_cluster_health_with_fallback_ports(
    port: Option<u16>,
    workspace_root: &Path,
    secret: Option<&ClusterSecret>,
    fallback_ports: &[u16],
) -> ModeExecutionResult {
    let bearer_token = secret.map(rusty_grid_master::dashboard_admin_token);
    probe_cluster_health_with_bearer_and_fallbacks(
        port,
        workspace_root,
        bearer_token.as_deref(),
        fallback_ports,
    )
}

fn probe_cluster_health_with_bearer(
    port: Option<u16>,
    workspace_root: &Path,
    bearer_token: Option<&str>,
) -> ModeExecutionResult {
    probe_cluster_health_with_bearer_and_fallbacks(
        port,
        workspace_root,
        bearer_token,
        DEFAULT_FALLBACK_STATUS_PORTS,
    )
}

fn probe_cluster_health_with_bearer_and_fallbacks(
    port: Option<u16>,
    workspace_root: &Path,
    bearer_token: Option<&str>,
    fallback_ports: &[u16],
) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "status");

    let mut candidate_ports = Vec::new();
    let configured_port = if let Some(explicit_port) = port {
        // Explicit `--port` / `RUSTY_GRID_DASHBOARD_PORT`: probe only the given port.
        candidate_ports.push(explicit_port);
        Some(explicit_port)
    } else {
        let mut from_cluster_file = None;
        let cluster_file = workspace_root.join(".dev_cluster.json");
        if let Ok(content) = fs::read_to_string(&cluster_file) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(p) = v.get("web_ui_port").and_then(|p| p.as_u64()) {
                    if let Ok(p_u16) = u16::try_from(p) {
                        from_cluster_file = Some(p_u16);
                        candidate_ports.push(p_u16);
                    }
                }
            }
        }
        // Append default fallback HTTP dashboard port(s) (8080 by default; 3000 and grid TCP port 8088 are never guessed).
        for &fp in fallback_ports {
            if !candidate_ports.contains(&fp) {
                candidate_ports.push(fp);
            }
        }
        from_cluster_file
    };

    let mut http_error_seen: Option<(String, u16, String)> = None;

    for &p in &candidate_ports {
        let url = format!("http://127.0.0.1:{}/api/status", p);
        // Only allow sending the dashboard admin Bearer token to an explicit `--port` or `.dev_cluster.json` port,
        // never to an unconfigured fallback port.
        let port_token = if configured_port == Some(p) {
            bearer_token
        } else {
            None
        };
        match http_get_local_status(p, port_token) {
            Ok((status_code, body)) if (200..300).contains(&status_code) => {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                    let has_workers_array =
                        json.get("workers").and_then(|w| w.as_array()).is_some();
                    let has_tasks_object = json.get("tasks").and_then(|t| t.as_object()).is_some();
                    if has_workers_array && has_tasks_object {
                        res.duration_ms = start.elapsed().as_millis() as u64;
                        res.success = true;
                        res.exit_code = Some(0);
                        let worker_count = json["workers"].as_array().map(|a| a.len()).unwrap_or(0);
                        let tasks_total = json["tasks"]["total"].as_u64().unwrap_or(0);
                        res.stdout = format!(
                            "Cluster Healthy at {}\nMaster: {}\nConnected Workers: {}\nTotal Tasks: {}\n",
                            url,
                            json["master"]["host"].as_str().unwrap_or("OxideSwarm"),
                            worker_count,
                            tasks_total
                        );
                        res = res.with_metric("http_status", status_code);
                        res = res.with_metric("workers_count", worker_count);
                        res = res.with_metric("tasks_total", tasks_total);
                        res = res.with_metric("endpoint", url);
                        res = res.with_metric("raw_status", json);
                        return res;
                    }
                }
            }
            Ok((status_code, body)) => {
                let snippet = body.lines().next().unwrap_or("").trim().to_string();
                if http_error_seen.is_none() || configured_port == Some(p) {
                    http_error_seen = Some((url, status_code, snippet));
                }
                if configured_port == Some(p) {
                    break;
                }
            }
            Err(_) => {}
        }
    }

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = false;
    res.exit_code = Some(1);
    if let Some((url, status_code, snippet)) = http_error_seen {
        res = res.with_metric("http_status", status_code);
        res = res.with_metric("endpoint", &url);
        res.stderr = if snippet.is_empty() {
            format!(
                "Master at {url} returned HTTP {status_code} (authentication required or request rejected)"
            )
        } else {
            format!(
                "Master at {url} returned HTTP {status_code} (authentication required or request rejected): {snippet}"
            )
        };
    } else {
        res.stderr = format!(
            "Cluster unreachable at candidate ports: {:?}",
            candidate_ports
        );
    }
    res
}

// ==============================================================================
// 3. DOCUMENTATION & SPECIFICATION ENGINE
// ==============================================================================

/// Resolves the Cargo target directory for `workspace_root`, honouring `CARGO_TARGET_DIR` when set.
pub fn resolve_cargo_target_dir(workspace_root: &Path) -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(val) if !val.is_empty() => {
            let p = PathBuf::from(val);
            if p.is_absolute() {
                p
            } else {
                workspace_root.join(p)
            }
        }
        _ => workspace_root.join("target"),
    }
}

/// Verifies that the generated rustdoc HTML index exists inside `target_dir` before reporting `doc build` success.
pub fn finalize_doc_build_output_with_target_dir(target_dir: &Path, res: &mut ModeExecutionResult) {
    if res.success {
        let doc_index = target_dir.join("doc/rusty_grid_core/index.html");
        if doc_index.is_file() {
            res.stdout.push_str(&format!(
                "\nDocumentation built successfully!\nHTML Index: {}\n",
                doc_index.display()
            ));
        } else {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!(
                "cargo doc exited 0, but expected HTML index does not exist: {}",
                doc_index.display()
            );
        }
    }
}

/// Verifies that the generated rustdoc HTML index exists on disk before reporting `doc build` success.
pub fn finalize_doc_build_output(workspace_root: &Path, res: &mut ModeExecutionResult) {
    let target_dir = resolve_cargo_target_dir(workspace_root);
    finalize_doc_build_output_with_target_dir(&target_dir, res);
}

/// Builds rustdoc for all workspace crates.
pub fn run_doc_build(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Doc, "build");

    let cmd_res = Command::new("cargo")
        .args(["doc", "--workspace", "--no-deps"])
        .current_dir(workspace_root)
        .output();

    res.duration_ms = start.elapsed().as_millis() as u64;

    match cmd_res {
        Ok(output) => {
            res.exit_code = output.status.code();
            res.success = output.status.success();
            res.stdout = String::from_utf8_lossy(&output.stdout).to_string();
            res.stderr = String::from_utf8_lossy(&output.stderr).to_string();
            finalize_doc_build_output(workspace_root, &mut res);
        }
        Err(e) => {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to spawn cargo doc: {}", e);
        }
    }
    res
}

/// Strips inline backtick code spans (`` `...` ``) from a single markdown line so links inside
/// inline code examples are not treated as document links.
fn strip_inline_code_spans(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut in_tick = false;
    for ch in line.chars() {
        if ch == '`' {
            in_tick = !in_tick;
        } else if !in_tick {
            out.push(ch);
        }
    }
    out
}

/// Extracts markdown link targets (`[label](target)`) from a line whose inline code spans were removed.
fn extract_markdown_link_targets(line: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'[' {
            if let Some(close_bracket_rel) = line[i + 1..].find("](") {
                let paren_start = i + 1 + close_bracket_rel + 2;
                if let Some(close_paren_rel) = line[paren_start..].find(')') {
                    let target = line[paren_start..paren_start + close_paren_rel]
                        .trim()
                        .to_string();
                    if !target.is_empty() {
                        targets.push(target);
                    }
                    i = paren_start + close_paren_rel + 1;
                    continue;
                }
            }
        }
        i += 1;
    }
    targets
}

/// Verifies markdown documentation files for syntax, headings, balanced code fences, and relative links.
pub fn verify_documentation(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Doc, "verify");

    let target_docs = [
        "README.md",
        "CLAUDE.md",
        "PROJECT.md",
        "DEPLOYMENT.md",
        "ANDROID_ARCHITECTURE.md",
    ];

    let mut doc_details = Vec::new();
    let mut all_exist = true;
    let mut total_lines = 0usize;
    let mut total_bytes = 0usize;
    let mut total_links_checked = 0usize;
    let mut total_broken_links = 0usize;
    let mut issues = Vec::new();

    for doc_name in &target_docs {
        let doc_path = workspace_root.join(doc_name);
        if !doc_path.exists() {
            all_exist = false;
            issues.push(format!("Missing documentation file: {}", doc_name));
            doc_details.push(serde_json::json!({
                "file": doc_name,
                "exists": false,
                "lines": 0,
                "bytes": 0,
                "headings_count": 0,
                "links_checked": 0,
                "broken_links": 0
            }));
            continue;
        }

        match fs::read_to_string(&doc_path) {
            Ok(content) => {
                let lines_count = content.lines().count();
                let bytes_count = content.len();
                let mut in_fence = false;
                let mut headings_count = 0usize;
                let mut doc_links_checked = 0usize;
                let mut doc_broken_links = 0usize;
                let doc_parent = doc_path.parent().unwrap_or(workspace_root);

                for (line_idx, l) in content.lines().enumerate() {
                    let trimmed = l.trim_start();
                    if trimmed.starts_with("```") {
                        in_fence = !in_fence;
                        continue;
                    }
                    if in_fence {
                        continue;
                    }
                    if trimmed.starts_with('#') {
                        headings_count += 1;
                    }

                    let cleaned = strip_inline_code_spans(l);
                    for target in extract_markdown_link_targets(&cleaned) {
                        if target.starts_with('#')
                            || target.contains("://")
                            || target.starts_with("mailto:")
                        {
                            continue;
                        }
                        let rel_path = target.split('#').next().unwrap_or("").trim();
                        if rel_path.is_empty() {
                            continue;
                        }
                        doc_links_checked += 1;
                        if !doc_parent.join(rel_path).exists() {
                            doc_broken_links += 1;
                            issues.push(format!(
                                "{}:{}: Broken relative link '{}'",
                                doc_name,
                                line_idx + 1,
                                target
                            ));
                        }
                    }
                }

                total_lines += lines_count;
                total_bytes += bytes_count;
                total_links_checked += doc_links_checked;
                total_broken_links += doc_broken_links;

                if headings_count == 0 {
                    issues.push(format!("{}: No markdown headings found", doc_name));
                }

                let fence_count = content
                    .lines()
                    .filter(|l| l.trim_start().starts_with("```"))
                    .count();
                if fence_count % 2 != 0 {
                    issues.push(format!(
                        "{}: Odd number of code fence delimiters (```) detected (unclosed fence)",
                        doc_name
                    ));
                }

                doc_details.push(serde_json::json!({
                    "file": doc_name,
                    "exists": true,
                    "lines": lines_count,
                    "bytes": bytes_count,
                    "headings_count": headings_count,
                    "fences_count": fence_count,
                    "links_checked": doc_links_checked,
                    "broken_links": doc_broken_links
                }));
            }
            Err(e) => {
                all_exist = false;
                issues.push(format!("Could not read {}: {}", doc_name, e));
            }
        }
    }

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = all_exist && issues.is_empty();
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let mut stdout = format!(
        "=== Documentation & Specification Verification ===\nFiles Checked: {}\nTotal Size: {:.1} KB across {} lines\nRelative Links Checked: {} ({} broken)\nStatus: {}\n\nVerified Documents:\n",
        target_docs.len(),
        total_bytes as f64 / 1024.0,
        total_lines,
        total_links_checked,
        total_broken_links,
        if res.success {
            "[PASSED]"
        } else {
            "[ISSUES DETECTED]"
        }
    );

    for d in &doc_details {
        stdout.push_str(&format!(
            "  ● {:<24} -> {} lines, {} headings, {} links checked ({} broken), {} bytes\n",
            d["file"].as_str().unwrap_or(""),
            d["lines"],
            d["headings_count"],
            d["links_checked"],
            d["broken_links"],
            d["bytes"]
        ));
    }

    if !issues.is_empty() {
        stdout.push_str("\nIdentified Issues:\n");
        for iss in &issues {
            stdout.push_str(&format!("  [!] {}\n", iss));
        }
        res.stderr = issues.join("\n");
    }

    res.stdout = stdout;
    res = res.with_metric("total_docs", target_docs.len());
    res = res.with_metric("total_lines", total_lines);
    res = res.with_metric("total_bytes", total_bytes);
    res = res.with_metric("links_checked", total_links_checked);
    res = res.with_metric("broken_links", total_broken_links);
    res = res.with_metric("issues_count", issues.len());
    res = res.with_metric("details", doc_details);

    res
}

/// Generates a factual workspace and wire-protocol specification summary from live code constants
/// and workspace files on disk.
pub fn export_specification(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Doc, "export");

    let now_str = Utc::now().to_rfc3339();
    let max_frame_mib = MAX_FRAME_SIZE / (1024 * 1024);
    let supported_caps_bits = ProtocolCapabilities::all_supported().bits();

    // Inspect workspace crates from Cargo.toml
    let mut crate_rows = Vec::new();
    let root_toml_path = workspace_root.join("Cargo.toml");
    if let Ok(root_toml_str) = fs::read_to_string(&root_toml_path) {
        if let Ok(root_toml) = root_toml_str.parse::<toml::Table>() {
            if let Some(members) = root_toml
                .get("workspace")
                .and_then(|w| w.get("members"))
                .and_then(|m| m.as_array())
            {
                for member in members {
                    if let Some(member_rel) = member.as_str() {
                        let member_toml_path = workspace_root.join(member_rel).join("Cargo.toml");
                        let pkg_name = fs::read_to_string(&member_toml_path)
                            .ok()
                            .and_then(|s| s.parse::<toml::Table>().ok())
                            .and_then(|t| {
                                t.get("package")
                                    .and_then(|p| p.get("name"))
                                    .and_then(|n| n.as_str())
                                    .map(str::to_string)
                            })
                            .unwrap_or_else(|| member_rel.to_string());
                        crate_rows.push(format!("- `{member_rel}` (`{pkg_name}`)"));
                    }
                }
            }
        }
    }

    // Inspect design records in docs/design/
    let mut design_rows = Vec::new();
    let design_dir = workspace_root.join("docs/design");
    if let Ok(entries) = fs::read_dir(&design_dir) {
        let mut paths: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|ext| ext.to_str()) == Some("md"))
            .collect();
        paths.sort();
        for p in paths {
            let file_name = p
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string();
            let first_heading = fs::read_to_string(&p)
                .ok()
                .and_then(|c| {
                    c.lines()
                        .find(|l| l.starts_with("# "))
                        .map(|l| l.trim_start_matches("# ").trim().to_string())
                })
                .unwrap_or_else(|| file_name.clone());
            design_rows.push(format!("- `docs/design/{file_name}`: {first_heading}"));
        }
    }

    let crates_section = if crate_rows.is_empty() {
        "- (No workspace crates discovered)".to_string()
    } else {
        crate_rows.join("\n")
    };
    let designs_section = if design_rows.is_empty() {
        "- (No design records found in `docs/design/`)".to_string()
    } else {
        design_rows.join("\n")
    };

    let spec_markdown = format!(
        "# OxideSwarm Generated Workspace & Wire Specification Summary\n\
Generated At: {now_str}\n\
Workspace Root: {}\n\n\
## 1. Wire Protocol Constants (`rusty_grid_core::protocol`)\n\
- **Current Protocol Version (`PROTOCOL_VERSION_CURRENT`)**: `{PROTOCOL_VERSION_CURRENT}`\n\
- **Minimum Compatible Version (`PROTOCOL_VERSION_MIN_COMPATIBLE`)**: `{PROTOCOL_VERSION_MIN_COMPATIBLE}`\n\
- **Maximum Frame Size (`MAX_FRAME_SIZE`)**: `{MAX_FRAME_SIZE}` bytes (`{max_frame_mib} MiB`)\n\
- **Length Prefix**: 4-byte big-endian unsigned integer (`u32`), followed by a 1-byte wire-format discriminator (`0x02` Bincode default, `0x01` tagged JSON, `0x03` cluster-auth handshake, `0x04` ChaCha20-Poly1305 AEAD encrypted frame)\n\
- **Supported Capability Bits (`ProtocolCapabilities::all_supported()`)**: `0x{supported_caps_bits:04x}`\n\n\
## 2. Workspace Crates (`Cargo.toml`)\n\
{crates_section}\n\n\
## 3. Recorded Design Documents (`docs/design/`)\n\
{designs_section}\n",
        workspace_root.display()
    );

    let target_dir = workspace_root.join("target/docs");
    let target_file = target_dir.join("OXIDESWARM_SPECIFICATION.md");

    let write_ok =
        fs::create_dir_all(&target_dir).and_then(|_| fs::write(&target_file, &spec_markdown));

    res.duration_ms = start.elapsed().as_millis() as u64;

    match write_ok {
        Ok(_) => {
            res.success = true;
            res.exit_code = Some(0);
            res.stdout = format!(
                "Workspace Specification Summary generated!\nTarget File: {}\nSpecification Size: {} bytes\nGenerated At: {}\n",
                target_file.display(),
                spec_markdown.len(),
                now_str
            );
            res = res.with_metric("target_path", target_file.to_string_lossy().to_string());
            res = res.with_metric("spec_bytes", spec_markdown.len());
            res = res.with_metric("max_frame_size_bytes", MAX_FRAME_SIZE);
            res = res.with_metric("protocol_version_current", PROTOCOL_VERSION_CURRENT);
            res = res.with_metric("design_records_count", design_rows.len());
            res = res.with_metric("updated_at", now_str);
        }
        Err(e) => {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to write specification: {}", e);
        }
    }

    res
}

// ==============================================================================
// 4. RESEARCH & PERFORMANCE BENCHMARKING ENGINE
// ==============================================================================

/// Captures real hardware resource profile and host telemetry.
pub fn run_hardware_profile() -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Research, "profile");

    let mut sys = System::new_with_specifics(
        RefreshKind::new()
            .with_cpu(CpuRefreshKind::new().with_cpu_usage())
            .with_memory(MemoryRefreshKind::new().with_ram()),
    );
    std::thread::sleep(Duration::from_millis(150));
    sys.refresh_cpu_specifics(CpuRefreshKind::new().with_cpu_usage());
    sys.refresh_memory();

    let total_ram_mb = sys.total_memory() / (1024 * 1024);
    let avail_bytes = if sys.available_memory() > 0 {
        sys.available_memory()
    } else if sys.free_memory() > 0 {
        sys.free_memory()
    } else {
        sys.total_memory().saturating_sub(sys.used_memory())
    };
    let avail_ram_mb = (avail_bytes / (1024 * 1024)).max(1);
    let used_ram_mb = total_ram_mb.saturating_sub(avail_ram_mb);
    let ram_usage_pct = if total_ram_mb > 0 {
        (used_ram_mb as f32 / total_ram_mb as f32) * 100.0
    } else {
        0.0
    };

    let cpu_count = sys.cpus().len();
    let cpu_brand = sys
        .cpus()
        .first()
        .map(|c| c.brand().trim().to_string())
        .unwrap_or_else(|| "Unknown CPU".to_string());
    let cpu_usage_pct = sys.global_cpu_info().cpu_usage();

    let os_name = System::name().unwrap_or_else(|| "Unknown OS".to_string());
    let os_version = System::os_version().unwrap_or_default();
    let host_name = System::host_name().unwrap_or_else(|| "localhost".to_string());

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = true;
    res.exit_code = Some(0);

    let stdout = format!(
        "=== OxideSwarm Hardware & Resource Profile ===\nHost: {} ({}, {})\nCPU Architecture: {} ({} logical cores)\nCPU Global Load: {:.1}%\nTotal Physical RAM: {} MB (~{:.1} GB)\nAvailable RAM: {} MB ({:.1}% in use)\n",
        host_name,
        os_name,
        os_version,
        cpu_brand,
        cpu_count,
        cpu_usage_pct,
        total_ram_mb,
        (total_ram_mb as f64) / 1024.0,
        avail_ram_mb,
        ram_usage_pct
    );

    res.stdout = stdout;
    res = res.with_metric("host_name", host_name);
    res = res.with_metric("os_name", os_name);
    res = res.with_metric("os_version", os_version);
    res = res.with_metric("cpu_brand", cpu_brand);
    res = res.with_metric("cpu_cores", cpu_count);
    res = res.with_metric("cpu_usage_pct", cpu_usage_pct);
    res = res.with_metric("ram_total_mb", total_ram_mb);
    res = res.with_metric("ram_available_mb", avail_ram_mb);
    res = res.with_metric("ram_usage_pct", ram_usage_pct);

    res
}

/// Executes in-memory micro-benchmarks for `Task` JSON serde and Bincode wire-protocol codec roundtrips.
pub fn run_micro_benchmarks() -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Research, "bench");

    let task_count = 10_000usize;
    let dummy_task = Task::new(
        TaskSpec::new_command("echo", vec!["benchmark_payload".to_string()]),
        TaskRequirements {
            cpu_cores: 2,
            ram_mb: 512,
            gpu_required: false,
            timeout_secs: 30,
            max_retries: None,
        },
    );

    // Benchmark 1: Task JSON serialization & deserialization roundtrip
    let t1_start = Instant::now();
    let mut json_bytes_total = 0usize;
    for _ in 0..task_count {
        let serialized = serde_json::to_string(&dummy_task).expect("Task JSON serialize");
        json_bytes_total += serialized.len();
        let _deserialized: Task = serde_json::from_str(&serialized).expect("Task JSON deserialize");
    }
    let t1_dur = t1_start.elapsed();
    let t1_secs = t1_dur.as_secs_f64().max(1e-9);
    let serde_ops_per_sec = (task_count as f64 / t1_secs) as u64;
    let serde_mb_per_sec = (json_bytes_total as f64 / (1024.0 * 1024.0)) / t1_secs;

    // Benchmark 2: Real Bincode wire-protocol serialize_message + deserialize_message roundtrip
    let codec_iters = 10_000usize;
    let expected_task_id = dummy_task.id.to_string();
    let mut wire_task = dummy_task;
    let t2_start = Instant::now();
    let mut wire_bytes_total = 0usize;
    let mut codec_frame_bytes = 0usize;
    let mut codec_wire_discriminator = 0u8;
    let mut codec_decoded_task_id = String::new();
    let mut codec_verified_iters = 0usize;
    for i in 0..codec_iters {
        let expected_timeout = (i as u64) + 1;
        wire_task.requirements.timeout_secs = expected_timeout;
        let wire_msg = MasterMessage::AssignTask {
            task: wire_task.clone(),
        };
        let encoded =
            serialize_message(&wire_msg, WireCodec::Bincode).expect("Bincode wire serialize");
        wire_bytes_total += encoded.len();
        let (decoded, codec): (MasterMessage, WireCodec) =
            deserialize_message(&encoded).expect("Bincode wire deserialize");
        let disc = encoded.first().copied().unwrap_or(0);
        if codec == WireCodec::Bincode && disc == 0x02 {
            if let MasterMessage::AssignTask { task } = decoded {
                if task.requirements.timeout_secs == expected_timeout {
                    codec_verified_iters += 1;
                    if i == 0 {
                        codec_frame_bytes = encoded.len();
                        codec_wire_discriminator = disc;
                        codec_decoded_task_id = task.id.to_string();
                    }
                }
            }
        }
    }
    let t2_dur = t2_start.elapsed();
    let t2_secs = t2_dur.as_secs_f64().max(1e-9);
    let codec_ops_per_sec = (codec_iters as f64 / t2_secs) as u64;
    let codec_mb_per_sec = (wire_bytes_total as f64 / (1024.0 * 1024.0)) / t2_secs;

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = codec_frame_bytes > 5
        && codec_wire_discriminator == 0x02
        && codec_decoded_task_id == expected_task_id
        && codec_verified_iters == codec_iters;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let stdout = format!(
        "=== OxideSwarm Micro-Benchmarks ===\n1. Task JSON Serde Roundtrip: {} tasks in {:.3}s -> {} ops/sec ({:.2} MB/s)\n2. Bincode Wire Codec Roundtrip (MasterMessage::AssignTask): {} iterations in {:.3}s -> {} ops/sec ({:.2} MB/s)\nTotal Duration: {} ms\n",
        task_count,
        t1_secs,
        serde_ops_per_sec,
        serde_mb_per_sec,
        codec_iters,
        t2_secs,
        codec_ops_per_sec,
        codec_mb_per_sec,
        res.duration_ms
    );

    res.stdout = stdout;
    res = res.with_metric("serde_tasks_count", task_count);
    res = res.with_metric("serde_ops_per_sec", serde_ops_per_sec);
    res = res.with_metric("serde_mb_per_sec", serde_mb_per_sec);
    res = res.with_metric("codec_ops_per_sec", codec_ops_per_sec);
    res = res.with_metric("codec_mb_per_sec", codec_mb_per_sec);
    res = res.with_metric("codec_frame_bytes", codec_frame_bytes);
    res = res.with_metric("codec_wire_discriminator", codec_wire_discriminator);
    res = res.with_metric("codec_decoded_task_id", codec_decoded_task_id);
    res = res.with_metric("expected_task_id", expected_task_id);
    res = res.with_metric("codec_verified_iters", codec_verified_iters);

    res
}

/// Synthetic worker profile for in-memory scheduling simulation.
#[derive(Debug, Clone)]
struct SimWorker {
    pub caps: WorkerCapabilities,
    pub active_tasks: usize,
    pub current_cpu_load: f32,
}

/// Executes an in-memory synthetic scheduling simulation and computes decision latency percentiles.
///
/// Explicitly labeled as an in-memory simulation (not a live cluster) with synthetic worker profiles.
pub fn run_distributed_benchmark(_workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Research, "distributed");

    let mut workers = [
        SimWorker {
            caps: WorkerCapabilities::new("sim-cpu-node-a", 16, 32768, false, false, None),
            active_tasks: 0,
            current_cpu_load: 15.0,
        },
        SimWorker {
            caps: WorkerCapabilities::new("sim-cpu-node-b", 8, 16384, false, false, None),
            active_tasks: 0,
            current_cpu_load: 25.0,
        },
        SimWorker {
            caps: WorkerCapabilities::new("sim-gpu-node", 12, 24576, false, true, None),
            active_tasks: 0,
            current_cpu_load: 10.0,
        },
        SimWorker {
            caps: WorkerCapabilities::new("sim-mobile-node", 8, 8192, false, false, None)
                .with_mobile(Some(MobileCapabilities {
                    os_version: "synthetic-mobile".to_string(),
                    soc_model: "synthetic-soc".to_string(),
                    battery_pct: Some(78),
                    is_charging: Some(false),
                    thermal_throttled: false,
                })),
            active_tasks: 0,
            current_cpu_load: 12.0,
        },
    ];

    let task_count = 1000usize;
    let mut tasks = Vec::with_capacity(task_count);
    for i in 0..task_count {
        let (cpu, ram, gpu) = match i % 10 {
            0..=5 => (1, 256, false),
            6..=8 => (4, 2048, false),
            _ => (2, 1024, true),
        };
        tasks.push(TaskRequirements {
            cpu_cores: cpu,
            ram_mb: ram,
            gpu_required: gpu,
            timeout_secs: 60,
            max_retries: None,
        });
    }

    let sim_start = Instant::now();
    let mut latencies_us = Vec::with_capacity(task_count);
    let mut worker_allocations: HashMap<String, usize> = HashMap::new();
    let mut scheduled_count = 0usize;

    for task_req in &tasks {
        let t_start = Instant::now();
        let mut best_idx = None;
        let mut best_score = f32::MAX;

        for (idx, w) in workers.iter().enumerate() {
            if !w.caps.satisfies(task_req) {
                continue;
            }

            if let Some(ref m) = w.caps.mobile {
                if m.thermal_throttled || m.battery_pct.unwrap_or(100) < 15 {
                    continue;
                }
            }

            let gpu_penalty =
                if !task_req.gpu_required && (w.caps.has_gpu || w.caps.is_simulated_gpu) {
                    100.0
                } else {
                    0.0
                };

            let score = (w.active_tasks as f32) * 10.0 + w.current_cpu_load + gpu_penalty;
            if score < best_score {
                best_score = score;
                best_idx = Some(idx);
            }
        }

        let elapsed_us = t_start.elapsed().as_nanos() as f64 / 1000.0;
        latencies_us.push(elapsed_us);

        if let Some(idx) = best_idx {
            scheduled_count += 1;
            let w = &mut workers[idx];
            w.active_tasks += 1;
            *worker_allocations.entry(w.caps.name.clone()).or_insert(0) += 1;
        }
    }

    let sim_dur = sim_start.elapsed();
    let sim_secs = sim_dur.as_secs_f64();
    let throughput_ops_sec = if sim_secs > 0.0 {
        (scheduled_count as f64 / sim_secs) as u64
    } else {
        scheduled_count as u64
    };

    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50_us = latencies_us
        .get(latencies_us.len() / 2)
        .copied()
        .unwrap_or(0.0);
    let p90_us = latencies_us
        .get((latencies_us.len() * 90) / 100)
        .copied()
        .unwrap_or(0.0);
    let p95_us = latencies_us
        .get((latencies_us.len() * 95) / 100)
        .copied()
        .unwrap_or(0.0);
    let p99_us = latencies_us
        .get((latencies_us.len() * 99) / 100)
        .copied()
        .unwrap_or(0.0);
    let min_us = latencies_us.first().copied().unwrap_or(0.0);
    let max_us = latencies_us.last().copied().unwrap_or(0.0);

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = scheduled_count == task_count;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let mut stdout = format!(
        "=== In-Memory Synthetic Scheduling Simulation (not a live cluster) ===\nStatus: {}\nTotal Synthetic Tasks: {}\nScheduled: {} / {}\nSimulated Decision Throughput: {} tasks/sec (Simulation time: {:.3}s)\n\nLatency Percentiles (In-memory decision time per task):\n  ● Min     : {:.2} µs\n  ● P50     : {:.2} µs\n  ● P90     : {:.2} µs\n  ● P95     : {:.2} µs\n  ● P99     : {:.2} µs\n  ● Max     : {:.2} µs\n\nSynthetic Worker Allocation Breakdown:\n",
        if res.success {
            "[COMPLETED - SYNTHETIC SIMULATION]"
        } else {
            "[INCOMPLETE]"
        },
        task_count,
        scheduled_count,
        task_count,
        throughput_ops_sec,
        sim_secs,
        min_us,
        p50_us,
        p90_us,
        p95_us,
        p99_us,
        max_us
    );

    let mut sorted_workers: Vec<_> = worker_allocations.iter().collect();
    sorted_workers.sort_by_key(|(name, _)| (*name).clone());
    for (worker_name, count) in sorted_workers {
        let pct = (*count as f64 / scheduled_count.max(1) as f64) * 100.0;
        stdout.push_str(&format!(
            "  ● {:<22} -> {:>4} tasks ({:>5.1}%)\n",
            worker_name, count, pct
        ));
    }

    res.stdout = stdout;
    res = res.with_metric("simulation_only", true);
    res = res.with_metric("total_tasks", task_count);
    res = res.with_metric("scheduled_tasks", scheduled_count);
    res = res.with_metric("throughput_tasks_per_sec", throughput_ops_sec);
    res = res.with_metric("latency_p50_us", p50_us);
    res = res.with_metric("latency_p90_us", p90_us);
    res = res.with_metric("latency_p95_us", p95_us);
    res = res.with_metric("latency_p99_us", p99_us);
    res = res.with_metric("latency_min_us", min_us);
    res = res.with_metric("latency_max_us", max_us);
    res = res.with_metric("worker_allocations", worker_allocations);
    res = res.with_metric("all_tasks_scheduled", scheduled_count == task_count);

    res
}

/// Generates a research report combining host resource telemetry, micro-benchmarks, and the synthetic simulation.
pub fn generate_research_report(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Research, "report");

    let profile = run_hardware_profile();
    let bench = run_micro_benchmarks();
    let dist = run_distributed_benchmark(workspace_root);

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = profile.success && bench.success && dist.success;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let report_data = serde_json::json!({
        "timestamp": Utc::now().to_rfc3339(),
        "hardware_profile": profile.metrics,
        "micro_benchmarks": bench.metrics,
        "distributed_simulation": dist.metrics,
        "summary": {
            "status": if res.success { "completed" } else { "failed" },
            "simulation_only": true,
            "total_benchmark_duration_ms": res.duration_ms
        }
    });

    let report_file = workspace_root.join("target/research_report.json");
    let write_res =
        fs::create_dir_all(report_file.parent().unwrap_or(workspace_root)).and_then(|_| {
            let json_str = serde_json::to_string_pretty(&report_data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            fs::write(&report_file, json_str)
        });
    if let Err(e) = write_res {
        res.success = false;
        res.exit_code = Some(1);
        res.stderr = format!(
            "Failed to write research report to {}: {}",
            report_file.display(),
            e
        );
        return res;
    }

    let stdout = format!(
        "=== OxideSwarm Research & Performance Benchmark Report ===\nReport Generated: {}\nSaved Path: {}\n\n{}\n{}\n{}\n",
        Utc::now().to_rfc3339(),
        report_file.display(),
        profile.stdout,
        bench.stdout,
        dist.stdout
    );

    res.stdout = stdout;
    res = res.with_metric("report_file", report_file.to_string_lossy().to_string());
    res = res.with_metric("hardware", &profile.metrics);
    res = res.with_metric("hardware_profile", &profile.metrics);
    res = res.with_metric("micro_benchmarks", &bench.metrics);
    res = res.with_metric("distributed_simulation", &dist.metrics);
    res = res.with_metric("report_data", report_data);

    res
}

// ==============================================================================
// 5. UNIFIED ENGINE DISPATCHER & CLI RUNNER
// ==============================================================================

/// Dispatches any workflow mode and action in the synchronous engine path.
///
/// Note: `dev cluster` and `dev stop` require process lifecycle management from the
/// `ox-mode` CLI (`execute_single_action`); calling them here (for example from the
/// Web UI dashboard) returns `success = false`.
pub fn execute_mode_action(
    mode: WorkflowMode,
    action: &str,
    workspace_root: &Path,
) -> ModeExecutionResult {
    let action_clean = action.trim().to_lowercase();
    match (mode, action_clean.as_str()) {
        // Test mode
        (WorkflowMode::Test, "quick") => run_quick_tests(workspace_root),
        (WorkflowMode::Test, "full") => run_full_tests(workspace_root),
        (WorkflowMode::Test, "lint") => run_lints(workspace_root),
        (WorkflowMode::Test, "flaky") => run_flaky_detection(workspace_root, 5),

        // Dev mode
        (WorkflowMode::Dev, "fast") => run_fast_check(workspace_root),
        (WorkflowMode::Dev, "watch") | (WorkflowMode::Dev, "reload") => {
            run_watch_check(workspace_root)
        }
        (WorkflowMode::Dev, "status") => probe_cluster_health_with_root(None, workspace_root),
        (WorkflowMode::Dev, "cluster") => {
            let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "cluster");
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = "Action 'dev cluster' cannot run in the non-CLI engine / dashboard path; run `ox-mode dev cluster` from the CLI.".to_string();
            res
        }
        (WorkflowMode::Dev, "stop") => {
            let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "stop");
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = "Action 'dev stop' cannot run in the non-CLI engine / dashboard path; run `ox-mode dev stop` from the CLI.".to_string();
            res
        }

        // Doc mode
        (WorkflowMode::Doc, "build") => run_doc_build(workspace_root),
        (WorkflowMode::Doc, "verify") => verify_documentation(workspace_root),
        (WorkflowMode::Doc, "export") | (WorkflowMode::Doc, "update") => {
            export_specification(workspace_root)
        }

        // Research mode
        (WorkflowMode::Research, "profile") => run_hardware_profile(),
        (WorkflowMode::Research, "bench") => run_micro_benchmarks(),
        (WorkflowMode::Research, "distributed") => run_distributed_benchmark(workspace_root),
        (WorkflowMode::Research, "report") => generate_research_report(workspace_root),

        // Fallback for unknown action
        (m, act) => {
            let mut res = ModeExecutionResult::new(m, act);
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!(
                "Unknown action '{}' for mode '{:?}'. Available actions: {:?}",
                act,
                m,
                m.available_actions()
            );
            res
        }
    }
}

fn extract_count(line: &str, keyword: &str) -> Option<usize> {
    if let Some(pos) = line.find(keyword) {
        let prefix = line[..pos].trim();
        if let Some(num_str) = prefix.split_whitespace().last() {
            return num_str.parse().ok();
        }
    }
    None
}

/// Executes the mode CLI workflow.
pub async fn run_mode_cli(args: ModeCliArgs) -> i32 {
    let workspace_root = find_workspace_root();
    let bearer_token = resolve_dashboard_bearer_token();
    run_mode_cli_with_bearer(args, &workspace_root, bearer_token.as_deref()).await
}

/// Executes the mode CLI workflow with an explicit workspace root and optional `ClusterSecret`.
pub async fn run_mode_cli_with_secret(
    args: ModeCliArgs,
    workspace_root: &Path,
    secret: Option<&ClusterSecret>,
) -> i32 {
    let bearer_token = secret.map(rusty_grid_master::dashboard_admin_token);
    run_mode_cli_with_bearer(args, workspace_root, bearer_token.as_deref()).await
}

async fn run_mode_cli_with_bearer(
    args: ModeCliArgs,
    workspace_root: &Path,
    bearer_token: Option<&str>,
) -> i32 {
    rusty_grid_core::mode::set_mode_runner_hook(execute_mode_action);

    if args.mode.is_none() || args.interactive {
        return run_interactive_menu(workspace_root).await;
    }

    let mode_str = args.mode.as_deref().unwrap_or("");
    let parsed_mode = match WorkflowMode::from_str_loose(mode_str) {
        Some(m) => m,
        None => {
            eprintln!(
                "{}Error: Unknown workflow mode '{}'. Available: test, dev, doc, research.{}",
                CLR_RED, mode_str, CLR_RESET
            );
            eprintln!("Run `ox-mode --help` or `ox-mode -i` for the interactive menu.");
            return 1;
        }
    };

    let action_str = args
        .action
        .or(args.action_opt)
        .unwrap_or_else(|| match parsed_mode {
            WorkflowMode::Test => "quick".to_string(),
            WorkflowMode::Dev => "fast".to_string(),
            WorkflowMode::Doc => "verify".to_string(),
            WorkflowMode::Research => "profile".to_string(),
        });

    let result =
        if parsed_mode == WorkflowMode::Dev && action_str.trim().eq_ignore_ascii_case("status") {
            probe_cluster_health_with_bearer(args.port, workspace_root, bearer_token)
        } else {
            execute_single_action(parsed_mode, &action_str, args.iterations, workspace_root).await
        };

    if args.json {
        if let Ok(json) = serde_json::to_string_pretty(&result) {
            println!("{}", json);
        }
    } else {
        render_execution_result(&result);
    }

    if result.success {
        0
    } else {
        result.exit_code.unwrap_or(1)
    }
}

/// Dispatches an action with CLI support for cluster launch/stop and live-reload.
pub async fn execute_single_action(
    mode: WorkflowMode,
    action: &str,
    iterations: usize,
    workspace_root: &Path,
) -> ModeExecutionResult {
    let action_clean = action.trim().to_lowercase();

    if mode == WorkflowMode::Test && action_clean == "flaky" {
        return run_flaky_detection(workspace_root, iterations);
    }

    if mode == WorkflowMode::Dev && (action_clean == "watch" || action_clean == "reload") {
        return run_live_reload_watch(workspace_root).await;
    }

    if mode == WorkflowMode::Dev && action_clean == "cluster" {
        return launch_local_dev_cluster(workspace_root).await;
    }

    if mode == WorkflowMode::Dev && action_clean == "stop" {
        return stop_local_dev_cluster(workspace_root).await;
    }

    execute_mode_action(mode, &action_clean, workspace_root)
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

static SPAWNED_CLUSTER_CHILDREN: std::sync::Mutex<Vec<std::process::Child>> =
    std::sync::Mutex::new(Vec::new());

fn register_spawned_cluster_children(children: Vec<std::process::Child>) {
    if let Ok(mut guard) = SPAWNED_CLUSTER_CHILDREN.lock() {
        guard.retain_mut(|c| matches!(c.try_wait(), Ok(None)));
        guard.extend(children);
    }
}

fn try_reap_spawned_cluster_child(pid: u32) -> bool {
    if let Ok(mut guard) = SPAWNED_CLUSTER_CHILDREN.lock() {
        let mut reaped = false;
        guard.retain_mut(|child| {
            if child.id() == pid {
                match child.try_wait() {
                    Ok(Some(_)) => {
                        reaped = true;
                        false
                    }
                    Ok(None) => true,
                    Err(_) => false,
                }
            } else {
                true
            }
        });
        return reaped;
    }
    false
}

fn query_process_start_time(pid: u32) -> Option<u64> {
    let mut sys = System::new();
    let sys_pid = sysinfo::Pid::from_u32(pid);
    if sys.refresh_process(sys_pid) {
        if let Some(proc) = sys.process(sys_pid) {
            let st = proc.start_time();
            if st > 0 {
                return Some(st);
            }
        }
    }
    None
}

fn write_cluster_tracking_files(
    pid_file: &Path,
    json_file: &Path,
    master_pid: u32,
    master_tcp_port: u16,
    web_ui_port: u16,
    spawned_pids: &[String],
    pid_start_times: &std::collections::BTreeMap<String, u64>,
) -> io::Result<()> {
    fs::write(pid_file, spawned_pids.join("\n"))?;
    let cluster_json = serde_json::json!({
        "master_pid": master_pid,
        "master_tcp": format!("127.0.0.1:{}", master_tcp_port),
        "web_ui_port": web_ui_port,
        "web_ui_url": format!("http://127.0.0.1:{}", web_ui_port),
        "launched_at_unix": current_unix_secs(),
        "pids": spawned_pids,
        "pid_start_times": pid_start_times,
    });
    let json_str = serde_json::to_string_pretty(&cluster_json)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    fs::write(json_file, json_str)
}

fn cleanup_failed_cluster_launch(
    children: &mut [&mut std::process::Child],
    pid_file: &Path,
    json_file: &Path,
    dash_port_file: &Path,
    isolated_home: Option<&Path>,
) {
    for child in children {
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = fs::remove_file(pid_file);
    let _ = fs::remove_file(json_file);
    let _ = fs::remove_file(dash_port_file);
    if let Some(home) = isolated_home {
        let _ = fs::remove_dir_all(home);
    }
}

/// Spawns local dev cluster: 1 Master with Web UI + 2 Workers in background with PID tracking,
/// verifying that all children stay alive and both workers appear in `/api/status`.
pub async fn launch_local_dev_cluster(workspace_root: &Path) -> ModeExecutionResult {
    launch_local_dev_cluster_with_ports(workspace_root, 9090, 8080).await
}

/// Spawns local dev cluster on the specified `master_tcp_port` and `web_ui_port`,
/// calling `try_wait()` on each child and polling `/api/status` until both workers register.
pub async fn launch_local_dev_cluster_with_ports(
    workspace_root: &Path,
    master_tcp_port: u16,
    web_ui_port: u16,
) -> ModeExecutionResult {
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "cluster");
    let start = Instant::now();

    let pid_file = workspace_root.join(".dev_cluster.pids");
    let json_file = workspace_root.join(".dev_cluster.json");
    let dash_port_file = workspace_root.join(".dev_cluster.dashboard_port");
    if pid_file.exists() || json_file.exists() {
        let stop_res = stop_local_dev_cluster(workspace_root).await;
        if !stop_res.success {
            res.duration_ms = start.elapsed().as_millis() as u64;
            res.success = false;
            res.exit_code = stop_res.exit_code.or(Some(1));
            res.stderr = format!(
                "Failed to stop existing dev cluster before launch: {}",
                if stop_res.stderr.is_empty() {
                    &stop_res.stdout
                } else {
                    &stop_res.stderr
                }
            );
            return res;
        }
    }
    let _ = fs::remove_file(&dash_port_file);

    // When workspace_root is a temporary/non-workspace directory (e.g. during integration tests),
    // isolate child HOME/USERPROFILE and config inside workspace_root so the host's real HOME is never touched.
    let isolated_home = if workspace_root.join("Cargo.toml").exists() {
        None
    } else {
        let home_dir = workspace_root.join(".dev_cluster_home");
        let _ = fs::create_dir_all(&home_dir);
        let _ = fs::write(home_dir.join("rusty-grid.toml"), "");
        Some(home_dir)
    };

    let bin_name = if cfg!(windows) {
        "rusty-grid.exe"
    } else {
        "rusty-grid"
    };
    let mut target_bin = resolve_cargo_target_dir(workspace_root)
        .join("debug")
        .join(bin_name);
    if !target_bin.exists() {
        let fallback_root = find_workspace_root();
        let fallback_bin = resolve_cargo_target_dir(&fallback_root)
            .join("debug")
            .join(bin_name);
        if fallback_bin.exists() {
            target_bin = fallback_bin;
        } else {
            match Command::new("cargo")
                .args(["build", "--bin", "rusty-grid"])
                .current_dir(&fallback_root)
                .output()
            {
                Ok(out) if out.status.success() => {
                    if fallback_bin.exists() {
                        target_bin = fallback_bin;
                    }
                }
                Ok(out) => {
                    res.success = false;
                    res.exit_code = out.status.code().or(Some(1));
                    res.stderr = format!(
                        "Failed to build rusty-grid binary: {}",
                        String::from_utf8_lossy(&out.stderr).trim()
                    );
                    return res;
                }
                Err(e) => {
                    res.success = false;
                    res.exit_code = Some(1);
                    res.stderr = format!("Failed to build rusty-grid binary: {}", e);
                    return res;
                }
            }
        }
    }

    let mut spawned_pids = Vec::new();
    let mut pid_start_times: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();

    let mut master_cmd = Command::new(&target_bin);
    if let Some(ref home_dir) = isolated_home {
        let cfg_path = home_dir.join("rusty-grid.toml");
        let blob_dir = home_dir.join("blobs");
        master_cmd
            .env("HOME", home_dir)
            .env("USERPROFILE", home_dir)
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("OXIDESWARM_SECRET_FILE")
            .env_remove("RUSTY_GRID_CONFIG")
            .arg("--config")
            .arg(&cfg_path);
        master_cmd.args([
            "master",
            "--listen",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--web-ui-addr",
            &format!("127.0.0.1:{}", web_ui_port),
            "--dashboard-port-file",
        ]);
        master_cmd.arg(&dash_port_file);
        master_cmd.args(["--ephemeral-key", "--blob-dir"]);
        master_cmd.arg(&blob_dir);
    } else {
        master_cmd.args([
            "master",
            "--listen",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--web-ui-addr",
            &format!("127.0.0.1:{}", web_ui_port),
            "--dashboard-port-file",
        ]);
        master_cmd.arg(&dash_port_file);
        master_cmd.arg("--ephemeral-key");
    }
    let master_res = master_cmd
        .current_dir(workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    let mut master_child = match master_res {
        Ok(child) => {
            let pid = child.id();
            spawned_pids.push(format!("master:{}", pid));
            if let Some(st) = query_process_start_time(pid) {
                pid_start_times.insert(pid.to_string(), st);
            }
            child
        }
        Err(e) => {
            cleanup_failed_cluster_launch(
                &mut [],
                &pid_file,
                &json_file,
                &dash_port_file,
                isolated_home.as_deref(),
            );
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to spawn Master: {}", e);
            return res;
        }
    };
    let master_pid = master_child.id();

    if let Err(e) = write_cluster_tracking_files(
        &pid_file,
        &json_file,
        master_pid,
        master_tcp_port,
        web_ui_port,
        &spawned_pids,
        &pid_start_times,
    ) {
        cleanup_failed_cluster_launch(
            &mut [&mut master_child],
            &pid_file,
            &json_file,
            &dash_port_file,
            isolated_home.as_deref(),
        );
        res.duration_ms = start.elapsed().as_millis() as u64;
        res.success = false;
        res.exit_code = Some(1);
        res.stderr = format!(
            "Failed to write cluster tracking files ({}): {}",
            pid_file.display(),
            e
        );
        return res;
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    if let Ok(Some(status)) = master_child.try_wait() {
        cleanup_failed_cluster_launch(
            &mut [&mut master_child],
            &pid_file,
            &json_file,
            &dash_port_file,
            isolated_home.as_deref(),
        );
        res.duration_ms = start.elapsed().as_millis() as u64;
        res.success = false;
        res.exit_code = Some(1);
        res.stderr = format!(
            "Master process exited immediately with {status} (port 127.0.0.1:{master_tcp_port} or 127.0.0.1:{web_ui_port} may already be in use)"
        );
        return res;
    }

    let mut w1_cmd = Command::new(&target_bin);
    if let Some(ref home_dir) = isolated_home {
        let cfg_path = home_dir.join("rusty-grid.toml");
        let sandbox_dir = home_dir.join("sandboxes");
        let _ = fs::create_dir_all(&sandbox_dir);
        w1_cmd
            .env("HOME", home_dir)
            .env("USERPROFILE", home_dir)
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("OXIDESWARM_SECRET_FILE")
            .env_remove("RUSTY_GRID_CONFIG")
            .arg("--config")
            .arg(&cfg_path);
        w1_cmd.args([
            "worker",
            "--master",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--name",
            "dev-worker-cpu-1",
            "--cores",
            "4",
            "--ram",
            "8192",
            "--no-portal",
            "--sandbox-base-dir",
        ]);
        w1_cmd.arg(&sandbox_dir);
    } else {
        w1_cmd.args([
            "worker",
            "--master",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--name",
            "dev-worker-cpu-1",
            "--cores",
            "4",
            "--ram",
            "8192",
            "--no-portal",
        ]);
    }
    let w1_res = w1_cmd
        .current_dir(workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    let mut w1_child = match w1_res {
        Ok(child) => {
            let pid = child.id();
            spawned_pids.push(format!("worker_cpu:{}", pid));
            if let Some(st) = query_process_start_time(pid) {
                pid_start_times.insert(pid.to_string(), st);
            }
            child
        }
        Err(e) => {
            cleanup_failed_cluster_launch(
                &mut [&mut master_child],
                &pid_file,
                &json_file,
                &dash_port_file,
                isolated_home.as_deref(),
            );
            res.duration_ms = start.elapsed().as_millis() as u64;
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to spawn worker dev-worker-cpu-1: {}", e);
            return res;
        }
    };

    if let Err(e) = write_cluster_tracking_files(
        &pid_file,
        &json_file,
        master_pid,
        master_tcp_port,
        web_ui_port,
        &spawned_pids,
        &pid_start_times,
    ) {
        cleanup_failed_cluster_launch(
            &mut [&mut master_child, &mut w1_child],
            &pid_file,
            &json_file,
            &dash_port_file,
            isolated_home.as_deref(),
        );
        res.duration_ms = start.elapsed().as_millis() as u64;
        res.success = false;
        res.exit_code = Some(1);
        res.stderr = format!(
            "Failed to write cluster tracking files ({}): {}",
            pid_file.display(),
            e
        );
        return res;
    }

    let mut w2_cmd = Command::new(&target_bin);
    if let Some(ref home_dir) = isolated_home {
        let cfg_path = home_dir.join("rusty-grid.toml");
        let sandbox_dir = home_dir.join("sandboxes");
        let _ = fs::create_dir_all(&sandbox_dir);
        w2_cmd
            .env("HOME", home_dir)
            .env("USERPROFILE", home_dir)
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("OXIDESWARM_SECRET_FILE")
            .env_remove("RUSTY_GRID_CONFIG")
            .arg("--config")
            .arg(&cfg_path);
        w2_cmd.args([
            "worker",
            "--master",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--name",
            "dev-worker-sim-gpu-2",
            "--cores",
            "8",
            "--ram",
            "16384",
            "--simulate-gpu",
            "--no-portal",
            "--sandbox-base-dir",
        ]);
        w2_cmd.arg(&sandbox_dir);
    } else {
        w2_cmd.args([
            "worker",
            "--master",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--name",
            "dev-worker-sim-gpu-2",
            "--cores",
            "8",
            "--ram",
            "16384",
            "--simulate-gpu",
            "--no-portal",
        ]);
    }
    let w2_res = w2_cmd
        .current_dir(workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    let mut w2_child = match w2_res {
        Ok(child) => {
            let pid = child.id();
            spawned_pids.push(format!("worker_gpu:{}", pid));
            if let Some(st) = query_process_start_time(pid) {
                pid_start_times.insert(pid.to_string(), st);
            }
            child
        }
        Err(e) => {
            cleanup_failed_cluster_launch(
                &mut [&mut master_child, &mut w1_child],
                &pid_file,
                &json_file,
                &dash_port_file,
                isolated_home.as_deref(),
            );
            res.duration_ms = start.elapsed().as_millis() as u64;
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to spawn worker dev-worker-sim-gpu-2: {}", e);
            return res;
        }
    };

    if let Err(e) = write_cluster_tracking_files(
        &pid_file,
        &json_file,
        master_pid,
        master_tcp_port,
        web_ui_port,
        &spawned_pids,
        &pid_start_times,
    ) {
        cleanup_failed_cluster_launch(
            &mut [&mut master_child, &mut w1_child, &mut w2_child],
            &pid_file,
            &json_file,
            &dash_port_file,
            isolated_home.as_deref(),
        );
        res.duration_ms = start.elapsed().as_millis() as u64;
        res.success = false;
        res.exit_code = Some(1);
        res.stderr = format!(
            "Failed to write cluster tracking files ({}): {}",
            pid_file.display(),
            e
        );
        return res;
    }

    let bearer_token = if let Some(ref home_dir) = isolated_home {
        let secret_file = home_dir.join(".oxideswarm").join("cluster.secret");
        ClusterSecret::load_file(&secret_file)
            .ok()
            .map(|s| rusty_grid_master::dashboard_admin_token(&s))
    } else {
        resolve_dashboard_bearer_token()
    };
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut cluster_ready = false;
    let mut failure_reason = format!(
        "Dashboard port file ({}) did not appear within 8s for 127.0.0.1:{web_ui_port}",
        dash_port_file.display()
    );

    while Instant::now() < deadline {
        let mut child_failed: Option<String> = None;
        for (label, child) in [
            ("Master", &mut master_child),
            ("Worker dev-worker-cpu-1", &mut w1_child),
            ("Worker dev-worker-sim-gpu-2", &mut w2_child),
        ] {
            match child.try_wait() {
                Ok(Some(status)) => {
                    child_failed = Some(format!("{label} exited prematurely with {status}"));
                    break;
                }
                Ok(None) => {}
                Err(e) => {
                    child_failed = Some(format!("Failed to check status of {label}: {e}"));
                    break;
                }
            }
        }
        if let Some(reason) = child_failed {
            cleanup_failed_cluster_launch(
                &mut [&mut master_child, &mut w1_child, &mut w2_child],
                &pid_file,
                &json_file,
                &dash_port_file,
                isolated_home.as_deref(),
            );
            res.duration_ms = start.elapsed().as_millis() as u64;
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = reason;
            return res;
        }

        // Never poll or send the dashboard Admin token to `web_ui_port` until `--dashboard-port-file`
        // proves that our spawned Master actually bound `web_ui_port`.
        let dashboard_bound = fs::read_to_string(&dash_port_file)
            .ok()
            .and_then(|s| s.trim().parse::<u16>().ok())
            == Some(web_ui_port);
        if !dashboard_bound {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        failure_reason = format!(
            "Timed out waiting for master on http://127.0.0.1:{web_ui_port}/api/status and both workers to register on 127.0.0.1:{master_tcp_port}"
        );

        if let Ok((status_code, body)) = http_get_local_status(web_ui_port, bearer_token.as_deref())
        {
            if (200..300).contains(&status_code) {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                    if let Some(workers) = json.get("workers").and_then(|w| w.as_array()) {
                        let has_w1 = workers.iter().any(|w| {
                            w.get("name").and_then(|n| n.as_str()) == Some("dev-worker-cpu-1")
                        });
                        let has_w2 = workers.iter().any(|w| {
                            w.get("name").and_then(|n| n.as_str()) == Some("dev-worker-sim-gpu-2")
                        });
                        if has_w1 && has_w2 {
                            cluster_ready = true;
                            break;
                        }
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if !cluster_ready {
        cleanup_failed_cluster_launch(
            &mut [&mut master_child, &mut w1_child, &mut w2_child],
            &pid_file,
            &json_file,
            &dash_port_file,
            isolated_home.as_deref(),
        );
        res.duration_ms = start.elapsed().as_millis() as u64;
        res.success = false;
        res.exit_code = Some(1);
        res.stderr = failure_reason;
        return res;
    }

    // Backfill any start times that were not yet populated at the instant of `spawn()`,
    // and retain the `Child` handles so an in-process `stop_local_dev_cluster` can reap them.
    for pid in [master_pid, w1_child.id(), w2_child.id()] {
        if let std::collections::btree_map::Entry::Vacant(e) =
            pid_start_times.entry(pid.to_string())
        {
            if let Some(st) = query_process_start_time(pid) {
                e.insert(st);
            }
        }
    }
    let _ = write_cluster_tracking_files(
        &pid_file,
        &json_file,
        master_pid,
        master_tcp_port,
        web_ui_port,
        &spawned_pids,
        &pid_start_times,
    );
    register_spawned_cluster_children(vec![master_child, w1_child, w2_child]);

    let _ = fs::remove_file(&dash_port_file);

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = true;
    res.exit_code = Some(0);
    res.stdout = format!(
        "=== Local Development Cluster Launched ===\nMaster Coordinator : tcp://127.0.0.1:{} (PID: {})\nWeb UI Dashboard   : http://127.0.0.1:{}\nWorker 1 (CPU Node) : dev-worker-cpu-1 (4 Cores, 8 GB RAM)\nWorker 2 (Sim GPU)  : dev-worker-sim-gpu-2 (8 Cores, 16 GB RAM, --simulate-gpu)\nPID tracking files : {}\n                     {}\n\nRun `ox-mode dev status` to inspect cluster health.\nRun `ox-mode dev stop` to shut down the cluster cleanly.\n",
        master_tcp_port,
        master_pid,
        web_ui_port,
        pid_file.display(),
        json_file.display()
    );

    res = res.with_metric("master_pid", master_pid);
    res = res.with_metric("master_tcp", format!("127.0.0.1:{}", master_tcp_port));
    res = res.with_metric("web_ui_url", format!("http://127.0.0.1:{}", web_ui_port));
    res = res.with_metric("web_ui_port", web_ui_port);

    res
}

fn tracking_file_unix_secs(pid_file: &Path, json_file: &Path) -> Option<u64> {
    if let Ok(content) = fs::read_to_string(json_file) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&content) {
            if let Some(ts) = val.get("launched_at_unix").and_then(|v| v.as_u64()) {
                if ts > 0 {
                    return Some(ts);
                }
            }
        }
    }
    let meta = fs::metadata(pid_file)
        .or_else(|_| fs::metadata(json_file))
        .ok()?;
    let mtime = meta.modified().ok()?;
    mtime
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs())
}

fn is_oxideswarm_cluster_process(proc: &sysinfo::Process) -> bool {
    let matches_stem = |raw: &str| {
        let stem = raw
            .trim()
            .strip_suffix(".exe")
            .or_else(|| raw.trim().strip_suffix(".EXE"))
            .unwrap_or(raw.trim());
        stem.eq_ignore_ascii_case("rusty-grid") || stem.eq_ignore_ascii_case("oxideswarm")
    };
    if matches_stem(proc.name()) {
        return true;
    }
    if let Some(exe_name) = proc
        .exe()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
    {
        if matches_stem(exe_name) {
            return true;
        }
    }
    false
}

fn has_dev_cluster_cmdline_signature(proc: &sysinfo::Process) -> bool {
    proc.cmd().iter().any(|arg| {
        arg == "--dashboard-port-file"
            || arg == "dev-worker-cpu-1"
            || arg == "dev-worker-sim-gpu-2"
            || arg.contains(".dev_cluster.dashboard_port")
    })
}

fn signal_verified_process(sys: &System, pid: u32) -> bool {
    #[cfg(unix)]
    {
        let _ = sys;
        Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
    #[cfg(windows)]
    {
        sys.process(sysinfo::Pid::from_u32(pid))
            .map(|p| p.kill())
            .unwrap_or(false)
    }
}

#[cfg(unix)]
fn is_unix_process_zombie_or_gone(pid: u32) -> bool {
    match Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
    {
        Ok(out) => {
            if !out.status.success() {
                return true;
            }
            let stat = String::from_utf8_lossy(&out.stdout);
            let trimmed = stat.trim();
            trimmed.is_empty() || trimmed.starts_with('Z') || trimmed.starts_with('z')
        }
        Err(_) => false,
    }
}

fn is_process_exited_or_zombie(pid: u32) -> bool {
    if try_reap_spawned_cluster_child(pid) {
        return true;
    }
    let sys_pid = sysinfo::Pid::from_u32(pid);
    // Construct a fresh `System` on each check: in `sysinfo` 0.30 on macOS, calling
    // `refresh_process` on an existing `System` does not update `ProcessStatus` when a
    // process transitions to `Zombie`.
    let mut fresh_sys = System::new();
    if !fresh_sys.refresh_process(sys_pid) {
        return true;
    }
    #[cfg(unix)]
    {
        if let Some(proc) = fresh_sys.process(sys_pid) {
            if matches!(proc.status(), sysinfo::ProcessStatus::Zombie) {
                return true;
            }
        }
        if is_unix_process_zombie_or_gone(pid) {
            return true;
        }
    }
    false
}

async fn wait_for_process_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if is_process_exited_or_zombie(pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    is_process_exited_or_zombie(pid)
}

/// Stops running dev cluster child processes cleanly with ownership verification and exit waiting.
pub async fn stop_local_dev_cluster(workspace_root: &Path) -> ModeExecutionResult {
    stop_local_dev_cluster_with_signaller(workspace_root, signal_verified_process).await
}

/// Stops running dev cluster child processes using an injectable signal function (for testing).
#[doc(hidden)]
pub async fn stop_local_dev_cluster_with_signaller<F>(
    workspace_root: &Path,
    mut signal_fn: F,
) -> ModeExecutionResult
where
    F: FnMut(&System, u32) -> bool,
{
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "stop");
    let start = Instant::now();

    let pid_file = workspace_root.join(".dev_cluster.pids");
    let json_file = workspace_root.join(".dev_cluster.json");
    let dash_port_file = workspace_root.join(".dev_cluster.dashboard_port");
    let isolated_home = workspace_root.join(".dev_cluster_home");

    if !pid_file.exists() && !json_file.exists() {
        let _ = fs::remove_file(&dash_port_file);
        let _ = fs::remove_dir_all(&isolated_home);
        res.success = true;
        res.exit_code = Some(0);
        res.stdout = "No running dev cluster tracking files found.".to_string();
        return res;
    }

    let file_unix_secs = tracking_file_unix_secs(&pid_file, &json_file);
    let mut recorded_start_times: std::collections::HashMap<u32, u64> =
        std::collections::HashMap::new();
    let mut candidate_entries: Vec<Result<u32, String>> = Vec::new();
    let mut seen_u64 = std::collections::HashSet::new();

    let mut record_raw_pid_str = |raw_str: &str| {
        let trimmed = raw_str.trim();
        if trimmed.is_empty() {
            return;
        }
        match trimmed.parse::<u64>() {
            Ok(val) => {
                if seen_u64.insert(val) {
                    match u32::try_from(val) {
                        Ok(pid) => candidate_entries.push(Ok(pid)),
                        Err(_) => candidate_entries.push(Err(trimmed.to_string())),
                    }
                }
            }
            Err(_) => {
                candidate_entries.push(Err(trimmed.to_string()));
            }
        }
    };

    if let Ok(content) = fs::read_to_string(&pid_file) {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Some((_role, pid_str)) = trimmed.split_once(':') {
                record_raw_pid_str(pid_str);
            } else {
                record_raw_pid_str(trimmed);
            }
        }
    }

    if let Ok(content) = fs::read_to_string(&json_file) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&content) {
            if let Some(master_val) = val.get("master_pid") {
                if let Some(master_u64) = master_val.as_u64() {
                    record_raw_pid_str(&master_u64.to_string());
                } else if !master_val.is_null() {
                    record_raw_pid_str(&master_val.to_string());
                }
            }
            if let Some(arr) = val.get("pids").and_then(|p| p.as_array()) {
                for item in arr {
                    if let Some(line) = item.as_str() {
                        if let Some((_, pid_str)) = line.split_once(':') {
                            record_raw_pid_str(pid_str);
                        } else {
                            record_raw_pid_str(line);
                        }
                    }
                }
            }
            if let Some(st_map) = val.get("pid_start_times").and_then(|m| m.as_object()) {
                for (pid_str, st_val) in st_map {
                    if let (Ok(pid), Some(st)) = (pid_str.parse::<u32>(), st_val.as_u64()) {
                        if st > 0 {
                            recorded_start_times.insert(pid, st);
                        }
                    }
                }
            }
        }
    }

    let mut sys = System::new();
    sys.refresh_processes();

    let self_pid = std::process::id();
    let mut signalled = 0usize;
    let mut exited = 0usize;
    let mut skipped = 0usize;
    let mut signal_failed = 0usize;

    for entry in candidate_entries {
        let pid = match entry {
            Ok(p) => p,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // Never signal PID <= 1 (process group / init) or ox-mode's own PID.
        if pid <= 1 || pid == self_pid {
            skipped += 1;
            continue;
        }

        let sys_pid = sysinfo::Pid::from_u32(pid);
        let Some(proc) = sys.process(sys_pid) else {
            skipped += 1;
            continue;
        };

        // Verify executable name matches rusty-grid / oxideswarm.
        if !is_oxideswarm_cluster_process(proc) {
            skipped += 1;
            continue;
        }

        // Verify ownership by matching recorded `sysinfo` start time at spawn or dev-cluster
        // command-line arguments (`--dashboard-port-file`, `dev-worker-cpu-1`, `dev-worker-sim-gpu-2`),
        // falling back to the tracking file timestamp only when neither is available.
        let proc_start = proc.start_time();
        let has_cmdline = has_dev_cluster_cmdline_signature(proc);
        if let Some(&expected_start) = recorded_start_times.get(&pid) {
            if proc_start > 0 && proc_start.abs_diff(expected_start) > 2 && !has_cmdline {
                skipped += 1;
                continue;
            }
        } else if !has_cmdline && proc_start > 0 {
            if let Some(ft) = file_unix_secs {
                if proc_start > ft.saturating_add(5) || ft > proc_start.saturating_add(120) {
                    skipped += 1;
                    continue;
                }
            }
        }

        if signal_fn(&sys, pid) {
            signalled += 1;
            if wait_for_process_exit(pid, Duration::from_secs(3)).await {
                exited += 1;
            }
        } else if is_process_exited_or_zombie(pid) {
            skipped += 1;
        } else {
            signal_failed += 1;
        }
    }

    let timed_out = signalled.saturating_sub(exited);
    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = timed_out == 0 && signal_failed == 0;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    // Only delete the tracking files when all verified processes exited cleanly;
    // keep them when any verified process could not be signalled or did not exit.
    if res.success {
        let _ = fs::remove_file(&pid_file);
        let _ = fs::remove_file(&json_file);
        let _ = fs::remove_file(&dash_port_file);
        let _ = fs::remove_dir_all(&isolated_home);
    }

    res.stdout = format!(
        "Dev cluster stop summary: {} signalled, {} exited, {} failed to signal, {} stale/unrelated PID entry(ies) skipped.",
        signalled, exited, signal_failed, skipped
    );
    if !res.success {
        res.stderr = format!(
            "{} of {} signalled dev cluster process(es) did not exit within timeout; {} verified process(es) could not be signalled.",
            timed_out, signalled, signal_failed
        );
    }
    res = res.with_metric("signalled_count", signalled);
    res = res.with_metric("exited_count", exited);
    res = res.with_metric("signal_failed_count", signal_failed);
    res = res.with_metric("skipped_count", skipped);
    res = res.with_metric("stopped_count", exited);

    res
}

/// Runs continuous live-reload watcher loop in the terminal.
pub async fn run_live_reload_watch(workspace_root: &Path) -> ModeExecutionResult {
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "watch");
    let start = Instant::now();

    println!(
        "{}{}================================================================================",
        CLR_BOLD, CLR_CYAN
    );
    println!("       🔄 OXIDESWARM LIVE-RELOAD (WATCH MODE) CONTROLLER");
    println!(
        "================================================================================{}",
        CLR_RESET
    );
    println!("Monitoring workspace Rust files (`crates/`, `tests/`, `Cargo.toml`)...");
    println!("Auto-triggering fast compiler check on file modification.");
    println!(
        "Press {}Ctrl+C{} to exit live-reload mode.\n",
        CLR_BOLD, CLR_RESET
    );

    println!(
        "{}--> Executing initial compiler check...{}",
        CLR_DIM, CLR_RESET
    );
    let init_res = execute_mode_action(WorkflowMode::Dev, "fast", workspace_root);
    render_execution_result(&init_res);

    let mut last_mtimes = collect_source_mtimes(workspace_root);
    let mut check_count = 0usize;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n{}Live-reload terminated by user (Ctrl+C).{}", CLR_YELLOW, CLR_RESET);
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }

        let current_mtimes = collect_source_mtimes(workspace_root);
        let mut changed_file = None;

        for (path, mtime) in &current_mtimes {
            if let Some(prev) = last_mtimes.get(path) {
                if mtime > prev {
                    changed_file = Some(path.clone());
                    break;
                }
            } else {
                changed_file = Some(path.clone());
                break;
            }
        }

        if let Some(path) = changed_file {
            check_count += 1;
            last_mtimes = current_mtimes;
            let rel_path = path.strip_prefix(workspace_root).unwrap_or(&path);
            println!(
                "\n{}[LIVE-RELOAD #{}] Change detected in: {}{}",
                CLR_YELLOW,
                check_count,
                rel_path.display(),
                CLR_RESET
            );
            println!("Triggering incremental compilation check...");
            let check_res = execute_mode_action(WorkflowMode::Dev, "fast", workspace_root);
            render_execution_result(&check_res);
        }
    }

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = true;
    res.exit_code = Some(0);
    res.stdout = format!(
        "Live-reload watch session completed after {} change trigger(s).",
        check_count
    );
    res = res.with_metric("reloads_count", check_count);
    res
}

fn collect_source_mtimes(workspace_root: &Path) -> HashMap<PathBuf, std::time::SystemTime> {
    let mut mtimes = HashMap::new();
    let scan_dirs = ["crates", "tests", "src"];
    for dir in &scan_dirs {
        scan_dir_mtimes(&workspace_root.join(dir), &mut mtimes);
    }
    let toml = workspace_root.join("Cargo.toml");
    if let Ok(meta) = fs::metadata(&toml) {
        if let Ok(mod_time) = meta.modified() {
            mtimes.insert(toml, mod_time);
        }
    }
    mtimes
}

fn scan_dir_mtimes(dir: &Path, mtimes: &mut HashMap<PathBuf, std::time::SystemTime>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name == "target" || name.starts_with('.') {
                        continue;
                    }
                }
                scan_dir_mtimes(&path, mtimes);
            } else if path.is_file() {
                if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                    if ext == "rs" || ext == "toml" {
                        if let Ok(meta) = entry.metadata() {
                            if let Ok(mtime) = meta.modified() {
                                mtimes.insert(path, mtime);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Formats and renders execution result to stdout with ANSI decorations.
pub fn render_execution_result(res: &ModeExecutionResult) {
    let status_str = if res.success {
        format!("{}[OK - SUCCESS]{}", CLR_GREEN, CLR_RESET)
    } else {
        format!("{}[FAILED]{}", CLR_RED, CLR_RESET)
    };

    println!();
    println!(
        "{}────────────────────────────────────────────────────────────────────────────────{}",
        CLR_DIM, CLR_RESET
    );
    println!(
        "{}Mode:{} {:<12} {}Action:{} {:<12} {}Status:{} {} ({} ms)",
        CLR_BOLD,
        CLR_RESET,
        res.mode.as_str().to_uppercase(),
        CLR_BOLD,
        CLR_RESET,
        res.action,
        CLR_BOLD,
        CLR_RESET,
        status_str,
        res.duration_ms
    );
    println!(
        "{}────────────────────────────────────────────────────────────────────────────────{}",
        CLR_DIM, CLR_RESET
    );

    if !res.stdout.trim().is_empty() {
        println!("{}", res.stdout.trim());
    }

    if !res.stderr.trim().is_empty() {
        eprintln!("{}Error / Diagnostic Output:{}", CLR_YELLOW, CLR_RESET);
        eprintln!("{}", res.stderr.trim());
    }

    println!(
        "{}────────────────────────────────────────────────────────────────────────────────{}",
        CLR_DIM, CLR_RESET
    );
    println!();
}

/// Runs interactive terminal menu loop.
pub async fn run_interactive_menu(workspace_root: &Path) -> i32 {
    loop {
        println!();
        println!(
            "{}{}================================================================================",
            CLR_BOLD, CLR_CYAN
        );
        println!("       🚀 OXIDESWARM WORKFLOW MODES & PRESETS CONTROL CONSOLE (ox-mode)");
        println!(
            "================================================================================{}",
            CLR_RESET
        );
        println!("  Select a workflow mode:");
        println!(
            "    {}1.{} {}🧪 Test & QA Mode{}         (Unit tests, Clippy lints, Flaky detector)",
            CLR_BOLD, CLR_RESET, CLR_CYAN, CLR_RESET
        );
        println!(
            "    {}2.{} {}⚡ Development Mode{}       (Fast check, Local cluster dev, Health probe)",
            CLR_BOLD, CLR_RESET, CLR_YELLOW, CLR_RESET
        );
        println!(
            "    {}3.{} {}📚 Documentation Mode{}     (Cargo doc build, Spec verify, Export summary)",
            CLR_BOLD, CLR_RESET, CLR_MAGENTA, CLR_RESET
        );
        println!(
            "    {}4.{} {}🔬 Research & Bench Mode{}  (Hardware profile, Micro-benchmarks, Synthetic sim)",
            CLR_BOLD, CLR_RESET, CLR_GREEN, CLR_RESET
        );
        println!("    {}0.{} Exit", CLR_BOLD, CLR_RESET);
        println!(
            "{}--------------------------------------------------------------------------------{}",
            CLR_DIM, CLR_RESET
        );
        print!("Enter your choice [0-4]: ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            return 0;
        }

        let choice = input.trim();
        match choice {
            "1" => {
                run_test_menu(workspace_root).await;
            }
            "2" => {
                run_dev_menu(workspace_root).await;
            }
            "3" => {
                run_doc_menu(workspace_root).await;
            }
            "4" => {
                run_research_menu(workspace_root).await;
            }
            "0" | "q" | "exit" => {
                println!("Goodbye!");
                return 0;
            }
            _ => {
                println!(
                    "{}Invalid selection. Please choose from 0 to 4.{}",
                    CLR_YELLOW, CLR_RESET
                );
            }
        }
    }
}

async fn run_test_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== 🧪 TEST & QUALITY ASSURANCE MODE ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Quick Unit Tests (cargo test --workspace --lib)");
    println!("  [2] Lint Checks & Formatting (cargo clippy & fmt)");
    println!("  [3] Flaky Test Detector (5 iterations test loop)");
    println!("  [4] Full Integration & E2E Test Suite (cargo test --workspace)");
    println!("  [0] Back to Main Menu");
    print!("Select [0-4]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            println!("Running Quick Unit Tests...");
            let res = execute_single_action(WorkflowMode::Test, "quick", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            println!("Running Lint & Format checks...");
            let res = execute_single_action(WorkflowMode::Test, "lint", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            println!("Running Flaky Test Detector (5 iterations)...");
            let res = execute_single_action(WorkflowMode::Test, "flaky", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "4" => {
            println!("Running Full Test Suite...");
            let res = execute_single_action(WorkflowMode::Test, "full", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}

async fn run_dev_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== ⚡ DEVELOPMENT & LOCAL CLUSTER MODE ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Fast Incremental Check (cargo check --workspace)");
    println!("  [2] Live-Reload Watch Mode (auto re-check on file changes)");
    println!("  [3] Start Local Dev Cluster (Master + 2 Workers)");
    println!("  [4] Check Cluster Health Status");
    println!("  [5] Stop Local Dev Cluster");
    println!("  [0] Back to Main Menu");
    print!("Select [0-5]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            let res = execute_single_action(WorkflowMode::Dev, "fast", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            let res = execute_single_action(WorkflowMode::Dev, "watch", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            let res = execute_single_action(WorkflowMode::Dev, "cluster", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "4" => {
            let res = execute_single_action(WorkflowMode::Dev, "status", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "5" => {
            let res = execute_single_action(WorkflowMode::Dev, "stop", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}

async fn run_doc_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== 📚 DOCUMENTATION & ARCHITECTURE SPECIFICATION MODE ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Verify Markdown Documentation & Relative Links");
    println!("  [2] Export Workspace & Wire Specification Summary");
    println!("  [3] Build Source Documentation (cargo doc --workspace --no-deps)");
    println!("  [0] Back to Main Menu");
    print!("Select [0-3]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            let res = execute_single_action(WorkflowMode::Doc, "verify", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            let res = execute_single_action(WorkflowMode::Doc, "export", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            let res = execute_single_action(WorkflowMode::Doc, "build", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}

async fn run_research_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== 🔬 RESEARCH & PERFORMANCE BENCHMARKING MODE ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Hardware Resource Profiler (CPU/RAM)");
    println!("  [2] Micro-Benchmarks (Task JSON Serde & Bincode Wire Codec)");
    println!("  [3] Synthetic Scheduling Simulation Benchmark");
    println!("  [4] Export Complete Research JSON Report");
    println!("  [0] Back to Main Menu");
    print!("Select [0-4]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            let res =
                execute_single_action(WorkflowMode::Research, "profile", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            let res =
                execute_single_action(WorkflowMode::Research, "bench", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            let res =
                execute_single_action(WorkflowMode::Research, "distributed", 5, workspace_root)
                    .await;
            render_execution_result(&res);
        }
        "4" => {
            let res =
                execute_single_action(WorkflowMode::Research, "report", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_profiling_execution() {
        let profile = run_hardware_profile();
        assert!(profile.success);
        assert!(profile.metrics.contains_key("cpu_cores"));
        assert!(profile.metrics.contains_key("ram_total_mb"));
        assert!(profile.stdout.contains("OxideSwarm Hardware"));
    }

    #[test]
    fn test_micro_benchmarks_execution() {
        let bench = run_micro_benchmarks();
        assert!(bench.success);
        assert!(bench.metrics.contains_key("serde_ops_per_sec"));
        assert!(bench.metrics.contains_key("codec_ops_per_sec"));
        assert_eq!(
            bench.metrics.get("codec_wire_discriminator"),
            Some(&serde_json::json!(0x02))
        );
        let decoded_id = bench
            .metrics
            .get("codec_decoded_task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let expected_id = bench
            .metrics
            .get("expected_task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("mismatch");
        assert!(!decoded_id.is_empty());
        assert_eq!(decoded_id, expected_id);
        assert!(
            bench
                .metrics
                .get("codec_frame_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                > 20
        );
        assert!(bench.stdout.contains("Bincode Wire Codec Roundtrip"));
    }

    #[test]
    fn test_distributed_simulation_is_honestly_labeled() {
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let dist = run_distributed_benchmark(&workspace_root);
        assert!(dist.success);
        assert_eq!(
            dist.metrics.get("simulation_only"),
            Some(&serde_json::json!(true))
        );
        let lower = dist.stdout.to_lowercase();
        assert!(!lower.contains("optimal"));
        assert!(!lower.contains("rtx 4090"));
        assert!(!lower.contains("rtx4090"));
        assert!(!lower.contains("worker-gpu-rtx4090"));
        assert!(!lower.contains("snapdragon"));

        let allocs = dist
            .metrics
            .get("worker_allocations")
            .and_then(|v| v.as_object())
            .expect("worker_allocations object");
        let allowed = [
            "sim-cpu-node-a",
            "sim-cpu-node-b",
            "sim-gpu-node",
            "sim-mobile-node",
        ];
        for key in allocs.keys() {
            assert!(
                allowed.contains(&key.as_str()),
                "unexpected synthetic worker name '{key}'"
            );
        }
    }

    #[test]
    fn test_parse_cargo_test_counts_two_passed_one_failed() {
        let fixture = "\
running 3 tests
test tests::test_alpha ... ok
test tests::test_beta ... ok
test tests::test_gamma ... FAILED

failures:

---- tests::test_gamma stdout ----
thread 'tests::test_gamma' panicked at 'assertion failed'

failures:
    tests::test_gamma

test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
";
        let (passed, failed) = parse_cargo_test_counts(fixture);
        assert_eq!(passed, 2);
        assert_eq!(failed, 1);

        let multi_crate_fixture = format!(
            "running 5 tests\ntest result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n{fixture}"
        );
        assert_eq!(parse_cargo_test_counts(&multi_crate_fixture), (7, 1));
    }

    #[test]
    fn test_lint_and_flaky_fail_when_only_part_passes() {
        let clippy_only = evaluate_lint_outcomes(true, "ok", "", false, "Needs formatting", "", 12);
        assert!(
            !clippy_only.success,
            "lint must fail when fmt fails even if clippy passed"
        );
        assert_eq!(clippy_only.exit_code, Some(1));

        let fmt_only = evaluate_lint_outcomes(false, "", "clippy warning", true, "", "", 12);
        assert!(
            !fmt_only.success,
            "lint must fail when clippy fails even if fmt passed"
        );
        assert_eq!(fmt_only.exit_code, Some(1));

        let both_ok = evaluate_lint_outcomes(true, "", "", true, "", "", 12);
        assert!(both_ok.success);
        assert_eq!(both_ok.exit_code, Some(0));

        let partial_flaky = run_flaky_detection_with(5, |iter| (iter != 3, 5));
        assert!(
            !partial_flaky.success,
            "flaky detection must fail when only 4 of 5 iterations pass"
        );
        assert_eq!(partial_flaky.exit_code, Some(1));
        assert_eq!(
            partial_flaky.metrics.get("passed"),
            Some(&serde_json::json!(4))
        );

        let all_flaky_pass = run_flaky_detection_with(5, |_| (true, 5));
        assert!(all_flaky_pass.success);
        assert_eq!(all_flaky_pass.exit_code, Some(0));
    }

    #[test]
    fn test_research_report_and_doc_build_fail_on_missing_or_unwritable_output() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("target"), b"not a directory").expect("write target file");

        let report_res = generate_research_report(temp.path());
        assert!(
            !report_res.success,
            "generate_research_report must fail when target is a regular file"
        );
        assert_eq!(report_res.exit_code, Some(1));
        assert!(
            !report_res.stdout.contains("Saved Path:"),
            "failed report must not claim Saved Path"
        );
        assert!(report_res
            .stderr
            .contains("Failed to write research report"));

        let mut doc_res = ModeExecutionResult::new(WorkflowMode::Doc, "build");
        doc_res.success = true;
        doc_res.exit_code = Some(0);
        finalize_doc_build_output_with_target_dir(
            &temp.path().join("missing_target"),
            &mut doc_res,
        );
        assert!(
            !doc_res.success,
            "finalize_doc_build_output must fail when index.html was not produced"
        );
        assert_eq!(doc_res.exit_code, Some(1));

        let custom_target = temp.path().join("custom_cargo_target");
        let doc_dir = custom_target.join("doc/rusty_grid_core");
        std::fs::create_dir_all(&doc_dir).expect("create doc dir");
        std::fs::write(doc_dir.join("index.html"), "<html></html>").expect("write index.html");
        let mut custom_doc_res = ModeExecutionResult::new(WorkflowMode::Doc, "build");
        custom_doc_res.success = true;
        custom_doc_res.exit_code = Some(0);
        finalize_doc_build_output_with_target_dir(&custom_target, &mut custom_doc_res);
        assert!(
            custom_doc_res.success,
            "finalize_doc_build_output must succeed when index.html exists in custom CARGO_TARGET_DIR"
        );
        assert_eq!(custom_doc_res.exit_code, Some(0));
    }
}
