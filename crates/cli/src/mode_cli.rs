//! Command-line interface, interactive menu, and execution engine for OxideSwarm Workflow Modes (`ox-mode`).

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use clap::Parser;
use rusty_grid_core::mode::chrono::Utc;
use rusty_grid_core::mode::sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use serde::{Deserialize, Serialize};

use rusty_grid_core::capabilities::{MobileCapabilities, WorkerCapabilities};
pub use rusty_grid_core::mode::{
    find_workspace_root, get_mode_descriptors, ModeActionDescriptor, ModeDescriptor,
    ModeExecutionResult, WorkflowMode,
};
use rusty_grid_core::protocol::{
    deserialize_message, serialize_message, MasterMessage, ProtocolCapabilities, WireCodec,
    MAX_FRAME_SIZE, PROTOCOL_VERSION_CURRENT, PROTOCOL_VERSION_MIN_COMPATIBLE,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};

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

            let mut passed_tests = 0;
            let mut failed_tests = 0;
            for line in res.stdout.lines() {
                if line.contains("test result: ok.") {
                    if let Some(passed) = extract_count(line, "passed;") {
                        passed_tests += passed;
                    }
                } else if line.contains("FAILED") {
                    failed_tests += 1;
                }
            }
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

/// Runs clippy lint checks and rustfmt verification.
pub fn run_lints(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "lint");

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

    res.duration_ms = start.elapsed().as_millis() as u64;

    let clippy_ok = clippy_out
        .as_ref()
        .map(|o| o.status.success())
        .unwrap_or(false);
    let fmt_ok = fmt_out
        .as_ref()
        .map(|o| o.status.success())
        .unwrap_or(false);

    res.success = clippy_ok && fmt_ok;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let mut combined_stdout = String::new();
    let mut combined_stderr = String::new();

    combined_stdout.push_str("=== 1. Cargo Clippy Lints ===\n");
    if let Ok(o) = clippy_out {
        combined_stdout.push_str(&String::from_utf8_lossy(&o.stdout));
        combined_stderr.push_str(&String::from_utf8_lossy(&o.stderr));
    }
    combined_stdout.push_str("\n=== 2. Rustfmt Code Style ===\n");
    if let Ok(o) = fmt_out {
        if o.status.success() {
            combined_stdout.push_str("All source files conform to standard Rust formatting.\n");
        } else {
            combined_stderr.push_str(&String::from_utf8_lossy(&o.stderr));
            combined_stderr.push_str(&String::from_utf8_lossy(&o.stdout));
        }
    }

    res.stdout = combined_stdout;
    res.stderr = combined_stderr;
    res = res.with_metric("clippy_passed", clippy_ok);
    res = res.with_metric("fmt_passed", fmt_ok);

    res
}

/// Runs test iterations to detect flaky non-deterministic tests.
pub fn run_flaky_detection(workspace_root: &Path, iterations: usize) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "flaky");
    let iters = if iterations == 0 { 5 } else { iterations };

    let mut iteration_results = Vec::new();
    let mut pass_count = 0;
    let mut total_duration_ms = 0u64;

    for i in 1..=iters {
        let iter_start = Instant::now();
        let cmd = Command::new("cargo")
            .args(["test", "--workspace", "--lib", "--quiet"])
            .current_dir(workspace_root)
            .output();

        let elapsed = iter_start.elapsed().as_millis() as u64;
        total_duration_ms += elapsed;

        let ok = cmd.as_ref().map(|o| o.status.success()).unwrap_or(false);
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

/// Performs a synchronous HTTP/1.1 GET against `127.0.0.1:<port>/api/status`,
/// returning `(http_status_code, response_body)`.
fn http_get_local_status(port: u16) -> Result<(u16, String), String> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(750))
        .map_err(|e| format!("connect 127.0.0.1:{port}: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));

    let req = format!(
        "GET /api/status HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
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
    Ok((status_code, body.to_string()))
}

/// Probes the health and status of a running local master node across candidate ports.
pub fn probe_cluster_health(port: u16) -> ModeExecutionResult {
    probe_cluster_health_with_root(port, &find_workspace_root())
}

/// Probes the health and status of a running local master node with a specific workspace root.
pub fn probe_cluster_health_with_root(port: u16, workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "status");

    let mut candidate_ports = Vec::new();
    let explicit_nondefault_port = port != 8080;
    if explicit_nondefault_port {
        candidate_ports.push(port);
    }

    let cluster_file = workspace_root.join(".dev_cluster.json");
    let mut configured_port = if explicit_nondefault_port {
        Some(port)
    } else {
        None
    };
    if let Ok(content) = fs::read_to_string(&cluster_file) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&content) {
            if let Some(p) = v.get("web_ui_port").and_then(|p| p.as_u64()) {
                let p_u16 = p as u16;
                if configured_port.is_none() {
                    configured_port = Some(p_u16);
                }
                if !candidate_ports.contains(&p_u16) {
                    candidate_ports.push(p_u16);
                }
            }
        }
    }
    if !candidate_ports.contains(&port) {
        candidate_ports.push(port);
    }
    for fallback in [8088, 8080, 3000] {
        if !candidate_ports.contains(&fallback) {
            candidate_ports.push(fallback);
        }
    }

    let mut http_error_seen: Option<(String, u16, String)> = None;

    for &p in &candidate_ports {
        let url = format!("http://127.0.0.1:{}/api/status", p);
        match http_get_local_status(p) {
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
            if res.success {
                let doc_index = workspace_root.join("target/doc/rusty_grid_core/index.html");
                res.stdout.push_str(&format!(
                    "\nDocumentation built successfully!\nHTML Index: {}\n",
                    doc_index.display()
                ));
            }
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
    let wire_msg = MasterMessage::AssignTask { task: dummy_task };
    let t2_start = Instant::now();
    let mut wire_bytes_total = 0usize;
    for _ in 0..codec_iters {
        let encoded =
            serialize_message(&wire_msg, WireCodec::Bincode).expect("Bincode wire serialize");
        wire_bytes_total += encoded.len();
        let (_decoded, _codec): (MasterMessage, WireCodec) =
            deserialize_message(&encoded).expect("Bincode wire deserialize");
    }
    let t2_dur = t2_start.elapsed();
    let t2_secs = t2_dur.as_secs_f64().max(1e-9);
    let codec_ops_per_sec = (codec_iters as f64 / t2_secs) as u64;
    let codec_mb_per_sec = (wire_bytes_total as f64 / (1024.0 * 1024.0)) / t2_secs;

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = true;
    res.exit_code = Some(0);

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
    if let Some(parent) = report_file.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(json_str) = serde_json::to_string_pretty(&report_data) {
        let _ = fs::write(&report_file, json_str);
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
        (WorkflowMode::Dev, "status") => probe_cluster_health_with_root(8080, workspace_root),
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
    rusty_grid_core::mode::set_mode_runner_hook(execute_mode_action);
    let workspace_root = find_workspace_root();

    if args.mode.is_none() || args.interactive {
        return run_interactive_menu(&workspace_root).await;
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
            probe_cluster_health_with_root(args.port.unwrap_or(8080), &workspace_root)
        } else {
            execute_single_action(parsed_mode, &action_str, args.iterations, &workspace_root).await
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

/// Spawns local dev cluster: 1 Master with Web UI + 2 Workers in background with PID tracking.
pub async fn launch_local_dev_cluster(workspace_root: &Path) -> ModeExecutionResult {
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "cluster");
    let start = Instant::now();

    let pid_file = workspace_root.join(".dev_cluster.pids");
    let json_file = workspace_root.join(".dev_cluster.json");
    if pid_file.exists() || json_file.exists() {
        let _ = stop_local_dev_cluster(workspace_root).await;
    }

    let bin_rel = if cfg!(windows) {
        "target/debug/rusty-grid.exe"
    } else {
        "target/debug/rusty-grid"
    };
    let target_bin = workspace_root.join(bin_rel);
    if !target_bin.exists() {
        match Command::new("cargo")
            .args(["build", "--bin", "rusty-grid"])
            .current_dir(workspace_root)
            .output()
        {
            Ok(out) if out.status.success() => {}
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

    let master_tcp_port = 9090u16;
    let web_ui_port = 8088u16;

    let mut spawned_pids = Vec::new();

    let master_res = Command::new(&target_bin)
        .args([
            "master",
            "--listen",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--web-ui-addr",
            &format!("127.0.0.1:{}", web_ui_port),
        ])
        .current_dir(workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    let master_pid = match master_res {
        Ok(child) => {
            let pid = child.id();
            spawned_pids.push(format!("master:{}", pid));
            pid
        }
        Err(e) => {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to spawn Master: {}", e);
            return res;
        }
    };

    tokio::time::sleep(Duration::from_millis(1000)).await;

    let w1_res = Command::new(&target_bin)
        .args([
            "worker",
            "--master",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--name",
            "dev-worker-cpu-1",
            "--cores",
            "4",
            "--ram",
            "8192",
        ])
        .current_dir(workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    if let Ok(child) = w1_res {
        spawned_pids.push(format!("worker_cpu:{}", child.id()));
    }

    let w2_res = Command::new(&target_bin)
        .args([
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
        ])
        .current_dir(workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    if let Ok(child) = w2_res {
        spawned_pids.push(format!("worker_gpu:{}", child.id()));
    }

    let _ = fs::write(&pid_file, spawned_pids.join("\n"));
    let cluster_json = serde_json::json!({
        "master_pid": master_pid,
        "master_tcp": format!("127.0.0.1:{}", master_tcp_port),
        "web_ui_port": web_ui_port,
        "web_ui_url": format!("http://127.0.0.1:{}", web_ui_port),
        "pids": spawned_pids,
    });
    if let Ok(json_str) = serde_json::to_string_pretty(&cluster_json) {
        let _ = fs::write(&json_file, json_str);
    }

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

/// Stops running dev cluster child processes cleanly with cross-platform termination.
pub async fn stop_local_dev_cluster(workspace_root: &Path) -> ModeExecutionResult {
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "stop");
    let start = Instant::now();

    let pid_file = workspace_root.join(".dev_cluster.pids");
    let json_file = workspace_root.join(".dev_cluster.json");

    if !pid_file.exists() && !json_file.exists() {
        res.success = true;
        res.exit_code = Some(0);
        res.stdout = "No running dev cluster tracking files found.".to_string();
        return res;
    }

    let mut pids_to_kill = Vec::new();

    if let Ok(content) = fs::read_to_string(&pid_file) {
        for line in content.lines() {
            if let Some((_role, pid_str)) = line.split_once(':') {
                if let Ok(pid) = pid_str.trim().parse::<u32>() {
                    pids_to_kill.push(pid);
                }
            }
        }
    }

    if let Ok(content) = fs::read_to_string(&json_file) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&content) {
            if let Some(master_pid) = val.get("master_pid").and_then(|p| p.as_u64()) {
                pids_to_kill.push(master_pid as u32);
            }
            if let Some(arr) = val.get("pids").and_then(|p| p.as_array()) {
                for item in arr {
                    if let Some(line) = item.as_str() {
                        if let Some((_, pid_str)) = line.split_once(':') {
                            if let Ok(pid) = pid_str.trim().parse::<u32>() {
                                pids_to_kill.push(pid);
                            }
                        }
                    }
                }
            }
        }
    }

    pids_to_kill.sort();
    pids_to_kill.dedup();

    let mut stopped = 0;
    for pid in pids_to_kill {
        if terminate_process(pid) {
            stopped += 1;
        }
    }

    let _ = fs::remove_file(&pid_file);
    let _ = fs::remove_file(&json_file);

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = true;
    res.exit_code = Some(0);
    res.stdout = format!("Stopped {} dev cluster process(es) cleanly.", stopped);
    res = res.with_metric("stopped_count", stopped);

    res
}

fn terminate_process(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let out = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .output();
        out.map(|o| o.status.success()).unwrap_or(false)
    }
    #[cfg(windows)]
    {
        let out = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/F"])
            .output();
        out.map(|o| o.status.success()).unwrap_or(false)
    }
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
        assert!(!dist.stdout.contains("OPTIMAL"));
        assert!(!dist.stdout.contains("RTX 4090"));
        assert!(!dist.stdout.contains("Snapdragon"));
    }
}
