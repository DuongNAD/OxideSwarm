//! `ox-mode dev` workflow engine, local dev cluster management, and interactive menu, split out
//! of `mode_cli.rs` (design 0080, MAINT-2).

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use sysinfo::System;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::mode::{find_workspace_root, ModeExecutionResult, WorkflowMode};

use crate::config::{load_config_file, SecretSection};
use crate::secret_cmd::SecretOpts;

#[cfg(windows)]
use super::dev_launch::clear_current_process_stdio_inherit_flags;
use super::dev_launch::{
    cleanup_failed_cluster_launch, master_log_path, master_log_stdio, with_master_log_tail,
};
use super::doc::resolve_cargo_target_dir;
use super::{
    execute_mode_action, execute_single_action, render_execution_result, CLR_BOLD, CLR_CYAN,
    CLR_DIM, CLR_RESET, CLR_YELLOW,
};

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
pub(super) fn resolve_dashboard_bearer_token() -> Option<String> {
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

pub(super) fn probe_cluster_health_with_bearer(
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

fn reap_spawned_cluster_child_after_exit(pid: u32) {
    if let Ok(mut guard) = SPAWNED_CLUSTER_CHILDREN.lock() {
        guard.retain_mut(|child| {
            if child.id() == pid {
                if matches!(child.try_wait(), Ok(None)) {
                    let _ = child.wait();
                }
                false
            } else {
                true
            }
        });
    }
}

fn reap_all_exited_cluster_children() {
    if let Ok(mut guard) = SPAWNED_CLUSTER_CHILDREN.lock() {
        guard.retain_mut(|child| matches!(child.try_wait(), Ok(None)));
    }
}

fn query_process_start_time(pid: u32) -> Option<u64> {
    let mut sys = System::new();
    let sys_pid = sysinfo::Pid::from_u32(pid);
    if sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[sys_pid]), true) > 0 {
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

fn read_bound_port(path: &Path, requested: u16) -> Option<u16> {
    let port = fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .filter(|&p| p > 0)?;
    if requested == 0 || port == requested {
        Some(port)
    } else {
        None
    }
}

/// Spawns local dev cluster: 1 Master with Web UI + 2 Workers in background on OS-picked loopback
/// ports (`127.0.0.1:0`) with PID tracking and port-file readiness checks before workers start.
pub async fn launch_local_dev_cluster(workspace_root: &Path) -> ModeExecutionResult {
    launch_local_dev_cluster_with_ports(workspace_root, 0, 0).await
}

/// Spawns local dev cluster on the specified `master_tcp_port` and `web_ui_port` (either or both
/// may be `0` to let the master pick ephemeral loopback ports and report them via port files),
/// waiting for both port files before spawning workers and polling `/api/status` until both register.
pub async fn launch_local_dev_cluster_with_ports(
    workspace_root: &Path,
    master_tcp_port: u16,
    web_ui_port: u16,
) -> ModeExecutionResult {
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "cluster");
    let start = Instant::now();
    if master_tcp_port != 0 && master_tcp_port == web_ui_port {
        // TEST-43: the master would take the port for its grid listener, fail to bind its dashboard
        // there, keep running without it, and the wait below would end after 8 s with no reason.
        res.success = false;
        res.exit_code = Some(1);
        res.stderr = format!(
            "The master port and the dashboard port are both {web_ui_port}; they must differ"
        );
        return res;
    }

    let pid_file = workspace_root.join(".dev_cluster.pids");
    let json_file = workspace_root.join(".dev_cluster.json");
    let master_port_file = workspace_root.join(".dev_cluster.master_port");
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
    let _ = fs::remove_file(&master_port_file);
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
        if let Ok(exe) = std::env::current_exe() {
            for ancestor in exe.ancestors().skip(1).take(2) {
                let cand = ancestor.join(bin_name);
                if cand.exists() {
                    target_bin = cand;
                    break;
                }
            }
        }
    }
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

    #[cfg(windows)]
    clear_current_process_stdio_inherit_flags();

    let mut master_cmd = Command::new(&target_bin);
    if let Some(ref home_dir) = isolated_home {
        let cfg_path = home_dir.join("rusty-grid.toml");
        master_cmd
            .env("HOME", home_dir)
            .env("USERPROFILE", home_dir)
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("OXIDESWARM_SECRET_FILE")
            .env_remove("RUSTY_GRID_CONFIG")
            .arg("--config")
            .arg(&cfg_path);
    }
    master_cmd.args([
        "master",
        "--listen",
        &format!("127.0.0.1:{}", master_tcp_port),
        "--port-file",
    ]);
    master_cmd.arg(&master_port_file);
    master_cmd.args([
        "--web-ui-addr",
        &format!("127.0.0.1:{}", web_ui_port),
        "--dashboard-port-file",
    ]);
    master_cmd.arg(&dash_port_file);
    master_cmd.args(["--ephemeral-key", "--discovery-port", "0"]); // BUG-26
    if let Some(ref home_dir) = isolated_home {
        master_cmd.arg("--blob-dir").arg(home_dir.join("blobs"));
    }
    let master_log = master_log_path(workspace_root);
    let (master_out, master_err) = master_log_stdio(&master_log);
    let master_res = master_cmd
        .current_dir(workspace_root)
        .stdin(Stdio::null())
        .stdout(master_out)
        .stderr(master_err)
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
                &master_port_file,
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
            &master_port_file,
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

    // Wait for both `--port-file` and `--dashboard-port-file` before spawning workers so port `0`
    // resolves to real OS-picked loopback ports without bind-and-drop (TEST-52).
    let port_deadline = Instant::now() + Duration::from_secs(8);
    let mut resolved_ports: Option<(u16, u16)> = None;
    while Instant::now() < port_deadline {
        match master_child.try_wait() {
            Ok(Some(status)) => {
                cleanup_failed_cluster_launch(
                    &mut [&mut master_child],
                    &pid_file,
                    &json_file,
                    &master_port_file,
                    &dash_port_file,
                    isolated_home.as_deref(),
                );
                res.duration_ms = start.elapsed().as_millis() as u64;
                res.success = false;
                res.exit_code = Some(1);
                let exit_msg = match (master_tcp_port, web_ui_port) {
                    (0, 0) => format!(
                        "Master process exited immediately with {status} before writing its port files; see the log below"
                    ),
                    (0, wp) => format!(
                        "Master process exited immediately with {status} before writing its port files (port 127.0.0.1:{wp} may already be in use; see the log below)"
                    ),
                    (mp, 0) => format!(
                        "Master process exited immediately with {status} before writing its port files (port 127.0.0.1:{mp} may already be in use; see the log below)"
                    ),
                    (mp, wp) => format!(
                        "Master process exited immediately with {status} (port 127.0.0.1:{mp} or 127.0.0.1:{wp} may already be in use)"
                    ),
                };
                res.stderr = with_master_log_tail(exit_msg, &master_log);
                return res;
            }
            Ok(None) => {}
            Err(e) => {
                cleanup_failed_cluster_launch(
                    &mut [&mut master_child],
                    &pid_file,
                    &json_file,
                    &master_port_file,
                    &dash_port_file,
                    isolated_home.as_deref(),
                );
                res.duration_ms = start.elapsed().as_millis() as u64;
                res.success = false;
                res.exit_code = Some(1);
                res.stderr = with_master_log_tail(
                    format!("Failed to check status of Master: {e}"),
                    &master_log,
                );
                return res;
            }
        }
        if let (Some(mp), Some(wp)) = (
            read_bound_port(&master_port_file, master_tcp_port),
            read_bound_port(&dash_port_file, web_ui_port),
        ) {
            resolved_ports = Some((mp, wp));
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let (actual_master_port, actual_web_ui_port) = match resolved_ports {
        Some(ports) => ports,
        None => {
            let reason = if read_bound_port(&master_port_file, master_tcp_port).is_none() {
                format!(
                    "Master port file ({}) did not appear within 8s for 127.0.0.1:{master_tcp_port}",
                    master_port_file.display()
                )
            } else {
                format!(
                    "Dashboard port file ({}) did not appear within 8s for 127.0.0.1:{web_ui_port}",
                    dash_port_file.display()
                )
            };
            cleanup_failed_cluster_launch(
                &mut [&mut master_child],
                &pid_file,
                &json_file,
                &master_port_file,
                &dash_port_file,
                isolated_home.as_deref(),
            );
            res.duration_ms = start.elapsed().as_millis() as u64;
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = with_master_log_tail(reason, &master_log);
            return res;
        }
    };

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
            &format!("127.0.0.1:{}", actual_master_port),
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
            &format!("127.0.0.1:{}", actual_master_port),
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
        .stdin(Stdio::null())
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
                &master_port_file,
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
        actual_master_port,
        actual_web_ui_port,
        &spawned_pids,
        &pid_start_times,
    ) {
        cleanup_failed_cluster_launch(
            &mut [&mut master_child, &mut w1_child],
            &pid_file,
            &json_file,
            &master_port_file,
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
            &format!("127.0.0.1:{}", actual_master_port),
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
            &format!("127.0.0.1:{}", actual_master_port),
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
        .stdin(Stdio::null())
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
                &master_port_file,
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
        actual_master_port,
        actual_web_ui_port,
        &spawned_pids,
        &pid_start_times,
    ) {
        cleanup_failed_cluster_launch(
            &mut [&mut master_child, &mut w1_child, &mut w2_child],
            &pid_file,
            &json_file,
            &master_port_file,
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
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut cluster_ready = false;
    let failure_reason = format!(
        "Timed out waiting for master on http://127.0.0.1:{actual_web_ui_port}/api/status and both workers to register on 127.0.0.1:{actual_master_port}"
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
                &master_port_file,
                &dash_port_file,
                isolated_home.as_deref(),
            );
            res.duration_ms = start.elapsed().as_millis() as u64;
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = with_master_log_tail(reason, &master_log);
            return res;
        }

        if let Ok((status_code, body)) =
            http_get_local_status(actual_web_ui_port, bearer_token.as_deref())
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
            &master_port_file,
            &dash_port_file,
            isolated_home.as_deref(),
        );
        res.duration_ms = start.elapsed().as_millis() as u64;
        res.success = false;
        res.exit_code = Some(1);
        res.stderr = with_master_log_tail(failure_reason, &master_log);
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
        actual_master_port,
        actual_web_ui_port,
        &spawned_pids,
        &pid_start_times,
    );
    register_spawned_cluster_children(vec![master_child, w1_child, w2_child]);

    let _ = fs::remove_file(&master_port_file);
    let _ = fs::remove_file(&dash_port_file);

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = true;
    res.exit_code = Some(0);
    res.stdout = format!(
        "=== Local Development Cluster Launched ===\nMaster Coordinator : tcp://127.0.0.1:{} (PID: {})\nWeb UI Dashboard   : http://127.0.0.1:{}\nWorker 1 (CPU Node) : dev-worker-cpu-1 (4 Cores, 8 GB RAM)\nWorker 2 (Sim GPU)  : dev-worker-sim-gpu-2 (8 Cores, 16 GB RAM, --simulate-gpu)\nPID tracking files : {}\n                     {}\n\nRun `ox-mode dev status` to inspect cluster health.\nRun `ox-mode dev stop` to shut down the cluster cleanly.\n",
        actual_master_port,
        master_pid,
        actual_web_ui_port,
        pid_file.display(),
        json_file.display()
    );

    res = res.with_metric("master_pid", master_pid);
    res = res.with_metric("master_tcp", format!("127.0.0.1:{}", actual_master_port));
    res = res.with_metric(
        "web_ui_url",
        format!("http://127.0.0.1:{}", actual_web_ui_port),
    );
    res = res.with_metric("web_ui_port", actual_web_ui_port);

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
    if matches_stem(&proc.name().to_string_lossy()) {
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

fn has_dev_cluster_cmdline_signature(proc: &sysinfo::Process, workspace_root: &Path) -> bool {
    let dash_port_path = workspace_root.join(".dev_cluster.dashboard_port");
    let home_path = workspace_root.join(".dev_cluster_home");
    let dash_str = dash_port_path.to_string_lossy();
    let home_str = home_path.to_string_lossy();
    let has_workspace_path = proc.cmd().iter().any(|arg| {
        let arg = arg.to_string_lossy();
        arg.contains(dash_str.as_ref()) || arg.contains(home_str.as_ref())
    }) || proc.cwd().map(|cwd| cwd == workspace_root).unwrap_or(false);
    let has_cluster_role = proc.cmd().iter().any(|arg| {
        let arg = arg.to_string_lossy();
        arg == "--dashboard-port-file" || arg == "dev-worker-cpu-1" || arg == "dev-worker-sim-gpu-2"
    });
    has_workspace_path && has_cluster_role
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

fn is_process_exited_or_zombie(sys: &mut System, pid: u32) -> bool {
    if try_reap_spawned_cluster_child(pid) {
        return true;
    }
    let sys_pid = sysinfo::Pid::from_u32(pid);
    #[cfg(windows)]
    {
        // On Windows, `sys` holds an `OpenProcess` handle from `refresh_processes()`. Calling
        // `sys.refresh_process(sys_pid)` on the existing `System` invokes `refresh_existing_process`
        // (`GetExitCodeProcess` != `STILL_ACTIVE`) and drops the handle once the process exits,
        // whereas a fresh `System::new().refresh_process(sys_pid)` would go through `new_from_pid`
        // (`OpenProcess` without checking the exit code) and report the dead process as still alive.
        if sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[sys_pid]), true) == 0 {
            reap_spawned_cluster_child_after_exit(pid);
            return true;
        }
    }
    #[cfg(unix)]
    {
        let _ = sys;
        // On Unix/macOS, construct a fresh `System` on each check because calling `refresh_process`
        // on an existing `System` in `sysinfo` 0.30 does not update `ProcessStatus` when a process
        // transitions to `Zombie`, and also check `ps -o stat=`.
        let mut fresh_sys = System::new();
        if fresh_sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[sys_pid]), true) == 0 {
            reap_spawned_cluster_child_after_exit(pid);
            return true;
        }
        if let Some(proc) = fresh_sys.process(sys_pid) {
            if matches!(proc.status(), sysinfo::ProcessStatus::Zombie) {
                reap_spawned_cluster_child_after_exit(pid);
                return true;
            }
        }
        if is_unix_process_zombie_or_gone(pid) {
            reap_spawned_cluster_child_after_exit(pid);
            return true;
        }
    }
    false
}

async fn wait_for_process_exit(sys: &mut System, pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if is_process_exited_or_zombie(sys, pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    is_process_exited_or_zombie(sys, pid)
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
    let master_port_file = workspace_root.join(".dev_cluster.master_port");
    let dash_port_file = workspace_root.join(".dev_cluster.dashboard_port");
    let isolated_home = workspace_root.join(".dev_cluster_home");

    if !pid_file.exists() && !json_file.exists() {
        let _ = fs::remove_file(&master_port_file);
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
    sys.refresh_processes(sysinfo::ProcessesToUpdate::All, true);

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

        // Verify ownership by matching recorded `sysinfo` start time at spawn (a start-time
        // mismatch always rejects the PID, even if its command line looks like a dev cluster),
        // or falling back to this workspace's command-line signature / tracking file timestamp
        // when no start time was recorded for the PID.
        let proc_start = proc.start_time();
        let has_cmdline = has_dev_cluster_cmdline_signature(proc, workspace_root);
        if let Some(&expected_start) = recorded_start_times.get(&pid) {
            if proc_start.abs_diff(expected_start) > 2 {
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
            if wait_for_process_exit(&mut sys, pid, Duration::from_secs(3)).await {
                exited += 1;
            }
        } else if is_process_exited_or_zombie(&mut sys, pid) {
            skipped += 1;
        } else {
            signal_failed += 1;
        }
    }

    reap_all_exited_cluster_children();

    let timed_out = signalled.saturating_sub(exited);
    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = timed_out == 0 && signal_failed == 0;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    // Only delete the tracking files when all verified processes exited cleanly;
    // keep them when any verified process could not be signalled or did not exit.
    if res.success {
        let _ = fs::remove_file(&pid_file);
        let _ = fs::remove_file(&json_file);
        let _ = fs::remove_file(&master_port_file);
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
pub(super) async fn run_live_reload_watch(workspace_root: &Path) -> ModeExecutionResult {
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

pub(super) async fn run_dev_menu(workspace_root: &Path) {
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

#[cfg(test)]
mod tests {
    use super::read_bound_port;

    #[test]
    fn test_read_bound_port_parsing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let port_file = dir.path().join("bound.port");

        // Missing file -> None
        assert_eq!(read_bound_port(&port_file, 0), None);

        // Empty and whitespace-only ("partial") -> None
        for raw in ["", "   \n"] {
            std::fs::write(&port_file, raw).expect("write empty/partial");
            assert_eq!(read_bound_port(&port_file, 0), None, "raw={raw:?}");
            assert_eq!(read_bound_port(&port_file, 8080), None, "raw={raw:?}");
        }

        // Zero -> None
        std::fs::write(&port_file, "0\n").expect("write zero");
        assert_eq!(read_bound_port(&port_file, 0), None);
        assert_eq!(read_bound_port(&port_file, 8080), None);

        // Non-numeric / out-of-range -> None
        for raw in ["abc\n", "123abc", "70000", "-1"] {
            std::fs::write(&port_file, raw).expect("write non-numeric");
            assert_eq!(read_bound_port(&port_file, 0), None, "raw={raw:?}");
        }

        // Valid port: accepted for requested == 0 or matching requested, rejected on mismatch
        std::fs::write(&port_file, " 45123 \n").expect("write valid port");
        assert_eq!(read_bound_port(&port_file, 0), Some(45123));
        assert_eq!(read_bound_port(&port_file, 45123), Some(45123));
        assert_eq!(read_bound_port(&port_file, 45124), None);
    }
}
