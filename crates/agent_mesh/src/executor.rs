//! Cross-Platform Command Execution Engine
//!
//! Provides asynchronous process spawning, stdout/stderr capture,
//! execution duration measurement, timeout safety watchdogs,
//! and native built-in commands (echo, system_info, ping, sha256_verify).

use rusty_grid_core::cluster_auth::{ENV_SECRET, ENV_SECRET_FILE};
use serde_json::Value;
use std::path::Path;
use std::time::{Duration, Instant};
use sysinfo::System;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tracing::{error, warn};

/// Most bytes kept from each of a command's stdout and stderr; the rest is drained and dropped so
/// the child never blocks on a full pipe. The cap keeps a `CommandResponse` (both streams, JSON
/// escaped - worst case six bytes per byte) below the hub's 16 MiB WebSocket message limit.
pub const MAX_CAPTURED_OUTPUT_BYTES: usize = 1024 * 1024;

/// How long to wait for the output readers to finish once a timed-out process tree is dead.
const READER_DRAIN_GRACE: Duration = Duration::from_secs(1);

/// Variables of the node's own environment that a command must never inherit: they hold the
/// cluster secret or the hub token (`--token` / `OXIDE_MESH_TOKEN`), and `shell_exec` runs
/// whatever a remote peer sends. A command can still be given one in its own `env`.
pub const SECRET_ENV_VARS: [&str; 3] = [ENV_SECRET, ENV_SECRET_FILE, "OXIDE_MESH_TOKEN"];

#[derive(Debug, Clone)]
pub struct CommandExecutionResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
    pub status: String,
}

pub struct CommandExecutor;

impl CommandExecutor {
    /// Dispatches a command request and returns structured execution results.
    pub async fn execute(
        command: &str,
        args: &Value,
        timeout_ms: Option<u64>,
    ) -> CommandExecutionResult {
        let start = Instant::now();
        let timeout_duration = Duration::from_millis(timeout_ms.unwrap_or(15000));

        // 1. Check for built-in lightweight commands first
        match command {
            "echo" => {
                let msg = if let Some(s) = args.as_str() {
                    s.to_string()
                } else if let Some(m) = args.get("message").and_then(|v| v.as_str()) {
                    m.to_string()
                } else if let Some(m) = args.get("msg").and_then(|v| v.as_str()) {
                    m.to_string()
                } else {
                    args.to_string()
                };

                let duration_ms = start.elapsed().as_millis() as u64;
                return CommandExecutionResult {
                    exit_code: 0,
                    stdout: msg,
                    stderr: String::new(),
                    duration_ms,
                    status: "success".to_string(),
                };
            }
            "ping" => {
                let duration_ms = start.elapsed().as_millis() as u64;
                return CommandExecutionResult {
                    exit_code: 0,
                    stdout: "pong".to_string(),
                    stderr: String::new(),
                    duration_ms,
                    status: "success".to_string(),
                };
            }
            "system_info" => {
                let mut sys = System::new_all();
                sys.refresh_all();

                let info = serde_json::json!({
                    "os_name": System::name().unwrap_or_else(|| std::env::consts::OS.to_string()),
                    "os_version": System::os_version().unwrap_or_default(),
                    "kernel_version": System::kernel_version().unwrap_or_default(),
                    "host_name": System::host_name().unwrap_or_default(),
                    "arch": std::env::consts::ARCH,
                    "cpus": sys.cpus().len(),
                    "total_memory_mb": sys.total_memory() / (1024 * 1024),
                    "free_memory_mb": sys.free_memory() / (1024 * 1024),
                });

                let duration_ms = start.elapsed().as_millis() as u64;
                return CommandExecutionResult {
                    exit_code: 0,
                    stdout: serde_json::to_string_pretty(&info).unwrap_or_default(),
                    stderr: String::new(),
                    duration_ms,
                    status: "success".to_string(),
                };
            }
            "sha256_verify" => {
                let data = args
                    .get("data")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let expected = args
                    .get("expected")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let actual = crate::protocol::compute_sha256(data.as_bytes());
                let matches = actual.eq_ignore_ascii_case(expected);
                let duration_ms = start.elapsed().as_millis() as u64;

                return CommandExecutionResult {
                    exit_code: if matches { 0 } else { 1 },
                    stdout: if matches {
                        "VERIFIED_MATCH".to_string()
                    } else {
                        "MISMATCH".to_string()
                    },
                    stderr: if matches {
                        String::new()
                    } else {
                        format!("Calculated {} != expected {}", actual, expected)
                    },
                    duration_ms,
                    status: if matches {
                        "success".to_string()
                    } else {
                        "failed".to_string()
                    },
                };
            }
            _ => {}
        }

        // 2. Cross-platform subprocess execution
        Self::execute_subprocess(command, args, timeout_duration).await
    }

    async fn execute_subprocess(
        command: &str,
        args: &Value,
        timeout_duration: Duration,
    ) -> CommandExecutionResult {
        let start = Instant::now();

        // Determine executable and argument list
        let mut cmd = if command == "shell_exec" {
            let cmd_str = if let Some(s) = args.get("cmd").and_then(|v| v.as_str()) {
                s.to_string()
            } else if let Some(s) = args.as_str() {
                s.to_string()
            } else if let Some(arr) = args.as_array() {
                arr.iter()
                    .map(|x| {
                        x.as_str()
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| x.to_string())
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            } else {
                args.to_string()
            };

            Self::create_shell_command(&cmd_str)
        } else {
            let mut c = Command::new(command);
            if let Some(arr) = args.as_array() {
                for item in arr {
                    if let Some(s) = item.as_str() {
                        c.arg(s);
                    } else {
                        c.arg(item.to_string());
                    }
                }
            } else if let Some(obj) = args.as_object() {
                if let Some(arr) = obj.get("args").and_then(|v| v.as_array()) {
                    for item in arr {
                        if let Some(s) = item.as_str() {
                            c.arg(s);
                        } else {
                            c.arg(item.to_string());
                        }
                    }
                }
            } else if let Some(s) = args.as_str() {
                c.arg(s);
            }
            c
        };

        // CRITICAL: Ensure Tokio terminates the direct child process upon drop/cancellation
        cmd.kill_on_drop(true);

        // Configure environment and working directory if specified
        if let Some(cwd) = args.get("cwd").and_then(|v| v.as_str()) {
            if Path::new(cwd).exists() {
                cmd.current_dir(cwd);
            }
        }

        // Removed before the command's own `env` is applied, so only what the peer passes
        // explicitly reaches the command.
        for name in SECRET_ENV_VARS {
            cmd.env_remove(name);
        }

        if let Some(env_map) = args.get("env").and_then(|v| v.as_object()) {
            for (k, v) in env_map {
                if let Some(s) = v.as_str() {
                    cmd.env(k, s);
                } else {
                    cmd.env(k, v.to_string());
                }
            }
        }

        // The command gets no stdin (EOF immediately) instead of inheriting the node's own.
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        // Unix: run the command as the leader of its own process group, so a timeout can take the
        // whole tree down with one `killpg` (grandchildren included) without ever naming a PID
        // that may already have been reaped and reused.
        #[cfg(unix)]
        cmd.process_group(0);

        // Spawn process
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                error!("Failed to spawn command '{}': {}", command, e);
                return CommandExecutionResult {
                    exit_code: 127, // Command not found / failed to spawn
                    stdout: String::new(),
                    stderr: format!("Failed to spawn process: {}", e),
                    duration_ms,
                    status: "failed".to_string(),
                };
            }
        };

        // The group id equals the leader's PID because of `process_group(0)`; remember it now,
        // while the PID is certainly ours.
        let process_group_id = child.id();

        // Bounded readers run as their own tasks so a timeout can still collect what they have.
        let mut stdout_reader = child
            .stdout
            .take()
            .map(|pipe| tokio::spawn(read_capped(pipe, MAX_CAPTURED_OUTPUT_BYTES)));
        let mut stderr_reader = child
            .stderr
            .take()
            .map(|pipe| tokio::spawn(read_capped(pipe, MAX_CAPTURED_OUTPUT_BYTES)));

        // Await the process and both streams with timeout safety watchdog. The streams end when
        // every holder of the pipes has exited, so a grandchild keeping a pipe open also times out.
        let finished = tokio::time::timeout(timeout_duration, async {
            let status = child.wait().await;
            let out = join_capture(stdout_reader.as_mut()).await;
            let err = join_capture(stderr_reader.as_mut()).await;
            (status, out, err)
        })
        .await;

        match finished {
            Ok((Ok(status), out, err)) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                let exit_code = status
                    .code()
                    .unwrap_or(if status.success() { 0 } else { 1 });
                let status_str = if status.success() {
                    "success"
                } else {
                    "failed"
                }
                .to_string();

                CommandExecutionResult {
                    exit_code,
                    stdout: render_capture(out),
                    stderr: render_capture(err),
                    duration_ms,
                    status: status_str,
                }
            }
            Ok((Err(e), out, err)) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                error!("I/O error waiting for command '{}': {}", command, e);
                let mut stderr = render_capture(err);
                if !stderr.is_empty() {
                    stderr.insert(0, '\n');
                }
                CommandExecutionResult {
                    exit_code: 1,
                    stdout: render_capture(out),
                    stderr: format!("Execution I/O error: {}{}", e, stderr),
                    duration_ms,
                    status: "failed".to_string(),
                }
            }
            Err(_) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                warn!(
                    "Command execution timed out after {} ms",
                    timeout_duration.as_millis()
                );

                // Take the whole tree down (e.g. the cmd.exe / sh children), then reap the leader.
                Self::terminate_process_tree(&mut child, process_group_id).await;

                // With every holder of the pipes dead the readers reach EOF; keep what they got.
                let out = drain_after_kill(stdout_reader.as_mut()).await;
                let err = drain_after_kill(stderr_reader.as_mut()).await;

                let mut stderr = format!(
                    "Command timed out after {} ms",
                    timeout_duration.as_millis()
                );
                let partial_err = render_capture(err);
                if !partial_err.is_empty() {
                    stderr.push('\n');
                    stderr.push_str(&partial_err);
                }

                CommandExecutionResult {
                    exit_code: 124, // Standard UNIX timeout exit code
                    stdout: render_capture(out),
                    stderr,
                    duration_ms,
                    status: "timeout".to_string(),
                }
            }
        }
    }

    /// Kills the command's whole process tree and reaps the leader.
    ///
    /// `process_group_id` is the PID recorded at spawn time, which is also the command's process
    /// group id on Unix. The group id stays reserved for as long as any member of the group is
    /// alive, so signalling it never hits an unrelated process even when the leader has already
    /// been reaped (e.g. it exited but a grandchild still holds the output pipes).
    async fn terminate_process_tree(child: &mut Child, process_group_id: Option<u32>) {
        // The leader's PID is only trustworthy while it has not been reaped: `Child::id` returns
        // `None` afterwards, so a stale PID is never used for the PID-based paths below.
        let live_leader = child.id();

        // Snapshot descendants first, to also catch processes that left the group (setsid).
        let escaped = live_leader
            .map(Self::collect_descendants)
            .unwrap_or_default();

        #[cfg(unix)]
        {
            if let Some(pgid) = process_group_id {
                // SAFETY: killpg only sends a signal; the id is the process group we created.
                unsafe {
                    libc::killpg(pgid as libc::pid_t, libc::SIGKILL);
                }
            }
        }

        #[cfg(target_os = "windows")]
        {
            // No process groups here: taskkill /F /T takes the leader and its children, but the PID
            // is only valid while the leader is unreaped (`live_leader`).
            let _ = process_group_id;
            if let Some(pid) = live_leader {
                let _ = Command::new("taskkill")
                    .args(["/F", "/T", "/PID", &pid.to_string()])
                    .output()
                    .await;
            }
        }

        #[cfg(not(any(unix, target_os = "windows")))]
        {
            let _ = process_group_id;
        }

        if !escaped.is_empty() {
            let mut sys = System::new();
            sys.refresh_processes();
            for pid in escaped {
                if let Some(proc) = sys.process(pid) {
                    let _ = proc.kill();
                }
            }
        }

        // Leader last (tokio ignores this if it is already gone), then reap it.
        let _ = child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
    }

    /// Every transitive child of `root`, from a fresh process snapshot. Only call this while
    /// `root` is a live (unreaped) child of ours, so the PID cannot belong to someone else.
    fn collect_descendants(root: u32) -> Vec<sysinfo::Pid> {
        let mut sys = System::new();
        sys.refresh_processes();

        let mut found = Vec::new();
        let mut queue = vec![sysinfo::Pid::from(root as usize)];

        while let Some(parent) = queue.pop() {
            for (&p, proc) in sys.processes() {
                if proc.parent() == Some(parent) && !found.contains(&p) {
                    found.push(p);
                    queue.push(p);
                }
            }
        }
        found
    }

    /// Selects the optimal shell interpreter depending on host OS (Windows, macOS, Ubuntu, Android)
    fn create_shell_command(cmd_str: &str) -> Command {
        #[cfg(target_os = "windows")]
        {
            let mut c = Command::new("cmd.exe");
            c.args(["/C", cmd_str]);
            c
        }

        #[cfg(not(target_os = "windows"))]
        {
            // Android uses /system/bin/sh if /bin/sh is absent
            let shell_path = if Path::new("/bin/sh").exists() {
                "/bin/sh"
            } else if Path::new("/system/bin/sh").exists() {
                "/system/bin/sh"
            } else {
                "sh"
            };

            let mut c = Command::new(shell_path);
            c.args(["-c", cmd_str]);
            c
        }
    }
}

/// Reads `reader` to EOF but keeps at most `cap` bytes. Returns what was kept and whether more
/// was produced (the excess is still drained so the writer never blocks).
async fn read_capped<R: AsyncRead + Unpin>(mut reader: R, cap: usize) -> (Vec<u8>, bool) {
    let mut kept = Vec::new();
    let mut truncated = false;
    let mut scratch = [0u8; 8192];

    loop {
        match reader.read(&mut scratch).await {
            Ok(0) => break,
            Ok(n) => {
                let room = cap.saturating_sub(kept.len());
                if n <= room {
                    kept.extend_from_slice(&scratch[..n]);
                } else {
                    kept.extend_from_slice(&scratch[..room]);
                    truncated = true;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    (kept, truncated)
}

type Capture = (Vec<u8>, bool);

/// Awaits a reader task; a missing pipe or a panicked task counts as empty output.
async fn join_capture(reader: Option<&mut tokio::task::JoinHandle<Capture>>) -> Capture {
    match reader {
        Some(handle) => handle.await.unwrap_or_default(),
        None => Capture::default(),
    }
}

/// Like `join_capture` for a reader whose writers were just killed: bounded wait, then give up.
async fn drain_after_kill(reader: Option<&mut tokio::task::JoinHandle<Capture>>) -> Capture {
    match reader {
        Some(handle) => match tokio::time::timeout(READER_DRAIN_GRACE, &mut *handle).await {
            Ok(joined) => joined.unwrap_or_default(),
            Err(_) => {
                handle.abort();
                Capture::default()
            }
        },
        None => Capture::default(),
    }
}

/// Lossy UTF-8 text of a capture, with a marker when it hit the size cap.
fn render_capture((bytes, truncated): Capture) -> String {
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if truncated {
        text.push_str(&format!(
            "\n[agent_mesh: output truncated after {MAX_CAPTURED_OUTPUT_BYTES} bytes]\n"
        ));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_echo_builtin_execution() {
        let res = CommandExecutor::execute(
            "echo",
            &serde_json::json!({"message": "test echo"}),
            Some(1000),
        )
        .await;
        assert_eq!(res.status, "success");
        assert_eq!(res.exit_code, 0);
        assert_eq!(res.stdout, "test echo");
    }

    #[tokio::test]
    async fn test_ping_builtin_execution() {
        let res = CommandExecutor::execute("ping", &Value::Null, Some(1000)).await;
        assert_eq!(res.status, "success");
        assert_eq!(res.exit_code, 0);
        assert_eq!(res.stdout, "pong");
    }

    #[tokio::test]
    async fn test_system_info_builtin_execution() {
        let res = CommandExecutor::execute("system_info", &Value::Null, Some(2000)).await;
        assert_eq!(res.status, "success");
        assert_eq!(res.exit_code, 0);
        assert!(res.stdout.contains("os_name"));
    }

    #[tokio::test]
    async fn test_shell_exec_execution() {
        let res = CommandExecutor::execute(
            "shell_exec",
            &serde_json::json!({"cmd": "echo shell_success"}),
            Some(3000),
        )
        .await;
        assert_eq!(res.status, "success");
        assert_eq!(res.exit_code, 0);
        assert!(res.stdout.contains("shell_success"));
    }

    #[tokio::test]
    async fn test_read_capped_keeps_the_prefix_and_flags_overflow() {
        let (kept, truncated) = read_capped(&b"abcdef"[..], 4).await;
        assert_eq!(kept, b"abcd");
        assert!(truncated);

        let (kept, truncated) = read_capped(&b"abcd"[..], 4).await;
        assert_eq!(kept, b"abcd");
        assert!(!truncated, "output exactly at the cap is not truncated");

        let (kept, truncated) = read_capped(&b""[..], 4).await;
        assert!(kept.is_empty() && !truncated);
    }

    #[cfg(unix)]
    fn process_is_alive(pid: u32) -> bool {
        let mut sys = System::new();
        sys.refresh_processes();
        sys.process(sysinfo::Pid::from(pid as usize))
            .is_some_and(|p| p.status() != sysinfo::ProcessStatus::Zombie)
    }

    /// Polls until the PID written to `pid_file` is gone (or `within` elapses).
    #[cfg(unix)]
    async fn assert_pid_in_file_dies(pid_file: &Path, within: Duration) {
        let pid: u32 = std::fs::read_to_string(pid_file)
            .expect("the command should have recorded its background PID")
            .trim()
            .parse()
            .expect("PID file should hold a number");
        let deadline = Instant::now() + within;
        while process_is_alive(pid) {
            if Instant::now() > deadline {
                // Do not leave the stray behind even when the assertion fails.
                // SAFETY: plain SIGKILL to a PID this test just proved is its own leftover.
                unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
                panic!("background process {pid} survived the timeout kill");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_output_is_capped_on_both_streams() {
        let res = CommandExecutor::execute(
            "shell_exec",
            &serde_json::json!({
                "cmd": "head -c 3000000 /dev/zero | tr '\\0' x; head -c 3000000 /dev/zero | tr '\\0' y >&2"
            }),
            Some(20_000),
        )
        .await;

        assert_eq!(
            res.status,
            "success",
            "a chatty command must still finish: {:?}",
            res.stderr.len()
        );
        assert_eq!(res.exit_code, 0);
        for (name, text, fill) in [("stdout", &res.stdout, 'x'), ("stderr", &res.stderr, 'y')] {
            assert!(
                text.len() < MAX_CAPTURED_OUTPUT_BYTES + 200,
                "{name} was not capped: {} bytes",
                text.len()
            );
            assert!(
                text.contains("output truncated"),
                "{name} lacks the truncation marker"
            );
            assert!(
                text.starts_with(fill),
                "{name} should keep the head of the output"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_secrets_in_the_nodes_environment_are_not_inherited_by_commands() {
        const SENTINELS: [(&str, &str); 3] = [
            ("OXIDESWARM_SECRET", "sentinel-cluster-secret"),
            ("OXIDESWARM_SECRET_FILE", "/sentinel/secret/file"),
            ("OXIDE_MESH_TOKEN", "sentinel-mesh-token"),
        ];
        // The node's own secrets live in this process's environment; remove them afterwards.
        struct Restore;
        impl Drop for Restore {
            fn drop(&mut self) {
                for (name, _) in SENTINELS {
                    std::env::remove_var(name);
                }
            }
        }
        let _restore = Restore;
        for (name, value) in SENTINELS {
            std::env::set_var(name, value);
        }

        let probe = "echo \"[${OXIDESWARM_SECRET-unset}][${OXIDESWARM_SECRET_FILE-unset}][${OXIDE_MESH_TOKEN-unset}]\"";
        let res = CommandExecutor::execute(
            "shell_exec",
            &serde_json::json!({ "cmd": probe }),
            Some(5000),
        )
        .await;
        assert_eq!(res.status, "success");
        assert_eq!(res.stdout.trim(), "[unset][unset][unset]");

        // Also for a plain program, not only for the shell wrapper.
        let res =
            CommandExecutor::execute("sh", &serde_json::json!(["-c", probe]), Some(5000)).await;
        assert_eq!(res.stdout.trim(), "[unset][unset][unset]");

        // What the peer passes explicitly is its own business and still arrives.
        let res = CommandExecutor::execute(
            "shell_exec",
            &serde_json::json!({ "cmd": probe, "env": { "OXIDE_MESH_TOKEN": "given-on-purpose" } }),
            Some(5000),
        )
        .await;
        assert_eq!(res.stdout.trim(), "[unset][unset][given-on-purpose]");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_stdin_is_closed_for_commands() {
        // `cat` with no file reads stdin; with stdin=/dev/null it ends at once instead of hanging.
        let res =
            CommandExecutor::execute("shell_exec", &serde_json::json!({"cmd": "cat"}), Some(3000))
                .await;
        assert_eq!(res.status, "success");
        assert!(res.stdout.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_timeout_kills_background_grandchild_and_keeps_partial_output() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("grandchild.pid");
        let cmd = format!(
            "echo partial_marker; sleep 60 & echo $! > '{}'; wait",
            pid_file.display()
        );

        let res =
            CommandExecutor::execute("shell_exec", &serde_json::json!({ "cmd": cmd }), Some(800))
                .await;

        assert_eq!(res.status, "timeout");
        assert_eq!(res.exit_code, 124);
        assert!(res.stderr.contains("timed out after 800 ms"));
        assert!(
            res.stdout.contains("partial_marker"),
            "partial output lost: {:?}",
            res.stdout
        );
        assert_pid_in_file_dies(&pid_file, Duration::from_secs(5)).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_timeout_kills_group_even_after_the_leader_was_reaped() {
        // The shell exits at once, but its background `sleep` keeps the output pipes open, so the
        // executor times out with the leader already reaped. The old code then signalled a stale
        // PID; the group id recorded at spawn time must still reach the sleeper.
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("orphan.pid");
        let cmd = format!("sleep 60 & echo $! > '{}'", pid_file.display());

        let res =
            CommandExecutor::execute("shell_exec", &serde_json::json!({ "cmd": cmd }), Some(800))
                .await;

        assert_eq!(res.status, "timeout");
        assert_eq!(res.exit_code, 124);
        assert_pid_in_file_dies(&pid_file, Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn test_command_timeout_kills_hung_process() {
        #[cfg(target_os = "windows")]
        let (cmd, args) = (
            "powershell",
            serde_json::json!({
                "args": ["-NoProfile", "-Command", "Start-Sleep -Seconds 10"]
            }),
        );

        #[cfg(not(target_os = "windows"))]
        let (cmd, args) = ("sleep", serde_json::json!(["10"]));

        let start = std::time::Instant::now();
        let res = CommandExecutor::execute(cmd, &args, Some(1000)).await;
        let elapsed = start.elapsed();

        assert_eq!(res.status, "timeout");
        assert_eq!(res.exit_code, 124);
        assert!(res.stderr.contains("timed out after 1000 ms"));
        assert!(
            elapsed.as_millis() >= 950 && elapsed.as_millis() < 4000,
            "Expected timeout in ~1000ms, elapsed: {:?}",
            elapsed
        );
    }
}
