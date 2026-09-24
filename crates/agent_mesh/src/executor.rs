//! Cross-Platform Command Execution Engine
//!
//! Provides asynchronous process spawning, stdout/stderr capture,
//! execution duration measurement, timeout safety watchdogs,
//! and native built-in commands (echo, system_info, ping, sha256_verify).

use serde_json::Value;
use std::path::Path;
use std::time::{Duration, Instant};
use sysinfo::System;
use tokio::process::Command;
use tracing::{error, warn};

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
                let data = args.get("data").and_then(|v| v.as_str()).unwrap_or_default();
                let expected = args.get("expected").and_then(|v| v.as_str()).unwrap_or_default();
                let actual = crate::protocol::compute_sha256(data.as_bytes());
                let matches = actual.eq_ignore_ascii_case(expected);
                let duration_ms = start.elapsed().as_millis() as u64;

                return CommandExecutionResult {
                    exit_code: if matches { 0 } else { 1 },
                    stdout: if matches { "VERIFIED_MATCH".to_string() } else { "MISMATCH".to_string() },
                    stderr: if matches { String::new() } else { format!("Calculated {} != expected {}", actual, expected) },
                    duration_ms,
                    status: if matches { "success".to_string() } else { "failed".to_string() },
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
                    .map(|x| x.as_str().map(|s| s.to_string()).unwrap_or_else(|| x.to_string()))
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

        if let Some(env_map) = args.get("env").and_then(|v| v.as_object()) {
            for (k, v) in env_map {
                if let Some(s) = v.as_str() {
                    cmd.env(k, s);
                } else {
                    cmd.env(k, v.to_string());
                }
            }
        }

        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        // Spawn process
        let child = match cmd.spawn() {
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

        let child_pid = child.id();
        let wait_fut = child.wait_with_output();
        tokio::pin!(wait_fut);

        // Await process output with timeout safety watchdog
        match tokio::time::timeout(timeout_duration, &mut wait_fut).await {
            Ok(Ok(output)) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                let exit_code = output.status.code().unwrap_or(if output.status.success() { 0 } else { 1 });
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                let status = if output.status.success() { "success" } else { "failed" }.to_string();

                CommandExecutionResult {
                    exit_code,
                    stdout,
                    stderr,
                    duration_ms,
                    status,
                }
            }
            Ok(Err(e)) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                error!("I/O error waiting for command '{}': {}", command, e);
                CommandExecutionResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: format!("Execution I/O error: {}", e),
                    duration_ms,
                    status: "failed".to_string(),
                }
            }
            Err(_) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                warn!("Command execution timed out after {} ms", timeout_duration.as_millis());

                // Terminate process tree to prevent orphaned child processes (e.g. cmd.exe / sh children)
                if let Some(pid) = child_pid {
                    Self::terminate_process_tree(pid);
                }

                CommandExecutionResult {
                    exit_code: 124, // Standard UNIX timeout exit code
                    stdout: String::new(),
                    stderr: format!("Command timed out after {} ms", timeout_duration.as_millis()),
                    duration_ms,
                    status: "timeout".to_string(),
                }
            }
        }
    }

    /// Cross-platform process tree terminator to eliminate orphaned grandchild processes.
    fn terminate_process_tree(pid: u32) {
        #[cfg(target_os = "windows")]
        {
            // taskkill /F /T forcefully terminates the specified process and all its child processes
            let _ = std::process::Command::new("taskkill")
                .args(["/F", "/T", "/PID", &pid.to_string()])
                .output();
        }

        #[cfg(unix)]
        {
            // On Unix, kill the process group (-pgid) and direct child processes via pkill
            let pgid = pid as i32;
            let _ = std::process::Command::new("kill")
                .args(["-9", &format!("-{pgid}")])
                .output();
            let _ = std::process::Command::new("pkill")
                .args(["-9", "-P", &pid.to_string()])
                .output();
        }

        let mut sys = System::new();
        sys.refresh_processes();
        let target_pid = sysinfo::Pid::from(pid as usize);

        // Collect all descendant PIDs
        let mut to_kill = Vec::new();
        let mut queue = vec![target_pid];

        while let Some(parent) = queue.pop() {
            for (&p, proc) in sys.processes() {
                if proc.parent() == Some(parent) {
                    to_kill.push(p);
                    queue.push(p);
                }
            }
        }

        for p in to_kill {
            if let Some(proc) = sys.process(p) {
                let _ = proc.kill();
            }
        }

        if let Some(proc) = sys.process(target_pid) {
            let _ = proc.kill();
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_echo_builtin_execution() {
        let res = CommandExecutor::execute("echo", &serde_json::json!({"message": "test echo"}), Some(1000)).await;
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
        ).await;
        assert_eq!(res.status, "success");
        assert_eq!(res.exit_code, 0);
        assert!(res.stdout.contains("shell_success"));
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
        assert!(elapsed.as_millis() >= 950 && elapsed.as_millis() < 4000,
            "Expected timeout in ~1000ms, elapsed: {:?}", elapsed);
    }
}
