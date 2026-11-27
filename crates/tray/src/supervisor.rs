//! Child-process supervisor for `oxideswarm worker` (`TRAY-1`, backoff + clean quit).

use crate::state::SupervisorPhase;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// Configuration for the supervised worker child process.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub prepend_worker_subcommand: bool,
    pub extra_env: Vec<(String, String)>,
    pub log_file: Option<PathBuf>,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub stable_reset_after: Duration,
}

impl SupervisorConfig {
    /// Creates defaults matching `design 0112 D1.1` (1s initial backoff capped at 30s).
    pub fn new(program: PathBuf, args: Vec<String>) -> Self {
        Self {
            program,
            args,
            prepend_worker_subcommand: true,
            extra_env: Vec::new(),
            log_file: None,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            stable_reset_after: Duration::from_secs(30),
        }
    }
}

/// Computes bounded exponential backoff (`initial * 2^(attempt - 1)` clamped to `max`).
pub fn compute_backoff(attempt: u32, initial: Duration, max: Duration) -> Duration {
    let shift = attempt.saturating_sub(1).min(16);
    let factor = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
    initial.saturating_mul(factor).min(max)
}

/// Normalizes CLI arguments so `oxideswarm` runs the `worker` subcommand unless disabled.
pub fn build_worker_args(config: &SupervisorConfig) -> Vec<String> {
    if !config.prepend_worker_subcommand {
        return config.args.clone();
    }
    if config.args.first().map(String::as_str) == Some("worker") {
        return config.args.clone();
    }
    let mut full = Vec::with_capacity(config.args.len() + 1);
    full.push("worker".to_string());
    full.extend(config.args.iter().cloned());
    full
}

/// Manages a single `oxideswarm worker` child process started by the tray.
pub struct WorkerSupervisor {
    config: SupervisorConfig,
    child: Option<Child>,
    started_at: Option<Instant>,
    consecutive_failures: u32,
    next_restart_at: Option<Instant>,
    last_exit_code: Option<i32>,
    stopped_by_user: bool,
}

impl WorkerSupervisor {
    /// Constructs a supervisor that starts on the first [`WorkerSupervisor::step`] tick.
    pub fn new(config: SupervisorConfig) -> Self {
        Self {
            config,
            child: None,
            started_at: None,
            consecutive_failures: 0,
            next_restart_at: None,
            last_exit_code: None,
            stopped_by_user: false,
        }
    }

    /// Returns whether the supervisor currently tracks an active child handle.
    pub fn has_child(&self) -> bool {
        self.child.is_some()
    }

    /// Clears a user pause/stop request so the worker restarts cleanly.
    pub fn start_if_stopped(&mut self) {
        self.stopped_by_user = false;
        self.consecutive_failures = 0;
        self.next_restart_at = None;
    }

    /// Advances child lifecycle once: detects crashes, applies backoff, and spawns when ready.
    pub fn step(&mut self, now: Instant) -> SupervisorPhase {
        if self.stopped_by_user {
            return SupervisorPhase::StoppedByUser;
        }

        if let Some(child) = self.child.as_mut() {
            match child.try_wait() {
                Ok(None) => {
                    let pid = child.id().unwrap_or(0);
                    let uptime = self
                        .started_at
                        .map(|t| now.saturating_duration_since(t))
                        .unwrap_or_default();
                    if uptime >= self.config.stable_reset_after {
                        self.consecutive_failures = 0;
                    }
                    return SupervisorPhase::Running {
                        pid,
                        uptime_secs: uptime.as_secs(),
                    };
                }
                Ok(Some(status)) => {
                    self.child = None;
                    let ran_long_enough = self
                        .started_at
                        .map(|t| now.saturating_duration_since(t) >= self.config.stable_reset_after)
                        .unwrap_or(false);
                    self.started_at = None;
                    if ran_long_enough {
                        self.consecutive_failures = 1;
                    } else {
                        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                    }
                    self.last_exit_code = status.code();
                    let delay = compute_backoff(
                        self.consecutive_failures,
                        self.config.initial_backoff,
                        self.config.max_backoff,
                    );
                    self.next_restart_at = Some(now + delay);
                    tracing::warn!(
                        exit_code = ?self.last_exit_code,
                        attempt = self.consecutive_failures,
                        delay_ms = delay.as_millis() as u64,
                        "worker process exited unexpectedly; scheduling restart"
                    );
                    return SupervisorPhase::Backoff {
                        attempt: self.consecutive_failures,
                        next_delay_secs: delay.as_secs().max(u64::from(!delay.is_zero())),
                        last_exit: self.last_exit_code,
                    };
                }
                Err(err) => {
                    tracing::warn!("try_wait failed on worker child: {err}");
                }
            }
        }

        if let Some(deadline) = self.next_restart_at {
            if now < deadline {
                let rem = deadline.saturating_duration_since(now);
                return SupervisorPhase::Backoff {
                    attempt: self.consecutive_failures,
                    next_delay_secs: rem.as_secs().max(u64::from(!rem.is_zero())),
                    last_exit: self.last_exit_code,
                };
            }
        }

        match self.spawn_child() {
            Ok(child) => {
                let pid = child.id().unwrap_or(0);
                self.child = Some(child);
                self.started_at = Some(now);
                self.next_restart_at = None;
                tracing::info!(pid, program = %self.config.program.display(), "started worker child");
                SupervisorPhase::Running {
                    pid,
                    uptime_secs: 0,
                }
            }
            Err(err) => {
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                self.last_exit_code = None;
                let delay = compute_backoff(
                    self.consecutive_failures,
                    self.config.initial_backoff,
                    self.config.max_backoff,
                );
                self.next_restart_at = Some(now + delay);
                tracing::error!(
                    attempt = self.consecutive_failures,
                    delay_ms = delay.as_millis() as u64,
                    "failed to spawn worker child: {err}"
                );
                SupervisorPhase::Backoff {
                    attempt: self.consecutive_failures,
                    next_delay_secs: delay.as_secs().max(u64::from(!delay.is_zero())),
                    last_exit: None,
                }
            }
        }
    }

    fn spawn_child(&self) -> std::io::Result<Child> {
        let mut cmd = Command::new(&self.config.program);
        cmd.args(build_worker_args(&self.config));
        cmd.stdin(Stdio::null());

        if let Some(log_path) = &self.config.log_file {
            if let Some(parent) = log_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match OpenOptions::new().create(true).append(true).open(log_path) {
                Ok(out) => match out.try_clone() {
                    Ok(err) => {
                        cmd.stdout(Stdio::from(out));
                        cmd.stderr(Stdio::from(err));
                    }
                    Err(_) => {
                        cmd.stdout(Stdio::from(out));
                        cmd.stderr(Stdio::null());
                    }
                },
                Err(e) => {
                    tracing::warn!("could not open worker log file {}: {e}", log_path.display());
                    cmd.stdout(Stdio::null());
                    cmd.stderr(Stdio::null());
                }
            }
        } else {
            cmd.stdout(Stdio::null());
            cmd.stderr(Stdio::null());
        }

        for (k, v) in &self.config.extra_env {
            cmd.env(k, v);
        }

        #[cfg(windows)]
        {
            // Never open a console window when launched from a GUI/logon tray session.
            cmd.creation_flags(CREATE_NO_WINDOW);
        }

        cmd.spawn()
    }

    /// Terminates ONLY the worker child process spawned by this supervisor and waits for exit.
    pub async fn stop_and_wait(&mut self) {
        self.stopped_by_user = true;
        self.next_restart_at = None;
        self.started_at = None;
        let Some(mut child) = self.child.take() else {
            return;
        };
        let pid = child.id();
        if let Err(err) = child.start_kill() {
            tracing::debug!(?pid, "worker child start_kill returned: {err}");
        }
        match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
            Ok(Ok(status)) => {
                tracing::info!(?pid, ?status, "stopped supervised worker child");
            }
            Ok(Err(err)) => {
                tracing::warn!(?pid, "error awaiting stopped worker child: {err}");
            }
            Err(_) => {
                tracing::warn!(?pid, "timed out waiting for stopped worker child to exit");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_backoff_doubles_and_caps() {
        let init = Duration::from_secs(1);
        let max = Duration::from_secs(30);
        assert_eq!(compute_backoff(1, init, max), Duration::from_secs(1));
        assert_eq!(compute_backoff(2, init, max), Duration::from_secs(2));
        assert_eq!(compute_backoff(3, init, max), Duration::from_secs(4));
        assert_eq!(compute_backoff(5, init, max), Duration::from_secs(16));
        assert_eq!(compute_backoff(6, init, max), Duration::from_secs(30));
        assert_eq!(compute_backoff(20, init, max), Duration::from_secs(30));
    }

    #[test]
    fn build_worker_args_prepends_worker_once() {
        let mut cfg = SupervisorConfig::new(
            PathBuf::from("oxideswarm"),
            vec!["--name".into(), "rig1".into()],
        );
        assert_eq!(build_worker_args(&cfg), vec!["worker", "--name", "rig1"]);

        cfg.args = vec!["worker".into(), "--no-gpu".into()];
        assert_eq!(build_worker_args(&cfg), vec!["worker", "--no-gpu"]);

        cfg.prepend_worker_subcommand = false;
        cfg.args = vec!["--exact".into()];
        assert_eq!(build_worker_args(&cfg), vec!["--exact"]);
    }
}
