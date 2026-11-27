//! Local same-user control channel and contributor pause/limits state (`design 0113` D2/D3).
//!
//! Each running worker listens on a private local endpoint (`control.sock` with mode `0600` inside
//! a `0700` directory on Unix, or `\\.\pipe\oxideswarm-<user>-<name>` on Windows) and accepts
//! bounded JSON-line commands (`status`, `pause`, `resume`, `limits get`, `limits set`). Peers are
//! verified to belong to the same OS user before any command runs.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Local, NaiveTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{broadcast, watch};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Hard cap on a single JSON-line control request or response (8 KiB).
pub const MAX_CONTROL_LINE_BYTES: usize = 8 * 1024;

/// Default pause rejection prefix understood by D3 masters (as well as 0.4.x masters via
/// `"rejected by worker"`).
pub const PAUSE_REJECTION_PREFIX: &str = "Task rejected by worker: paused: ";

/// Rejection error/stderr text emitted when `--no-gpu-tasks` is enabled on this worker.
pub const NO_GPU_TASKS_REJECTION_MESSAGE: &str = "Task rejected by worker: no gpu tasks";

/// Contributor-configurable worker limits (`design 0113` D1/D2).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct WorkerLimitsConfig {
    #[serde(default)]
    pub local_max_cpu_pct: Option<u8>,
    #[serde(default)]
    pub hours: Option<String>,
    #[serde(default)]
    pub max_task_hours_per_day: Option<f64>,
    #[serde(default)]
    pub no_tasks_on_battery: Option<bool>,
    #[serde(default)]
    pub no_tasks_on_metered: Option<bool>,
    #[serde(default)]
    pub no_gpu_tasks: bool,
}

impl WorkerLimitsConfig {
    /// Parses a `key=value` update and applies it to a cloned copy after validation.
    pub fn apply_kv(&mut self, key: &str, raw_value: &str) -> Result<String, String> {
        let norm_key = normalize_limit_key(key);
        let val = raw_value.trim();
        match norm_key.as_str() {
            "local_max_cpu_pct" => {
                let parsed = if is_clear_token(val) {
                    None
                } else {
                    let pct: u8 = val.parse().map_err(|_| {
                        format!("invalid local_max_cpu_pct '{val}' (expected 1..=100 or none)")
                    })?;
                    if !(1..=100).contains(&pct) {
                        return Err(format!(
                            "local_max_cpu_pct must be between 1 and 100 (got {pct})"
                        ));
                    }
                    Some(pct)
                };
                self.local_max_cpu_pct = parsed;
            }
            "hours" => {
                let parsed = if is_clear_token(val) {
                    None
                } else {
                    validate_hours_window(val)?;
                    Some(val.to_string())
                };
                self.hours = parsed;
            }
            "max_task_hours_per_day" => {
                let parsed = if is_clear_token(val) {
                    None
                } else {
                    let hrs: f64 = val.parse().map_err(|_| {
                        format!(
                            "invalid max_task_hours_per_day '{val}' (expected number >= 0 or none)"
                        )
                    })?;
                    if !hrs.is_finite() || hrs <= 0.0 || hrs > 24.0 {
                        return Err(format!(
                            "max_task_hours_per_day must be in (0, 24] (got {hrs})"
                        ));
                    }
                    Some(hrs)
                };
                self.max_task_hours_per_day = parsed;
            }
            "no_tasks_on_battery" => {
                self.no_tasks_on_battery = if is_clear_token(val) {
                    None
                } else {
                    Some(parse_bool_flag(val, "no_tasks_on_battery")?)
                };
            }
            "no_tasks_on_metered" => {
                self.no_tasks_on_metered = if is_clear_token(val) {
                    None
                } else {
                    Some(parse_bool_flag(val, "no_tasks_on_metered")?)
                };
            }
            "no_gpu_tasks" => {
                self.no_gpu_tasks = parse_bool_flag(val, "no_gpu_tasks")?;
            }
            _ => {
                return Err(format!(
                    "unknown worker limit key '{key}' (expected one of: local_max_cpu_pct, hours, max_task_hours_per_day, no_tasks_on_battery, no_tasks_on_metered, no_gpu_tasks)"
                ));
            }
        }
        Ok(norm_key)
    }
}

fn normalize_limit_key(key: &str) -> String {
    key.trim().to_ascii_lowercase().replace('-', "_")
}

fn is_clear_token(val: &str) -> bool {
    matches!(
        val.to_ascii_lowercase().as_str(),
        "none" | "unset" | "null" | ""
    )
}

fn parse_bool_flag(val: &str, key: &str) -> Result<bool, String> {
    match val.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(format!("invalid boolean '{val}' for {key}")),
    }
}

fn validate_hours_window(val: &str) -> Result<(), String> {
    let Some((start, end)) = val.split_once('-') else {
        return Err(format!(
            "invalid hours window '{val}' (expected HH:MM-HH:MM)"
        ));
    };
    parse_hhmm(start.trim())?;
    parse_hhmm(end.trim())?;
    Ok(())
}

fn parse_hhmm(s: &str) -> Result<NaiveTime, String> {
    NaiveTime::parse_from_str(s, "%H:%M")
        .map_err(|_| format!("invalid time '{s}' (expected HH:MM 24-hour clock)"))
}

/// High-level local operational state reported by `oxideswarm worker status`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerControlState {
    Running,
    Held,
    Paused,
}

impl WorkerControlState {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkerControlState::Running => "running",
            WorkerControlState::Held => "held",
            WorkerControlState::Paused => "paused",
        }
    }
}

/// Snapshot returned by `status`, `pause`, `resume`, and `limits set`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ControlStatusSnapshot {
    pub worker_id: Uuid,
    pub worker_name: String,
    pub state: WorkerControlState,
    pub reason: Option<String>,
    pub paused_until_epoch_ms: Option<u64>,
    pub no_gpu_tasks: bool,
    pub active_tasks: usize,
    pub limits: WorkerLimitsConfig,
    pub config_path: Option<String>,
}

/// Control request sent over the worker's local socket or named pipe.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlRequest {
    Status,
    Pause {
        until_epoch_ms: Option<u64>,
        reason: Option<String>,
    },
    Resume,
    LimitsGet,
    LimitsSet {
        key: String,
        value: String,
    },
}

/// Control response returned by the worker over its local endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ControlResponse {
    Status {
        snapshot: ControlStatusSnapshot,
    },
    Ok {
        message: String,
        snapshot: ControlStatusSnapshot,
    },
    Limits {
        limits: WorkerLimitsConfig,
        config_path: Option<String>,
    },
    Error {
        message: String,
    },
}

/// Local IPC endpoint where a running worker listens for same-user control commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlEndpoint {
    #[cfg(unix)]
    UnixSocket(PathBuf),
    #[cfg(windows)]
    NamedPipe(String),
}

impl ControlEndpoint {
    /// Human-readable description for logs and CLI output.
    pub fn display_target(&self) -> String {
        match self {
            #[cfg(unix)]
            ControlEndpoint::UnixSocket(p) => p.display().to_string(),
            #[cfg(windows)]
            ControlEndpoint::NamedPipe(name) => name.clone(),
        }
    }

    /// Resolves the control endpoint for `worker_name`, using `explicit_socket` when provided.
    pub fn resolve(explicit_socket: Option<&Path>, worker_name: Option<&str>) -> Option<Self> {
        #[cfg(unix)]
        {
            if let Some(path) = explicit_socket {
                return Some(ControlEndpoint::UnixSocket(path.to_path_buf()));
            }
            default_unix_socket_path(worker_name).map(ControlEndpoint::UnixSocket)
        }
        #[cfg(windows)]
        {
            if let Some(path) = explicit_socket {
                return Some(ControlEndpoint::NamedPipe(
                    path.to_string_lossy().into_owned(),
                ));
            }
            Some(ControlEndpoint::NamedPipe(default_windows_pipe_name(
                worker_name,
            )))
        }
    }
}

/// Sanitizes a worker or user segment for use in a socket filename or Windows pipe name.
pub fn sanitize_endpoint_segment(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches(|c| c == '.' || c == '_');
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Default Unix control socket path (`$OXIDESWARM_HOME/...`, `$XDG_RUNTIME_DIR/oxideswarm/...`, or
/// `$HOME/.oxideswarm/...`).
#[cfg(unix)]
pub fn default_unix_socket_path(worker_name: Option<&str>) -> Option<PathBuf> {
    default_unix_socket_path_with(
        worker_name,
        std::env::var_os("OXIDESWARM_HOME").as_deref(),
        std::env::var_os("XDG_RUNTIME_DIR").as_deref(),
        std::env::var_os("HOME").as_deref(),
    )
}

#[cfg(unix)]
pub(crate) fn default_unix_socket_path_with(
    worker_name: Option<&str>,
    oxideswarm_home: Option<&std::ffi::OsStr>,
    xdg_runtime_dir: Option<&std::ffi::OsStr>,
    home: Option<&std::ffi::OsStr>,
) -> Option<PathBuf> {
    let dir = if let Some(ox_home) = oxideswarm_home.filter(|v| !v.is_empty()) {
        PathBuf::from(ox_home)
    } else if let Some(xdg) = xdg_runtime_dir.filter(|v| !v.is_empty()) {
        PathBuf::from(xdg).join("oxideswarm")
    } else {
        let home = home.filter(|v| !v.is_empty())?;
        PathBuf::from(home).join(".oxideswarm")
    };
    let file = match worker_name.map(str::trim).filter(|s| !s.is_empty()) {
        Some(name) => format!("{}.control.sock", sanitize_endpoint_segment(name)),
        None => "control.sock".to_string(),
    };
    Some(dir.join(file))
}

/// Default Windows named pipe path (`\\.\pipe\oxideswarm-<user>-<worker-name>`).
#[cfg(windows)]
pub fn default_windows_pipe_name(worker_name: Option<&str>) -> String {
    let user = std::env::var("USERNAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "user".to_string());
    let worker = worker_name
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("default");
    format!(
        r"\\.\pipe\oxideswarm-{}-{}",
        sanitize_endpoint_segment(&user),
        sanitize_endpoint_segment(worker)
    )
}

/// Asynchronous state transition events emitted by [`ControlHandle`] to the active worker session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlEvent {
    PauseHandBack { reason: String },
    ResumeReconnect,
}

/// Assignment eligibility outcome checked in `WorkerClient` right before running an `AssignTask`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssignmentDecision {
    Allow,
    RejectPaused(String),
    RejectNoGpuTasks,
}

/// Callback type invoked when contributor limits are persisted to disk.
pub type LimitPersistFn =
    Arc<dyn Fn(&WorkerLimitsConfig) -> Result<Option<PathBuf>, String> + Send + Sync>;

/// Hook invoked by `limits set` to persist updated `[worker]` settings into the config file.
#[derive(Clone)]
pub struct LimitPersistHook(pub LimitPersistFn);

impl std::fmt::Debug for LimitPersistHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LimitPersistHook(..)")
    }
}

#[derive(Debug)]
struct ControlStateInner {
    paused: bool,
    pause_reason: Option<String>,
    paused_until: Option<tokio::time::Instant>,
    paused_until_epoch_ms: Option<u64>,
    held: bool,
    hold_reason: Option<String>,
    limits: WorkerLimitsConfig,
}

/// Thread-safe handle shared between the worker execution engine and the local control server.
pub struct ControlHandle {
    worker_id: Uuid,
    worker_name: String,
    active_tasks: Arc<AtomicUsize>,
    config_path: Mutex<Option<PathBuf>>,
    persist_hook: Option<LimitPersistHook>,
    inner: Mutex<ControlStateInner>,
    event_tx: broadcast::Sender<ControlEvent>,
    session_sent_pause_rejection: AtomicBool,
}

impl std::fmt::Debug for ControlHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlHandle")
            .field("worker_id", &self.worker_id)
            .field("worker_name", &self.worker_name)
            .finish()
    }
}

impl ControlHandle {
    /// Creates a new [`ControlHandle`] with initial limits and optional persistence hook.
    pub fn new(
        worker_id: Uuid,
        worker_name: impl Into<String>,
        active_tasks: Arc<AtomicUsize>,
        limits: WorkerLimitsConfig,
        config_path: Option<PathBuf>,
        persist_hook: Option<LimitPersistHook>,
    ) -> Arc<Self> {
        let (event_tx, _) = broadcast::channel(32);
        Arc::new(Self {
            worker_id,
            worker_name: worker_name.into(),
            active_tasks,
            config_path: Mutex::new(config_path),
            persist_hook,
            inner: Mutex::new(ControlStateInner {
                paused: false,
                pause_reason: None,
                paused_until: None,
                paused_until_epoch_ms: None,
                held: false,
                hold_reason: None,
                limits,
            }),
            event_tx,
            session_sent_pause_rejection: AtomicBool::new(false),
        })
    }

    /// Subscribes to pause/resume transitions for the active session loop.
    pub fn subscribe(&self) -> broadcast::Receiver<ControlEvent> {
        self.event_tx.subscribe()
    }

    /// Records that the current master session received a pause refusal or hold-back.
    pub fn mark_session_pause_rejection(&self) {
        self.session_sent_pause_rejection
            .store(true, Ordering::Release);
    }

    /// Resets the per-session pause refusal tracker when a fresh master session connects.
    pub fn clear_session_pause_rejection(&self) {
        self.session_sent_pause_rejection
            .store(false, Ordering::Release);
    }

    /// Returns and clears whether the current session needs an immediate reconnect on resume.
    pub fn take_session_pause_rejection(&self) -> bool {
        self.session_sent_pause_rejection
            .swap(false, Ordering::AcqRel)
    }

    /// Formats the worker's `Task rejected by worker: paused: <reason>` message.
    pub fn format_pause_rejection(reason: &str) -> String {
        let trimmed = reason.trim();
        let why = if trimmed.is_empty() {
            "contributor paused worker"
        } else {
            trimmed
        };
        format!("{PAUSE_REJECTION_PREFIX}{why}")
    }

    /// Returns the current control snapshot (auto-resuming if a timed pause expired).
    pub fn snapshot(&self) -> ControlStatusSnapshot {
        let _ = self.check_pause_expiry();
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        self.build_snapshot(&inner)
    }

    fn build_snapshot(&self, inner: &ControlStateInner) -> ControlStatusSnapshot {
        let (state, reason) = if inner.paused {
            (
                WorkerControlState::Paused,
                inner
                    .pause_reason
                    .clone()
                    .or_else(|| Some("contributor paused worker".to_string())),
            )
        } else if inner.held {
            (
                WorkerControlState::Held,
                inner
                    .hold_reason
                    .clone()
                    .or_else(|| Some("local contributor limit active".to_string())),
            )
        } else {
            (WorkerControlState::Running, None)
        };
        let config_path = self
            .config_path
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|p| p.display().to_string());
        ControlStatusSnapshot {
            worker_id: self.worker_id,
            worker_name: self.worker_name.clone(),
            state,
            reason,
            paused_until_epoch_ms: inner.paused_until_epoch_ms,
            no_gpu_tasks: inner.limits.no_gpu_tasks,
            active_tasks: self.active_tasks.load(Ordering::Acquire),
            limits: inner.limits.clone(),
            config_path,
        }
    }

    /// Pauses the worker immediately and requests hand-back of running tasks within 5 seconds.
    pub fn pause(
        &self,
        until_epoch_ms: Option<u64>,
        reason: Option<String>,
    ) -> Result<ControlStatusSnapshot, String> {
        let until_instant = if let Some(target_ms) = until_epoch_ms {
            let now_ms = current_epoch_ms();
            if target_ms <= now_ms {
                return Err("--until timestamp is already in the past".to_string());
            }
            Some(tokio::time::Instant::now() + Duration::from_millis(target_ms - now_ms))
        } else {
            None
        };
        let why = reason
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("contributor paused worker")
            .to_string();
        let snapshot = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.paused = true;
            inner.pause_reason = Some(why.clone());
            inner.paused_until = until_instant;
            inner.paused_until_epoch_ms = until_epoch_ms;
            self.build_snapshot(&inner)
        };
        let _ = self
            .event_tx
            .send(ControlEvent::PauseHandBack { reason: why });
        Ok(snapshot)
    }

    /// Clears contributor pause state and asks the active session to reconnect if held back.
    pub fn resume(&self) -> ControlStatusSnapshot {
        let (was_paused, snapshot) = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let was_paused = inner.paused;
            inner.paused = false;
            inner.pause_reason = None;
            inner.paused_until = None;
            inner.paused_until_epoch_ms = None;
            (was_paused, self.build_snapshot(&inner))
        };
        if was_paused || self.session_sent_pause_rejection.load(Ordering::Acquire) {
            let _ = self.event_tx.send(ControlEvent::ResumeReconnect);
        }
        snapshot
    }

    /// Sets or clears automatic hold state (for D1 limit enforcement; `NET-9b`).
    pub fn set_held(&self, held: bool, reason: Option<String>, hand_back_running: bool) {
        let mut trigger_handback = None;
        let mut trigger_resume = false;
        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let was_held = inner.held;
            inner.held = held;
            inner.hold_reason = if held {
                Some(
                    reason
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .unwrap_or("local contributor limit active")
                        .to_string(),
                )
            } else {
                None
            };
            if held && !was_held && hand_back_running {
                trigger_handback = inner.hold_reason.clone();
            } else if !held && was_held && !inner.paused {
                trigger_resume = true;
            }
        }
        if let Some(why) = trigger_handback {
            let _ = self
                .event_tx
                .send(ControlEvent::PauseHandBack { reason: why });
        } else if trigger_resume {
            let _ = self.event_tx.send(ControlEvent::ResumeReconnect);
        }
    }

    /// Checks whether a timed pause has expired; if so, clears it and emits `ResumeReconnect`.
    pub fn check_pause_expiry(&self) -> Option<ControlStatusSnapshot> {
        let expired = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.paused
                && inner
                    .paused_until
                    .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        };
        if expired {
            Some(self.resume())
        } else {
            None
        }
    }

    /// Checks whether a new task assignment may begin execution on this worker.
    pub fn check_assignment(&self, requires_gpu: bool) -> AssignmentDecision {
        let _ = self.check_pause_expiry();
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.paused {
            return AssignmentDecision::RejectPaused(
                inner
                    .pause_reason
                    .clone()
                    .unwrap_or_else(|| "contributor paused worker".to_string()),
            );
        }
        if inner.held {
            return AssignmentDecision::RejectPaused(
                inner
                    .hold_reason
                    .clone()
                    .unwrap_or_else(|| "local contributor limit active".to_string()),
            );
        }
        if requires_gpu && inner.limits.no_gpu_tasks {
            return AssignmentDecision::RejectNoGpuTasks;
        }
        AssignmentDecision::Allow
    }

    /// Returns the active GPU task rejection preference.
    pub fn no_gpu_tasks(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .limits
            .no_gpu_tasks
    }

    /// Updates a single limit key, persists via the hook (if set), and commits atomically.
    pub fn set_limit(&self, key: &str, value: &str) -> Result<ControlStatusSnapshot, String> {
        let mut candidate = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.limits.clone()
        };
        let norm_key = candidate.apply_kv(key, value)?;
        let gpu_toggled_off = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            norm_key == "no_gpu_tasks" && inner.limits.no_gpu_tasks && !candidate.no_gpu_tasks
        };
        if let Some(ref hook) = self.persist_hook {
            if let Some(saved_path) = (hook.0)(&candidate)? {
                *self.config_path.lock().unwrap_or_else(|e| e.into_inner()) = Some(saved_path);
            }
        }
        let snapshot = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.limits = candidate;
            self.build_snapshot(&inner)
        };
        if gpu_toggled_off {
            let _ = self.event_tx.send(ControlEvent::ResumeReconnect);
        }
        Ok(snapshot)
    }

    /// Dispatches a parsed [`ControlRequest`] synchronously and produces a [`ControlResponse`].
    pub fn handle_request(&self, req: ControlRequest) -> ControlResponse {
        match req {
            ControlRequest::Status => ControlResponse::Status {
                snapshot: self.snapshot(),
            },
            ControlRequest::Pause {
                until_epoch_ms,
                reason,
            } => match self.pause(until_epoch_ms, reason) {
                Ok(snapshot) => ControlResponse::Ok {
                    message: match snapshot.paused_until_epoch_ms {
                        Some(ms) => format!("Worker paused until epoch ms {ms}"),
                        None => "Worker paused".to_string(),
                    },
                    snapshot,
                },
                Err(message) => ControlResponse::Error { message },
            },
            ControlRequest::Resume => {
                let snapshot = self.resume();
                ControlResponse::Ok {
                    message: "Worker resumed".to_string(),
                    snapshot,
                }
            }
            ControlRequest::LimitsGet => {
                let snap = self.snapshot();
                ControlResponse::Limits {
                    limits: snap.limits,
                    config_path: snap.config_path,
                }
            }
            ControlRequest::LimitsSet { key, value } => match self.set_limit(&key, &value) {
                Ok(snapshot) => ControlResponse::Ok {
                    message: format!("Updated limit '{key}'"),
                    snapshot,
                },
                Err(message) => ControlResponse::Error { message },
            },
        }
    }
}

/// Parses `--until` specs: relative durations (`30s`, `5m`, `2h`, `1d`), `HH:MM`, or RFC3339.
pub fn parse_pause_until(spec: &str, now_utc: DateTime<Utc>) -> Result<u64, String> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Err("--until value cannot be empty".to_string());
    }
    if let Some(secs) = parse_relative_duration_secs(trimmed)? {
        let target = now_utc + chrono::Duration::seconds(secs as i64);
        return u64::try_from(target.timestamp_millis())
            .map_err(|_| "pause deadline out of range".to_string());
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        let ms = dt.timestamp_millis();
        if ms <= now_utc.timestamp_millis() {
            return Err("--until timestamp is already in the past".to_string());
        }
        return u64::try_from(ms).map_err(|_| "pause deadline out of range".to_string());
    }
    if let Ok(time) = NaiveTime::parse_from_str(trimmed, "%H:%M") {
        let local_now = Local::now();
        let today = local_now.date_naive();
        let candidate_naive = today.and_time(time);
        let mut target_local = Local
            .from_local_datetime(&candidate_naive)
            .single()
            .or_else(|| Local.from_local_datetime(&candidate_naive).earliest())
            .ok_or_else(|| format!("ambiguous local time for '{trimmed}'"))?;
        if target_local <= local_now {
            let tomorrow = today
                .succ_opt()
                .ok_or_else(|| "calendar overflow computing tomorrow".to_string())?
                .and_time(time);
            target_local = Local
                .from_local_datetime(&tomorrow)
                .single()
                .or_else(|| Local.from_local_datetime(&tomorrow).earliest())
                .ok_or_else(|| format!("ambiguous local time for '{trimmed}'"))?;
        }
        return u64::try_from(target_local.with_timezone(&Utc).timestamp_millis())
            .map_err(|_| "pause deadline out of range".to_string());
    }
    Err(format!(
        "could not parse --until '{spec}' (use e.g. '2h', '30m', '18:00', or RFC3339 timestamp)"
    ))
}

fn parse_relative_duration_secs(raw: &str) -> Result<Option<u64>, String> {
    let Some(last) = raw.chars().last() else {
        return Ok(None);
    };
    let mult = match last {
        's' | 'S' => 1u64,
        'm' | 'M' => 60u64,
        'h' | 'H' => 3600u64,
        'd' | 'D' => 86_400u64,
        _ => return Ok(None),
    };
    let num_part = raw[..raw.len() - last.len_utf8()].trim();
    if num_part.is_empty() || !num_part.chars().all(|c| c.is_ascii_digit()) {
        return Ok(None);
    }
    let val: u64 = num_part
        .parse()
        .map_err(|_| format!("invalid duration '{raw}'"))?;
    if val == 0 {
        return Err("--until duration must be greater than zero".to_string());
    }
    val.checked_mul(mult)
        .map(Some)
        .ok_or_else(|| format!("duration overflow '{raw}'"))
}

fn current_epoch_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0)
}

async fn process_control_stream<S>(stream: S, handle: Arc<ControlHandle>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut limited = BufReader::new(tokio::io::AsyncReadExt::take(
        reader,
        (MAX_CONTROL_LINE_BYTES + 1) as u64,
    ));
    let mut line = String::new();
    let resp = match limited.read_line(&mut line).await {
        Ok(0) => return,
        Ok(n) if n > MAX_CONTROL_LINE_BYTES || !line.ends_with('\n') => ControlResponse::Error {
            message: format!("control request exceeded {MAX_CONTROL_LINE_BYTES} byte limit"),
        },
        Ok(_) => match serde_json::from_str::<ControlRequest>(line.trim()) {
            Ok(req) => handle.handle_request(req),
            Err(e) => ControlResponse::Error {
                message: format!("invalid control request: {e}"),
            },
        },
        Err(e) => {
            debug!(error = %e, "Failed reading local control stream");
            return;
        }
    };
    if let Ok(mut encoded) = serde_json::to_string(&resp) {
        encoded.push('\n');
        let _ = writer.write_all(encoded.as_bytes()).await;
        let _ = writer.flush().await;
    }
}

#[cfg(unix)]
mod unix_ipc {
    use super::*;
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
    use tokio::net::{UnixListener, UnixStream};

    struct SocketCleanup(PathBuf);
    impl Drop for SocketCleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    pub(super) async fn current_process_uid() -> Result<u32, String> {
        let (a, _b) = UnixStream::pair().map_err(|e| format!("unix socketpair failed: {e}"))?;
        let cred = a
            .peer_cred()
            .map_err(|e| format!("peer_cred query failed: {e}"))?;
        Ok(cred.uid())
    }

    fn ensure_private_socket_dir(dir: &Path, my_uid: u32) -> Result<(), String> {
        if let Ok(meta) = std::fs::symlink_metadata(dir) {
            if meta.file_type().is_symlink() {
                return Err(format!(
                    "refusing symlinked control socket directory {}",
                    dir.display()
                ));
            }
            if !meta.is_dir() {
                return Err(format!(
                    "control socket parent {} is not a directory",
                    dir.display()
                ));
            }
            if meta.uid() != my_uid {
                return Err(format!(
                    "control socket directory {} is owned by uid {}, expected {my_uid}",
                    dir.display(),
                    meta.uid()
                ));
            }
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                .map_err(|e| format!("failed to set mode 0700 on {}: {e}", dir.display()))?;
            return Ok(());
        }
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("failed to create control socket dir {}: {e}", dir.display()))?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("failed to set mode 0700 on {}: {e}", dir.display()))?;
        let meta = std::fs::symlink_metadata(dir)
            .map_err(|e| format!("stat control dir {}: {e}", dir.display()))?;
        if meta.file_type().is_symlink() || meta.uid() != my_uid {
            return Err(format!("unsafe control socket directory {}", dir.display()));
        }
        Ok(())
    }

    pub(super) async fn run_unix_server(
        sock_path: PathBuf,
        handle: Arc<ControlHandle>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<(), String> {
        let my_uid = current_process_uid().await?;
        let parent = sock_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or_else(|| {
                format!(
                    "control socket {} has no parent directory",
                    sock_path.display()
                )
            })?;
        ensure_private_socket_dir(parent, my_uid)?;

        if let Ok(meta) = std::fs::symlink_metadata(&sock_path) {
            if meta.file_type().is_symlink() {
                return Err(format!(
                    "refusing symlink at control socket path {}",
                    sock_path.display()
                ));
            }
            if !meta.file_type().is_socket() {
                return Err(format!(
                    "control socket path {} exists and is not a socket",
                    sock_path.display()
                ));
            }
            if meta.uid() != my_uid {
                return Err(format!(
                    "control socket {} is owned by uid {}, expected {my_uid}",
                    sock_path.display(),
                    meta.uid()
                ));
            }
            match tokio::time::timeout(Duration::from_millis(250), UnixStream::connect(&sock_path))
                .await
            {
                Ok(Ok(_)) => {
                    return Err(format!(
                        "another worker is already listening on {}",
                        sock_path.display()
                    ));
                }
                Ok(Err(_)) | Err(_) => {
                    std::fs::remove_file(&sock_path).map_err(|e| {
                        format!("failed to remove stale socket {}: {e}", sock_path.display())
                    })?;
                }
            }
        }

        let listener = UnixListener::bind(&sock_path)
            .map_err(|e| format!("failed to bind control socket {}: {e}", sock_path.display()))?;
        let _cleanup = SocketCleanup(sock_path.clone());
        std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("failed to set mode 0600 on {}: {e}", sock_path.display()))?;
        info!(path = %sock_path.display(), "Worker local control socket listening (mode 0600)");

        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((stream, _addr)) => {
                            let peer_ok = stream.peer_cred().is_ok_and(|c| c.uid() == my_uid);
                            if !peer_ok {
                                warn!("Rejected worker control connection from mismatched UID");
                                continue;
                            }
                            let h = Arc::clone(&handle);
                            tokio::spawn(async move {
                                process_control_stream(stream, h).await;
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "Worker control socket accept error");
                        }
                    }
                }
                res = shutdown_rx.changed() => {
                    if res.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn send_unix_request(
        sock_path: &Path,
        req: &ControlRequest,
    ) -> Result<ControlResponse, String> {
        let my_uid = current_process_uid().await?;
        let meta = std::fs::symlink_metadata(sock_path).map_err(|e| {
            format!(
                "cannot stat worker control socket {}: {e}",
                sock_path.display()
            )
        })?;
        if meta.file_type().is_symlink() {
            return Err(format!(
                "refusing symlinked worker control socket {}",
                sock_path.display()
            ));
        }
        if !meta.file_type().is_socket() {
            return Err(format!(
                "{} is not a Unix domain socket",
                sock_path.display()
            ));
        }
        if meta.uid() != my_uid {
            return Err(format!(
                "worker control socket {} is owned by uid {}, expected {my_uid}",
                sock_path.display(),
                meta.uid()
            ));
        }

        let stream = tokio::time::timeout(Duration::from_secs(5), UnixStream::connect(sock_path))
            .await
            .map_err(|_| format!("timed out connecting to {}", sock_path.display()))?
            .map_err(|e| format!("failed to connect to {}: {e}", sock_path.display()))?;
        let peer_cred = stream
            .peer_cred()
            .map_err(|e| format!("cannot verify control server peer_cred: {e}"))?;
        if peer_cred.uid() != my_uid {
            return Err(format!(
                "control socket server uid {} does not match caller uid {my_uid}",
                peer_cred.uid()
            ));
        }

        let mut payload = serde_json::to_string(req)
            .map_err(|e| format!("failed to serialize control request: {e}"))?;
        payload.push('\n');
        let (reader, mut writer) = tokio::io::split(stream);
        writer
            .write_all(payload.as_bytes())
            .await
            .map_err(|e| format!("failed writing control request: {e}"))?;
        writer
            .flush()
            .await
            .map_err(|e| format!("failed flushing control request: {e}"))?;

        let mut limited = BufReader::new(tokio::io::AsyncReadExt::take(
            reader,
            (MAX_CONTROL_LINE_BYTES + 1) as u64,
        ));
        let mut resp_line = String::new();
        let read = tokio::time::timeout(Duration::from_secs(5), limited.read_line(&mut resp_line))
            .await
            .map_err(|_| "timed out waiting for worker control response".to_string())?
            .map_err(|e| format!("failed reading worker control response: {e}"))?;
        if read == 0 {
            return Err("worker control server closed connection without responding".to_string());
        }
        if read > MAX_CONTROL_LINE_BYTES {
            return Err("worker control response exceeded size limit".to_string());
        }
        serde_json::from_str::<ControlResponse>(resp_line.trim())
            .map_err(|e| format!("invalid worker control response: {e}"))
    }
}

#[cfg(windows)]
// LV-4: cfg(windows) named-pipe same-user DACL restricting access to owner (`OW`).
#[allow(unsafe_code)]
mod windows_ipc {
    use super::*;
    use tokio::net::windows::named_pipe::{ClientOptions, PipeMode, ServerOptions};
    use windows_sys::Win32::Foundation::{LocalFree, HLOCAL};
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES};

    struct LocalSdGuard(PSECURITY_DESCRIPTOR);
    impl Drop for LocalSdGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: `self.0` was allocated by `ConvertStringSecurityDescriptorToSecurityDescriptorW`.
                unsafe {
                    LocalFree(self.0 as HLOCAL);
                }
            }
        }
    }

    fn create_private_pipe_server(
        pipe_name: &str,
        first: bool,
    ) -> Result<tokio::net::windows::named_pipe::NamedPipeServer, String> {
        // Owner (`OW`) gets Generic All under a Protected DACL (`D:P`).
        let sddl: Vec<u16> = "D:P(A;;GA;;;OW)\0".encode_utf16().collect();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: `sddl` is NUL-terminated UTF-16 and `sd` receives the allocated descriptor.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut sd,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 || sd.is_null() {
            return Err(format!(
                "failed to build named-pipe DACL: {}",
                std::io::Error::last_os_error()
            ));
        }
        let _guard = LocalSdGuard(sd);
        let mut sa = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: sd,
            bInheritHandle: 0,
        };
        let mut opts = ServerOptions::new();
        opts.first_pipe_instance(first)
            .pipe_mode(PipeMode::Byte)
            .reject_remote_clients(true);
        // SAFETY: `sa` points to a valid `SECURITY_ATTRIBUTES` + `sd` for the duration of this call.
        unsafe {
            opts.create_with_security_attributes_raw(
                pipe_name,
                (&mut sa as *mut SECURITY_ATTRIBUTES).cast(),
            )
        }
        .map_err(|e| format!("failed to create named pipe {pipe_name}: {e}"))
    }

    pub(super) async fn run_windows_server(
        pipe_name: String,
        handle: Arc<ControlHandle>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<(), String> {
        let mut server = create_private_pipe_server(&pipe_name, true)?;
        info!(pipe = %pipe_name, "Worker local named-pipe control server listening");
        loop {
            tokio::select! {
                res = server.connect() => {
                    match res {
                        Ok(()) => {
                            let connected = server;
                            server = create_private_pipe_server(&pipe_name, false)?;
                            let h = Arc::clone(&handle);
                            tokio::spawn(async move {
                                process_control_stream(connected, h).await;
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "Named-pipe control accept error");
                        }
                    }
                }
                res = shutdown_rx.changed() => {
                    if res.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn send_windows_request(
        pipe_name: &str,
        req: &ControlRequest,
    ) -> Result<ControlResponse, String> {
        let client = ClientOptions::new()
            .open(pipe_name)
            .map_err(|e| format!("failed to open control pipe {pipe_name}: {e}"))?;
        let mut payload = serde_json::to_string(req)
            .map_err(|e| format!("failed to serialize control request: {e}"))?;
        payload.push('\n');
        let (reader, mut writer) = tokio::io::split(client);
        writer
            .write_all(payload.as_bytes())
            .await
            .map_err(|e| format!("failed writing control request: {e}"))?;
        writer
            .flush()
            .await
            .map_err(|e| format!("failed flushing control request: {e}"))?;
        let mut limited = BufReader::new(tokio::io::AsyncReadExt::take(
            reader,
            (MAX_CONTROL_LINE_BYTES + 1) as u64,
        ));
        let mut resp_line = String::new();
        let read = tokio::time::timeout(Duration::from_secs(5), limited.read_line(&mut resp_line))
            .await
            .map_err(|_| "timed out waiting for worker control response".to_string())?
            .map_err(|e| format!("failed reading worker control response: {e}"))?;
        if read == 0 {
            return Err("worker control server closed connection without responding".to_string());
        }
        serde_json::from_str::<ControlResponse>(resp_line.trim())
            .map_err(|e| format!("invalid worker control response: {e}"))
    }
}

/// Runs the local worker control server until `shutdown_rx` becomes `true`.
pub async fn serve_control_endpoint(
    endpoint: ControlEndpoint,
    handle: Arc<ControlHandle>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), String> {
    match endpoint {
        #[cfg(unix)]
        ControlEndpoint::UnixSocket(path) => {
            unix_ipc::run_unix_server(path, handle, shutdown_rx).await
        }
        #[cfg(windows)]
        ControlEndpoint::NamedPipe(name) => {
            windows_ipc::run_windows_server(name, handle, shutdown_rx).await
        }
    }
}

/// Sends one control request to a running worker over its local endpoint.
pub async fn send_control_request(
    endpoint: &ControlEndpoint,
    req: &ControlRequest,
) -> Result<ControlResponse, String> {
    match endpoint {
        #[cfg(unix)]
        ControlEndpoint::UnixSocket(path) => unix_ipc::send_unix_request(path, req).await,
        #[cfg(windows)]
        ControlEndpoint::NamedPipe(name) => windows_ipc::send_windows_request(name, req).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_handle(
        limits: WorkerLimitsConfig,
        hook: Option<LimitPersistHook>,
    ) -> Arc<ControlHandle> {
        ControlHandle::new(
            Uuid::new_v4(),
            "test-worker",
            Arc::new(AtomicUsize::new(0)),
            limits,
            None,
            hook,
        )
    }

    #[test]
    fn control_handle_pause_resume_and_gpu_only_gate() {
        let initial = WorkerLimitsConfig {
            no_gpu_tasks: true,
            ..Default::default()
        };
        let handle = test_handle(initial, None);
        let mut rx = handle.subscribe();

        assert_eq!(handle.check_assignment(false), AssignmentDecision::Allow);
        assert_eq!(
            handle.check_assignment(true),
            AssignmentDecision::RejectNoGpuTasks
        );

        let snap = handle.pause(None, Some("gaming".into())).unwrap();
        assert_eq!(snap.state, WorkerControlState::Paused);
        assert_eq!(snap.reason.as_deref(), Some("gaming"));
        assert_eq!(
            handle.check_assignment(false),
            AssignmentDecision::RejectPaused("gaming".into())
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            ControlEvent::PauseHandBack {
                reason: "gaming".into()
            }
        );

        let resumed = handle.resume();
        assert_eq!(resumed.state, WorkerControlState::Running);
        assert_eq!(rx.try_recv().unwrap(), ControlEvent::ResumeReconnect);
        assert_eq!(handle.check_assignment(false), AssignmentDecision::Allow);
        assert_eq!(
            handle.check_assignment(true),
            AssignmentDecision::RejectNoGpuTasks
        );
    }

    #[test]
    fn limits_set_validates_and_rolls_back_on_persist_error() {
        let hook = LimitPersistHook(Arc::new(|_| Err("disk full".to_string())));
        let handle = test_handle(WorkerLimitsConfig::default(), Some(hook));
        let err = handle.set_limit("no_gpu_tasks", "true").unwrap_err();
        assert!(err.contains("disk full"));
        assert!(
            !handle.no_gpu_tasks(),
            "state must roll back on persist error"
        );

        let ok_handle = test_handle(WorkerLimitsConfig::default(), None);
        assert!(ok_handle.set_limit("local-max-cpu-pct", "150").is_err());
        ok_handle.set_limit("local-max-cpu-pct", "50").unwrap();
        assert_eq!(ok_handle.snapshot().limits.local_max_cpu_pct, Some(50));
        ok_handle.set_limit("no_gpu_tasks", "true").unwrap();
        assert!(ok_handle.no_gpu_tasks());
        ok_handle.set_limit("hours", "18:00-08:00").unwrap();
        assert_eq!(
            ok_handle.snapshot().limits.hours.as_deref(),
            Some("18:00-08:00")
        );
    }

    #[test]
    fn parse_pause_until_handles_relative_and_past() {
        let now = Utc::now();
        let ms = parse_pause_until("2h", now).unwrap();
        let diff_ms = ms as i64 - now.timestamp_millis();
        assert!(
            (7_199_000..=7_201_000).contains(&diff_ms),
            "unexpected diff: {diff_ms}"
        );
        assert!(parse_pause_until("0m", now).is_err());
        assert!(parse_pause_until("2020-01-01T00:00:00Z", now).is_err());
        assert!(parse_pause_until("not-a-time", now).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_control_socket_round_trip_and_security_checks() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let sock_dir = dir.path().join("priv");
        let sock = sock_dir.join("worker.control.sock");
        let handle = test_handle(WorkerLimitsConfig::default(), None);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let server_handle = Arc::clone(&handle);
        let server_sock = sock.clone();
        let srv = tokio::spawn(async move {
            serve_control_endpoint(
                ControlEndpoint::UnixSocket(server_sock),
                server_handle,
                shutdown_rx,
            )
            .await
        });

        // Wait briefly for listener creation (bounded synchronization, not timing assertion).
        for _ in 0..50 {
            if sock.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(sock.exists(), "socket must exist");
        let dir_mode = std::fs::metadata(&sock_dir).unwrap().permissions().mode() & 0o777;
        let sock_mode = std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700);
        assert_eq!(sock_mode, 0o600);

        let ep = ControlEndpoint::UnixSocket(sock.clone());
        let resp = send_control_request(&ep, &ControlRequest::Status)
            .await
            .unwrap();
        match resp {
            ControlResponse::Status { snapshot } => {
                assert_eq!(snapshot.state, WorkerControlState::Running);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        let resp = send_control_request(
            &ep,
            &ControlRequest::Pause {
                until_epoch_ms: None,
                reason: Some("call".into()),
            },
        )
        .await
        .unwrap();
        match resp {
            ControlResponse::Ok { snapshot, .. } => {
                assert_eq!(snapshot.state, WorkerControlState::Paused);
                assert_eq!(snapshot.reason.as_deref(), Some("call"));
            }
            other => panic!("unexpected response: {other:?}"),
        }

        let resp = send_control_request(
            &ep,
            &ControlRequest::LimitsSet {
                key: "no-gpu-tasks".into(),
                value: "true".into(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(resp, ControlResponse::Ok { .. }));
        assert!(handle.no_gpu_tasks());

        let resp = send_control_request(&ep, &ControlRequest::Resume)
            .await
            .unwrap();
        match resp {
            ControlResponse::Ok { snapshot, .. } => {
                assert_eq!(snapshot.state, WorkerControlState::Running);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        // Symlink refusal check.
        let link = sock_dir.join("evil.link.sock");
        std::os::unix::fs::symlink(&sock, &link).unwrap();
        let err = send_control_request(&ControlEndpoint::UnixSocket(link), &ControlRequest::Status)
            .await
            .unwrap_err();
        assert!(err.contains("symlink"), "unexpected error: {err}");

        let _ = shutdown_tx.send(true);
        srv.await.unwrap().unwrap();
        assert!(!sock.exists(), "socket must be removed cleanly on shutdown");
    }

    #[cfg(unix)]
    #[test]
    fn default_unix_socket_path_honours_oxideswarm_home_over_xdg_and_home() {
        use std::ffi::OsStr;

        assert_eq!(
            default_unix_socket_path_with(
                Some("box1"),
                Some(OsStr::new("/custom/ox")),
                Some(OsStr::new("/run/user/1000")),
                Some(OsStr::new("/home/alice")),
            ),
            Some(PathBuf::from("/custom/ox/box1.control.sock"))
        );
        assert_eq!(
            default_unix_socket_path_with(
                Some("box1"),
                Some(OsStr::new("")),
                Some(OsStr::new("/run/user/1000")),
                Some(OsStr::new("/home/alice")),
            ),
            Some(PathBuf::from("/run/user/1000/oxideswarm/box1.control.sock"))
        );
        assert_eq!(
            default_unix_socket_path_with(
                None,
                None,
                Some(OsStr::new("")),
                Some(OsStr::new("/home/alice")),
            ),
            Some(PathBuf::from("/home/alice/.oxideswarm/control.sock"))
        );
        assert_eq!(default_unix_socket_path_with(None, None, None, None), None);
    }
}
