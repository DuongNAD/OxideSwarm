//! Local same-user control channel and contributor pause/limits state (`design 0113` D2/D3).
//!
//! Each running worker listens on a private local endpoint (`control.sock` with mode `0600` inside
//! a `0700` directory on Unix, or `\\.\pipe\oxideswarm-<user>-<name>` on Windows) and accepts
//! bounded JSON-line commands (`status`, `pause`, `resume`, `limits get`, `limits set`). Peers are
//! verified to belong to the same OS user before any command runs.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Local, NaiveTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{broadcast, watch, Semaphore};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Hard cap on a single JSON-line control request or response (8 KiB).
pub const MAX_CONTROL_LINE_BYTES: usize = 8 * 1024;

/// Maximum concurrent local control IPC connections handled simultaneously.
const MAX_CONCURRENT_CONTROL_CONNS: usize = 16;

/// Timeout for reading one JSON-line request on an accepted control connection.
const CONTROL_STREAM_IO_TIMEOUT: Duration = Duration::from_secs(5);

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
    #[serde(default)]
    pub hold_ends_tasks: bool,
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
            "hold_ends_tasks" => {
                self.hold_ends_tasks = parse_bool_flag(val, "hold_ends_tasks")?;
            }
            _ => {
                return Err(format!(
                    "unknown worker limit key '{key}' (expected one of: local_max_cpu_pct, hours, max_task_hours_per_day, no_tasks_on_battery, no_tasks_on_metered, no_gpu_tasks, hold_ends_tasks)"
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
    crate::limits::HoursSchedule::parse(val).map(|_| ())
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub paused_until_epoch_ms: Option<u64>,
    pub no_gpu_tasks: bool,
    pub active_tasks: usize,
    pub limits: WorkerLimitsConfig,
    pub config_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_server_error: Option<String>,
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

/// Default Unix control socket path (`$XDG_RUNTIME_DIR/oxideswarm/...`, `$OXIDESWARM_HOME/...`,
/// or `$HOME/.oxideswarm/...`, per `design 0113` D2.4).
#[cfg(unix)]
pub fn default_unix_socket_path(worker_name: Option<&str>) -> Option<PathBuf> {
    default_unix_socket_path_with(
        worker_name,
        std::env::var_os("XDG_RUNTIME_DIR").as_deref(),
        std::env::var_os("OXIDESWARM_HOME").as_deref(),
        std::env::var_os("HOME").as_deref(),
    )
}

#[cfg(unix)]
pub(crate) fn default_unix_socket_path_with(
    worker_name: Option<&str>,
    xdg_runtime_dir: Option<&std::ffi::OsStr>,
    oxideswarm_home: Option<&std::ffi::OsStr>,
    home: Option<&std::ffi::OsStr>,
) -> Option<PathBuf> {
    let dir = if let Some(xdg) = xdg_runtime_dir.filter(|v| !v.is_empty()) {
        PathBuf::from(xdg).join("oxideswarm")
    } else if let Some(ox_home) = oxideswarm_home.filter(|v| !v.is_empty()) {
        PathBuf::from(ox_home)
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
    LimitsUpdated,
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
    hold_detail: Option<String>,
    hold_hand_back_running: bool,
    limits: WorkerLimitsConfig,
}

/// Thread-safe handle shared between the worker execution engine and the local control server.
pub struct ControlHandle {
    worker_id: Uuid,
    worker_name: String,
    active_tasks: Arc<AtomicUsize>,
    config_path: Mutex<Option<PathBuf>>,
    persist_hook: Option<LimitPersistHook>,
    control_server_error: Mutex<Option<String>>,
    limit_update_lock: Mutex<()>,
    inner: Mutex<ControlStateInner>,
    event_tx: broadcast::Sender<ControlEvent>,
    next_rejection_seq: AtomicU64,
    session_pause_rejection_gen: AtomicU64,
    session_gpu_pause_rejection_gen: AtomicU64,
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
            control_server_error: Mutex::new(None),
            limit_update_lock: Mutex::new(()),
            inner: Mutex::new(ControlStateInner {
                paused: false,
                pause_reason: None,
                paused_until: None,
                paused_until_epoch_ms: None,
                held: false,
                hold_reason: None,
                hold_detail: None,
                hold_hand_back_running: false,
                limits,
            }),
            event_tx,
            next_rejection_seq: AtomicU64::new(1),
            session_pause_rejection_gen: AtomicU64::new(0),
            session_gpu_pause_rejection_gen: AtomicU64::new(0),
        })
    }

    /// Records or clears a startup/runtime error from the local control server listener.
    pub fn set_control_server_error(&self, err: Option<String>) {
        *self
            .control_server_error
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = err;
    }

    /// Returns any recorded control server failure message.
    pub fn control_server_error(&self) -> Option<String> {
        self.control_server_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Returns the active hand-back reason if the worker is currently paused (or held with
    /// running task hand-back) when a session starts or restarts.
    pub fn active_pause_handback_reason(&self) -> Option<String> {
        let _ = self.check_pause_expiry();
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.paused {
            Some(
                inner
                    .pause_reason
                    .clone()
                    .unwrap_or_else(|| "contributor paused worker".to_string()),
            )
        } else if inner.held && inner.hold_hand_back_running {
            Some(
                inner
                    .hold_reason
                    .clone()
                    .unwrap_or_else(|| "local contributor limit active".to_string()),
            )
        } else {
            None
        }
    }

    /// Subscribes to pause/resume transitions for the active session loop.
    pub fn subscribe(&self) -> broadcast::Receiver<ControlEvent> {
        self.event_tx.subscribe()
    }

    /// Records that the current master session received a full-worker pause or hold refusal.
    pub fn mark_session_pause_rejection(&self) {
        let gen_id = self.next_rejection_seq.fetch_add(1, Ordering::AcqRel);
        self.session_pause_rejection_gen
            .store(gen_id, Ordering::Release);
    }

    /// Records that the current master session received a `--no-gpu-tasks` refusal.
    pub fn mark_session_gpu_pause_rejection(&self) {
        let gen_id = self.next_rejection_seq.fetch_add(1, Ordering::AcqRel);
        self.session_gpu_pause_rejection_gen
            .store(gen_id, Ordering::Release);
    }

    /// Captures the current full-pause and GPU-pause rejection generations before sending a result.
    pub fn pause_rejection_snapshot(&self) -> (u64, u64) {
        (
            self.session_pause_rejection_gen.load(Ordering::Acquire),
            self.session_gpu_pause_rejection_gen.load(Ordering::Acquire),
        )
    }

    /// Clears holdback tracking when a completed task result resets Master's hold-back timer.
    pub fn clear_session_pause_rejection_on_completed_result(
        &self,
        snapshot: (u64, u64),
        task_requires_gpu: bool,
    ) {
        if snapshot.0 != 0 {
            let _ = self.session_pause_rejection_gen.compare_exchange(
                snapshot.0,
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        if task_requires_gpu && snapshot.1 != 0 {
            let _ = self.session_gpu_pause_rejection_gen.compare_exchange(
                snapshot.1,
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    /// Resets the per-session pause refusal tracker when a fresh master session connects.
    pub fn clear_session_pause_rejection(&self) {
        self.session_pause_rejection_gen.store(0, Ordering::Release);
        self.session_gpu_pause_rejection_gen
            .store(0, Ordering::Release);
    }

    /// Returns and clears whether the current session sent a full-worker pause refusal.
    pub fn take_session_pause_rejection(&self) -> bool {
        self.session_pause_rejection_gen.swap(0, Ordering::AcqRel) != 0
    }

    /// Returns and clears whether the current session should reconnect now that local holds cleared.
    pub fn take_session_reconnect_needed(&self) -> bool {
        if self.is_paused_or_held() {
            return false;
        }
        let full = self.session_pause_rejection_gen.swap(0, Ordering::AcqRel) != 0;
        let gpu = !self.no_gpu_tasks()
            && self
                .session_gpu_pause_rejection_gen
                .swap(0, Ordering::AcqRel)
                != 0;
        full || gpu
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
        let (state, reason, detail) = if inner.paused {
            (
                WorkerControlState::Paused,
                inner
                    .pause_reason
                    .clone()
                    .or_else(|| Some("contributor paused worker".to_string())),
                None,
            )
        } else if inner.held {
            (
                WorkerControlState::Held,
                inner
                    .hold_reason
                    .clone()
                    .or_else(|| Some("local contributor limit active".to_string())),
                inner.hold_detail.clone(),
            )
        } else {
            (WorkerControlState::Running, None, None)
        };
        let config_path = self
            .config_path
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|p| p.display().to_string());
        let control_server_error = self
            .control_server_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        ControlStatusSnapshot {
            worker_id: self.worker_id,
            worker_name: self.worker_name.clone(),
            state,
            reason,
            detail,
            paused_until_epoch_ms: inner.paused_until_epoch_ms,
            no_gpu_tasks: inner.limits.no_gpu_tasks,
            active_tasks: self.active_tasks.load(Ordering::Acquire),
            limits: inner.limits.clone(),
            config_path,
            control_server_error,
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
        let (was_paused, still_held, snapshot) = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let was_paused = inner.paused;
            inner.paused = false;
            inner.pause_reason = None;
            inner.paused_until = None;
            inner.paused_until_epoch_ms = None;
            (was_paused, inner.held, self.build_snapshot(&inner))
        };
        if !still_held
            && (was_paused || self.session_pause_rejection_gen.load(Ordering::Acquire) != 0)
        {
            let _ = self.event_tx.send(ControlEvent::ResumeReconnect);
        }
        snapshot
    }

    /// Sets or clears automatic hold state (for D1 limit enforcement; `NET-9b`).
    pub fn set_held(&self, held: bool, reason: Option<String>, hand_back_running: bool) {
        self.set_held_with_detail(held, reason, None, hand_back_running);
    }

    /// Sets or clears automatic hold state alongside a local-only explanation string.
    pub fn set_held_with_detail(
        &self,
        held: bool,
        reason: Option<String>,
        detail: Option<String>,
        hand_back_running: bool,
    ) {
        let mut trigger_handback = None;
        let mut trigger_resume = false;
        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let was_held = inner.held;
            inner.held = held;
            inner.hold_hand_back_running = held && hand_back_running;
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
            inner.hold_detail = if held {
                detail
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            } else {
                None
            };
            if held && !was_held && !inner.paused && hand_back_running {
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

    /// Returns `true` when either manual pause or an automatic contributor limit hold is active.
    pub fn is_paused_or_held(&self) -> bool {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.paused || inner.held
    }

    /// Returns a cloned snapshot of the current contributor limits configuration.
    pub fn limits(&self) -> WorkerLimitsConfig {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .limits
            .clone()
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
        let _serialize = self
            .limit_update_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner());
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
        let _ = self.event_tx.send(ControlEvent::LimitsUpdated);
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
    let resp = match tokio::time::timeout(CONTROL_STREAM_IO_TIMEOUT, limited.read_line(&mut line))
        .await
    {
        Err(_) => {
            debug!("Timed out reading local control stream");
            return;
        }
        Ok(Ok(0)) => return,
        Ok(Ok(n)) if n > MAX_CONTROL_LINE_BYTES || !line.ends_with('\n') => {
            ControlResponse::Error {
                message: format!("control request exceeded {MAX_CONTROL_LINE_BYTES} byte limit"),
            }
        }
        Ok(Ok(_)) => match serde_json::from_str::<ControlRequest>(line.trim()) {
            Ok(req) => handle.handle_request(req),
            Err(e) => ControlResponse::Error {
                message: format!("invalid control request: {e}"),
            },
        },
        Ok(Err(e)) => {
            debug!(error = %e, "Failed reading local control stream");
            return;
        }
    };
    if let Ok(mut encoded) = serde_json::to_string(&resp) {
        encoded.push('\n');
        let _ = tokio::time::timeout(CONTROL_STREAM_IO_TIMEOUT, async {
            let _ = writer.write_all(encoded.as_bytes()).await;
            let _ = writer.flush().await;
        })
        .await;
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

    pub(super) fn verify_peer_uid(expected_uid: u32, actual_uid: u32) -> Result<(), String> {
        if actual_uid != expected_uid {
            return Err(format!(
                "control socket peer uid {actual_uid} does not match expected uid {expected_uid}"
            ));
        }
        Ok(())
    }

    pub(super) async fn current_process_uid() -> Result<u32, String> {
        let (a, _b) = UnixStream::pair().map_err(|e| format!("unix socketpair failed: {e}"))?;
        let cred = a
            .peer_cred()
            .map_err(|e| format!("peer_cred query failed: {e}"))?;
        Ok(cred.uid())
    }

    fn is_default_oxideswarm_socket_dir(dir: &Path) -> bool {
        let mut candidates = Vec::new();
        if let Some(xdg) = std::env::var_os("XDG_RUNTIME_DIR").filter(|v| !v.is_empty()) {
            candidates.push(PathBuf::from(xdg).join("oxideswarm"));
        }
        if let Some(ox_home) = std::env::var_os("OXIDESWARM_HOME").filter(|v| !v.is_empty()) {
            candidates.push(PathBuf::from(ox_home));
        }
        if let Some(home) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
            candidates.push(PathBuf::from(home).join(".oxideswarm"));
        }
        candidates.into_iter().any(|c| c == dir)
    }

    pub(super) fn ensure_private_socket_dir(dir: &Path, my_uid: u32) -> Result<(), String> {
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
            let mode = meta.mode() & 0o777;
            if (mode & 0o077) != 0 {
                if is_default_oxideswarm_socket_dir(dir) {
                    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(
                        |e| format!("failed to set mode 0700 on {}: {e}", dir.display()),
                    )?;
                } else {
                    return Err(format!(
                        "control socket directory {} has mode {:04o}; expected 0700 (refusing to chmod custom parent directory)",
                        dir.display(),
                        mode
                    ));
                }
            }
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

        let conn_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_CONTROL_CONNS));
        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((stream, _addr)) => {
                            let peer_ok = stream
                                .peer_cred()
                                .is_ok_and(|c| verify_peer_uid(my_uid, c.uid()).is_ok());
                            if !peer_ok {
                                warn!("Rejected worker control connection from mismatched UID");
                                continue;
                            }
                            let Ok(permit) = Arc::clone(&conn_sem).try_acquire_owned() else {
                                debug!("Dropping excess concurrent worker control connection");
                                continue;
                            };
                            let h = Arc::clone(&handle);
                            tokio::spawn(async move {
                                let _permit = permit;
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
        verify_peer_uid(my_uid, peer_cred.uid())?;

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
// LV-4: cfg(windows) named-pipe same-user DACL restricting access to owner (`OW`) and client-side
// verification that the connected pipe server runs under the caller's own user SID.
#[allow(unsafe_code)]
mod windows_ipc {
    use super::*;
    use std::os::windows::io::AsRawHandle;
    use tokio::net::windows::named_pipe::{
        ClientOptions, NamedPipeClient, NamedPipeServer, PipeMode, ServerOptions,
    };
    use windows_sys::Win32::Foundation::{CloseHandle, LocalFree, HANDLE, HLOCAL};
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{
        EqualSid, GetLengthSid, GetTokenInformation, IsValidSid, TokenUser, PSECURITY_DESCRIPTOR,
        SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    pub(super) const WIN_ERROR_PIPE_BUSY: i32 = 231;

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

    struct HandleGuard(HANDLE);
    impl Drop for HandleGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: `self.0` is an owned Win32 kernel handle opened by `OpenProcess` or `OpenProcessToken`.
                unsafe {
                    let _ = CloseHandle(self.0);
                }
            }
        }
    }

    pub(super) fn process_user_sid(process_handle: HANDLE) -> Result<Vec<u8>, String> {
        let mut token: HANDLE = std::ptr::null_mut();
        // SAFETY: `process_handle` is a valid process handle and `&mut token` receives an owned token handle.
        if unsafe { OpenProcessToken(process_handle, TOKEN_QUERY, &mut token) } == 0 {
            return Err(format!(
                "OpenProcessToken failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        let _token_guard = HandleGuard(token);

        let mut needed: u32 = 0;
        // SAFETY: `token` is valid; null/0 performs the documented Win32 length probe for `TokenUser`.
        unsafe {
            let _ = GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut needed);
        }
        if needed == 0 {
            return Err(format!(
                "GetTokenInformation size probe failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        let words = (needed as usize)
            .div_ceil(std::mem::size_of::<usize>())
            .max(1);
        let mut aligned = vec![0usize; words];
        let buf_len = (words * std::mem::size_of::<usize>()) as u32;
        // SAFETY: `aligned` provides `buf_len` writable `usize`-aligned bytes for `TOKEN_USER` + SID.
        let ok = unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                aligned.as_mut_ptr().cast(),
                buf_len,
                &mut needed,
            )
        };
        if ok == 0 {
            return Err(format!(
                "GetTokenInformation(TokenUser) failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: `GetTokenInformation` succeeded on a `usize`-aligned buffer at least `size_of::<TOKEN_USER>()` large.
        let token_user = unsafe { &*(aligned.as_ptr() as *const TOKEN_USER) };
        let psid = token_user.User.Sid;
        // SAFETY: checking pointer nullness before reading validated SID structure.
        if psid.is_null() || unsafe { IsValidSid(psid) } == 0 {
            return Err("process token returned an invalid Windows SID".to_string());
        }
        // SAFETY: `IsValidSid(psid)` succeeded above.
        let len = unsafe { GetLengthSid(psid) } as usize;
        // SAFETY: `psid` is a valid SID allocation inside `aligned` of length `len`.
        let slice = unsafe { std::slice::from_raw_parts(psid as *const u8, len) };
        Ok(slice.to_vec())
    }

    pub(super) fn verify_sids_equal(expected_sid: &[u8], server_sid: &[u8]) -> Result<(), String> {
        fn aligned_sid_buf(bytes: &[u8]) -> Result<Vec<u32>, String> {
            if bytes.is_empty() {
                return Err("invalid Windows SID encountered during pipe owner check".to_string());
            }
            let words = bytes.len().div_ceil(std::mem::size_of::<u32>());
            let mut aligned = vec![0u32; words];
            // SAFETY: `aligned` is newly allocated with `words * 4 >= bytes.len()` writable bytes.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    aligned.as_mut_ptr().cast(),
                    bytes.len(),
                );
            }
            let psid = aligned.as_mut_ptr().cast();
            // SAFETY: `psid` is 4-byte aligned and holds all `bytes.len()` bytes; `IsValidSid`
            // validates the header before `GetLengthSid` inspects the subauthority count.
            let valid =
                unsafe { IsValidSid(psid) != 0 && GetLengthSid(psid) as usize == bytes.len() };
            if !valid {
                return Err("invalid Windows SID encountered during pipe owner check".to_string());
            }
            Ok(aligned)
        }

        let mut expected_aligned = aligned_sid_buf(expected_sid)?;
        let mut server_aligned = aligned_sid_buf(server_sid)?;
        // SAFETY: both aligned buffers passed `IsValidSid` and exact length verification above.
        let equal = unsafe {
            EqualSid(
                expected_aligned.as_mut_ptr().cast(),
                server_aligned.as_mut_ptr().cast(),
            )
        };
        if equal == 0 {
            return Err(
                "named-pipe server SID does not match caller user SID (refusing impostor pipe)"
                    .to_string(),
            );
        }
        Ok(())
    }

    pub(super) fn verify_pipe_server_same_user(
        client: &NamedPipeClient,
        pipe_name: &str,
    ) -> Result<(), String> {
        let mut server_pid: u32 = 0;
        let handle = client.as_raw_handle() as HANDLE;
        // SAFETY: `handle` is an open connected named-pipe handle and `&mut server_pid` receives the PID.
        let ok = unsafe { GetNamedPipeServerProcessId(handle, &mut server_pid) };
        if ok == 0 || server_pid == 0 {
            return Err(format!(
                "cannot verify owner of control pipe {pipe_name}: GetNamedPipeServerProcessId failed ({})",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: `GetCurrentProcess()` returns the current process pseudo-handle (no closing needed).
        let caller_sid = process_user_sid(unsafe { GetCurrentProcess() })?;
        // SAFETY: `server_pid` is non-zero and closed immediately via `HandleGuard`.
        let server_proc = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, server_pid) };
        if server_proc.is_null() {
            return Err(format!(
                "refusing control pipe {pipe_name}: cannot query server pid {server_pid} ({})",
                std::io::Error::last_os_error()
            ));
        }
        let _proc_guard = HandleGuard(server_proc);
        let server_sid = process_user_sid(server_proc).map_err(|e| {
            format!(
                "refusing control pipe {pipe_name}: server pid {server_pid} SID check failed ({e})"
            )
        })?;
        verify_sids_equal(&caller_sid, &server_sid)
    }

    pub(super) fn create_private_pipe_server(
        pipe_name: &str,
        first: bool,
    ) -> Result<NamedPipeServer, String> {
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

    async fn next_pipe_server(
        pipe_name: &str,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> Result<Option<NamedPipeServer>, String> {
        let mut backoff = Duration::from_millis(50);
        for attempt in 1..=8u8 {
            match create_private_pipe_server(pipe_name, false) {
                Ok(srv) => return Ok(Some(srv)),
                Err(e) => {
                    warn!(attempt, error = %e, "Failed creating next named-pipe instance; retrying");
                    if attempt == 8 {
                        return Err(e);
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        res = shutdown_rx.changed() => {
                            if res.is_err() || *shutdown_rx.borrow() {
                                return Ok(None);
                            }
                        }
                    }
                    backoff = (backoff * 2).min(Duration::from_millis(500));
                }
            }
        }
        unreachable!()
    }

    pub(super) async fn run_windows_server(
        pipe_name: String,
        handle: Arc<ControlHandle>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<(), String> {
        let mut server = create_private_pipe_server(&pipe_name, true)?;
        info!(pipe = %pipe_name, "Worker local named-pipe control server listening");
        let conn_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_CONTROL_CONNS));
        loop {
            tokio::select! {
                res = server.connect() => {
                    match res {
                        Ok(()) => {
                            let connected = server;
                            let Some(next) = next_pipe_server(&pipe_name, &mut shutdown_rx).await? else {
                                break;
                            };
                            server = next;
                            let Ok(permit) = Arc::clone(&conn_sem).try_acquire_owned() else {
                                debug!("Dropping excess concurrent named-pipe control connection");
                                continue;
                            };
                            let h = Arc::clone(&handle);
                            tokio::spawn(async move {
                                let _permit = permit;
                                process_control_stream(connected, h).await;
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "Named-pipe control accept error; recycling pipe instance");
                            let _ = server.disconnect();
                            let Some(next) = next_pipe_server(&pipe_name, &mut shutdown_rx).await? else {
                                break;
                            };
                            server = next;
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
        let busy_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let client = loop {
            match ClientOptions::new().open(pipe_name) {
                Ok(c) => break c,
                Err(e)
                    if e.raw_os_error() == Some(WIN_ERROR_PIPE_BUSY)
                        && tokio::time::Instant::now() < busy_deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => {
                    return Err(format!("failed to open control pipe {pipe_name}: {e}"));
                }
            }
        };
        verify_pipe_server_same_user(&client, pipe_name)?;
        let mut payload = serde_json::to_string(req)
            .map_err(|e| format!("failed to serialize control request: {e}"))?;
        payload.push('\n');
        let (reader, mut writer) = tokio::io::split(client);
        tokio::time::timeout(
            CONTROL_STREAM_IO_TIMEOUT,
            writer.write_all(payload.as_bytes()),
        )
        .await
        .map_err(|_| "timed out writing worker control request".to_string())?
        .map_err(|e| format!("failed writing control request: {e}"))?;
        tokio::time::timeout(CONTROL_STREAM_IO_TIMEOUT, writer.flush())
            .await
            .map_err(|_| "timed out flushing worker control request".to_string())?
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

    #[test]
    fn resume_while_held_skips_reconnect_and_limits_set_emits_updated() {
        let handle = ControlHandle::new(
            Uuid::new_v4(),
            "held-box",
            Arc::new(AtomicUsize::new(0)),
            WorkerLimitsConfig::default(),
            None,
            None,
        );
        let mut rx = handle.subscribe();
        handle.set_held_with_detail(
            true,
            Some("on battery".into()),
            Some("running on battery power".into()),
            false,
        );
        handle.mark_session_pause_rejection();
        let paused = handle.pause(None, Some("break".into())).unwrap();
        assert_eq!(paused.state, WorkerControlState::Paused);
        let _ = rx.try_recv(); // PauseHandBack

        // Manual resume while held keeps Held state and must not emit ResumeReconnect.
        let resumed = handle.resume();
        assert_eq!(resumed.state, WorkerControlState::Held);
        assert_eq!(resumed.reason.as_deref(), Some("on battery"));
        assert_eq!(resumed.detail.as_deref(), Some("running on battery power"));
        assert!(rx.try_recv().is_err());

        // `limits set` emits LimitsUpdated and supports D1 weekday schedule syntax.
        handle
            .set_limit("hours", "mon-fri 22:00-07:00; sat-sun 00:00-24:00")
            .unwrap();
        assert_eq!(rx.try_recv().unwrap(), ControlEvent::LimitsUpdated);
        assert_eq!(
            handle.limits().hours.as_deref(),
            Some("mon-fri 22:00-07:00; sat-sun 00:00-24:00")
        );
        assert!(handle.set_limit("hours", "10:00-10:00").is_err());
    }

    #[test]
    fn session_pause_rejection_clears_on_completed_result_and_respects_gpu_separation() {
        let handle = ControlHandle::new(
            Uuid::new_v4(),
            "race-box",
            Arc::new(AtomicUsize::new(0)),
            WorkerLimitsConfig::default(),
            None,
            None,
        );

        // 1. Full pause rejection + completed CPU result CAS clears it cleanly.
        handle.mark_session_pause_rejection();
        let snap = handle.pause_rejection_snapshot();
        handle.clear_session_pause_rejection_on_completed_result(snap, false);
        assert!(!handle.take_session_reconnect_needed());

        // 2. Stale snapshot CAS must NOT clear a newer pause rejection.
        handle.mark_session_pause_rejection();
        let stale = handle.pause_rejection_snapshot();
        handle.mark_session_pause_rejection();
        handle.clear_session_pause_rejection_on_completed_result(stale, false);
        assert!(handle.take_session_reconnect_needed());
        assert!(!handle.take_session_reconnect_needed());

        // 3. GPU-only rejection stays separate from CPU completions and gates reconnect on !no_gpu_tasks.
        handle.set_limit("no_gpu_tasks", "true").unwrap();
        handle.mark_session_gpu_pause_rejection();
        let gpu_snap = handle.pause_rejection_snapshot();
        handle.clear_session_pause_rejection_on_completed_result(gpu_snap, false);
        assert!(!handle.take_session_reconnect_needed()); // still no_gpu_tasks=true
        handle.set_limit("no_gpu_tasks", "false").unwrap();
        assert!(handle.take_session_reconnect_needed()); // reconnect clears gpu gen

        // Re-mark GPU rejection and clear via GPU result.
        handle.mark_session_gpu_pause_rejection();
        let gpu_snap2 = handle.pause_rejection_snapshot();
        handle.clear_session_pause_rejection_on_completed_result(gpu_snap2, true);
        assert!(!handle.take_session_reconnect_needed());
    }

    #[test]
    fn limits_set_and_get_hold_ends_tasks_round_trip() {
        let handle = test_handle(WorkerLimitsConfig::default(), None);

        match handle.handle_request(ControlRequest::LimitsGet) {
            ControlResponse::Limits { limits, .. } => assert!(!limits.hold_ends_tasks),
            other => panic!("expected LimitsGet response, got {other:?}"),
        }

        let set_true = handle.handle_request(ControlRequest::LimitsSet {
            key: "hold_ends_tasks".into(),
            value: "true".into(),
        });
        assert!(
            matches!(set_true, ControlResponse::Ok { .. }),
            "expected Ok setting hold_ends_tasks=true, got {set_true:?}"
        );
        match handle.handle_request(ControlRequest::LimitsGet) {
            ControlResponse::Limits { limits, .. } => assert!(limits.hold_ends_tasks),
            other => panic!("expected LimitsGet after setting true, got {other:?}"),
        }

        let set_false = handle.handle_request(ControlRequest::LimitsSet {
            key: "hold-ends-tasks".into(),
            value: "false".into(),
        });
        assert!(
            matches!(set_false, ControlResponse::Ok { .. }),
            "expected Ok setting hold-ends-tasks=false, got {set_false:?}"
        );
        match handle.handle_request(ControlRequest::LimitsGet) {
            ControlResponse::Limits { limits, .. } => assert!(!limits.hold_ends_tasks),
            other => panic!("expected LimitsGet after setting false, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_control_checks_reject_broad_custom_dir_wrong_uid_and_slow_clients() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let broad = dir.path().join("broad-home");
        std::fs::create_dir(&broad).unwrap();
        std::fs::set_permissions(&broad, std::fs::Permissions::from_mode(0o755)).unwrap();

        let err = unix_ipc::ensure_private_socket_dir(
            &broad,
            unix_ipc::current_process_uid().await.unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("refusing to chmod"), "unexpected error: {err}");
        let mode_after = std::fs::metadata(&broad).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode_after, 0o755,
            "broad custom directory must not be chmodded"
        );

        // Cross-user rejection checks (helper + real root-owned /tmp dir when non-root).
        let uid = unix_ipc::current_process_uid().await.unwrap();
        let err_uid = unix_ipc::verify_peer_uid(uid, uid.saturating_add(1)).unwrap_err();
        assert!(err_uid.contains("does not match expected uid"));
        if uid != 0 && std::path::Path::new("/tmp").exists() {
            let err_root = unix_ipc::ensure_private_socket_dir(Path::new("/tmp"), uid).unwrap_err();
            assert!(
                err_root.contains("owned by uid") || err_root.contains("symlink"),
                "unexpected /tmp ownership error: {err_root}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn default_unix_socket_path_honours_xdg_then_oxideswarm_home_then_home() {
        use std::ffi::OsStr;

        assert_eq!(
            default_unix_socket_path_with(
                Some("box1"),
                Some(OsStr::new("/run/user/1000")),
                Some(OsStr::new("/custom/ox")),
                Some(OsStr::new("/home/alice")),
            ),
            Some(PathBuf::from("/run/user/1000/oxideswarm/box1.control.sock"))
        );
        assert_eq!(
            default_unix_socket_path_with(
                Some("box1"),
                Some(OsStr::new("")),
                Some(OsStr::new("/custom/ox")),
                Some(OsStr::new("/home/alice")),
            ),
            Some(PathBuf::from("/custom/ox/box1.control.sock"))
        );
        assert_eq!(
            default_unix_socket_path_with(
                None,
                Some(OsStr::new("")),
                None,
                Some(OsStr::new("/home/alice")),
            ),
            Some(PathBuf::from("/home/alice/.oxideswarm/control.sock"))
        );
        assert_eq!(default_unix_socket_path_with(None, None, None, None), None);
    }

    #[cfg(windows)]
    #[tokio::test]
    // LV-4: cfg(windows) named-pipe test invokes `GetCurrentProcess` for SID self-verification.
    #[allow(unsafe_code)]
    async fn windows_named_pipe_round_trip_owner_check_and_early_disconnect_recovery() {
        use tokio::net::windows::named_pipe::ClientOptions;
        use windows_sys::Win32::System::Threading::GetCurrentProcess;

        // Caller SID matches itself; an altered SID (flipped authority byte) is refused.
        // SAFETY: `GetCurrentProcess()` returns the current process pseudo-handle.
        let my_sid = windows_ipc::process_user_sid(unsafe { GetCurrentProcess() }).unwrap();
        windows_ipc::verify_sids_equal(&my_sid, &my_sid).unwrap();
        let mut fake_sid = my_sid.clone();
        if let Some(last) = fake_sid.last_mut() {
            *last ^= 0xFF;
        }
        assert!(windows_ipc::verify_sids_equal(&my_sid, &fake_sid).is_err());

        let pipe_name = format!(r"\\.\pipe\oxideswarm-test-{}", Uuid::new_v4());
        let handle = test_handle(WorkerLimitsConfig::default(), None);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let srv_handle = Arc::clone(&handle);
        let srv_pipe = pipe_name.clone();
        let srv = tokio::spawn(async move {
            serve_control_endpoint(
                ControlEndpoint::NamedPipe(srv_pipe),
                srv_handle,
                shutdown_rx,
            )
            .await
        });

        let ep = ControlEndpoint::NamedPipe(pipe_name.clone());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let status_resp = loop {
            match send_control_request(&ep, &ControlRequest::Status).await {
                Ok(r) => break r,
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("pipe server did not become ready: {e}"),
            }
        };
        assert!(matches!(status_resp, ControlResponse::Status { .. }));

        // Second `first_pipe_instance` on the same pipe name must fail visibly.
        assert!(windows_ipc::create_private_pipe_server(&pipe_name, true).is_err());

        // Early-closing clients must not trap `run_windows_server` in a busy loop or break next RPCs.
        for _ in 0..3 {
            let open_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let early = loop {
                match ClientOptions::new().open(&pipe_name) {
                    Ok(c) => break c,
                    Err(e)
                        if e.raw_os_error() == Some(windows_ipc::WIN_ERROR_PIPE_BUSY)
                            && tokio::time::Instant::now() < open_deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(e) => panic!("open early: {e}"),
                }
            };
            drop(early);
        }
        let resp = send_control_request(
            &ep,
            &ControlRequest::Pause {
                until_epoch_ms: None,
                reason: Some("win-test".into()),
            },
        )
        .await
        .expect("pipe server must stay healthy after early client disconnects");
        assert!(matches!(resp, ControlResponse::Ok { .. }));
        let resp = send_control_request(&ep, &ControlRequest::Resume)
            .await
            .unwrap();
        assert!(matches!(resp, ControlResponse::Ok { .. }));

        let _ = shutdown_tx.send(true);
        srv.await.unwrap().unwrap();
    }
}
