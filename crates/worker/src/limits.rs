//! Contributor-enforced local resource limits (`design 0113` D1, NET-9b).
//!
//! Covers:
//! - `--local-max-cpu-pct <P>`: 30-second sliding host CPU average limit
//! - `--hours "<spec>"`: local weekly schedule (supports multi-clause day specs and overnight spans)
//! - `--max-task-hours-per-day <H>`: daily cumulative task execution cap reset at local midnight
//! - `--no-tasks-on-battery`: hold new tasks while running on battery power
//! - `--no-tasks-on-metered`: hold new tasks while the OS reports a metered network
//! - `--no-gpu-tasks`: refuse GPU tasks while continuing to run CPU tasks
//! - `--hold-ends-tasks`: also hand back running tasks when an automatic limit enters hold

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike, Weekday};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Default sliding window over which host CPU usage is averaged (`design 0113` D1).
pub const CPU_AVERAGE_WINDOW: Duration = Duration::from_secs(30);

/// Hysteresis drop below `local_max_cpu_pct` required before releasing a CPU hold (`design 0113` D1).
pub const CPU_RELEASE_HYSTERESIS_PCT: f32 = 10.0;

/// Default cadence for re-evaluating contributor limits (`design 0113` D2).
pub const LIMITS_EVAL_INTERVAL: Duration = Duration::from_secs(10);

/// Rejection prefix recognized by `rusty_grid_master::queue::is_worker_rejection`.
pub const PAUSE_REJECTION_PREFIX: &str = "Task rejected by worker: paused: ";

/// Rejection text emitted when `--no-gpu-tasks` blocks a GPU task.
pub const NO_GPU_TASKS_REJECTION_MESSAGE: &str = "Task rejected by worker: no gpu tasks";

/// Formats a contributor-limit refusal message that a Master handles without charging a retry.
///
/// The `reason` MUST be a generic category (never the contributor's schedule or location, per
/// `design 0113` Security).
pub fn format_limit_rejection(reason: &str) -> String {
    let trimmed = reason.trim();
    if trimmed.is_empty() {
        format!("{PAUSE_REJECTION_PREFIX}contributor limit")
    } else {
        format!("{PAUSE_REJECTION_PREFIX}{trimmed}")
    }
}

/// Contributor-configurable worker limits (`design 0113` D1).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct WorkerLimits {
    /// Hold new tasks when 30 s average host CPU % exceeds this threshold (`1..=100`).
    #[serde(default)]
    pub local_max_cpu_pct: Option<u8>,
    /// Weekly schedule in local time (e.g. `"mon-fri 18:00-08:00; sat-sun 00:00-24:00"`).
    #[serde(default)]
    pub hours: Option<String>,
    /// Maximum summed task run time per local calendar day in hours (`(0, 24]`).
    #[serde(default)]
    pub max_task_hours_per_day: Option<f64>,
    /// Hold new tasks while operating on battery power.
    #[serde(default)]
    pub no_tasks_on_battery: bool,
    /// Hold new tasks while on a metered network connection.
    #[serde(default)]
    pub no_tasks_on_metered: bool,
    /// Refuse GPU tasks while continuing to accept CPU tasks.
    #[serde(default)]
    pub no_gpu_tasks: bool,
    /// Also hand back already-running tasks when an automatic limit triggers hold.
    #[serde(default)]
    pub hold_ends_tasks: bool,
}

impl WorkerLimits {
    /// Converts control/CLI persisted limits into an evaluator config.
    pub fn from_control_config(cfg: &crate::control::WorkerLimitsConfig) -> Self {
        Self {
            local_max_cpu_pct: cfg.local_max_cpu_pct,
            hours: cfg.hours.clone(),
            max_task_hours_per_day: cfg.max_task_hours_per_day,
            no_tasks_on_battery: cfg.no_tasks_on_battery.unwrap_or(false),
            no_tasks_on_metered: cfg.no_tasks_on_metered.unwrap_or(false),
            no_gpu_tasks: cfg.no_gpu_tasks,
            hold_ends_tasks: cfg.hold_ends_tasks,
        }
    }

    /// Validates every configured limit value upfront.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(pct) = self.local_max_cpu_pct {
            if !(1..=100).contains(&pct) {
                return Err(format!(
                    "--local-max-cpu-pct must be between 1 and 100 (got {pct})"
                ));
            }
        }
        if let Some(spec) = self.hours.as_deref() {
            HoursSchedule::parse(spec)?;
        }
        if let Some(hours_per_day) = self.max_task_hours_per_day {
            if !hours_per_day.is_finite() || hours_per_day <= 0.0 || hours_per_day > 24.0 {
                return Err(format!(
                    "--max-task-hours-per-day must be finite in (0, 24] (got {hours_per_day})"
                ));
            }
        }
        Ok(())
    }

    /// Returns `true` when at least one contributor limit is active.
    pub fn any_active(&self) -> bool {
        self.local_max_cpu_pct.is_some()
            || self.hours.is_some()
            || self.max_task_hours_per_day.is_some()
            || self.no_tasks_on_battery
            || self.no_tasks_on_metered
            || self.no_gpu_tasks
    }
}

/// A parsed weekly schedule of allowed execution windows in local wall-clock time.
///
/// Format: semicolon-separated clauses of `[DAYS ]HH:MM-HH:MM`, e.g.:
/// - `"mon-fri 18:00-08:00; sat-sun 00:00-24:00"`
/// - `"mon,wed,fri 09:00-17:30"`
/// - `"18:00-08:00"` (applies to every day)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoursSchedule {
    /// Allowed windows keyed by ISO weekday index (`0 = Mon .. 6 = Sun`).
    windows: [Vec<TimeSpan>; 7],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeSpan {
    /// Seconds from midnight (`0..86_400`).
    start_sec: u32,
    /// Seconds from midnight (`1..=86_400`, where `< start_sec` means crossing midnight).
    end_sec: u32,
}

impl HoursSchedule {
    /// Parses a weekly schedule string into validated per-weekday windows.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err("hours schedule cannot be empty".to_string());
        }
        let mut windows: [Vec<TimeSpan>; 7] = Default::default();
        let mut clause_count = 0usize;

        for clause in trimmed.split(';') {
            let clause = clause.trim();
            if clause.is_empty() {
                return Err(format!(
                    "hours schedule '{raw}' contains an empty semicolon clause"
                ));
            }
            clause_count += 1;
            let tokens: Vec<&str> = clause.split_whitespace().collect();
            let (days, span_str) = match tokens.as_slice() {
                [span] => ([true; 7], *span),
                [days_spec, span] => (parse_days_spec(days_spec)?, *span),
                _ => {
                    return Err(format!(
                        "invalid hours clause '{clause}' (expected '[DAYS ]HH:MM-HH:MM')"
                    ));
                }
            };
            let span = parse_time_span(span_str)?;
            for (idx, enabled) in days.into_iter().enumerate() {
                if enabled {
                    windows[idx].push(span);
                }
            }
        }

        if clause_count == 0 {
            return Err("hours schedule cannot be empty".to_string());
        }
        Ok(Self { windows })
    }

    /// Returns `true` when `local_dt` (wall-clock local time) falls inside any allowed window.
    ///
    /// Evaluating wall-clock `(weekday, secs_since_midnight)` avoids panics or missed checks
    /// across daylight-saving spring-forward and fall-back transitions.
    pub fn is_allowed_local(&self, local_dt: NaiveDateTime) -> bool {
        let day_idx = weekday_index(local_dt.weekday());
        let prev_idx = (day_idx + 6) % 7;
        let sec = local_dt.time().num_seconds_from_midnight();

        for &span in &self.windows[day_idx] {
            if span.start_sec < span.end_sec {
                if sec >= span.start_sec && sec < span.end_sec {
                    return true;
                }
            } else {
                // Overnight window starting on `day_idx` covers `[start_sec, 24:00)`.
                if sec >= span.start_sec {
                    return true;
                }
            }
        }

        // Tail of any overnight window that started on the previous weekday (`[00:00, end_sec)`).
        self.windows[prev_idx]
            .iter()
            .any(|span| span.start_sec > span.end_sec && sec < span.end_sec)
    }

    /// Returns `true` when `dt` falls inside any allowed window in its timezone's local wall-clock time.
    pub fn is_allowed_at<Tz: TimeZone>(&self, dt: &DateTime<Tz>) -> bool {
        self.is_allowed_local(dt.naive_local())
    }
}

fn weekday_index(day: Weekday) -> usize {
    day.num_days_from_monday() as usize
}

fn parse_weekday_token(tok: &str) -> Result<usize, String> {
    match tok.trim().to_ascii_lowercase().as_str() {
        "mon" | "monday" => Ok(0),
        "tue" | "tues" | "tuesday" => Ok(1),
        "wed" | "wednesday" => Ok(2),
        "thu" | "thur" | "thurs" | "thursday" => Ok(3),
        "fri" | "friday" => Ok(4),
        "sat" | "saturday" => Ok(5),
        "sun" | "sunday" => Ok(6),
        other => Err(format!(
            "unknown weekday '{other}' (expected mon..sun, daily, or weekdays)"
        )),
    }
}

fn parse_days_spec(spec: &str) -> Result<[bool; 7], String> {
    let norm = spec.trim().to_ascii_lowercase();
    if norm.is_empty() {
        return Err("empty day specifier in hours clause".to_string());
    }
    if matches!(norm.as_str(), "all" | "daily" | "everyday" | "*") {
        return Ok([true; 7]);
    }
    if matches!(norm.as_str(), "weekdays" | "weekday") {
        return Ok([true, true, true, true, true, false, false]);
    }
    if matches!(norm.as_str(), "weekends" | "weekend") {
        return Ok([false, false, false, false, false, true, true]);
    }

    let mut mask = [false; 7];
    for part in norm.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(format!("invalid empty day item in '{spec}'"));
        }
        if let Some((start_raw, end_raw)) = part.split_once('-') {
            let start = parse_weekday_token(start_raw)?;
            let end = parse_weekday_token(end_raw)?;
            let mut cur = start;
            loop {
                mask[cur] = true;
                if cur == end {
                    break;
                }
                cur = (cur + 1) % 7;
            }
        } else {
            let idx = parse_weekday_token(part)?;
            mask[idx] = true;
        }
    }
    Ok(mask)
}

fn parse_time_span(raw: &str) -> Result<TimeSpan, String> {
    let Some((start_raw, end_raw)) = raw.trim().split_once('-') else {
        return Err(format!(
            "invalid time window '{raw}' (expected HH:MM-HH:MM)"
        ));
    };
    let start_sec = parse_hhmm_seconds(start_raw, false)?;
    let end_sec = parse_hhmm_seconds(end_raw, true)?;
    if start_sec == end_sec {
        return Err(format!(
            "empty zero-length time window '{raw}' (use 00:00-24:00 for all day)"
        ));
    }
    Ok(TimeSpan { start_sec, end_sec })
}

fn parse_hhmm_seconds(raw: &str, allow_2400: bool) -> Result<u32, String> {
    let trimmed = raw.trim();
    let Some((h_str, m_str)) = trimmed.split_once(':') else {
        return Err(format!("invalid time '{trimmed}' (expected HH:MM)"));
    };
    if h_str.len() != 2
        || m_str.len() != 2
        || !h_str.bytes().all(|b| b.is_ascii_digit())
        || !m_str.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(format!(
            "invalid time '{trimmed}' (expected two-digit HH:MM 24-hour clock)"
        ));
    }
    let hour: u32 = h_str
        .parse()
        .map_err(|_| format!("invalid hour in '{trimmed}'"))?;
    let minute: u32 = m_str
        .parse()
        .map_err(|_| format!("invalid minute in '{trimmed}'"))?;
    if hour == 24 && minute == 0 && allow_2400 {
        return Ok(86_400);
    }
    if hour >= 24 || minute >= 60 {
        return Err(format!(
            "time '{trimmed}' out of range (expected 00:00..=23:59{})",
            if allow_2400 { " or 24:00" } else { "" }
        ));
    }
    Ok(hour * 3_600 + minute * 60)
}

/// Sliding-window host CPU utilization average (`design 0113` D1: 30 seconds).
#[derive(Debug, Clone)]
pub struct CpuAverager {
    window: Duration,
    samples: VecDeque<(Instant, f32)>,
}

impl Default for CpuAverager {
    fn default() -> Self {
        Self::new(CPU_AVERAGE_WINDOW)
    }
}

impl CpuAverager {
    /// Creates an averager keeping samples within `window` of the latest observation.
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            samples: VecDeque::new(),
        }
    }

    /// Records a host CPU utilization sample (`0.0..=100.0`) at `at` and prunes expired samples.
    pub fn record(&mut self, at: Instant, cpu_pct: f32) {
        let clamped = if cpu_pct.is_finite() {
            cpu_pct.clamp(0.0, 100.0)
        } else {
            0.0
        };
        self.samples.push_back((at, clamped));
        self.prune_before(at);
    }

    fn prune_before(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.window);
        if let Some(cutoff_inst) = cutoff {
            while self
                .samples
                .front()
                .is_some_and(|(ts, _)| *ts < cutoff_inst)
            {
                self.samples.pop_front();
            }
        }
    }

    /// Returns the time-weighted mean CPU % across unexpired samples as of `now`, or `None` if empty.
    ///
    /// Uses trapezoidal integration across sample timestamps within `[now - window, now]` (and holds
    /// the last reading up to `now`) so rapid wakeup bursts do not distort the 30 s window.
    pub fn average(&mut self, now: Instant) -> Option<f32> {
        self.prune_before(now);
        if self.samples.is_empty() {
            return None;
        }
        if self.samples.len() == 1 {
            return Some(self.samples[0].1);
        }
        let first_t = self.samples[0].0;
        let total_secs = now.saturating_duration_since(first_t).as_secs_f64();
        if total_secs <= 0.0 {
            let sum: f64 = self.samples.iter().map(|(_, v)| f64::from(*v)).sum();
            return Some((sum / self.samples.len() as f64) as f32);
        }
        let mut weighted_sum = 0.0f64;
        for i in 1..self.samples.len() {
            let (t0, v0) = self.samples[i - 1];
            let (t1, v1) = self.samples[i];
            let dt = t1.saturating_duration_since(t0).as_secs_f64();
            if dt > 0.0 {
                weighted_sum += 0.5 * (f64::from(v0) + f64::from(v1)) * dt;
            }
        }
        let (last_t, last_v) = *self.samples.back().expect("non-empty samples");
        let tail_dt = now.saturating_duration_since(last_t).as_secs_f64();
        if tail_dt > 0.0 {
            weighted_sum += f64::from(last_v) * tail_dt;
        }
        Some(((weighted_sum / total_secs) as f32).clamp(0.0, 100.0))
    }

    /// Returns `true` when the sliding-window average strictly exceeds `limit_pct`.
    pub fn exceeds(&mut self, now: Instant, limit_pct: u8) -> bool {
        self.average(now)
            .is_some_and(|avg| avg > f32::from(limit_pct))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveTaskRun {
    last_local: NaiveDateTime,
    started_at: Instant,
    accounted_monotonic_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedDailyBudget {
    day: NaiveDate,
    used_ms: u64,
}

fn check_not_symlink(path: &Path) -> std::io::Result<Option<std::fs::Metadata>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("refusing symbolic link '{}'", path.display()),
                ));
            }
            Ok(Some(meta))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn load_persisted_budget(path: &Path) -> std::io::Result<Option<PersistedDailyBudget>> {
    let Some(meta) = check_not_symlink(path)? else {
        return Ok(None);
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let uid = meta.uid();
        // SAFETY: `geteuid` is always safe to call and takes no pointers.
        let euid = unsafe { libc::geteuid() };
        if uid != euid {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "budget state '{}' owned by uid {uid} (expected {euid})",
                    path.display()
                ),
            ));
        }
        if (meta.mode() & 0o077) != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "budget state '{}' has unsafe permissions {:o}; expected 0600",
                    path.display(),
                    meta.mode() & 0o777
                ),
            ));
        }
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true).custom_flags(libc::O_NOFOLLOW);
        let file = opts.open(path)?;
        let parsed: PersistedDailyBudget = serde_json::from_reader(file)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Some(parsed))
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        let _ = meta;
        rusty_grid_core::private_file::warn_if_readable_by_others(path, "daily task budget state");
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true).custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
        let file = opts.open(path)?;
        let parsed: PersistedDailyBudget = serde_json::from_reader(file)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Some(parsed))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = meta;
        let file = std::fs::File::open(path)?;
        let parsed: PersistedDailyBudget = serde_json::from_reader(file)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Some(parsed))
    }
}

fn save_persisted_budget(path: &Path, state: &PersistedDailyBudget) -> std::io::Result<()> {
    let _ = check_not_symlink(path)?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        #[cfg(windows)]
        rusty_grid_core::private_file::create_private_dir_all(parent)?;
        #[cfg(not(windows))]
        {
            let existed = parent.exists();
            std::fs::create_dir_all(parent)?;
            #[cfg(unix)]
            if !existed {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
            }
        }
    }

    let bytes = serde_json::to_vec(state)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    #[cfg(windows)]
    {
        rusty_grid_core::private_file::write_private_replace(path, &bytes)
    }
    #[cfg(not(windows))]
    {
        use std::io::Write;
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "daily-budget.json".to_string());
        let tmp = path.with_file_name(format!(
            ".{name}.{}.{}.tmp",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let res = opts
            .open(&tmp)
            .and_then(|mut f| {
                f.write_all(&bytes)?;
                f.sync_all()
            })
            .and_then(|()| std::fs::rename(&tmp, path));
        if res.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        res
    }
}

/// Daily cumulative task runtime budget reset at local midnight (`design 0113` D1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyTaskBudget {
    day: NaiveDate,
    used_ms: u64,
    active_runs: HashMap<u64, ActiveTaskRun>,
    state_path: Option<PathBuf>,
    last_persisted: Option<(NaiveDate, u64)>,
}

impl DailyTaskBudget {
    /// Starts an empty daily counter anchored on `initial_day`.
    pub fn new(initial_day: NaiveDate) -> Self {
        Self {
            day: initial_day,
            used_ms: 0,
            active_runs: HashMap::new(),
            state_path: None,
            last_persisted: None,
        }
    }

    /// Creates a daily counter backed by a same-user private state file at `path`.
    pub fn with_state_path(initial_day: NaiveDate, path: PathBuf) -> Self {
        let (day, used_ms, last_persisted) = match load_persisted_budget(&path) {
            Ok(Some(persisted)) => {
                if persisted.day >= initial_day {
                    (
                        persisted.day,
                        persisted.used_ms,
                        Some((persisted.day, persisted.used_ms)),
                    )
                } else {
                    (initial_day, 0, None)
                }
            }
            Ok(None) => (initial_day, 0, None),
            Err(err) => {
                warn!(path = %path.display(), error = %err, "Ignoring unusable persisted daily task budget file");
                (initial_day, 0, None)
            }
        };
        Self {
            day,
            used_ms,
            active_runs: HashMap::new(),
            state_path: Some(path),
            last_persisted,
        }
    }

    fn maybe_persist(&mut self, force: bool) {
        let Some(ref path) = self.state_path else {
            return;
        };
        let should_write = match self.last_persisted {
            None => self.used_ms > 0 || force,
            Some((prev_day, prev_ms)) => {
                if self.day != prev_day {
                    true
                } else if force {
                    self.used_ms != prev_ms
                } else {
                    self.used_ms.saturating_sub(prev_ms) >= 1_000
                }
            }
        };
        if !should_write {
            return;
        }
        let payload = PersistedDailyBudget {
            day: self.day,
            used_ms: self.used_ms,
        };
        match save_persisted_budget(path, &payload) {
            Ok(()) => {
                self.last_persisted = Some((payload.day, payload.used_ms));
            }
            Err(err) => {
                warn!(path = %path.display(), error = %err, "Failed to persist daily task budget");
            }
        }
    }

    /// Forces persistence of any un-flushed daily budget state.
    pub fn flush(&mut self) {
        self.maybe_persist(true);
    }

    /// Advances forward to `day`, resetting cumulative runtime ONLY when `day > self.day` so
    /// backward wall-clock adjustments or same-day restarts never clear accrued runtime.
    pub fn advance_day(&mut self, day: NaiveDate) {
        if day > self.day {
            self.day = day;
            self.used_ms = 0;
        }
    }

    /// Splits a local wall-clock interval `[from_local, to_local)` across local midnights.
    fn record_local_interval(&mut self, from_local: NaiveDateTime, to_local: NaiveDateTime) -> u64 {
        if to_local <= from_local {
            return 0;
        }
        let mut cur = from_local;
        let mut total_added_ms = 0u64;
        while cur < to_local {
            let cur_day = cur.date();
            let Some(next_day) = cur_day.succ_opt() else {
                break;
            };
            let next_midnight = next_day.and_hms_opt(0, 0, 0).expect("valid midnight");
            let seg_end = to_local.min(next_midnight);
            let seg_ms = u64::try_from((seg_end - cur).num_milliseconds().max(0)).unwrap_or(0);
            if cur_day == self.day {
                self.used_ms = self.used_ms.saturating_add(seg_ms);
            } else if cur_day > self.day {
                self.day = cur_day;
                self.used_ms = seg_ms;
            }
            total_added_ms = total_added_ms.saturating_add(seg_ms);
            cur = seg_end;
        }
        total_added_ms
    }

    /// Registers an actively running task so its runtime is accrued before completion.
    pub fn begin_active_task(
        &mut self,
        run_id: u64,
        started_local: NaiveDateTime,
        started_at: Instant,
    ) {
        self.advance_day(started_local.date());
        self.active_runs.insert(
            run_id,
            ActiveTaskRun {
                last_local: started_local,
                started_at,
                accounted_monotonic_ms: 0,
            },
        );
    }

    /// Accounts elapsed runtime for all currently running tasks up to `(local_now, now)`.
    pub fn account_active_tasks(&mut self, local_now: NaiveDateTime, now: Instant) {
        self.advance_day(local_now.date());
        let run_ids: Vec<u64> = self.active_runs.keys().copied().collect();
        for run_id in run_ids {
            let Some((last_local, started_at, accounted_monotonic_ms)) = self
                .active_runs
                .get(&run_id)
                .map(|r| (r.last_local, r.started_at, r.accounted_monotonic_ms))
            else {
                continue;
            };
            let mono_total_ms =
                u64::try_from(now.saturating_duration_since(started_at).as_millis())
                    .unwrap_or(u64::MAX);
            if local_now > last_local {
                let wall_ms = self.record_local_interval(last_local, local_now);
                if let Some(run) = self.active_runs.get_mut(&run_id) {
                    run.last_local = local_now;
                    run.accounted_monotonic_ms = run
                        .accounted_monotonic_ms
                        .saturating_add(wall_ms)
                        .max(mono_total_ms);
                }
            } else {
                let extra_ms = mono_total_ms.saturating_sub(accounted_monotonic_ms);
                if extra_ms > 0 {
                    if local_now.date() >= self.day {
                        self.used_ms = self.used_ms.saturating_add(extra_ms);
                    }
                    if let Some(run) = self.active_runs.get_mut(&run_id) {
                        run.accounted_monotonic_ms = mono_total_ms;
                    }
                }
            }
        }
        self.maybe_persist(false);
    }

    /// Finalizes a finished, cancelled, handed-back, or aborted task run and persists state.
    pub fn finish_active_task(
        &mut self,
        run_id: u64,
        ended_local: NaiveDateTime,
        elapsed: Duration,
    ) {
        let total_elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        if let Some(run) = self.active_runs.remove(&run_id) {
            let wall_ms = if ended_local > run.last_local {
                self.record_local_interval(run.last_local, ended_local)
            } else {
                0
            };
            let total_accounted = run.accounted_monotonic_ms.saturating_add(wall_ms);
            let rem_ms = total_elapsed_ms.saturating_sub(total_accounted);
            if rem_ms > 0 {
                let end_day = ended_local.date();
                self.advance_day(end_day);
                if end_day >= self.day {
                    self.used_ms = self.used_ms.saturating_add(rem_ms);
                }
            }
        } else {
            self.record_task_duration(ended_local.date(), elapsed);
        }
        self.maybe_persist(true);
    }

    /// Adds completed task `duration` on `day` (resetting first if `day > self.day`).
    pub fn record_task_duration(&mut self, day: NaiveDate, duration: Duration) {
        self.advance_day(day);
        if day >= self.day {
            let add_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
            self.used_ms = self.used_ms.saturating_add(add_ms);
        }
        self.maybe_persist(true);
    }

    /// Returns total recorded task hours on `day` (resets to `0.0` if `day > self.day`).
    pub fn used_hours(&mut self, day: NaiveDate) -> f64 {
        self.advance_day(day);
        self.used_ms as f64 / 3_600_000.0
    }

    /// Returns `true` once cumulative runtime on `day` reaches or exceeds `cap_hours`.
    pub fn is_exhausted(&mut self, day: NaiveDate, cap_hours: f64) -> bool {
        self.advance_day(day);
        self.used_ms as f64 >= cap_hours * 3_600_000.0
    }
}

/// Host AC / battery power status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatteryStatus {
    OnAc,
    OnBattery,
    Unknown,
}

/// Abstract host power source probe for unit tests and live runtime checks.
pub trait BatteryProbe: Send + Sync {
    fn probe_battery(&self) -> BatteryStatus;
}

/// Network metered-cost classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeteredStatus {
    Unmetered,
    Metered,
    Unknown,
}

/// Abstract host metered-network probe for unit tests and live runtime checks.
pub trait MeteredNetworkProbe: Send + Sync {
    fn probe_metered(&self) -> MeteredStatus;
}

/// Abstract host CPU utilization probe for unit tests and live runtime checks (`0.0..=100.0`).
pub trait HostCpuProbe: Send + Sync {
    fn probe_cpu_pct(&self) -> Option<f32>;
}

/// Live host CPU probe backed by the worker's non-blocking telemetry sampler.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemHostCpuProbe;

/// Subtracts the worker's own task process tree CPU% from global host CPU% on a `0..=100` scale.
pub fn subtract_worker_tree_cpu_pct(
    global_cpu_pct: f32,
    worker_tree_cpu_pct_sum: f32,
    logical_cpus: usize,
) -> f32 {
    let cpus = logical_cpus.max(1) as f32;
    let tree_host_pct = if worker_tree_cpu_pct_sum.is_finite() {
        worker_tree_cpu_pct_sum.max(0.0) / cpus
    } else {
        0.0
    };
    let global = if global_cpu_pct.is_finite() {
        global_cpu_pct.clamp(0.0, 100.0)
    } else {
        0.0
    };
    (global - tree_host_pct).clamp(0.0, 100.0)
}

/// Stateful host CPU sampler that excludes the worker's own active task process trees.
pub struct WorkerTreeCpuSampler {
    sys: sysinfo::System,
}

impl std::fmt::Debug for WorkerTreeCpuSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerTreeCpuSampler")
            .finish_non_exhaustive()
    }
}

impl Default for WorkerTreeCpuSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerTreeCpuSampler {
    /// Initializes a persistent `sysinfo::System` CPU baseline so subsequent samples produce deltas.
    pub fn new() -> Self {
        let mut sys = sysinfo::System::new_with_specifics(
            sysinfo::RefreshKind::nothing()
                .with_cpu(sysinfo::CpuRefreshKind::nothing().with_cpu_usage()),
        );
        sys.refresh_cpu_specifics(sysinfo::CpuRefreshKind::nothing().with_cpu_usage());
        Self { sys }
    }

    /// Samples global host CPU utilization minus all descendants of `root_task_pids`.
    pub fn sample_external_cpu_pct(&mut self, root_task_pids: &[u32]) -> f32 {
        self.sys
            .refresh_cpu_specifics(sysinfo::CpuRefreshKind::nothing().with_cpu_usage());
        let global = self.sys.global_cpu_usage();
        let roots: Vec<sysinfo::Pid> = root_task_pids
            .iter()
            .copied()
            .filter(|&pid| pid != 0)
            .map(sysinfo::Pid::from_u32)
            .collect();
        if roots.is_empty() {
            return if global.is_finite() {
                global.clamp(0.0, 100.0)
            } else {
                0.0
            };
        }

        self.sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::All,
            true,
            sysinfo::ProcessRefreshKind::nothing().with_cpu(),
        );
        let procs = self.sys.processes();
        let mut children_by_parent: HashMap<sysinfo::Pid, Vec<sysinfo::Pid>> = HashMap::new();
        for (pid, proc_info) in procs {
            if let Some(parent) = proc_info.parent() {
                children_by_parent.entry(parent).or_default().push(*pid);
            }
        }
        let mut visited = std::collections::HashSet::with_capacity(roots.len() * 4);
        let mut queue = VecDeque::new();
        for root in roots {
            if visited.insert(root) {
                queue.push_back(root);
            }
        }
        let mut tree_sum = 0.0f32;
        while let Some(pid) = queue.pop_front() {
            if let Some(proc_info) = procs.get(&pid) {
                tree_sum += proc_info.cpu_usage();
            }
            if let Some(children) = children_by_parent.get(&pid) {
                for child in children {
                    if visited.insert(*child) {
                        queue.push_back(*child);
                    }
                }
            }
        }
        subtract_worker_tree_cpu_pct(global, tree_sum, self.sys.cpus().len())
    }
}

impl HostCpuProbe for SystemHostCpuProbe {
    fn probe_cpu_pct(&self) -> Option<f32> {
        Some(crate::heartbeat::get_system_telemetry().0)
    }
}

/// Abstract local wall-clock time source for schedule and daily budget evaluation.
pub trait LocalClock: Send + Sync {
    fn local_now(&self) -> NaiveDateTime;
}

/// Live wall-clock time source (`chrono::Local::now().naive_local()`).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemLocalClock;

impl LocalClock for SystemLocalClock {
    fn local_now(&self) -> NaiveDateTime {
        chrono::Local::now().naive_local()
    }
}

/// Classifies a Windows `SYSTEM_POWER_STATUS` (`ACLineStatus`, `BatteryFlag`) reading.
pub fn classify_windows_power_status(ac_line_status: u8, battery_flag: u8) -> BatteryStatus {
    const BATTERY_FLAG_NO_BATTERY: u8 = 128;
    const AC_LINE_OFFLINE: u8 = 0;
    const AC_LINE_ONLINE: u8 = 1;
    const AC_LINE_UNKNOWN: u8 = 255;

    if (battery_flag & BATTERY_FLAG_NO_BATTERY) != 0 || ac_line_status == AC_LINE_UNKNOWN {
        return BatteryStatus::Unknown;
    }
    match ac_line_status {
        AC_LINE_OFFLINE => BatteryStatus::OnBattery,
        AC_LINE_ONLINE => BatteryStatus::OnAc,
        _ => BatteryStatus::Unknown,
    }
}

/// Classifies a Windows `NLM_CONNECTION_COST` bitmask returned by `INetworkCostManager::GetCost`.
pub fn classify_windows_nlm_cost(cost: u32) -> MeteredStatus {
    const UNRESTRICTED: u32 = 0x0000_0001;
    const FIXED: u32 = 0x0000_0002;
    const VARIABLE: u32 = 0x0000_0004;
    const OVER_DATA_LIMIT: u32 = 0x0001_0000;
    const CONGESTED: u32 = 0x0002_0000;
    const ROAMING: u32 = 0x0004_0000;
    const APPROACHING_DATA_LIMIT: u32 = 0x0008_0000;

    let metered_mask = FIXED | VARIABLE | OVER_DATA_LIMIT | ROAMING | APPROACHING_DATA_LIMIT;
    if (cost & metered_mask) != 0 {
        MeteredStatus::Metered
    } else if (cost & UNRESTRICTED) != 0 || (cost & CONGESTED) != 0 {
        MeteredStatus::Unmetered
    } else {
        MeteredStatus::Unknown
    }
}

/// Parses Linux NetworkManager `Metered` output (`busctl get-property` or `nmcli`).
///
/// NetworkManager `Metered` enum values:
/// - `0`: Unknown
/// - `1`: Yes (explicit metered)
/// - `2`: No (explicit unmetered)
/// - `3`: GuessYes
/// - `4`: GuessNo
pub fn parse_linux_nm_metered_output(output: &str) -> MeteredStatus {
    let trimmed = output.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return MeteredStatus::Unknown;
    }
    if let Some(rest) = trimmed.strip_prefix("u ") {
        return match rest.trim() {
            "1" | "3" => MeteredStatus::Metered,
            "2" | "4" => MeteredStatus::Unmetered,
            _ => MeteredStatus::Unknown,
        };
    }
    if trimmed.starts_with("yes") || matches!(trimmed.as_str(), "1" | "3") {
        MeteredStatus::Metered
    } else if trimmed.starts_with("no") || matches!(trimmed.as_str(), "2" | "4") {
        MeteredStatus::Unmetered
    } else {
        MeteredStatus::Unknown
    }
}

/// Parses macOS `NWPath` cost flags (`isExpensive` / `isConstrained`) into [`MeteredStatus`].
pub fn parse_macos_path_flags(is_expensive: bool, is_constrained: bool) -> MeteredStatus {
    if is_expensive || is_constrained {
        MeteredStatus::Metered
    } else {
        MeteredStatus::Unmetered
    }
}

/// Live OS battery probe (`GetSystemPowerStatus` on Windows; `detect_live_mobile_status` on
/// Linux/macOS/Android).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemBatteryProbe;

impl BatteryProbe for SystemBatteryProbe {
    fn probe_battery(&self) -> BatteryStatus {
        #[cfg(windows)]
        {
            windows_battery::query_windows_battery_status()
        }
        #[cfg(not(windows))]
        {
            let Some(live) = rusty_grid_core::capabilities::detect_live_mobile_status() else {
                return BatteryStatus::Unknown;
            };
            match live.is_charging {
                Some(false) => BatteryStatus::OnBattery,
                Some(true) => BatteryStatus::OnAc,
                None => BatteryStatus::Unknown,
            }
        }
    }
}

/// Live OS metered-network probe (`INetworkCostManager` on Windows, NetworkManager on Linux,
/// `Unknown` with one-time notice elsewhere per `design 0113` D1).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemMeteredNetworkProbe;

#[cfg(windows)]
impl SystemMeteredNetworkProbe {
    /// Queries Windows NLM cost once and returns `(classified_status, raw_cost)`.
    pub fn probe_metered_with_raw(&self) -> (MeteredStatus, Option<u32>) {
        let raw = windows_metered::query_windows_network_cost_raw();
        let status = match raw {
            Some(cost) => classify_windows_nlm_cost(cost),
            None => MeteredStatus::Unknown,
        };
        (status, raw)
    }
}

impl MeteredNetworkProbe for SystemMeteredNetworkProbe {
    fn probe_metered(&self) -> MeteredStatus {
        #[cfg(windows)]
        {
            self.probe_metered_with_raw().0
        }
        #[cfg(target_os = "linux")]
        {
            linux_metered::query_linux_nm_metered()
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        {
            MeteredStatus::Unknown
        }
    }
}

#[cfg(windows)]
mod windows_battery {
    use super::{classify_windows_power_status, BatteryStatus};
    use windows_sys::Win32::System::Power::{GetSystemPowerStatus, SYSTEM_POWER_STATUS};

    pub(super) fn query_windows_battery_status() -> BatteryStatus {
        // SAFETY: `SYSTEM_POWER_STATUS` is plain integer data zero-initialized before passing its
        // pointer to Win32 `GetSystemPowerStatus`.
        let mut status: SYSTEM_POWER_STATUS = unsafe { std::mem::zeroed() };
        // SAFETY: `status` points to valid initialized stack storage.
        let ok = unsafe { GetSystemPowerStatus(&mut status) };
        if ok == 0 {
            return BatteryStatus::Unknown;
        }
        classify_windows_power_status(status.ACLineStatus, status.BatteryFlag)
    }
}

#[cfg(windows)]
mod windows_metered {
    use core::ffi::c_void;
    use std::ptr;

    use windows_sys::core::GUID;
    use windows_sys::Win32::Foundation::{RPC_E_CHANGED_MODE, S_FALSE, S_OK};
    use windows_sys::Win32::System::Com::{
        CoCreateInstance, CoInitializeEx, CoUninitialize, CLSCTX_ALL, COINIT_MULTITHREADED,
    };

    // CLSID_NetworkListManager = {DCB00C01-570F-4A9B-8D69-199FDBA5723B}
    const CLSID_NETWORK_LIST_MANAGER: GUID =
        GUID::from_u128(0xdcb00c01_570f_4a9b_8d69_199fdba5723b);
    // IID_INetworkCostManager = {DCB00008-570F-4A9B-8D69-199FDBA5723B}
    const IID_INETWORK_COST_MANAGER: GUID = GUID::from_u128(0xdcb00008_570f_4a9b_8d69_199fdba5723b);

    #[repr(C)]
    struct IUnknownVtbl {
        query_interface: usize,
        add_ref: usize,
        release: unsafe extern "system" fn(*mut c_void) -> u32,
    }

    #[repr(C)]
    struct INetworkCostManagerVtbl {
        base: IUnknownVtbl,
        get_cost: unsafe extern "system" fn(*mut c_void, *mut u32, *const c_void) -> i32,
    }

    struct ComInitGuard {
        should_uninit: bool,
    }

    impl ComInitGuard {
        fn new() -> Option<Self> {
            // SAFETY: `CoInitializeEx` initializes COM for the calling thread with null reserved pointer.
            let hr = unsafe { CoInitializeEx(ptr::null(), COINIT_MULTITHREADED as u32) };
            if hr == S_OK || hr == S_FALSE {
                Some(Self {
                    should_uninit: true,
                })
            } else if hr == RPC_E_CHANGED_MODE {
                // Thread already initialized COM under another apartment model; use it without
                // balancing with CoUninitialize.
                Some(Self {
                    should_uninit: false,
                })
            } else {
                None
            }
        }
    }

    impl Drop for ComInitGuard {
        fn drop(&mut self) {
            if self.should_uninit {
                // SAFETY: balances a successful `CoInitializeEx` on this same thread.
                unsafe { CoUninitialize() };
            }
        }
    }

    struct ComPtrGuard {
        raw: *mut c_void,
    }

    impl Drop for ComPtrGuard {
        fn drop(&mut self) {
            if !self.raw.is_null() {
                // SAFETY: `raw` is a valid non-null `INetworkCostManager` COM pointer whose first
                // field is the vtable pointer.
                unsafe {
                    let vtbl = *(self.raw as *const *const IUnknownVtbl);
                    ((*vtbl).release)(self.raw);
                }
            }
        }
    }

    /// Queries Windows `INetworkCostManager::GetCost` via COM and returns the raw
    /// `NLM_CONNECTION_COST` bitmask, or `None` if COM or Network List Manager is unavailable.
    pub fn query_windows_network_cost_raw() -> Option<u32> {
        let _com = ComInitGuard::new()?;
        let mut raw: *mut c_void = ptr::null_mut();
        // SAFETY: `CLSID_NETWORK_LIST_MANAGER` and `IID_INETWORK_COST_MANAGER` are standard Windows
        // Network List Manager GUIDs; `raw` receives an owned `INetworkCostManager` pointer on S_OK.
        let hr = unsafe {
            CoCreateInstance(
                &CLSID_NETWORK_LIST_MANAGER,
                ptr::null_mut(),
                CLSCTX_ALL,
                &IID_INETWORK_COST_MANAGER,
                &mut raw,
            )
        };
        if hr < 0 || raw.is_null() {
            return None;
        }
        let guard = ComPtrGuard { raw };
        let mut cost: u32 = 0;
        // SAFETY: `guard.raw` is an `INetworkCostManager` pointer and slot 3 matches
        // `INetworkCostManager::GetCost(DWORD *pCost, NLM_SOCKADDR *pDestIPAddr)`.
        let cost_hr = unsafe {
            let vtbl = *(guard.raw as *const *const INetworkCostManagerVtbl);
            ((*vtbl).get_cost)(guard.raw, &mut cost, ptr::null())
        };
        if cost_hr < 0 {
            None
        } else {
            Some(cost)
        }
    }
}

#[cfg(target_os = "linux")]
mod linux_metered {
    use std::io::Read;
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    use rusty_grid_core::capabilities::HOST_PROBE_TIMEOUT;
    use rusty_grid_core::cluster_auth::probe_command;

    use super::{parse_linux_nm_metered_output, MeteredStatus};

    fn run_probe_stdout(program: &str, args: &[&str]) -> Option<String> {
        let mut cmd = probe_command(program);
        cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::null());
        let mut child = cmd.spawn().ok()?;
        let deadline = Instant::now() + HOST_PROBE_TIMEOUT;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        return None;
                    }
                    let mut buf = Vec::new();
                    if let Some(mut out) = child.stdout.take() {
                        let _ = out.read_to_end(&mut buf);
                    }
                    return String::from_utf8(buf).ok();
                }
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return None;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
            }
        }
    }

    pub(super) fn query_linux_nm_metered() -> MeteredStatus {
        if let Some(out) = run_probe_stdout(
            "busctl",
            &[
                "get-property",
                "org.freedesktop.NetworkManager",
                "/org/freedesktop/NetworkManager",
                "org.freedesktop.NetworkManager",
                "Metered",
            ],
        ) {
            let status = parse_linux_nm_metered_output(&out);
            if status != MeteredStatus::Unknown {
                return status;
            }
        }
        if let Some(out) = run_probe_stdout("nmcli", &["-g", "GENERAL.METERED", "general"]) {
            return parse_linux_nm_metered_output(&out);
        }
        MeteredStatus::Unknown
    }
}

/// Snapshot of host sensors fed into [`LimitsEvaluator::evaluate`].
#[derive(Debug, Clone, Copy)]
pub struct LimitSample {
    pub now: Instant,
    pub local_now: NaiveDateTime,
    pub host_cpu_pct: Option<f32>,
    pub battery: BatteryStatus,
    pub metered: MeteredStatus,
}

/// Result of evaluating all contributor limits at a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoldEvaluation {
    /// Whether contributor limits currently hold new tasks back.
    pub held: bool,
    /// Generic category suitable for master refusal text (never reveals schedule or location).
    pub reason: Option<String>,
    /// Detailed explanation for local logs and same-user `status` inspection.
    pub local_detail: Option<String>,
    /// Whether already-running tasks should also be handed back (`--hold-ends-tasks`).
    pub hand_back_running: bool,
    /// `true` on the first evaluation when `--no-tasks-on-metered` is set and metered status is `Unknown`.
    pub metered_unknown_notice: bool,
}

/// Evaluates all D1 contributor limits (`local-max-cpu-pct`, `hours`, `max-task-hours-per-day`,
/// `no-tasks-on-battery`, `no-tasks-on-metered`, `no-gpu-tasks`) against injected or live readings.
#[derive(Debug)]
pub struct LimitsEvaluator {
    limits: WorkerLimits,
    schedule: Option<HoursSchedule>,
    cpu: CpuAverager,
    cpu_held: bool,
    budget: DailyTaskBudget,
    warned_metered_unknown: bool,
}

impl LimitsEvaluator {
    /// Builds a validated evaluator initialized on `initial_day`.
    pub fn new(limits: WorkerLimits, initial_day: NaiveDate) -> Result<Self, String> {
        Self::with_budget_path(limits, initial_day, None)
    }

    /// Builds a validated evaluator initialized on `initial_day` with optional persistent budget file.
    pub fn with_budget_path(
        limits: WorkerLimits,
        initial_day: NaiveDate,
        budget_path: Option<PathBuf>,
    ) -> Result<Self, String> {
        limits.validate()?;
        let schedule = limits
            .hours
            .as_deref()
            .map(HoursSchedule::parse)
            .transpose()?;
        let budget = match budget_path {
            Some(path) => DailyTaskBudget::with_state_path(initial_day, path),
            None => DailyTaskBudget::new(initial_day),
        };
        Ok(Self {
            limits,
            schedule,
            cpu: CpuAverager::default(),
            cpu_held: false,
            budget,
            warned_metered_unknown: false,
        })
    }

    /// Starts accounting wall-clock/monotonic runtime for an active task run.
    pub fn begin_active_task(
        &mut self,
        run_id: u64,
        started_local: NaiveDateTime,
        started_at: Instant,
    ) {
        self.budget
            .begin_active_task(run_id, started_local, started_at);
    }

    /// Finalizes accounting for a completed, cancelled, or handed-back active task run.
    pub fn finish_active_task(
        &mut self,
        run_id: u64,
        ended_local: NaiveDateTime,
        elapsed: Duration,
    ) {
        self.budget.finish_active_task(run_id, ended_local, elapsed);
    }

    /// Flushes any dirty daily budget state to disk.
    pub fn flush_budget(&mut self) {
        self.budget.flush();
    }

    /// Replaces active contributor limits (e.g. from `limits set`) after validation.
    pub fn update_limits(&mut self, limits: WorkerLimits) -> Result<(), String> {
        limits.validate()?;
        let schedule = limits
            .hours
            .as_deref()
            .map(HoursSchedule::parse)
            .transpose()?;
        if !limits.no_tasks_on_metered {
            self.warned_metered_unknown = false;
        }
        if limits.local_max_cpu_pct.is_none() {
            self.cpu_held = false;
        }
        self.limits = limits;
        self.schedule = schedule;
        Ok(())
    }

    /// Current validated limits.
    pub fn limits(&self) -> &WorkerLimits {
        &self.limits
    }

    /// Records runtime from a completed task toward the daily cap.
    pub fn record_completed_task(&mut self, local_day: NaiveDate, duration: Duration) {
        self.budget.record_task_duration(local_day, duration);
    }

    /// Cumulative hours accumulated so far on `local_day`.
    pub fn used_task_hours_today(&mut self, local_day: NaiveDate) -> f64 {
        self.budget.used_hours(local_day)
    }

    /// Evaluates whether `--no-gpu-tasks` blocks an incoming assignment.
    pub fn check_gpu_admission(&self, requires_gpu: bool) -> Result<(), &'static str> {
        if requires_gpu && self.limits.no_gpu_tasks {
            Err(NO_GPU_TASKS_REJECTION_MESSAGE)
        } else {
            Ok(())
        }
    }

    fn update_cpu_hold(&mut self, now: Instant) -> Option<String> {
        let Some(limit_pct) = self.limits.local_max_cpu_pct else {
            self.cpu_held = false;
            return None;
        };
        let avg = self.cpu.average(now)?;
        let threshold = f32::from(limit_pct);
        let release_below = (threshold - CPU_RELEASE_HYSTERESIS_PCT).max(0.0);
        if self.cpu_held {
            let cleared = if release_below > 0.0 {
                avg < release_below
            } else {
                avg <= 0.0
            };
            if cleared {
                self.cpu_held = false;
            }
        } else if avg > threshold {
            self.cpu_held = true;
        }
        self.cpu_held.then(|| {
            format!(
                "30s average host CPU {avg:.1}% > limit {limit_pct}% (release < {release_below:.0}%)"
            )
        })
    }

    /// Evaluates automatic hold limits against `sample`.
    pub fn evaluate(&mut self, sample: LimitSample) -> HoldEvaluation {
        let day = sample.local_now.date();
        self.budget
            .account_active_tasks(sample.local_now, sample.now);

        if let Some(cpu_pct) = sample.host_cpu_pct {
            self.cpu.record(sample.now, cpu_pct);
        }
        let cpu_hold_detail = self.update_cpu_hold(sample.now);

        let mut metered_unknown_notice = false;
        if self.limits.no_tasks_on_metered
            && sample.metered == MeteredStatus::Unknown
            && !self.warned_metered_unknown
        {
            self.warned_metered_unknown = true;
            metered_unknown_notice = true;
            info!(
                "Cannot determine whether the active network connection is metered; treating network as unmetered"
            );
        }

        let hand_back_running = self.limits.hold_ends_tasks;

        if self.limits.no_tasks_on_battery && sample.battery == BatteryStatus::OnBattery {
            return HoldEvaluation {
                held: true,
                reason: Some("on battery".to_string()),
                local_detail: Some("running on battery power".to_string()),
                hand_back_running,
                metered_unknown_notice,
            };
        }

        if self.limits.no_tasks_on_metered && sample.metered == MeteredStatus::Metered {
            return HoldEvaluation {
                held: true,
                reason: Some("metered network".to_string()),
                local_detail: Some("connected to a metered network".to_string()),
                hand_back_running,
                metered_unknown_notice,
            };
        }

        if let Some(sched) = &self.schedule {
            if !sched.is_allowed_local(sample.local_now) {
                return HoldEvaluation {
                    held: true,
                    reason: Some("outside allowed hours".to_string()),
                    local_detail: Some(format!(
                        "local time {} is outside allowed schedule",
                        sample.local_now.format("%a %H:%M")
                    )),
                    hand_back_running,
                    metered_unknown_notice,
                };
            }
        }

        if let Some(cap_hours) = self.limits.max_task_hours_per_day {
            if self.budget.is_exhausted(day, cap_hours) {
                let used = self.budget.used_hours(day);
                return HoldEvaluation {
                    held: true,
                    reason: Some("daily task-hours cap reached".to_string()),
                    local_detail: Some(format!(
                        "used {used:.2}h of {cap_hours:.2}h daily task-hours limit"
                    )),
                    hand_back_running,
                    metered_unknown_notice,
                };
            }
        }

        // CPU holds never hand back running tasks (`design 0113` / oscillation guard).
        if let Some(detail) = cpu_hold_detail {
            return HoldEvaluation {
                held: true,
                reason: Some("host cpu above limit".to_string()),
                local_detail: Some(detail),
                hand_back_running: false,
                metered_unknown_notice,
            };
        }

        HoldEvaluation {
            held: false,
            reason: None,
            local_detail: None,
            hand_back_running: false,
            metered_unknown_notice,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveTime;

    fn dt(y: i32, m: u32, d: u32, hh: u32, mm: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(hh, mm, 0).unwrap())
    }

    #[test]
    fn t1_hours_schedule_parses_weekly_overnight_wrap_and_2400() {
        let sched = HoursSchedule::parse("mon-fri 18:00-08:00; sat-sun 00:00-24:00").unwrap();

        // Monday morning before 08:00 is NOT allowed because Sunday does not have an overnight window.
        assert!(!sched.is_allowed_local(dt(2026, 11, 23, 7, 59)));
        // Monday evening from 18:00 onward IS allowed.
        assert!(!sched.is_allowed_local(dt(2026, 11, 23, 17, 59)));
        assert!(sched.is_allowed_local(dt(2026, 11, 23, 18, 0)));
        // Tuesday morning < 08:00 carries over from Monday's overnight span.
        assert!(sched.is_allowed_local(dt(2026, 11, 24, 7, 59)));
        assert!(!sched.is_allowed_local(dt(2026, 11, 24, 8, 0)));
        // Friday overnight spills into Saturday morning, and Sat/Sun are 00:00-24:00.
        assert!(sched.is_allowed_local(dt(2026, 11, 27, 23, 30)));
        assert!(sched.is_allowed_local(dt(2026, 11, 28, 12, 0)));
        assert!(sched.is_allowed_local(dt(2026, 11, 29, 23, 59)));

        // Wrap-around day range: fri-mon 20:00-22:00 covers Fri, Sat, Sun, Mon, not Wed.
        let wrap = HoursSchedule::parse("fri-mon 20:00-22:00").unwrap();
        assert!(wrap.is_allowed_local(dt(2026, 11, 27, 21, 0))); // Fri
        assert!(wrap.is_allowed_local(dt(2026, 11, 30, 21, 0))); // Mon
        assert!(!wrap.is_allowed_local(dt(2026, 11, 25, 21, 0))); // Wed

        // Bare time window applies all days.
        let bare = HoursSchedule::parse("09:00-17:00").unwrap();
        assert!(bare.is_allowed_local(dt(2026, 11, 25, 9, 0)));
        assert!(!bare.is_allowed_local(dt(2026, 11, 25, 17, 0)));

        // Invalid specs must error cleanly.
        for bad in [
            "",
            "mon 24:01-08:00",
            "mon 12:00-12:00",
            "24:00-08:00",
            "funday 09:00-17:00",
            "mon 09:00-17:00;",
        ] {
            assert!(
                HoursSchedule::parse(bad).is_err(),
                "expected parse failure for '{bad}'"
            );
        }
    }

    #[test]
    fn t1_hours_schedule_handles_dst_transitions_without_panic() {
        // Evaluate wall-clock hours on US spring-forward (2026-03-08) and fall-back (2026-11-01)
        // dates via chrono::NaiveDateTime directly.
        let sched = HoursSchedule::parse("sun 01:00-04:00").unwrap();
        assert!(sched.is_allowed_local(dt(2026, 3, 8, 1, 30)));
        assert!(sched.is_allowed_local(dt(2026, 3, 8, 3, 30)));
        assert!(!sched.is_allowed_local(dt(2026, 3, 8, 4, 0)));
        assert!(sched.is_allowed_local(dt(2026, 11, 1, 1, 59)));
        assert!(sched.is_allowed_local(dt(2026, 11, 1, 2, 0)));
    }

    #[test]
    fn t1_cpu_averager_30s_sliding_window_and_boundary() {
        let base = Instant::now();
        let mut avg = CpuAverager::new(CPU_AVERAGE_WINDOW);
        avg.record(base, 95.0);
        avg.record(base + Duration::from_secs(10), 90.0);
        // After 45 s, both old 95%/90% samples are > 30 s old and evicted.
        let t_later = base + Duration::from_secs(45);
        avg.record(t_later, 40.0);
        avg.record(t_later + Duration::from_secs(5), 60.0);
        let mean = avg.average(t_later + Duration::from_secs(5)).unwrap();
        assert!((mean - 50.0).abs() < 0.01, "expected 50.0, got {mean}");
        // Equal to threshold is allowed; strictly above holds.
        assert!(!avg.exceeds(t_later + Duration::from_secs(5), 50));
        assert!(avg.exceeds(t_later + Duration::from_secs(5), 49));
    }

    #[test]
    fn t1_daily_task_budget_accumulates_and_resets_at_local_midnight() {
        let day1 = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2026, 11, 27).unwrap();
        let mut budget = DailyTaskBudget::new(day1);
        budget.record_task_duration(day1, Duration::from_secs(3_600));
        assert!(!budget.is_exhausted(day1, 1.5));
        budget.record_task_duration(day1, Duration::from_secs(1_800));
        assert!(budget.is_exhausted(day1, 1.5));

        // Crossing local midnight into `day2` resets usage to zero.
        assert!(!budget.is_exhausted(day2, 1.5));
        assert_eq!(budget.used_hours(day2), 0.0);
    }

    #[test]
    fn t1_metered_parsers_cover_windows_linux_and_macos() {
        // Windows NLM_CONNECTION_COST constants
        assert_eq!(classify_windows_nlm_cost(0x1), MeteredStatus::Unmetered);
        assert_eq!(classify_windows_nlm_cost(0x2), MeteredStatus::Metered); // FIXED
        assert_eq!(classify_windows_nlm_cost(0x4), MeteredStatus::Metered); // VARIABLE
        assert_eq!(classify_windows_nlm_cost(0x10000), MeteredStatus::Metered); // OVERDATALIMIT
        assert_eq!(classify_windows_nlm_cost(0x40000), MeteredStatus::Metered); // ROAMING
        assert_eq!(classify_windows_nlm_cost(0x80000), MeteredStatus::Metered); // APPROACHINGDATALIMIT
        assert_eq!(classify_windows_nlm_cost(0), MeteredStatus::Unknown);

        // Linux NetworkManager outputs (busctl & nmcli)
        assert_eq!(
            parse_linux_nm_metered_output("u 1\n"),
            MeteredStatus::Metered
        );
        assert_eq!(
            parse_linux_nm_metered_output("u 3\n"),
            MeteredStatus::Metered
        );
        assert_eq!(
            parse_linux_nm_metered_output("u 2\n"),
            MeteredStatus::Unmetered
        );
        assert_eq!(
            parse_linux_nm_metered_output("u 4\n"),
            MeteredStatus::Unmetered
        );
        assert_eq!(
            parse_linux_nm_metered_output("u 0\n"),
            MeteredStatus::Unknown
        );
        assert_eq!(
            parse_linux_nm_metered_output("yes (guessed)"),
            MeteredStatus::Metered
        );
        assert_eq!(
            parse_linux_nm_metered_output("no (guessed)"),
            MeteredStatus::Unmetered
        );
        assert_eq!(parse_linux_nm_metered_output(""), MeteredStatus::Unknown);

        // macOS NWPath flags
        assert_eq!(
            parse_macos_path_flags(false, false),
            MeteredStatus::Unmetered
        );
        assert_eq!(parse_macos_path_flags(true, false), MeteredStatus::Metered);
        assert_eq!(parse_macos_path_flags(false, true), MeteredStatus::Metered);

        // Windows power status mapping
        assert_eq!(
            classify_windows_power_status(0, 1),
            BatteryStatus::OnBattery
        );
        assert_eq!(classify_windows_power_status(1, 1), BatteryStatus::OnAc);
        assert_eq!(
            classify_windows_power_status(1, 128),
            BatteryStatus::Unknown
        );
        assert_eq!(
            classify_windows_power_status(255, 0),
            BatteryStatus::Unknown
        );
    }

    #[test]
    fn t1_evaluator_holds_reasons_privacy_unknown_notice_and_master_rejection_compat() {
        let base = Instant::now();
        let day = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();
        let limits = WorkerLimits {
            local_max_cpu_pct: Some(70),
            hours: Some("thu 10:00-12:00".to_string()),
            max_task_hours_per_day: Some(1.0),
            no_tasks_on_battery: true,
            no_tasks_on_metered: true,
            no_gpu_tasks: true,
            hold_ends_tasks: false,
        };
        let mut eval = LimitsEvaluator::new(limits, day).unwrap();

        // Unknown metered logs notice once, treats network as unmetered.
        let allowed = eval.evaluate(LimitSample {
            now: base,
            local_now: dt(2026, 11, 26, 11, 0),
            host_cpu_pct: Some(25.0),
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unknown,
        });
        assert!(!allowed.held);
        assert!(allowed.metered_unknown_notice);

        let allowed_again = eval.evaluate(LimitSample {
            now: base + Duration::from_secs(10),
            local_now: dt(2026, 11, 26, 11, 1),
            host_cpu_pct: Some(25.0),
            battery: BatteryStatus::Unknown,
            metered: MeteredStatus::Unknown,
        });
        assert!(!allowed_again.held);
        assert!(!allowed_again.metered_unknown_notice);

        // Outside allowed hours holds without leaking schedule details in refusal reason.
        let outside = eval.evaluate(LimitSample {
            now: base + Duration::from_secs(20),
            local_now: dt(2026, 11, 26, 13, 0),
            host_cpu_pct: Some(10.0),
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unmetered,
        });
        assert!(outside.held);
        let reason = outside.reason.as_deref().unwrap();
        assert!(!reason.contains("10:00") && !reason.contains("12:00"));
        let wire_msg = format_limit_rejection(reason);
        let paused_res = rusty_grid_core::task::TaskResult::failure(
            uuid::Uuid::new_v4(),
            rusty_grid_core::TaskId(uuid::Uuid::new_v4()),
            1,
            "",
            wire_msg.as_str(),
            0,
            Some(wire_msg.clone()),
        );
        assert!(
            rusty_grid_master::queue::is_worker_rejection(&paused_res),
            "master must classify '{wire_msg}' as uncharged worker rejection"
        );
        let no_gpu_res = rusty_grid_core::task::TaskResult::failure(
            uuid::Uuid::new_v4(),
            rusty_grid_core::TaskId(uuid::Uuid::new_v4()),
            1,
            "",
            NO_GPU_TASKS_REJECTION_MESSAGE,
            0,
            Some(NO_GPU_TASKS_REJECTION_MESSAGE.to_string()),
        );
        assert!(
            rusty_grid_master::queue::is_worker_rejection(&no_gpu_res),
            "master must classify --no-gpu-tasks rejection as uncharged worker rejection"
        );

        // GPU admission blocks only GPU tasks.
        assert!(eval.check_gpu_admission(false).is_ok());
        assert_eq!(
            eval.check_gpu_admission(true).unwrap_err(),
            NO_GPU_TASKS_REJECTION_MESSAGE
        );

        // Battery and metered holds take effect cleanly.
        let on_batt = eval.evaluate(LimitSample {
            now: base + Duration::from_secs(25),
            local_now: dt(2026, 11, 26, 11, 5),
            host_cpu_pct: Some(10.0),
            battery: BatteryStatus::OnBattery,
            metered: MeteredStatus::Unmetered,
        });
        assert_eq!(on_batt.reason.as_deref(), Some("on battery"));

        let on_metered = eval.evaluate(LimitSample {
            now: base + Duration::from_secs(30),
            local_now: dt(2026, 11, 26, 11, 6),
            host_cpu_pct: Some(10.0),
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Metered,
        });
        assert_eq!(on_metered.reason.as_deref(), Some("metered network"));
    }

    #[cfg(windows)]
    #[test]
    fn t4_live_windows_network_cost_and_battery_probe() {
        let (metered, raw_cost) = SystemMeteredNetworkProbe.probe_metered_with_raw();
        let battery = SystemBatteryProbe.probe_battery();
        println!(
            "T4_WINDOWS_LIVE: raw_nlm_cost={raw_cost:?} metered={metered:?} battery={battery:?}"
        );
        if let Some(cost) = raw_cost {
            assert_eq!(metered, classify_windows_nlm_cost(cost));
        } else {
            assert_eq!(metered, MeteredStatus::Unknown);
        }
    }

    #[cfg(unix)]
    #[test]
    fn persisted_daily_budget_refuses_symlinks_and_enforces_0600_mode() {
        use std::os::unix::fs::{symlink, MetadataExt};

        let dir = tempfile::tempdir().unwrap();
        let real_path = dir.path().join("daily-budget.json");
        let day = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();

        let mut eval = LimitsEvaluator::with_budget_path(
            WorkerLimits::default(),
            day,
            Some(real_path.clone()),
        )
        .unwrap();
        eval.record_completed_task(day, Duration::from_secs(60));
        let mode = std::fs::metadata(&real_path).unwrap().mode() & 0o777;
        assert_eq!(mode, 0o600, "budget file must have mode 0600");

        let link_path = dir.path().join("daily-budget-link.json");
        symlink(&real_path, &link_path).unwrap();
        assert!(
            load_persisted_budget(&link_path).is_err(),
            "symlink budget path must be refused"
        );
    }

    #[test]
    fn daily_cap_counts_running_tasks_splits_midnight_and_counts_cancelled() {
        let day1 = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2026, 11, 27).unwrap();
        let t0 = Instant::now();
        let limits = WorkerLimits {
            max_task_hours_per_day: Some(0.1), // 6 minutes
            ..Default::default()
        };
        let mut eval = LimitsEvaluator::new(limits, day1).unwrap();

        // Task starts at 23:55 on day1; after 4 running minutes at 23:59 it has not exceeded 6m.
        eval.begin_active_task(1, dt(2026, 11, 26, 23, 55), t0);
        let before_cap = eval.evaluate(LimitSample {
            now: t0 + Duration::from_secs(240),
            local_now: dt(2026, 11, 26, 23, 59),
            host_cpu_pct: None,
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unmetered,
        });
        assert!(!before_cap.held);
        assert!(
            (eval.used_task_hours_today(day1) - (4.0 / 60.0)).abs() < 0.002,
            "running task must accrue 4m on day1, got {}",
            eval.used_task_hours_today(day1)
        );

        // 3 minutes later at 00:02 on day2, total runtime would be 7m if unsplit, which MUST
        // split cleanly: 5m on day1 (23:55..24:00) and 2m on day2 (00:00..00:02 < 6m cap)!
        let cross_midnight = eval.evaluate(LimitSample {
            now: t0 + Duration::from_secs(420),
            local_now: dt(2026, 11, 27, 0, 2),
            host_cpu_pct: None,
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unmetered,
        });
        assert!(
            !cross_midnight.held,
            "2m after midnight must not trip 6m day2 cap: {cross_midnight:?}"
        );
        assert!(
            (eval.used_task_hours_today(day2) - (2.0 / 60.0)).abs() < 0.002,
            "expected ~2m accrued on day2 after midnight split, got {}",
            eval.used_task_hours_today(day2)
        );

        // Task is cancelled / handed back at 00:07 (5m more -> 7m total on day2 > 6m cap).
        eval.finish_active_task(1, dt(2026, 11, 27, 0, 7), Duration::from_secs(720));
        let after_cancel = eval.evaluate(LimitSample {
            now: t0 + Duration::from_secs(720),
            local_now: dt(2026, 11, 27, 0, 7),
            host_cpu_pct: None,
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unmetered,
        });
        assert!(
            after_cancel.held,
            "cancelled/handed-back runtime must push day2 over 6m cap"
        );
        assert_eq!(
            after_cancel.reason.as_deref(),
            Some("daily task-hours cap reached")
        );
    }

    #[test]
    fn daily_cap_persists_across_restart_and_resists_clock_step_back() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("daily-budget.json");
        let day1 = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2026, 11, 27).unwrap();
        let limits = WorkerLimits {
            max_task_hours_per_day: Some(0.1), // 6 minutes
            ..Default::default()
        };

        // First worker run records 7 minutes on day2 and persists.
        let mut eval1 =
            LimitsEvaluator::with_budget_path(limits.clone(), day2, Some(state_path.clone()))
                .unwrap();
        eval1.record_completed_task(day2, Duration::from_secs(420));
        assert!(eval1.used_task_hours_today(day2) >= 0.1);

        // Restart on same day2 reloads persisted usage and refuses further tasks immediately.
        let mut eval_same_day =
            LimitsEvaluator::with_budget_path(limits.clone(), day2, Some(state_path.clone()))
                .unwrap();
        let res_same = eval_same_day.evaluate(LimitSample {
            now: Instant::now(),
            local_now: dt(2026, 11, 27, 9, 0),
            host_cpu_pct: None,
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unmetered,
        });
        assert!(
            res_same.held,
            "persisted exhausted daily budget must stay held after same-day restart"
        );

        // Clock steps backward from day2 to day1: must NOT reset persisted day2 usage!
        let mut eval_step_back =
            LimitsEvaluator::with_budget_path(limits, day1, Some(state_path)).unwrap();
        let res_back = eval_step_back.evaluate(LimitSample {
            now: Instant::now(),
            local_now: dt(2026, 11, 26, 23, 30),
            host_cpu_pct: None,
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unmetered,
        });
        assert!(
            res_back.held,
            "backward clock step must not reset persisted daily budget"
        );
    }

    #[test]
    fn cpu_limit_hysteresis_prevents_oscillation_near_threshold() {
        let day = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();
        let t0 = Instant::now();
        let mut eval = LimitsEvaluator::new(
            WorkerLimits {
                local_max_cpu_pct: Some(50),
                hold_ends_tasks: false,
                ..Default::default()
            },
            day,
        )
        .unwrap();

        // Cross above 50% -> Held("host cpu above limit").
        let r1 = eval.evaluate(LimitSample {
            now: t0,
            local_now: dt(2026, 11, 26, 12, 0),
            host_cpu_pct: Some(65.0),
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unmetered,
        });
        assert!(r1.held);
        assert_eq!(r1.reason.as_deref(), Some("host cpu above limit"));
        assert!(!r1.hand_back_running);

        // Drop CPU to 45% (above limit - CPU_RELEASE_HYSTERESIS_PCT = 40%) over >30 s:
        // average is now 45%, which MUST stay held without oscillating!
        for step in [35u64, 45, 55, 65, 75] {
            let r = eval.evaluate(LimitSample {
                now: t0 + Duration::from_secs(step),
                local_now: dt(2026, 11, 26, 12, 1),
                host_cpu_pct: Some(45.0),
                battery: BatteryStatus::OnAc,
                metered: MeteredStatus::Unmetered,
            });
            assert!(
                r.held,
                "45% (>= 50 - {CPU_RELEASE_HYSTERESIS_PCT} = 40%) must remain held at +{step}s"
            );
            assert!(!r.hand_back_running);
        }

        // Drop CPU to 35% (< 40%) until 30 s average clears < 40% -> releases hold!
        let mut released = false;
        for step in [85u64, 95, 105, 115, 125] {
            let r = eval.evaluate(LimitSample {
                now: t0 + Duration::from_secs(step),
                local_now: dt(2026, 11, 26, 12, 2),
                host_cpu_pct: Some(35.0),
                battery: BatteryStatus::OnAc,
                metered: MeteredStatus::Unmetered,
            });
            if !r.held {
                released = true;
                break;
            }
        }
        assert!(
            released,
            "dropping below 40% over 30s must release CPU hold"
        );
    }

    #[test]
    fn cpu_limit_never_hands_back_running_tasks_even_with_hold_ends_tasks() {
        let day = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();
        let t0 = Instant::now();
        let mut eval = LimitsEvaluator::new(
            WorkerLimits {
                local_max_cpu_pct: Some(50),
                no_tasks_on_battery: true,
                hold_ends_tasks: true,
                ..Default::default()
            },
            day,
        )
        .unwrap();

        // CPU spike holds new tasks, keeps hysteresis, and NEVER sets hand_back_running=true.
        let r_spike = eval.evaluate(LimitSample {
            now: t0,
            local_now: dt(2026, 11, 26, 12, 0),
            host_cpu_pct: Some(80.0),
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unmetered,
        });
        assert!(r_spike.held);
        assert_eq!(r_spike.reason.as_deref(), Some("host cpu above limit"));
        assert!(
            !r_spike.hand_back_running,
            "CPU limit must never hand back running tasks even when --hold-ends-tasks is enabled"
        );

        let r_hyst = eval.evaluate(LimitSample {
            now: t0 + Duration::from_secs(40),
            local_now: dt(2026, 11, 26, 12, 1),
            host_cpu_pct: Some(44.0),
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unmetered,
        });
        let r_hyst2 = eval.evaluate(LimitSample {
            now: t0 + Duration::from_secs(75),
            local_now: dt(2026, 11, 26, 12, 1),
            host_cpu_pct: Some(44.0),
            battery: BatteryStatus::OnAc,
            metered: MeteredStatus::Unmetered,
        });
        assert!(r_hyst.held && r_hyst2.held);
        assert!(!r_hyst2.hand_back_running);

        // Battery hold with `--hold-ends-tasks` STILL hands back running tasks as expected.
        let r_batt = eval.evaluate(LimitSample {
            now: t0 + Duration::from_secs(80),
            local_now: dt(2026, 11, 26, 12, 2),
            host_cpu_pct: Some(10.0),
            battery: BatteryStatus::OnBattery,
            metered: MeteredStatus::Unmetered,
        });
        assert!(r_batt.held);
        assert_eq!(r_batt.reason.as_deref(), Some("on battery"));
        assert!(r_batt.hand_back_running);
    }

    #[test]
    fn subtract_worker_tree_cpu_pct_excludes_own_task_load() {
        // 8 logical CPUs: task tree uses 320% single-core sum = 40% of total host capacity.
        // Global host CPU = 75% => external non-worker CPU = 35% (< 50% threshold).
        let adjusted = subtract_worker_tree_cpu_pct(75.0, 320.0, 8);
        assert!(
            (adjusted - 35.0).abs() < 0.01,
            "expected 35.0% external CPU, got {adjusted}"
        );
        // Never goes negative if process delta lags slightly ahead of global reading.
        assert_eq!(subtract_worker_tree_cpu_pct(20.0, 500.0, 4), 0.0);
    }
}
