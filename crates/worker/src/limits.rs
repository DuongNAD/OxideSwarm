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

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike, Weekday};
use serde::{Deserialize, Serialize};
use tracing::info;

/// Default sliding window over which host CPU usage is averaged (`design 0113` D1).
pub const CPU_AVERAGE_WINDOW: Duration = Duration::from_secs(30);

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

    /// Returns the mean CPU % across unexpired samples as of `now`, or `None` if empty.
    pub fn average(&mut self, now: Instant) -> Option<f32> {
        self.prune_before(now);
        if self.samples.is_empty() {
            return None;
        }
        let sum: f64 = self.samples.iter().map(|(_, v)| f64::from(*v)).sum();
        Some((sum / self.samples.len() as f64) as f32)
    }

    /// Returns `true` when the sliding-window average strictly exceeds `limit_pct`.
    pub fn exceeds(&mut self, now: Instant, limit_pct: u8) -> bool {
        self.average(now)
            .is_some_and(|avg| avg > f32::from(limit_pct))
    }
}

/// Daily cumulative task runtime budget reset at local midnight (`design 0113` D1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyTaskBudget {
    day: NaiveDate,
    used_ms: u64,
}

impl DailyTaskBudget {
    /// Starts an empty daily counter anchored on `initial_day`.
    pub fn new(initial_day: NaiveDate) -> Self {
        Self {
            day: initial_day,
            used_ms: 0,
        }
    }

    /// Advances to `day`, resetting cumulative runtime if the local calendar date changed.
    pub fn advance_day(&mut self, day: NaiveDate) {
        if day != self.day {
            self.day = day;
            self.used_ms = 0;
        }
    }

    /// Adds completed task `duration` on `day` (resetting first if the day rolled over).
    pub fn record_task_duration(&mut self, day: NaiveDate, duration: Duration) {
        self.advance_day(day);
        let add_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.used_ms = self.used_ms.saturating_add(add_ms);
    }

    /// Returns total recorded task hours on `day` (resets to `0.0` if `day` is a new day).
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

impl MeteredNetworkProbe for SystemMeteredNetworkProbe {
    fn probe_metered(&self) -> MeteredStatus {
        #[cfg(windows)]
        {
            match windows_metered::query_windows_network_cost_raw() {
                Some(cost) => classify_windows_nlm_cost(cost),
                None => MeteredStatus::Unknown,
            }
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
    budget: DailyTaskBudget,
    warned_metered_unknown: bool,
}

impl LimitsEvaluator {
    /// Builds a validated evaluator initialized on `initial_day`.
    pub fn new(limits: WorkerLimits, initial_day: NaiveDate) -> Result<Self, String> {
        limits.validate()?;
        let schedule = limits
            .hours
            .as_deref()
            .map(HoursSchedule::parse)
            .transpose()?;
        Ok(Self {
            limits,
            schedule,
            cpu: CpuAverager::default(),
            budget: DailyTaskBudget::new(initial_day),
            warned_metered_unknown: false,
        })
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

    /// Evaluates automatic hold limits against `sample`.
    pub fn evaluate(&mut self, sample: LimitSample) -> HoldEvaluation {
        let day = sample.local_now.date();
        self.budget.advance_day(day);

        if let Some(cpu_pct) = sample.host_cpu_pct {
            self.cpu.record(sample.now, cpu_pct);
        }

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

        if let Some(limit_pct) = self.limits.local_max_cpu_pct {
            if let Some(avg) = self.cpu.average(sample.now) {
                if avg > f32::from(limit_pct) {
                    return HoldEvaluation {
                        held: true,
                        reason: Some("host cpu above limit".to_string()),
                        local_detail: Some(format!(
                            "30s average host CPU {avg:.1}% > limit {limit_pct}%"
                        )),
                        hand_back_running,
                        metered_unknown_notice,
                    };
                }
            }
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
        let raw_cost = windows_metered::query_windows_network_cost_raw();
        let metered = SystemMeteredNetworkProbe.probe_metered();
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
}
