//! Local contributor controls (`NET-9`, [design 0095](../../docs/design/0095-contributor-controls.md)).
//!
//! Lets each contributor cap host CPU usage, restrict allowed local-time weekly windows and daily
//! cumulative task hours, pause automatically on battery or metered networks, and toggle a local
//! pause switch (`oxideswarm worker pause|resume|status`) that ends running tasks within `5 s`
//! without charging a task retry or dropping the master session.

use std::fmt;
use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{Datelike, Local, NaiveDate, Timelike, Weekday};
use tokio::sync::Notify;

use rusty_grid_core::capabilities::detect_live_mobile_status;
use rusty_grid_core::contributor_control_refusal;

use crate::heartbeat::MobileTelemetryHandle;
use crate::identity::{default_state_dir, sanitize_name};

#[cfg(test)]
mod tests;

/// Maximum time the worker waits for running process trees to exit after `worker pause` before
/// force-aborting the tasks (`NET-9` / design 0095 D5).
pub const PAUSE_STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// How often an active session polls the pause control file on disk.
pub const CONTROL_FILE_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How often an active session re-probes battery / metered network state when those controls are
/// enabled.
pub const RESOURCE_PROBE_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Source of power for the host machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PowerSource {
    Ac = 0,
    Battery = 1,
    Unknown = 2,
}

impl PowerSource {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Ac,
            1 => Self::Battery,
            _ => Self::Unknown,
        }
    }
}

/// Network metering status reported by the OS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NetworkCost {
    Unmetered = 0,
    Metered = 1,
    Unsupported = 2,
}

impl NetworkCost {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Unmetered,
            1 => Self::Metered,
            _ => Self::Unsupported,
        }
    }
}

/// Injectable local clock for schedule windows and daily cumulative task-hour resets.
pub trait Clock: Send + Sync {
    fn local_now(&self) -> chrono::DateTime<Local>;
}

/// System clock backed by `chrono::Local::now()`.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn local_now(&self) -> chrono::DateTime<Local> {
        Local::now()
    }
}

/// Injectable host CPU usage percentage reader (`0.0..=100.0`).
pub trait HostCpuProbe: Send + Sync {
    fn cpu_usage_pct(&self) -> f32;
    /// Called by the background heartbeat telemetry sampler when a live reading arrives. Test
    /// probes keep the default no-op so real host CPU activity cannot overwrite injected values.
    fn record_sample(&self, _pct: f32) {}
}

/// Default CPU probe updated asynchronously by the background `sysinfo` telemetry sampler.
#[derive(Debug, Default)]
pub struct AtomicHostCpuProbe {
    bits: AtomicU32,
}

impl AtomicHostCpuProbe {
    pub fn new(initial_pct: f32) -> Self {
        Self {
            bits: AtomicU32::new(initial_pct.to_bits()),
        }
    }

    pub fn set(&self, pct: f32) {
        self.bits.store(pct.to_bits(), Ordering::Release);
    }
}

impl HostCpuProbe for AtomicHostCpuProbe {
    fn cpu_usage_pct(&self) -> f32 {
        f32::from_bits(self.bits.load(Ordering::Acquire))
    }

    fn record_sample(&self, pct: f32) {
        self.set(pct);
    }
}

/// Injectable power source probe (`AC`, `Battery`, or `Unknown`).
pub trait PowerProbe: Send + Sync {
    fn power_source(&self) -> PowerSource;
    fn refresh_blocking(&self) {}
}

/// System power probe cached in an `AtomicU8` so admission checks never spawn processes inline.
pub struct CachedSystemPowerProbe {
    cached: AtomicU8,
    mobile: Option<MobileTelemetryHandle>,
    no_battery: bool,
}

impl CachedSystemPowerProbe {
    pub fn new(mobile: Option<MobileTelemetryHandle>, no_battery: bool) -> Self {
        let initial = if no_battery {
            PowerSource::Unknown
        } else if let Some(status) = mobile.as_ref().and_then(MobileTelemetryHandle::live_status) {
            power_from_is_charging(status.is_charging)
        } else {
            PowerSource::Unknown
        };
        Self {
            cached: AtomicU8::new(initial as u8),
            mobile,
            no_battery,
        }
    }
}

impl PowerProbe for CachedSystemPowerProbe {
    fn power_source(&self) -> PowerSource {
        if self.no_battery {
            return PowerSource::Unknown;
        }
        if let Some(ref mob) = self.mobile {
            if mob.is_externally_fed() {
                if let Some(status) = mob.live_status() {
                    return power_from_is_charging(status.is_charging);
                }
            }
        }
        PowerSource::from_u8(self.cached.load(Ordering::Acquire))
    }

    fn refresh_blocking(&self) {
        if self.no_battery {
            self.cached
                .store(PowerSource::Unknown as u8, Ordering::Release);
            return;
        }
        if let Some(ref mob) = self.mobile {
            if mob.is_externally_fed() {
                if let Some(status) = mob.live_status() {
                    let src = power_from_is_charging(status.is_charging);
                    self.cached.store(src as u8, Ordering::Release);
                    return;
                }
            }
        }
        let detected = detect_system_power_source_blocking();
        self.cached.store(detected as u8, Ordering::Release);
    }
}

fn power_from_is_charging(is_charging: Option<bool>) -> PowerSource {
    match is_charging {
        Some(true) => PowerSource::Ac,
        Some(false) => PowerSource::Battery,
        None => PowerSource::Unknown,
    }
}

/// Injectable network metering probe (`Unmetered`, `Metered`, or `Unsupported`).
pub trait NetworkCostProbe: Send + Sync {
    fn network_cost(&self) -> NetworkCost;
    fn refresh_blocking(&self) {}
}

/// System metered-network probe cached in an `AtomicU8` and refreshed on `spawn_blocking`.
pub struct CachedSystemNetworkCostProbe {
    cached: AtomicU8,
}

impl Default for CachedSystemNetworkCostProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl CachedSystemNetworkCostProbe {
    pub fn new() -> Self {
        Self {
            cached: AtomicU8::new(NetworkCost::Unsupported as u8),
        }
    }
}

impl NetworkCostProbe for CachedSystemNetworkCostProbe {
    fn network_cost(&self) -> NetworkCost {
        NetworkCost::from_u8(self.cached.load(Ordering::Acquire))
    }

    fn refresh_blocking(&self) {
        let detected = detect_network_cost_blocking();
        self.cached.store(detected as u8, Ordering::Release);
    }
}

/// Single day-of-week + minute-of-day half-open window `[start_minute, end_minute)`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DayWindow {
    weekday: Weekday,
    start_minute: u16,
    end_minute: u16,
}

/// Compiled weekly local-time schedule parsed from `--allow-hours`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllowHoursSchedule {
    raw: String,
    windows: Vec<DayWindow>,
}

impl AllowHoursSchedule {
    /// Parses clauses such as `"Mon-Fri 22:00-07:00, Sat-Sun 00:00-24:00"` or `"Mon,Wed 09:00-17:00"`.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            return Err("--allow-hours cannot be empty".to_string());
        }
        let mut windows = Vec::new();
        for segment in trimmed.split(';') {
            let segment = segment.trim();
            if segment.is_empty() {
                continue;
            }
            parse_schedule_segment(segment, &mut windows)?;
        }
        if windows.is_empty() {
            return Err(format!(
                "invalid --allow-hours schedule {spec:?}: expected '<Days> <HH:MM>-<HH:MM>'"
            ));
        }
        Ok(Self {
            raw: trimmed.to_string(),
            windows,
        })
    }

    /// True if `dt` (in local wall-clock time) falls inside any allowed window.
    pub fn contains(&self, dt: chrono::DateTime<Local>) -> bool {
        let weekday = dt.weekday();
        let minute = (dt.hour() * 60 + dt.minute()) as u16;
        self.windows
            .iter()
            .any(|w| w.weekday == weekday && minute >= w.start_minute && minute < w.end_minute)
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }
}

impl fmt::Display for AllowHoursSchedule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.raw)
    }
}

fn is_time_range_token(tok: &str) -> bool {
    let clean = tok.trim_end_matches(',');
    let Some((a, b)) = clean.split_once('-') else {
        return false;
    };
    a.contains(':') && b.contains(':')
}

fn parse_schedule_segment(segment: &str, out: &mut Vec<DayWindow>) -> Result<(), String> {
    let mut day_tokens: Vec<String> = Vec::new();
    let mut parsed_any = false;
    for raw_tok in segment.split_whitespace() {
        let clean_comma = raw_tok.trim_end_matches(',').trim();
        if clean_comma.is_empty() {
            continue;
        }
        if is_time_range_token(clean_comma) {
            if day_tokens.is_empty() {
                return Err(format!(
                    "missing day specification before time window {clean_comma:?} in {segment:?}"
                ));
            }
            let days = parse_days_spec(&day_tokens.join(","))?;
            let (start_min, end_min) = parse_time_range(clean_comma)?;
            expand_windows(&days, start_min, end_min, out);
            day_tokens.clear();
            parsed_any = true;
        } else {
            day_tokens.push(clean_comma.to_string());
        }
    }
    if !day_tokens.is_empty() || !parsed_any {
        return Err(format!(
            "incomplete schedule clause {segment:?}: expected '<Days> <HH:MM>-<HH:MM>'"
        ));
    }
    Ok(())
}

fn parse_days_spec(spec: &str) -> Result<Vec<Weekday>, String> {
    let mut days = Vec::new();
    for part in spec.split(',') {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        let lower = p.to_ascii_lowercase();
        if matches!(lower.as_str(), "daily" | "all") {
            for wd in [
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
                Weekday::Sat,
                Weekday::Sun,
            ] {
                if !days.contains(&wd) {
                    days.push(wd);
                }
            }
        } else if lower == "weekdays" {
            for wd in [
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ] {
                if !days.contains(&wd) {
                    days.push(wd);
                }
            }
        } else if lower == "weekends" {
            for wd in [Weekday::Sat, Weekday::Sun] {
                if !days.contains(&wd) {
                    days.push(wd);
                }
            }
        } else if let Some((start_s, end_s)) = p.split_once('-') {
            let start = parse_weekday(start_s)?;
            let end = parse_weekday(end_s)?;
            let mut cur = start;
            loop {
                if !days.contains(&cur) {
                    days.push(cur);
                }
                if cur == end {
                    break;
                }
                cur = cur.succ();
            }
        } else {
            let wd = parse_weekday(p)?;
            if !days.contains(&wd) {
                days.push(wd);
            }
        }
    }
    if days.is_empty() {
        return Err(format!("no valid days in {spec:?}"));
    }
    Ok(days)
}

fn parse_weekday(raw: &str) -> Result<Weekday, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "mon" | "monday" => Ok(Weekday::Mon),
        "tue" | "tues" | "tuesday" => Ok(Weekday::Tue),
        "wed" | "wednesday" => Ok(Weekday::Wed),
        "thu" | "thur" | "thurs" | "thursday" => Ok(Weekday::Thu),
        "fri" | "friday" => Ok(Weekday::Fri),
        "sat" | "saturday" => Ok(Weekday::Sat),
        "sun" | "sunday" => Ok(Weekday::Sun),
        _ => Err(format!(
            "unknown day {raw:?} (expected Mon..Sun, Daily, Weekdays, or Weekends)"
        )),
    }
}

fn parse_hhmm(raw: &str) -> Result<u16, String> {
    let (h_s, m_s) = raw
        .trim()
        .split_once(':')
        .ok_or_else(|| format!("expected HH:MM, got {raw:?}"))?;
    if m_s.len() != 2 {
        return Err(format!("minute must have 2 digits in {raw:?}"));
    }
    let h: u16 = h_s
        .parse()
        .map_err(|_| format!("invalid hour in {raw:?}"))?;
    let m: u16 = m_s
        .parse()
        .map_err(|_| format!("invalid minute in {raw:?}"))?;
    if h > 24 || m >= 60 || (h == 24 && m != 0) {
        return Err(format!("time out of range in {raw:?} (max 24:00)"));
    }
    Ok(h * 60 + m)
}

fn parse_time_range(raw: &str) -> Result<(u16, u16), String> {
    let (start_s, end_s) = raw
        .split_once('-')
        .ok_or_else(|| format!("expected HH:MM-HH:MM, got {raw:?}"))?;
    let start = parse_hhmm(start_s)?;
    let end = parse_hhmm(end_s)?;
    if start == 1440 {
        return Err(format!("start time cannot be 24:00 in {raw:?}"));
    }
    if start == end {
        return Err(format!(
            "empty zero-width window {raw:?} (use 00:00-24:00 for a full day)"
        ));
    }
    Ok((start, end))
}

fn expand_windows(days: &[Weekday], start: u16, end: u16, out: &mut Vec<DayWindow>) {
    for &day in days {
        if start < end {
            out.push(DayWindow {
                weekday: day,
                start_minute: start,
                end_minute: end,
            });
        } else {
            out.push(DayWindow {
                weekday: day,
                start_minute: start,
                end_minute: 1440,
            });
            if end > 0 {
                out.push(DayWindow {
                    weekday: day.succ(),
                    start_minute: 0,
                    end_minute: end,
                });
            }
        }
    }
}

/// Validates `--max-host-cpu-pct` (`1.0..=100.0`, finite).
pub fn validate_max_host_cpu_pct(pct: f32) -> Result<f32, String> {
    if !pct.is_finite() || !(1.0..=100.0).contains(&pct) {
        return Err(format!(
            "--max-host-cpu-pct must be between 1.0 and 100.0 (got {pct})"
        ));
    }
    Ok(pct)
}

/// Validates `--daily-task-hours` (finite, strictly positive).
pub fn validate_daily_task_hours(hours: f64) -> Result<f64, String> {
    if !hours.is_finite() || hours <= 0.0 {
        return Err(format!(
            "--daily-task-hours must be a finite number > 0.0 (got {hours})"
        ));
    }
    Ok(hours)
}

/// Contributor-enforced local resource and pause configuration.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContributorControlsConfig {
    pub max_host_cpu_pct: Option<f32>,
    pub allow_hours: Option<AllowHoursSchedule>,
    pub daily_task_hours: Option<f64>,
    pub pause_on_battery: bool,
    pub pause_on_metered: bool,
    pub control_file: Option<PathBuf>,
}

impl ContributorControlsConfig {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(pct) = self.max_host_cpu_pct {
            validate_max_host_cpu_pct(pct)?;
        }
        if let Some(hours) = self.daily_task_hours {
            validate_daily_task_hours(hours)?;
        }
        Ok(())
    }

    pub fn is_any_limit_configured(&self) -> bool {
        self.max_host_cpu_pct.is_some()
            || self.allow_hours.is_some()
            || self.daily_task_hours.is_some()
            || self.pause_on_battery
            || self.pause_on_metered
    }
}

/// Which contributor control is currently holding tasks back.
#[derive(Debug, Clone, PartialEq)]
pub enum BlockReason {
    Paused,
    HostCpu { usage_pct: f32, limit_pct: f32 },
    OutsideHours { schedule: String },
    DailyHoursLimit { used_secs: f64, limit_hours: f64 },
    OnBattery,
    MeteredNetwork,
}

impl BlockReason {
    pub fn control_name(&self) -> &'static str {
        match self {
            Self::Paused => "local_pause",
            Self::HostCpu { .. } => "max_host_cpu_pct",
            Self::OutsideHours { .. } => "allow_hours",
            Self::DailyHoursLimit { .. } => "daily_task_hours",
            Self::OnBattery => "pause_on_battery",
            Self::MeteredNetwork => "pause_on_metered",
        }
    }

    pub fn refusal_message(&self) -> String {
        contributor_control_refusal(self)
    }
}

impl fmt::Display for BlockReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Paused => f.write_str("paused by contributor"),
            Self::HostCpu {
                usage_pct,
                limit_pct,
            } => write!(
                f,
                "max-host-cpu-pct exceeded ({usage_pct:.1}% > {limit_pct:.1}%)"
            ),
            Self::OutsideHours { schedule } => {
                write!(f, "outside --allow-hours ({schedule})")
            }
            Self::DailyHoursLimit {
                used_secs,
                limit_hours,
            } => write!(
                f,
                "daily-task-hours limit reached ({:.2}h >= {:.2}h)",
                used_secs / 3600.0,
                limit_hours
            ),
            Self::OnBattery => f.write_str("pause-on-battery active"),
            Self::MeteredNetwork => f.write_str("pause-on-metered active"),
        }
    }
}

/// Edge transition on the local pause state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseTransition {
    Unchanged,
    BecamePaused,
    BecameResumed,
}

#[derive(Debug)]
struct DailyUsage {
    date: NaiveDate,
    used_ms: u64,
}

/// Runtime evaluator and controller for all `NET-9` contributor controls on a worker.
pub struct WorkerControls {
    config: ContributorControlsConfig,
    clock: Arc<dyn Clock>,
    cpu_probe: Arc<dyn HostCpuProbe>,
    power_probe: Arc<dyn PowerProbe>,
    network_probe: Arc<dyn NetworkCostProbe>,
    daily_usage: Mutex<DailyUsage>,
    paused: AtomicBool,
    last_file_present: AtomicBool,
    /// Pending pause-transition notification observed by `run_session`.
    pending_transition: AtomicU8, // 0 = none, 1 = became paused, 2 = became resumed
    state_notify: Arc<Notify>,
    pause_notify: Arc<Notify>,
    total_completed_tasks_ms: AtomicU64,
}

pub type WorkerControlsHandle = Arc<WorkerControls>;

impl fmt::Debug for WorkerControls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerControls")
            .field("config", &self.config)
            .field("paused", &self.paused.load(Ordering::Relaxed))
            .finish()
    }
}

impl WorkerControls {
    pub fn new(
        config: ContributorControlsConfig,
        mobile: Option<MobileTelemetryHandle>,
        no_battery: bool,
    ) -> Arc<Self> {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let cpu_probe: Arc<dyn HostCpuProbe> = Arc::new(AtomicHostCpuProbe::new(0.0));
        let power_probe: Arc<dyn PowerProbe> =
            Arc::new(CachedSystemPowerProbe::new(mobile, no_battery));
        let network_probe: Arc<dyn NetworkCostProbe> =
            Arc::new(CachedSystemNetworkCostProbe::new());
        Self::with_probes(config, clock, cpu_probe, power_probe, network_probe)
    }

    pub fn with_probes(
        config: ContributorControlsConfig,
        clock: Arc<dyn Clock>,
        cpu_probe: Arc<dyn HostCpuProbe>,
        power_probe: Arc<dyn PowerProbe>,
        network_probe: Arc<dyn NetworkCostProbe>,
    ) -> Arc<Self> {
        let initial_file = config
            .control_file
            .as_deref()
            .and_then(|p| is_pause_file_present(p).ok())
            .unwrap_or(false);
        let today = clock.local_now().date_naive();
        Arc::new(Self {
            config,
            clock,
            cpu_probe,
            power_probe,
            network_probe,
            daily_usage: Mutex::new(DailyUsage {
                date: today,
                used_ms: 0,
            }),
            paused: AtomicBool::new(initial_file),
            last_file_present: AtomicBool::new(initial_file),
            pending_transition: AtomicU8::new(if initial_file { 1 } else { 0 }),
            state_notify: Arc::new(Notify::new()),
            pause_notify: Arc::new(Notify::new()),
            total_completed_tasks_ms: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &ContributorControlsConfig {
        &self.config
    }

    pub fn state_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.state_notify)
    }

    pub fn pause_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.pause_notify)
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// Triggers an immediate in-process pause (matching `worker pause`) and wakes session/heartbeat.
    pub fn pause(&self) {
        if !self.paused.swap(true, Ordering::AcqRel) {
            self.pending_transition.store(1, Ordering::Release);
            self.state_notify.notify_waiters();
            self.pause_notify.notify_waiters();
        }
    }

    /// Clears an in-process pause (matching `worker resume`) and wakes heartbeat.
    pub fn resume(&self) {
        if self.paused.swap(false, Ordering::AcqRel) {
            self.pending_transition.store(2, Ordering::Release);
            self.state_notify.notify_waiters();
            self.pause_notify.notify_waiters();
        }
    }

    /// Drains any pending pause/resume transition triggered via [`Self::pause`] / [`Self::resume`].
    pub fn take_pause_transition(&self) -> PauseTransition {
        match self.pending_transition.swap(0, Ordering::AcqRel) {
            1 => PauseTransition::BecamePaused,
            2 => PauseTransition::BecameResumed,
            _ => PauseTransition::Unchanged,
        }
    }

    /// Polls `control_file` if configured and folds edge transitions into the pause state.
    pub fn sync_pause_file(&self) -> PauseTransition {
        if let Some(ref path) = self.config.control_file {
            let file_present = match is_pause_file_present(path) {
                Ok(present) => present,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "Ignoring unreadable/unsafe pause control file");
                    return self.take_pause_transition();
                }
            };
            let prev_file = self.last_file_present.swap(file_present, Ordering::AcqRel);
            if file_present && !prev_file {
                self.pause();
            } else if !file_present && prev_file {
                self.resume();
            }
        }
        self.take_pause_transition()
    }

    /// Updates the live host CPU percentage from the background telemetry sampler.
    pub fn record_host_cpu_sample(&self, pct: f32) {
        self.cpu_probe.record_sample(pct);
    }

    /// Current host CPU percentage reported by the configured [`HostCpuProbe`].
    pub fn host_cpu_pct(&self) -> f32 {
        self.cpu_probe.cpu_usage_pct()
    }

    /// Refreshes blocking OS probes (`battery`, `metered network`) for enabled controls.
    /// Returns `true` if admission state or probe state changed meaningfully.
    pub fn refresh_os_probes_blocking(&self) -> bool {
        let prev_power = self.power_probe.power_source();
        let prev_net = self.network_probe.network_cost();
        if self.config.pause_on_battery {
            self.power_probe.refresh_blocking();
        }
        if self.config.pause_on_metered {
            self.network_probe.refresh_blocking();
        }
        let changed = self.power_probe.power_source() != prev_power
            || self.network_probe.network_cost() != prev_net;
        if changed {
            self.state_notify.notify_waiters();
        }
        changed
    }

    /// Records completed task execution duration toward today's `--daily-task-hours` budget.
    pub fn record_completed_task(&self, duration: Duration) {
        let add_ms = duration.as_millis().min(u64::MAX as u128) as u64;
        self.total_completed_tasks_ms
            .fetch_add(add_ms, Ordering::Relaxed);
        let today = self.clock.local_now().date_naive();
        let mut usage = self
            .daily_usage
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if usage.date != today {
            usage.date = today;
            usage.used_ms = 0;
        }
        usage.used_ms = usage.used_ms.saturating_add(add_ms);
        if let Some(limit_hours) = self.config.daily_task_hours {
            if (usage.used_ms as f64) / 3_600_000.0 >= limit_hours {
                self.state_notify.notify_waiters();
            }
        }
    }

    pub fn used_task_hours_today(&self) -> f64 {
        let today = self.clock.local_now().date_naive();
        let mut usage = self
            .daily_usage
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if usage.date != today {
            usage.date = today;
            usage.used_ms = 0;
        }
        usage.used_ms as f64 / 3_600_000.0
    }

    /// Evaluates every enabled contributor control in priority order.
    pub fn check_admission(&self) -> Result<(), BlockReason> {
        if self.is_paused() {
            return Err(BlockReason::Paused);
        }
        if let Some(limit_pct) = self.config.max_host_cpu_pct {
            let usage_pct = self.cpu_probe.cpu_usage_pct();
            if usage_pct > limit_pct {
                return Err(BlockReason::HostCpu {
                    usage_pct,
                    limit_pct,
                });
            }
        }
        let now = self.clock.local_now();
        if let Some(ref schedule) = self.config.allow_hours {
            if !schedule.contains(now) {
                return Err(BlockReason::OutsideHours {
                    schedule: schedule.raw().to_string(),
                });
            }
        }
        if let Some(limit_hours) = self.config.daily_task_hours {
            let today = now.date_naive();
            let used_ms = {
                let mut usage = self
                    .daily_usage
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if usage.date != today {
                    usage.date = today;
                    usage.used_ms = 0;
                }
                usage.used_ms
            };
            let used_hours = used_ms as f64 / 3_600_000.0;
            if used_hours >= limit_hours {
                return Err(BlockReason::DailyHoursLimit {
                    used_secs: used_ms as f64 / 1000.0,
                    limit_hours,
                });
            }
        }
        if self.config.pause_on_battery && self.power_probe.power_source() == PowerSource::Battery {
            return Err(BlockReason::OnBattery);
        }
        if self.config.pause_on_metered && self.network_probe.network_cost() == NetworkCost::Metered
        {
            return Err(BlockReason::MeteredNetwork);
        }
        Ok(())
    }

    /// Human-readable startup notices when `--pause-on-metered` or `--pause-on-battery` cannot
    /// read a sensor on this host.
    pub fn startup_diagnostics(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.config.pause_on_battery && self.power_probe.power_source() == PowerSource::Unknown {
            out.push(
                "--pause-on-battery is enabled, but no readable battery sensor was detected on this machine"
                    .to_string(),
            );
        }
        if self.config.pause_on_metered
            && self.network_probe.network_cost() == NetworkCost::Unsupported
        {
            out.push(
                "--pause-on-metered is enabled, but this OS/network setup does not report metered status (pause-on-metered stays inactive while unsupported)"
                    .to_string(),
            );
        }
        out
    }
}

/// Default local stop-switch control file path: `<OXIDESWARM_HOME>/worker_pause[.<sanitized_name>]`
/// (reusing [`crate::identity`] state directory and name sanitization).
pub fn default_control_file_path(worker_name: Option<&str>) -> Option<PathBuf> {
    let dir = default_state_dir()?;
    let file = match worker_name.map(sanitize_name).filter(|n| !n.is_empty()) {
        Some(n) => format!("worker_pause.{n}"),
        None => "worker_pause".to_string(),
    };
    Some(dir.join(file))
}

fn reject_symlink(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("refusing symbolic link at control file {}", path.display()),
                ))
            } else {
                Ok(())
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Writes the local pause control file with owner-only permissions (`0700` dir, `0600` file on Unix)
/// and refuses symbolic links.
pub fn write_pause_file(path: &Path) -> io::Result<()> {
    reject_symlink(path)?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
        reject_symlink(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(parent, fs::Permissions::from_mode(0o700));
        }
    }
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        let mut file = opts.open(path)?;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
        file.write_all(b"paused\n")?;
        file.sync_all()?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let mut file = opts.open(path)?;
        file.write_all(b"paused\n")?;
        file.sync_all()?;
        Ok(())
    }
}

/// Removes the local pause control file if present (`Ok(false)` if absent), refusing symlinks.
pub fn remove_pause_file(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("refusing symbolic link at control file {}", path.display()),
                ));
            }
            fs::remove_file(path)?;
            Ok(true)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Returns `true` if a regular pause control file exists at `path`, `false` if absent, and errors
/// on symbolic links.
pub fn is_pause_file_present(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("refusing symbolic link at control file {}", path.display()),
                ))
            } else {
                Ok(meta.is_file())
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

// ---------- Per-OS blocking power & metered network detection + pure parsers ----------

#[cfg(windows)]
#[repr(C)]
struct Win32SystemPowerStatus {
    ac_line_status: u8,
    battery_flag: u8,
    battery_life_percent: u8,
    system_status_flag: u8,
    battery_life_time: u32,
    battery_full_life_time: u32,
}

#[cfg(windows)]
#[link(name = "kernel32")]
extern "system" {
    fn GetSystemPowerStatus(lp_system_power_status: *mut Win32SystemPowerStatus) -> i32;
}

/// Parses Win32 `SYSTEM_POWER_STATUS` fields (`ACLineStatus`, `BatteryFlag`):
/// `ACLineStatus == 0 && BatteryFlag != 128 && BatteryFlag != 255` -> `Battery`,
/// `ACLineStatus == 1` -> `Ac`, otherwise `Unknown`.
pub fn parse_windows_power_status(ac_line_status: u8, battery_flag: u8) -> PowerSource {
    if ac_line_status == 1 {
        PowerSource::Ac
    } else if ac_line_status == 0 && battery_flag != 128 && battery_flag != 255 {
        PowerSource::Battery
    } else {
        PowerSource::Unknown
    }
}

fn detect_system_power_source_blocking() -> PowerSource {
    #[cfg(windows)]
    {
        let mut status = Win32SystemPowerStatus {
            ac_line_status: 255,
            battery_flag: 255,
            battery_life_percent: 255,
            system_status_flag: 0,
            battery_life_time: u32::MAX,
            battery_full_life_time: u32::MAX,
        };
        // SAFETY: `status` is a valid aligned `Win32SystemPowerStatus` stack struct passed by mutable pointer as required by Win32 `GetSystemPowerStatus`.
        let ok = unsafe { GetSystemPowerStatus(&mut status as *mut Win32SystemPowerStatus) };
        if ok != 0 {
            let win_src = parse_windows_power_status(status.ac_line_status, status.battery_flag);
            if win_src != PowerSource::Unknown {
                return win_src;
            }
        }
    }
    if let Some(status) = detect_live_mobile_status() {
        return power_from_is_charging(status.is_charging);
    }
    PowerSource::Unknown
}

#[cfg(any(target_os = "macos", target_os = "linux", windows))]
fn run_command_bounded(program: &str, args: &[&str], timeout: Duration) -> Option<String> {
    let mut cmd = std::process::Command::new(program);
    cmd.args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    let mut child = cmd.spawn().ok()?;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return None;
                }
                use std::io::Read;
                let mut out = String::new();
                if let Some(mut stdout) = child.stdout.take() {
                    let _ = stdout.read_to_string(&mut out);
                }
                return Some(out);
            }
            Ok(None) => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(_) => return None,
        }
    }
}

/// Parses `scutil --nwi` output, returning `(is_wwan, primary_interfaces)`.
pub fn parse_macos_scutil_nwi(output: &str) -> (bool, Vec<String>) {
    let mut is_wwan = false;
    let mut ifaces = Vec::new();
    for line in output.lines() {
        if line.contains("IsWWAN") {
            is_wwan = true;
        }
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("Network interfaces:") {
            for tok in rest.split_whitespace() {
                if !tok.is_empty() && !ifaces.contains(&tok.to_string()) {
                    ifaces.push(tok.to_string());
                }
            }
        } else if let Some((left, right)) = trimmed.split_once(':') {
            let iface = left.trim();
            if right.trim_start().starts_with("flags")
                && !iface.is_empty()
                && !iface.contains(' ')
                && !matches!(iface, "REACH")
                && !ifaces.contains(&iface.to_string())
            {
                ifaces.push(iface.to_string());
            }
        }
    }
    (is_wwan, ifaces)
}

/// Parses `/sbin/ifconfig -v <iface>` output for macOS `EXPENSIVE` or `CONSTRAINED` interface flags.
pub fn parse_macos_ifconfig_v(output: &str) -> bool {
    for line in output.lines() {
        let mut rest = line;
        while let Some(open) = rest.find('<') {
            let after_open = &rest[open + 1..];
            if let Some(close) = after_open.find('>') {
                let inside = &after_open[..close];
                if inside.split(',').any(|flag| {
                    matches!(
                        flag.trim().to_ascii_uppercase().as_str(),
                        "EXPENSIVE" | "CONSTRAINED"
                    )
                }) {
                    return true;
                }
                rest = &after_open[close + 1..];
            } else {
                break;
            }
        }
    }
    false
}

/// Parses `nmcli -t -f METERED general` output on Linux.
pub fn parse_linux_nmcli_metered(output: &str) -> NetworkCost {
    for line in output.lines() {
        let lower = line.trim().to_ascii_lowercase();
        if lower.is_empty() {
            continue;
        }
        if lower.starts_with("yes") {
            return NetworkCost::Metered;
        }
        if lower.starts_with("no") {
            return NetworkCost::Unmetered;
        }
    }
    NetworkCost::Unsupported
}

/// Parses `busctl get-property ... NetworkManager Metered` (`u 1`..`u 4`, `u 0`) on Linux.
pub fn parse_linux_busctl_metered(output: &str) -> NetworkCost {
    let mut tokens = output.split_whitespace();
    if let (Some("u"), Some(val)) = (tokens.next(), tokens.next()) {
        return match val {
            "1" | "3" => NetworkCost::Metered,
            "2" | "4" => NetworkCost::Unmetered,
            _ => NetworkCost::Unsupported,
        };
    }
    NetworkCost::Unsupported
}

/// Parses our Windows WinRT `GetConnectionCost` script output (`Metered` / `Unmetered` / `Unsupported`).
pub fn parse_windows_connection_cost(output: &str) -> NetworkCost {
    for line in output.lines() {
        match line.trim().to_ascii_lowercase().as_str() {
            "metered" | "fixed" | "variable" => return NetworkCost::Metered,
            "unmetered" | "unrestricted" => return NetworkCost::Unmetered,
            "unsupported" | "unknown" => return NetworkCost::Unsupported,
            _ => {}
        }
    }
    NetworkCost::Unsupported
}

#[cfg(target_os = "macos")]
fn detect_network_cost_blocking() -> NetworkCost {
    let Some(nwi) = run_command_bounded("scutil", &["--nwi"], Duration::from_secs(2)) else {
        return NetworkCost::Unsupported;
    };
    let (is_wwan, ifaces) = parse_macos_scutil_nwi(&nwi);
    if is_wwan {
        return NetworkCost::Metered;
    }
    if ifaces.is_empty() {
        return NetworkCost::Unsupported;
    }
    let mut saw_any = false;
    for iface in ifaces {
        if let Some(ifcfg) =
            run_command_bounded("/sbin/ifconfig", &["-v", &iface], Duration::from_secs(2))
        {
            saw_any = true;
            if parse_macos_ifconfig_v(&ifcfg) {
                return NetworkCost::Metered;
            }
        }
    }
    if saw_any {
        NetworkCost::Unmetered
    } else {
        NetworkCost::Unsupported
    }
}

#[cfg(target_os = "linux")]
fn detect_network_cost_blocking() -> NetworkCost {
    if let Some(out) = run_command_bounded(
        "nmcli",
        &["-t", "-f", "METERED", "general"],
        Duration::from_secs(2),
    ) {
        let parsed = parse_linux_nmcli_metered(&out);
        if parsed != NetworkCost::Unsupported {
            return parsed;
        }
    }
    if let Some(out) = run_command_bounded(
        "busctl",
        &[
            "get-property",
            "org.freedesktop.NetworkManager",
            "/org/freedesktop/NetworkManager",
            "org.freedesktop.NetworkManager",
            "Metered",
        ],
        Duration::from_secs(2),
    ) {
        return parse_linux_busctl_metered(&out);
    }
    NetworkCost::Unsupported
}

#[cfg(windows)]
fn detect_network_cost_blocking() -> NetworkCost {
    const SCRIPT: &str = "\
Add-Type -AssemblyName System.Runtime.WindowsRuntime | Out-Null; \
$p = [Windows.Networking.Connectivity.NetworkInformation,Windows.Networking.Connectivity,ContentType=WindowsRuntime]::GetInternetConnectionProfile(); \
if ($null -eq $p) { 'Unsupported' } else { \
  $c = $p.GetConnectionCost(); \
  if ($null -eq $c) { 'Unsupported' } \
  elseif ($c.Roaming -or $c.OverDataLimit -or $c.NetworkCostType -in @('Fixed','Variable')) { 'Metered' } \
  elseif ($c.NetworkCostType -eq 'Unrestricted') { 'Unmetered' } \
  else { 'Unsupported' } \
}";
    if let Some(out) = run_command_bounded(
        "powershell.exe",
        &["-NoProfile", "-NonInteractive", "-Command", SCRIPT],
        Duration::from_secs(3),
    ) {
        return parse_windows_connection_cost(&out);
    }
    NetworkCost::Unsupported
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
fn detect_network_cost_blocking() -> NetworkCost {
    NetworkCost::Unsupported
}
