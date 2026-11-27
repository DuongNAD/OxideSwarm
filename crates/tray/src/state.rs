//! Pure-function tray state machine, icon rendering, and menu view-model (`TRAY-1`).

use rusty_grid_worker::control::{
    ControlLinkKind, ControlStatusSnapshot, WorkerControlState, WorkerLimitsConfig,
};
use serde::Serialize;

/// High-level visual state for the contributor tray icon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrayIconState {
    Starting,
    Idle,
    Busy,
    Paused,
    Disconnected,
    Error,
}

impl TrayIconState {
    /// Human-readable label for logs and CLI output.
    pub fn as_str(self) -> &'static str {
        match self {
            TrayIconState::Starting => "starting",
            TrayIconState::Idle => "idle",
            TrayIconState::Busy => "busy",
            TrayIconState::Paused => "paused",
            TrayIconState::Disconnected => "disconnected",
            TrayIconState::Error => "error",
        }
    }
}

/// Categorized contributor pause or hold reason shown in the tray status line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReasonKind {
    Manual,
    Fullscreen,
    Battery,
    Metered,
    Hours,
    DailyCap,
    LimitOther,
    Unknown,
}

impl PauseReasonKind {
    /// Short user-facing label for the pause/hold reason.
    pub fn label(self) -> &'static str {
        match self {
            PauseReasonKind::Manual => "paused by hand",
            PauseReasonKind::Fullscreen => "paused for full-screen",
            PauseReasonKind::Battery => "paused on battery",
            PauseReasonKind::Metered => "paused on metered network",
            PauseReasonKind::Hours => "outside allowed hours",
            PauseReasonKind::DailyCap => "daily task cap reached",
            PauseReasonKind::LimitOther => "paused by local limit",
            PauseReasonKind::Unknown => "paused",
        }
    }
}

/// Lifecycle phase of the supervised `oxideswarm worker` process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum SupervisorPhase {
    External,
    Starting,
    Running {
        pid: u32,
        uptime_secs: u64,
    },
    Backoff {
        attempt: u32,
        next_delay_secs: u64,
        last_exit: Option<i32>,
    },
    StoppedByUser,
}

/// Latest result of polling the local worker control channel.
#[derive(Debug, Clone, PartialEq)]
pub enum ControlPollState {
    Pending,
    Connected(Box<ControlStatusSnapshot>),
    Unavailable(String),
}

/// Derived UI state consumed by the native tray menu and `--status` output.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MenuViewModel {
    pub icon_state: TrayIconState,
    pub status_line: String,
    pub tooltip: String,
    pub pause_1h_enabled: bool,
    pub pause_indef_enabled: bool,
    pub pause_label_suffix: &'static str,
    pub resume_enabled: bool,
    pub limits_enabled: bool,
    pub limits: WorkerLimitsConfig,
    pub dashboard_url: Option<String>,
    pub fullscreen_guard_enabled: bool,
    pub legacy_fallback: bool,
}

/// Classifies a worker pause/hold reason into a display category.
pub fn classify_pause_reason(state: WorkerControlState, reason: Option<&str>) -> PauseReasonKind {
    let lower = reason.unwrap_or("").trim().to_ascii_lowercase();
    if lower.contains("fullscreen") || lower.contains("game") {
        return PauseReasonKind::Fullscreen;
    }
    if lower.contains("battery") {
        return PauseReasonKind::Battery;
    }
    if lower.contains("metered") {
        return PauseReasonKind::Metered;
    }
    if lower.contains("hour") && !lower.contains("daily") && !lower.contains("day") {
        return PauseReasonKind::Hours;
    }
    if lower.contains("daily") || lower.contains("cap") || lower.contains("per day") {
        return PauseReasonKind::DailyCap;
    }
    if lower.contains("contributor paused") || lower.contains("manual") || lower.contains("by hand")
    {
        return PauseReasonKind::Manual;
    }
    match state {
        WorkerControlState::Held => PauseReasonKind::LimitOther,
        WorkerControlState::Paused if lower.is_empty() => PauseReasonKind::Manual,
        WorkerControlState::Paused => PauseReasonKind::Unknown,
        WorkerControlState::Running => PauseReasonKind::Unknown,
    }
}

/// Validates that a dashboard URL is a clean `http://` or `https://` target without control chars.
pub fn validate_dashboard_url(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return None;
    }
    let rest = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))?;
    let host = rest.split(&['/', '?', '#'][..]).next().unwrap_or("");
    if host.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

/// Computes the target Unix-epoch millisecond timestamp for a 1-hour pause.
pub fn pause_until_1h_ms(now_epoch_ms: u64) -> u64 {
    now_epoch_ms.saturating_add(3_600_000)
}

/// Emits a one-time notice the first time `tasks_started` becomes non-zero.
pub fn check_first_task_notice(
    already_notified: bool,
    snapshot: &ControlStatusSnapshot,
) -> Option<String> {
    if already_notified || snapshot.tasks_started == 0 {
        return None;
    }
    let cluster = snapshot
        .master_addr
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("your cluster");
    Some(format!(
        "OxideSwarm started its first task for {cluster}. You can pause anytime from the tray menu."
    ))
}

fn format_link_suffix(snapshot: &ControlStatusSnapshot) -> String {
    let mut parts = Vec::new();
    if let Some(kind) = snapshot.link_kind {
        parts.push(match kind {
            ControlLinkKind::Direct => "direct",
            ControlLinkKind::Relay => "relay",
            ControlLinkKind::Tcp => "tcp",
        });
    }
    if let Some(master) = snapshot
        .master_addr
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        parts.push(master);
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(" · "))
    }
}

/// Derives icon state, menu labels, and button enablement from current poll and supervisor phases.
pub fn derive_view(
    poll: &ControlPollState,
    sup: &SupervisorPhase,
    fullscreen_guard_enabled: bool,
    legacy_fallback: bool,
) -> MenuViewModel {
    match sup {
        SupervisorPhase::StoppedByUser => {
            return MenuViewModel {
                icon_state: TrayIconState::Paused,
                status_line: "Status: Paused (worker stopped)".to_string(),
                tooltip: "OxideSwarm — Paused (worker stopped)".to_string(),
                pause_1h_enabled: false,
                pause_indef_enabled: false,
                pause_label_suffix: if legacy_fallback {
                    " (stops worker)"
                } else {
                    ""
                },
                resume_enabled: true,
                limits_enabled: false,
                limits: WorkerLimitsConfig::default(),
                dashboard_url: None,
                fullscreen_guard_enabled,
                legacy_fallback,
            };
        }
        SupervisorPhase::Backoff {
            attempt,
            next_delay_secs,
            last_exit,
        } => {
            let exit_text = last_exit
                .map(|c| format!(" (exit {c})"))
                .unwrap_or_default();
            let status_line =
                format!("Status: Worker exited{exit_text}; retry #{attempt} in {next_delay_secs}s");
            return MenuViewModel {
                icon_state: TrayIconState::Error,
                tooltip: format!("OxideSwarm — {status_line}"),
                status_line,
                pause_1h_enabled: false,
                pause_indef_enabled: false,
                pause_label_suffix: "",
                resume_enabled: false,
                limits_enabled: false,
                limits: WorkerLimitsConfig::default(),
                dashboard_url: None,
                fullscreen_guard_enabled,
                legacy_fallback,
            };
        }
        SupervisorPhase::Starting => {
            return MenuViewModel {
                icon_state: TrayIconState::Starting,
                status_line: "Status: Starting worker...".to_string(),
                tooltip: "OxideSwarm — Starting worker...".to_string(),
                pause_1h_enabled: false,
                pause_indef_enabled: false,
                pause_label_suffix: "",
                resume_enabled: false,
                limits_enabled: false,
                limits: WorkerLimitsConfig::default(),
                dashboard_url: None,
                fullscreen_guard_enabled,
                legacy_fallback,
            };
        }
        SupervisorPhase::External | SupervisorPhase::Running { .. } => {}
    }

    match poll {
        ControlPollState::Pending => MenuViewModel {
            icon_state: TrayIconState::Starting,
            status_line: "Status: Connecting to worker...".to_string(),
            tooltip: "OxideSwarm — Connecting to worker...".to_string(),
            pause_1h_enabled: legacy_fallback,
            pause_indef_enabled: legacy_fallback,
            pause_label_suffix: if legacy_fallback {
                " (stops worker)"
            } else {
                ""
            },
            resume_enabled: false,
            limits_enabled: false,
            limits: WorkerLimitsConfig::default(),
            dashboard_url: None,
            fullscreen_guard_enabled,
            legacy_fallback,
        },
        ControlPollState::Unavailable(err) => {
            let suffix = if legacy_fallback {
                " (stops worker)"
            } else {
                ""
            };
            let (icon_state, status_line) = if legacy_fallback {
                (
                    TrayIconState::Idle,
                    "Status: Legacy worker running (Pause stops worker)".to_string(),
                )
            } else {
                (
                    TrayIconState::Disconnected,
                    format!("Status: Control channel unavailable ({err})"),
                )
            };
            MenuViewModel {
                icon_state,
                tooltip: format!("OxideSwarm — {status_line}"),
                status_line,
                pause_1h_enabled: legacy_fallback,
                pause_indef_enabled: legacy_fallback,
                pause_label_suffix: suffix,
                resume_enabled: false,
                limits_enabled: false,
                limits: WorkerLimitsConfig::default(),
                dashboard_url: None,
                fullscreen_guard_enabled,
                legacy_fallback,
            }
        }
        ControlPollState::Connected(snap) => derive_connected_view(snap, fullscreen_guard_enabled),
    }
}

fn derive_connected_view(
    snap: &ControlStatusSnapshot,
    fullscreen_guard_enabled: bool,
) -> MenuViewModel {
    let link_suffix = format_link_suffix(snap);
    let dashboard_url = snap
        .dashboard_url
        .as_deref()
        .and_then(validate_dashboard_url);
    let err_hint = snap
        .last_error
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let (icon_state, status_line, pause_enabled, resume_enabled) = match snap.connected {
        Some(false) => {
            let icon = if err_hint.is_some() {
                TrayIconState::Error
            } else {
                TrayIconState::Disconnected
            };
            let detail = err_hint.unwrap_or("waiting for master");
            let line = format!("Status: Not connected{link_suffix} — {detail}");
            let can_pause = snap.state == WorkerControlState::Running;
            let can_resume = matches!(
                snap.state,
                WorkerControlState::Paused | WorkerControlState::Held
            );
            (icon, line, can_pause, can_resume)
        }
        // Treat both `Some(true)` and legacy `None` (no `connected` field) as connected.
        Some(true) | None => match snap.state {
            WorkerControlState::Running => {
                if snap.active_tasks > 0 {
                    let plural = if snap.active_tasks == 1 { "" } else { "s" };
                    let line = format!(
                        "Status: Busy ({} task{plural}){link_suffix}",
                        snap.active_tasks
                    );
                    (TrayIconState::Busy, line, true, false)
                } else {
                    let line = format!("Status: Connected & idle{link_suffix}");
                    (TrayIconState::Idle, line, true, false)
                }
            }
            WorkerControlState::Paused | WorkerControlState::Held => {
                let kind = classify_pause_reason(snap.state, snap.reason.as_deref());
                let raw_note = snap
                    .reason
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let reason_text = match (kind, raw_note) {
                    (PauseReasonKind::Unknown | PauseReasonKind::LimitOther, Some(note)) => {
                        format!("{} ({note})", kind.label())
                    }
                    _ => kind.label().to_string(),
                };
                let line = format!("Status: {reason_text}{link_suffix}");
                let can_resume = snap.state == WorkerControlState::Paused;
                (TrayIconState::Paused, line, false, can_resume)
            }
        },
    };

    MenuViewModel {
        icon_state,
        tooltip: format!("OxideSwarm — {status_line}"),
        status_line,
        pause_1h_enabled: pause_enabled,
        pause_indef_enabled: pause_enabled,
        pause_label_suffix: "",
        resume_enabled,
        limits_enabled: true,
        limits: snap.limits.clone(),
        dashboard_url,
        fullscreen_guard_enabled,
        legacy_fallback: false,
    }
}

/// Renders a crisp 16x16 RGBA circular status badge for the tray icon.
pub fn render_icon_rgba(state: TrayIconState) -> (Vec<u8>, u32, u32) {
    const SIZE: u32 = 16;
    let (r, g, b): (u8, u8, u8) = match state {
        TrayIconState::Idle => (34, 197, 94),           // Green
        TrayIconState::Busy => (14, 165, 233),          // Cyan-blue
        TrayIconState::Paused => (245, 158, 11),        // Amber
        TrayIconState::Disconnected => (148, 163, 184), // Slate gray
        TrayIconState::Starting => (100, 116, 139),     // Dim gray
        TrayIconState::Error => (239, 68, 68),          // Red
    };

    let mut rgba = vec![0u8; (SIZE * SIZE * 4) as usize];
    for y in 0..SIZE {
        for x in 0..SIZE {
            let dx = (x as i32) * 2 - 15;
            let dy = (y as i32) * 2 - 15;
            let dist_sq = dx * dx + dy * dy;
            let idx = ((y * SIZE + x) * 4) as usize;
            if dist_sq <= 14 * 14 {
                if dist_sq >= 11 * 11 {
                    // Subtle dark ring for visibility on light & dark taskbars.
                    rgba[idx] = r / 3;
                    rgba[idx + 1] = g / 3;
                    rgba[idx + 2] = b / 3;
                    rgba[idx + 3] = 255;
                } else {
                    rgba[idx] = r;
                    rgba[idx + 1] = g;
                    rgba[idx + 2] = b;
                    rgba[idx + 3] = 255;
                }
            }
        }
    }
    (rgba, SIZE, SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn sample_snapshot() -> ControlStatusSnapshot {
        ControlStatusSnapshot {
            worker_id: Uuid::nil(),
            worker_name: "default".to_string(),
            state: WorkerControlState::Running,
            reason: None,
            paused_until_epoch_ms: None,
            no_gpu_tasks: false,
            active_tasks: 0,
            limits: WorkerLimitsConfig::default(),
            config_path: None,
            connected: Some(true),
            link_kind: Some(ControlLinkKind::Direct),
            last_error: None,
            master_addr: Some("127.0.0.1:8088".to_string()),
            dashboard_url: Some("http://127.0.0.1:18080/dashboard".to_string()),
            tasks_started: 0,
        }
    }

    #[test]
    fn derive_view_covers_idle_busy_paused_and_disconnected_states() {
        let sup = SupervisorPhase::Running {
            pid: 4242,
            uptime_secs: 15,
        };

        let idle_snap = sample_snapshot();
        let idle = derive_view(
            &ControlPollState::Connected(Box::new(idle_snap)),
            &sup,
            true,
            false,
        );
        assert_eq!(idle.icon_state, TrayIconState::Idle);
        assert!(idle.pause_1h_enabled);
        assert!(idle.pause_indef_enabled);
        assert!(!idle.resume_enabled);
        assert!(idle.limits_enabled);
        assert!(idle.tooltip.contains("direct"));
        assert!(idle.tooltip.contains("127.0.0.1:8088"));

        let mut busy_snap = sample_snapshot();
        busy_snap.active_tasks = 2;
        busy_snap.link_kind = Some(ControlLinkKind::Relay);
        let busy = derive_view(
            &ControlPollState::Connected(Box::new(busy_snap)),
            &sup,
            true,
            false,
        );
        assert_eq!(busy.icon_state, TrayIconState::Busy);
        assert!(busy.status_line.contains("2 tasks"));
        assert!(busy.tooltip.contains("relay"));

        let mut paused_snap = sample_snapshot();
        paused_snap.state = WorkerControlState::Paused;
        paused_snap.reason = Some("fullscreen".to_string());
        let paused = derive_view(
            &ControlPollState::Connected(Box::new(paused_snap)),
            &sup,
            true,
            false,
        );
        assert_eq!(paused.icon_state, TrayIconState::Paused);
        assert!(!paused.pause_1h_enabled);
        assert!(paused.resume_enabled);
        assert!(paused.status_line.contains("full-screen"));

        let mut disconn_snap = sample_snapshot();
        disconn_snap.connected = Some(false);
        disconn_snap.last_error = None;
        let disconn = derive_view(
            &ControlPollState::Connected(Box::new(disconn_snap)),
            &sup,
            true,
            false,
        );
        assert_eq!(disconn.icon_state, TrayIconState::Disconnected);

        let mut err_snap = sample_snapshot();
        err_snap.connected = Some(false);
        err_snap.last_error = Some("quic timeout".to_string());
        let err = derive_view(
            &ControlPollState::Connected(Box::new(err_snap)),
            &sup,
            true,
            false,
        );
        assert_eq!(err.icon_state, TrayIconState::Error);
        assert!(err.tooltip.contains("quic timeout"));
    }

    #[test]
    fn classify_pause_reason_distinguishes_all_categories() {
        assert_eq!(
            classify_pause_reason(WorkerControlState::Paused, Some("fullscreen")),
            PauseReasonKind::Fullscreen
        );
        assert_eq!(
            classify_pause_reason(WorkerControlState::Held, Some("on battery (remaining 55%)")),
            PauseReasonKind::Battery
        );
        assert_eq!(
            classify_pause_reason(WorkerControlState::Held, Some("metered network detected")),
            PauseReasonKind::Metered
        );
        assert_eq!(
            classify_pause_reason(WorkerControlState::Held, Some("outside configured hours")),
            PauseReasonKind::Hours
        );
        assert_eq!(
            classify_pause_reason(WorkerControlState::Held, Some("daily task cap reached")),
            PauseReasonKind::DailyCap
        );
        assert_eq!(
            classify_pause_reason(
                WorkerControlState::Paused,
                Some("contributor paused worker")
            ),
            PauseReasonKind::Manual
        );
        assert_eq!(
            classify_pause_reason(WorkerControlState::Held, Some("cpu threshold")),
            PauseReasonKind::LimitOther
        );
    }

    #[test]
    fn first_task_notice_fires_once_and_names_cluster() {
        let mut snap = sample_snapshot();
        assert_eq!(check_first_task_notice(false, &snap), None);
        snap.tasks_started = 1;
        let notice = check_first_task_notice(false, &snap).expect("notice expected");
        assert!(notice.contains("127.0.0.1:8088"));
        assert!(notice.contains("pause"));
        assert_eq!(check_first_task_notice(true, &snap), None);
    }

    #[test]
    fn validate_dashboard_url_rejects_unsafe_schemes_and_whitespace() {
        assert_eq!(
            validate_dashboard_url(" https://example.org:8443/dash "),
            Some("https://example.org:8443/dash".to_string())
        );
        assert_eq!(
            validate_dashboard_url("http://127.0.0.1:18080/"),
            Some("http://127.0.0.1:18080/".to_string())
        );
        assert_eq!(validate_dashboard_url("file:///C:/Windows/win.ini"), None);
        assert_eq!(validate_dashboard_url("javascript:alert(1)"), None);
        assert_eq!(validate_dashboard_url("http://127.0.0.1:8080/a b"), None);
        assert_eq!(validate_dashboard_url("https://"), None);
    }

    #[test]
    fn legacy_snapshot_and_pre_net9_fallback_behave_cleanly() {
        let sup = SupervisorPhase::Running {
            pid: 99,
            uptime_secs: 60,
        };
        let mut legacy_snap = sample_snapshot();
        legacy_snap.connected = None;
        legacy_snap.link_kind = None;
        legacy_snap.master_addr = None;
        let view = derive_view(
            &ControlPollState::Connected(Box::new(legacy_snap)),
            &sup,
            false,
            false,
        );
        assert_eq!(view.icon_state, TrayIconState::Idle);
        assert!(!view.legacy_fallback);

        let fallback = derive_view(
            &ControlPollState::Unavailable("pipe missing".to_string()),
            &sup,
            true,
            true,
        );
        assert_eq!(fallback.icon_state, TrayIconState::Idle);
        assert!(fallback.pause_1h_enabled);
        assert_eq!(fallback.pause_label_suffix, " (stops worker)");
        assert!(!fallback.limits_enabled);

        let stopped = derive_view(
            &ControlPollState::Unavailable("stopped".to_string()),
            &SupervisorPhase::StoppedByUser,
            true,
            true,
        );
        assert_eq!(stopped.icon_state, TrayIconState::Paused);
        assert!(stopped.resume_enabled);
        assert!(!stopped.pause_1h_enabled);
    }

    #[test]
    fn render_icon_rgba_returns_16x16_rgba_for_every_state() {
        let mut seen = Vec::new();
        for state in [
            TrayIconState::Starting,
            TrayIconState::Idle,
            TrayIconState::Busy,
            TrayIconState::Paused,
            TrayIconState::Disconnected,
            TrayIconState::Error,
        ] {
            let (rgba, w, h) = render_icon_rgba(state);
            assert_eq!((w, h), (16, 16));
            assert_eq!(rgba.len(), 16 * 16 * 4);
            assert!(!seen.contains(&rgba));
            seen.push(rgba);
        }
        assert_eq!(pause_until_1h_ms(1_000), 3_601_000);
    }
}
