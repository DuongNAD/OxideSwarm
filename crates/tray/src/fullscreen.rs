//! Full-screen game/application detection and single-slot pause guard (`TRAY-4`, `design 0112 D4.1`).

use rusty_grid_worker::control::{ControlStatusSnapshot, WorkerControlState};
use std::time::{Duration, Instant};

/// Standardized worker control pause reason emitted by the full-screen guard.
pub const FULLSCREEN_PAUSE_REASON: &str = "fullscreen";

/// Delay after full-screen exits before auto-resuming a fullscreen-paused worker.
pub const DEFAULT_RESUME_DELAY: Duration = Duration::from_secs(60);

/// Why the active foreground window triggered full-screen mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullscreenReason {
    Direct3DFullscreen,
    BusyFullscreen,
}

/// Output of probing the local OS full-screen state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FullscreenReading {
    Active(FullscreenReason),
    Inactive,
    Unsupported,
    ProbeError(String),
}

impl FullscreenReading {
    /// Returns `true` when an active full-screen application was detected.
    pub fn is_active(&self) -> bool {
        matches!(self, FullscreenReading::Active(_))
    }
}

/// Maps Windows `QUERY_USER_NOTIFICATION_STATE` values to [`FullscreenReading`].
///
/// Values follow `windows-sys` `Win32::UI::Shell`:
/// - `2` (`QUNS_BUSY`): exclusive/full-screen or presentation busy
/// - `3` (`QUNS_RUNNING_D3D_FULL_SCREEN`): Direct3D exclusive full-screen
pub fn classify_windows_quns(raw_state: i32) -> FullscreenReading {
    match raw_state {
        3 => FullscreenReading::Active(FullscreenReason::Direct3DFullscreen),
        2 => FullscreenReading::Active(FullscreenReason::BusyFullscreen),
        1 | 4 | 5 | 6 | 7 => FullscreenReading::Inactive,
        other => FullscreenReading::ProbeError(format!("unexpected QUNS value {other}")),
    }
}

#[cfg(target_os = "windows")]
mod win_ffi {
    #![allow(unsafe_code)]

    use super::{classify_windows_quns, FullscreenReading};
    use windows_sys::Win32::UI::Shell::{
        SHQueryUserNotificationState, QUERY_USER_NOTIFICATION_STATE,
    };

    pub fn probe_windows_quns() -> FullscreenReading {
        let mut state: QUERY_USER_NOTIFICATION_STATE = 0;
        // SAFETY: `SHQueryUserNotificationState` takes a valid aligned mutable pointer to a
        // `QUERY_USER_NOTIFICATION_STATE` (`i32`) out-parameter and has no lifetime requirements beyond the call.
        let hr = unsafe { SHQueryUserNotificationState(&mut state) };
        if hr < 0 {
            FullscreenReading::ProbeError(format!(
                "SHQueryUserNotificationState failed (HRESULT 0x{:08X})",
                hr as u32
            ))
        } else {
            classify_windows_quns(state)
        }
    }
}

/// Probes whether a full-screen application currently owns the foreground session.
pub fn probe_fullscreen() -> FullscreenReading {
    #[cfg(target_os = "windows")]
    {
        win_ffi::probe_windows_quns()
    }
    #[cfg(not(target_os = "windows"))]
    {
        FullscreenReading::Unsupported
    }
}

/// Action emitted by [`FullscreenGuard::evaluate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FullscreenAction {
    None,
    Pause { reason: String },
    Resume,
}

/// Single-slot pause guard ensuring full-screen pause never overwrites or lifts manual pauses.
#[derive(Debug, Clone)]
pub struct FullscreenGuard {
    pub enabled: bool,
    pub resume_delay: Duration,
    pending_resume_since: Option<Instant>,
}

impl FullscreenGuard {
    /// Creates a new guard with the configured enable flag and post-exit resume delay.
    pub fn new(enabled: bool, resume_delay: Duration) -> Self {
        Self {
            enabled,
            resume_delay,
            pending_resume_since: None,
        }
    }

    /// Evaluates whether to pause or resume based on `fullscreen_active` and the worker snapshot.
    pub fn evaluate(
        &mut self,
        now: Instant,
        fullscreen_active: bool,
        snapshot: Option<&ControlStatusSnapshot>,
    ) -> FullscreenAction {
        let Some(snap) = snapshot else {
            self.pending_resume_since = None;
            return FullscreenAction::None;
        };
        if !self.enabled {
            self.pending_resume_since = None;
            return FullscreenAction::None;
        }

        if fullscreen_active {
            self.pending_resume_since = None;
            if snap.state == WorkerControlState::Running {
                return FullscreenAction::Pause {
                    reason: FULLSCREEN_PAUSE_REASON.to_string(),
                };
            }
            return FullscreenAction::None;
        }

        let paused_by_fullscreen = snap.state == WorkerControlState::Paused
            && snap.reason.as_deref() == Some(FULLSCREEN_PAUSE_REASON);

        if paused_by_fullscreen {
            let since = *self.pending_resume_since.get_or_insert(now);
            if now.duration_since(since) >= self.resume_delay {
                self.pending_resume_since = None;
                return FullscreenAction::Resume;
            }
        } else {
            self.pending_resume_since = None;
        }

        FullscreenAction::None
    }

    /// Clears any pending post-fullscreen resume timer.
    pub fn reset(&mut self) {
        self.pending_resume_since = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_worker::control::WorkerLimitsConfig;
    use uuid::Uuid;

    fn snapshot(state: WorkerControlState, reason: Option<&str>) -> ControlStatusSnapshot {
        ControlStatusSnapshot {
            worker_id: Uuid::nil(),
            worker_name: "default".to_string(),
            state,
            reason: reason.map(str::to_string),
            paused_until_epoch_ms: None,
            no_gpu_tasks: false,
            active_tasks: 0,
            limits: WorkerLimitsConfig::default(),
            config_path: None,
            connected: Some(true),
            link_kind: None,
            last_error: None,
            master_addr: None,
            dashboard_url: None,
            tasks_started: 0,
        }
    }

    #[test]
    fn classify_windows_quns_matches_design_0112_d4() {
        assert_eq!(classify_windows_quns(1), FullscreenReading::Inactive);
        assert_eq!(
            classify_windows_quns(2),
            FullscreenReading::Active(FullscreenReason::BusyFullscreen)
        );
        assert_eq!(
            classify_windows_quns(3),
            FullscreenReading::Active(FullscreenReason::Direct3DFullscreen)
        );
        for code in [4, 5, 6, 7] {
            assert_eq!(classify_windows_quns(code), FullscreenReading::Inactive);
        }
        assert!(matches!(
            classify_windows_quns(99),
            FullscreenReading::ProbeError(_)
        ));
    }

    #[test]
    fn guard_pauses_running_and_resumes_60s_after_exit() {
        let mut guard = FullscreenGuard::new(true, DEFAULT_RESUME_DELAY);
        let t0 = Instant::now();
        let running = snapshot(WorkerControlState::Running, None);
        let paused_fs = snapshot(WorkerControlState::Paused, Some(FULLSCREEN_PAUSE_REASON));

        assert_eq!(
            guard.evaluate(t0, true, Some(&running)),
            FullscreenAction::Pause {
                reason: "fullscreen".to_string()
            }
        );
        assert_eq!(
            guard.evaluate(t0 + Duration::from_secs(10), true, Some(&paused_fs)),
            FullscreenAction::None
        );

        // Fullscreen exits at t0+20s; 59s later should still wait.
        let t_exit = t0 + Duration::from_secs(20);
        assert_eq!(
            guard.evaluate(t_exit, false, Some(&paused_fs)),
            FullscreenAction::None
        );
        assert_eq!(
            guard.evaluate(t_exit + Duration::from_secs(59), false, Some(&paused_fs)),
            FullscreenAction::None
        );
        assert_eq!(
            guard.evaluate(t_exit + Duration::from_secs(60), false, Some(&paused_fs)),
            FullscreenAction::Resume
        );
    }

    #[test]
    fn guard_resets_cooldown_if_fullscreen_returns() {
        let mut guard = FullscreenGuard::new(true, DEFAULT_RESUME_DELAY);
        let t0 = Instant::now();
        let paused_fs = snapshot(WorkerControlState::Paused, Some(FULLSCREEN_PAUSE_REASON));

        assert_eq!(
            guard.evaluate(t0, false, Some(&paused_fs)),
            FullscreenAction::None
        );
        assert_eq!(
            guard.evaluate(t0 + Duration::from_secs(45), false, Some(&paused_fs)),
            FullscreenAction::None
        );
        // Back to fullscreen resets the 60s countdown.
        assert_eq!(
            guard.evaluate(t0 + Duration::from_secs(50), true, Some(&paused_fs)),
            FullscreenAction::None
        );
        let t_exit2 = t0 + Duration::from_secs(60);
        assert_eq!(
            guard.evaluate(t_exit2, false, Some(&paused_fs)),
            FullscreenAction::None
        );
        assert_eq!(
            guard.evaluate(t_exit2 + Duration::from_secs(59), false, Some(&paused_fs)),
            FullscreenAction::None
        );
        assert_eq!(
            guard.evaluate(t_exit2 + Duration::from_secs(60), false, Some(&paused_fs)),
            FullscreenAction::Resume
        );
    }

    #[test]
    fn guard_preserves_manual_pause_and_held_states() {
        let mut guard = FullscreenGuard::new(true, DEFAULT_RESUME_DELAY);
        let t0 = Instant::now();
        let held = snapshot(WorkerControlState::Held, Some("on battery"));
        let manual_paused = snapshot(
            WorkerControlState::Paused,
            Some("contributor paused worker"),
        );

        // Must never overwrite Held or manual pause when fullscreen starts.
        assert_eq!(
            guard.evaluate(t0, true, Some(&held)),
            FullscreenAction::None
        );
        assert_eq!(
            guard.evaluate(t0, true, Some(&manual_paused)),
            FullscreenAction::None
        );
        // Must never auto-resume a manual pause even after 60s without fullscreen.
        assert_eq!(
            guard.evaluate(t0 + Duration::from_secs(120), false, Some(&manual_paused)),
            FullscreenAction::None
        );
    }

    #[test]
    fn disabled_guard_does_nothing() {
        let mut guard = FullscreenGuard::new(false, DEFAULT_RESUME_DELAY);
        let t0 = Instant::now();
        let running = snapshot(WorkerControlState::Running, None);
        let paused_fs = snapshot(WorkerControlState::Paused, Some(FULLSCREEN_PAUSE_REASON));

        assert_eq!(
            guard.evaluate(t0, true, Some(&running)),
            FullscreenAction::None
        );
        assert_eq!(
            guard.evaluate(t0 + Duration::from_secs(120), false, Some(&paused_fs)),
            FullscreenAction::None
        );
    }
}
