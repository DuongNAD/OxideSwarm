//! Cross-platform tray runner, native menu wiring (`tray-icon` + `muda`), and backend loop.

use crate::control::ControlClient;
use crate::fullscreen::{probe_fullscreen, FullscreenAction, FullscreenGuard};
use crate::state::{
    check_first_task_notice, derive_view, pause_until_1h_ms, validate_dashboard_url,
    ControlPollState, MenuViewModel, SupervisorPhase,
};
use crate::supervisor::{SupervisorConfig, WorkerSupervisor};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc as tokio_mpsc;

const LEGACY_CONTROL_GRACE: Duration = Duration::from_secs(10);
const RELEASES_URL: &str = "https://github.com/DuongNAD/OxideSwarm/releases";

/// Runtime configuration for the tray application.
#[derive(Debug, Clone)]
pub struct TrayAppConfig {
    pub supervisor: Option<SupervisorConfig>,
    pub control_endpoint: Option<ControlClient>,
    pub log_dir: PathBuf,
    pub poll_interval: Duration,
    pub fullscreen_guard_enabled: bool,
    pub fullscreen_resume_delay: Duration,
}

/// User-initiated command sent from the native tray menu to the backend loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrayCommand {
    PauseFor1h,
    PauseUntilResume,
    Resume,
    SetLimit { key: String, value: String },
    ToggleFullscreenGuard,
    OpenDashboard,
    OpenLogs,
    CheckUpdates,
    Quit,
}

/// Safely opens an `http://` or `https://` URL using the OS default browser without a shell.
pub fn open_url(raw: &str) -> Result<(), String> {
    let url = validate_dashboard_url(raw)
        .ok_or_else(|| format!("refusing non-http(s) or malformed URL '{raw}'"))?;
    launch_os_open(&url)
}

/// Safely opens a local directory path in the system file manager without a shell.
pub fn open_path(path: &Path) -> Result<(), String> {
    if let Err(err) = std::fs::create_dir_all(path) {
        tracing::debug!("create_dir_all({}) skipped: {err}", path.display());
    }
    let target = path.to_string_lossy().into_owned();
    launch_os_open(&target)
}

fn launch_os_open(arg: &str) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    let program = "explorer.exe";
    #[cfg(target_os = "macos")]
    let program = "open";
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    let program = "xdg-open";

    Command::new(program)
        .arg(arg)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("failed to launch '{program}': {e}"))
}

fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Runs the tray app until the user quits or an interrupt signal arrives.
pub fn run_app(config: TrayAppConfig) -> Result<(), String> {
    let (cmd_tx, cmd_rx) = tokio_mpsc::unbounded_channel::<TrayCommand>();
    let (view_tx, view_rx) = mpsc::channel::<MenuViewModel>();
    let log_dir = config.log_dir.clone();

    let initial_view = derive_view(
        &ControlPollState::Pending,
        if config.supervisor.is_some() {
            &SupervisorPhase::Starting
        } else {
            &SupervisorPhase::External
        },
        config.fullscreen_guard_enabled,
        false,
    );

    let backend_handle = std::thread::Builder::new()
        .name("oxideswarm-tray-backend".to_string())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    tracing::error!("failed to build tokio runtime for tray backend: {err}");
                    return;
                }
            };
            rt.block_on(run_backend_loop(config, cmd_rx, view_tx));
        })
        .map_err(|e| format!("failed to spawn tray backend thread: {e}"))?;

    let ui_res = run_platform_ui(initial_view, log_dir, cmd_tx.clone(), view_rx);
    let _ = cmd_tx.send(TrayCommand::Quit);
    let _ = backend_handle.join();
    ui_res
}

struct BackendState<'a> {
    config: &'a TrayAppConfig,
    supervisor: Option<WorkerSupervisor>,
    sup_phase: SupervisorPhase,
    sup_running_since: Option<Instant>,
    poll_state: ControlPollState,
    guard: FullscreenGuard,
    first_task_notified: bool,
    legacy_fallback: bool,
    last_view: Option<MenuViewModel>,
}

impl<'a> BackendState<'a> {
    fn new(config: &'a TrayAppConfig, supervisor: Option<WorkerSupervisor>) -> Self {
        let sup_phase = if supervisor.is_some() {
            SupervisorPhase::Starting
        } else {
            SupervisorPhase::External
        };
        Self {
            config,
            supervisor,
            sup_phase,
            sup_running_since: None,
            poll_state: ControlPollState::Pending,
            guard: FullscreenGuard::new(
                config.fullscreen_guard_enabled,
                config.fullscreen_resume_delay,
            ),
            first_task_notified: false,
            legacy_fallback: false,
            last_view: None,
        }
    }

    async fn handle_command(&mut self, cmd: TrayCommand) -> bool {
        match cmd {
            TrayCommand::Quit => return false,
            TrayCommand::PauseFor1h => {
                if self.legacy_fallback {
                    self.stop_supervised_for_pause().await;
                } else if let Some(client) = &self.config.control_endpoint {
                    let until = pause_until_1h_ms(current_epoch_ms());
                    match client
                        .pause(Some(until), Some("contributor paused worker (1h)".into()))
                        .await
                    {
                        Ok(snap) => self.poll_state = ControlPollState::Connected(Box::new(snap)),
                        Err(err) => tracing::warn!("pause (1h) failed: {err}"),
                    }
                }
            }
            TrayCommand::PauseUntilResume => {
                if self.legacy_fallback {
                    self.stop_supervised_for_pause().await;
                } else if let Some(client) = &self.config.control_endpoint {
                    match client
                        .pause(None, Some("contributor paused worker".into()))
                        .await
                    {
                        Ok(snap) => self.poll_state = ControlPollState::Connected(Box::new(snap)),
                        Err(err) => tracing::warn!("pause failed: {err}"),
                    }
                }
            }
            TrayCommand::Resume => {
                if self.sup_phase == SupervisorPhase::StoppedByUser {
                    if let Some(sup) = self.supervisor.as_mut() {
                        sup.start_if_stopped();
                        self.sup_phase = sup.step(Instant::now());
                        if matches!(self.sup_phase, SupervisorPhase::Running { .. }) {
                            self.sup_running_since = Some(Instant::now());
                        }
                    }
                } else if let Some(client) = &self.config.control_endpoint {
                    match client.resume().await {
                        Ok(snap) => self.poll_state = ControlPollState::Connected(Box::new(snap)),
                        Err(err) => tracing::warn!("resume failed: {err}"),
                    }
                }
            }
            TrayCommand::SetLimit { key, value } => {
                if let Some(client) = &self.config.control_endpoint {
                    match client.limits_set(&key, &value).await {
                        Ok(_) => {
                            if let Ok(snap) = client.poll_status().await {
                                self.poll_state = ControlPollState::Connected(Box::new(snap));
                            }
                        }
                        Err(err) => tracing::warn!("limits_set({key}={value}) failed: {err}"),
                    }
                }
            }
            TrayCommand::ToggleFullscreenGuard => {
                self.guard.enabled = !self.guard.enabled;
                if !self.guard.enabled {
                    self.guard.reset();
                }
            }
            TrayCommand::OpenDashboard => {
                let url = self
                    .last_view
                    .as_ref()
                    .and_then(|v| v.dashboard_url.as_deref());
                if let Some(url) = url {
                    if let Err(err) = open_url(url) {
                        tracing::warn!("{err}");
                    }
                }
            }
            TrayCommand::OpenLogs => {
                if let Err(err) = open_path(&self.config.log_dir) {
                    tracing::warn!("{err}");
                }
            }
            TrayCommand::CheckUpdates => {
                if let Err(err) = open_url(RELEASES_URL) {
                    tracing::warn!("{err}");
                }
            }
        }
        true
    }

    async fn stop_supervised_for_pause(&mut self) {
        if let Some(sup) = self.supervisor.as_mut() {
            sup.stop_and_wait().await;
            self.sup_phase = SupervisorPhase::StoppedByUser;
            self.sup_running_since = None;
        }
    }

    async fn tick(&mut self) {
        let now = Instant::now();
        if let Some(sup) = self.supervisor.as_mut() {
            self.sup_phase = sup.step(now);
            match self.sup_phase {
                SupervisorPhase::Running { .. } => {
                    self.sup_running_since.get_or_insert(now);
                }
                _ => {
                    self.sup_running_since = None;
                }
            }
        }

        let can_poll = !matches!(
            self.sup_phase,
            SupervisorPhase::Backoff { .. } | SupervisorPhase::StoppedByUser
        );
        if !can_poll {
            self.guard.evaluate(now, false, None);
            return;
        }

        let Some(client) = &self.config.control_endpoint else {
            return;
        };

        match client.poll_status().await {
            Ok(snap) => {
                self.legacy_fallback = false;
                if let Some(notice) = check_first_task_notice(self.first_task_notified, &snap) {
                    tracing::info!("{notice}");
                    self.first_task_notified = true;
                }
                let fs_active = if self.guard.enabled {
                    probe_fullscreen().is_active()
                } else {
                    false
                };
                match self.guard.evaluate(now, fs_active, Some(&snap)) {
                    FullscreenAction::Pause { reason } => {
                        tracing::info!("full-screen activity detected; pausing worker");
                        match client.pause(None, Some(reason)).await {
                            Ok(updated) => {
                                self.poll_state = ControlPollState::Connected(Box::new(updated));
                                return;
                            }
                            Err(err) => tracing::warn!("fullscreen pause failed: {err}"),
                        }
                    }
                    FullscreenAction::Resume => {
                        tracing::info!(
                            "full-screen cooldown elapsed; resuming fullscreen-paused worker"
                        );
                        match client.resume().await {
                            Ok(updated) => {
                                self.poll_state = ControlPollState::Connected(Box::new(updated));
                                return;
                            }
                            Err(err) => tracing::warn!("fullscreen resume failed: {err}"),
                        }
                    }
                    FullscreenAction::None => {}
                }
                self.poll_state = ControlPollState::Connected(Box::new(snap));
            }
            Err(err) => {
                self.guard.evaluate(now, false, None);
                self.poll_state = ControlPollState::Unavailable(err);
                if self
                    .sup_running_since
                    .is_some_and(|t| t.elapsed() >= LEGACY_CONTROL_GRACE)
                {
                    self.legacy_fallback = true;
                }
            }
        }
    }

    fn push_view(&mut self, view_tx: &mpsc::Sender<MenuViewModel>) {
        let view = derive_view(
            &self.poll_state,
            &self.sup_phase,
            self.guard.enabled,
            self.legacy_fallback,
        );
        if self.last_view.as_ref() != Some(&view) {
            self.last_view = Some(view.clone());
            let _ = view_tx.send(view);
        }
    }
}

async fn run_backend_loop(
    config: TrayAppConfig,
    mut cmd_rx: tokio_mpsc::UnboundedReceiver<TrayCommand>,
    view_tx: mpsc::Sender<MenuViewModel>,
) {
    let supervisor = config.supervisor.clone().map(WorkerSupervisor::new);
    let mut state = BackendState::new(&config, supervisor);
    let mut interval = tokio::time::interval(config.poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            maybe_cmd = cmd_rx.recv() => {
                let Some(cmd) = maybe_cmd else {
                    break;
                };
                if !state.handle_command(cmd).await {
                    break;
                }
                state.push_view(&view_tx);
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received interrupt signal; shutting down worker");
                break;
            }
            _ = interval.tick() => {
                state.tick().await;
                state.push_view(&view_tx);
            }
        }
    }

    if let Some(sup) = state.supervisor.as_mut() {
        sup.stop_and_wait().await;
    }
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
fn run_platform_ui(
    initial_view: MenuViewModel,
    _log_dir: PathBuf,
    cmd_tx: tokio_mpsc::UnboundedSender<TrayCommand>,
    view_rx: mpsc::Receiver<MenuViewModel>,
) -> Result<(), String> {
    native_ui::run(initial_view, cmd_tx, view_rx)
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
fn run_platform_ui(
    initial_view: MenuViewModel,
    _log_dir: PathBuf,
    _cmd_tx: tokio_mpsc::UnboundedSender<TrayCommand>,
    view_rx: mpsc::Receiver<MenuViewModel>,
) -> Result<(), String> {
    tracing::info!(
        "running in headless tray supervisor mode (use `oxideswarm-tray --status` to inspect state)"
    );
    tracing::info!("{}", initial_view.status_line);
    while let Ok(view) = view_rx.recv() {
        tracing::info!("{} [{}]", view.status_line, view.icon_state.as_str());
    }
    Ok(())
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
mod native_ui {
    use super::TrayCommand;
    use crate::state::{render_icon_rgba, MenuViewModel, TrayIconState};
    use muda::{CheckMenuItem, IsMenuItem, Menu, MenuEvent, MenuItem, PredefinedMenuItem, Submenu};
    use std::sync::mpsc;
    use tokio::sync::mpsc as tokio_mpsc;
    use tray_icon::{Icon, TrayIcon, TrayIconBuilder};

    const ID_STATUS: &str = "status";
    const ID_PAUSE_1H: &str = "pause_1h";
    const ID_PAUSE_INDEF: &str = "pause_indef";
    const ID_RESUME: &str = "resume";
    const ID_ALLOW_GPU: &str = "limit_allow_gpu";
    const ID_FULLSCREEN_PAUSE: &str = "limit_fullscreen_pause";
    const ID_PAUSE_BATTERY: &str = "limit_battery";
    const ID_PAUSE_METERED: &str = "limit_metered";
    const ID_OPEN_DASHBOARD: &str = "open_dashboard";
    const ID_OPEN_LOGS: &str = "open_logs";
    const ID_CHECK_UPDATES: &str = "check_updates";
    const ID_QUIT: &str = "quit";

    const LIMIT_CHOICES: &[(&str, &str, &str)] = &[
        ("cpu_none", "local_max_cpu_pct", "none"),
        ("cpu_25", "local_max_cpu_pct", "25"),
        ("cpu_50", "local_max_cpu_pct", "50"),
        ("cpu_75", "local_max_cpu_pct", "75"),
        ("cpu_90", "local_max_cpu_pct", "90"),
        ("hours_all", "hours", "none"),
        ("hours_night", "hours", "22:00-07:00"),
        ("hours_offwork", "hours", "18:00-08:00"),
        ("hours_day", "hours", "09:00-17:00"),
        ("cap_none", "max_task_hours_per_day", "none"),
        ("cap_2", "max_task_hours_per_day", "2"),
        ("cap_4", "max_task_hours_per_day", "4"),
        ("cap_8", "max_task_hours_per_day", "8"),
        ("cap_12", "max_task_hours_per_day", "12"),
    ];

    struct MenuHandles {
        status: MenuItem,
        pause_1h: MenuItem,
        pause_indef: MenuItem,
        resume: MenuItem,
        limits_submenu: Submenu,
        allow_gpu: CheckMenuItem,
        fullscreen_pause: CheckMenuItem,
        pause_battery: CheckMenuItem,
        pause_metered: CheckMenuItem,
        open_dashboard: MenuItem,
    }

    fn build_icon(state: TrayIconState) -> Result<Icon, String> {
        let (rgba, w, h) = render_icon_rgba(state);
        Icon::from_rgba(rgba, w, h).map_err(|e| format!("failed to build tray icon: {e}"))
    }

    fn build_menu(initial: &MenuViewModel) -> Result<(Menu, MenuHandles), String> {
        let status = MenuItem::with_id(ID_STATUS, &initial.status_line, false, None);
        let pause_1h = MenuItem::with_id(
            ID_PAUSE_1H,
            format!("Pause for 1 hour{}", initial.pause_label_suffix),
            initial.pause_1h_enabled,
            None,
        );
        let pause_indef = MenuItem::with_id(
            ID_PAUSE_INDEF,
            format!("Pause until I resume{}", initial.pause_label_suffix),
            initial.pause_indef_enabled,
            None,
        );
        let resume = MenuItem::with_id(ID_RESUME, "Resume", initial.resume_enabled, None);

        let allow_gpu = CheckMenuItem::with_id(
            ID_ALLOW_GPU,
            "Allow GPU tasks",
            initial.limits_enabled,
            !initial.limits.no_gpu_tasks,
            None,
        );
        let fullscreen_pause = CheckMenuItem::with_id(
            ID_FULLSCREEN_PAUSE,
            "Pause while full-screen",
            true,
            initial.fullscreen_guard_enabled,
            None,
        );
        let pause_battery = CheckMenuItem::with_id(
            ID_PAUSE_BATTERY,
            "Pause on battery",
            initial.limits_enabled,
            initial.limits.no_tasks_on_battery == Some(true),
            None,
        );
        let pause_metered = CheckMenuItem::with_id(
            ID_PAUSE_METERED,
            "Pause on metered network",
            initial.limits_enabled,
            initial.limits.no_tasks_on_metered == Some(true),
            None,
        );

        let cpu_none = MenuItem::with_id("cpu_none", "No limit", true, None);
        let cpu_25 = MenuItem::with_id("cpu_25", "25%", true, None);
        let cpu_50 = MenuItem::with_id("cpu_50", "50%", true, None);
        let cpu_75 = MenuItem::with_id("cpu_75", "75%", true, None);
        let cpu_90 = MenuItem::with_id("cpu_90", "90%", true, None);
        let cpu_menu = Submenu::with_items(
            "CPU limit",
            true,
            &[&cpu_none, &cpu_25, &cpu_50, &cpu_75, &cpu_90],
        )
        .map_err(|e| format!("failed to create CPU limit submenu: {e}"))?;

        let h_all = MenuItem::with_id("hours_all", "Always (any hour)", true, None);
        let h_night = MenuItem::with_id("hours_night", "Overnight (22:00-07:00)", true, None);
        let h_off = MenuItem::with_id("hours_offwork", "Off-work (18:00-08:00)", true, None);
        let h_day = MenuItem::with_id("hours_day", "Daytime (09:00-17:00)", true, None);
        let hours_menu =
            Submenu::with_items("Allowed hours", true, &[&h_all, &h_night, &h_off, &h_day])
                .map_err(|e| format!("failed to create Allowed hours submenu: {e}"))?;

        let c_none = MenuItem::with_id("cap_none", "Unlimited", true, None);
        let c_2 = MenuItem::with_id("cap_2", "2 hours/day", true, None);
        let c_4 = MenuItem::with_id("cap_4", "4 hours/day", true, None);
        let c_8 = MenuItem::with_id("cap_8", "8 hours/day", true, None);
        let c_12 = MenuItem::with_id("cap_12", "12 hours/day", true, None);
        let cap_menu =
            Submenu::with_items("Daily task cap", true, &[&c_none, &c_2, &c_4, &c_8, &c_12])
                .map_err(|e| format!("failed to create Daily task cap submenu: {e}"))?;

        let sep_limits = PredefinedMenuItem::separator();
        let limits_submenu = Submenu::with_items(
            "Limits",
            initial.limits_enabled,
            &[
                &allow_gpu,
                &fullscreen_pause,
                &sep_limits,
                &pause_battery,
                &pause_metered,
                &cpu_menu,
                &hours_menu,
                &cap_menu,
            ],
        )
        .map_err(|e| format!("failed to create Limits submenu: {e}"))?;

        let open_dashboard = MenuItem::with_id(
            ID_OPEN_DASHBOARD,
            "Open dashboard",
            initial.dashboard_url.is_some(),
            None,
        );
        let open_logs = MenuItem::with_id(ID_OPEN_LOGS, "Open logs", true, None);
        let check_updates = MenuItem::with_id(ID_CHECK_UPDATES, "Check for updates", true, None);
        let quit = MenuItem::with_id(ID_QUIT, "Quit", true, None);

        let sep1 = PredefinedMenuItem::separator();
        let sep2 = PredefinedMenuItem::separator();
        let sep3 = PredefinedMenuItem::separator();
        let sep4 = PredefinedMenuItem::separator();

        let menu = Menu::new();
        let items: [&dyn IsMenuItem; 13] = [
            &status,
            &sep1,
            &pause_1h,
            &pause_indef,
            &resume,
            &sep2,
            &limits_submenu,
            &sep3,
            &open_dashboard,
            &open_logs,
            &check_updates,
            &sep4,
            &quit,
        ];
        menu.append_items(&items)
            .map_err(|e| format!("failed to construct tray menu: {e}"))?;

        Ok((
            menu,
            MenuHandles {
                status,
                pause_1h,
                pause_indef,
                resume,
                limits_submenu,
                allow_gpu,
                fullscreen_pause,
                pause_battery,
                pause_metered,
                open_dashboard,
            },
        ))
    }

    fn sync_view(
        handles: &MenuHandles,
        tray: &TrayIcon,
        view: &MenuViewModel,
        last_icon: &mut TrayIconState,
    ) {
        handles.status.set_text(&view.status_line);
        handles
            .pause_1h
            .set_text(format!("Pause for 1 hour{}", view.pause_label_suffix));
        handles.pause_1h.set_enabled(view.pause_1h_enabled);
        handles
            .pause_indef
            .set_text(format!("Pause until I resume{}", view.pause_label_suffix));
        handles.pause_indef.set_enabled(view.pause_indef_enabled);
        handles.resume.set_enabled(view.resume_enabled);
        handles.limits_submenu.set_enabled(view.limits_enabled);
        handles.allow_gpu.set_enabled(view.limits_enabled);
        handles.allow_gpu.set_checked(!view.limits.no_gpu_tasks);
        handles
            .fullscreen_pause
            .set_checked(view.fullscreen_guard_enabled);
        handles.pause_battery.set_enabled(view.limits_enabled);
        handles
            .pause_battery
            .set_checked(view.limits.no_tasks_on_battery == Some(true));
        handles.pause_metered.set_enabled(view.limits_enabled);
        handles
            .pause_metered
            .set_checked(view.limits.no_tasks_on_metered == Some(true));
        handles
            .open_dashboard
            .set_enabled(view.dashboard_url.is_some());
        if let Err(err) = tray.set_tooltip(Some(&view.tooltip)) {
            tracing::debug!("failed to update tray tooltip: {err}");
        }
        if *last_icon != view.icon_state {
            if let Ok(icon) = build_icon(view.icon_state) {
                if let Err(err) = tray.set_icon(Some(icon)) {
                    tracing::debug!("failed to update tray icon: {err}");
                } else {
                    *last_icon = view.icon_state;
                }
            }
        }
    }

    fn map_menu_event(id: &str, current: &MenuViewModel) -> Option<TrayCommand> {
        match id {
            ID_PAUSE_1H => Some(TrayCommand::PauseFor1h),
            ID_PAUSE_INDEF => Some(TrayCommand::PauseUntilResume),
            ID_RESUME => Some(TrayCommand::Resume),
            ID_ALLOW_GPU => Some(TrayCommand::SetLimit {
                key: "no_gpu_tasks".to_string(),
                value: if current.limits.no_gpu_tasks {
                    "false"
                } else {
                    "true"
                }
                .to_string(),
            }),
            ID_FULLSCREEN_PAUSE => Some(TrayCommand::ToggleFullscreenGuard),
            ID_PAUSE_BATTERY => Some(TrayCommand::SetLimit {
                key: "no_tasks_on_battery".to_string(),
                value: if current.limits.no_tasks_on_battery == Some(true) {
                    "false"
                } else {
                    "true"
                }
                .to_string(),
            }),
            ID_PAUSE_METERED => Some(TrayCommand::SetLimit {
                key: "no_tasks_on_metered".to_string(),
                value: if current.limits.no_tasks_on_metered == Some(true) {
                    "false"
                } else {
                    "true"
                }
                .to_string(),
            }),
            ID_OPEN_DASHBOARD => Some(TrayCommand::OpenDashboard),
            ID_OPEN_LOGS => Some(TrayCommand::OpenLogs),
            ID_CHECK_UPDATES => Some(TrayCommand::CheckUpdates),
            ID_QUIT => Some(TrayCommand::Quit),
            other => LIMIT_CHOICES
                .iter()
                .find(|(choice_id, _, _)| *choice_id == other)
                .map(|(_, key, val)| TrayCommand::SetLimit {
                    key: (*key).to_string(),
                    value: (*val).to_string(),
                }),
        }
    }

    #[cfg(target_os = "windows")]
    mod win_pump {
        #![allow(unsafe_code)]

        use std::mem::zeroed;
        use std::ptr;
        use windows_sys::Win32::UI::WindowsAndMessaging::{
            DispatchMessageW, MsgWaitForMultipleObjects, PeekMessageW, TranslateMessage, MSG,
            PM_REMOVE, QS_ALLINPUT, WM_QUIT,
        };

        pub fn pump_messages() -> bool {
            // SAFETY: `MSG` is a plain C struct safe to zero-initialize; `PeekMessageW`,
            // `TranslateMessage`, and `DispatchMessageW` operate on the valid stack `msg`.
            unsafe {
                let mut msg: MSG = zeroed();
                while PeekMessageW(&mut msg, ptr::null_mut(), 0, 0, PM_REMOVE) != 0 {
                    if msg.message == WM_QUIT {
                        return false;
                    }
                    TranslateMessage(&msg);
                    DispatchMessageW(&msg);
                }
            }
            true
        }

        pub fn wait_briefly(timeout_ms: u32) {
            // SAFETY: Passing zero wait handles with `pHandles = null` is explicitly permitted
            // by `MsgWaitForMultipleObjects` to wait up to `timeout_ms` for window messages.
            unsafe {
                MsgWaitForMultipleObjects(0, ptr::null(), 0, timeout_ms, QS_ALLINPUT);
            }
        }
    }

    pub fn run(
        initial_view: MenuViewModel,
        cmd_tx: tokio_mpsc::UnboundedSender<TrayCommand>,
        view_rx: mpsc::Receiver<MenuViewModel>,
    ) -> Result<(), String> {
        let icon = build_icon(initial_view.icon_state)?;
        let (menu, handles) = build_menu(&initial_view)?;
        let tray = TrayIconBuilder::new()
            .with_menu(Box::new(menu))
            .with_tooltip(&initial_view.tooltip)
            .with_icon(icon)
            .build()
            .map_err(|e| format!("failed to create tray icon: {e}"))?;

        let mut current_view = initial_view;
        let mut last_icon = current_view.icon_state;
        let menu_rx = MenuEvent::receiver();

        loop {
            #[cfg(target_os = "windows")]
            if !win_pump::pump_messages() {
                let _ = cmd_tx.send(TrayCommand::Quit);
                break;
            }

            while let Ok(event) = menu_rx.try_recv() {
                if let Some(cmd) = map_menu_event(event.id().as_ref(), &current_view) {
                    let is_quit = cmd == TrayCommand::Quit;
                    let _ = cmd_tx.send(cmd);
                    if is_quit {
                        return Ok(());
                    }
                }
            }

            loop {
                match view_rx.try_recv() {
                    Ok(updated) => {
                        sync_view(&handles, &tray, &updated, &mut last_icon);
                        current_view = updated;
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
                }
            }

            #[cfg(target_os = "windows")]
            win_pump::wait_briefly(80);
            #[cfg(not(target_os = "windows"))]
            std::thread::sleep(std::time::Duration::from_millis(80));
        }

        Ok(())
    }
}
