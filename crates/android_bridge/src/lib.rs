//! Android JNI Bridge for OxideSwarm Worker (`liboxideworker.so`).
//!
//! Provides genuine JNI exported functions allowing Android applications
//! (`com.oxideswarm.worker.service.OxideWorkerBridge` / `com.oxideswarm.worker.runner.OxideWorkerBridge`)
//! to start, monitor, and stop the OxideSwarm distributed grid worker
//! entirely in-process on native POSIX threads running multi-threaded Tokio runtimes.
//!
//! Running in-process produces 0 child processes for the worker loop itself, ensuring immunity
//! against the Android 12+ Phantom Process Killer (PPK) and W^X execution restrictions.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};
use uuid::Uuid;

use jni::objects::{JClass, JString};
use jni::refs::Reference;
use jni::sys::{jboolean, jfloat, jint, jlong, jstring, JNI_FALSE, JNI_TRUE};
use jni::{Env, EnvUnowned};
use rusty_grid_core::capabilities::MobileLiveStatus;
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_worker::{
    MobileTelemetryHandle, SandboxConfig, SandboxError, WorkerClient, WorkerConfig,
};

// ==============================================================================
// Mobile Telemetry Model
// ==============================================================================

/// Mobile-specific hardware and environmental telemetry injected from the Android SDK.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MobileTelemetry {
    pub battery_pct: Option<u8>,
    pub battery_temperature: Option<f32>,
    pub is_charging: Option<bool>,
    pub thermal_throttled: bool,
    pub network_type: Option<String>,
}

impl MobileTelemetry {
    /// Convenience getter for battery temperature in degrees Celsius.
    pub fn battery_temp_c(&self) -> Option<f32> {
        self.battery_temperature
    }
}

// ==============================================================================
// Worker State & Lifecycle
// ==============================================================================

/// Global state representing the in-process worker instance.
struct WorkerState {
    lifecycle: Mutex<()>,
    running: AtomicBool,
    stopping: AtomicBool,
    generation: AtomicU64,
    p2p_minimal_preset: AtomicBool,
    worker_id: Mutex<Option<Uuid>>,
    p2p_ticket: Mutex<Option<String>>,
    cluster_secret: Mutex<Option<ClusterSecret>>,
    status: Mutex<String>,
    telemetry: Mutex<MobileTelemetry>,
    /// The same readings, shared with the running `WorkerClient`: it reports every change to the
    /// Master (`WorkerMessage::MobileTelemetry`) and refuses tasks on a critically low battery.
    mobile: MobileTelemetryHandle,
    /// Writable directory for task sandboxes, handed over by the app (its cache directory) before
    /// the worker starts. The default, `/data/local/tmp`, is closed to apps.
    sandbox_dir: Mutex<Option<PathBuf>>,
    evacuation_reason: Mutex<Option<String>>,
    shutdown_tx: Mutex<Option<watch::Sender<bool>>>,
    thread_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl WorkerState {
    fn new() -> Self {
        Self {
            lifecycle: Mutex::new(()),
            running: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            p2p_minimal_preset: AtomicBool::new(false),
            worker_id: Mutex::new(None),
            p2p_ticket: Mutex::new(None),
            cluster_secret: Mutex::new(None),
            status: Mutex::new("STOPPED".to_string()),
            telemetry: Mutex::new(MobileTelemetry::default()),
            mobile: MobileTelemetryHandle::default(),
            sandbox_dir: Mutex::new(None),
            evacuation_reason: Mutex::new(None),
            shutdown_tx: Mutex::new(None),
            thread_handle: Mutex::new(None),
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

static LIVE_WORKER_THREADS: AtomicUsize = AtomicUsize::new(0);
static MAX_LIVE_WORKER_THREADS: AtomicUsize = AtomicUsize::new(0);

type StartClaimHook = Box<dyn FnOnce() + Send + 'static>;

static START_CLAIM_HOOK: Mutex<Option<StartClaimHook>> = Mutex::new(None);

struct WorkerThreadGuard;

impl WorkerThreadGuard {
    fn new() -> Self {
        let live = LIVE_WORKER_THREADS.fetch_add(1, Ordering::SeqCst) + 1;
        MAX_LIVE_WORKER_THREADS.fetch_max(live, Ordering::SeqCst);
        debug_assert!(
            live <= 1,
            "Invariant violated: multiple worker threads alive simultaneously (count={live})"
        );
        Self
    }
}

impl Drop for WorkerThreadGuard {
    fn drop(&mut self) {
        LIVE_WORKER_THREADS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Returns the current number of active worker threads (for testing).
#[doc(hidden)]
pub fn live_worker_threads_for_testing() -> usize {
    LIVE_WORKER_THREADS.load(Ordering::SeqCst)
}

/// Returns the maximum number of simultaneously active worker threads observed (for testing).
#[doc(hidden)]
pub fn max_live_worker_threads_for_testing() -> usize {
    MAX_LIVE_WORKER_THREADS.load(Ordering::SeqCst)
}

/// Resets the worker thread count and high-water mark (for testing).
#[doc(hidden)]
pub fn reset_worker_thread_counts_for_testing() {
    LIVE_WORKER_THREADS.store(0, Ordering::SeqCst);
    MAX_LIVE_WORKER_THREADS.store(0, Ordering::SeqCst);
}

static WORKER_STATE: OnceLock<Arc<WorkerState>> = OnceLock::new();

fn get_worker_state() -> Arc<WorkerState> {
    WORKER_STATE
        .get_or_init(|| {
            rusty_grid_worker::client::set_evacuation_reason_hook(get_evacuation_reason_impl);
            Arc::new(WorkerState::new())
        })
        .clone()
}

/// Names the directory task sandboxes are created in (the app's cache directory, for example
/// `<cacheDir>/sandboxes`). Call it before [`start_worker_impl`]; without it the worker falls back
/// to `RUSTY_GRID_SANDBOX_BASE_DIR`, `TMPDIR` and the platform temp dir, and on Android that last
/// one (`/data/local/tmp`) is not writable for an app. An empty string clears the setting.
pub fn set_sandbox_dir_impl(dir: String) -> bool {
    let dir = dir.trim();
    *lock_unpoisoned(&get_worker_state().sandbox_dir) =
        (!dir.is_empty()).then(|| PathBuf::from(dir));
    true
}

/// Selects `iroh::endpoint::presets::Minimal` (no relay or external address lookup) for the
/// in-process Android worker's P2P endpoint during tests.
#[doc(hidden)]
pub fn set_p2p_minimal_preset_for_testing(enabled: bool) {
    get_worker_state()
        .p2p_minimal_preset
        .store(enabled, Ordering::SeqCst);
}

/// Configures or clears the shared cluster secret for worker authentication against the Master node.
/// Accepts either a hex string (at least 32 hex chars / 16 bytes) or a file path to a cluster secret file.
/// Passing an empty string clears the secret.
pub fn set_cluster_secret_impl(secret_text: &str) -> bool {
    let state = get_worker_state();
    let trimmed = secret_text.trim();
    if trimmed.is_empty() {
        *lock_unpoisoned(&state.cluster_secret) = None;
        return true;
    }

    let secret = match ClusterSecret::from_hex(trimmed) {
        Ok(s) => s,
        Err(_) => {
            let path = PathBuf::from(trimmed);
            match ClusterSecret::load_file(&path) {
                Ok(s) => s,
                Err(e) => {
                    error!(error = %e, "Invalid cluster secret: neither valid hex nor loadable file");
                    *lock_unpoisoned(&state.status) = format!("ERROR: Invalid cluster secret: {e}");
                    return false;
                }
            }
        }
    };

    *lock_unpoisoned(&state.cluster_secret) = Some(secret);
    true
}

/// Same check as the worker's own start-up check (`SandboxConfig::ensure_usable`), without a
/// runtime: `start_worker_impl` has to answer the app synchronously, and may itself be called
/// from inside one. For the default base it first creates the private root with mode 700
/// (design 0021); the app's cache directory is used as given.
fn check_sandbox_usable(sandbox: &SandboxConfig) -> Result<(), SandboxError> {
    sandbox.ensure_private_root_blocking()?;
    let base_dir = &sandbox.base_dir;
    let unusable = |source: std::io::Error| SandboxError::BaseDirUnusable {
        path: base_dir.to_path_buf(),
        source,
    };
    std::fs::create_dir_all(base_dir).map_err(unusable)?;
    let probe = base_dir.join(format!(".write_probe_{}", std::process::id()));
    std::fs::write(&probe, b"probe").map_err(unusable)?;
    let _ = std::fs::remove_file(&probe);
    Ok(())
}

/// Core implementation for starting the native worker client in a background Tokio runtime.
/// Accepts an optional `p2p_ticket` for connecting across WAN/CGNAT via Iroh QUIC/DERP relays.
///
/// Returns `false`, with the reason in the status (`ERROR: ...` or `STOPPING`, see
/// [`get_worker_status_impl`]), when the worker cannot start at all: a previous worker is still
/// draining after [`stop_worker_impl`], no writable sandbox directory is configured, or the thread
/// cannot be spawned. A failure after that point (an invalid P2P ticket) is reported through the
/// status.
pub fn start_worker_impl(
    master_addr: String,
    p2p_ticket: Option<String>,
    worker_name: String,
    cores: i32,
    ram_mb: i64,
    simulate_gpu: bool,
    heartbeat_interval_secs: i64,
) -> bool {
    let state = get_worker_state();

    // If a previous `stop_worker_impl` call is still draining its worker thread, wait briefly
    // (up to 30 ms) outside the lifecycle lock for an already-idle runtime to finish joining.
    if state.stopping.load(Ordering::SeqCst) {
        let deadline = std::time::Instant::now() + Duration::from_millis(30);
        while state.stopping.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    let _lifecycle_guard = lock_unpoisoned(&state.lifecycle);

    // Claim `running` atomically under the lifecycle lock so two concurrent start calls start at
    // most one worker; every early-return error path below releases it (`running = false`).
    if state
        .running
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        info!("Worker is already running in-process");
        return true;
    }

    // Test hook fired immediately after claiming `running`; production never sets this hook.
    if let Some(hook) = lock_unpoisoned(&START_CLAIM_HOOK).take() {
        hook();
    }

    // Re-check `stopping` under the lifecycle lock: if the previous worker is still draining tasks
    // after the short wait above, refuse the start immediately with status `STOPPING` rather than
    // blocking the calling UI thread on `join()` or running two workers at once.
    if state.stopping.load(Ordering::SeqCst) {
        *lock_unpoisoned(&state.status) = "STOPPING".to_string();
        state.running.store(false, Ordering::SeqCst);
        return false;
    }

    let mut config = WorkerConfig::new(&master_addr);
    if let Some(ticket) = p2p_ticket.as_ref().filter(|t| !t.trim().is_empty()) {
        config = config.with_p2p_ticket(ticket.clone());
    }
    if state.p2p_minimal_preset.load(Ordering::SeqCst) {
        config = config.with_p2p_minimal_preset(true);
    }
    if !worker_name.is_empty() {
        config = config.with_name(&worker_name);
    }
    if cores > 0 {
        config = config.with_cores(cores as usize);
    }
    if ram_mb > 0 {
        config = config.with_ram_mb(ram_mb as u64);
    }
    config = config
        .with_simulate_gpu(simulate_gpu)
        .with_no_gpu(!simulate_gpu)
        .with_no_blender(true)
        .with_no_cuda(true);
    if let Some(dir) = lock_unpoisoned(&state.sandbox_dir).clone() {
        config = config.with_sandbox_base_dir(dir);
    }
    if let Some(sec) = lock_unpoisoned(&state.cluster_secret).clone() {
        config = config.with_cluster_secret(sec);
    }
    // Battery / thermal readings pushed by the Android app (`update_telemetry_*`) flow into the
    // worker through this handle for as long as it runs.
    config = config.with_mobile_telemetry(state.mobile.clone());
    if heartbeat_interval_secs > 0 {
        config.default_heartbeat_interval = Duration::from_secs(heartbeat_interval_secs as u64);
    }

    // The worker refuses to run without a writable sandbox directory (every process task would
    // fail otherwise); say so here, where the app can show it, instead of from a thread that
    // exits unnoticed.
    let sandbox = match config.sandbox_base_dir.clone() {
        Some(dir) => SandboxConfig::new(dir),
        None => SandboxConfig::default(),
    };
    if let Err(e) = check_sandbox_usable(&sandbox) {
        error!(error = %e, "Android worker cannot start: task sandboxes cannot be created");
        *lock_unpoisoned(&state.status) = format!("ERROR: {e}");
        state.running.store(false, Ordering::SeqCst);
        return false;
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_id = config.worker_id.unwrap_or_else(Uuid::new_v4);
    config = config.with_worker_id(worker_id);

    *lock_unpoisoned(&state.worker_id) = Some(worker_id);
    *lock_unpoisoned(&state.p2p_ticket) = p2p_ticket;
    *lock_unpoisoned(&state.shutdown_tx) = Some(shutdown_tx);
    *lock_unpoisoned(&state.status) = "STARTING".to_string();

    let my_generation = state.generation.fetch_add(1, Ordering::SeqCst) + 1;

    let state_clone = Arc::clone(&state);
    let handle = std::thread::Builder::new()
        .name("OxideWorker-Tokio".to_string())
        .spawn(move || {
            let self_exit_status: Option<String> = {
                let _worker_guard = WorkerThreadGuard::new();
                if *shutdown_rx.borrow() {
                    None
                } else {
                    match tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => {
                            let status_opt = rt.block_on(async {
                                if *shutdown_rx.borrow() {
                                    return None;
                                }
                                {
                                    let _lifecycle_guard = lock_unpoisoned(&state_clone.lifecycle);
                                    if state_clone.generation.load(Ordering::SeqCst) == my_generation
                                        && !state_clone.stopping.load(Ordering::SeqCst)
                                    {
                                        *lock_unpoisoned(&state_clone.status) = "RUNNING".to_string();
                                    }
                                }
                                let mut init_shutdown_rx = shutdown_rx.clone();
                                let client_opt = tokio::select! {
                                    biased;
                                    _ = init_shutdown_rx.changed() => None,
                                    res = tokio::task::spawn_blocking(move || WorkerClient::new(config)) => res.ok(),
                                };
                                let Some(mut client) = client_opt else {
                                    return (!*shutdown_rx.borrow())
                                        .then(|| "STOPPED".to_string());
                                };
                                if *shutdown_rx.borrow() {
                                    return None;
                                }
                                info!(worker_id = %worker_id, "In-process Android worker starting run loop");
                                let stop_check_rx = shutdown_rx.clone();
                                let outcome = client.run(shutdown_rx).await;
                                if *stop_check_rx.borrow() {
                                    None
                                } else {
                                    Some(match outcome {
                                        Ok(()) => "STOPPED".to_string(),
                                        Err(e) => {
                                            error!(worker_id = %worker_id, error = %e, "Worker supervisor exited with error");
                                            format!("ERROR: {e}")
                                        }
                                    })
                                }
                            });
                            rt.shutdown_background();
                            status_opt
                        }
                        Err(e) => {
                            error!("Failed to initialize Tokio runtime for Android worker: {}", e);
                            (!*shutdown_rx.borrow()).then(|| format!("ERROR: {e}"))
                        }
                    }
                }
            };

            if let Some(final_status) = self_exit_status {
                let _lifecycle_guard = lock_unpoisoned(&state_clone.lifecycle);
                if state_clone.generation.load(Ordering::SeqCst) == my_generation
                    && !state_clone.stopping.load(Ordering::SeqCst)
                {
                    *lock_unpoisoned(&state_clone.status) = final_status;
                    state_clone.running.store(false, Ordering::SeqCst);
                }
            }
        });

    match handle {
        Ok(h) => {
            *lock_unpoisoned(&state.thread_handle) = Some(h);
            true
        }
        Err(e) => {
            error!("Failed to spawn native worker thread: {}", e);
            *lock_unpoisoned(&state.shutdown_tx) = None;
            state.running.store(false, Ordering::SeqCst);
            *lock_unpoisoned(&state.status) = format!("SPAWN_ERROR: {}", e);
            false
        }
    }
}

/// Backward-compatible worker start implementation without P2P ticket parameter.
pub fn start_worker_legacy_impl(
    master_addr: String,
    worker_name: String,
    cores: i32,
    ram_mb: i64,
    simulate_gpu: bool,
    heartbeat_interval_secs: i64,
) -> bool {
    start_worker_impl(
        master_addr,
        None,
        worker_name,
        cores,
        ram_mb,
        simulate_gpu,
        heartbeat_interval_secs,
    )
}

/// Worker start implementation accepting an explicit cluster secret parameter.
#[allow(clippy::too_many_arguments)]
pub fn start_worker_with_secret_impl(
    master_addr: String,
    p2p_ticket: Option<String>,
    cluster_secret: Option<String>,
    worker_name: String,
    cores: i32,
    ram_mb: i64,
    simulate_gpu: bool,
    heartbeat_interval_secs: i64,
) -> bool {
    if let Some(ref sec) = cluster_secret {
        if !set_cluster_secret_impl(sec) {
            return false;
        }
    }
    start_worker_impl(
        master_addr,
        p2p_ticket,
        worker_name,
        cores,
        ram_mb,
        simulate_gpu,
        heartbeat_interval_secs,
    )
}

/// Core implementation for stopping the native worker client without blocking the calling thread.
///
/// Signals the worker's shutdown channel, marks the bridge as stopping (`stopping = true`,
/// status `STOPPING`), releases the running state, and spawns a detached thread to `join()` the
/// worker thread and set the final status (`STOPPED`) once the worker finishes draining.
pub fn stop_worker_impl() -> bool {
    let state = get_worker_state();
    let _lifecycle_guard = lock_unpoisoned(&state.lifecycle);
    let was_stopping = state.stopping.swap(true, Ordering::SeqCst);

    let handle_opt = lock_unpoisoned(&state.thread_handle).take();
    let tx_opt = lock_unpoisoned(&state.shutdown_tx).take();
    if let Some(ref tx) = tx_opt {
        let _ = tx.send(true);
    }

    if handle_opt.is_some() {
        *lock_unpoisoned(&state.status) = "STOPPING".to_string();
    }

    let was_running = state
        .running
        .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok();

    if !was_running && handle_opt.is_none() {
        if !was_stopping {
            state.stopping.store(false, Ordering::SeqCst);
        }
        return true;
    }

    if let Some(h) = handle_opt {
        let state_clone = Arc::clone(&state);
        std::thread::spawn(move || {
            let _ = h.join();
            let _lifecycle_guard = lock_unpoisoned(&state_clone.lifecycle);
            *lock_unpoisoned(&state_clone.status) = "STOPPED".to_string();
            state_clone.stopping.store(false, Ordering::SeqCst);
        });
    } else if !was_stopping {
        state.stopping.store(false, Ordering::SeqCst);
        *lock_unpoisoned(&state.status) = "STOPPED".to_string();
    }
    true
}

/// Core implementation for checking if the native worker is running.
pub fn is_running_impl() -> bool {
    let state = get_worker_state();
    state.running.load(Ordering::SeqCst)
}

/// Core implementation for querying detailed worker status JSON.
pub fn get_worker_status_impl() -> String {
    let state = get_worker_state();
    let status = lock_unpoisoned(&state.status).clone();
    let worker_id_str = lock_unpoisoned(&state.worker_id)
        .map(|u| u.to_string())
        .unwrap_or_default();
    let is_running = state.running.load(Ordering::SeqCst);
    let p2p_ticket = lock_unpoisoned(&state.p2p_ticket).clone();
    let telemetry = lock_unpoisoned(&state.telemetry).clone();

    serde_json::json!({
        "status": status,
        "running": is_running,
        "worker_id": worker_id_str,
        "p2p_ticket": p2p_ticket,
        "telemetry": {
            "battery_pct": telemetry.battery_pct,
            "battery_temperature": telemetry.battery_temperature,
            "is_charging": telemetry.is_charging,
            "thermal_throttled": telemetry.thermal_throttled,
            "network_type": telemetry.network_type,
        },
    })
    .to_string()
}

/// Detailed mobile telemetry update supporting battery temperature and active network type.
pub fn update_telemetry_detailed_impl(
    battery_pct: i32,
    is_charging: bool,
    thermal_throttled: bool,
    battery_temperature: Option<f32>,
    network_type: Option<String>,
) -> bool {
    let state = get_worker_state();
    let pct = if (0..=100).contains(&battery_pct) {
        Some(battery_pct as u8)
    } else {
        None
    };
    let temp = battery_temperature.filter(|&t| !t.is_nan() && (-50.0..=120.0).contains(&t));
    let net = network_type.and_then(|s| {
        let trimmed = s.trim();
        if trimmed.is_empty()
            || trimmed.eq_ignore_ascii_case("unknown")
            || trimmed.eq_ignore_ascii_case("none")
        {
            None
        } else {
            Some(trimmed.to_lowercase())
        }
    });

    let mut telem = lock_unpoisoned(&state.telemetry);
    telem.battery_pct = pct;
    telem.battery_temperature = temp;
    telem.is_charging = Some(is_charging);
    telem.thermal_throttled = thermal_throttled;
    telem.network_type = net.clone();
    // The worker's scheduling view of this device: without this the Master would keep the
    // registration-time snapshot for the whole run.
    let bat_tier = MobileLiveStatus::compute_battery_tier(pct, Some(is_charging));
    let therm_tier = MobileLiveStatus::compute_thermal_tier(temp, thermal_throttled);
    state.mobile.update(MobileLiveStatus {
        battery_pct: pct,
        is_charging: Some(is_charging),
        thermal_throttled,
        battery_temperature: temp,
        network_type: net,
        battery_tier: Some(bat_tier),
        thermal_tier: Some(therm_tier),
    });
    true
}

/// Updates the mobile telemetry state injected from the Android SDK.
/// Backward-compatible wrapper calling `update_telemetry_detailed_impl`.
pub fn update_telemetry_impl(battery_pct: i32, is_charging: bool, thermal_throttled: bool) -> bool {
    update_telemetry_detailed_impl(battery_pct, is_charging, thermal_throttled, None, None)
}

/// Triggers fast pre-sleep task evacuation (< 50ms) before the OS suspends the app process.
/// Stores the evacuation reason and triggers shutdown_tx so active tasks are aborted immediately
/// without waiting for the 10-second drain timeout.
pub fn trigger_fast_evacuation_impl(reason: &str) -> bool {
    let state = get_worker_state();
    let r = if reason.trim().is_empty() {
        "PRE_SLEEP_EVACUATION".to_string()
    } else {
        reason.trim().to_string()
    };
    info!(reason = %r, "Triggering fast pre-sleep task evacuation (<50ms)");
    *lock_unpoisoned(&state.evacuation_reason) = Some(r);
    rusty_grid_worker::client::set_evacuation_reason_hook(get_evacuation_reason_impl);
    if let Some(tx) = lock_unpoisoned(&state.shutdown_tx).as_ref() {
        let _ = tx.send(true);
    }
    true
}

/// Retrieves the active evacuation reason, if any.
pub fn get_evacuation_reason_impl() -> Option<String> {
    let state = get_worker_state();
    let reason = lock_unpoisoned(&state.evacuation_reason).clone();
    reason
}

/// Retrieves the current cached mobile telemetry.
pub fn get_telemetry_impl() -> MobileTelemetry {
    let state = get_worker_state();
    let telem = lock_unpoisoned(&state.telemetry).clone();
    telem
}

/// Core implementation for probing the LAN via UDP discovery to locate an active Master.
pub fn discover_master_impl(timeout_ms: i64) -> String {
    let timeout = if timeout_ms > 0 {
        Duration::from_millis(timeout_ms as u64)
    } else {
        Duration::from_millis(2500)
    };

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            return serde_json::json!({
                "found": false,
                "error": format!("Failed to initialize Tokio runtime: {e}")
            })
            .to_string();
        }
    };

    let beacon_opt = rt.block_on(rusty_grid_core::discovery::discover_master(
        timeout,
        rusty_grid_core::DEFAULT_DISCOVERY_PORT,
    ));

    match beacon_opt {
        Some(beacon) => serde_json::json!({
            "found": true,
            "cluster_addr": beacon.cluster_addr,
            "web_ui_url": beacon.web_ui_url,
            "hostname": beacon.hostname,
            "worker_count": beacon.worker_count,
            "timestamp": beacon.timestamp,
        })
        .to_string(),
        None => serde_json::json!({
            "found": false
        })
        .to_string(),
    }
}

// ==============================================================================
// JNI helpers (jni 0.22: native methods receive an `EnvUnowned`, JNI calls need an `Env`)
// ==============================================================================

/// Runs `f` with the thread's `Env`. A JNI error is returned to the caller, who picks the
/// fallback; a Rust panic is not caught here, so it still stops at the `extern "system"`
/// boundary exactly as it did with `jni` 0.21.
fn with_jni_env<'local, T>(
    env: &mut EnvUnowned<'local>,
    f: impl FnOnce(&mut Env<'local>) -> jni::errors::Result<T>,
) -> jni::errors::Result<T> {
    match env.with_env_no_catch(f).into_outcome() {
        jni::Outcome::Ok(value) => Ok(value),
        jni::Outcome::Err(err) => Err(err),
        jni::Outcome::Panic(payload) => std::panic::resume_unwind(payload),
    }
}

/// Copies a Java string into a Rust `String`.
fn jstring_to_string<'local>(
    env: &mut EnvUnowned<'local>,
    value: &JString<'local>,
) -> jni::errors::Result<String> {
    with_jni_env(env, |env| value.try_to_string(env))
}

/// Builds a Java string and hands its raw reference to the JVM, or null when that fails.
fn string_to_jstring<'local>(env: &mut EnvUnowned<'local>, value: &str) -> jstring {
    match with_jni_env(env, |env| JString::from_str(env, value)) {
        Ok(s) => s.as_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

// ==============================================================================
// JNI Exported Functions: com.oxideswarm.worker.runner.OxideWorkerBridge
// ==============================================================================

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeStartWorker<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    master_addr: JString<'local>,
    p2p_ticket: JString<'local>,
    worker_name: JString<'local>,
    cores: jint,
    ram_mb: jlong,
    simulate_gpu: jboolean,
    heartbeat_interval_secs: jlong,
) -> jboolean {
    let master: String = match jstring_to_string(&mut env, &master_addr) {
        Ok(s) => s,
        Err(_) => return JNI_FALSE,
    };
    let ticket_str: Option<String> = match jstring_to_string(&mut env, &p2p_ticket) {
        Ok(s) => {
            let val: String = s;
            if val.trim().is_empty() {
                None
            } else {
                Some(val)
            }
        }
        Err(_) => None,
    };
    let name: String = jstring_to_string(&mut env, &worker_name).unwrap_or_default();
    if start_worker_impl(
        master,
        ticket_str,
        name,
        cores,
        ram_mb,
        simulate_gpu,
        heartbeat_interval_secs,
    ) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

/// Hands the app's writable sandbox directory to the worker; call it before `nativeStartWorker`.
#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeSetSandboxDir<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    sandbox_dir: JString<'local>,
) -> jboolean {
    let dir: String = match jstring_to_string(&mut env, &sandbox_dir) {
        Ok(s) => s,
        Err(_) => return JNI_FALSE,
    };
    if set_sandbox_dir_impl(dir) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

/// Sets the shared cluster secret before starting the worker.
#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeSetClusterSecret<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    cluster_secret: JString<'local>,
) -> jboolean {
    let secret_str: String = match jstring_to_string(&mut env, &cluster_secret) {
        Ok(s) => s,
        Err(_) => return JNI_FALSE,
    };
    if set_cluster_secret_impl(&secret_str) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeUpdateTelemetry<
    'local,
>(
    _env: EnvUnowned<'local>,
    _class: JClass<'local>,
    battery_pct: jint,
    is_charging: jboolean,
    thermal_throttled: jboolean,
) -> jboolean {
    if update_telemetry_impl(battery_pct, is_charging, thermal_throttled) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeUpdateTelemetryDetailed<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    battery_pct: jint,
    is_charging: jboolean,
    thermal_throttled: jboolean,
    battery_temperature: jfloat,
    network_type: JString<'local>,
) -> jboolean {
    let net_opt: Option<String> = match jstring_to_string(&mut env, &network_type) {
        Ok(s) => {
            let val: String = s;
            if val.trim().is_empty() {
                None
            } else {
                Some(val)
            }
        }
        Err(_) => None,
    };
    let temp_opt = if battery_temperature < -40.0 {
        None
    } else {
        Some(battery_temperature)
    };

    if update_telemetry_detailed_impl(
        battery_pct,
        is_charging,
        thermal_throttled,
        temp_opt,
        net_opt,
    ) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeStopWorker<
    'local,
>(
    _env: EnvUnowned<'local>,
    _class: JClass<'local>,
) -> jboolean {
    if stop_worker_impl() {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeIsRunning<
    'local,
>(
    _env: EnvUnowned<'local>,
    _class: JClass<'local>,
) -> jboolean {
    if is_running_impl() {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeGetWorkerStatus<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
) -> jstring {
    let status = get_worker_status_impl();
    string_to_jstring(&mut env, &status)
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeDiscoverMaster<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    timeout_ms: jlong,
) -> jstring {
    let status = discover_master_impl(timeout_ms);
    string_to_jstring(&mut env, &status)
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeTriggerFastEvacuation<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    reason: JString<'local>,
) -> jboolean {
    let reason_str: String = match jstring_to_string(&mut env, &reason) {
        Ok(s) => s,
        Err(_) => "PRE_SLEEP_EVACUATION".to_string(),
    };
    if trigger_fast_evacuation_impl(&reason_str) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

// ==============================================================================
// Unit Tests
// ==============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn set_start_claim_hook_for_testing(hook: impl FnOnce() + Send + 'static) {
        *lock_unpoisoned(&START_CLAIM_HOOK) = Some(Box::new(hook));
    }

    /// Design 0021: when the app names no sandbox directory, the bridge's check creates the
    /// default private root with mode 700 itself, so the worker's own check accepts it. Without
    /// `ensure_private_root_blocking`, `create_dir_all` would leave the umask mode (755 under the
    /// usual 022) and the worker would refuse the root for good.
    #[cfg(unix)]
    #[test]
    fn test_bridge_check_creates_the_default_private_root_with_mode_700() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let sandbox = SandboxConfig::default_under(temp.path());
        check_sandbox_usable(&sandbox).expect("the bridge check passes");
        let root = sandbox.base_dir.parent().unwrap();
        let mode = std::fs::symlink_metadata(root)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(mode, 0o700, "root {} has mode {mode:o}", root.display());
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(sandbox.ensure_usable())
            .expect("the worker's own check accepts what the bridge created");
    }

    static TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn wait_until_not_stopping() {
        let ws = get_worker_state();
        for _ in 0..200 {
            if !ws.stopping.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn reset_test_state() {
        *lock_unpoisoned(&START_CLAIM_HOOK) = None;
        stop_worker_impl();
        wait_until_not_stopping();
        reset_worker_thread_counts_for_testing();
        set_p2p_minimal_preset_for_testing(false);
        let ws = get_worker_state();
        *lock_unpoisoned(&ws.telemetry) = MobileTelemetry::default();
        ws.mobile.set(None);
        *lock_unpoisoned(&ws.worker_id) = None;
        *lock_unpoisoned(&ws.p2p_ticket) = None;
        *lock_unpoisoned(&ws.cluster_secret) = None;
        *lock_unpoisoned(&ws.sandbox_dir) = None;
        *lock_unpoisoned(&ws.evacuation_reason) = None;
        *lock_unpoisoned(&ws.status) = "STOPPED".to_string();
        ws.running.store(false, Ordering::SeqCst);
        ws.stopping.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_jni_telemetry_reaches_the_handle_shared_with_the_worker() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        let state = get_worker_state();
        assert!(
            state.mobile.live_status().is_none(),
            "nothing known before the app reports"
        );

        assert!(update_telemetry_detailed_impl(
            23,
            false,
            true,
            Some(41.5),
            None
        ));
        let live = state
            .mobile
            .live_status()
            .expect("the report must reach the worker's handle");
        assert_eq!(live.battery_pct, Some(23));
        assert_eq!(live.is_charging, Some(false));
        assert!(live.thermal_throttled);

        // Later reports replace it; an out-of-range percentage becomes "unknown", not a wrong number.
        assert!(update_telemetry_impl(150, true, false));
        let live = state.mobile.live_status().unwrap();
        assert_eq!(live.battery_pct, None);
        assert_eq!(live.is_charging, Some(true));
        assert!(!live.thermal_throttled);
        reset_test_state();
    }

    /// A fresh, empty directory under the platform temp dir (removed again by the caller).
    fn scratch_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("oxide-bridge-{label}-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn status_text() -> String {
        let status: serde_json::Value = serde_json::from_str(&get_worker_status_impl()).unwrap();
        status["status"].as_str().unwrap().to_string()
    }

    #[test]
    fn test_worker_start_refuses_an_unwritable_sandbox_dir_and_says_why() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        // What an app sees when nobody handed it a writable directory: the base cannot be created.
        let dir = scratch_dir("blocked");
        let blocker = dir.join("not-a-directory");
        std::fs::write(&blocker, b"x").unwrap();
        assert!(set_sandbox_dir_impl(
            blocker.join("sandboxes").display().to_string()
        ));

        assert!(
            !start_worker_impl(
                "127.0.0.1:59990".into(),
                None,
                "NoSandbox".into(),
                2,
                1024,
                false,
                5
            ),
            "a worker that cannot create task sandboxes must not pretend to have started"
        );
        assert!(!is_running_impl());
        let status = status_text();
        assert!(status.starts_with("ERROR: "), "{status}");
        assert!(status.contains("sandbox base directory"), "{status}");
        assert!(status.contains("not-a-directory"), "{status}");
        reset_test_state();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_worker_start_uses_the_sandbox_dir_the_app_handed_over() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        let dir = scratch_dir("handed-over");
        let sandboxes = dir.join("cache").join("sandboxes");
        assert!(set_sandbox_dir_impl(sandboxes.display().to_string()));

        assert!(start_worker_impl(
            "127.0.0.1:59991".into(),
            None,
            "WithSandbox".into(),
            2,
            1024,
            false,
            5
        ));
        assert!(
            sandboxes.is_dir(),
            "the worker must have prepared the directory it was given"
        );
        assert!(stop_worker_impl());
        reset_test_state();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_worker_that_fails_after_starting_reports_the_reason_in_its_status() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        set_p2p_minimal_preset_for_testing(true);
        let dir = scratch_dir("bad-ticket");
        assert!(set_sandbox_dir_impl(
            dir.join("sandboxes").display().to_string()
        ));

        // `run()` refuses an unusable P2P ticket right away, on the worker's own thread.
        assert!(start_worker_impl(
            "127.0.0.1:59992".into(),
            Some("INVALID_P2P_TICKET_PAYLOAD_NOT_JSON".into()),
            "BadTicket".into(),
            2,
            1024,
            false,
            5,
        ));
        let mut status = status_text();
        for _ in 0..100 {
            if status.starts_with("ERROR: ") {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
            status = status_text();
        }
        assert!(status.starts_with("ERROR: "), "{status}");
        assert!(status.contains("P2P ticket"), "{status}");
        assert!(!is_running_impl());
        assert!(stop_worker_impl());
        reset_test_state();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_initial_worker_state() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        let state = get_worker_state();
        let status = get_worker_status_impl();
        assert!(status.contains("status"));
        let running = is_running_impl();
        assert!(!running);
        assert!(!state.running.load(Ordering::SeqCst));
        reset_test_state();
    }

    #[test]
    fn test_worker_lifecycle_stop() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        assert!(stop_worker_impl());
        assert!(!is_running_impl());
        reset_test_state();
    }

    #[test]
    fn test_telemetry_injection_and_retrieval() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        assert!(update_telemetry_impl(85, true, false));
        let telem = get_telemetry_impl();
        assert_eq!(telem.battery_pct, Some(85));
        assert_eq!(telem.is_charging, Some(true));
        assert!(!telem.thermal_throttled);

        // Update with thermal throttling
        assert!(update_telemetry_impl(42, false, true));
        let telem2 = get_telemetry_impl();
        assert_eq!(telem2.battery_pct, Some(42));
        assert_eq!(telem2.is_charging, Some(false));
        assert!(telem2.thermal_throttled);

        // Invalid percentage (e.g. -1 or 120) should map to None
        assert!(update_telemetry_impl(-1, true, false));
        let telem3 = get_telemetry_impl();
        assert_eq!(telem3.battery_pct, None);
        reset_test_state();
    }

    #[test]
    fn test_detailed_telemetry() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        assert!(update_telemetry_detailed_impl(
            80,
            true,
            false,
            Some(38.5),
            Some("WIFI".to_string())
        ));
        let telem = get_telemetry_impl();
        assert_eq!(telem.battery_pct, Some(80));
        assert_eq!(telem.battery_temperature, Some(38.5));
        assert_eq!(telem.battery_temp_c(), Some(38.5));
        assert_eq!(telem.is_charging, Some(true));
        assert!(!telem.thermal_throttled);
        assert_eq!(telem.network_type.as_deref(), Some("wifi"));

        // Boundary tests: NaN or extreme temperatures
        assert!(update_telemetry_detailed_impl(
            50,
            false,
            true,
            Some(f32::NAN),
            Some("  CELLULAR  ".to_string())
        ));
        let telem2 = get_telemetry_impl();
        assert_eq!(telem2.battery_temperature, None);
        assert_eq!(telem2.network_type.as_deref(), Some("cellular"));
        reset_test_state();
    }

    #[test]
    fn test_discover_master_impl() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        let res = discover_master_impl(50);
        assert!(res.contains("found"));
        reset_test_state();
    }

    #[test]
    fn test_bridge_cluster_secret_handling() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();

        // 1. Valid hex secret (64 hex characters / 32 bytes)
        let valid_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(set_cluster_secret_impl(valid_hex));
        {
            let state = get_worker_state();
            let sec = lock_unpoisoned(&state.cluster_secret);
            assert!(sec.is_some());
            assert_eq!(sec.as_ref().unwrap().to_hex(), valid_hex);
        }

        // 2. Clear secret with empty string
        assert!(set_cluster_secret_impl("   "));
        {
            let state = get_worker_state();
            assert!(lock_unpoisoned(&state.cluster_secret).is_none());
        }

        // 3. Valid secret file
        let dir = scratch_dir("secret-file");
        let secret_path = dir.join("test.secret");
        std::fs::write(&secret_path, format!("{valid_hex}\n")).unwrap();
        assert!(set_cluster_secret_impl(&secret_path.display().to_string()));
        {
            let state = get_worker_state();
            let sec = lock_unpoisoned(&state.cluster_secret);
            assert!(sec.is_some());
            assert_eq!(sec.as_ref().unwrap().to_hex(), valid_hex);
        }

        // 4. Invalid secret (neither valid hex nor existing file)
        assert!(!set_cluster_secret_impl("not_a_valid_hex_or_file_xyz_123"));
        let status = status_text();
        assert!(
            status.starts_with("ERROR: Invalid cluster secret:"),
            "{status}"
        );

        reset_test_state();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_stop_worker_returns_in_under_100ms_while_worker_drains() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        let dir = scratch_dir("stop-nonblocking");
        assert!(set_sandbox_dir_impl(
            dir.join("sandboxes").display().to_string()
        ));

        let state = get_worker_state();
        let (drain_release_tx, drain_release_rx) = std::sync::mpsc::channel::<()>();
        let (worker_started_tx, worker_started_rx) = std::sync::mpsc::channel::<()>();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        *lock_unpoisoned(&state.shutdown_tx) = Some(shutdown_tx);
        *lock_unpoisoned(&state.status) = "RUNNING".to_string();
        state.running.store(true, Ordering::SeqCst);

        // Simulate a worker thread that takes 5 s to drain after receiving the shutdown signal
        // (matching `TASK_DRAIN_TIMEOUT` task cleanup on a running `WorkerClient`).
        let handle = std::thread::Builder::new()
            .name("OxideWorker-DrainSim".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let _ = worker_started_tx.send(());
                    let _ = shutdown_rx.changed().await;
                    let _ = drain_release_rx.recv_timeout(Duration::from_secs(5));
                });
            })
            .unwrap();
        *lock_unpoisoned(&state.thread_handle) = Some(handle);
        worker_started_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let start = std::time::Instant::now();
        assert!(stop_worker_impl());
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "stop_worker_impl took {elapsed:?} (>= 500 ms); must not block the calling thread"
        );
        assert!(!is_running_impl());
        assert_eq!(status_text(), "STOPPING");

        // Starting while the previous worker is still draining is refused without blocking.
        let start_while_stopping = std::time::Instant::now();
        assert!(
            !start_worker_impl(
                "127.0.0.1:59993".into(),
                None,
                "BlockedWhileStopping".into(),
                2,
                1024,
                false,
                5,
            ),
            "start_worker_impl must refuse to start while the previous worker is still stopping"
        );
        assert!(
            start_while_stopping.elapsed() < Duration::from_millis(500),
            "start refusal while stopping must not block"
        );
        assert_eq!(status_text(), "STOPPING");

        // Release the simulated drain and verify the join thread sets final status `STOPPED`.
        let _ = drain_release_tx.send(());
        wait_until_not_stopping();
        assert_eq!(status_text(), "STOPPED");

        reset_test_state();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_concurrent_start_calls_start_only_one_worker() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        let dir = scratch_dir("concurrent-start");
        assert!(set_sandbox_dir_impl(
            dir.join("sandboxes").display().to_string()
        ));

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted_clone = Arc::clone(&accepted);
        let accept_stop = Arc::new(AtomicBool::new(false));
        let accept_stop_clone = Arc::clone(&accept_stop);

        listener.set_nonblocking(true).unwrap();
        let accept_thread = std::thread::spawn(move || {
            let mut held_streams = Vec::new();
            while !accept_stop_clone.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        accepted_clone.fetch_add(1, Ordering::SeqCst);
                        // Hold the connection open during the window so the single worker does not
                        // immediately reconnect and increment the connection count a second time.
                        held_streams.push(stream);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut join_handles = Vec::new();
        for idx in 0..2 {
            let b = Arc::clone(&barrier);
            let master_addr = addr.clone();
            join_handles.push(std::thread::spawn(move || {
                b.wait();
                start_worker_impl(
                    master_addr,
                    None,
                    format!("ConcurrentWorker-{idx}"),
                    2,
                    1024,
                    false,
                    5,
                )
            }));
        }

        for h in join_handles {
            assert!(h.join().unwrap());
        }
        assert!(is_running_impl());

        // Wait for the started worker to connect to our listener.
        for _ in 0..100 {
            if accepted.load(Ordering::SeqCst) >= 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        // Give any hypothetical second worker thread time to connect as well.
        std::thread::sleep(Duration::from_millis(250));
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "Two concurrent start_worker_impl calls must spawn exactly one worker"
        );

        accept_stop.store(true, Ordering::SeqCst);
        let _ = accept_thread.join();
        assert!(stop_worker_impl());
        wait_until_not_stopping();
        assert_eq!(status_text(), "STOPPED");

        reset_test_state();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_stop_then_start_again_works() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        let dir = scratch_dir("stop-then-start");
        assert!(set_sandbox_dir_impl(
            dir.join("sandboxes").display().to_string()
        ));

        assert!(start_worker_impl(
            "127.0.0.1:59994".into(),
            None,
            "FirstRun".into(),
            2,
            1024,
            false,
            5,
        ));
        assert!(is_running_impl());
        let first_id: serde_json::Value = serde_json::from_str(&get_worker_status_impl()).unwrap();
        let first_worker_id = first_id["worker_id"].as_str().unwrap().to_string();
        assert!(!first_worker_id.is_empty());

        let stop_start = std::time::Instant::now();
        assert!(stop_worker_impl());
        assert!(
            stop_start.elapsed() < Duration::from_millis(100),
            "stop_worker_impl must return in under 100 ms"
        );
        assert!(!is_running_impl());

        assert!(start_worker_impl(
            "127.0.0.1:59994".into(),
            None,
            "SecondRun".into(),
            2,
            1024,
            false,
            5,
        ));
        assert!(is_running_impl());
        let second_id: serde_json::Value = serde_json::from_str(&get_worker_status_impl()).unwrap();
        let second_worker_id = second_id["worker_id"].as_str().unwrap().to_string();
        assert!(!second_worker_id.is_empty());
        assert_ne!(first_worker_id, second_worker_id);

        assert!(stop_worker_impl());
        wait_until_not_stopping();
        assert_eq!(status_text(), "STOPPED");

        reset_test_state();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_interleaved_stop_start_at_most_one_worker_thread() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        let dir = scratch_dir("interleave-stop-start");
        assert!(set_sandbox_dir_impl(
            dir.join("sandboxes").display().to_string()
        ));

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let barrier2 = Arc::clone(&barrier);

        let start_time = std::time::Instant::now();

        let h1 = std::thread::spawn(move || {
            for i in 0..200 {
                barrier.wait();
                if i % 2 == 0 {
                    start_worker_impl(
                        "127.0.0.1:59995".to_string(),
                        None,
                        format!("Interleave-T1-{i}"),
                        1,
                        512,
                        false,
                        5,
                    );
                } else {
                    stop_worker_impl();
                }
                assert!(
                    live_worker_threads_for_testing() <= 1,
                    "More than 1 worker thread detected in thread 1 at iteration {i}"
                );
            }
        });

        let h2 = std::thread::spawn(move || {
            for i in 0..200 {
                barrier2.wait();
                if i % 2 == 0 {
                    stop_worker_impl();
                } else {
                    start_worker_impl(
                        "127.0.0.1:59995".to_string(),
                        None,
                        format!("Interleave-T2-{i}"),
                        1,
                        512,
                        false,
                        5,
                    );
                }
                assert!(
                    live_worker_threads_for_testing() <= 1,
                    "More than 1 worker thread detected in thread 2 at iteration {i}"
                );
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();

        let elapsed = start_time.elapsed();
        assert!(
            elapsed < Duration::from_secs(10),
            "Test exceeded 10s: took {elapsed:?}"
        );

        assert!(stop_worker_impl());
        wait_until_not_stopping();

        assert_eq!(
            live_worker_threads_for_testing(),
            0,
            "Expected 0 live worker threads after stop"
        );
        assert!(
            max_live_worker_threads_for_testing() <= 1,
            "Max live worker threads exceeded 1: {}",
            max_live_worker_threads_for_testing()
        );

        reset_test_state();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cancelled_start_does_not_clobber_stop_status() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        set_p2p_minimal_preset_for_testing(true);
        let dir = scratch_dir("cancelled-start-aba");
        assert!(set_sandbox_dir_impl(
            dir.join("sandboxes").display().to_string()
        ));

        let (claimed_tx, claimed_rx) = std::sync::mpsc::channel::<()>();
        set_start_claim_hook_for_testing(move || {
            let _ = claimed_tx.send(());
            std::thread::sleep(Duration::from_millis(40));
        });

        // Start1 enters `start_worker_impl`, claims `running`, and fires the hook under the
        // lifecycle lock.
        let h_start1 = std::thread::spawn(|| {
            start_worker_impl(
                "127.0.0.1:59996".to_string(),
                Some("ticket-start1".to_string()),
                "Start1".to_string(),
                1,
                512,
                false,
                5,
            )
        });

        claimed_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // While Start1 is inside the hook (holding the lifecycle lock), issue Stop followed by
        // Start2: because the lifecycle lock serializes all transitions, Stop waits for Start1 to
        // finish spawning, stops Start1's worker cleanly, and Start2 then starts a single replacement
        // worker without Start1 clobbering Start2's worker_id, ticket, or status.
        let h_stop_and_start2 = std::thread::spawn(|| {
            assert!(stop_worker_impl());
            wait_until_not_stopping();
            assert_eq!(status_text(), "STOPPED");
            let started = start_worker_impl(
                "127.0.0.1:59996".to_string(),
                Some("ticket-start2".to_string()),
                "Start2".to_string(),
                1,
                512,
                false,
                5,
            );
            assert!(started);
            let st: serde_json::Value = serde_json::from_str(&get_worker_status_impl()).unwrap();
            st["worker_id"].as_str().unwrap().to_string()
        });

        assert!(h_start1.join().unwrap());
        let start2_worker_id = h_stop_and_start2.join().unwrap();

        // Wait for Start2's worker thread to enter its runtime so status advances past `STARTING`.
        for _ in 0..100 {
            if status_text() != "STARTING" {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        // Start2's worker may already have exited because its ticket is not parsable,
        // so check "never two" rather than "exactly one".
        assert!(
            live_worker_threads_for_testing() <= 1,
            "At most one worker thread may be live after Start1 -> Stop -> Start2"
        );
        assert!(
            max_live_worker_threads_for_testing() <= 1,
            "At most one worker thread may be live at any point (max={})",
            max_live_worker_threads_for_testing()
        );
        let final_json: serde_json::Value =
            serde_json::from_str(&get_worker_status_impl()).unwrap();
        assert_eq!(
            final_json["worker_id"].as_str().unwrap(),
            start2_worker_id,
            "Start2's worker_id must be preserved"
        );
        assert_eq!(
            final_json["p2p_ticket"].as_str().unwrap(),
            "ticket-start2",
            "Start2's p2p_ticket must not be overwritten by Start1"
        );
        let status = final_json["status"].as_str().unwrap();
        assert_ne!(
            status, "STARTING",
            "Status must not remain stuck at STARTING"
        );
        assert!(
            status == "RUNNING" || status.starts_with("ERROR: "),
            "Final status must be Start2's outcome (RUNNING or ERROR: ...), got: {status}"
        );

        assert!(stop_worker_impl());
        wait_until_not_stopping();
        assert_eq!(status_text(), "STOPPED");
        assert_eq!(live_worker_threads_for_testing(), 0);

        reset_test_state();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_self_exiting_worker_then_immediate_start_does_not_panic() {
        let _guard = lock_unpoisoned(&TEST_MUTEX);
        reset_test_state();
        set_p2p_minimal_preset_for_testing(true);
        let dir = scratch_dir("self-exiting-loop");
        assert!(set_sandbox_dir_impl(
            dir.join("sandboxes").display().to_string()
        ));

        for i in 0..50 {
            let started = start_worker_impl(
                "127.0.0.1:59997".to_string(),
                Some("INVALID_P2P_TICKET_PAYLOAD_NOT_JSON".to_string()),
                format!("SelfExit-{i}"),
                1,
                512,
                false,
                5,
            );
            assert!(started, "start_worker_impl must succeed on iteration {i}");
            // Wait until the worker self-exits (`running` becomes false after runtime and guard
            // drop), then immediately start the next iteration.
            for _ in 0..200 {
                if !is_running_impl() {
                    break;
                }
                assert!(
                    live_worker_threads_for_testing() <= 1,
                    "At most one worker thread may be live during iteration {i}"
                );
                std::thread::sleep(Duration::from_millis(2));
            }
            assert!(
                !is_running_impl(),
                "Self-exiting worker must clear running on iteration {i}"
            );
            assert!(
                live_worker_threads_for_testing() <= 1,
                "At most one live worker thread after self-exit on iteration {i}"
            );
        }

        assert!(
            max_live_worker_threads_for_testing() <= 1,
            "Max live worker threads exceeded 1 across 50 self-exit restarts: {}",
            max_live_worker_threads_for_testing()
        );
        assert_eq!(live_worker_threads_for_testing(), 0);

        reset_test_state();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
