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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};
use uuid::Uuid;

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jfloat, jint, jlong, jstring, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
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
    running: AtomicBool,
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
            running: AtomicBool::new(false),
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
    *get_worker_state().sandbox_dir.lock().unwrap() = (!dir.is_empty()).then(|| PathBuf::from(dir));
    true
}

/// Configures or clears the shared cluster secret for worker authentication against the Master node.
/// Accepts either a hex string (at least 32 hex chars / 16 bytes) or a file path to a cluster secret file.
/// Passing an empty string clears the secret.
pub fn set_cluster_secret_impl(secret_text: &str) -> bool {
    let state = get_worker_state();
    let trimmed = secret_text.trim();
    if trimmed.is_empty() {
        *state.cluster_secret.lock().unwrap() = None;
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
                    *state.status.lock().unwrap() = format!("ERROR: Invalid cluster secret: {e}");
                    return false;
                }
            }
        }
    };

    *state.cluster_secret.lock().unwrap() = Some(secret);
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
/// Returns `false`, with the reason in the status (`ERROR: ...`, see [`get_worker_status_impl`]),
/// when the worker cannot start at all: no writable sandbox directory, or the thread cannot be
/// spawned. A failure after that point (an invalid P2P ticket) is reported through the status.
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
    if state.running.load(Ordering::SeqCst) {
        info!("Worker is already running in-process");
        return true;
    }

    let mut config = WorkerConfig::new(&master_addr);
    if let Some(ticket) = p2p_ticket.as_ref().filter(|t| !t.trim().is_empty()) {
        config = config.with_p2p_ticket(ticket.clone());
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
    config = config.with_simulate_gpu(simulate_gpu);
    if let Some(dir) = state.sandbox_dir.lock().unwrap().clone() {
        config = config.with_sandbox_base_dir(dir);
    }
    if let Some(sec) = state.cluster_secret.lock().unwrap().clone() {
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
        *state.status.lock().unwrap() = format!("ERROR: {e}");
        return false;
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_id = config.worker_id.unwrap_or_else(Uuid::new_v4);
    config = config.with_worker_id(worker_id);

    *state.worker_id.lock().unwrap() = Some(worker_id);
    *state.p2p_ticket.lock().unwrap() = p2p_ticket;
    *state.shutdown_tx.lock().unwrap() = Some(shutdown_tx);
    *state.status.lock().unwrap() = "STARTING".to_string();
    state.running.store(true, Ordering::SeqCst);

    let state_clone = Arc::clone(&state);
    let handle = std::thread::Builder::new()
        .name("OxideWorker-Tokio".to_string())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    error!("Failed to initialize Tokio runtime for Android worker: {}", e);
                    state_clone.running.store(false, Ordering::SeqCst);
                    *state_clone.status.lock().unwrap() = format!("ERROR: {}", e);
                    return;
                }
            };

            rt.block_on(async {
                *state_clone.status.lock().unwrap() = "RUNNING".to_string();
                let mut client = WorkerClient::new(config);
                info!(worker_id = %worker_id, "In-process Android worker starting run loop");
                let outcome = client.run(shutdown_rx).await;
                *state_clone.status.lock().unwrap() = match outcome {
                    Ok(()) => "STOPPED".to_string(),
                    Err(e) => {
                        error!(worker_id = %worker_id, error = %e, "Worker supervisor exited with error");
                        format!("ERROR: {e}")
                    }
                };
                state_clone.running.store(false, Ordering::SeqCst);
            });
        });

    match handle {
        Ok(h) => {
            *state.thread_handle.lock().unwrap() = Some(h);
            true
        }
        Err(e) => {
            error!("Failed to spawn native worker thread: {}", e);
            state.running.store(false, Ordering::SeqCst);
            *state.status.lock().unwrap() = format!("SPAWN_ERROR: {}", e);
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

/// Core implementation for stopping the native worker client gracefully.
pub fn stop_worker_impl() -> bool {
    let state = get_worker_state();
    if !state.running.load(Ordering::SeqCst) {
        return true;
    }

    if let Some(tx) = state.shutdown_tx.lock().unwrap().take() {
        let _ = tx.send(true);
    }
    state.running.store(false, Ordering::SeqCst);
    *state.status.lock().unwrap() = "STOPPED".to_string();

    if let Some(h) = state.thread_handle.lock().unwrap().take() {
        let _ = h.join();
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
    let status = state.status.lock().unwrap().clone();
    let worker_id_str = state
        .worker_id
        .lock()
        .unwrap()
        .map(|u| u.to_string())
        .unwrap_or_default();
    let is_running = state.running.load(Ordering::SeqCst);
    let p2p_ticket = state.p2p_ticket.lock().unwrap().clone();
    let telemetry = state.telemetry.lock().unwrap().clone();

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

    let mut telem = state.telemetry.lock().unwrap();
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
    *state.evacuation_reason.lock().unwrap() = Some(r);
    rusty_grid_worker::client::set_evacuation_reason_hook(get_evacuation_reason_impl);
    if let Some(tx) = state.shutdown_tx.lock().unwrap().as_ref() {
        let _ = tx.send(true);
    }
    true
}

/// Retrieves the active evacuation reason, if any.
pub fn get_evacuation_reason_impl() -> Option<String> {
    get_worker_state().evacuation_reason.lock().unwrap().clone()
}

/// Retrieves the current cached mobile telemetry.
pub fn get_telemetry_impl() -> MobileTelemetry {
    let state = get_worker_state();
    let telem = state.telemetry.lock().unwrap().clone();
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
// JNI Exported Functions: com.oxideswarm.worker.service.OxideWorkerBridge
// ==============================================================================

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeStartWorker<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    master_addr: JString<'local>,
    p2p_ticket: JString<'local>,
    worker_name: JString<'local>,
    cores: jint,
    ram_mb: jlong,
    simulate_gpu: jboolean,
    heartbeat_interval_secs: jlong,
) -> jboolean {
    let master: String = match env.get_string(&master_addr) {
        Ok(s) => s.into(),
        Err(_) => return JNI_FALSE,
    };
    let ticket_str: Option<String> = match env.get_string(&p2p_ticket) {
        Ok(s) => {
            let val: String = s.into();
            if val.trim().is_empty() {
                None
            } else {
                Some(val)
            }
        }
        Err(_) => None,
    };
    let name: String = match env.get_string(&worker_name) {
        Ok(s) => s.into(),
        Err(_) => String::new(),
    };
    if start_worker_impl(
        master,
        ticket_str,
        name,
        cores,
        ram_mb,
        simulate_gpu != 0,
        heartbeat_interval_secs,
    ) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

/// Backward-compatible 6-argument JNI export for legacy service callers without p2p_ticket.
#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeStartWorkerLegacy<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    master_addr: JString<'local>,
    worker_name: JString<'local>,
    cores: jint,
    ram_mb: jlong,
    simulate_gpu: jboolean,
    heartbeat_interval_secs: jlong,
) -> jboolean {
    let master: String = match env.get_string(&master_addr) {
        Ok(s) => s.into(),
        Err(_) => return JNI_FALSE,
    };
    let name: String = match env.get_string(&worker_name) {
        Ok(s) => s.into(),
        Err(_) => String::new(),
    };
    if start_worker_impl(
        master,
        None,
        name,
        cores,
        ram_mb,
        simulate_gpu != 0,
        heartbeat_interval_secs,
    ) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

/// Hands the app's writable sandbox directory to the worker; call it before `nativeStartWorker`.
#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeSetSandboxDir<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    sandbox_dir: JString<'local>,
) -> jboolean {
    let dir: String = match env.get_string(&sandbox_dir) {
        Ok(s) => s.into(),
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
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeSetClusterSecret<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    cluster_secret: JString<'local>,
) -> jboolean {
    let secret_str: String = match env.get_string(&cluster_secret) {
        Ok(s) => s.into(),
        Err(_) => return JNI_FALSE,
    };
    if set_cluster_secret_impl(&secret_str) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeUpdateTelemetry<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    battery_pct: jint,
    is_charging: jboolean,
    thermal_throttled: jboolean,
) -> jboolean {
    if update_telemetry_impl(battery_pct, is_charging != 0, thermal_throttled != 0) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeUpdateTelemetryDetailed<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    battery_pct: jint,
    is_charging: jboolean,
    thermal_throttled: jboolean,
    battery_temperature: jfloat,
    network_type: JString<'local>,
) -> jboolean {
    let net_opt: Option<String> = match env.get_string(&network_type) {
        Ok(s) => {
            let val: String = s.into();
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
        is_charging != 0,
        thermal_throttled != 0,
        temp_opt,
        net_opt,
    ) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeStopWorker<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    if stop_worker_impl() {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeIsRunning<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    if is_running_impl() {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeGetWorkerStatus<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let status = get_worker_status_impl();
    match env.new_string(status) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeDiscoverMaster<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    timeout_ms: jlong,
) -> jstring {
    let status = discover_master_impl(timeout_ms);
    match env.new_string(status) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeTriggerFastEvacuation<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    reason: JString<'local>,
) -> jboolean {
    let reason_str: String = match env.get_string(&reason) {
        Ok(s) => s.into(),
        Err(_) => "PRE_SLEEP_EVACUATION".to_string(),
    };
    if trigger_fast_evacuation_impl(&reason_str) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

// ==============================================================================
// JNI Exported Functions: com.oxideswarm.worker.runner.OxideWorkerBridge
// ==============================================================================

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeStartWorker<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    master_addr: JString<'local>,
    p2p_ticket: JString<'local>,
    worker_name: JString<'local>,
    cores: jint,
    ram_mb: jlong,
    simulate_gpu: jboolean,
    heartbeat_interval_secs: jlong,
) -> jboolean {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeStartWorker(
        env,
        class,
        master_addr,
        p2p_ticket,
        worker_name,
        cores,
        ram_mb,
        simulate_gpu,
        heartbeat_interval_secs,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeSetSandboxDir<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    sandbox_dir: JString<'local>,
) -> jboolean {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeSetSandboxDir(
        env,
        class,
        sandbox_dir,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeSetClusterSecret<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    cluster_secret: JString<'local>,
) -> jboolean {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeSetClusterSecret(
        env,
        class,
        cluster_secret,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeStartWorkerLegacy<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    master_addr: JString<'local>,
    worker_name: JString<'local>,
    cores: jint,
    ram_mb: jlong,
    simulate_gpu: jboolean,
    heartbeat_interval_secs: jlong,
) -> jboolean {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeStartWorkerLegacy(
        env,
        class,
        master_addr,
        worker_name,
        cores,
        ram_mb,
        simulate_gpu,
        heartbeat_interval_secs,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeUpdateTelemetry<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    battery_pct: jint,
    is_charging: jboolean,
    thermal_throttled: jboolean,
) -> jboolean {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeUpdateTelemetry(
        env,
        class,
        battery_pct,
        is_charging,
        thermal_throttled,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeUpdateTelemetryDetailed<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    battery_pct: jint,
    is_charging: jboolean,
    thermal_throttled: jboolean,
    battery_temperature: jfloat,
    network_type: JString<'local>,
) -> jboolean {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeUpdateTelemetryDetailed(
        env,
        class,
        battery_pct,
        is_charging,
        thermal_throttled,
        battery_temperature,
        network_type,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeStopWorker<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
) -> jboolean {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeStopWorker(env, class)
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeIsRunning<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
) -> jboolean {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeIsRunning(env, class)
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeGetWorkerStatus<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
) -> jstring {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeGetWorkerStatus(env, class)
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeDiscoverMaster<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    timeout_ms: jlong,
) -> jstring {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeDiscoverMaster(
        env, class, timeout_ms,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeTriggerFastEvacuation<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    reason: JString<'local>,
) -> jboolean {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeTriggerFastEvacuation(
        env, class, reason,
    )
}

// ==============================================================================
// Unit Tests
// ==============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn reset_test_state() {
        stop_worker_impl();
        let ws = get_worker_state();
        *ws.telemetry.lock().unwrap() = MobileTelemetry::default();
        ws.mobile.set(None);
        *ws.worker_id.lock().unwrap() = None;
        *ws.p2p_ticket.lock().unwrap() = None;
        *ws.cluster_secret.lock().unwrap() = None;
        *ws.sandbox_dir.lock().unwrap() = None;
        *ws.status.lock().unwrap() = "STOPPED".to_string();
        ws.running.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_jni_telemetry_reaches_the_handle_shared_with_the_worker() {
        let _guard = TEST_MUTEX.lock().unwrap();
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
        let _guard = TEST_MUTEX.lock().unwrap();
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
        let _guard = TEST_MUTEX.lock().unwrap();
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
        let _guard = TEST_MUTEX.lock().unwrap();
        reset_test_state();
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
        let _guard = TEST_MUTEX.lock().unwrap();
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
        let _guard = TEST_MUTEX.lock().unwrap();
        reset_test_state();
        assert!(stop_worker_impl());
        assert!(!is_running_impl());
        reset_test_state();
    }

    #[test]
    fn test_telemetry_injection_and_retrieval() {
        let _guard = TEST_MUTEX.lock().unwrap();
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
        let _guard = TEST_MUTEX.lock().unwrap();
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
        let _guard = TEST_MUTEX.lock().unwrap();
        reset_test_state();
        let res = discover_master_impl(50);
        assert!(res.contains("found"));
        reset_test_state();
    }

    #[test]
    fn test_bridge_cluster_secret_handling() {
        let _guard = TEST_MUTEX.lock().unwrap();
        reset_test_state();

        // 1. Valid hex secret (64 hex characters / 32 bytes)
        let valid_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(set_cluster_secret_impl(valid_hex));
        {
            let state = get_worker_state();
            let sec = state.cluster_secret.lock().unwrap();
            assert!(sec.is_some());
            assert_eq!(sec.as_ref().unwrap().to_hex(), valid_hex);
        }

        // 2. Clear secret with empty string
        assert!(set_cluster_secret_impl("   "));
        {
            let state = get_worker_state();
            assert!(state.cluster_secret.lock().unwrap().is_none());
        }

        // 3. Valid secret file
        let dir = scratch_dir("secret-file");
        let secret_path = dir.join("test.secret");
        std::fs::write(&secret_path, format!("{valid_hex}\n")).unwrap();
        assert!(set_cluster_secret_impl(&secret_path.display().to_string()));
        {
            let state = get_worker_state();
            let sec = state.cluster_secret.lock().unwrap();
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
}
