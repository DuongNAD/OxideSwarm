//! Android JNI Bridge for OxideSwarm Worker (`liboxideworker.so`).
//!
//! Provides genuine JNI exported functions allowing Android applications
//! (`com.oxideswarm.worker.service.OxideWorkerBridge` / `com.oxideswarm.worker.runner.OxideWorkerBridge`)
//! to start, monitor, and stop the OxideSwarm distributed grid worker entirely in-process
//! on a native POSIX thread running a multi-threaded Tokio runtime.
//!
//! Running in-process produces 0 child processes, ensuring complete immunity
//! against the Android 12+ Phantom Process Killer (PPK) and W^X execution restrictions.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};
use uuid::Uuid;

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jint, jlong, jstring, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use rusty_grid_worker::{WorkerClient, WorkerConfig};

/// Global state representing the in-process worker instance.
struct WorkerState {
    running: AtomicBool,
    worker_id: Mutex<Option<Uuid>>,
    status: Mutex<String>,
    shutdown_tx: Mutex<Option<watch::Sender<bool>>>,
    thread_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl WorkerState {
    fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
            worker_id: Mutex::new(None),
            status: Mutex::new("STOPPED".to_string()),
            shutdown_tx: Mutex::new(None),
            thread_handle: Mutex::new(None),
        }
    }
}

static WORKER_STATE: OnceLock<Arc<WorkerState>> = OnceLock::new();

fn get_worker_state() -> Arc<WorkerState> {
    WORKER_STATE
        .get_or_init(|| Arc::new(WorkerState::new()))
        .clone()
}

/// Core implementation for starting the native worker client in a background Tokio runtime.
pub fn start_worker_impl(
    master_addr: String,
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
    if heartbeat_interval_secs > 0 {
        config.default_heartbeat_interval = Duration::from_secs(heartbeat_interval_secs as u64);
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_id = config.worker_id.unwrap_or_else(Uuid::new_v4);
    config = config.with_worker_id(worker_id);

    *state.worker_id.lock().unwrap() = Some(worker_id);
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
                if let Err(e) = client.run(shutdown_rx).await {
                    error!(worker_id = %worker_id, error = %e, "Worker supervisor exited with error");
                }
                *state_clone.status.lock().unwrap() = "STOPPED".to_string();
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

    serde_json::json!({
        "status": status,
        "running": is_running,
        "worker_id": worker_id_str,
    })
    .to_string()
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
        worker_name,
        cores,
        ram_mb,
        simulate_gpu,
        heartbeat_interval_secs,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_worker_state() {
        let state = get_worker_state();
        let status = get_worker_status_impl();
        assert!(status.contains("status"));
        let running = is_running_impl();
        assert!(!running);
        assert!(!state.running.load(Ordering::SeqCst));
    }

    #[test]
    fn test_worker_lifecycle_stop() {
        assert!(stop_worker_impl());
        assert!(!is_running_impl());
    }

    #[test]
    fn test_discover_master_impl() {
        let res = discover_master_impl(50);
        assert!(res.contains("found"));
    }
}
