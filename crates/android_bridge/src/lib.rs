//! Android JNI Bridge for OxideSwarm Worker & Mobile Coordinator (`liboxideworker.so`).
//!
//! Provides genuine JNI exported functions allowing Android applications
//! (`com.oxideswarm.worker.service.OxideWorkerBridge` / `com.oxideswarm.worker.runner.OxideWorkerBridge`)
//! to start, monitor, and stop the OxideSwarm distributed grid worker and master coordinator
//! entirely in-process on native POSIX threads running multi-threaded Tokio runtimes.
//!
//! Running in-process produces 0 child processes, ensuring complete immunity
//! against the Android 12+ Phantom Process Killer (PPK) and W^X execution restrictions.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, warn};
use uuid::Uuid;

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jfloat, jint, jlong, jstring, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};
use rusty_grid_core::transport::{
    iroh, serialize_p2p_ticket, BiStream, GridStream, GRID_ALPN,
};
use rusty_grid_worker::{WorkerClient, WorkerConfig};

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
    status: Mutex<String>,
    telemetry: Mutex<MobileTelemetry>,
    shutdown_tx: Mutex<Option<watch::Sender<bool>>>,
    thread_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl WorkerState {
    fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
            worker_id: Mutex::new(None),
            p2p_ticket: Mutex::new(None),
            status: Mutex::new("STOPPED".to_string()),
            telemetry: Mutex::new(MobileTelemetry::default()),
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
/// Accepts an optional `p2p_ticket` for connecting across WAN/CGNAT via Iroh QUIC/DERP relays.
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
    if heartbeat_interval_secs > 0 {
        config.default_heartbeat_interval = Duration::from_secs(heartbeat_interval_secs as u64);
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
    telem.network_type = net;
    true
}

/// Updates the mobile telemetry state injected from the Android SDK.
/// Backward-compatible wrapper calling `update_telemetry_detailed_impl`.
pub fn update_telemetry_impl(battery_pct: i32, is_charging: bool, thermal_throttled: bool) -> bool {
    update_telemetry_detailed_impl(battery_pct, is_charging, thermal_throttled, None, None)
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
// Master Coordinator State & Lifecycle
// ==============================================================================

/// Global state representing the in-process mobile cluster coordinator instance.
struct MasterState {
    running: AtomicBool,
    bind_port: Mutex<u16>,
    dashboard_port: Mutex<u16>,
    p2p_enabled: AtomicBool,
    p2p_ticket: Mutex<Option<String>>,
    p2p_endpoint: Mutex<Option<iroh::Endpoint>>,
    status: Mutex<String>,
    connected_workers: Mutex<usize>,
    shutdown_tx: Mutex<Option<watch::Sender<bool>>>,
    thread_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl MasterState {
    fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
            bind_port: Mutex::new(0),
            dashboard_port: Mutex::new(0),
            p2p_enabled: AtomicBool::new(false),
            p2p_ticket: Mutex::new(None),
            p2p_endpoint: Mutex::new(None),
            status: Mutex::new("STOPPED".to_string()),
            connected_workers: Mutex::new(0),
            shutdown_tx: Mutex::new(None),
            thread_handle: Mutex::new(None),
        }
    }
}

static MASTER_STATE: OnceLock<Arc<MasterState>> = OnceLock::new();

fn get_master_state() -> Arc<MasterState> {
    MASTER_STATE
        .get_or_init(|| Arc::new(MasterState::new()))
        .clone()
}

/// Core implementation for starting the mobile cluster coordinator in a background Tokio runtime.
/// Returns the generated P2P ticket string on success.
pub fn start_master_impl(
    bind_port: i32,
    dashboard_port: i32,
    enable_p2p: bool,
) -> Result<String, String> {
    let state = get_master_state();
    if state.running.load(Ordering::SeqCst) {
        let ticket = state.p2p_ticket.lock().unwrap().clone().unwrap_or_default();
        return Ok(ticket);
    }

    // Reset coordinator worker count for new coordinator lifecycle
    *state.connected_workers.lock().unwrap() = 0;

    let bind_port_u16 = if bind_port > 0 { bind_port as u16 } else { 0 };
    let dashboard_port_u16 = if dashboard_port > 0 { dashboard_port as u16 } else { 0 };

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();

    let state_clone = Arc::clone(&state);
    let handle = std::thread::Builder::new()
        .name("OxideMaster-Tokio".to_string())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ready_tx.send(Err(format!("Tokio runtime init error: {e}")));
                    state_clone.running.store(false, Ordering::SeqCst);
                    return;
                }
            };

            rt.block_on(async {
                // 1. Bind primary TCP coordinator listener
                let bind_addr = format!("0.0.0.0:{}", bind_port_u16);
                let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        let _ = ready_tx.send(Err(format!("Failed to bind TCP coordinator on {bind_addr}: {e}")));
                        state_clone.running.store(false, Ordering::SeqCst);
                        return;
                    }
                };

                let actual_port = match listener.local_addr() {
                    Ok(addr) => addr.port(),
                    Err(e) => {
                        let _ = ready_tx.send(Err(format!("Failed to query bound local_addr: {e}")));
                        return;
                    }
                };
                *state_clone.bind_port.lock().unwrap() = actual_port;

                // 2. Optionally bind dashboard listener if requested
                let actual_dashboard_port = if dashboard_port_u16 > 0 {
                    let d_addr = format!("0.0.0.0:{}", dashboard_port_u16);
                    match tokio::net::TcpListener::bind(&d_addr).await {
                        Ok(dl) => {
                            let dport = dl.local_addr().map(|a| a.port()).unwrap_or(dashboard_port_u16);
                            let mut d_shutdown_rx = shutdown_rx.clone();
                            tokio::spawn(async move {
                                loop {
                                    tokio::select! {
                                        accept_res = dl.accept() => {
                                            if let Ok((mut stream, _)) = accept_res {
                                                tokio::spawn(async move {
                                                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                                    let mut buf = [0u8; 1024];
                                                    let _ = stream.read(&mut buf).await;
                                                    let resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\"RUNNING\",\"role\":\"mobile_master\"}";
                                                    let _ = stream.write_all(resp.as_bytes()).await;
                                                });
                                            }
                                        }
                                        _ = d_shutdown_rx.changed() => break,
                                    }
                                }
                            });
                            dport
                        }
                        Err(e) => {
                            warn!("Failed to bind dashboard listener on {d_addr}: {e}");
                            0
                        }
                    }
                } else {
                    0
                };
                *state_clone.dashboard_port.lock().unwrap() = actual_dashboard_port;

                // 3. Construct P2P Endpoint and Ticket, or LAN connection string
                let ticket = if enable_p2p {
                    match iroh::Endpoint::builder(iroh::endpoint::presets::N0)
                        .alpns(vec![GRID_ALPN.to_vec()])
                        .bind()
                        .await
                    {
                        Ok(ep) => {
                            // Await home DERP relay connection and STUN discovery with short timeout
                            let _ = tokio::time::timeout(Duration::from_millis(300), ep.online()).await;
                            let mut addr = ep.addr();
                            // Ensure home relay is present for stable WAN/test compatibility even when offline
                            if !addr.addrs.iter().any(|a| a.is_relay()) {
                                if let Ok(relay_url) = "https://relay.n0.iroh.link./".parse::<iroh::RelayUrl>() {
                                    addr = addr.with_relay_url(relay_url);
                                }
                            }
                            match serialize_p2p_ticket(&addr) {
                                Ok(t) => {
                                    *state_clone.p2p_endpoint.lock().unwrap() = Some(ep.clone());

                                    // Spawn dedicated P2P accept loop
                                    let p2p_ep = ep;
                                    let p2p_state = Arc::clone(&state_clone);
                                    let mut p2p_shutdown_rx = shutdown_rx.clone();
                                    tokio::spawn(async move {
                                        loop {
                                            tokio::select! {
                                                incoming = p2p_ep.accept() => {
                                                    if let Some(incoming) = incoming {
                                                        let conn_state = Arc::clone(&p2p_state);
                                                        let conn_shutdown = p2p_shutdown_rx.clone();
                                                        tokio::spawn(async move {
                                                            if let Ok(conn) = incoming.await {
                                                                if let Ok((send, mut recv)) = conn.accept_bi().await {
                                                                    let mut tag = [0u8; 1];
                                                                    let p2p_to = Duration::from_millis(500);
                                                                    let is_m2_multiplexed = match tokio::time::timeout(
                                                                        p2p_to,
                                                                        tokio::io::AsyncReadExt::read_exact(&mut recv, &mut tag),
                                                                    ).await {
                                                                        Ok(Ok(_)) => tag[0] == rusty_grid_core::transport::STREAM_CONTROL || tag[0] == rusty_grid_core::transport::STREAM_DATA,
                                                                        _ => false,
                                                                    };

                                                                    if is_m2_multiplexed {
                                                                        let (ctrl_send, ctrl_recv) = if tag[0] == rusty_grid_core::transport::STREAM_CONTROL {
                                                                            let conn_clone = conn.clone();
                                                                            tokio::spawn(async move {
                                                                                if let Ok(Ok((_s2, mut r2))) = tokio::time::timeout(p2p_to, conn_clone.accept_bi()).await {
                                                                                    let mut t2 = [0u8; 1];
                                                                                    let _ = tokio::io::AsyncReadExt::read_exact(&mut r2, &mut t2).await;
                                                                                }
                                                                            });
                                                                            (send, recv)
                                                                        } else {
                                                                            if let Ok(Ok((s2, mut r2))) = tokio::time::timeout(p2p_to, conn.accept_bi()).await {
                                                                                let mut t2 = [0u8; 1];
                                                                                let _ = tokio::io::AsyncReadExt::read_exact(&mut r2, &mut t2).await;
                                                                                (s2, r2)
                                                                            } else {
                                                                                (send, recv)
                                                                            }
                                                                        };
                                                                        let grid_stream = GridStream::P2p(BiStream::new(ctrl_recv, ctrl_send));
                                                                        handle_coordinator_stream(grid_stream, conn_state, conn_shutdown).await;
                                                                    } else {
                                                                        let grid_stream = GridStream::P2p(BiStream::new(recv, send));
                                                                        handle_coordinator_stream(grid_stream, conn_state, conn_shutdown).await;
                                                                    }
                                                                }
                                                            }
                                                        });
                                                    } else {
                                                        break;
                                                    }
                                                }
                                                _ = p2p_shutdown_rx.changed() => break,
                                            }
                                        }
                                    });

                                    t
                                }
                                Err(e) => {
                                    let _ = ready_tx.send(Err(format!("Failed to serialize P2P ticket: {e}")));
                                    state_clone.running.store(false, Ordering::SeqCst);
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("Failed to bind iroh endpoint: {e}")));
                            state_clone.running.store(false, Ordering::SeqCst);
                            return;
                        }
                    }
                } else {
                    format!("127.0.0.1:{}", actual_port)
                };

                *state_clone.p2p_ticket.lock().unwrap() = Some(ticket.clone());
                state_clone.p2p_enabled.store(enable_p2p, Ordering::SeqCst);
                state_clone.running.store(true, Ordering::SeqCst);
                *state_clone.status.lock().unwrap() = "RUNNING".to_string();

                let _ = ready_tx.send(Ok(ticket));

                // 4. Coordinator TCP Accept Loop
                loop {
                    tokio::select! {
                        accept_res = listener.accept() => {
                            match accept_res {
                                Ok((stream, _peer_addr)) => {
                                    let grid_stream = GridStream::Tcp(stream);
                                    let state_for_conn = Arc::clone(&state_clone);
                                    let conn_shutdown_rx = shutdown_rx.clone();
                                    tokio::spawn(async move {
                                        handle_coordinator_stream(grid_stream, state_for_conn, conn_shutdown_rx).await;
                                    });
                                }
                                Err(e) => {
                                    error!("Coordinator accept error: {e}");
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            info!("Mobile coordinator received shutdown signal");
                            break;
                        }
                    }
                }

                // Cleanly close and release active Iroh P2P endpoint inside background Tokio runtime
                if let Some(ep) = state_clone.p2p_endpoint.lock().unwrap().take() {
                    ep.close().await;
                }

                *state_clone.status.lock().unwrap() = "STOPPED".to_string();
                state_clone.running.store(false, Ordering::SeqCst);
            });
        });

    match handle {
        Ok(h) => {
            *state.thread_handle.lock().unwrap() = Some(h);
            *state.shutdown_tx.lock().unwrap() = Some(shutdown_tx);
            match ready_rx.recv() {
                Ok(Ok(ticket)) => Ok(ticket),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(format!("Thread communication error: {e}")),
            }
        }
        Err(e) => {
            state.running.store(false, Ordering::SeqCst);
            Err(format!("Failed to spawn coordinator thread: {e}"))
        }
    }
}

/// Handles incoming connection stream (TCP or P2P QUIC) for the mobile coordinator.
async fn handle_coordinator_stream(
    stream: GridStream,
    state_for_conn: Arc<MasterState>,
    mut conn_shutdown_rx: watch::Receiver<bool>,
) {
    let mut transport = MessageTransport::new(stream);
    let mut registered_id: Option<Uuid> = None;

    loop {
        tokio::select! {
            msg_res = transport.recv_msg::<WorkerMessage>() => {
                match msg_res {
                    Ok(Some(msg)) => {
                        match msg {
                            WorkerMessage::Register { worker_id, .. } => {
                                // Idempotent registration guard on the same connection
                                if registered_id.is_none() {
                                    registered_id = Some(worker_id);
                                    *state_for_conn.connected_workers.lock().unwrap() += 1;
                                } else {
                                    registered_id = Some(worker_id);
                                }
                                let ack = MasterMessage::RegisterAck {
                                    accepted: true,
                                    worker_id,
                                    heartbeat_interval_secs: 3,
                                    message: Some("Registered with mobile coordinator".to_string()),
                                };
                                if transport.send_msg(&ack).await.is_err() {
                                    break;
                                }
                            }
                            WorkerMessage::Heartbeat { timestamp, .. } => {
                                let ack = MasterMessage::HeartbeatAck { timestamp };
                                if transport.send_msg(&ack).await.is_err() {
                                    break;
                                }
                            }
                            WorkerMessage::Disconnecting { .. } => {
                                // Zero-latency pre-sleep evacuation: take ownership of registered_id
                                // so post-loop cleanup does not perform a duplicate decrement
                                if registered_id.take().is_some() {
                                    let mut count = state_for_conn.connected_workers.lock().unwrap();
                                    if *count > 0 { *count -= 1; }
                                }
                                let ack = MasterMessage::Shutdown {
                                    reason: "Worker pre-sleep evacuation acknowledged".to_string(),
                                    grace_period_secs: Some(0),
                                };
                                let _ = transport.send_msg(&ack).await;
                                break;
                            }
                            _ => {}
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            _ = conn_shutdown_rx.changed() => break,
        }
    }

    if registered_id.take().is_some() {
        let mut count = state_for_conn.connected_workers.lock().unwrap();
        if *count > 0 { *count -= 1; }
    }
}

/// Core implementation for stopping the mobile cluster coordinator.
pub fn stop_master_impl() -> bool {
    let state = get_master_state();
    if !state.running.load(Ordering::SeqCst) {
        *state.connected_workers.lock().unwrap() = 0;
        return true;
    }

    if let Some(tx) = state.shutdown_tx.lock().unwrap().take() {
        let _ = tx.send(true);
    }
    state.running.store(false, Ordering::SeqCst);
    *state.status.lock().unwrap() = "STOPPED".to_string();

    // Ensure endpoint reference is cleared if not already taken
    let _ = state.p2p_endpoint.lock().unwrap().take();

    if let Some(h) = state.thread_handle.lock().unwrap().take() {
        let _ = h.join();
    }

    // Reset coordinator counters and metadata on shutdown
    *state.connected_workers.lock().unwrap() = 0;
    *state.p2p_ticket.lock().unwrap() = None;
    *state.bind_port.lock().unwrap() = 0;
    *state.dashboard_port.lock().unwrap() = 0;

    true
}

/// Helper query: returns true if the mobile master coordinator is actively running.
pub fn is_master_running_impl() -> bool {
    get_master_state().running.load(Ordering::SeqCst)
}

/// Helper query: returns the active Master P2P ticket if available.
pub fn get_master_ticket_impl() -> Option<String> {
    get_master_state().p2p_ticket.lock().unwrap().clone()
}

/// Helper query: returns detailed Master coordinator status as JSON.
pub fn get_master_status_impl() -> String {
    let state = get_master_state();
    serde_json::json!({
        "running": state.running.load(Ordering::SeqCst),
        "status": *state.status.lock().unwrap(),
        "bind_port": *state.bind_port.lock().unwrap(),
        "dashboard_port": *state.dashboard_port.lock().unwrap(),
        "p2p_enabled": state.p2p_enabled.load(Ordering::SeqCst),
        "connected_workers": *state.connected_workers.lock().unwrap(),
        "p2p_ticket": *state.p2p_ticket.lock().unwrap(),
    })
    .to_string()
}

/// Helper query: returns current connected worker count to the mobile coordinator.
pub fn get_master_worker_count_impl() -> usize {
    *get_master_state().connected_workers.lock().unwrap()
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
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeStartMaster<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    bind_port: jint,
    dashboard_port: jint,
    enable_p2p: jboolean,
) -> jstring {
    let result = match start_master_impl(bind_port, dashboard_port, enable_p2p != 0) {
        Ok(ticket) => ticket,
        Err(e) => format!("{{\"error\": \"{e}\"}}"),
    };
    match env.new_string(result) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeStopMaster<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    if stop_master_impl() {
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
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeStartMaster<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    bind_port: jint,
    dashboard_port: jint,
    enable_p2p: jboolean,
) -> jstring {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeStartMaster(
        env,
        class,
        bind_port,
        dashboard_port,
        enable_p2p,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeStopMaster<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
) -> jboolean {
    Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeStopMaster(env, class)
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

// ==============================================================================
// Unit Tests
// ==============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn reset_test_state() {
        stop_worker_impl();
        stop_master_impl();
        let ws = get_worker_state();
        *ws.telemetry.lock().unwrap() = MobileTelemetry::default();
        *ws.worker_id.lock().unwrap() = None;
        *ws.p2p_ticket.lock().unwrap() = None;
        *ws.status.lock().unwrap() = "STOPPED".to_string();
        ws.running.store(false, Ordering::SeqCst);

        let ms = get_master_state();
        *ms.connected_workers.lock().unwrap() = 0;
        *ms.p2p_ticket.lock().unwrap() = None;
        *ms.bind_port.lock().unwrap() = 0;
        *ms.dashboard_port.lock().unwrap() = 0;
        *ms.status.lock().unwrap() = "STOPPED".to_string();
        ms.running.store(false, Ordering::SeqCst);
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
    fn test_master_coordinator_lifecycle() {
        let _guard = TEST_MUTEX.lock().unwrap();
        reset_test_state();
        // Stop any running master before test
        stop_master_impl();

        let ticket = start_master_impl(0, 0, true).expect("Master should bind on dynamic port");
        assert!(!ticket.is_empty());
        assert!(is_master_running_impl());

        // Verify genuine P2P ticket parses as iroh::EndpointAddr
        let parsed = rusty_grid_core::transport::parse_p2p_ticket(&ticket);
        assert!(parsed.is_ok(), "Generated P2P ticket must parse into EndpointAddr: {:?}", parsed.err());

        let status_json = get_master_status_impl();
        assert!(status_json.contains("\"running\":true"));
        assert!(status_json.contains("\"status\":\"RUNNING\""));

        assert_eq!(get_master_worker_count_impl(), 0);

        // Clean stop
        assert!(stop_master_impl());
        assert!(!is_master_running_impl());
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
}
