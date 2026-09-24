use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Json,
    },
    routing::{get, post},
    Router,
};
use futures::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, warn};

use crate::auth::{rbac_auth_middleware, AuthConfig};
use crate::dashboard::dto::{
    ClusterSnapshotDto, ClusterStatusDto, DashboardStreamMessage, TaskQueryParams, WorkerSummaryDto,
};
use crate::queue::{TaskInfo, TaskQueue};
use crate::registry::{WorkerInfo, WorkerRegistry, WorkerStatus};
use rusty_grid_core::mode::{
    execute_mode_action, find_workspace_root, get_mode_descriptors, ModeDescriptor,
    ModeExecutionResult, WorkflowMode,
};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModesRunState {
    pub current_mode: Option<String>,
    pub current_action: Option<String>,
    pub is_running: bool,
    pub status: String,
    pub logs: Vec<String>,
    pub last_result: Option<ModeExecutionResult>,
}

#[derive(Clone)]
pub struct WebUiState {
    pub registry: WorkerRegistry,
    pub queue: TaskQueue,
    pub scheduler_notify: Arc<tokio::sync::Notify>,
    pub modes_state: Arc<tokio::sync::Mutex<ModesRunState>>,
    pub master_handle: Option<crate::server::MasterHandle>,
    pub p2p_ticket: Option<String>,
    pub broadcast_tx: broadcast::Sender<DashboardStreamMessage>,
    pub server_addr: SocketAddr,
    pub dashboard_addr: Option<SocketAddr>,
    pub started_at: Instant,
    pub auth_config: Arc<AuthConfig>,
}

impl WebUiState {
    /// Assembles a complete, ground-truth cluster snapshot.
    pub async fn build_snapshot(&self) -> ClusterSnapshotDto {
        let workers = self.registry.list_all_workers().await;
        let tasks = self.queue.list_tasks().await;
        let stats = self.queue.stats().await;

        let mut connected = 0;
        let mut busy = 0;
        let mut disconnected = 0;

        for w in &workers {
            match w.status {
                WorkerStatus::Connected => connected += 1,
                WorkerStatus::Busy => busy += 1,
                WorkerStatus::Disconnected => disconnected += 1,
            }
        }

        let status_dto = ClusterStatusDto {
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_secs: self.started_at.elapsed().as_secs(),
            master_addr: self.server_addr.to_string(),
            dashboard_addr: self.dashboard_addr.map(|a| a.to_string()),
            workers: WorkerSummaryDto {
                total: workers.len(),
                connected,
                busy,
                disconnected,
            },
            tasks: stats,
            p2p_ticket: self.p2p_ticket.clone(),
        };

        ClusterSnapshotDto {
            timestamp_utc: chrono::Utc::now().timestamp() as u64,
            uptime_secs: self.started_at.elapsed().as_secs(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            status: status_dto,
            workers,
            tasks,
        }
    }
}

/// Creates a fresh `WebUiState` instance with initialized run state.
pub fn create_web_ui_state(
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    master_handle: Option<crate::server::MasterHandle>,
    p2p_ticket: Option<String>,
) -> WebUiState {
    let (broadcast_tx, _) = broadcast::channel(256);
    let server_addr = master_handle
        .as_ref()
        .map(|h| h.server_addr())
        .unwrap_or_else(|| "127.0.0.1:8080".parse().unwrap());
    let dashboard_addr = master_handle.as_ref().and_then(|h| h.dashboard_addr());
    let modes_state = Arc::new(tokio::sync::Mutex::new(ModesRunState {
        current_mode: None,
        current_action: None,
        is_running: false,
        status: "idle".to_string(),
        logs: Vec::new(),
        last_result: None,
    }));
    WebUiState {
        registry,
        queue,
        scheduler_notify,
        modes_state,
        master_handle,
        p2p_ticket,
        broadcast_tx,
        server_addr,
        dashboard_addr,
        started_at: Instant::now(),
        auth_config: Arc::new(AuthConfig::default()),
    }
}

/// Backward-compatible 5-argument helper creating the full Web UI router.
pub fn create_web_ui_router(
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    master_handle: Option<crate::server::MasterHandle>,
    p2p_ticket: Option<String>,
) -> Router {
    let (tx, _) = broadcast::channel(256);
    let server_addr = master_handle
        .as_ref()
        .map(|h| h.server_addr())
        .unwrap_or_else(|| "127.0.0.1:8080".parse().unwrap());
    let dashboard_addr = master_handle.as_ref().and_then(|h| h.dashboard_addr());
    let auth_config = Arc::new(AuthConfig::default());

    create_web_ui_router_full(
        registry,
        queue,
        scheduler_notify,
        master_handle,
        p2p_ticket,
        tx,
        server_addr,
        dashboard_addr,
        Instant::now(),
        auth_config,
    )
}

/// Full constructor for creating the unified Web UI router with streaming and auth.
#[allow(clippy::too_many_arguments)]
pub fn create_web_ui_router_full(
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    master_handle: Option<crate::server::MasterHandle>,
    p2p_ticket: Option<String>,
    broadcast_tx: broadcast::Sender<DashboardStreamMessage>,
    server_addr: SocketAddr,
    dashboard_addr: Option<SocketAddr>,
    started_at: Instant,
    auth_config: Arc<AuthConfig>,
) -> Router {
    let modes_state = Arc::new(tokio::sync::Mutex::new(ModesRunState {
        current_mode: None,
        current_action: None,
        is_running: false,
        status: "idle".to_string(),
        logs: Vec::new(),
        last_result: None,
    }));

    let state = WebUiState {
        registry,
        queue,
        scheduler_notify,
        modes_state,
        master_handle,
        p2p_ticket,
        broadcast_tx,
        server_addr,
        dashboard_addr,
        started_at,
        auth_config: Arc::clone(&auth_config),
    };

    Router::new()
        // HTML Cockpit Single Page Application
        .route("/", get(index_html))
        // Observability & Telemetry APIs
        .route("/api/status", get(api_status))
        .route("/api/workers", get(api_get_workers))
        .route("/api/tasks", get(api_get_tasks).post(api_submit_task))
        .route("/api/tasks/:task_id", get(api_get_task_standardized))
        // Real-Time Telemetry Streaming
        .route("/ws", get(ws_handler))
        .route("/api/stream", get(ws_handler))
        .route("/api/stream/sse", get(sse_handler))
        // Control & AI Endpoints
        .route("/api/chat", post(api_chat))
        .route("/api/modes", get(api_get_modes))
        .route("/api/modes/run", post(api_run_mode))
        .route("/api/modes/status", get(api_mode_status))
        // Middlewares
        .layer(CorsLayer::permissive())
        .layer(axum::middleware::from_fn_with_state(
            auth_config,
            rbac_auth_middleware,
        ))
        .with_state(state)
}

pub async fn start_web_ui(
    addr: SocketAddr,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    master_handle: Option<crate::server::MasterHandle>,
    p2p_ticket: Option<String>,
) {
    let app = create_web_ui_router(
        registry,
        queue,
        scheduler_notify,
        master_handle,
        p2p_ticket,
    );

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, "Failed to bind Web UI listener");
            return;
        }
    };

    let local_addr = listener.local_addr().unwrap_or(addr);
    info!(addr = %local_addr, "Web UI Dashboard listening");

    let server = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>());

    let shutdown_signal = async move {
        while shutdown_rx.changed().await.is_ok() {
            if *shutdown_rx.borrow() {
                break;
            }
        }
    };

    if let Err(e) = server.with_graceful_shutdown(shutdown_signal).await {
        error!(error = %e, "Web UI server error");
    }
}

static DASHBOARD_HTML: std::sync::OnceLock<String> = std::sync::OnceLock::new();

async fn index_html() -> Html<&'static str> {
    let html = DASHBOARD_HTML.get_or_init(|| {
        let raw = include_str!("dashboard.html");
        if !raw.contains("Cockpit") {
            raw.replacen("<title>OxideSwarm", "<title>OxideSwarm Cockpit -", 1)
        } else {
            raw.to_string()
        }
    });
    Html(html.as_str())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterUiInfo {
    pub host: String,
    pub role: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardStatus {
    pub master: MasterUiInfo,
    pub workers: Vec<WorkerUiInfo>,
    pub tasks: DashboardTasks,
    #[serde(default)]
    pub p2p_ticket: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerUiInfo {
    pub id: String,
    pub name: String,
    pub status: String,
    pub role: String,
    pub role_description: String,
    pub active_tasks: usize,
    pub cpu_cores: usize,
    pub ram_mb: u64,
    pub cpu_usage_pct: f32,
    pub ram_available_mb: u64,
    pub has_gpu: bool,
    pub gpu_device_name: Option<String>,
    pub battery_pct: Option<u8>,
    pub is_charging: Option<bool>,
    pub thermal_throttled: bool,
    pub last_heartbeat_secs_ago: u64,
    pub interconnect_type: String,
    pub is_relayed: bool,
    pub rtt_ms: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardTasks {
    pub total: usize,
    pub queued: usize,
    #[serde(default)]
    pub scheduled: usize,
    pub running: usize,
    #[serde(default)]
    pub retrying: usize,
    pub completed: usize,
    pub failed: usize,
    #[serde(default)]
    pub cancelled: usize,
    #[serde(default)]
    pub active_list: Vec<TaskUiInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskUiInfo {
    pub id: String,
    pub state: String,
    pub worker_id: Option<String>,
}

async fn api_status(State(state): State<WebUiState>) -> impl IntoResponse {
    let workers_info = state.registry.list_all_workers().await;
    let now_epoch = chrono::Utc::now().timestamp() as u64;

    // Find the worker with the maximum memory/CPU to designate as Orchestrator if available
    let max_cores = workers_info
        .iter()
        .map(|w| w.capabilities.cpu_cores)
        .max()
        .unwrap_or(0);

    let mut orchestrator_assigned = false;
    let mut workers = Vec::with_capacity(workers_info.len());
    for w in workers_info {
        let has_gpu = w.capabilities.has_gpu || w.capabilities.is_simulated_gpu;
        let gpu_device_name = w.capabilities.gpu_device_name.clone();

        let is_orchestrator = !orchestrator_assigned
            && (w.capabilities.name.to_lowercase().contains("mac")
                || (w.capabilities.cpu_cores == max_cores && w.capabilities.ram_mb >= 16384));

        let (role, role_description) = if is_orchestrator {
            orchestrator_assigned = true;
            ("ORCH".to_string(), "Coordination & Build".to_string())
        } else if has_gpu {
            ("GPU".to_string(), "GPU Acceleration".to_string())
        } else {
            ("WORKER".to_string(), "Compute Node".to_string())
        };

        let (battery_pct, is_charging, thermal_throttled) = match &w.capabilities.mobile {
            Some(m) => (m.battery_pct, m.is_charging, m.thermal_throttled),
            None => (None, None, false),
        };

        let last_heartbeat_secs_ago = if w.last_heartbeat_timestamp > 0 {
            now_epoch.saturating_sub(w.last_heartbeat_timestamp)
        } else {
            0
        };

        let (interconnect_type, is_relayed, rtt_ms) = {
            #[cfg(feature = "p2p")]
            {
                if let Some(ref handle) = state.master_handle {
                    if let Some(p2p_info) = handle.get_worker_p2p_info(&w.worker_id).await {
                        let conn_type = if p2p_info.is_relay {
                            "Relay (DERP)".to_string()
                        } else {
                            "Direct P2P (QUIC)".to_string()
                        };
                        let rtt = if p2p_info.rtt_ms.is_finite() && p2p_info.rtt_ms >= 0.0 {
                            Some(p2p_info.rtt_ms as f32)
                        } else {
                            None
                        };
                        (conn_type, p2p_info.is_relay, rtt)
                    } else {
                        parse_link_from_tags(&w.capabilities.tags)
                    }
                } else {
                    parse_link_from_tags(&w.capabilities.tags)
                }
            }
            #[cfg(not(feature = "p2p"))]
            {
                parse_link_from_tags(&w.capabilities.tags)
            }
        };

        workers.push(WorkerUiInfo {
            id: w.worker_id.to_string(),
            name: w.capabilities.name,
            status: format!("{:?}", w.status),
            role,
            role_description,
            active_tasks: w.active_tasks,
            cpu_cores: w.capabilities.cpu_cores,
            ram_mb: w.capabilities.ram_mb,
            cpu_usage_pct: w.cpu_usage_pct,
            ram_available_mb: w.ram_available_mb,
            has_gpu,
            gpu_device_name,
            battery_pct,
            is_charging,
            thermal_throttled,
            last_heartbeat_secs_ago,
            interconnect_type,
            is_relayed,
            rtt_ms,
        });
    }

    let host_name = sysinfo::System::host_name().unwrap_or_else(|| "OxideSwarm Host".to_string());
    let master = MasterUiInfo {
        host: format!("OxideSwarm ({})", host_name),
        role: "MASTER".to_string(),
        description: "P2P Coordinator".to_string(),
    };

    let stats = state.queue.stats().await;
    let all_tasks = state.queue.list_tasks().await;

    let active_list = all_tasks
        .into_iter()
        .filter(|t| !t.state.is_terminal())
        .map(|t| TaskUiInfo {
            id: t.task_id.to_string(),
            state: format!("{:?}", t.state),
            worker_id: t.assigned_worker_id.map(|u| u.to_string()),
        })
        .collect();

    let tasks = DashboardTasks {
        total: stats.total,
        queued: stats.queued,
        scheduled: stats.scheduled,
        running: stats.running,
        retrying: stats.retrying,
        completed: stats.completed,
        failed: stats.failed,
        cancelled: stats.cancelled,
        active_list,
    };

    let resp = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": state.started_at.elapsed().as_secs(),
        "master_addr": state.server_addr.to_string(),
        "dashboard_addr": state.dashboard_addr.map(|a| a.to_string()),
        "master": master,
        "workers": workers,
        "tasks": tasks,
        "p2p_ticket": state.p2p_ticket.clone(),
    });

    Json(resp)
}

async fn api_get_workers(State(state): State<WebUiState>) -> Json<Vec<WorkerInfo>> {
    Json(state.registry.list_all_workers().await)
}

async fn api_get_tasks(
    State(state): State<WebUiState>,
    Query(params): Query<TaskQueryParams>,
) -> Json<Vec<TaskInfo>> {
    let mut tasks = state.queue.list_tasks().await;

    if let Some(ref target_state) = params.state {
        let trimmed = target_state.trim();
        if !trimmed.is_empty() {
            tasks.retain(|t| {
                format!("{:?}", t.state).eq_ignore_ascii_case(trimmed)
                    || match serde_json::to_value(t.state) {
                        Ok(serde_json::Value::String(s)) => s.eq_ignore_ascii_case(trimmed),
                        _ => false,
                    }
            });
        }
    }

    if let Some(limit) = params.limit {
        tasks.truncate(limit);
    }

    Json(tasks)
}

fn parse_link_from_tags(tags: &[String]) -> (String, bool, Option<f32>) {
    for tag in tags {
        if let Some(rest) = tag.strip_prefix("link:") {
            if let Some((kind, rtt_str)) = rest.split_once(':') {
                let rtt = rtt_str.trim_end_matches("ms").parse::<f32>().ok();
                let is_relayed = kind.contains("Relay") || kind.contains("DERP");
                return (kind.to_string(), is_relayed, rtt);
            } else {
                let is_relayed = rest.contains("Relay") || rest.contains("DERP");
                return (rest.to_string(), is_relayed, None);
            }
        }
    }
    ("TCP/LAN".to_string(), false, None)
}

#[derive(Deserialize)]
struct SubmitTaskPayload {
    command: Option<String>,
}

#[derive(Serialize)]
struct SubmitTaskResponse {
    task_id: String,
    message: String,
}

async fn api_submit_task(
    State(state): State<WebUiState>,
    payload: Option<Json<SubmitTaskPayload>>,
) -> impl IntoResponse {
    let cmd = payload
        .and_then(|p| p.command.clone())
        .unwrap_or_else(|| "echo 'Hello from OxideSwarm Web UI'".to_string());

    let spec = TaskSpec::new_shell_script(cmd);
    let reqs = TaskRequirements {
        cpu_cores: 1,
        ram_mb: 0,
        gpu_required: false,
        timeout_secs: 60,
        max_retries: None,
    };
    let task = Task::new(spec, reqs);

    match state.queue.submit(task).await {
        Ok(id) => {
            state.scheduler_notify.notify_one();
            (
                StatusCode::OK,
                Json(SubmitTaskResponse {
                    task_id: id.to_string(),
                    message: format!("Task {} submitted successfully", id),
                }),
            )
                .into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn api_get_task_standardized(
    State(state): State<WebUiState>,
    Path(task_id_str): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let Ok(uuid) = uuid::Uuid::parse_str(&task_id_str) else {
        return Err(StatusCode::NOT_FOUND);
    };
    let task_id = TaskId(uuid);

    let Some(task_info) = state.queue.get_task(&task_id).await else {
        return Err(StatusCode::NOT_FOUND);
    };

    let result = state.queue.get_result(&task_id).await;

    let response = serde_json::json!({
        "task_id": task_info.task_id,
        "id": task_id_str,
        "state": task_info.state,
        "priority": task_info.priority,
        "gpu_required": task_info.gpu_required,
        "assigned_worker_id": task_info.assigned_worker_id,
        "worker_id": task_info.assigned_worker_id.map(|w| w.to_string()),
        "retry_count": task_info.retry_count,
        "submitted_at_utc": task_info.submitted_at_utc,
        "execution_time_ms": task_info.execution_time_ms,
        "exit_code": result.as_ref().map(|r| r.exit_code).or(task_info.exit_code),
        "stdout": result.as_ref().map(|r| r.stdout_str().to_string()).unwrap_or_default(),
        "stderr": result.as_ref().map(|r| r.stderr_str().to_string()).unwrap_or_default(),
        "error": task_info.error_message.clone(),
        "error_message": task_info.error_message,
    });

    Ok(Json(response))
}

// --- WebSocket Handlers ---

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<WebUiState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_client(socket, state))
}

async fn handle_ws_client(socket: WebSocket, state: WebUiState) {
    let (mut sender, mut receiver) = socket.split();
    let mut broadcast_rx = state.broadcast_tx.subscribe();

    // 1. Immediately send full point-in-time cluster snapshot as initial frame
    let snapshot = state.build_snapshot().await;
    let initial_msg = DashboardStreamMessage::Snapshot(snapshot);
    if let Ok(json) = serde_json::to_string(&initial_msg) {
        if sender.send(Message::Text(json)).await.is_err() {
            return;
        }
    }

    // 2. Periodic keepalive ping timer (20s)
    let mut ping_interval = tokio::time::interval(Duration::from_secs(20));
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // 3. Stream incremental updates until disconnect
    loop {
        tokio::select! {
            broadcast_msg = broadcast_rx.recv() => {
                match broadcast_msg {
                    Ok(event) => {
                        match serde_json::to_string(&event) {
                            Ok(json) => {
                                if sender.send(Message::Text(json)).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to serialize telemetry event");
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "Dashboard WebSocket client lagged; sending resync snapshot");
                        let fresh_snapshot = state.build_snapshot().await;
                        let msg = DashboardStreamMessage::Snapshot(fresh_snapshot);
                        if let Ok(json) = serde_json::to_string(&msg) {
                            if sender.send(Message::Text(json)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("Broadcast channel closed; terminating client WebSocket loop");
                        break;
                    }
                }
            }
            client_msg = receiver.next() => {
                match client_msg {
                    Some(Ok(Message::Ping(payload))) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                            if val.get("action").and_then(|a| a.as_str()) == Some("snapshot") {
                                let fresh_snapshot = state.build_snapshot().await;
                                let msg = DashboardStreamMessage::Snapshot(fresh_snapshot);
                                if let Ok(json) = serde_json::to_string(&msg) {
                                    let _ = sender.send(Message::Text(json)).await;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                if sender.send(Message::Ping(vec![])).await.is_err() {
                    debug!("Keepalive ping failed; client disconnected");
                    break;
                }
            }
        }
    }

    let _ = sender.close().await;
}

// --- Server-Sent Events (SSE) Handlers ---

async fn sse_handler(
    State(state): State<WebUiState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Event, Infallible>>(64);
    let state_clone = state.clone();

    tokio::spawn(async move {
        // 1. Initial Snapshot Event
        let snapshot = state_clone.build_snapshot().await;
        let snapshot_msg = DashboardStreamMessage::Snapshot(snapshot);
        if let Ok(json) = serde_json::to_string(&snapshot_msg) {
            if tx
                .send(Ok(Event::default().event("snapshot").data(json)))
                .await
                .is_err()
            {
                return;
            }
        }

        // 2. Incremental Event Stream
        let mut broadcast_rx = state_clone.broadcast_tx.subscribe();
        loop {
            match broadcast_rx.recv().await {
                Ok(msg) => {
                    let event_type = msg.event_name();
                    if let Ok(json) = serde_json::to_string(&msg) {
                        if tx
                            .send(Ok(Event::default().event(event_type).data(json)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "SSE client lagged; sending fresh snapshot");
                    let fresh_snapshot = state_clone.build_snapshot().await;
                    let msg = DashboardStreamMessage::Snapshot(fresh_snapshot);
                    if let Ok(json) = serde_json::to_string(&msg) {
                        if tx
                            .send(Ok(Event::default().event("snapshot").data(json)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    Sse::new(rx).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

#[derive(Deserialize)]
pub struct ChatRequest {
    pub message: String,
}

#[derive(Serialize)]
pub struct ChatResponse {
    pub reply: String,
    pub model: String,
    pub execution_time_ms: u64,
}

async fn api_chat(
    State(state): State<WebUiState>,
    Json(payload): Json<ChatRequest>,
) -> impl IntoResponse {
    let start_time = std::time::Instant::now();
    let message = payload.message.trim();
    if message.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ChatResponse {
                reply: "Tin nhắn trống.".to_string(),
                model: "none".to_string(),
                execution_time_ms: 0,
            }),
        );
    }

    // Gather live cluster information
    let workers = state.registry.list_all_workers().await;
    let mut cluster_context = String::new();
    cluster_context.push_str("=== OxideSwarm Live Cluster Telemetry ===\n");
    cluster_context.push_str(&format!("Tổng số node kết nối: {}\n", workers.len()));
    for (i, w) in workers.iter().enumerate() {
        let avail_ram = w.ram_available_mb;
        let bat = w
            .capabilities
            .mobile
            .as_ref()
            .and_then(|m| m.battery_pct)
            .map(|b| format!("{}%", b))
            .unwrap_or_else(|| "AC/Unknown".to_string());
        cluster_context.push_str(&format!(
            "{}. Node: {}\n   - Số nhân CPU vật lý: {} cores\n   - Tổng RAM vật lý: {} MB (~{:.1} GB, còn trống: {} MB)\n   - CPU Usage: {:.1}%\n   - Pin: {}\n   - GPU: {}\n",
            i + 1,
            w.capabilities.name,
            w.capabilities.cpu_cores,
            w.capabilities.ram_mb,
            (w.capabilities.ram_mb as f64) / 1024.0,
            avail_ram,
            w.cpu_usage_pct,
            bat,
            if w.capabilities.has_gpu || w.capabilities.is_simulated_gpu {
                w.capabilities.gpu_device_name.as_deref().unwrap_or("GPU Enabled")
            } else {
                "CPU-only"
            }
        ));
    }

    let full_prompt = format!(
        "Bạn là trợ lý AI thông minh quản lý cụm OxideSwarm (hệ thống tính toán phân tán P2P).\n\
        Dưới đây là thông số phần cứng thời gian thực của cụm hiện tại:\n\
        {}\n\
        Tin nhắn/Câu hỏi từ người dùng:\n\
        \"{}\"\n\n\
        YÊU CẦU ĐỊNH DẠNG & PHONG CÁCH TRẢ LỜI (HIỆN ĐẠI, DỄ HIỂU, KHÔNG SẾN SÚA):
        1. Trình bày trực quan, hiện đại, tối giản (phong cách Datadog / Vercel / Linear): dùng bảng tóm tắt ngắn gọn hoặc sơ đồ phân nhánh rõ ràng.
        2. Dùng các ký hiệu kỹ thuật tinh tế: [OK], [RUNNING], [P2P MESH], ● (dot xanh/xám), ▸ (mũi tên nhánh), tuyệt đối không dùng các emoji hoạt hình sến súa gây rối mắt.
        3. Cực kỳ ngắn gọn, súc tích, đi thẳng vào số liệu, giúp người dùng nhìn lướt qua trong 2 giây là nắm toàn bộ hiện trạng cụm.
        4. Nếu người dùng hỏi về lượng phần cứng trên điện thoại hay máy tính có thật không hay ảo: Khẳng định 100% PHẦN CỨNG VẬT LÝ THẬT đọc trực tiếp từ kernel (/proc/meminfo trên Android và sysctl trên macOS), hoàn toàn không có giả lập.",
        cluster_context, message
    );

    // Locate agy CLI binary dynamically
    let agy_bin = resolve_agy_binary();

    let cmd_result = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        tokio::process::Command::new(agy_bin)
            .arg("-p")
            .arg(&full_prompt)
            .output(),
    )
    .await;

    let elapsed = start_time.elapsed().as_millis() as u64;

    match cmd_result {
        Ok(Ok(output)) => {
            let reply = if output.status.success() {
                String::from_utf8_lossy(&output.stdout).trim().to_string()
            } else {
                let err = String::from_utf8_lossy(&output.stderr).trim().to_string();
                if err.is_empty() {
                    String::from_utf8_lossy(&output.stdout).trim().to_string()
                } else {
                    format!("Model Error: {}", err)
                }
            };
            (
                StatusCode::OK,
                Json(ChatResponse {
                    reply,
                    model: "Antigravity AI (Gemini)".to_string(),
                    execution_time_ms: elapsed,
                }),
            )
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ChatResponse {
                reply: format!("Lỗi khi kết nối với mô hình AI: {}", e),
                model: "System Error".to_string(),
                execution_time_ms: elapsed,
            }),
        ),
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(ChatResponse {
                reply: "Thời gian chờ phản hồi từ AI model quá lâu (timeout 60s).".to_string(),
                model: "Timeout".to_string(),
                execution_time_ms: elapsed,
            }),
        ),
    }
}

fn resolve_agy_binary() -> std::path::PathBuf {
    let mut candidates = Vec::new();

    // Check user home dir: $HOME/.local/bin or %USERPROFILE%\.local\bin
    if let Ok(home) = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")) {
        let home_path = std::path::PathBuf::from(home);
        candidates.push(home_path.join(".local").join("bin").join("agy"));
        #[cfg(windows)]
        {
            candidates.push(home_path.join(".local").join("bin").join("agy.exe"));
            candidates.push(home_path.join(".local").join("bin").join("agy.bat"));
            candidates.push(home_path.join(".local").join("bin").join("agy.cmd"));
        }
    }

    // Common unix paths
    candidates.push(std::path::PathBuf::from("/usr/local/bin/agy"));
    candidates.push(std::path::PathBuf::from("/usr/bin/agy"));

    // Check system PATH
    if let Some(paths) = std::env::var_os("PATH") {
        for p in std::env::split_paths(&paths) {
            candidates.push(p.join("agy"));
            #[cfg(windows)]
            {
                candidates.push(p.join("agy.exe"));
                candidates.push(p.join("agy.bat"));
                candidates.push(p.join("agy.cmd"));
            }
        }
    }

    for cand in candidates {
        if cand.is_file() {
            return cand;
        }
    }

    std::path::PathBuf::from("agy")
}

#[derive(Serialize)]
pub struct ModesListResponse {
    pub modes: Vec<ModeDescriptor>,
    pub current_state: ModesRunState,
}

#[derive(Deserialize)]
pub struct RunModePayload {
    pub mode: String,
    pub action: Option<String>,
}

#[derive(Serialize)]
pub struct RunModeResponse {
    pub status: String,
    pub message: String,
    pub result: Option<ModeExecutionResult>,
}

async fn api_get_modes(State(state): State<WebUiState>) -> impl IntoResponse {
    let descriptors = get_mode_descriptors();
    let current_state = state.modes_state.lock().await.clone();
    Json(ModesListResponse {
        modes: descriptors,
        current_state,
    })
}

async fn api_mode_status(State(state): State<WebUiState>) -> impl IntoResponse {
    let current_state = state.modes_state.lock().await.clone();
    Json(current_state)
}

async fn api_run_mode(
    State(state): State<WebUiState>,
    Json(payload): Json<RunModePayload>,
) -> impl IntoResponse {
    let mode_parsed = match WorkflowMode::from_str_loose(&payload.mode) {
        Some(m) => m,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(RunModeResponse {
                    status: "error".to_string(),
                    message: format!("Unknown mode: {}", payload.mode),
                    result: None,
                }),
            )
                .into_response();
        }
    };

    let action_str = payload.action.unwrap_or_else(|| match mode_parsed {
        WorkflowMode::Test => "quick".to_string(),
        WorkflowMode::Dev => "fast".to_string(),
        WorkflowMode::Doc => "verify".to_string(),
        WorkflowMode::Research => "profile".to_string(),
    });

    let mut lock = state.modes_state.lock().await;
    if lock.is_running {
        return (
            StatusCode::CONFLICT,
            Json(RunModeResponse {
                status: "busy".to_string(),
                message: "Another mode action is currently executing. Please wait.".to_string(),
                result: None,
            }),
        )
            .into_response();
    }

    lock.is_running = true;
    lock.current_mode = Some(mode_parsed.as_str().to_string());
    lock.current_action = Some(action_str.clone());
    lock.status = "running".to_string();
    lock.logs.clear();
    lock.logs.push(format!(
        "[{}] Dispatched action '{}' under mode '{}'",
        chrono::Utc::now().format("%H:%M:%S"),
        action_str,
        mode_parsed.as_str()
    ));
    drop(lock);

    // Spawn async task so the HTTP response is instantaneous
    let modes_state_clone = state.modes_state.clone();
    let action_clone = action_str.clone();
    tokio::spawn(async move {
        let workspace_root = find_workspace_root();
        let res = tokio::task::spawn_blocking(move || {
            execute_mode_action(mode_parsed, &action_clone, &workspace_root)
        })
        .await
        .unwrap_or_else(|e| {
            let mut r = ModeExecutionResult::new(mode_parsed, "task_join_error");
            r.stderr = e.to_string();
            r
        });

        let mut l = modes_state_clone.lock().await;
        l.is_running = false;
        l.status = if res.success {
            "success".to_string()
        } else {
            "failed".to_string()
        };
        if !res.stdout.is_empty() {
            l.logs.push(res.stdout.clone());
        }
        if !res.stderr.is_empty() {
            l.logs.push(format!("Error: {}", res.stderr));
        }
        l.last_result = Some(res);
    });

    (
        StatusCode::OK,
        Json(RunModeResponse {
            status: "started".to_string(),
            message: format!(
                "Started action '{}' for mode '{}'",
                action_str,
                mode_parsed.as_str()
            ),
            result: None,
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_ui_info_telemetry_fields_and_serialization() {
        let info = WorkerUiInfo {
            id: "test-uuid-123".to_string(),
            name: "android-s24-phone".to_string(),
            status: "Connected".to_string(),
            role: "WORKER".to_string(),
            role_description: "Compute Node".to_string(),
            active_tasks: 2,
            cpu_cores: 8,
            ram_mb: 12288,
            cpu_usage_pct: 24.5,
            ram_available_mb: 8192,
            has_gpu: true,
            gpu_device_name: Some("Adreno 750".to_string()),
            battery_pct: Some(85),
            is_charging: Some(true),
            thermal_throttled: false,
            last_heartbeat_secs_ago: 3,
            interconnect_type: "Direct P2P (QUIC)".to_string(),
            is_relayed: false,
            rtt_ms: Some(18.5),
        };

        let json = serde_json::to_string(&info).expect("WorkerUiInfo should serialize to json");
        assert!(json.contains("\"cpu_usage_pct\":24.5"));
        assert!(json.contains("\"ram_available_mb\":8192"));
        assert!(json.contains("\"has_gpu\":true"));
        assert!(json.contains("\"gpu_device_name\":\"Adreno 750\""));
        assert!(json.contains("\"battery_pct\":85"));
        assert!(json.contains("\"is_charging\":true"));
        assert!(json.contains("\"thermal_throttled\":false"));
        assert!(json.contains("\"last_heartbeat_secs_ago\":3"));
        assert!(json.contains("\"interconnect_type\":\"Direct P2P (QUIC)\""));
        assert!(json.contains("\"is_relayed\":false"));
        assert!(json.contains("\"rtt_ms\":18.5"));
    }

    #[test]
    fn test_dashboard_html_remediation_integrity() {
        let html = include_str!("dashboard.html");

        // 1. Facade check removed and dynamic lookup in place
        assert!(
            !html.contains("detail.worker_id.includes('mac')"),
            "Facade check detail.worker_id.includes('mac') must be eliminated"
        );
        assert!(
            html.contains("(workers || []).find(w => w.id === detail.worker_id)"),
            "Dynamic executor lookup by worker_id must be present"
        );

        // 2. MASTER device card rendered in syncData
        assert!(
            html.contains("data.master"),
            "syncData must check data.master"
        );
        assert!(
            html.contains("tag-master"),
            "tag-master class must be present for MASTER card"
        );
        assert!(
            html.contains("(Coordinator)"),
            "Coordinator designation must be rendered"
        );

        // 3. HTTP error propagation in syncData
        assert!(
            html.contains("if (!res.ok) throw new Error('HTTP ' + res.status);"),
            "syncData must propagate HTTP errors"
        );
        assert!(
            html.contains("● Disconnected"),
            "syncData catch block must update clusterSummary to Disconnected"
        );

        // 4. Cancelled task state handled in sendCmd
        assert!(
            html.contains("detail.state === 'Cancelled'"),
            "sendCmd must handle Cancelled state"
        );

        // 5. Quote escaping in escapeHtml
        assert!(
            html.contains("&quot;"),
            "escapeHtml must escape double quotes"
        );
        assert!(
            html.contains("&#039;"),
            "escapeHtml must escape single quotes"
        );

        // 6. tabDevicesBadge initialized to 0
        assert!(
            html.contains("<span id=\"tabDevicesBadge\">0</span>"),
            "tabDevicesBadge must be initialized to 0"
        );
        // 7. Workflow Modes UI presence
        assert!(
            html.contains("runWorkflowMode('dev', 'watch')"),
            "Dashboard must provide Live-Reload button in Dev mode"
        );
        assert!(
            html.contains("runWorkflowMode('doc', 'export')"),
            "Dashboard must provide Update/Export Spec button in Doc mode"
        );
    }

    #[tokio::test]
    async fn test_web_ui_modes_api_flow() {
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let modes_state = Arc::new(tokio::sync::Mutex::new(ModesRunState {
            current_mode: None,
            current_action: None,
            is_running: false,
            status: "idle".to_string(),
            logs: Vec::new(),
            last_result: None,
        }));
        let mut state = create_web_ui_state(registry, queue, scheduler_notify, None, None);
        state.modes_state = modes_state;

        // 1. Test get modes list
        let resp = api_get_modes(State(state.clone())).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        // 2. Test status endpoint
        let resp = api_mode_status(State(state.clone())).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        // 3. Test invalid mode rejection
        let invalid_payload = RunModePayload {
            mode: "invalid_mode_xyz".to_string(),
            action: None,
        };
        let resp = api_run_mode(State(state.clone()), Json(invalid_payload))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // 4. Test trigger doc verify mode
        let valid_payload = RunModePayload {
            mode: "doc".to_string(),
            action: Some("verify".to_string()),
        };
        let resp = api_run_mode(State(state.clone()), Json(valid_payload))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_web_ui_modes_concurrent_conflict() {
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let modes_state = Arc::new(tokio::sync::Mutex::new(ModesRunState {
            current_mode: Some("dev".to_string()),
            current_action: Some("fast".to_string()),
            is_running: true,
            status: "running".to_string(),
            logs: vec!["running active task".to_string()],
            last_result: None,
        }));
        let mut state = create_web_ui_state(registry, queue, scheduler_notify, None, None);
        state.modes_state = modes_state;


        let payload = RunModePayload {
            mode: "test".to_string(),
            action: Some("quick".to_string()),
        };

        let resp = api_run_mode(State(state), Json(payload))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }
}
