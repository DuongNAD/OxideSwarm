use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tracing::{error, info};

use crate::{queue::TaskQueue, registry::WorkerRegistry};
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
}

/// Creates a fresh `WebUiState` instance with initialized run state.
pub fn create_web_ui_state(
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    master_handle: Option<crate::server::MasterHandle>,
    p2p_ticket: Option<String>,
) -> WebUiState {
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
    }
}

/// Creates the full Axum router for the Web UI dashboard and REST API.
pub fn create_web_ui_router(
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    master_handle: Option<crate::server::MasterHandle>,
    p2p_ticket: Option<String>,
) -> Router {
    let state = create_web_ui_state(
        registry,
        queue,
        scheduler_notify,
        master_handle,
        p2p_ticket,
    );
    Router::new()
        .route("/", get(index_html))
        .route("/api/status", get(api_status))
        .route("/api/tasks", post(api_submit_task))
        .route("/api/tasks/{task_id}", get(api_get_task))
        .route("/api/chat", post(api_chat))
        .route("/api/modes", get(api_get_modes))
        .route("/api/modes/run", post(api_run_mode))
        .route("/api/modes/status", get(api_mode_status))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

pub async fn start_web_ui(
    addr: SocketAddr,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
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

    let mut shutdown_rx = shutdown_rx;
    let server = axum::serve(listener, app);

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

async fn index_html() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
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

pub type ClusterStatusDto = crate::dashboard::dto::ClusterStatusDto;

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
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
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
        running: stats.running,
        completed: stats.completed,
        failed: stats.failed,
        active_list,
    };

    Json(DashboardStatus {
        master,
        workers,
        tasks,
        p2p_ticket: state.p2p_ticket.clone(),
    })
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

#[derive(Serialize)]
struct TaskDetailResponse {
    id: String,
    state: String,
    worker_id: Option<String>,
    exit_code: Option<i32>,
    stdout: Option<String>,
    stderr: Option<String>,
    execution_time_ms: Option<u64>,
    error: Option<String>,
}

async fn api_get_task(
    State(state): State<WebUiState>,
    Path(task_id_str): Path<String>,
) -> impl IntoResponse {
    let task_id = match TaskId::from_str(&task_id_str) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid Task ID format").into_response(),
    };

    let task_state = state.queue.get_state(&task_id).await;
    if task_state.is_none() {
        return (StatusCode::NOT_FOUND, "Task not found").into_response();
    }
    let state_str = format!("{:?}", task_state.unwrap());

    let task_info = state.queue.get_task(&task_id).await;
    let worker_id = task_info.and_then(|t| t.assigned_worker_id.map(|w| w.to_string()));

    let result = state.queue.get_result(&task_id).await;

    let response = TaskDetailResponse {
        id: task_id_str,
        state: state_str,
        worker_id,
        exit_code: result.as_ref().map(|r| r.exit_code),
        stdout: result.as_ref().map(|r| r.stdout.clone()),
        stderr: result.as_ref().map(|r| r.stderr.clone()),
        execution_time_ms: result.as_ref().map(|r| r.execution_time_ms),
        error: result.and_then(|r| r.error),
    };

    Json(response).into_response()
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
        let state = WebUiState {
            registry,
            queue,
            scheduler_notify,
            modes_state,
            master_handle: None,
            p2p_ticket: None,
        };

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
        let state = WebUiState {
            registry,
            queue,
            scheduler_notify,
            modes_state,
            master_handle: None,
            p2p_ticket: None,
        };

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
