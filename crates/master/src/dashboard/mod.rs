//! Embedded Web Observability Dashboard for OxideSwarm Master node.
//!
//! Provides a self-contained web dashboard (SPA embedded via `include_str!`),
//! JSON REST APIs for cluster monitoring, and real-time telemetry streaming
//! via WebSockets and Server-Sent Events (SSE).

pub mod dto;

use std::convert::Infallible;
use std::net::SocketAddr;
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
    routing::get,
    Router,
};
use futures::{SinkExt, Stream, StreamExt};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, warn};

use crate::queue::TaskInfo;
use crate::registry::{WorkerInfo, WorkerRegistry, WorkerStatus};
use crate::TaskQueue;
pub use dto::*;
use rusty_grid_core::task::TaskId;

/// Shared application state injected into Axum route handlers.
#[derive(Clone)]
pub struct DashboardState {
    /// Reference to Master's WorkerRegistry.
    pub registry: WorkerRegistry,
    /// Reference to Master's TaskQueue.
    pub queue: TaskQueue,
    /// Master node TCP listener address.
    pub server_addr: SocketAddr,
    /// Dashboard HTTP server bound address (if known).
    pub dashboard_addr: Option<SocketAddr>,
    /// Instant when the Master server was launched.
    pub started_at: Instant,
    /// Non-blocking broadcast channel for streaming telemetry to WebSocket/SSE clients.
    pub broadcast_tx: broadcast::Sender<DashboardStreamMessage>,
    /// Master's active P2P ticket, if enabled.
    pub p2p_ticket: Option<String>,
}

impl DashboardState {
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

/// Builds and configures the complete Axum Router for the embedded dashboard.
pub fn create_dashboard_router(state: DashboardState) -> Router {
    Router::new()
        // Embedded Single Page Application
        .route("/", get(dashboard_index_handler))
        // REST APIs
        .route("/api/status", get(get_status_handler))
        .route("/api/workers", get(get_workers_handler))
        .route("/api/tasks", get(get_tasks_handler))
        .route("/api/tasks/:task_id", get(get_task_by_id_handler))
        // Real-time WebSocket telemetry stream (standard /ws and alias /api/stream)
        .route("/ws", get(ws_handler))
        .route("/api/stream", get(ws_handler))
        // Real-time Server-Sent Events stream
        .route("/api/stream/sse", get(sse_handler))
        // Permissive CORS layer for cross-origin integration
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// --- REST Handlers ---

/// Serves the self-contained dashboard Single Page Application HTML.
async fn dashboard_index_handler() -> impl IntoResponse {
    Html(include_str!("index.html"))
}

/// Returns aggregated cluster health, version, uptime, worker counts, and queue stats.
async fn get_status_handler(State(state): State<DashboardState>) -> Json<ClusterStatusDto> {
    let snapshot = state.build_snapshot().await;
    Json(snapshot.status)
}

/// Returns the snapshot of all registered worker nodes and their telemetry.
async fn get_workers_handler(State(state): State<DashboardState>) -> Json<Vec<WorkerInfo>> {
    Json(state.registry.list_all_workers().await)
}

/// Returns task list, optionally filtered by `state` and truncated by `limit`.
async fn get_tasks_handler(
    State(state): State<DashboardState>,
    Query(params): Query<TaskQueryParams>,
) -> Json<Vec<TaskInfo>> {
    let mut tasks = state.queue.list_tasks().await;

    // Filter by state if provided and non-empty
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

    // Limit returned records if provided
    if let Some(limit) = params.limit {
        tasks.truncate(limit);
    }

    Json(tasks)
}

/// Returns detailed metadata for a specific task by its UUID, or HTTP 404 if not found.
async fn get_task_by_id_handler(
    State(state): State<DashboardState>,
    Path(task_id_str): Path<String>,
) -> Result<Json<TaskInfo>, StatusCode> {
    let Ok(uuid) = uuid::Uuid::parse_str(&task_id_str) else {
        return Err(StatusCode::NOT_FOUND);
    };
    let task_id = TaskId(uuid);

    state
        .queue
        .get_task(&task_id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

// --- WebSocket Handlers ---

/// Upgrades HTTP connection to WebSocket for live telemetry streaming.
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<DashboardState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_client(socket, state))
}

/// Manages the lifecycle of a connected WebSocket telemetry client.
async fn handle_ws_client(socket: WebSocket, state: DashboardState) {
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

    // 2. Periodic keepalive ping timer (20s) to keep intermediate proxies alive
    let mut ping_interval = tokio::time::interval(Duration::from_secs(20));
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // 3. Stream incremental updates until client disconnects or broadcast terminates
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

/// Server-Sent Events streaming handler mounted at `/api/stream/sse`.
async fn sse_handler(
    State(state): State<DashboardState>,
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
