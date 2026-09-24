//! Embedded Web Observability Dashboard for OxideSwarm Master node.

pub mod dto;

use std::net::SocketAddr;
use std::time::Instant;

use axum::Router;
use tokio::sync::broadcast;

use crate::queue::TaskQueue;
use crate::registry::{WorkerRegistry, WorkerStatus};
pub use dto::*;

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

/// Builds and configures the complete Axum Router for the embedded dashboard, delegating to the unified Web UI router.
pub fn create_dashboard_router(state: DashboardState) -> Router {
    crate::web_ui::create_web_ui_router_full(
        state.registry,
        state.queue,
        std::sync::Arc::new(tokio::sync::Notify::new()),
        None,
        state.p2p_ticket,
        state.broadcast_tx,
        state.server_addr,
        state.dashboard_addr,
        state.started_at,
        std::sync::Arc::new(crate::auth::AuthConfig::default()),
    )
}
