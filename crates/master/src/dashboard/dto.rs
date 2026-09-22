//! Data Transfer Objects (DTO) for OxideSwarm Web Observability Dashboard.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::queue::{QueueStats, TaskInfo};
use crate::registry::WorkerInfo;

/// Aggregated cluster health and operational metrics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatusDto {
    /// Framework / master version string.
    pub version: String,
    /// Master server uptime in seconds.
    pub uptime_secs: u64,
    /// Master TCP socket address string (e.g. "127.0.0.1:8080").
    pub master_addr: String,
    /// Embedded dashboard HTTP socket address string (e.g. "127.0.0.1:3000"), if enabled.
    pub dashboard_addr: Option<String>,
    /// Summary counts of worker nodes by operational state.
    pub workers: WorkerSummaryDto,
    /// Summary counts of tasks by lifecycle state.
    pub tasks: QueueStats,
}

/// Summary counts of connected and active worker nodes.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WorkerSummaryDto {
    /// Total registered workers across all states.
    pub total: usize,
    /// Workers in `Connected` (idle/ready) state.
    pub connected: usize,
    /// Workers currently executing one or more tasks (`Busy`).
    pub busy: usize,
    /// Workers currently disconnected or reaped.
    pub disconnected: usize,
}

/// Full point-in-time state snapshot of the cluster, delivered upon WebSocket handshake.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterSnapshotDto {
    /// Epoch timestamp in seconds when this snapshot was captured.
    pub timestamp_utc: u64,
    /// Master node uptime in seconds.
    pub uptime_secs: u64,
    /// Framework version string.
    pub version: String,
    /// High-level cluster status summary.
    pub status: ClusterStatusDto,
    /// Snapshot of all registered worker nodes and their telemetry.
    pub workers: Vec<WorkerInfo>,
    /// Snapshot of all tasks currently in the master queue.
    pub tasks: Vec<TaskInfo>,
}

/// Real-time event messages streamed to WebSocket and SSE clients.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum DashboardStreamMessage {
    /// Initial full snapshot sent immediately upon WebSocket connection establishment.
    Snapshot(ClusterSnapshotDto),
    /// Incremental worker heartbeat telemetry event.
    WorkerHeartbeat {
        worker_id: Uuid,
        cpu_usage_pct: f32,
        ram_available_mb: u64,
        active_tasks: usize,
        timestamp: u64,
    },
    /// Notification that a new worker registered or an existing worker reconnected.
    WorkerRegistered(WorkerInfo),
    /// Notification that a worker disconnected or was reaped.
    WorkerDisconnected { worker_id: Uuid, reason: String },
    /// Notification that a task changed state, was assigned, or finished.
    TaskUpdated(TaskInfo),
    /// Periodic or event-driven update of queue statistics.
    StatsUpdated(QueueStats),
}

impl DashboardStreamMessage {
    /// Returns the SSE event type identifier string.
    pub fn event_name(&self) -> &'static str {
        match self {
            DashboardStreamMessage::Snapshot(_) => "snapshot",
            DashboardStreamMessage::WorkerRegistered(_) => "worker_registered",
            DashboardStreamMessage::WorkerHeartbeat { .. } => "worker_heartbeat",
            DashboardStreamMessage::WorkerDisconnected { .. } => "worker_disconnected",
            DashboardStreamMessage::TaskUpdated(_) => "task_updated",
            DashboardStreamMessage::StatsUpdated(_) => "stats_updated",
        }
    }
}

/// URL query parameters for filtering `GET /api/tasks`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TaskQueryParams {
    /// Optional filter by task state (e.g. "queued", "running", "completed", "failed").
    pub state: Option<String>,
    /// Optional maximum number of tasks to return.
    pub limit: Option<usize>,
}
