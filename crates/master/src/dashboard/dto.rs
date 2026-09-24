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
    /// Master's active P2P ticket for 1-click worker pairing.
    #[serde(default)]
    pub p2p_ticket: Option<String>,
}

/// Summary counts of connected and active worker nodes.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
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

impl<'de> Deserialize<'de> for WorkerSummaryDto {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct WorkerSummaryVisitor;

        impl<'de> serde::de::Visitor<'de> for WorkerSummaryVisitor {
            type Value = WorkerSummaryDto;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a worker summary object or a list of worker items")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut total = 0;
                let mut connected = 0;
                let mut busy = 0;
                let mut disconnected = 0;

                while let Some(val) = seq.next_element::<serde_json::Value>()? {
                    total += 1;
                    if let Some(status_str) = val.get("status").and_then(|s| s.as_str()) {
                        let lower = status_str.to_lowercase();
                        if lower.contains("busy") {
                            busy += 1;
                        } else if lower.contains("disconnect") || lower.contains("reap") {
                            disconnected += 1;
                        } else {
                            connected += 1;
                        }
                    } else {
                        connected += 1;
                    }
                }

                Ok(WorkerSummaryDto {
                    total,
                    connected,
                    busy,
                    disconnected,
                })
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                let mut total = None;
                let mut connected = None;
                let mut busy = None;
                let mut disconnected = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "total" => total = Some(map.next_value()?),
                        "connected" => connected = Some(map.next_value()?),
                        "busy" => busy = Some(map.next_value()?),
                        "disconnected" => disconnected = Some(map.next_value()?),
                        _ => {
                            let _ = map.next_value::<serde_json::Value>()?;
                        }
                    }
                }

                Ok(WorkerSummaryDto {
                    total: total.unwrap_or(0),
                    connected: connected.unwrap_or(0),
                    busy: busy.unwrap_or(0),
                    disconnected: disconnected.unwrap_or(0),
                })
            }
        }

        deserializer.deserialize_any(WorkerSummaryVisitor)
    }
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
