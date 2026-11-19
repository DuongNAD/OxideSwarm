//! MCP `status` tool implementation, split out of `mcp.rs` (design 0080, MAINT-2).

use serde_json::{json, Value};
use uuid::Uuid;

use rusty_grid_core::placement::{listed_worker_id, tag_value, STATE_TAG_PREFIX};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::TaskId;

use super::{McpState, ToolCallResult};

pub(super) async fn tool_status(state: &McpState, args: &Value) -> ToolCallResult {
    let task_id_opt = args.get("task_id").and_then(Value::as_str);

    if let Some(task_id_str) = task_id_opt {
        let task_uuid = match Uuid::parse_str(task_id_str) {
            Ok(u) => u,
            Err(e) => {
                return ToolCallResult::error(format!("Invalid task_id UUID '{task_id_str}': {e}"))
            }
        };

        let msg = ClientMessage::GetTaskStatus {
            task_id: TaskId(task_uuid),
        };

        match state.request(&msg).await {
            Ok(ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            }) => ToolCallResult::ok_json(&json!({
                "task_id": task_id.to_string(),
                "status": format!("{:?}", status),
                "state": state_name,
                "assigned_worker": assigned_worker,
                "error": error,
            })),
            Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
            Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
            Err(e) => ToolCallResult::error(e),
        }
    } else {
        // Query cluster status & summary via ClientMessage::ClusterStatus
        match state.request(&ClientMessage::ClusterStatus).await {
            Ok(ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                cancelled_tasks,
                workers,
                active_bytes,
                max_active_bytes,
                store_error,
            }) => {
                let connected_count = workers
                    .iter()
                    .filter(|w| tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected"))
                    .count();

                let cluster_status = if store_error.is_some() {
                    "degraded"
                } else if connected_count > 0 {
                    "ok"
                } else {
                    "no_workers"
                };

                let include_workers = args
                    .get("workers")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);

                let mut summary = json!({
                    "cluster_status": cluster_status,
                    "master": state.master_addr,
                    "total_workers": workers.len(),
                    "connected_workers": connected_count,
                    "total_tasks": total_tasks,
                    "pending_tasks": pending_tasks,
                    "running_tasks": running_tasks,
                    "completed_tasks": completed_tasks,
                    "failed_tasks": failed_tasks,
                    "cancelled_tasks": cancelled_tasks,
                    "active_bytes": active_bytes,
                    "max_active_bytes": max_active_bytes,
                });
                if let Some(err) = store_error {
                    summary["store_error"] = Value::String(err);
                }

                if include_workers {
                    let worker_list: Vec<Value> = workers
                        .iter()
                        .map(|w| {
                            let is_connected =
                                tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected");
                            json!({
                                "name": w.name,
                                "id": listed_worker_id(w),
                                "cpu_cores": w.cpu_cores,
                                "ram_mb": w.ram_mb,
                                "vram_mb": w.vram_mb,
                                "has_gpu": w.has_gpu,
                                "is_simulated_gpu": w.is_simulated_gpu,
                                "gpu_device_name": w.gpu_device_name,
                                "tags": w.tags,
                                "connected": is_connected,
                            })
                        })
                        .collect();
                    summary["workers"] = Value::Array(worker_list);
                }

                ToolCallResult::ok_json(&summary)
            }
            Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
            Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
            Err(e) => ToolCallResult::error(e),
        }
    }
}
