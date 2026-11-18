//! MCP `cancel` tool implementation, split out of `mcp.rs` (design 0080, MAINT-2).

use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;

use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::TaskId;

use super::{McpState, ToolCallResult, DEFAULT_MASTER_RPC_TIMEOUT};

pub(super) async fn tool_cancel(state: &McpState, args: &Value) -> ToolCallResult {
    let task_id_str = match args.get("task_id").and_then(Value::as_str) {
        Some(s) => s,
        None => return ToolCallResult::error("Missing required property 'task_id'"),
    };

    let task_uuid = match Uuid::parse_str(task_id_str) {
        Ok(u) => u,
        Err(e) => {
            return ToolCallResult::error(format!("Invalid task_id UUID '{task_id_str}': {e}"))
        }
    };
    let task_id = TaskId(task_uuid);

    match state
        .request_with_retry_info(
            &ClientMessage::CancelTask { task_id },
            DEFAULT_MASTER_RPC_TIMEOUT,
        )
        .await
    {
        Ok((ClientResponse::TaskCancelled { task_id, success }, retried_after_pooled_write)) => {
            let mut cancelled = success;
            if !cancelled && retried_after_pooled_write {
                if let Ok(ClientResponse::TaskStatusInfo { status, .. }) = state
                    .request_with_timeout(
                        &ClientMessage::GetTaskStatus { task_id },
                        Duration::from_millis(500),
                    )
                    .await
                {
                    if matches!(status, rusty_grid_core::TaskStatus::Cancelled) {
                        cancelled = true;
                    }
                }
            }
            ToolCallResult::ok_json(&json!({
                "task_id": task_id.to_string(),
                "cancelled": cancelled,
            }))
        }
        Ok((ClientResponse::Error { message }, _)) => ToolCallResult::error(message),
        Ok((other, _)) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
        Err(e) => ToolCallResult::error(e),
    }
}
