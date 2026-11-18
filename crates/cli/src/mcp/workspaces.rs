//! MCP `workspaces` tool implementation, split out of `mcp.rs` (design 0080, MAINT-2).

use serde_json::{json, Value};

use rusty_grid_core::protocol::{ClientMessage, ClientResponse};

use super::{McpState, ToolCallResult};

pub(super) async fn tool_workspaces(state: &McpState) -> ToolCallResult {
    match state.request(&ClientMessage::ListWorkspaces).await {
        Ok(ClientResponse::Workspaces { workspaces }) => {
            let list: Vec<Value> = workspaces
                .into_iter()
                .map(|ws| {
                    json!({
                        "name": ws.name,
                        "worker": ws.worker.to_string(),
                        "last_used_utc": ws.last_used_utc,
                    })
                })
                .collect();
            ToolCallResult::ok_json(&Value::Array(list))
        }
        Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
        Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
        Err(e) => ToolCallResult::error(e),
    }
}
