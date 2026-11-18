//! MCP `workers` tool implementation, split out of `mcp.rs` (design 0080, MAINT-2).

use serde_json::{json, Value};

use rusty_grid_core::placement::{
    listed_worker_id, tag_value, ARCH_TAG_PREFIX, OS_TAG_PREFIX, STATE_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};

use super::{McpState, ToolCallResult};

pub(super) async fn tool_workers(state: &McpState, args: &Value) -> ToolCallResult {
    let connected_only = args
        .get("connected_only")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let os_filter = args
        .get("os")
        .and_then(Value::as_str)
        .map(str::to_lowercase);
    let arch_filter = args
        .get("arch")
        .and_then(Value::as_str)
        .map(str::to_lowercase);
    let gpu_only = args
        .get("gpu_only")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    match state.request(&ClientMessage::ListWorkers).await {
        Ok(ClientResponse::WorkerList { workers }) => {
            let filtered: Vec<Value> = workers
                .into_iter()
                .filter(|w| {
                    let is_connected = tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected");
                    if connected_only && !is_connected {
                        return false;
                    }
                    if gpu_only && !w.has_gpu {
                        return false;
                    }
                    if let Some(target_os) = &os_filter {
                        let w_os = tag_value(&w.tags, OS_TAG_PREFIX).map(str::to_lowercase);
                        if w_os.as_deref() != Some(target_os.as_str()) {
                            return false;
                        }
                    }
                    if let Some(target_arch) = &arch_filter {
                        let w_arch = tag_value(&w.tags, ARCH_TAG_PREFIX).map(str::to_lowercase);
                        if w_arch.as_deref() != Some(target_arch.as_str()) {
                            return false;
                        }
                    }
                    true
                })
                .map(|w| {
                    let is_connected = tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected");
                    json!({
                        "name": w.name,
                        "id": listed_worker_id(&w),
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

            ToolCallResult::ok_json(&Value::Array(filtered))
        }
        Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
        Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
        Err(e) => ToolCallResult::error(e),
    }
}
