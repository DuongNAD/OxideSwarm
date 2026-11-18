//! MCP `doctor` tool implementation, split out of `mcp.rs` (design 0080, MAINT-2).

use std::time::Duration;

use serde_json::Value;

use crate::doctor::{run_doctor, DoctorOptions};

use super::{McpState, ToolCallResult, ToolContent};

pub(super) async fn tool_doctor(state: &McpState, args: &Value) -> ToolCallResult {
    let mut options = DoctorOptions::default();

    if let Some(exp) = args.get("expect_workers").and_then(Value::as_u64) {
        options.expect_workers = exp as usize;
    }
    if let Some(to) = args.get("timeout_secs").and_then(Value::as_u64) {
        options.probe_timeout = Duration::from_secs(to);
    }
    if let Some(dim) = args.get("gpu_dim").and_then(Value::as_u64) {
        options.gpu_dim = dim as u32;
    }
    if let Some(spread) = args.get("spread_tasks").and_then(Value::as_u64) {
        options.spread_tasks = Some(spread as usize);
    }
    if let Some(skip_gpu) = args.get("skip_gpu").and_then(Value::as_bool) {
        options.skip_gpu = skip_gpu;
    }
    if let Some(skip_spread) = args.get("skip_spread").and_then(Value::as_bool) {
        options.skip_spread = skip_spread;
    }
    if let Some(skip_throughput) = args.get("skip_throughput").and_then(Value::as_bool) {
        options.skip_throughput = skip_throughput;
    }

    let target_addr = state
        .resolve_target_addr()
        .await
        .unwrap_or_else(|_| state.master_addr.clone());
    let report = run_doctor(&target_addr, state.secret.as_ref(), &options).await;
    let is_error = report.failed() || report.auth_failed;
    match serde_json::to_value(&report) {
        Ok(val) => {
            let text = serde_json::to_string_pretty(&val).unwrap_or_else(|_| val.to_string());
            ToolCallResult {
                content: vec![ToolContent::Text { text }],
                is_error,
            }
        }
        Err(e) => ToolCallResult::error(format!("Failed to serialize doctor report: {e}")),
    }
}
