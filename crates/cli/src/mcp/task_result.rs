//! MCP `task_result` tool implementation, split out of `mcp.rs` (design 0080, MAINT-2).

use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;

use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::TaskId;

use super::submit::{format_completed_task_result_with_start, prepare_mcp_output_dir_in_dir};
use super::{McpState, ToolCallContext, ToolCallResult};

pub(super) async fn tool_task_result(
    state: &McpState,
    args: &Value,
    ctx: &mut ToolCallContext,
) -> ToolCallResult {
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

    if let Some(out_dir_str) = args.get("output_dir").and_then(Value::as_str) {
        let cwd = match std::env::current_dir() {
            Ok(dir) => dir,
            Err(e) => {
                return ToolCallResult::error(format!(
                    "Output directory error: cannot determine working directory: {e}"
                ))
            }
        };
        if let Err(e) = prepare_mcp_output_dir_in_dir(&cwd, out_dir_str) {
            return ToolCallResult::error(format!("Output directory error: {e}"));
        }
    }

    let wait = args.get("wait").and_then(Value::as_bool).unwrap_or(false);
    let timeout_secs = args
        .get("wait_secs")
        .or_else(|| args.get("timeout_secs"))
        .and_then(Value::as_u64)
        .unwrap_or(60);

    if wait && timeout_secs > 0 {
        if let Err(e) = state.resolve_target_addr().await {
            return ToolCallResult::error(e);
        }
        let deadline = crate::matrix::safe_deadline(Duration::from_secs(timeout_secs));
        let mut transport = match state.connect_dedicated_until(deadline).await {
            Ok(t) => t,
            Err(e) => return ToolCallResult::error(e),
        };
        let mut cancel_rx = ctx.cancel_rx.clone();
        let mut cancel_closed = false;
        let mut step = 0u64;

        loop {
            if *cancel_rx.borrow() {
                ctx.aborted_by_cancel = true;
                return ToolCallResult::error(format!("Wait for task {task_id} cancelled"));
            }

            let msg = ClientMessage::GetTaskResult { task_id };
            let send_res = tokio::select! {
                biased;
                res = cancel_rx.changed(), if !cancel_closed => {
                    if res.is_err() {
                        cancel_closed = true;
                        continue;
                    }
                    if *cancel_rx.borrow() {
                        ctx.aborted_by_cancel = true;
                        return ToolCallResult::error(format!("Wait for task {task_id} cancelled"));
                    }
                    continue;
                }
                res = tokio::time::timeout_at(deadline, transport.send_msg(&msg)) => res,
            };
            match send_res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    return ToolCallResult::error(format!("Failed to query task result: {e}"))
                }
                Err(_) => {
                    return ToolCallResult::ok_json(&json!({
                        "task_id": task_id.to_string(),
                        "status": "Unknown",
                        "state": "unknown",
                        "timed_out": true,
                    }));
                }
            }

            let recv_res = tokio::select! {
                biased;
                res = cancel_rx.changed(), if !cancel_closed => {
                    if res.is_err() {
                        cancel_closed = true;
                        continue;
                    }
                    if *cancel_rx.borrow() {
                        ctx.aborted_by_cancel = true;
                        return ToolCallResult::error(format!("Wait for task {task_id} cancelled"));
                    }
                    continue;
                }
                res = tokio::time::timeout_at(deadline, transport.recv_msg::<ClientResponse>()) => res,
            };

            let resp = match recv_res {
                Ok(r) => r,
                Err(_) => {
                    return ToolCallResult::ok_json(&json!({
                        "task_id": task_id.to_string(),
                        "status": "Unknown",
                        "state": "unknown",
                        "timed_out": true,
                    }));
                }
            };

            match resp {
                Ok(Some(ClientResponse::TaskCompleted { result, .. })) => {
                    ctx.send_progress(
                        step + 1,
                        Some(step + 1),
                        format!("Task {task_id} completed"),
                    );
                    let started_at = state.submitted_at(task_id).await;
                    return format_completed_task_result_with_start(
                        state, &result, args, None, started_at,
                    )
                    .await;
                }
                Ok(Some(ClientResponse::TaskStatusInfo {
                    task_id: tid,
                    status,
                    state_name,
                    error,
                    ..
                })) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return ToolCallResult::ok_json(&json!({
                            "task_id": tid.to_string(),
                            "status": format!("{:?}", status),
                            "state": state_name,
                            "error": error,
                            "timed_out": true,
                        }));
                    }
                    step += 1;
                    ctx.send_progress(step, None, format!("Task {task_id} state: {state_name}"));
                    let sleep_until = (now + Duration::from_millis(150)).min(deadline);
                    tokio::select! {
                        biased;
                        res = cancel_rx.changed(), if !cancel_closed => {
                            if res.is_err() {
                                cancel_closed = true;
                            } else if *cancel_rx.borrow() {
                                ctx.aborted_by_cancel = true;
                                return ToolCallResult::error(format!("Wait for task {task_id} cancelled"));
                            }
                        }
                        _ = tokio::time::sleep_until(sleep_until) => {
                            if tokio::time::Instant::now() >= deadline {
                                return ToolCallResult::ok_json(&json!({
                                    "task_id": tid.to_string(),
                                    "status": format!("{:?}", status),
                                    "state": state_name,
                                    "error": error,
                                    "timed_out": true,
                                }));
                            }
                        }
                    }
                }
                Ok(Some(ClientResponse::Error { message })) => {
                    return ToolCallResult::error(message)
                }
                Ok(Some(other)) => {
                    return ToolCallResult::error(format!("Unexpected Master response: {other:?}"))
                }
                Ok(None) => {
                    return ToolCallResult::error(
                        "Master closed connection while polling task result",
                    )
                }
                Err(e) => return ToolCallResult::error(format!("Communication error: {e}")),
            }
        }
    } else {
        match state
            .request(&ClientMessage::GetTaskResult { task_id })
            .await
        {
            Ok(ClientResponse::TaskCompleted { result, .. }) => {
                let started_at = state.submitted_at(task_id).await;
                format_completed_task_result_with_start(state, &result, args, None, started_at)
                    .await
            }
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
    }
}
