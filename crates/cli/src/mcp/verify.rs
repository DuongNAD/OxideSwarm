//! MCP `verify` tool implementation (design 0111, D4 / TEAM-3).

use std::path::Path;
use std::time::Duration;

use serde_json::Value;

use crate::plan::lease::{CIRCUIT_BREAKER_THRESHOLD, DEFAULT_REMOTE};
use crate::verify_gate::{
    resolve_hub_dir, resolve_max_verifies_per_hour, run_verify_gate_with_options,
    VerifyGateOptions, DEFAULT_VERIFY_TIMEOUT_SECS,
};

use super::plan::confine_mcp_path;
use super::{McpState, ToolCallContext, ToolCallResult};

pub(super) async fn tool_verify(
    state: &McpState,
    args: &Value,
    ctx: &mut ToolCallContext,
) -> ToolCallResult {
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            return ToolCallResult::error(format!("cannot determine working directory: {e}"));
        }
    };
    tool_verify_in_dir(state, &cwd, args, ctx).await
}

pub(crate) async fn tool_verify_in_dir(
    state: &McpState,
    cwd: &Path,
    args: &Value,
    ctx: &mut ToolCallContext,
) -> ToolCallResult {
    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error("Verify cancelled by client");
    }

    let Some(task_id) = args
        .get("task")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return ToolCallResult::error("Property 'task' must be a non-empty task id string");
    };

    let repo_dir = match args.get("repo_dir").and_then(Value::as_str) {
        Some(raw) => match confine_mcp_path(cwd, raw, "repo_dir", true) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(e),
        },
        None => cwd.to_path_buf(),
    };
    let plan_path = match args.get("plan_path").and_then(Value::as_str) {
        Some(raw) => match confine_mcp_path(&repo_dir, raw, "plan_path", false) {
            Ok(p) => Some(p),
            Err(e) => return ToolCallResult::error(e),
        },
        None => None,
    };

    let profiles: Vec<String> = args
        .get("profiles")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let base_override = args.get("base").and_then(Value::as_str);
    let timeout_secs = args
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_VERIFY_TIMEOUT_SECS);
    if timeout_secs == 0 {
        return ToolCallResult::error("Property 'timeout_secs' must be at least 1");
    }

    let target_addr = match state.resolve_target_addr().await {
        Ok(addr) => addr,
        Err(e) => return ToolCallResult::error(e),
    };

    let workspace_limits = state.workspace_limits;
    let remote = args
        .get("remote")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_REMOTE)
        .to_string();
    let agent_id = args
        .get("agent")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    // Hourly cap and hub_dir come strictly from server env (an agent cannot raise its own cap
    // or redirect mailbox writes via MCP tool arguments).
    let max_verifies_per_hour = match resolve_max_verifies_per_hour(None) {
        Ok(v) => v,
        Err(e) => return ToolCallResult::error(e),
    };
    let options = VerifyGateOptions {
        remote,
        agent_id,
        max_verifies_per_hour,
        hub_dir: resolve_hub_dir(None),
        breaker_threshold: CIRCUIT_BREAKER_THRESHOLD,
    };

    match run_verify_gate_with_options(
        &repo_dir,
        plan_path.as_deref(),
        task_id,
        &profiles,
        base_override,
        &target_addr,
        state.secret.clone(),
        Duration::from_secs(timeout_secs),
        workspace_limits,
        options,
    )
    .await
    {
        Ok((_report, compact_json)) => ToolCallResult::ok(compact_json),
        Err(e) => ToolCallResult::error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::{call_tool, validate_tool_arguments, McpState, ToolContent};
    use serde_json::json;

    #[test]
    fn verify_tool_arguments_validate_paths_and_types() {
        assert!(validate_tool_arguments("verify", &json!({"task": "T-1"})).is_ok());
        assert!(validate_tool_arguments("verify", &json!({})).is_err());
        assert!(
            validate_tool_arguments("verify", &json!({"task": "T-1", "repo_dir": "../bad"}))
                .is_err()
        );
        assert!(
            validate_tool_arguments("verify", &json!({"task": "T-1", "timeout_secs": 0})).is_err()
        );
    }

    #[tokio::test]
    async fn tool_verify_refuses_symlinks_and_parent_traversal() {
        let outer = tempfile::tempdir().unwrap();
        let cwd = outer.path().join("repo");
        std::fs::create_dir_all(cwd.join(".oxideswarm")).unwrap();
        let outside = outer.path().join("outside.toml");
        std::fs::write(&outside, "[project]\nbase = \"master\"\n").unwrap();

        let state = McpState::new("127.0.0.1:0".to_string(), None);
        let mut ctx = ToolCallContext::default();
        let trav = tool_verify_in_dir(
            &state,
            &cwd,
            &json!({"task": "T-1", "repo_dir": "../escape"}),
            &mut ctx,
        )
        .await;
        assert!(trav.is_error);
        let ToolContent::Text { text: trav_text } = &trav.content[0];
        assert!(
            trav_text.contains("..") || trav_text.contains("parent"),
            "{trav_text}"
        );

        #[cfg(unix)]
        {
            let link = cwd.join("link.toml");
            std::os::unix::fs::symlink(&outside, &link).unwrap();
            let res = tool_verify_in_dir(
                &state,
                &cwd,
                &json!({"task": "T-1", "plan_path": "link.toml"}),
                &mut ctx,
            )
            .await;
            assert!(res.is_error);
            let ToolContent::Text { text } = &res.content[0];
            assert!(text.contains("symbolic link"), "{text}");
        }
        let res = call_tool(&state, "verify", &json!({"task": 123})).await;
        assert!(res.is_error);
    }
}
