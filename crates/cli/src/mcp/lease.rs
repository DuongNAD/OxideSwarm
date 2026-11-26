//! MCP `task_claim`, `task_renew`, and `task_release` tool implementations (design 0111, TEAM-2).

use std::path::{Component, Path, PathBuf};

use serde_json::Value;

use super::plan::confine_mcp_path;
use super::{check_opt_bool, check_opt_str, check_opt_u64, ToolCallResult};
use crate::plan::lease::{
    claim_task, release_task, renew_task, validate_branch_name, validate_git_token,
    validate_holder, validate_remote_name, validate_ttl_minutes, ClaimOptions, ReleaseOptions,
    RenewOptions, DEFAULT_LEASE_TTL_MINS, DEFAULT_REMOTE, MAX_LEASE_TTL_MINS,
};
use crate::plan::validate_task_id;

fn validate_optional_path_prop(
    obj: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<String>, String> {
    if let Some(raw) = check_opt_str(obj, field)? {
        if raw.trim().is_empty()
            || Path::new(raw)
                .components()
                .any(|c| matches!(c, Component::ParentDir))
        {
            return Err(format!(
                "Property '{field}' ('{raw}') must be non-empty and must not contain '..'"
            ));
        }
        return Ok(Some(raw.to_string()));
    }
    Ok(None)
}

fn validate_ttl_prop(obj: &serde_json::Map<String, Value>) -> Result<i64, String> {
    match check_opt_u64(obj, "ttl_minutes")? {
        Some(v) => {
            let signed = i64::try_from(v).map_err(|_| {
                format!("Property 'ttl_minutes' must be between 1 and {MAX_LEASE_TTL_MINS}")
            })?;
            validate_ttl_minutes(signed)
        }
        None => Ok(DEFAULT_LEASE_TTL_MINS),
    }
}

pub(super) fn validate_task_claim_args(obj: &serde_json::Map<String, Value>) -> Result<(), String> {
    let task_id = check_opt_str(obj, "task_id")?
        .ok_or_else(|| "Missing required property 'task_id'".to_string())?;
    validate_task_id(task_id)?;
    let holder = check_opt_str(obj, "holder")?
        .ok_or_else(|| "Missing required property 'holder'".to_string())?;
    validate_holder(holder)?;
    check_opt_str(obj, "person")?;
    if let Some(remote) = check_opt_str(obj, "remote")? {
        validate_remote_name(remote)?;
    }
    if let Some(base) = check_opt_str(obj, "base")? {
        validate_git_token(base, "base")?;
    }
    if let Some(branch) = check_opt_str(obj, "branch")? {
        validate_branch_name(branch)?;
    }
    validate_optional_path_prop(obj, "plan_path")?;
    validate_optional_path_prop(obj, "repo_dir")?;
    validate_optional_path_prop(obj, "worktree_root")?;
    check_opt_bool(obj, "create_worktree")?;
    validate_ttl_prop(obj)?;
    Ok(())
}

pub(super) fn validate_task_renew_args(obj: &serde_json::Map<String, Value>) -> Result<(), String> {
    let task_id = check_opt_str(obj, "task_id")?
        .ok_or_else(|| "Missing required property 'task_id'".to_string())?;
    validate_task_id(task_id)?;
    let holder = check_opt_str(obj, "holder")?
        .ok_or_else(|| "Missing required property 'holder'".to_string())?;
    validate_holder(holder)?;
    if let Some(remote) = check_opt_str(obj, "remote")? {
        validate_remote_name(remote)?;
    }
    if let Some(oid) = check_opt_str(obj, "expected_oid")? {
        validate_git_token(oid, "expected_oid")?;
    }
    validate_optional_path_prop(obj, "repo_dir")?;
    validate_ttl_prop(obj)?;
    check_opt_bool(obj, "blocked")?;
    check_opt_u64(obj, "verify_attempts")?;
    check_opt_u64(obj, "verify_failures")?;
    check_opt_str(obj, "last_failure")?;
    check_opt_str(obj, "last_fingerprint")?;
    Ok(())
}

pub(super) fn validate_task_release_args(
    obj: &serde_json::Map<String, Value>,
) -> Result<(), String> {
    let task_id = check_opt_str(obj, "task_id")?
        .ok_or_else(|| "Missing required property 'task_id'".to_string())?;
    validate_task_id(task_id)?;
    if let Some(holder) = check_opt_str(obj, "holder")? {
        validate_holder(holder)?;
    }
    if let Some(remote) = check_opt_str(obj, "remote")? {
        validate_remote_name(remote)?;
    }
    validate_optional_path_prop(obj, "repo_dir")?;
    check_opt_bool(obj, "force")?;
    Ok(())
}

fn ensure_confined_worktree_root(cwd: &Path, raw: Option<&str>) -> Result<PathBuf, String> {
    if let Some(path_str) = raw {
        return confine_mcp_path(cwd, path_str, "worktree_root", true);
    }
    // Keep default MCP worktrees strictly inside cwd rather than `../wt-<id>`.
    let canon_cwd = std::fs::canonicalize(cwd)
        .map_err(|e| format!("cannot resolve working directory '{}': {e}", cwd.display()))?;
    let mut cur = cwd.to_path_buf();
    for seg in [".oxideswarm", "worktrees"] {
        cur.push(seg);
        match std::fs::symlink_metadata(&cur) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(format!(
                        "Default worktree root '{}' traverses a symlink, which is refused",
                        cur.display()
                    ));
                }
                if !meta.is_dir() {
                    return Err(format!(
                        "Default worktree root '{}' exists and is not a directory",
                        cur.display()
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&cur).map_err(|ce| {
                    format!("failed to create worktree root '{}': {ce}", cur.display())
                })?;
            }
            Err(e) => {
                return Err(format!(
                    "cannot inspect worktree root '{}': {e}",
                    cur.display()
                ));
            }
        }
    }
    let canon_target = std::fs::canonicalize(&cur)
        .map_err(|e| format!("cannot resolve worktree root '{}': {e}", cur.display()))?;
    if !canon_target.starts_with(&canon_cwd) {
        return Err("Default worktree root escapes the working directory".to_string());
    }
    Ok(cur)
}

pub(crate) fn tool_task_claim_in_dir(cwd: &Path, args: &Value) -> ToolCallResult {
    let Some(obj) = args.as_object() else {
        return ToolCallResult::error("Tool 'arguments' must be a JSON object");
    };
    if let Err(e) = validate_task_claim_args(obj) {
        return ToolCallResult::error(e);
    }
    let repo_dir = match check_opt_str(obj, "repo_dir").ok().flatten() {
        Some(raw) => match confine_mcp_path(cwd, raw, "repo_dir", true) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(e),
        },
        None => cwd.to_path_buf(),
    };
    let plan_path = match check_opt_str(obj, "plan_path").ok().flatten() {
        Some(raw) => match confine_mcp_path(&repo_dir, raw, "plan_path", false) {
            Ok(p) => Some(p),
            Err(e) => return ToolCallResult::error(e),
        },
        None => None,
    };
    let create_worktree = obj
        .get("create_worktree")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let worktree_root = if create_worktree {
        let raw_root = check_opt_str(obj, "worktree_root").ok().flatten();
        let base_dir = if raw_root.is_some() { cwd } else { &repo_dir };
        match ensure_confined_worktree_root(base_dir, raw_root) {
            Ok(p) => Some(p),
            Err(e) => return ToolCallResult::error(e),
        }
    } else {
        None
    };

    let ttl_minutes = match validate_ttl_prop(obj) {
        Ok(v) => v,
        Err(e) => return ToolCallResult::error(e),
    };
    let opts = ClaimOptions {
        task_id: obj["task_id"].as_str().unwrap().trim().to_string(),
        holder: obj["holder"].as_str().unwrap().trim().to_string(),
        person: obj
            .get("person")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string),
        remote: obj
            .get("remote")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_REMOTE)
            .trim()
            .to_string(),
        base: obj
            .get("base")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(ToString::to_string),
        plan_path,
        branch: obj
            .get("branch")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(ToString::to_string),
        worktree_root,
        create_worktree,
        ttl_minutes,
    };

    match claim_task(&repo_dir, &opts) {
        Ok(res) => match serde_json::to_value(&res) {
            Ok(val) => ToolCallResult::ok_json(&val),
            Err(e) => ToolCallResult::error(format!("Failed to serialize claim result: {e}")),
        },
        Err(e) => ToolCallResult::error(e),
    }
}

pub(crate) fn tool_task_renew_in_dir(cwd: &Path, args: &Value) -> ToolCallResult {
    let Some(obj) = args.as_object() else {
        return ToolCallResult::error("Tool 'arguments' must be a JSON object");
    };
    if let Err(e) = validate_task_renew_args(obj) {
        return ToolCallResult::error(e);
    }
    let repo_dir = match check_opt_str(obj, "repo_dir").ok().flatten() {
        Some(raw) => match confine_mcp_path(cwd, raw, "repo_dir", true) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(e),
        },
        None => cwd.to_path_buf(),
    };
    let ttl_minutes = match validate_ttl_prop(obj) {
        Ok(v) => v,
        Err(e) => return ToolCallResult::error(e),
    };
    let to_u32 = |k: &str| -> Result<Option<u32>, String> {
        match check_opt_u64(obj, k)? {
            Some(v) => u32::try_from(v)
                .map(Some)
                .map_err(|_| format!("Property '{k}' exceeds u32 range")),
            None => Ok(None),
        }
    };
    let verify_attempts = match to_u32("verify_attempts") {
        Ok(v) => v,
        Err(e) => return ToolCallResult::error(e),
    };
    let verify_failures = match to_u32("verify_failures") {
        Ok(v) => v,
        Err(e) => return ToolCallResult::error(e),
    };

    let opts = RenewOptions {
        task_id: obj["task_id"].as_str().unwrap().trim().to_string(),
        holder: obj["holder"].as_str().unwrap().trim().to_string(),
        remote: obj
            .get("remote")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_REMOTE)
            .trim()
            .to_string(),
        expected_oid: obj
            .get("expected_oid")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(ToString::to_string),
        ttl_minutes,
        blocked: obj.get("blocked").and_then(Value::as_bool),
        verify_attempts,
        verify_failures,
        last_failure: obj
            .get("last_failure")
            .and_then(Value::as_str)
            .map(|s| Some(s.to_string())),
        last_fingerprint: obj
            .get("last_fingerprint")
            .and_then(Value::as_str)
            .map(|s| Some(s.to_string())),
    };

    match renew_task(&repo_dir, &opts) {
        Ok(res) => match serde_json::to_value(&res) {
            Ok(val) => ToolCallResult::ok_json(&val),
            Err(e) => ToolCallResult::error(format!("Failed to serialize renew result: {e}")),
        },
        Err(e) => ToolCallResult::error(e),
    }
}

pub(crate) fn tool_task_release_in_dir(cwd: &Path, args: &Value) -> ToolCallResult {
    let Some(obj) = args.as_object() else {
        return ToolCallResult::error("Tool 'arguments' must be a JSON object");
    };
    if let Err(e) = validate_task_release_args(obj) {
        return ToolCallResult::error(e);
    }
    let repo_dir = match check_opt_str(obj, "repo_dir").ok().flatten() {
        Some(raw) => match confine_mcp_path(cwd, raw, "repo_dir", true) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(e),
        },
        None => cwd.to_path_buf(),
    };
    let opts = ReleaseOptions {
        task_id: obj["task_id"].as_str().unwrap().trim().to_string(),
        holder: obj
            .get("holder")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(ToString::to_string),
        remote: obj
            .get("remote")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_REMOTE)
            .trim()
            .to_string(),
        force: obj.get("force").and_then(Value::as_bool).unwrap_or(false),
    };

    match release_task(&repo_dir, &opts) {
        Ok(res) => match serde_json::to_value(&res) {
            Ok(val) => ToolCallResult::ok_json(&val),
            Err(e) => ToolCallResult::error(format!("Failed to serialize release result: {e}")),
        },
        Err(e) => ToolCallResult::error(e),
    }
}

pub(super) async fn tool_task_claim(args: &Value) -> ToolCallResult {
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => return ToolCallResult::error(format!("cannot determine working directory: {e}")),
    };
    let owned_args = args.clone();
    match tokio::task::spawn_blocking(move || tool_task_claim_in_dir(&cwd, &owned_args)).await {
        Ok(res) => res,
        Err(e) => ToolCallResult::error(format!("task_claim worker panicked: {e}")),
    }
}

pub(super) async fn tool_task_renew(args: &Value) -> ToolCallResult {
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => return ToolCallResult::error(format!("cannot determine working directory: {e}")),
    };
    let owned_args = args.clone();
    match tokio::task::spawn_blocking(move || tool_task_renew_in_dir(&cwd, &owned_args)).await {
        Ok(res) => res,
        Err(e) => ToolCallResult::error(format!("task_renew worker panicked: {e}")),
    }
}

pub(super) async fn tool_task_release(args: &Value) -> ToolCallResult {
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => return ToolCallResult::error(format!("cannot determine working directory: {e}")),
    };
    let owned_args = args.clone();
    match tokio::task::spawn_blocking(move || tool_task_release_in_dir(&cwd, &owned_args)).await {
        Ok(res) => res,
        Err(e) => ToolCallResult::error(format!("task_release worker panicked: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::{call_tool, validate_tool_arguments, McpState, ToolContent};
    use serde_json::json;

    #[test]
    fn test_mcp_lease_validation_refuses_traversal_and_flag_injection() {
        assert!(validate_tool_arguments(
            "task_claim",
            &json!({"task_id": "T-1", "holder": "agent-win-2"})
        )
        .is_ok());
        assert!(validate_tool_arguments(
            "task_claim",
            &json!({"task_id": "T-1", "holder": "agent-win-2", "worktree_root": "../escape"})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "task_claim",
            &json!({"task_id": "T-1", "holder": "agent-win-2", "remote": "--upload-pack=sh"})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "task_claim",
            &json!({"task_id": "T-1", "holder": "agent-win-2", "branch": "-bad"})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "task_claim",
            &json!({"task_id": "T-1", "holder": "agent-win-2", "ttl_minutes": 0})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "task_renew",
            &json!({"task_id": "T-1", "holder": "agent-win-2", "ttl_minutes": 45})
        )
        .is_ok());
        assert!(validate_tool_arguments("task_release", &json!({"task_id": "bad/id"})).is_err());
    }

    #[test]
    fn test_mcp_lease_confines_repo_dir_and_worktree_root() {
        let outer = tempfile::tempdir().unwrap();
        let cwd = outer.path().join("workspace");
        std::fs::create_dir_all(&cwd).unwrap();

        let outside = outer.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();

        let err_repo = tool_task_claim_in_dir(
            &cwd,
            &json!({
                "task_id": "T-1",
                "holder": "agent-win-2",
                "repo_dir": outside.to_string_lossy()
            }),
        );
        assert!(err_repo.is_error);

        let err_wt = tool_task_claim_in_dir(
            &cwd,
            &json!({
                "task_id": "T-1",
                "holder": "agent-win-2",
                "worktree_root": "../outside"
            }),
        );
        assert!(err_wt.is_error);
        let ToolContent::Text { text } = &err_wt.content[0];
        assert!(text.contains(".."), "{text}");
    }

    #[tokio::test]
    async fn test_call_tool_task_lease_validation_rejects_missing_required() {
        let state = McpState::new("127.0.0.1:0".to_string(), None);
        let res = call_tool(&state, "task_claim", &json!({"task_id": "T-1"})).await;
        assert!(res.is_error);
        let ToolContent::Text { text } = &res.content[0];
        assert!(text.contains("holder"), "{text}");

        let res_rel = call_tool(&state, "task_release", &json!({"force": true})).await;
        assert!(res_rel.is_error);
    }
}
