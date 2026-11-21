//! MCP `submit` tool implementation and input/output staging helpers, split out of `mcp.rs`
//! (design 0080, MAINT-2).

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{json, Value};

use rusty_grid_core::placement::{IsolationLevel, Placement, REQUIRE_PREFIX};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};

use crate::connect::save_output_artifacts;

use super::{
    McpState, ToolCallContext, ToolCallResult, ToolContent, DEFAULT_MASTER_RPC_TIMEOUT,
    DEFAULT_MCP_MAX_OUTPUT_BYTES, DEFAULT_WAIT_MARGIN_SECS,
};

fn reject_symlinks_in_dir_tree(root: &std::path::Path) -> Result<(), String> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .map_err(|e| format!("cannot read directory '{}': {e}", dir.display()))?;
        for entry in entries {
            let entry =
                entry.map_err(|e| format!("cannot read directory '{}': {e}", dir.display()))?;
            let path = entry.path();
            let ft = entry
                .file_type()
                .map_err(|e| format!("cannot inspect '{}': {e}", path.display()))?;
            if ft.is_symlink() {
                return Err(format!(
                    "'{}' is a symbolic link; symbolic links are refused in MCP submit files",
                    path.display()
                ));
            }
            if ft.is_dir() {
                stack.push(path);
            }
        }
    }
    Ok(())
}

fn rel_path_within_cwd(
    cwd: &std::path::Path,
    canon_cwd: &std::path::Path,
    path: &std::path::Path,
) -> Option<std::path::PathBuf> {
    if !path.is_absolute() {
        return Some(path.to_path_buf());
    }
    if let Ok(rel) = path
        .strip_prefix(cwd)
        .or_else(|_| path.strip_prefix(canon_cwd))
    {
        return Some(rel.to_path_buf());
    }
    let ancestors: Vec<&std::path::Path> = path.ancestors().collect();
    for anc in ancestors.into_iter().rev() {
        if let Ok(canon_anc) = std::fs::canonicalize(anc) {
            if canon_anc == *canon_cwd {
                return path
                    .strip_prefix(anc)
                    .ok()
                    .map(std::path::Path::to_path_buf);
            }
        }
    }
    None
}

/// Validates that every `LOCAL[:SANDBOX]` spec in `specs` stays inside `cwd` and neither is nor
/// traverses a symbolic link, then hashes the inputs via [`crate::blobs::scan_input_paths`].
pub fn scan_mcp_inputs_in_dir(
    cwd: &std::path::Path,
    specs: &[String],
) -> Result<crate::blobs::PreparedInputs, String> {
    use std::path::Component;

    let canon_cwd = std::fs::canonicalize(cwd)
        .map_err(|e| format!("cannot resolve working directory '{}': {e}", cwd.display()))?;

    let mut checked_specs = Vec::with_capacity(specs.len());
    for spec in specs {
        let (local, dest) = crate::blobs::parse_input_spec(spec)?;
        if local.as_os_str().is_empty() {
            return Err("input path cannot be empty".to_string());
        }
        if local
            .components()
            .any(|c| matches!(c, Component::ParentDir))
        {
            return Err(format!(
                "path '{}' must not contain '..' or leave the working directory",
                local.display()
            ));
        }

        let rel_path = rel_path_within_cwd(cwd, &canon_cwd, &local).ok_or_else(|| {
            format!(
                "path '{}' is outside the working directory '{}'",
                local.display(),
                cwd.display()
            )
        })?;

        let mut cur = cwd.to_path_buf();
        let mut saw_normal = false;
        for comp in rel_path.components() {
            match comp {
                Component::CurDir => {}
                Component::Normal(part) => {
                    saw_normal = true;
                    cur.push(part);
                    let meta = std::fs::symlink_metadata(&cur)
                        .map_err(|e| format!("'{}': {e}", local.display()))?;
                    if meta.file_type().is_symlink() {
                        return Err(format!(
                            "'{}' is or traverses a symbolic link ('{}'), which is refused in MCP submit files",
                            local.display(),
                            cur.display()
                        ));
                    }
                }
                _ => {
                    return Err(format!(
                        "path '{}' is not a relative path inside the working directory",
                        local.display()
                    ));
                }
            }
        }

        let target_meta =
            std::fs::symlink_metadata(&cur).map_err(|e| format!("'{}': {e}", local.display()))?;
        if target_meta.file_type().is_symlink() {
            return Err(format!(
                "'{}' is a symbolic link, which is refused in MCP submit files",
                local.display()
            ));
        }
        let canon_target =
            std::fs::canonicalize(&cur).map_err(|e| format!("'{}': {e}", local.display()))?;
        if !canon_target.starts_with(&canon_cwd) {
            return Err(format!(
                "path '{}' resolves outside the working directory '{}'",
                local.display(),
                cwd.display()
            ));
        }
        if target_meta.is_dir() {
            reject_symlinks_in_dir_tree(&cur)?;
        }

        let effective_dest = match dest {
            Some(d) => Some(d),
            None if saw_normal => Some(rel_path.to_string_lossy().replace('\\', "/")),
            None => Some(rusty_grid_core::blob::SANDBOX_ROOT.to_string()),
        };
        checked_specs.push((cur, effective_dest));
    }

    crate::blobs::scan_input_paths_with_limits(&checked_specs, crate::blobs::InputLimits::default())
}

/// Validates that `raw_sync_dir` stays inside `cwd`, contains no `..`, and neither is nor
/// traverses a symbolic link along the path from `cwd` to `raw_sync_dir`, then scans the
/// workspace tree via [`crate::blobs::scan_workspace_source`].
pub fn scan_mcp_workspace_in_dir(
    cwd: &std::path::Path,
    raw_sync_dir: &str,
    limits: crate::blobs::WorkspaceLimits,
) -> Result<crate::blobs::WorkspaceScan, String> {
    use std::path::Component;

    let trimmed = raw_sync_dir.trim();
    if trimmed.is_empty() {
        return Err("workspace.sync_dir cannot be empty".to_string());
    }
    let sync_path = std::path::Path::new(trimmed);
    if sync_path
        .components()
        .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(format!(
            "workspace.sync_dir '{}' must not contain '..' or leave the working directory",
            sync_path.display()
        ));
    }

    let canon_cwd = std::fs::canonicalize(cwd)
        .map_err(|e| format!("cannot resolve working directory '{}': {e}", cwd.display()))?;

    let rel_path = rel_path_within_cwd(cwd, &canon_cwd, sync_path).ok_or_else(|| {
        format!(
            "workspace.sync_dir '{}' is outside the working directory '{}'",
            sync_path.display(),
            cwd.display()
        )
    })?;

    let mut cur = cwd.to_path_buf();
    for comp in rel_path.components() {
        match comp {
            Component::CurDir => {}
            Component::Normal(part) => {
                cur.push(part);
                let meta = std::fs::symlink_metadata(&cur)
                    .map_err(|e| format!("'{}': {e}", sync_path.display()))?;
                if meta.file_type().is_symlink() {
                    return Err(format!(
                        "'{}' is or traverses a symbolic link ('{}'), which is refused in MCP workspace.sync_dir",
                        sync_path.display(),
                        cur.display()
                    ));
                }
            }
            _ => {
                return Err(format!(
                    "workspace.sync_dir '{}' is not a relative path inside the working directory",
                    sync_path.display()
                ));
            }
        }
    }

    let target_meta =
        std::fs::symlink_metadata(&cur).map_err(|e| format!("'{}': {e}", sync_path.display()))?;
    if target_meta.file_type().is_symlink() {
        return Err(format!(
            "'{}' is a symbolic link, which is refused in MCP workspace.sync_dir",
            sync_path.display()
        ));
    }
    if !target_meta.is_dir() {
        return Err(format!(
            "workspace.sync_dir '{}' is not a directory",
            sync_path.display()
        ));
    }
    let canon_target =
        std::fs::canonicalize(&cur).map_err(|e| format!("'{}': {e}", sync_path.display()))?;
    if !canon_target.starts_with(&canon_cwd) {
        return Err(format!(
            "workspace.sync_dir '{}' resolves outside the working directory '{}'",
            sync_path.display(),
            cwd.display()
        ));
    }

    crate::blobs::scan_workspace_source(&cur, limits)
}

/// Validates that `raw` stays inside `cwd`, contains no `..`, and neither is nor traverses a
/// symbolic link, creating missing directory components one by one and returning the checked
/// directory path.
pub fn prepare_mcp_output_dir_in_dir(
    cwd: &std::path::Path,
    raw: &str,
) -> Result<std::path::PathBuf, String> {
    use std::path::Component;

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("output_dir cannot be empty".to_string());
    }
    let out_path = std::path::Path::new(trimmed);
    if out_path
        .components()
        .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(format!(
            "output_dir '{}' must not contain '..' or leave the working directory",
            out_path.display()
        ));
    }

    let canon_cwd = std::fs::canonicalize(cwd)
        .map_err(|e| format!("cannot resolve working directory '{}': {e}", cwd.display()))?;

    let rel_path = rel_path_within_cwd(cwd, &canon_cwd, out_path).ok_or_else(|| {
        format!(
            "output_dir '{}' is outside the working directory '{}'",
            out_path.display(),
            cwd.display()
        )
    })?;

    let mut cur = cwd.to_path_buf();
    for comp in rel_path.components() {
        match comp {
            Component::CurDir => {}
            Component::Normal(part) => {
                cur.push(part);
                match std::fs::symlink_metadata(&cur) {
                    Ok(meta) => {
                        if meta.file_type().is_symlink() {
                            return Err(format!(
                                "output_dir '{}' is or traverses a symbolic link ('{}'), which is refused in MCP output_dir",
                                out_path.display(),
                                cur.display()
                            ));
                        }
                        if !meta.is_dir() {
                            return Err(format!(
                                "output_dir '{}': '{}' exists and is not a directory",
                                out_path.display(),
                                cur.display()
                            ));
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        std::fs::create_dir(&cur).map_err(|e| {
                            format!(
                                "output_dir '{}': cannot create directory '{}': {e}",
                                out_path.display(),
                                cur.display()
                            )
                        })?;
                        let meta = std::fs::symlink_metadata(&cur)
                            .map_err(|e| format!("output_dir '{}': {e}", out_path.display()))?;
                        if meta.file_type().is_symlink() || !meta.is_dir() {
                            return Err(format!(
                                "output_dir '{}': '{}' is not a regular directory",
                                out_path.display(),
                                cur.display()
                            ));
                        }
                    }
                    Err(e) => return Err(format!("output_dir '{}': {e}", out_path.display())),
                }
            }
            _ => {
                return Err(format!(
                    "output_dir '{}' is not a relative path inside the working directory",
                    out_path.display()
                ));
            }
        }
    }

    let target_meta =
        std::fs::symlink_metadata(&cur).map_err(|e| format!("output_dir '{}': {e}", raw))?;
    if target_meta.file_type().is_symlink() {
        return Err(format!(
            "output_dir '{}' is a symbolic link, which is refused in MCP output_dir",
            out_path.display()
        ));
    }
    if !target_meta.is_dir() {
        return Err(format!(
            "output_dir '{}': '{}' is not a directory",
            out_path.display(),
            cur.display()
        ));
    }
    let canon_target =
        std::fs::canonicalize(&cur).map_err(|e| format!("output_dir '{}': {e}", raw))?;
    if !canon_target.starts_with(&canon_cwd) {
        return Err(format!(
            "output_dir '{}' resolves outside the working directory '{}'",
            out_path.display(),
            cwd.display()
        ));
    }

    Ok(cur)
}

pub(super) const WORKER_TRUNCATION_NOTICE: &[u8] =
    b"\n[rusty_grid: output truncated after exceeding size limit]\n";

/// True when `bytes` was cut short by the worker's stream capture limit (`StreamCapture` inserts
/// [`WORKER_TRUNCATION_NOTICE`] at the exact midpoint between `head` and `tail` for any configured
/// worker cap). Short output that merely prints the marker text does not have matching non-empty
/// `head` and `tail` halves around the notice and therefore does not match.
pub fn stream_was_worker_truncated(bytes: &[u8]) -> bool {
    let notice_len = WORKER_TRUNCATION_NOTICE.len();
    if bytes.len() <= notice_len {
        return false;
    }
    let head_cap = (bytes.len() - notice_len) / 2;
    head_cap > 0
        && bytes
            .get(head_cap..head_cap + notice_len)
            .is_some_and(|s| s == WORKER_TRUNCATION_NOTICE)
}

/// Number of bytes `ch` occupies in the outer JSON-RPC response line after `ToolCallResult::ok_json`
/// encodes the tool body and `JsonRpcResponse` encodes that JSON string a second time.
fn escaped_char_bytes(ch: char) -> usize {
    match ch {
        '"' | '\\' => 4,
        '\n' | '\r' | '\t' | '\x08' | '\x0c' => 3,
        c if (c as u32) < 0x20 => 7,
        c => c.len_utf8(),
    }
}

fn escaped_str_bytes(s: &str) -> usize {
    s.chars().map(escaped_char_bytes).sum()
}

fn take_head_escaped(s: &str, max_escaped: usize) -> (&str, usize) {
    let mut used = 0usize;
    let mut end_byte = 0usize;
    for (idx, ch) in s.char_indices() {
        let cost = escaped_char_bytes(ch);
        if used + cost > max_escaped {
            break;
        }
        used += cost;
        end_byte = idx + ch.len_utf8();
    }
    (&s[..end_byte], end_byte)
}

fn take_tail_escaped(s: &str, max_escaped: usize) -> (&str, usize) {
    let mut used = 0usize;
    let mut start_byte = s.len();
    for (idx, ch) in s.char_indices().rev() {
        let cost = escaped_char_bytes(ch);
        if used + cost > max_escaped {
            break;
        }
        used += cost;
        start_byte = idx;
    }
    (&s[start_byte..], s.len() - start_byte)
}

fn split_stream_budgets(
    stdout_need: usize,
    stderr_need: usize,
    max_output_bytes: usize,
) -> (usize, usize) {
    if stdout_need.saturating_add(stderr_need) <= max_output_bytes {
        return (stdout_need, stderr_need);
    }
    let half = max_output_bytes / 2;
    let other_half = max_output_bytes - half;
    if stdout_need <= half {
        (stdout_need, max_output_bytes - stdout_need)
    } else if stderr_need <= other_half {
        (max_output_bytes - stderr_need, stderr_need)
    } else {
        (half, other_half)
    }
}

/// Formats a raw output stream (`stdout` or `stderr`) for an LLM tool reply, decoding first via
/// `String::from_utf8_lossy`, cutting at `char` boundaries within `max_output_bytes` escaped
/// bytes, and reporting `(formatted_text, was_truncated_by_mcp, captured_bytes)`.
pub fn truncate_stream_for_llm(bytes: &[u8], max_output_bytes: usize) -> (String, bool, usize) {
    let captured_bytes = bytes.len();
    let decoded = String::from_utf8_lossy(bytes);
    let total_escaped = escaped_str_bytes(&decoded);

    if total_escaped <= max_output_bytes {
        return (decoded.into_owned(), false, captured_bytes);
    }

    if max_output_bytes == 0 {
        return (String::new(), true, captured_bytes);
    }

    let worst_notice = format!("\n...[truncated {captured_bytes} bytes]...\n");
    let worst_notice_escaped = escaped_str_bytes(&worst_notice);
    if max_output_bytes <= worst_notice_escaped + 2 {
        let head_budget = max_output_bytes / 2;
        let tail_budget = max_output_bytes - head_budget;
        let (head, _) = take_head_escaped(&decoded, head_budget);
        let (tail, _) = take_tail_escaped(&decoded, tail_budget);
        return (format!("{head}{tail}"), true, captured_bytes);
    }

    let content_budget = max_output_bytes - worst_notice_escaped;
    let head_budget = content_budget / 2;
    let tail_budget = content_budget - head_budget;
    let (head, head_utf8) = take_head_escaped(&decoded, head_budget);
    let (tail, tail_utf8) = take_tail_escaped(&decoded, tail_budget);
    let kept_bytes = head_utf8.saturating_add(tail_utf8).min(captured_bytes);
    let omitted = captured_bytes.saturating_sub(kept_bytes).max(1);
    let notice = format!("\n...[truncated {omitted} bytes]...\n");
    (format!("{head}{notice}{tail}"), true, captured_bytes)
}

#[cfg(test)]
pub(super) async fn format_completed_task_result(
    state: &McpState,
    result: &rusty_grid_core::task::TaskResult,
    args: &Value,
    warning: Option<String>,
) -> ToolCallResult {
    format_completed_task_result_with_start(
        state,
        result,
        args,
        warning,
        Some(std::time::Instant::now()),
    )
    .await
}

pub(super) async fn format_completed_task_result_with_start(
    state: &McpState,
    result: &rusty_grid_core::task::TaskResult,
    args: &Value,
    warning: Option<String>,
    started_at: Option<std::time::Instant>,
) -> ToolCallResult {
    let output_dir_opt = args.get("output_dir").and_then(Value::as_str);
    let max_output_bytes = args
        .get("max_output_bytes")
        .and_then(Value::as_u64)
        .map(|n| n as usize)
        .unwrap_or(DEFAULT_MCP_MAX_OUTPUT_BYTES);

    let mut saved_outputs: Option<Vec<std::path::PathBuf>> = None;
    let mut saved_artifacts: Option<Vec<std::path::PathBuf>> = None;

    if let Some(out_dir_str) = output_dir_opt {
        let cwd = match std::env::current_dir() {
            Ok(dir) => dir,
            Err(e) => {
                return ToolCallResult::error(format!(
                    "Output directory error: cannot determine working directory: {e}"
                ))
            }
        };
        let out_dir = match prepare_mcp_output_dir_in_dir(&cwd, out_dir_str) {
            Ok(dir) => dir,
            Err(e) => return ToolCallResult::error(format!("Output directory error: {e}")),
        };
        if !result.outputs.is_empty() {
            let target_addr = match state.resolve_target_addr().await {
                Ok(addr) => addr,
                Err(e) => return ToolCallResult::error(e),
            };
            let conn = match tokio::time::timeout(
                DEFAULT_MASTER_RPC_TIMEOUT,
                crate::blobs::BlobConnection::connect(&target_addr, state.secret.as_ref()),
            )
            .await
            {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => {
                    return ToolCallResult::error(format!(
                        "Failed to connect to Master for output download: {e}"
                    ))
                }
                Err(_) => {
                    return ToolCallResult::error(format!(
                        "Timed out connecting to Master at {target_addr} for output download"
                    ))
                }
            };
            let dl_res = crate::blobs::download_outputs(&conn, &result.outputs, &out_dir).await;
            conn.close().await;
            match dl_res {
                Ok(paths) => saved_outputs = Some(paths),
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Failed to download output files to '{}': {e}",
                        out_dir.display()
                    ))
                }
            }
        }
        if !result.artifacts.is_empty() {
            match save_output_artifacts(&result.artifacts, &out_dir) {
                Ok(paths) => saved_artifacts = Some(paths),
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Failed to save output artifacts to '{}': {e}",
                        out_dir.display()
                    ))
                }
            }
        }
    }

    let mut out_artifacts: Vec<Value> = Vec::new();
    for (idx, output) in result.outputs.iter().enumerate() {
        let mut entry = json!({
            "path": output.path,
            "size_bytes": output.size,
            "blob_id": output.id.to_hex(),
            "checksum_blake3": output.id.to_hex(),
            "executable": output.executable,
        });
        if let Some(ref paths) = saved_outputs {
            if let Some(saved_path) = paths.get(idx) {
                entry["saved_path"] = Value::String(saved_path.display().to_string());
            }
        }
        out_artifacts.push(entry);
    }
    for (idx, artifact) in result.artifacts.iter().enumerate() {
        let mut entry = json!({
            "path": artifact.path,
            "size_bytes": artifact.size,
            "checksum_sha256": artifact.sha256_hex(),
        });
        if let Some(ref paths) = saved_artifacts {
            if let Some(saved_path) = paths.get(idx) {
                entry["saved_path"] = Value::String(saved_path.display().to_string());
            }
        }
        out_artifacts.push(entry);
    }

    let stdout_raw = result.stdout.as_bytes();
    let stderr_raw = result.stderr.as_bytes();
    let stdout_worker_truncated = stream_was_worker_truncated(stdout_raw);
    let stderr_worker_truncated = stream_was_worker_truncated(stderr_raw);
    let worker_truncated = stdout_worker_truncated || stderr_worker_truncated;

    let stdout_need = escaped_str_bytes(&String::from_utf8_lossy(stdout_raw));
    let stderr_need = escaped_str_bytes(&String::from_utf8_lossy(stderr_raw));
    let (stdout_budget, stderr_budget) =
        split_stream_budgets(stdout_need, stderr_need, max_output_bytes);

    let (stdout_str, stdout_mcp_truncated, stdout_captured_bytes) =
        truncate_stream_for_llm(stdout_raw, stdout_budget);
    let (stderr_str, stderr_mcp_truncated, stderr_captured_bytes) =
        truncate_stream_for_llm(stderr_raw, stderr_budget);
    let stdout_truncated = stdout_mcp_truncated || stdout_worker_truncated;
    let stderr_truncated = stderr_mcp_truncated || stderr_worker_truncated;
    let truncated = stdout_truncated || stderr_truncated;
    let captured_bytes = stdout_captured_bytes + stderr_captured_bytes;

    let status_str = if result.is_success() {
        "completed"
    } else {
        "failed"
    };

    let waited_ms: Option<u64> =
        started_at.map(|t| t.elapsed().as_millis().min(u64::MAX as u128) as u64);
    let summary = crate::run_summary::RunSummary::from_results(
        std::slice::from_ref(result),
        waited_ms.unwrap_or(0),
    );

    let mut body = json!({
        "task_id": result.task_id.to_string(),
        "status": status_str,
        "exit_code": result.exit_code,
        "cached": result.cached,
        "stdout": stdout_str,
        "stderr": stderr_str,
        "truncated": truncated,
        "worker_truncated": worker_truncated,
        "captured_bytes": captured_bytes,
        "stdout_truncated": stdout_truncated,
        "stdout_worker_truncated": stdout_worker_truncated,
        "stdout_captured_bytes": stdout_captured_bytes,
        "stderr_truncated": stderr_truncated,
        "stderr_worker_truncated": stderr_worker_truncated,
        "stderr_captured_bytes": stderr_captured_bytes,
        "duration_ms": result.execution_time_ms,
        "waited_ms": waited_ms,
        "task_time_sum_ms": summary.task_time_sum_ms,
        "machines": summary.machines,
        "error": result.error,
        "artifacts": out_artifacts,
    });
    if let Some(w) = warning {
        body["warning"] = Value::String(w);
    }
    ToolCallResult::ok_json(&body)
}

// ----------------------------------------------------------------------------
// Tool: submit
// ----------------------------------------------------------------------------

use super::submit_support::{
    attach_verify, attach_workspace_summary, check_matching_worker_warning, verify_completed,
    VerifyArgs, WorkspaceSubmitMeta,
};

pub(super) async fn tool_submit(
    state: &McpState,
    args: &Value,
    ctx: &mut ToolCallContext,
) -> ToolCallResult {
    let task_type = args
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("command")
        .to_lowercase();
    let verify = VerifyArgs::parse(args);
    let cache = args.get("cache").and_then(Value::as_bool).unwrap_or(false);
    if cache && verify.replicas.is_some() {
        return ToolCallResult::error(
            "Property 'cache' cannot be combined with 'verify': verification runs replicas directly on workers"
                .to_string(),
        );
    }
    let wait =
        verify.replicas.is_some() || args.get("wait").and_then(Value::as_bool).unwrap_or(false);

    // Environment variables
    let mut env_map = HashMap::new();
    if let Some(env_val) = args.get("env").and_then(Value::as_object) {
        for (k, v) in env_val {
            if let Some(val_str) = v.as_str() {
                env_map.insert(k.clone(), val_str.to_string());
            }
        }
    }

    // Build TaskSpec
    let spec = match task_type.as_str() {
        "gpu" => {
            let dim = args.get("matrix_dim").and_then(Value::as_u64).unwrap_or(64) as u32;
            let tile = args
                .get("work_group_size")
                .and_then(Value::as_u64)
                .unwrap_or(16) as u32;
            TaskSpec::gpu_compute_with_params("matrix_multiply", vec![1, 2, 3, 4], tile, dim)
        }
        "shell" => {
            let script = match args
                .get("script")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
            {
                Some(s) => s.to_string(),
                None => match args
                    .get("command")
                    .and_then(Value::as_str)
                    .filter(|c| !c.trim().is_empty())
                {
                    Some(c) => c.to_string(),
                    None => {
                        return ToolCallResult::error(
                            "Shell task requires a non-empty 'script' or 'command' property",
                        )
                    }
                },
            };
            let interpreter = args
                .get("interpreter")
                .and_then(Value::as_str)
                .map(str::to_string);
            TaskSpec::ShellScript {
                script,
                interpreter,
                env: env_map,
            }
        }
        "command" => {
            let program = match args
                .get("command")
                .and_then(Value::as_str)
                .filter(|c| !c.trim().is_empty())
            {
                Some(c) => c.to_string(),
                None => {
                    return ToolCallResult::error(
                        "Command task requires a non-empty 'command' property",
                    )
                }
            };

            let cmd_args: Vec<String> = args
                .get("args")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();

            let stdin = args
                .get("stdin")
                .and_then(Value::as_str)
                .map(|s| s.as_bytes().to_vec());

            let working_dir = match args
                .get("workspace")
                .and_then(Value::as_object)
                .and_then(|ws| ws.get("cwd"))
                .and_then(Value::as_str)
            {
                Some(raw_cwd) => match crate::cli_app::normalize_workspace_cwd(raw_cwd) {
                    Ok(dir) => dir,
                    Err(e) => return ToolCallResult::error(e),
                },
                None => None,
            };

            TaskSpec::Command {
                program,
                args: cmd_args,
                env: env_map,
                working_dir,
                stdin,
            }
        }
        "compile" => {
            let mut source_files = HashMap::new();
            if let Some(sf_obj) = args.get("source_files").and_then(Value::as_object) {
                for (k, v) in sf_obj {
                    if let Some(s) = v.as_str() {
                        source_files.insert(k.clone(), s.to_string());
                    }
                }
            }
            let compiler_flags: Vec<String> = args
                .get("args")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let crate_name = args
                .get("command")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .or_else(|| {
                    source_files
                        .get("Cargo.toml")
                        .and_then(|manifest| toml::from_str::<toml::Value>(manifest).ok())
                        .and_then(|val| {
                            val.get("package")?
                                .get("name")?
                                .as_str()
                                .map(|s| s.trim().to_string())
                        })
                        .filter(|s| !s.is_empty())
                })
                .unwrap_or_else(|| "crate".to_string());
            TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir: None,
            }
        }
        other => {
            return ToolCallResult::error(format!(
                "Invalid task 'type': '{other}' (expected 'command', 'shell', 'gpu', or 'compile')"
            ));
        }
    };

    // Requirements
    let cores = args.get("cores").and_then(Value::as_u64).unwrap_or(1) as usize;
    let ram_mb = args.get("ram_mb").and_then(Value::as_u64).unwrap_or(0);
    let vram_mb = args
        .get("vram_mb")
        .and_then(Value::as_u64)
        .filter(|&v| v > 0);
    let gpu = args
        .get("gpu")
        .and_then(Value::as_bool)
        .unwrap_or(task_type == "gpu")
        || vram_mb.is_some();
    let has_workspace = args.get("workspace").is_some_and(|v| !v.is_null());
    let default_timeout_secs = if has_workspace { 1800 } else { 60 };
    let timeout_secs = args
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .unwrap_or(default_timeout_secs);
    let wait_secs = args
        .get("wait_secs")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| timeout_secs.saturating_add(DEFAULT_WAIT_MARGIN_SECS));

    let mut requirements = TaskRequirements::new(cores, ram_mb, gpu, timeout_secs);
    if let Some(vram) = vram_mb {
        requirements = requirements.with_vram_mb(vram);
    }
    if let Some(retries) = args.get("max_retries").and_then(Value::as_u64) {
        requirements = requirements.with_max_retries(retries as u32);
    }

    // Placement
    let mut placement_tags = Vec::new();
    if let Some(os) = args.get("os").and_then(Value::as_str) {
        placement_tags.push(format!("{REQUIRE_PREFIX}os={os}"));
    }
    if let Some(arch) = args.get("arch").and_then(Value::as_str) {
        placement_tags.push(format!("{REQUIRE_PREFIX}arch={arch}"));
    }
    if let Some(worker) = args.get("worker").and_then(Value::as_str) {
        placement_tags.push(format!("{REQUIRE_PREFIX}worker={worker}"));
    }
    if let Some(level) = args.get("isolation").and_then(Value::as_str) {
        match IsolationLevel::parse_required(level) {
            Ok(level) => placement_tags.push(format!("{REQUIRE_PREFIX}iso={level}")),
            Err(e) => return ToolCallResult::error(format!("Invalid placement constraints: {e}")),
        }
    }
    if let Some(tags_arr) = args.get("require_tags").and_then(Value::as_array) {
        for t in tags_arr.iter().filter_map(Value::as_str) {
            placement_tags.push(format!("{REQUIRE_PREFIX}tag={t}"));
        }
    }
    if let Some(tools_arr) = args.get("tools").and_then(Value::as_array) {
        for t in tools_arr.iter().filter_map(Value::as_str) {
            placement_tags.push(format!("{REQUIRE_PREFIX}tool={t}"));
        }
    }

    let mut placement = match Placement::from_task_tags(&placement_tags) {
        Ok(p) => p,
        Err(e) => return ToolCallResult::error(format!("Invalid placement constraints: {e}")),
    };

    // Validate output_dir upfront if provided
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

    // Staged files (content-addressed blobs, restricted to non-symlink paths inside the working directory)
    let mut prepared_inputs = crate::blobs::PreparedInputs::default();
    if let Some(files_arr) = args.get("files").and_then(Value::as_array) {
        let string_paths: Vec<String> = files_arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        if !string_paths.is_empty() {
            let cwd = match std::env::current_dir() {
                Ok(dir) => dir,
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Staged files error: cannot determine working directory: {e}"
                    ))
                }
            };
            let scan_res =
                tokio::task::spawn_blocking(move || scan_mcp_inputs_in_dir(&cwd, &string_paths))
                    .await
                    .map_err(|e| format!("input scan task failed: {e}"))
                    .and_then(|r| r);
            match scan_res {
                Ok(inputs) => prepared_inputs = inputs,
                Err(e) => return ToolCallResult::error(format!("Staged files error: {e}")),
            }
        }
    }

    if let TaskSpec::RustCompilation { source_files, .. } = &spec {
        if placement.tools.is_empty() {
            let has_cargo_manifest = source_files.contains_key("Cargo.toml")
                || prepared_inputs
                    .inputs
                    .iter()
                    .any(|inp| inp.path == "Cargo.toml");
            let default_tool = if has_cargo_manifest { "cargo" } else { "rustc" };
            placement.tools.push(default_tool.to_string());
        }
    }

    // Expected outputs (blob output globs)
    let mut output_globs = Vec::new();
    if let Some(outputs_arr) = args.get("outputs").and_then(Value::as_array) {
        let string_outputs: Vec<String> = outputs_arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        if !string_outputs.is_empty() {
            if let Err(e) = rusty_grid_core::blob::validate_output_patterns(&string_outputs) {
                return ToolCallResult::error(format!("Expected outputs error: {e}"));
            }
            output_globs = string_outputs;
        }
    }

    if let Some(replicas) = verify.replicas {
        let pinned = placement.worker.is_some();
        if let Err(e) =
            crate::verify::check_options(replicas, verify.file, !output_globs.is_empty(), pinned)
        {
            return ToolCallResult::error(e);
        }
        if let Some(f) = verify.file.filter(|f| !output_globs.iter().any(|g| g == f)) {
            output_globs.push(f.to_string());
        }
    }

    // Persistent workspace sync (mutually exclusive with `files`)
    let mut ws_meta: Option<WorkspaceSubmitMeta> = None;
    let mut ws_uploads: Option<crate::blobs::PreparedInputs> = None;
    if let Some(ws_obj) = args.get("workspace").and_then(Value::as_object) {
        let ws_name = match ws_obj.get("name").and_then(Value::as_str) {
            Some(n) => n.to_string(),
            None => return ToolCallResult::error("Missing required property 'workspace.name'"),
        };
        if let Err(e) = rusty_grid_core::task::validate_workspace_name(&ws_name) {
            return ToolCallResult::error(e);
        }
        let sync_dir_opt: Option<String> = match ws_obj.get("sync_dir") {
            None => Some(".".to_string()),
            Some(Value::Null) => None,
            Some(v) => match v.as_str() {
                Some(s) => Some(s.to_string()),
                None => {
                    return ToolCallResult::error("Property 'workspace.sync_dir' must be a string")
                }
            },
        };
        if let Some(sync_dir_str) = sync_dir_opt {
            let cwd = match std::env::current_dir() {
                Ok(dir) => dir,
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Workspace sync error: cannot determine working directory: {e}"
                    ))
                }
            };
            let limits = state.workspace_limits;
            let scan_res = tokio::task::spawn_blocking(move || {
                scan_mcp_workspace_in_dir(&cwd, &sync_dir_str, limits)
            })
            .await
            .map_err(|e| format!("workspace scan task failed: {e}"))
            .and_then(|r| r);
            let prepared_ws = match scan_res {
                Ok(ws) => ws,
                Err(e) => return ToolCallResult::error(format!("Workspace sync error: {e}")),
            };
            let files = prepared_ws.prepared.files;
            let skipped = prepared_ws.prepared.skipped.len();
            let skipped_entries = prepared_ws.prepared.skipped.clone();
            let manifest_id = prepared_ws.manifest_id;
            let manifest_len = prepared_ws.manifest_len;
            ws_uploads = Some(prepared_ws.prepared);
            ws_meta = Some(WorkspaceSubmitMeta {
                name: ws_name.clone(),
                spec: rusty_grid_core::task::WorkspaceRef::new(ws_name)
                    .with_tree(manifest_id, manifest_len),
                files,
                uploaded_bytes: 0,
                skipped,
                skipped_entries,
            });
        } else {
            ws_meta = Some(WorkspaceSubmitMeta {
                name: ws_name.clone(),
                spec: rusty_grid_core::task::WorkspaceRef::new(ws_name),
                files: 0,
                uploaded_bytes: 0,
                skipped: 0,
                skipped_entries: Vec::new(),
            });
        }
    }

    let mut task = Task::new(spec, requirements.clone()).with_placement(&placement);
    if !prepared_inputs.inputs.is_empty() {
        task = task.with_inputs(prepared_inputs.inputs.clone());
    }
    if let Some(ref meta) = ws_meta {
        task = task.with_workspace(meta.spec.clone());
    }
    if !output_globs.is_empty() {
        task = task.with_output_globs(output_globs);
    }
    if cache {
        task = task.with_action_cache();
        if let Err(e) = task.validate() {
            return ToolCallResult::error(format!("Invalid cached submission: {e}"));
        }
    }
    let task_id = task.id;
    let verify_template = verify.replicas.map(|_| task.clone());
    *ctx.submitted_task_id.lock().await = Some(task_id);

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
    }

    let submit_started = std::time::Instant::now();

    let target_addr = match state.resolve_target_addr().await {
        Ok(addr) => addr,
        Err(e) => return ToolCallResult::error(e),
    };

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
    }

    let blob_connect_timeout =
        Duration::from_secs(wait_secs.max(1)).min(DEFAULT_MASTER_RPC_TIMEOUT);

    if !prepared_inputs.inputs.is_empty() {
        let conn = match tokio::time::timeout(
            blob_connect_timeout,
            crate::blobs::BlobConnection::connect(&target_addr, state.secret.as_ref()),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                return ToolCallResult::error(format!(
                    "Failed to connect to Master for input blob upload: {e}"
                ))
            }
            Err(_) => {
                return ToolCallResult::error(format!(
                    "Timed out connecting to Master at {target_addr} for input blob upload"
                ))
            }
        };
        let up_res = crate::blobs::upload_inputs(&conn, &prepared_inputs).await;
        conn.close().await;
        if let Err(e) = up_res {
            return ToolCallResult::error(format!("Failed to upload input files: {e}"));
        }
    }

    if let Some(ref uploads) = ws_uploads {
        let conn = match tokio::time::timeout(
            blob_connect_timeout,
            crate::blobs::BlobConnection::connect(&target_addr, state.secret.as_ref()),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                return ToolCallResult::error(format!(
                    "Failed to connect to Master for workspace blob upload: {e}"
                ))
            }
            Err(_) => {
                return ToolCallResult::error(format!(
                    "Timed out connecting to Master at {target_addr} for workspace blob upload"
                ))
            }
        };
        let ws_up_res = crate::blobs::upload_inputs(&conn, uploads).await;
        conn.close().await;
        match ws_up_res {
            Ok(stats) => {
                if let Some(ref mut meta) = ws_meta {
                    meta.uploaded_bytes = stats.bytes;
                }
            }
            Err(e) => {
                return ToolCallResult::error(format!("Failed to upload workspace files: {e}"))
            }
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_secs);
    let preflight_timeout =
        Duration::from_millis(500).min(Duration::from_secs(wait_secs.max(1)) / 2);
    let warning = check_matching_worker_warning(
        state,
        &placement,
        &requirements,
        ws_meta.is_some(),
        preflight_timeout,
    )
    .await;

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
    }

    if wait {
        let submit_rpc_timeout =
            Duration::from_secs(wait_secs.max(1)).min(DEFAULT_MASTER_RPC_TIMEOUT);
        let mut transport = match state
            .connect_dedicated_with_timeout(submit_rpc_timeout)
            .await
        {
            Ok(t) => t,
            Err(e) => return ToolCallResult::error(e),
        };

        let msg = ClientMessage::SubmitTask { task, wait: true };
        if let Err(e) = transport
            .send_msg_with_idle_timeout(&msg, submit_rpc_timeout)
            .await
        {
            return if matches!(e, rusty_grid_core::protocol::ProtocolError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::TimedOut)
            {
                ToolCallResult::error(format!(
                    "Timed out submitting task to Master at {}",
                    state.master_addr
                ))
            } else {
                ToolCallResult::error(format!("Failed to submit task: {e}"))
            };
        }

        state.record_submitted(task_id, submit_started).await;
        ctx.send_progress(
            0,
            None,
            format!("Task {task_id} submitted; waiting for completion"),
        );

        let mut cancel_rx = ctx.cancel_rx.clone();
        let mut cancel_closed = false;
        let mut progress_tick = tokio::time::interval(Duration::from_millis(500));
        // Skip immediate first tick since we already sent progress 0 above.
        progress_tick.tick().await;
        let mut step = 0u64;

        let recv_outcome = loop {
            if *cancel_rx.borrow() {
                ctx.aborted_by_cancel = true;
                drop(transport);
                let _ = state
                    .request_with_timeout(
                        &ClientMessage::CancelTask { task_id },
                        Duration::from_millis(500),
                    )
                    .await;
                return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
            }

            tokio::select! {
                biased;
                res = cancel_rx.changed(), if !cancel_closed => {
                    if res.is_err() {
                        cancel_closed = true;
                        continue;
                    }
                    if *cancel_rx.borrow() {
                        ctx.aborted_by_cancel = true;
                        drop(transport);
                        let _ = state
                            .request_with_timeout(
                                &ClientMessage::CancelTask { task_id },
                                Duration::from_millis(500),
                            )
                            .await;
                        return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
                    }
                }
                res = transport.recv_msg::<ClientResponse>() => {
                    break Some(res);
                }
                _ = tokio::time::sleep_until(deadline) => {
                    break None;
                }
                _ = progress_tick.tick() => {
                    step += 1;
                    ctx.send_progress(step, None, format!("Waiting for task {task_id}"));
                }
            }
        };

        match recv_outcome {
            Some(Ok(Some(ClientResponse::TaskCompleted { result, .. }))) => {
                ctx.send_progress(
                    step + 1,
                    Some(step + 1),
                    format!("Task {task_id} completed"),
                );
                let worker_id = result.worker_id;
                let res = format_completed_task_result_with_start(
                    state,
                    &result,
                    args,
                    warning,
                    Some(submit_started),
                )
                .await;
                let verify_outcome = match (verify.replicas, &verify_template) {
                    (Some(n), Some(tpl)) if result.is_success() => {
                        Some(verify_completed(state, tpl, &placement, &result, n, verify).await)
                    }
                    _ => None,
                };
                let res = attach_verify(res, verify_outcome);
                attach_workspace_summary(res, ws_meta.as_ref(), Some(worker_id))
            }
            Some(Ok(Some(ClientResponse::Error { message }))) => ToolCallResult::error(message),
            Some(Ok(Some(other))) => {
                ToolCallResult::error(format!("Unexpected Master response: {other:?}"))
            }
            Some(Ok(None)) => {
                ToolCallResult::error("Master closed connection while waiting for task")
            }
            Some(Err(e)) => ToolCallResult::error(format!("Communication error: {e}")),
            None => {
                drop(transport);
                let (status_str, state_name) = match state
                    .request_with_timeout(
                        &ClientMessage::GetTaskStatus { task_id },
                        Duration::from_millis(500),
                    )
                    .await
                {
                    Ok(ClientResponse::TaskStatusInfo {
                        status, state_name, ..
                    }) => (format!("{:?}", status), state_name),
                    _ => ("Unknown".to_string(), "unknown".to_string()),
                };
                let mut body = json!({
                    "task_id": task_id.to_string(),
                    "status": status_str,
                    "state": state_name,
                    "timed_out": true,
                });
                if let Some(w) = warning {
                    body["warning"] = Value::String(w);
                }
                attach_workspace_summary(ToolCallResult::ok_json(&body), ws_meta.as_ref(), None)
            }
        }
    } else {
        // Submit asynchronously
        let msg = ClientMessage::SubmitTask { task, wait: false };
        let submit_timeout = args
            .get("wait_secs")
            .and_then(Value::as_u64)
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_MASTER_RPC_TIMEOUT);
        let remaining = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .min(submit_timeout)
            .max(Duration::from_millis(100));

        match state.request_with_timeout(&msg, remaining).await {
            Ok(ClientResponse::TaskSubmitted { task_id }) => {
                if ctx.is_cancelled() {
                    ctx.aborted_by_cancel = true;
                    let _ = state
                        .request_with_timeout(
                            &ClientMessage::CancelTask { task_id },
                            Duration::from_millis(500),
                        )
                        .await;
                    return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
                }
                state.record_submitted(task_id, submit_started).await;
                ctx.send_progress(1, Some(1), format!("Task {task_id} submitted"));
                let mut body = json!({
                    "task_id": task_id.to_string(),
                    "status": "submitted",
                });
                if let Some(w) = warning {
                    body["warning"] = Value::String(w);
                }
                attach_workspace_summary(ToolCallResult::ok_json(&body), ws_meta.as_ref(), None)
            }
            Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
            Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
            Err(e)
                if e.starts_with("Timed out waiting for response")
                    || e == "Master closed connection unexpectedly"
                    || e.starts_with("Failed to receive response from Master") =>
            {
                if ctx.is_cancelled() {
                    ctx.aborted_by_cancel = true;
                    let _ = state
                        .request_with_timeout(
                            &ClientMessage::CancelTask { task_id },
                            Duration::from_millis(500),
                        )
                        .await;
                    return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
                }
                let (status_str, state_name, is_error, detail) = match state
                    .request_with_timeout(
                        &ClientMessage::GetTaskStatus { task_id },
                        Duration::from_millis(500),
                    )
                    .await
                {
                    Ok(ClientResponse::TaskStatusInfo {
                        status, state_name, ..
                    }) => {
                        state.record_submitted(task_id, submit_started).await;
                        (
                            format!("{:?}", status),
                            state_name,
                            false,
                            format!(
                                "Task {task_id} was submitted ({e}); query its outcome with task_result or status"
                            ),
                        )
                    }
                    Ok(ClientResponse::Error { message }) if message.contains("not found") => {
                        // A frame written before a receive timeout may still be in flight or may
                        // have been lost when the pooled connection closed. Resend the same
                        // `SubmitTask` (with the same `task_id`) once on a fresh connection: the
                        // Master deduplicates by `task_id`, so treating `already exists` as
                        // submitted guarantees at most one task is queued.
                        match state
                            .request_with_timeout(&msg, DEFAULT_MASTER_RPC_TIMEOUT)
                            .await
                        {
                            Ok(ClientResponse::TaskSubmitted { task_id }) => {
                                if ctx.is_cancelled() {
                                    ctx.aborted_by_cancel = true;
                                    let _ = state
                                        .request_with_timeout(
                                            &ClientMessage::CancelTask { task_id },
                                            Duration::from_millis(500),
                                        )
                                        .await;
                                    return ToolCallResult::error(format!(
                                        "Task {task_id} cancelled by client"
                                    ));
                                }
                                state.record_submitted(task_id, submit_started).await;
                                ctx.send_progress(1, Some(1), format!("Task {task_id} submitted"));
                                let mut body = json!({
                                    "task_id": task_id.to_string(),
                                    "status": "submitted",
                                });
                                if let Some(w) = warning {
                                    body["warning"] = Value::String(w);
                                }
                                return attach_workspace_summary(
                                    ToolCallResult::ok_json(&body),
                                    ws_meta.as_ref(),
                                    None,
                                );
                            }
                            Ok(ClientResponse::Error { ref message })
                                if message.contains("already exists") =>
                            {
                                if ctx.is_cancelled() {
                                    ctx.aborted_by_cancel = true;
                                    let _ = state
                                        .request_with_timeout(
                                            &ClientMessage::CancelTask { task_id },
                                            Duration::from_millis(500),
                                        )
                                        .await;
                                    return ToolCallResult::error(format!(
                                        "Task {task_id} cancelled by client"
                                    ));
                                }
                                state.record_submitted(task_id, submit_started).await;
                                ctx.send_progress(1, Some(1), format!("Task {task_id} submitted"));
                                let mut body = json!({
                                    "task_id": task_id.to_string(),
                                    "status": "submitted",
                                });
                                if let Some(w) = warning {
                                    body["warning"] = Value::String(w);
                                }
                                return attach_workspace_summary(
                                    ToolCallResult::ok_json(&body),
                                    ws_meta.as_ref(),
                                    None,
                                );
                            }
                            Ok(ClientResponse::Error { message }) => {
                                return ToolCallResult::error(message);
                            }
                            _ => (
                                "Unknown".to_string(),
                                "unknown".to_string(),
                                false,
                                format!(
                                    "Task {task_id} may have been submitted ({e}); query its outcome with task_result or status"
                                ),
                            ),
                        }
                    }
                    _ => (
                        "Unknown".to_string(),
                        "unknown".to_string(),
                        false,
                        format!(
                            "Task {task_id} may have been submitted ({e}); query its outcome with task_result or status"
                        ),
                    ),
                };
                let mut body = json!({
                    "task_id": task_id.to_string(),
                    "status": status_str,
                    "state": state_name,
                    "timed_out": true,
                    "message": detail,
                });
                if let Some(w) = warning {
                    body["warning"] = Value::String(w);
                }
                let res = ToolCallResult {
                    content: vec![ToolContent::Text {
                        text: serde_json::to_string_pretty(&body)
                            .unwrap_or_else(|_| body.to_string()),
                    }],
                    is_error,
                };
                attach_workspace_summary(res, ws_meta.as_ref(), None)
            }
            Err(e) => ToolCallResult::error(e),
        }
    }
}
