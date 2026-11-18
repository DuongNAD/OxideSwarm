//! Helpers of the MCP `submit` tool (workspace summary, worker preflight), split out of `mcp.rs`
//! to keep that file under its size allowance (design 0080).

use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;

use rusty_grid_core::placement::{
    listed_supports_workspaces, tag_value, Placement, STATE_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::TaskRequirements;

use super::{McpState, ToolCallResult, ToolContent};

pub(super) struct WorkspaceSubmitMeta {
    pub(super) name: String,
    pub(super) spec: rusty_grid_core::task::WorkspaceRef,
    pub(super) files: usize,
    pub(super) uploaded_bytes: u64,
    pub(super) skipped: usize,
    pub(super) skipped_entries: Vec<String>,
}

pub(super) fn attach_workspace_summary(
    res: ToolCallResult,
    ws_meta: Option<&WorkspaceSubmitMeta>,
    worker: Option<Uuid>,
) -> ToolCallResult {
    let Some(meta) = ws_meta else {
        return res;
    };
    if res.is_error {
        return res;
    }
    let Some(ToolContent::Text { text }) = res.content.first() else {
        return res;
    };
    let Ok(mut body) = serde_json::from_str::<Value>(text) else {
        return res;
    };
    let mut ws_summary = json!({
        "name": meta.name,
        "worker": worker.filter(|w| !w.is_nil()).map(|w| w.to_string()),
        "files": meta.files,
        "uploaded_bytes": meta.uploaded_bytes,
        "skipped": meta.skipped,
    });
    if !meta.skipped_entries.is_empty() {
        ws_summary["skipped_entries"] = json!(meta.skipped_entries);
    }
    body["workspace"] = ws_summary;
    ToolCallResult::ok_json(&body)
}

pub(super) async fn check_matching_worker_warning(
    state: &McpState,
    placement: &Placement,
    requirements: &TaskRequirements,
    has_workspace: bool,
    timeout: Duration,
) -> Option<String> {
    let Ok(ClientResponse::WorkerList { workers }) = state
        .request_with_timeout(&ClientMessage::ListWorkers, timeout)
        .await
    else {
        return None;
    };

    if has_workspace
        && !workers.iter().any(|w| {
            let connected = tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected");
            connected && listed_supports_workspaces(w)
        })
    {
        let msg =
            "Warning: no connected worker supports workspaces yet; the task will wait".to_string();
        eprintln!("[mcp] {msg}");
        return Some(msg);
    }

    let req_vram = requirements.effective_vram_mb();
    let any_matches = workers.iter().any(|w| {
        let connected = tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected");
        connected
            && placement.matches_listed(w)
            && (!requirements.requires_gpu_or_vram() || w.can_execute_gpu())
            && w.cpu_cores >= requirements.cpu_cores
            && w.ram_mb >= requirements.ram_mb
            && (req_vram == 0 || w.vram_mb.is_some_and(|v| v >= req_vram))
    });

    if any_matches {
        None
    } else {
        let msg = if !placement.is_empty() {
            format!(
                "Warning: no connected worker matches the placement constraints ({placement}); the task waits in the queue until one does"
            )
        } else {
            "Warning: no connected worker matches the task resource requirements; the task waits in the queue until one does".to_string()
        };
        eprintln!("[mcp] {msg}");
        Some(msg)
    }
}

/// The `verify`, `verify_file` and `verify_any_system` arguments of `submit` (design 0089 D3, D6).
#[derive(Clone, Copy)]
pub(super) struct VerifyArgs<'a> {
    /// The number of replicas; `None` when the task is not verified.
    pub(super) replicas: Option<usize>,
    pub(super) file: Option<&'a str>,
    /// Replicas may run on another operating system or CPU architecture (default: they do not).
    pub(super) any_system: bool,
}

impl<'a> VerifyArgs<'a> {
    pub(super) fn parse(args: &'a Value) -> Self {
        Self {
            replicas: args
                .get("verify")
                .and_then(Value::as_u64)
                .map(|n| n as usize),
            file: args.get("verify_file").and_then(Value::as_str),
            any_system: args
                .get("verify_any_system")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }

    /// Checks the types of the three arguments (part of the `submit` argument validation).
    pub(super) fn check(obj: &serde_json::Map<String, Value>) -> Result<(), String> {
        super::check_opt_u64(obj, "verify")?;
        super::check_opt_str(obj, "verify_file")?;
        super::check_opt_bool(obj, "verify_any_system")
    }
}

/// `submit` with `verify`: runs the completed task again on other workers (design 0089 D3), on
/// the first worker's operating system and architecture unless `verify_any_system` (D6).
pub(super) async fn verify_completed(
    state: &McpState,
    template: &rusty_grid_core::task::Task,
    placement: &Placement,
    first: &rusty_grid_core::task::TaskResult,
    replicas: usize,
    args: VerifyArgs<'_>,
) -> Result<crate::verify::Report, String> {
    let timeout = Duration::from_secs(template.requirements.timeout_secs.saturating_add(30));
    let workers = match state
        .request_with_timeout(
            &ClientMessage::ListWorkers,
            super::DEFAULT_MASTER_RPC_TIMEOUT,
        )
        .await
    {
        Ok(ClientResponse::WorkerList { workers }) => workers,
        _ => return Err("could not list the workers".to_string()),
    };
    let first_name = workers
        .iter()
        .find(|w| rusty_grid_core::placement::listed_worker_id(w) == Some(first.worker_id))
        .map(|w| w.name.clone());
    crate::verify::verify_on_workers(
        crate::verify::Request {
            workers: &workers,
            template,
            placement,
            first,
            first_name,
            replicas,
            file: args.file,
            any_system: args.any_system,
        },
        || state.connect_dedicated_with_timeout(timeout),
    )
    .await
}

/// Adds the `verify` object to a completed `submit` reply; a mismatch or an error makes the reply
/// an error result (the CLI's non-zero exit).
pub(super) fn attach_verify(
    res: ToolCallResult,
    outcome: Option<Result<crate::verify::Report, String>>,
) -> ToolCallResult {
    let Some(outcome) = outcome else {
        return res;
    };
    let Some(ToolContent::Text { text }) = res.content.first() else {
        return res;
    };
    let Ok(mut body) = serde_json::from_str::<Value>(text) else {
        return res;
    };
    let failed = match &outcome {
        Ok(report) => {
            body["verify"] = report.to_json();
            !report.verified()
        }
        Err(e) => {
            body["verify"] = json!({"status": "error", "error": e});
            true
        }
    };
    let mut out = ToolCallResult::ok_json(&body);
    out.is_error = res.is_error || failed;
    out
}
