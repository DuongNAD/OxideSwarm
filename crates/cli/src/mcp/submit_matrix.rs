//! MCP `submit` `matrix` fan-out handler (design 0101, AX-1).

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::mpsc;

use rusty_grid_core::placement::Placement;
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::Task;

use crate::blobs::{PreparedInputs, UploadReport};
use crate::connect::{connect_master, save_output_artifacts};
use crate::matrix::{
    check_placeability, execute_matrix, expand_cells, MatrixProgressEvent, MatrixReport,
    MatrixSpec, DEFAULT_MAX_MATRIX_CELLS,
};

use super::submit::prepare_mcp_output_dir_in_dir;
use super::submit_support::WorkspaceSubmitMeta;
use super::{McpState, ToolCallContext, ToolCallResult, DEFAULT_MASTER_RPC_TIMEOUT};

fn ensure_non_symlink_cell_dir(
    base_out_dir: &std::path::Path,
    slug: &str,
) -> Result<std::path::PathBuf, String> {
    let cell_dir = base_out_dir.join(slug);
    match std::fs::symlink_metadata(&cell_dir) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(format!(
                    "cell output directory '{}' is a symbolic link, which is refused in MCP output_dir",
                    cell_dir.display()
                ));
            }
            if !meta.is_dir() {
                return Err(format!(
                    "cell output path '{}' exists and is not a directory",
                    cell_dir.display()
                ));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(format!(
                "cannot inspect cell output directory '{}': {e}",
                cell_dir.display()
            ));
        }
    }
    Ok(cell_dir)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_mcp_matrix(
    state: &McpState,
    args: &Value,
    ctx: &mut ToolCallContext,
    matrix_val: &Value,
    template: Task,
    base_placement: &Placement,
    allow_env_axes: bool,
    prepared_inputs: PreparedInputs,
    ws_uploads: Option<PreparedInputs>,
    mut ws_meta: Option<WorkspaceSubmitMeta>,
    wait_secs: u64,
    max_output_bytes: usize,
    submit_started: Instant,
) -> ToolCallResult {
    let max_cells = args
        .get("max_matrix_cells")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(DEFAULT_MAX_MATRIX_CELLS);

    let spec = match MatrixSpec::parse_mcp_value(matrix_val, max_cells) {
        Ok(s) => s,
        Err(e) => return ToolCallResult::error(e),
    };
    let cells = match expand_cells(&spec, base_placement, allow_env_axes) {
        Ok(c) => c,
        Err(e) => return ToolCallResult::error(e),
    };

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error("Matrix submission cancelled by client");
    }

    let target_addr = match state.resolve_target_addr().await {
        Ok(addr) => addr,
        Err(e) => return ToolCallResult::error(e),
    };

    let rpc_timeout = Duration::from_secs(wait_secs.clamp(2, 10)).max(DEFAULT_MASTER_RPC_TIMEOUT);
    let workers = match state
        .request_with_timeout(&ClientMessage::ListWorkers, rpc_timeout)
        .await
    {
        Ok(ClientResponse::WorkerList { workers }) => workers,
        Ok(ClientResponse::Error { message }) => {
            return ToolCallResult::error(format!("Failed to list workers: {message}"))
        }
        Ok(other) => {
            return ToolCallResult::error(format!(
                "Unexpected Master response to ListWorkers: {other:?}"
            ))
        }
        Err(e) => return ToolCallResult::error(format!("Failed to list workers: {e}")),
    };

    let placeability =
        check_placeability(&cells, &workers, &template.requirements, ws_meta.is_some());
    let placeable_count = placeability.iter().filter(|r| r.is_none()).count();
    let total_cells = cells.len();

    let mut input_upload: Option<UploadReport> = None;
    if placeable_count > 0 {
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
            match up_res {
                Ok(stats) => input_upload = Some(stats),
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to upload input files: {e}"))
                }
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
    }

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error("Matrix submission cancelled by client");
    }

    ctx.send_progress(
        0,
        Some(total_cells as u64),
        format!("Starting matrix submission ({placeable_count}/{total_cells} cell(s) placeable)"),
    );

    let (prog_tx, mut prog_rx) = mpsc::unbounded_channel::<MatrixProgressEvent>();
    let addr_arc = Arc::new(target_addr.clone());
    let secret_arc = Arc::new(state.secret.clone());
    let connect = Arc::new(move || {
        let addr = Arc::clone(&addr_arc);
        let sec = Arc::clone(&secret_arc);
        async move {
            connect_master(&addr, sec.as_ref().as_ref())
                .await
                .map_err(|e| format!("Failed to connect to Master at {addr}: {e}"))
        }
    });

    let cancel_rx = ctx.cancel_rx.clone();
    let exec_fut = execute_matrix(
        cells,
        &workers,
        placeability,
        &template,
        Duration::from_secs(wait_secs.max(1)),
        Some(cancel_rx),
        Some(prog_tx),
        connect,
    );

    let progress_fut = async {
        let mut completed = 0u64;
        while let Some(ev) = prog_rx.recv().await {
            match ev {
                MatrixProgressEvent::Submitted { task_id, .. } => {
                    state.record_submitted(task_id, submit_started).await;
                }
                MatrixProgressEvent::Finished { label, status, .. } => {
                    completed += 1;
                    ctx.send_progress(
                        completed,
                        Some(total_cells as u64),
                        format!("Cell {label} -> {}", status.as_str()),
                    );
                }
            }
        }
    };

    let (mut executions, ()) = tokio::join!(exec_fut, progress_fut);

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error("Matrix submission cancelled by client");
    }

    if let Some(out_dir_str) = args.get("output_dir").and_then(Value::as_str) {
        let cwd = match std::env::current_dir() {
            Ok(dir) => dir,
            Err(e) => {
                return ToolCallResult::error(format!(
                    "Output directory error: cannot determine working directory: {e}"
                ))
            }
        };
        let base_out_dir = match prepare_mcp_output_dir_in_dir(&cwd, out_dir_str) {
            Ok(dir) => dir,
            Err(e) => return ToolCallResult::error(format!("Output directory error: {e}")),
        };

        let any_outputs = executions
            .iter()
            .any(|e| e.result.as_ref().is_some_and(|r| !r.outputs.is_empty()));
        if any_outputs {
            let dl_timeout = Duration::from_secs(wait_secs.max(1)).min(DEFAULT_MASTER_RPC_TIMEOUT);
            match tokio::time::timeout(
                dl_timeout,
                crate::blobs::BlobConnection::connect(&target_addr, state.secret.as_ref()),
            )
            .await
            {
                Ok(Ok(conn)) => {
                    for exec in &mut executions {
                        let Some(ref result) = exec.result else {
                            continue;
                        };
                        if result.outputs.is_empty() {
                            continue;
                        }
                        let cell_dir =
                            match ensure_non_symlink_cell_dir(&base_out_dir, &exec.cell.slug) {
                                Ok(dir) => dir,
                                Err(e) => {
                                    exec.download_error = Some(e);
                                    continue;
                                }
                            };
                        match crate::blobs::download_outputs(&conn, &result.outputs, &cell_dir)
                            .await
                        {
                            Ok(paths) => exec.saved_outputs = Some(paths),
                            Err(e) => {
                                exec.download_error =
                                    Some(format!("Failed to download output files: {e}"));
                            }
                        }
                    }
                    conn.close().await;
                }
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
            }
        }

        for exec in &mut executions {
            let Some(ref result) = exec.result else {
                continue;
            };
            if !result.artifacts.is_empty() {
                let cell_dir = match ensure_non_symlink_cell_dir(&base_out_dir, &exec.cell.slug) {
                    Ok(dir) => dir,
                    Err(e) => {
                        exec.download_error.get_or_insert(e);
                        continue;
                    }
                };
                match save_output_artifacts(&result.artifacts, &cell_dir) {
                    Ok(paths) => exec.saved_artifacts = Some(paths),
                    Err(e) => {
                        exec.download_error
                            .get_or_insert_with(|| format!("Failed to save artifacts: {e}"));
                    }
                }
            }
        }
    }

    let workspace_summary = ws_meta.as_ref().map(|meta| {
        let mut ws = json!({
            "name": meta.name,
            "files": meta.files,
            "uploaded_bytes": meta.uploaded_bytes,
            "skipped": meta.skipped,
        });
        if !meta.skipped_entries.is_empty() {
            ws["skipped_entries"] = json!(meta.skipped_entries);
        }
        ws
    });

    let executed_count = executions
        .iter()
        .filter(|e| e.result.is_some())
        .count()
        .max(1);
    let per_cell_max_bytes = if max_output_bytes == 0 {
        0
    } else {
        (max_output_bytes / executed_count)
            .max(512)
            .min(max_output_bytes)
    };

    let waited_ms = submit_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let report = MatrixReport::from_executions(
        executions,
        waited_ms,
        per_cell_max_bytes,
        input_upload,
        workspace_summary,
    );

    let is_error = report.any_failed();
    let mut out = ToolCallResult::ok_json(&report.to_json());
    out.is_error = is_error;
    out
}

#[cfg(all(test, unix))]
mod tests {
    use super::ensure_non_symlink_cell_dir;

    #[test]
    fn rejects_preexisting_symlink_cell_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let base_out = tmp.path().join("out");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&base_out).unwrap();
        std::fs::create_dir_all(&outside).unwrap();

        let slug = "cell-00-a";
        std::os::unix::fs::symlink(&outside, base_out.join(slug)).unwrap();

        let err = ensure_non_symlink_cell_dir(&base_out, slug).unwrap_err();
        assert!(err.contains("symbolic link"), "unexpected error: {err}");

        assert!(ensure_non_symlink_cell_dir(&base_out, "cell-01-b").is_ok());
    }
}
