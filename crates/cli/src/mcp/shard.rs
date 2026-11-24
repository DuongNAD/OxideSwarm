//! MCP `shard` tool implementation (`AX-2`, `docs/design/0103-ax2-sharded-runs.md`).

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::mpsc;

use rusty_grid_core::blob::{OutputPattern, TaskInput, SANDBOX_ROOT};
use rusty_grid_core::blob_transfer::download_blob;
use rusty_grid_core::placement::{IsolationLevel, Placement, OPT_REPEATABLE_TAG, REQUIRE_PREFIX};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::{StagedFile, Task, TaskRequirements, TaskSpec, WorkspaceRef};

use crate::blobs::{BlobConnection, PreparedInputs, UploadReport};
use crate::connect::{connect_master, save_output_artifacts};
use crate::matrix::{ensure_non_symlink_cell_dir, CellStatus};
use crate::shard::{
    blob_matches_junit, check_build_once_path_collision, execute_build_once, execute_shards,
    filter_eligible_workers, junit, normalize_build_once_workers, plan_build_once_groups,
    scan_build_source, spread_shards_swrr, upload_build_source, validate_shard_count,
    BuildOnceOutcome, JunitSummary, ShardProgressEvent, ShardReport, BUILD_ONCE_NEXTEST_JUNIT_REL,
    DEFAULT_BUILD_ONCE_TIMEOUT_SECS, DEFAULT_JUNIT_GLOB, DEFAULT_SHARD_TIMEOUT_SECS,
};

use super::submit::{
    prepare_mcp_output_dir_in_dir, scan_mcp_inputs_in_dir, scan_mcp_workspace_in_dir,
};
use super::{
    McpState, ToolCallContext, ToolCallResult, DEFAULT_MASTER_RPC_TIMEOUT,
    DEFAULT_MCP_MAX_OUTPUT_BYTES,
};

fn resolve_mcp_confined_dir(cwd: &Path, raw: &str, label: &str) -> Result<PathBuf, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(format!("{label} cannot be empty"));
    }
    let path = Path::new(trimmed);
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(format!(
            "{label} '{trimmed}' must not contain '..' or leave the working directory"
        ));
    }
    let canon_cwd = std::fs::canonicalize(cwd)
        .map_err(|e| format!("cannot resolve working directory '{}': {e}", cwd.display()))?;
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let rel = candidate.strip_prefix(cwd).map_err(|_| {
        format!(
            "{label} '{trimmed}' is outside the working directory '{}'",
            cwd.display()
        )
    })?;
    let mut cur = cwd.to_path_buf();
    for comp in rel.components() {
        match comp {
            Component::CurDir => {}
            Component::Normal(part) => {
                cur.push(part);
                let meta = std::fs::symlink_metadata(&cur)
                    .map_err(|e| format!("{label} '{trimmed}': {e}"))?;
                if meta.file_type().is_symlink() {
                    return Err(format!(
                        "{label} '{trimmed}' is or traverses a symbolic link, which is refused"
                    ));
                }
            }
            _ => {
                return Err(format!(
                    "{label} '{trimmed}' is not a relative path inside the working directory"
                ));
            }
        }
    }
    let canon_target =
        std::fs::canonicalize(&cur).map_err(|e| format!("{label} '{trimmed}': {e}"))?;
    if !canon_target.starts_with(&canon_cwd) {
        return Err(format!(
            "{label} '{trimmed}' resolves outside the working directory '{}'",
            cwd.display()
        ));
    }
    if !canon_target.is_dir() {
        return Err(format!("{label} '{trimmed}' is not a directory"));
    }
    Ok(cur)
}

fn resolve_mcp_confined_junit_out(cwd: &Path, raw: &str) -> Result<PathBuf, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("junit_out cannot be empty".to_string());
    }
    let path = Path::new(trimmed);
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(format!(
            "junit_out '{trimmed}' must not contain '..' or leave the working directory"
        ));
    }
    if path.is_absolute() && path.strip_prefix(cwd).is_err() {
        return Err(format!(
            "junit_out '{trimmed}' is outside the working directory '{}'",
            cwd.display()
        ));
    }
    let rel = if path.is_absolute() {
        path.strip_prefix(cwd).unwrap()
    } else {
        path
    };
    let file_name = rel
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|s| !s.is_empty() && *s != ".")
        .ok_or_else(|| format!("junit_out '{trimmed}' must name a file"))?;
    let parent_dir = match rel.parent() {
        Some(p) if !p.as_os_str().is_empty() && p != Path::new(".") => {
            prepare_mcp_output_dir_in_dir(cwd, &p.to_string_lossy())?
        }
        _ => cwd.to_path_buf(),
    };
    let target = parent_dir.join(file_name);
    if let Ok(meta) = std::fs::symlink_metadata(&target) {
        if meta.file_type().is_symlink() {
            return Err(format!(
                "junit_out '{trimmed}' is a symbolic link, which is refused"
            ));
        }
        if meta.is_dir() {
            return Err(format!("junit_out '{trimmed}' exists and is a directory"));
        }
    }
    Ok(target)
}

pub(super) async fn tool_shard(
    state: &McpState,
    args: &Value,
    ctx: &mut ToolCallContext,
) -> ToolCallResult {
    let submit_started = Instant::now();
    let count = match args
        .get("count")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
    {
        Some(n) => match validate_shard_count(n) {
            Ok(valid) => valid,
            Err(e) => return ToolCallResult::error(e),
        },
        None => return ToolCallResult::error("Property 'count' must be a positive integer"),
    };

    let build_once = args
        .get("build_once")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let fail_fast = args
        .get("fail_fast")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let task_type = args
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("command")
        .to_ascii_lowercase();

    let mut env_map = HashMap::new();
    if let Some(env_val) = args.get("env").and_then(Value::as_object) {
        for (k, v) in env_val {
            if let Some(val_str) = v.as_str() {
                env_map.insert(k.clone(), val_str.to_string());
            }
        }
    }

    let extra_args: Vec<String> = args
        .get("args")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    let spec = if build_once {
        TaskSpec::Command {
            program: "cargo".to_string(),
            args: Vec::new(),
            env: env_map.clone(),
            working_dir: None,
            stdin: None,
        }
    } else if task_type == "shell" {
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
                        "Shell shard task requires a non-empty 'script' or 'command' property",
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
            env: env_map.clone(),
        }
    } else {
        let program = match args
            .get("command")
            .and_then(Value::as_str)
            .filter(|c| !c.trim().is_empty())
        {
            Some(c) => c.to_string(),
            None => {
                return ToolCallResult::error(
                    "Command shard task requires a non-empty 'command' property",
                )
            }
        };
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
            args: extra_args.clone(),
            env: env_map.clone(),
            working_dir,
            stdin,
        }
    };

    let mut placement_tags = Vec::new();
    if let Some(level) = args.get("isolation").and_then(Value::as_str) {
        match IsolationLevel::parse_required(level) {
            Ok(parsed) => placement_tags.push(format!("{REQUIRE_PREFIX}iso={parsed}")),
            Err(e) => return ToolCallResult::error(format!("Invalid isolation: {e}")),
        }
    }
    if let Some(os) = args.get("os").and_then(Value::as_str) {
        placement_tags.push(format!("{REQUIRE_PREFIX}os={os}"));
    }
    if let Some(arch) = args.get("arch").and_then(Value::as_str) {
        placement_tags.push(format!("{REQUIRE_PREFIX}arch={arch}"));
    }
    if let Some(worker) = args.get("worker").and_then(Value::as_str) {
        placement_tags.push(format!("{REQUIRE_PREFIX}worker={worker}"));
    }
    if let Some(tags) = args.get("require_tags").and_then(Value::as_array) {
        for t in tags.iter().filter_map(Value::as_str) {
            placement_tags.push(format!("{REQUIRE_PREFIX}tag={t}"));
        }
    }
    if let Some(tools) = args.get("tools").and_then(Value::as_array) {
        for t in tools.iter().filter_map(Value::as_str) {
            placement_tags.push(format!("{REQUIRE_PREFIX}tool={t}"));
        }
    }
    let placement = match Placement::from_task_tags(&placement_tags) {
        Ok(p) => p,
        Err(e) => return ToolCallResult::error(format!("Invalid placement constraints: {e}")),
    };

    let cores = args.get("cores").and_then(Value::as_u64).unwrap_or(1) as usize;
    let ram_mb = args.get("ram_mb").and_then(Value::as_u64).unwrap_or(0);
    let vram_mb = args
        .get("vram_mb")
        .and_then(Value::as_u64)
        .filter(|&v| v > 0);
    let gpu = args.get("gpu").and_then(Value::as_bool).unwrap_or(false) || vram_mb.is_some();
    let default_timeout = if build_once || args.get("workspace").is_some_and(|v| !v.is_null()) {
        DEFAULT_BUILD_ONCE_TIMEOUT_SECS
    } else {
        DEFAULT_SHARD_TIMEOUT_SECS
    };
    let timeout_secs = args
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .or_else(|| args.get("wait_secs").and_then(Value::as_u64))
        .unwrap_or(default_timeout)
        .max(1);
    let max_retries = args
        .get("max_retries")
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok());
    let repeatable = args
        .get("repeatable")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let max_output_bytes = args
        .get("max_output_bytes")
        .and_then(Value::as_u64)
        .map(|n| n as usize)
        .unwrap_or(DEFAULT_MCP_MAX_OUTPUT_BYTES);

    let requirements = TaskRequirements {
        cpu_cores: cores.max(1),
        ram_mb,
        gpu_required: gpu,
        timeout_secs,
        max_retries,
        vram_mb,
        extensions: Vec::new(),
    };

    let cwd = match std::env::current_dir() {
        Ok(dir) => dir,
        Err(e) => return ToolCallResult::error(format!("cannot determine working directory: {e}")),
    };

    let junit_out_path = match args.get("junit_out").and_then(Value::as_str) {
        Some(raw) => match resolve_mcp_confined_junit_out(&cwd, raw) {
            Ok(p) => Some(p),
            Err(e) => return ToolCallResult::error(e),
        },
        None => None,
    };

    let base_out_dir = match args.get("output_dir").and_then(Value::as_str) {
        Some(raw) => match prepare_mcp_output_dir_in_dir(&cwd, raw) {
            Ok(dir) => Some(dir),
            Err(e) => return ToolCallResult::error(format!("Output directory error: {e}")),
        },
        None => None,
    };

    let user_outputs: Vec<String> = args
        .get("outputs")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    let junit_patterns: Vec<OutputPattern> = if junit_out_path.is_some() {
        if let Some(pat) = args.get("junit_glob").and_then(Value::as_str) {
            match OutputPattern::parse(pat) {
                Ok(p) => vec![p],
                Err(e) => return ToolCallResult::error(format!("Invalid junit_glob: {e}")),
            }
        } else if build_once {
            vec![
                OutputPattern::parse(&format!("**/{BUILD_ONCE_NEXTEST_JUNIT_REL}"))
                    .expect("static pattern is valid"),
                OutputPattern::parse(DEFAULT_JUNIT_GLOB).expect("static pattern is valid"),
            ]
        } else {
            vec![OutputPattern::parse(DEFAULT_JUNIT_GLOB).expect("static pattern is valid")]
        }
    } else {
        Vec::new()
    };

    let mut effective_output_globs = user_outputs.clone();
    for pat in &junit_patterns {
        let s = pat.as_str().to_string();
        if !effective_output_globs.contains(&s) {
            effective_output_globs.push(s);
        }
    }

    let build_source_dir = if build_once {
        let raw = args
            .get("build_source")
            .and_then(Value::as_str)
            .unwrap_or(".");
        match resolve_mcp_confined_dir(&cwd, raw, "build_source") {
            Ok(dir) => Some(dir),
            Err(e) => return ToolCallResult::error(e),
        }
    } else {
        None
    };

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error("Shard submission cancelled by client");
    }

    let has_workspace = args.get("workspace").is_some_and(|v| !v.is_null());
    let has_raw_files = args
        .get("files")
        .and_then(Value::as_array)
        .is_some_and(|arr| !arr.is_empty());
    if has_workspace && build_once {
        return ToolCallResult::error(
            "Property 'workspace' cannot be used together with 'build_once'",
        );
    }
    if has_workspace && has_raw_files {
        return ToolCallResult::error("Property 'workspace' cannot be used together with 'files'");
    }

    let target_addr = match state.resolve_target_addr().await {
        Ok(addr) => addr,
        Err(e) => return ToolCallResult::error(e),
    };

    let rpc_timeout =
        Duration::from_secs(timeout_secs.clamp(2, 10)).max(DEFAULT_MASTER_RPC_TIMEOUT);
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

    let eligible = match filter_eligible_workers(
        &workers,
        &placement,
        &requirements,
        has_workspace,
        build_once,
    ) {
        Ok(e) => e,
        Err(e) => return ToolCallResult::error(e),
    };

    if has_workspace && eligible.len() > 1 {
        return ToolCallResult::error(format!(
            "Property 'workspace' cannot shard across {} eligible workers because workspaces hold a single-worker lock; specify 'worker' or use 'files' / 'build_once'",
            eligible.len()
        ));
    }

    let (eligible_for_swrr, platform_tools) = if build_once {
        match normalize_build_once_workers(eligible) {
            Ok(pair) => pair,
            Err(e) => return ToolCallResult::error(e),
        }
    } else {
        (eligible, Default::default())
    };

    let assignments = match spread_shards_swrr(count, &eligible_for_swrr, &placement) {
        Ok(a) => a,
        Err(e) => return ToolCallResult::error(e),
    };

    let extra_inputs: PreparedInputs = match args.get("files").and_then(Value::as_array) {
        Some(arr) if !arr.is_empty() => {
            let specs: Vec<String> = arr
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect();
            match scan_mcp_inputs_in_dir(&cwd, &specs) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(e),
            }
        }
        _ => PreparedInputs::default(),
    };
    if has_workspace && !extra_inputs.inputs.is_empty() {
        return ToolCallResult::error("Property 'workspace' cannot be used together with 'files'");
    }

    let ws_limits = state.workspace_limits;

    let mut total_upload: Option<UploadReport> = None;
    let blob_timeout = Duration::from_secs(timeout_secs).min(DEFAULT_MASTER_RPC_TIMEOUT);
    let mut build_source_scan = None;
    if let Some(ref src_dir) = build_source_dir {
        let scan = match scan_build_source(src_dir, ws_limits) {
            Ok(s) => s,
            Err(e) => return ToolCallResult::error(e),
        };
        for file in scan.manifest.files() {
            if extra_inputs.inputs.iter().any(|i| i.path == file.path) {
                return ToolCallResult::error(format!(
                    "files input '{}' collides with scanned build_source file '{}'",
                    file.path, file.path
                ));
            }
        }
        let conn = match tokio::time::timeout(
            blob_timeout,
            BlobConnection::connect(&target_addr, state.secret.as_ref()),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                return ToolCallResult::error(format!(
                    "Failed to connect to Master for build_source upload: {e}"
                ))
            }
            Err(_) => {
                return ToolCallResult::error(format!(
                    "Timed out connecting to Master at {target_addr} for build_source upload"
                ))
            }
        };
        let up_res = upload_build_source(&conn, &scan).await;
        conn.close().await;
        match up_res {
            Ok(report) => total_upload = Some(report),
            Err(e) => return ToolCallResult::error(format!("Failed to upload build_source: {e}")),
        }
        build_source_scan = Some(scan);
    }

    if !extra_inputs.inputs.is_empty() {
        let conn = match tokio::time::timeout(
            blob_timeout,
            BlobConnection::connect(&target_addr, state.secret.as_ref()),
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
        let up_res = crate::blobs::upload_inputs(&conn, &extra_inputs).await;
        conn.close().await;
        match up_res {
            Ok(report) => {
                total_upload = Some(match total_upload {
                    Some(prev) => UploadReport {
                        blobs: prev.blobs + report.blobs,
                        uploaded: prev.uploaded + report.uploaded,
                        bytes: prev.bytes + report.bytes,
                    },
                    None => report,
                });
            }
            Err(e) => return ToolCallResult::error(format!("Failed to upload input files: {e}")),
        }
    }

    let workspace_ref = match args.get("workspace").and_then(Value::as_object) {
        Some(ws_obj) => {
            let ws_name = ws_obj
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let sync_dir_opt = match ws_obj.get("sync_dir") {
                None => Some("."),
                Some(Value::Null) => None,
                Some(v) => v.as_str(),
            };
            if let Some(sync_dir) = sync_dir_opt {
                let scan = match scan_mcp_workspace_in_dir(&cwd, sync_dir, ws_limits) {
                    Ok(s) => s,
                    Err(e) => return ToolCallResult::error(e),
                };
                let conn = match tokio::time::timeout(
                    blob_timeout,
                    BlobConnection::connect(&target_addr, state.secret.as_ref()),
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
                let up_res = crate::blobs::upload_inputs(&conn, &scan.prepared).await;
                conn.close().await;
                match up_res {
                    Ok(report) => total_upload = Some(report),
                    Err(e) => {
                        return ToolCallResult::error(format!(
                            "Failed to upload workspace files: {e}"
                        ))
                    }
                }
                Some(WorkspaceRef::new(ws_name).with_tree(scan.manifest_id, scan.manifest_len))
            } else {
                Some(WorkspaceRef::new(ws_name))
            }
        }
        None => None,
    };

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error("Shard submission cancelled by client");
    }

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

    let mut template_inputs = extra_inputs.inputs.clone();
    let mut build_once_map = None;
    let mut build_once_list: Vec<BuildOnceOutcome> = Vec::new();
    let mut nextest_config_staged: Option<StagedFile> = None;

    if let Some(ref scan) = build_source_scan {
        let manifest_input = TaskInput::tree(SANDBOX_ROOT, scan.manifest_id, scan.manifest_len);
        let user_worker = args.get("worker").and_then(Value::as_str);
        let (groups, config_rel_opt) = match plan_build_once_groups(
            &assignments,
            &platform_tools,
            user_worker,
            &scan.manifest,
            &extra_inputs.inputs,
            junit_out_path.is_some(),
        ) {
            Ok(res) => res,
            Err(e) => return ToolCallResult::error(e),
        };
        if let Some(ref cfg_rel) = config_rel_opt {
            if let Err(e) =
                check_build_once_path_collision(cfg_rel, &scan.manifest, &extra_inputs.inputs)
            {
                return ToolCallResult::error(e);
            }
            let cfg_toml =
                format!("[profile.default.junit]\npath = \"{BUILD_ONCE_NEXTEST_JUNIT_REL}\"\n");
            nextest_config_staged = Some(StagedFile::new(cfg_rel, cfg_toml.into_bytes(), false));
        }

        ctx.send_progress(
            0,
            Some(count as u64),
            format!(
                "Building {} platform archive(s) for {count} shards",
                groups.len()
            ),
        );

        let mut env_for_build = HashMap::new();
        if let Some(env_val) = args.get("env").and_then(Value::as_object) {
            for (k, v) in env_val {
                if let Some(val_str) = v.as_str() {
                    env_for_build.insert(k.clone(), val_str.to_string());
                }
            }
        }

        let outcomes = match execute_build_once(
            &groups,
            &manifest_input,
            &extra_inputs.inputs,
            &placement,
            &requirements,
            &env_for_build,
            repeatable,
            &workers,
            Duration::from_secs(timeout_secs),
            Some(ctx.cancel_rx.clone()),
            Arc::clone(&connect),
        )
        .await
        {
            Ok(map) => map,
            Err(e) => return ToolCallResult::error(e),
        };
        build_once_list = outcomes.values().cloned().collect();
        build_once_map = Some(outcomes);
        template_inputs.insert(0, manifest_input);
    }

    ctx.send_progress(0, Some(count as u64), format!("Starting {count} shard(s)"));

    let mut template = Task::new(spec, requirements.clone())
        .with_placement(&placement)
        .with_inputs(template_inputs)
        .with_output_globs(effective_output_globs);
    if repeatable {
        template.tags.push(OPT_REPEATABLE_TAG.to_string());
    }
    if let Some(ws) = workspace_ref {
        template = template.with_workspace(ws);
    }

    let (prog_tx, mut prog_rx) = mpsc::unbounded_channel::<ShardProgressEvent>();
    let exec_fut = execute_shards(
        assignments,
        &workers,
        &template,
        build_once_map.as_ref(),
        nextest_config_staged.as_ref(),
        &extra_args,
        Duration::from_secs(timeout_secs),
        fail_fast,
        Some(ctx.cancel_rx.clone()),
        Some(prog_tx),
        connect,
    );

    let progress_fut = async {
        let mut completed = 0u64;
        while let Some(ev) = prog_rx.recv().await {
            match ev {
                ShardProgressEvent::Submitted { task_id, .. } => {
                    state.record_submitted(task_id, submit_started).await;
                }
                ShardProgressEvent::Finished {
                    shard_index,
                    status,
                } => {
                    completed += 1;
                    ctx.send_progress(
                        completed,
                        Some(count as u64),
                        format!("Shard {shard_index}/{count} -> {}", status.as_str()),
                    );
                }
            }
        }
    };

    let (mut executions, ()) = tokio::join!(exec_fut, progress_fut);

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error("Shard submission cancelled by client");
    }

    let any_outputs = executions
        .iter()
        .any(|e| e.result.as_ref().is_some_and(|r| !r.outputs.is_empty()));
    let mut junit_inputs = Vec::with_capacity(executions.len());

    if any_outputs && (base_out_dir.is_some() || junit_out_path.is_some()) {
        let dl_timeout = Duration::from_secs(timeout_secs).min(DEFAULT_MASTER_RPC_TIMEOUT);
        match tokio::time::timeout(
            dl_timeout,
            BlobConnection::connect(&target_addr, state.secret.as_ref()),
        )
        .await
        {
            Ok(Ok(conn)) => {
                for exec in &mut executions {
                    let Some(ref result) = exec.result else {
                        continue;
                    };
                    if let Some(ref out_dir) = base_out_dir {
                        if !result.outputs.is_empty() {
                            match ensure_non_symlink_cell_dir(out_dir, &exec.assignment.slug) {
                                Ok(shard_dir) => {
                                    match crate::blobs::download_outputs(
                                        &conn,
                                        &result.outputs,
                                        &shard_dir,
                                    )
                                    .await
                                    {
                                        Ok(paths) => exec.saved_outputs = Some(paths),
                                        Err(e) => {
                                            exec.download_error =
                                                Some(format!("Failed to download outputs: {e}"));
                                        }
                                    }
                                }
                                Err(e) => {
                                    exec.download_error = Some(e);
                                }
                            }
                        }
                    }
                    if junit_out_path.is_some() {
                        let mut blobs = Vec::new();
                        let mut junit_dl_error: Option<String> = None;
                        for output in &result.outputs {
                            if blob_matches_junit(&output.path, &junit_patterns) {
                                let mut buf = Vec::new();
                                match download_blob(&conn, output.id, Some(output.size), &mut buf)
                                    .await
                                {
                                    Ok(_) => blobs.push(junit::ShardJunitBlob {
                                        path: output.path.clone(),
                                        bytes: buf,
                                    }),
                                    Err(e) => {
                                        let msg = format!(
                                            "Failed to download JUnit blob '{}': {e}",
                                            output.path
                                        );
                                        junit_dl_error.get_or_insert_with(|| msg.clone());
                                        exec.download_error.get_or_insert(msg);
                                    }
                                }
                            }
                        }
                        let (junit_status, junit_error) = if junit_dl_error.is_some() {
                            (CellStatus::Error, exec.download_error.clone())
                        } else {
                            (exec.status, None)
                        };
                        junit_inputs.push(junit::ShardJunitInput {
                            shard_index: exec.assignment.index,
                            status: junit_status,
                            duration_ms: result.execution_time_ms,
                            stderr_excerpt: result.stderr_str().trim().to_string(),
                            error: junit_error,
                            blobs,
                        });
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

    if junit_out_path.is_some() {
        for exec in &executions {
            if let Some(ref r) = exec.result {
                if !any_outputs {
                    junit_inputs.push(junit::ShardJunitInput {
                        shard_index: exec.assignment.index,
                        status: exec.status,
                        duration_ms: r.execution_time_ms,
                        stderr_excerpt: r.stderr_str().trim().to_string(),
                        error: exec.error.clone(),
                        blobs: Vec::new(),
                    });
                }
            } else {
                junit_inputs.push(junit::ShardJunitInput {
                    shard_index: exec.assignment.index,
                    status: exec.status,
                    duration_ms: 0,
                    stderr_excerpt: exec.error.clone().unwrap_or_default(),
                    error: exec.error.clone(),
                    blobs: Vec::new(),
                });
            }
        }
    }

    if let Some(ref out_dir) = base_out_dir {
        for exec in &mut executions {
            let Some(ref result) = exec.result else {
                continue;
            };
            if !result.artifacts.is_empty() {
                match ensure_non_symlink_cell_dir(out_dir, &exec.assignment.slug) {
                    Ok(shard_dir) => match save_output_artifacts(&result.artifacts, &shard_dir) {
                        Ok(paths) => exec.saved_artifacts = Some(paths),
                        Err(e) => {
                            exec.download_error
                                .get_or_insert_with(|| format!("Failed to save artifacts: {e}"));
                        }
                    },
                    Err(e) => {
                        exec.download_error.get_or_insert(e);
                    }
                }
            }
        }
    }

    let (junit_summary, junit_error) = if let Some(ref jpath) = junit_out_path {
        let glob_desc = junit_patterns
            .iter()
            .map(OutputPattern::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        match junit::merge_shard_junit_inputs(junit_inputs, &glob_desc) {
            Ok(merged) => match junit::write_junit_atomic(jpath, &merged.xml) {
                Ok(()) => (
                    Some(JunitSummary {
                        output_path: jpath.display().to_string(),
                        tests: merged.tests,
                        failures: merged.failures,
                        errors: merged.errors,
                        skipped: merged.skipped,
                        time_secs: merged.time_secs,
                        suites: merged.suites,
                        synthesized_shards: merged.synthesized_shards,
                        parse_errors: merged.parse_errors,
                    }),
                    None,
                ),
                Err(e) => (None, Some(e)),
            },
            Err(e) => (None, Some(e)),
        }
    } else {
        (None, None)
    };

    let executed_count = executions
        .iter()
        .filter(|e| e.result.is_some())
        .count()
        .max(1);
    let per_shard_max_bytes = if max_output_bytes == 0 {
        0
    } else {
        (max_output_bytes / executed_count)
            .max(512)
            .min(max_output_bytes)
    };

    let waited_ms = submit_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let report = ShardReport::from_executions(
        executions,
        waited_ms,
        per_shard_max_bytes,
        build_once_list,
        junit_summary,
        junit_error,
        total_upload,
    );

    let is_error = report.any_failed();
    let mut out = ToolCallResult::ok_json(&report.to_json());
    out.is_error = is_error;
    out
}
