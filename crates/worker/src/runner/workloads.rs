//! Workload-specific task execution handlers (workspace, command, shell, Rust build, 3D, auto-rig).

use std::collections::HashMap;
#[cfg(windows)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
#[cfg(feature = "3d")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
#[cfg(feature = "3d")]
use tracing::info;
use tracing::warn;

use rusty_grid_core::blob::{TreeManifest, MAX_MANIFEST_BYTES};
use rusty_grid_core::protocol::WorkerMessage;
#[cfg(feature = "3d")]
use rusty_grid_core::task::Engine3D;
use rusty_grid_core::task::{
    Bytes, BytesMut, CheckpointData, Task, TaskResult, TaskSpec, WorkspaceRef, WorkspaceSource,
};

use crate::sandbox::{sanitize_relative_path, set_permissions, Sandbox, SandboxError};
use crate::workspace::WorkspaceError;

#[cfg(feature = "3d")]
use super::capture::read_output_file;
use super::capture::{append_note, cancellation, check_cancelled_now};
use super::checkpoint::{ProcessCheckpointCtx, PROCESS_CHECKPOINT_DIR_NAME};
#[cfg(not(windows))]
use super::supervise::resolve_default_shell;
#[cfg(windows)]
use super::supervise::{augment_windows_path, resolve_windows_executable};
use super::supervise::{
    detect_worker_rustup_toolchain, resolve_workspace_cwd, validate_cargo_compiler_flags,
    WorkspaceProcessCtx,
};
#[cfg(feature = "3d")]
use super::EXIT_CODE_TIMEOUT;
use super::{TaskRunner, EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR};

/// The refusal for a sandbox base that cannot be used: recognised by the master as a worker
/// refusal (design 0090), instant and without output.
pub(crate) fn base_refusal(
    worker_id: uuid::Uuid,
    task_id: rusty_grid_core::task::TaskId,
    e: &SandboxError,
) -> TaskResult {
    let text = rusty_grid_core::sandbox_base_refusal(e);
    TaskResult::failure(
        worker_id,
        task_id,
        EXIT_CODE_GENERAL_ERROR,
        "",
        text.clone(),
        0,
        Some(text),
    )
}

impl TaskRunner {
    /// Executes a `Command` or `ShellScript` task inside a persistent named workspace (`design 0039`).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_workspace_workload(
        &self,
        task: &Task,
        ws: &WorkspaceRef,
        checkpoint: Option<&CheckpointData>,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let fail = |msg: String| {
            TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                format!("Workspace error: {msg}"),
                start_time.elapsed().as_millis() as u64,
                Some(msg),
            )
        };

        if !task.inputs.is_empty() || !task.staged_files.is_empty() {
            return fail(
                "a task with a workspace cannot also declare per-task inputs or staged_files; use the workspace source tree".to_string(),
            );
        }

        let store = match self.ensure_workspace_store().await {
            Ok(Some(store)) => store,
            Ok(None) => {
                return fail(
                    "this worker was started without --workspaces, so it cannot run workspace tasks"
                        .to_string(),
                );
            }
            Err(e) => return fail(format!("cannot open workspace store: {e}")),
        };

        let cache = if ws.source.is_some() {
            match self.ensure_blob_cache().await {
                Ok(c) => Some(c),
                Err(e) => return fail(e),
            }
        } else {
            None
        };

        // Poll the workspace lock in short intervals and check task cancellation between attempts
        // so a wait on a busy workspace never delays cancellation by up to `workspace_lock_wait`.
        let wait = self.config.workspace_lock_wait.min(timeout);
        let lock_deadline = tokio::time::Instant::now() + wait;
        let guard = loop {
            if let Some(rx) = cancel_rx.as_mut() {
                if let Ok(reason) = rx.try_recv() {
                    return TaskResult::failure(
                        self.worker_id,
                        task.id,
                        EXIT_CODE_CANCELLED,
                        "",
                        "",
                        start_time.elapsed().as_millis() as u64,
                        Some(format!("Cancelled: {reason}")),
                    );
                }
            }
            let store_try = store.clone();
            let name_try = ws.name.clone();
            let attempt =
                tokio::task::spawn_blocking(move || store_try.acquire(&name_try, Duration::ZERO))
                    .await;
            match attempt {
                Ok(Ok(guard)) => break guard,
                Ok(Err(WorkspaceError::Busy(_))) => {
                    let now = tokio::time::Instant::now();
                    if now >= lock_deadline {
                        return fail(WorkspaceError::Busy(ws.name.clone()).to_string());
                    }
                    let step = Duration::from_millis(20).min(lock_deadline - now);
                    tokio::select! {
                        reason = cancellation(&mut cancel_rx) => {
                            return TaskResult::failure(
                                self.worker_id,
                                task.id,
                                EXIT_CODE_CANCELLED,
                                "",
                                "",
                                start_time.elapsed().as_millis() as u64,
                                Some(format!("Cancelled: {reason}")),
                            );
                        }
                        _ = tokio::time::sleep(step) => {}
                    }
                }
                Ok(Err(e)) => return fail(e.to_string()),
                Err(e) => return fail(format!("workspace lock task panicked: {e}")),
            }
        };

        let ws_clone = ws.clone();
        let spec_clone = task.spec.clone();

        let prepared = tokio::task::spawn_blocking(move || -> Result<_, String> {
            // Pre-run symlink / canonical path check on `<ws>/tree` and `<ws>/tmp` before every
            // workspace run (with or without a source).
            guard
                .check_existing_dirs()
                .map_err(|e| format!("workspace '{}': {e}", ws_clone.name))?;
            let mut sync_warning: Option<String> = None;
            if let Some(WorkspaceSource::Tree {
                manifest,
                manifest_size,
            }) = &ws_clone.source
            {
                if *manifest_size > MAX_MANIFEST_BYTES {
                    return Err(format!(
                        "workspace '{}': directory manifest size ({manifest_size} bytes) exceeds limit ({MAX_MANIFEST_BYTES} bytes)",
                        ws_clone.name
                    ));
                }
                let cache = cache.as_ref().expect("cache opened for tree source");
                let bytes = cache
                    .read_verified(manifest, MAX_MANIFEST_BYTES)
                    .map_err(|e| format!("workspace '{}': directory manifest: {e}", ws_clone.name))?;
                let tree = TreeManifest::decode(&bytes)
                    .map_err(|e| format!("workspace '{}': {e}", ws_clone.name))?;
                match guard.sync(&tree, cache.as_ref()) {
                    Ok(report) => {
                        sync_warning = report.warning;
                    }
                    Err(err) => {
                        if let WorkspaceError::Integrity { ref path } = err {
                            if let Some(f) = tree.files().iter().find(|f| f.path == *path) {
                                cache.drop_corrupt(&f.id);
                            }
                        }
                        return Err(err.to_string());
                    }
                }
            } else if !guard.is_ready() {
                return Err(format!(
                    "workspace '{}' has no synced tree on this worker (run once without --no-sync first)",
                    ws_clone.name
                ));
            }
            guard.prepare_tmp().map_err(|e| e.to_string())?;
            guard
                .verify_run_dirs()
                .map_err(|e| format!("workspace '{}': {e}", ws_clone.name))?;

            let tree_dir = guard.tree_dir();
            let tmp_dir = guard.tmp_dir();

            match spec_clone {
                TaskSpec::Command {
                    program,
                    args,
                    env,
                    working_dir,
                    stdin,
                } => {
                    let effective_cwd = match working_dir {
                        Some(ref sub) => resolve_workspace_cwd(&tree_dir, sub)?,
                        None => tree_dir.clone(),
                    };
                    Ok((
                        guard,
                        program,
                        args,
                        effective_cwd,
                        env,
                        stdin.map(|b| b.to_vec()),
                        sync_warning,
                    ))
                }
                TaskSpec::ShellScript {
                    script,
                    interpreter,
                    env,
                } => {
                    let script_path = tmp_dir.join("task_script.sh");
                    std::fs::write(&script_path, script.as_bytes()).map_err(|e| {
                        format!("cannot write shell script to workspace tmp: {e}")
                    })?;
                    let _ = set_permissions(&script_path, 0o700);

                    #[cfg(windows)]
                    let (prog, args) = if let Some(interp) = interpreter {
                        (
                            interp,
                            vec![script_path.to_string_lossy().to_string()],
                        )
                    } else {
                        let base = env
                            .get("PATH")
                            .cloned()
                            .unwrap_or_else(|| std::env::var("PATH").unwrap_or_default());
                        let effective_path = augment_windows_path(&base);
                        let sh_resolved = resolve_windows_executable("sh", &effective_path);
                        if Path::new(&sh_resolved).is_file() {
                            (sh_resolved, vec![script_path.to_string_lossy().to_string()])
                        } else {
                            (
                                "cmd.exe".to_string(),
                                vec![
                                    "/C".to_string(),
                                    script_path.to_string_lossy().to_string(),
                                ],
                            )
                        }
                    };

                    #[cfg(not(windows))]
                    let (prog, args) = if let Some(interp) = interpreter {
                        (
                            interp,
                            vec![script_path.to_string_lossy().to_string()],
                        )
                    } else {
                        let path = env
                            .get("PATH")
                            .cloned()
                            .unwrap_or_else(|| std::env::var("PATH").unwrap_or_default());
                        (
                            resolve_default_shell(&path),
                            vec![script_path.to_string_lossy().to_string()],
                        )
                    };

                    Ok((guard, prog, args, tree_dir, env, None, sync_warning))
                }
                _ => unreachable!("checked before execute_workspace_workload"),
            }
        })
        .await;

        let (guard, program, args, effective_cwd, env, stdin_data, sync_warning) = match prepared {
            Ok(Ok(tuple)) => tuple,
            Ok(Err(e)) => return fail(e),
            Err(e) => return fail(format!("workspace preparation panicked: {e}")),
        };

        let tree_dir = guard.tree_dir();
        let tmp_dir = guard.tmp_dir();
        let ws_task_env = guard.task_env();
        let ws_ctx = WorkspaceProcessCtx {
            tmp_dir: &tmp_dir,
            workspace_root: store.root(),
            task_env: ws_task_env,
        };

        let cp_dir = tmp_dir.join(PROCESS_CHECKPOINT_DIR_NAME);
        let cp_owned = checkpoint.cloned();
        let cp_task_id = task.id;
        let cp_poll = self.config.checkpoint_poll_interval;
        let cp_ctx = match tokio::task::spawn_blocking(move || {
            ProcessCheckpointCtx::prepare_and_restore(
                cp_task_id,
                cp_dir,
                cp_owned.as_ref(),
                checkpoint_tx,
                cp_poll,
            )
        })
        .await
        {
            Ok(Ok(ctx)) => ctx,
            Ok(Err(e)) => return fail(e),
            Err(e) => return fail(format!("checkpoint preparation panicked: {e}")),
        };

        let result = self
            .execute_process_supervised_with_checkpoint(
                task.id,
                &tree_dir,
                &program,
                &args,
                &effective_cwd,
                &env,
                stdin_data,
                timeout,
                child_pid,
                cancel_rx,
                start_time,
                Some(ws_ctx),
                Some(cp_ctx),
            )
            .await;

        let process_failed = !result.is_success();
        let result = self
            .harvest_and_apply_artifacts_from_dir(&tree_dir, task, result, process_failed)
            .await;
        let mut result = self
            .collect_blob_outputs_from_dir(&tree_dir, task, result, &[], false)
            .await;
        if let Some(warning) = sync_warning {
            result.stderr = append_note(result.stderr, &format!("[rusty_grid: {warning}]\n"));
        }

        let finish_res = tokio::task::spawn_blocking(move || {
            let res = guard.finish();
            drop(guard);
            res
        })
        .await;
        match finish_res {
            Ok(Ok(report)) => {
                if let Some(warning) = report.warning {
                    result.stderr =
                        append_note(result.stderr, &format!("[rusty_grid: {warning}]\n"));
                }
            }
            Ok(Err(e)) => {
                warn!(task_id = %task.id, workspace = %ws.name, error = %e, "Workspace finish failed");
                result.stderr = append_note(
                    result.stderr,
                    &format!("[rusty_grid: workspace finish failed: {e}]\n"),
                );
            }
            Err(e) => {
                warn!(task_id = %task.id, workspace = %ws.name, error = %e, "Workspace finish task failed");
            }
        }
        result
    }

    /// Executes `TaskSpec::Command` inside an isolated sandbox directory.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_command_workload(
        &self,
        task: &Task,
        program: &str,
        args: &[String],
        env: &HashMap<String, String>,
        working_dir: Option<&PathBuf>,
        stdin: Option<&[u8]>,
        checkpoint: Option<&CheckpointData>,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        if let Some(reason) = check_cancelled_now(&mut cancel_rx) {
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_CANCELLED,
                "",
                "",
                start_time.elapsed().as_millis() as u64,
                Some(format!("Cancelled: {reason}")),
            );
        }

        let mut sandbox = match Sandbox::create(&self.config.sandbox_config, task.id).await {
            Ok(s) => s,
            Err(e) if e.is_create_refusal() => {
                return base_refusal(self.worker_id, task.id, &e);
            }
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to initialize sandbox: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Sandbox initialization error: {e}")),
                );
            }
        };

        if let Err(e) = self.prepare_staged_files(&sandbox, task).await {
            let _ = sandbox.destroy().await;
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                format!("Failed to stage input files: {e}"),
                start_time.elapsed().as_millis() as u64,
                Some(format!("File staging error: {e}")),
            );
        }

        if let Err(e) = self.prepare_blob_inputs(&sandbox, task).await {
            let _ = sandbox.destroy().await;
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                format!("Failed to place input files: {e}"),
                start_time.elapsed().as_millis() as u64,
                Some(format!("Input files: {e}")),
            );
        }

        let effective_cwd = if let Some(sub) = working_dir {
            match sanitize_relative_path(sandbox.path(), sub) {
                Ok(p) => {
                    let _ = tokio::fs::create_dir_all(&p).await;
                    p
                }
                Err(e) => {
                    let _ = sandbox.destroy().await;
                    return TaskResult::failure(
                        self.worker_id,
                        task.id,
                        EXIT_CODE_GENERAL_ERROR,
                        "",
                        format!("Illegal working_dir: {e}"),
                        start_time.elapsed().as_millis() as u64,
                        Some(format!("Invalid working directory path: {e}")),
                    );
                }
            }
        } else {
            sandbox.path().to_path_buf()
        };

        let cp_dir = sandbox.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        let cp_owned = checkpoint.cloned();
        let cp_task_id = task.id;
        let cp_poll = self.config.checkpoint_poll_interval;
        let cp_ctx = match tokio::task::spawn_blocking(move || {
            ProcessCheckpointCtx::prepare_and_restore(
                cp_task_id,
                cp_dir,
                cp_owned.as_ref(),
                checkpoint_tx,
                cp_poll,
            )
        })
        .await
        {
            Ok(Ok(ctx)) => ctx,
            Ok(Err(e)) => {
                let _ = sandbox.destroy().await;
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to initialize checkpoint directory: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Checkpoint directory error: {e}")),
                );
            }
            Err(e) => {
                let _ = sandbox.destroy().await;
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Checkpoint preparation panicked: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Checkpoint preparation panicked: {e}")),
                );
            }
        };

        let result = self
            .execute_process_supervised_with_checkpoint(
                task.id,
                sandbox.path(),
                program,
                args,
                &effective_cwd,
                env,
                stdin.map(|s| s.to_vec()),
                timeout,
                child_pid,
                cancel_rx,
                start_time,
                None,
                Some(cp_ctx),
            )
            .await;

        let process_failed = !result.is_success();
        let result = self
            .harvest_and_apply_artifacts(&sandbox, task, result, process_failed)
            .await;
        let result = self.collect_blob_outputs(&sandbox, task, result, &[]).await;
        let _ = sandbox.destroy().await;
        result
    }

    /// Executes `TaskSpec::ShellScript` by materializing the script into sandbox and executing via interpreter.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_shell_script_workload(
        &self,
        task: &Task,
        script: &str,
        interpreter: Option<&str>,
        env: &HashMap<String, String>,
        checkpoint: Option<&CheckpointData>,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        if let Some(reason) = check_cancelled_now(&mut cancel_rx) {
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_CANCELLED,
                "",
                "",
                start_time.elapsed().as_millis() as u64,
                Some(format!("Cancelled: {reason}")),
            );
        }

        let mut sandbox = match Sandbox::create(&self.config.sandbox_config, task.id).await {
            Ok(s) => s,
            Err(e) if e.is_create_refusal() => {
                return base_refusal(self.worker_id, task.id, &e);
            }
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to initialize sandbox: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Sandbox initialization error: {e}")),
                );
            }
        };

        if let Err(e) = self.prepare_staged_files(&sandbox, task).await {
            let _ = sandbox.destroy().await;
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                format!("Failed to stage input files: {e}"),
                start_time.elapsed().as_millis() as u64,
                Some(format!("File staging error: {e}")),
            );
        }

        if let Err(e) = self.prepare_blob_inputs(&sandbox, task).await {
            let _ = sandbox.destroy().await;
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                format!("Failed to place input files: {e}"),
                start_time.elapsed().as_millis() as u64,
                Some(format!("Input files: {e}")),
            );
        }

        let script_path = match sandbox.write_script("task_script.sh", script).await {
            Ok(p) => p,
            Err(e) => {
                let _ = sandbox.destroy().await;
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to materialize shell script: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Script write error: {e}")),
                );
            }
        };

        #[cfg(windows)]
        let effective_path = {
            let base = env
                .get("PATH")
                .cloned()
                .unwrap_or_else(|| std::env::var("PATH").unwrap_or_default());
            augment_windows_path(&base)
        };

        #[cfg(windows)]
        let (prog, args) = if let Some(interp) = interpreter {
            (
                interp.to_string(),
                vec![script_path.to_string_lossy().to_string()],
            )
        } else {
            let sh_resolved = resolve_windows_executable("sh", &effective_path);
            if Path::new(&sh_resolved).is_file() {
                (sh_resolved, vec![script_path.to_string_lossy().to_string()])
            } else {
                (
                    "cmd.exe".to_string(),
                    vec!["/C".to_string(), script_path.to_string_lossy().to_string()],
                )
            }
        };

        #[cfg(not(windows))]
        let (prog, args) = if let Some(interp) = interpreter {
            (
                interp.to_string(),
                vec![script_path.to_string_lossy().to_string()],
            )
        } else {
            let path = env
                .get("PATH")
                .cloned()
                .unwrap_or_else(|| std::env::var("PATH").unwrap_or_default());
            (
                resolve_default_shell(&path),
                vec![script_path.to_string_lossy().to_string()],
            )
        };

        let cp_dir = sandbox.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        let cp_owned = checkpoint.cloned();
        let cp_task_id = task.id;
        let cp_poll = self.config.checkpoint_poll_interval;
        let cp_ctx = match tokio::task::spawn_blocking(move || {
            ProcessCheckpointCtx::prepare_and_restore(
                cp_task_id,
                cp_dir,
                cp_owned.as_ref(),
                checkpoint_tx,
                cp_poll,
            )
        })
        .await
        {
            Ok(Ok(ctx)) => ctx,
            Ok(Err(e)) => {
                let _ = sandbox.destroy().await;
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to initialize checkpoint directory: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Checkpoint directory error: {e}")),
                );
            }
            Err(e) => {
                let _ = sandbox.destroy().await;
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Checkpoint preparation panicked: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Checkpoint preparation panicked: {e}")),
                );
            }
        };

        let result = self
            .execute_process_supervised_with_checkpoint(
                task.id,
                sandbox.path(),
                &prog,
                &args,
                sandbox.path(),
                env,
                None,
                timeout,
                child_pid,
                cancel_rx,
                start_time,
                None,
                Some(cp_ctx),
            )
            .await;

        let process_failed = !result.is_success();
        let result = self
            .harvest_and_apply_artifacts(&sandbox, task, result, process_failed)
            .await;
        let result = self.collect_blob_outputs(&sandbox, task, result, &[]).await;
        let _ = sandbox.destroy().await;
        result
    }

    /// Executes `TaskSpec::RustCompilation` by materializing source files, invoking `rustc`, and verifying artifacts.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_rust_compilation_workload(
        &self,
        task: &Task,
        crate_name: &str,
        source_files: &HashMap<String, String>,
        compiler_flags: &[String],
        target_dir: Option<&PathBuf>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        if let Some(reason) = check_cancelled_now(&mut cancel_rx) {
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_CANCELLED,
                "",
                "",
                start_time.elapsed().as_millis() as u64,
                Some(format!("Cancelled: {reason}")),
            );
        }

        let mut sandbox = match Sandbox::create(&self.config.sandbox_config, task.id).await {
            Ok(s) => s,
            Err(e) if e.is_create_refusal() => {
                return base_refusal(self.worker_id, task.id, &e);
            }
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to initialize sandbox: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Sandbox initialization error: {e}")),
                );
            }
        };

        if let Err(e) = self.prepare_staged_files(&sandbox, task).await {
            let _ = sandbox.destroy().await;
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                format!("Failed to stage input files: {e}"),
                start_time.elapsed().as_millis() as u64,
                Some(format!("File staging error: {e}")),
            );
        }

        if let Err(e) = self.prepare_blob_inputs(&sandbox, task).await {
            let _ = sandbox.destroy().await;
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                format!("Failed to place input files: {e}"),
                start_time.elapsed().as_millis() as u64,
                Some(format!("Input files: {e}")),
            );
        }

        // Materialize source files into sandbox
        for (rel_path, content) in source_files {
            if let Err(e) = sandbox.write_file(rel_path, content.as_bytes()).await {
                let _ = sandbox.destroy().await;
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to write source file '{rel_path}': {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Source materialization error: {e}")),
                );
            }
        }

        let mut out_dir = if let Some(td) = target_dir {
            match sanitize_relative_path(sandbox.path(), td) {
                Ok(p) => p,
                Err(e) => {
                    let _ = sandbox.destroy().await;
                    return TaskResult::failure(
                        self.worker_id,
                        task.id,
                        EXIT_CODE_GENERAL_ERROR,
                        "",
                        format!("Invalid target_dir: {e}"),
                        start_time.elapsed().as_millis() as u64,
                        Some(format!("Target directory path error: {e}")),
                    );
                }
            }
        } else {
            sandbox.path().join("target")
        };

        let use_cargo = sandbox.path().join("Cargo.toml").is_file();
        let (program, args, compile_env) = if use_cargo {
            let (filtered_flags, override_target_dir) =
                match validate_cargo_compiler_flags(compiler_flags, sandbox.path()) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = sandbox.destroy().await;
                        return TaskResult::failure(
                            self.worker_id,
                            task.id,
                            EXIT_CODE_GENERAL_ERROR,
                            "",
                            format!("Invalid cargo compiler_flags: {e}"),
                            start_time.elapsed().as_millis() as u64,
                            Some(format!("Invalid cargo compiler_flags: {e}")),
                        );
                    }
                };
            if let Some(td) = override_target_dir {
                out_dir = td;
            }
            let _ = tokio::fs::create_dir_all(&out_dir).await;

            // Remove any task-supplied `rust-toolchain` or `rust-toolchain.toml` from the sandbox
            // root and pin `RUSTUP_TOOLCHAIN` to the worker's active toolchain so `rustup` never
            // attempts to download or switch to an uncached toolchain over the network.
            let _ = tokio::fs::remove_file(sandbox.path().join("rust-toolchain")).await;
            let _ = tokio::fs::remove_file(sandbox.path().join("rust-toolchain.toml")).await;

            let mut cargo_args = vec![
                "build".to_string(),
                "--offline".to_string(),
                "--target-dir".to_string(),
                out_dir.to_string_lossy().to_string(),
            ];
            cargo_args.extend(filtered_flags);

            // Point `CARGO_HOME` and `RUSTUP_HOME` at the worker's host directories so pre-warmed
            // crate caches in `$CARGO_HOME/registry` and `$CARGO_HOME/git` are readable by
            // `cargo build --offline`. Under `--isolation fs|fs+net` (Design 0038 / 0039 D8),
            // `CARGO_HOME` is mounted/ACL'd read-only and `$CARGO_HOME/credentials.toml` is
            // explicitly denied.
            let mut env = HashMap::new();
            env.insert(
                "CARGO_TARGET_DIR".to_string(),
                out_dir.to_string_lossy().to_string(),
            );
            env.insert("CARGO_NET_OFFLINE".to_string(), "true".to_string());
            env.insert("RUSTUP_AUTO_INSTALL".to_string(), "0".to_string());
            env.insert(
                "CARGO_HOME".to_string(),
                self.config
                    .host_dirs
                    .cargo_home
                    .to_string_lossy()
                    .to_string(),
            );
            env.insert(
                "RUSTUP_HOME".to_string(),
                self.config
                    .host_dirs
                    .rustup_home
                    .to_string_lossy()
                    .to_string(),
            );
            if let Some(toolchain) = detect_worker_rustup_toolchain(&self.config.host_dirs) {
                env.insert("RUSTUP_TOOLCHAIN".to_string(), toolchain);
            }
            ("cargo", cargo_args, env)
        } else {
            let _ = tokio::fs::create_dir_all(&out_dir).await;
            // Determine entry point and default crate type
            let entry_candidates = [
                ("src/lib.rs", "lib"),
                ("src/main.rs", "bin"),
                ("lib.rs", "lib"),
                ("main.rs", "bin"),
            ];

            let (entry_rel, default_crate_type) = entry_candidates
                .iter()
                .find(|(path, _)| source_files.contains_key(*path))
                .map(|(path, ctype)| (*path, *ctype))
                .unwrap_or_else(|| {
                    // Fallback: look for any .rs file in source_files
                    source_files
                        .keys()
                        .find(|k| k.ends_with(".rs"))
                        .map(|k| (k.as_str(), "lib"))
                        .unwrap_or(("src/lib.rs", "lib"))
                });

            let entry_file = sandbox.path().join(entry_rel);

            let mut rustc_args = vec![
                entry_file.to_string_lossy().to_string(),
                "--crate-name".to_string(),
                crate_name.to_string(),
                "--out-dir".to_string(),
                out_dir.to_string_lossy().to_string(),
            ];

            if !compiler_flags.iter().any(|f| f == "--crate-type") {
                rustc_args.push("--crate-type".to_string());
                rustc_args.push(default_crate_type.to_string());
            }

            for flag in compiler_flags {
                rustc_args.push(flag.clone());
            }

            ("rustc", rustc_args, HashMap::new())
        };

        let mut res = self
            .execute_process_supervised(
                task.id,
                sandbox.path(),
                program,
                &args,
                sandbox.path(),
                &compile_env,
                None,
                timeout,
                child_pid,
                cancel_rx,
                start_time,
            )
            .await;

        // On successful compilation, check for generated library / binary artifact; everything
        // placed directly into the output directory is returned as an output file.
        let mut artifact_patterns = Vec::new();
        if res.is_success() {
            if use_cargo {
                for meta_name in ["CACHEDIR.TAG", ".rustc_info.json", ".cargo-lock"] {
                    let _ = tokio::fs::remove_file(out_dir.join(meta_name)).await;
                }
                for profile in ["release", "debug"] {
                    let profile_dir = out_dir.join(profile);
                    if let Ok(mut entries) = tokio::fs::read_dir(&profile_dir).await {
                        while let Ok(Some(entry)) = entries.next_entry().await {
                            if !entry.file_type().await.is_ok_and(|ft| ft.is_file()) {
                                continue;
                            }
                            let fname = entry.file_name().to_string_lossy().to_string();
                            if !fname.starts_with('.')
                                && !fname.ends_with(".d")
                                && !fname.ends_with(".pdb")
                                && !fname.ends_with(".exp")
                                && !fname.ends_with(".ilk")
                            {
                                let _ = tokio::fs::copy(entry.path(), out_dir.join(&fname)).await;
                            }
                        }
                    }
                }
            }
            if let Some(rel_out_dir) = out_dir
                .strip_prefix(sandbox.path())
                .ok()
                .and_then(|rel| rel.to_str())
                .filter(|rel| !rel.is_empty())
            {
                artifact_patterns.push(format!("{}/*", rel_out_dir.replace('\\', "/")));
            }
            let mut found_artifacts = Vec::new();
            if let Ok(mut entries) = tokio::fs::read_dir(&out_dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    if !entry.file_type().await.is_ok_and(|ft| ft.is_file()) {
                        continue;
                    }
                    let fname = entry.file_name().to_string_lossy().to_string();
                    if fname.ends_with(".rlib")
                        || fname.ends_with(".dylib")
                        || fname.ends_with(".so")
                        || fname.ends_with(".a")
                        || fname.ends_with(".dll")
                        || fname == crate_name
                        || fname == format!("{crate_name}.exe")
                        || (use_cargo
                            && !fname.starts_with('.')
                            && !fname.ends_with(".d")
                            && !fname.ends_with(".pdb")
                            && !fname.ends_with(".exp")
                            && !fname.ends_with(".ilk"))
                    {
                        found_artifacts.push(fname);
                    }
                }
            }
            found_artifacts.sort();
            if !found_artifacts.is_empty() {
                let notice = format!(
                    "\n[rusty_grid: compilation artifacts generated in target/: {}]\n",
                    found_artifacts.join(", ")
                );
                let mut combined = BytesMut::from(res.stdout.as_bytes());
                combined.extend_from_slice(notice.as_bytes());
                res.stdout = combined.freeze().into();
            }
        }

        let process_failed = !res.is_success();
        let res = self
            .harvest_and_apply_artifacts(&sandbox, task, res, process_failed)
            .await;
        let res = self
            .collect_blob_outputs(&sandbox, task, res, &artifact_patterns)
            .await;
        let _ = sandbox.destroy().await;
        res
    }

    /// Fallback when `rusty_grid_worker` is compiled without the `3d` Cargo feature.
    #[cfg(not(feature = "3d"))]
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_generative_3d_workload(
        &self,
        task: &Task,
        _spec: &rusty_grid_core::task::Generative3DSpec,
        _checkpoint: Option<&CheckpointData>,
        _checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        _timeout: Duration,
        _child_pid: Option<Arc<AtomicU32>>,
        _cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let msg = "worker was built without the '3d' feature; rebuild with --features 3d to run Generative3D tasks";
        TaskResult::failure(
            self.worker_id,
            task.id,
            EXIT_CODE_GENERAL_ERROR,
            Bytes::new(),
            Bytes::from_static(b"worker was built without the '3d' feature; rebuild with --features 3d to run Generative3D tasks\n"),
            start_time.elapsed().as_millis() as u64,
            Some(msg.to_string()),
        )
    }

    /// Fallback when `rusty_grid_worker` is compiled without the `3d` Cargo feature.
    #[cfg(not(feature = "3d"))]
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_auto_rig_workload(
        &self,
        task: &Task,
        _spec: &rusty_grid_core::task::AutoRigSpec,
        _checkpoint: Option<&CheckpointData>,
        _checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        _timeout: Duration,
        _child_pid: Option<Arc<AtomicU32>>,
        _cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let msg = "worker was built without the '3d' feature; rebuild with --features 3d to run AutoRig tasks";
        TaskResult::failure(
            self.worker_id,
            task.id,
            EXIT_CODE_GENERAL_ERROR,
            Bytes::new(),
            Bytes::from_static(b"worker was built without the '3d' feature; rebuild with --features 3d to run AutoRig tasks\n"),
            start_time.elapsed().as_millis() as u64,
            Some(msg.to_string()),
        )
    }

    /// Executes a `TaskSpec::Generative3D` workload via Tier 0 (SDF) or Tier 1 (Subprocess).
    #[cfg(feature = "3d")]
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_generative_3d_workload(
        &self,
        task: &Task,
        spec: &rusty_grid_core::task::Generative3DSpec,
        _checkpoint: Option<&CheckpointData>,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let engine = match spec.try_engine() {
            Ok(e) => e,
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    Bytes::new(),
                    Bytes::from(format!("Invalid 3D generation engine: {e}")),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("unknown 3D engine: {e}")),
                );
            }
        };

        // 1. Intercept Trellis before spawning Python and fail with explicit unsupported error
        if engine == Engine3D::Trellis {
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_GENERAL_ERROR,
                Bytes::new(),
                Bytes::from_static(
                    b"Trellis 3D neural engine is not supported; use sdf or triposr",
                ),
                start_time.elapsed().as_millis() as u64,
                Some("Trellis 3D neural engine is not supported; use sdf or triposr".to_string()),
            );
        }

        // 2. If explicit SDF requested, execute Tier 0 directly in-process (< 50ms)
        // 3D-1 / D3: auto always runs the SDF tier
        if engine == Engine3D::Sdf || engine == Engine3D::Auto {
            info!(task_id = %task.id, "Executing Tier 0 Procedural SDF engine");
            return self
                .execute_tier0_sdf(
                    task,
                    spec,
                    checkpoint_tx.clone(),
                    timeout,
                    cancel_rx,
                    start_time,
                )
                .await;
        }

        // 3. Check for Python & Entrypoint availability
        let py_bin = crate::gen3d::runner::resolve_python_runtime();
        let script = crate::gen3d::runner::resolve_entrypoint_script();

        if py_bin.is_none() || script.is_none() {
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_GENERAL_ERROR,
                Bytes::new(),
                Bytes::from_static(
                    b"TripoSR model not available: missing Python or engines/generative_3d/entrypoint.py",
                ),
                start_time.elapsed().as_millis() as u64,
                Some(
                    "TripoSR model not available: missing Python or engines/generative_3d/entrypoint.py"
                        .to_string(),
                ),
            );
        }

        let py_bin = py_bin.unwrap();
        let script = script.unwrap();

        // 3. Create isolated sandbox workspace
        let mut sandbox = match Sandbox::create(&self.config.sandbox_config, task.id).await {
            Ok(s) => s,
            Err(e) if e.is_create_refusal() => {
                return base_refusal(self.worker_id, task.id, &e);
            }
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    Bytes::new(),
                    format!("Failed to create 3D task sandbox: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Sandbox error: {e}")),
                );
            }
        };

        // 4. Stage input assets into sandbox
        let mut cmd_args = Vec::new();
        cmd_args.push(script.to_string_lossy().to_string());

        if let Some(ref img_bytes) = spec.image_data {
            let input_img_path = sandbox.path().join("input_concept.png");
            if let Err(e) = tokio::fs::write(&input_img_path, img_bytes.as_slice()).await {
                let _ = sandbox.destroy().await;
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    Bytes::new(),
                    format!("Failed to write input image to sandbox: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("I/O error: {e}")),
                );
            }
            cmd_args.push("--input".to_string());
            cmd_args.push(input_img_path.to_string_lossy().to_string());
        }

        if let Some(ref prompt) = spec.prompt {
            cmd_args.push("--prompt".to_string());
            cmd_args.push(prompt.clone());
        }

        let output_glb_path = sandbox.path().join("output_model.glb");
        cmd_args.push("--output".to_string());
        cmd_args.push(output_glb_path.to_string_lossy().to_string());

        // Device Selection
        let device = if self.capabilities.has_cuda {
            "cuda:0"
        } else {
            "cpu"
        };
        cmd_args.push("--device".to_string());
        cmd_args.push(device.to_string());

        cmd_args.push("--quality".to_string());
        cmd_args.push(spec.quality.clone());

        if spec.export_texture {
            cmd_args.push("--export-texture".to_string());
        } else {
            cmd_args.push("--no-texture".to_string());
        }

        cmd_args.push("--offline".to_string());

        // 5. Prepare environment and route through hardened process runner
        let mut child_env = HashMap::new();
        child_env.insert("HF_HUB_OFFLINE".to_string(), "1".to_string());
        child_env.insert("TRANSFORMERS_OFFLINE".to_string(), "1".to_string());

        let mut res = self
            .execute_process_supervised(
                task.id,
                sandbox.path(),
                &py_bin.to_string_lossy(),
                &cmd_args,
                sandbox.path(),
                &child_env,
                None,
                timeout,
                child_pid,
                cancel_rx,
                start_time,
            )
            .await;

        // On cancellation or watchdog timeout, return failure immediately without sending checkpoints
        if res.exit_code == EXIT_CODE_CANCELLED
            || (res.exit_code == EXIT_CODE_TIMEOUT && res.error.is_some())
        {
            let _ = sandbox.destroy().await;
            return res;
        }

        // Process progress checkpoints from stdout if checkpoint_tx provided
        if let Some(ref tx) = checkpoint_tx {
            let stdout_str = String::from_utf8_lossy(res.stdout.as_slice());
            let mut seq = 1u64;
            for line in stdout_str.lines() {
                if let Some((frac, msg)) = crate::gen3d::runner::parse_progress_marker(line) {
                    let delta = serde_json::json!({
                        "progress": frac,
                        "message": msg,
                        "stage": "generative_3d",
                    });
                    let _ = tx
                        .send(WorkerMessage::Checkpoint {
                            task_id: task.id,
                            sequence: seq,
                            delta_state: Bytes::from(delta.to_string().into_bytes()),
                        })
                        .await;
                    seq += 1;
                }
            }
        }

        // Validate glTF 2.0 binary header if execution succeeded
        if res.is_success() {
            match read_output_file(&output_glb_path).await {
                Ok(Some(data)) => {
                    if data.len() >= 12 && data.starts_with(b"glTF") {
                        let stdout_str = String::from_utf8_lossy(res.stdout.as_slice());
                        let mut engine_label = None;
                        for line in stdout_str.lines() {
                            if let Some(label) = crate::gen3d::runner::parse_engine_marker(line) {
                                engine_label = Some(label);
                            }
                        }
                        res.asset_bytes = Some(Bytes::from(data));
                        if let Some(device) = engine_label {
                            res = res.with_device_name(device);
                        }
                        res.is_gpu_executed = self.capabilities.has_cuda;
                    } else {
                        let _ = sandbox.destroy().await;
                        return TaskResult::failure(
                            self.worker_id,
                            task.id,
                            EXIT_CODE_GENERAL_ERROR,
                            res.stdout,
                            res.stderr,
                            start_time.elapsed().as_millis() as u64,
                            Some("Generated .glb file has invalid glTF 2.0 header".to_string()),
                        );
                    }
                }
                Ok(None) => {
                    let _ = sandbox.destroy().await;
                    return TaskResult::failure(
                        self.worker_id,
                        task.id,
                        EXIT_CODE_GENERAL_ERROR,
                        res.stdout,
                        res.stderr,
                        start_time.elapsed().as_millis() as u64,
                        Some(format!(
                            "Process succeeded but expected output file '{}' was not produced",
                            output_glb_path.display()
                        )),
                    );
                }
                Err(e) => {
                    let _ = sandbox.destroy().await;
                    return TaskResult::failure(
                        self.worker_id,
                        task.id,
                        EXIT_CODE_GENERAL_ERROR,
                        res.stdout,
                        res.stderr,
                        start_time.elapsed().as_millis() as u64,
                        Some(format!("Failed to read generated .glb asset: {e}")),
                    );
                }
            }
        } else if res.error.as_deref().unwrap_or("").is_empty() {
            let last_err_line = String::from_utf8_lossy(res.stderr.as_slice())
                .lines()
                .map(|l| l.trim())
                .rfind(|l| !l.is_empty())
                .map(|s| s.to_string());

            let err_msg = match last_err_line {
                Some(line) => format!(
                    "3D engine subprocess failed with exit code {}: {line}",
                    res.exit_code
                ),
                None => format!(
                    "3D engine subprocess failed with exit code {}",
                    res.exit_code
                ),
            };
            res.error = Some(err_msg);
        }

        let _ = sandbox.destroy().await;
        res
    }

    /// Raises the cooperative stop flag when dropped (cancel, timeout, or future abort).
    #[cfg(feature = "3d")]
    fn make_stop_guard(stop: Arc<AtomicBool>) -> WorkloadStopOnDrop {
        WorkloadStopOnDrop(stop)
    }

    /// Direct Tier 0 Procedural Signed Distance Function mesh generator (< 50 ms)
    /// with incremental milestone progress checkpoints (25%, 50%, 75%, 100%).
    #[cfg(feature = "3d")]
    async fn execute_tier0_sdf(
        &self,
        task: &Task,
        spec: &rusty_grid_core::task::Generative3DSpec,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        timeout: Duration,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let task_id = task.id;

        if let Some(reason) = check_cancelled_now(&mut cancel_rx) {
            let elapsed = start_time.elapsed().as_millis() as u64;
            let err_msg = if reason.starts_with("Cancelled:") {
                reason
            } else {
                format!("Cancelled: {reason}")
            };
            return TaskResult::failure(
                self.worker_id,
                task_id,
                EXIT_CODE_CANCELLED,
                Bytes::new(),
                Bytes::new(),
                elapsed,
                Some(err_msg),
            );
        }

        // Reject image input on SDF tier
        if spec.image_data.is_some() {
            let elapsed = start_time.elapsed().as_millis() as u64;
            return TaskResult::failure(
                self.worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                Bytes::new(),
                Bytes::from_static(
                    b"Tier 0 procedural SDF engine does not support image input (text prompt only)\n",
                ),
                elapsed,
                Some("Tier 0 procedural SDF engine does not support image input (text prompt only)".to_string()),
            );
        }

        let (prog_tx, mut prog_rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_forward = checkpoint_tx.clone();
        let forward_handle = tokio::spawn(async move {
            let mut seq = 1u64;
            while let Some((frac, stage, msg)) = prog_rx.recv().await {
                if let Some(ref tx) = tx_forward {
                    let delta = serde_json::json!({
                        "progress": frac,
                        "stage": stage,
                        "message": msg,
                    });
                    let _ = tx
                        .send(WorkerMessage::Checkpoint {
                            task_id,
                            sequence: seq,
                            delta_state: Bytes::from(delta.to_string().into_bytes()),
                        })
                        .await;
                    seq += 1;
                }
            }
        });

        let stop = Arc::new(AtomicBool::new(false));
        let _stop_on_exit = Self::make_stop_guard(Arc::clone(&stop));
        let stop_clone = Arc::clone(&stop);
        let spec_clone = spec.clone();

        let mut compute_handle = tokio::task::spawn_blocking(move || {
            crate::gen3d::generate_procedural_3d_cancellable(
                &spec_clone,
                move |frac, stage, msg| {
                    let _ = prog_tx.send((frac, stage, msg));
                },
                &|| stop_clone.load(Ordering::Relaxed),
            )
        });

        tokio::select! {
            biased;

            reason = cancellation(&mut cancel_rx) => {
                stop.store(true, Ordering::Release);
                forward_handle.abort();
                let _ = tokio::time::timeout(Duration::from_millis(200), &mut compute_handle).await;
                let elapsed = start_time.elapsed().as_millis() as u64;
                let err_msg = if reason.starts_with("Cancelled:") {
                    reason
                } else {
                    format!("Cancelled: {reason}")
                };
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_CANCELLED,
                    Bytes::new(),
                    Bytes::new(),
                    elapsed,
                    Some(err_msg),
                )
            }

            _ = tokio::time::sleep(timeout) => {
                stop.store(true, Ordering::Release);
                forward_handle.abort();
                let _ = tokio::time::timeout(Duration::from_millis(200), &mut compute_handle).await;
                let elapsed = start_time.elapsed().as_millis() as u64;
                let msg = format!("Task timed out after {}s", timeout.as_secs());
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_TIMEOUT,
                    Bytes::new(),
                    Bytes::from(msg.clone()),
                    elapsed,
                    Some(msg),
                )
            }

            joined = &mut compute_handle => {
                let _ = forward_handle.await;
                match joined {
                    Ok(Ok((glb_bytes, stats))) => {
                        let elapsed = start_time.elapsed().as_millis() as u64;
                        let mut res = TaskResult::success(
                            self.worker_id,
                            task_id,
                            Bytes::from(format!(
                                "Tier 0 procedural SDF mesh generated: {} vertices, {} triangles in {} ms\n",
                                stats.vertex_count, stats.triangle_count, stats.execution_time_ms
                            )),
                            elapsed,
                            false,
                        );
                        res = res.with_asset_bytes(Bytes::from(glb_bytes));
                        res = res.with_device_name("Procedural-SDF-PureRust");
                        res
                    }
                    Ok(Err(e)) => {
                        let elapsed = start_time.elapsed().as_millis() as u64;
                        TaskResult::failure(
                            self.worker_id,
                            task_id,
                            EXIT_CODE_GENERAL_ERROR,
                            Bytes::new(),
                            format!("Procedural SDF generation error: {e}"),
                            elapsed,
                            Some(format!("SDF error: {e}")),
                        )
                    }
                    Err(e) => {
                        let elapsed = start_time.elapsed().as_millis() as u64;
                        TaskResult::failure(
                            self.worker_id,
                            task_id,
                            EXIT_CODE_GENERAL_ERROR,
                            Bytes::new(),
                            format!("Compute join error: {e}"),
                            elapsed,
                            Some(format!("Join error: {e}")),
                        )
                    }
                }
            }
        }
    }

    /// Executes an automated skeleton rigging and animation synthesis workload (`TaskSpec::AutoRig`)
    /// with incremental milestone progress checkpoints (25%, 50%, 75%, 100%).
    #[cfg(feature = "3d")]
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_auto_rig_workload(
        &self,
        task: &Task,
        spec: &rusty_grid_core::task::AutoRigSpec,
        _checkpoint: Option<&CheckpointData>,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let task_id = task.id;
        if let Some(reason) = check_cancelled_now(&mut cancel_rx) {
            let elapsed = start_time.elapsed().as_millis() as u64;
            let err_msg = if reason.starts_with("Cancelled:") {
                reason
            } else {
                format!("Cancelled: {reason}")
            };
            return TaskResult::failure(
                self.worker_id,
                task_id,
                EXIT_CODE_CANCELLED,
                Bytes::new(),
                Bytes::new(),
                elapsed,
                Some(err_msg),
            );
        }

        let (prog_tx, mut prog_rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_forward = checkpoint_tx.clone();

        // Background forwarder for progress checkpoints
        let forward_handle = tokio::spawn(async move {
            let mut seq = 1u64;
            while let Some((frac, stage, msg)) = prog_rx.recv().await {
                if let Some(ref tx) = tx_forward {
                    let delta = serde_json::json!({
                        "progress": frac,
                        "stage": stage,
                        "message": msg,
                    });
                    let _ = tx
                        .send(WorkerMessage::Checkpoint {
                            task_id,
                            sequence: seq,
                            delta_state: Bytes::from(delta.to_string().into_bytes()),
                        })
                        .await;
                    seq += 1;
                }
            }
        });

        let result = crate::autorig::auto_rig_mesh_async_supervised(
            self,
            task_id,
            spec,
            timeout,
            child_pid,
            cancel_rx,
            start_time,
            move |frac, stage, msg| {
                let _ = prog_tx.send((frac, stage, msg.to_string()));
            },
        )
        .await;

        match result {
            Ok(outcome) => {
                let _ = forward_handle.await;
                let elapsed = start_time.elapsed().as_millis() as u64;
                let mut res = TaskResult::success(
                    self.worker_id,
                    task.id,
                    Bytes::from(format!(
                        "Auto-rigging completed successfully (provider: {}): {} bytes .glb generated in {} ms\n",
                        outcome.provider.as_str(),
                        outcome.glb_bytes.len(),
                        elapsed
                    )),
                    elapsed,
                    false,
                );
                res = res.with_asset_bytes(Bytes::from(outcome.glb_bytes));
                res = res.with_device_name(outcome.provider.device_name());
                res
            }
            Err(crate::autorig::WorkerError::Cancelled(reason)) => {
                forward_handle.abort();
                let elapsed = start_time.elapsed().as_millis() as u64;
                let err_msg = if reason.starts_with("Cancelled:") {
                    reason
                } else {
                    format!("Cancelled: {reason}")
                };
                TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_CANCELLED,
                    Bytes::new(),
                    Bytes::new(),
                    elapsed,
                    Some(err_msg),
                )
            }
            Err(crate::autorig::WorkerError::TimedOut(msg)) => {
                forward_handle.abort();
                let elapsed = start_time.elapsed().as_millis() as u64;
                TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_TIMEOUT,
                    Bytes::new(),
                    Bytes::from(msg.clone()),
                    elapsed,
                    Some(msg),
                )
            }
            Err(e) => {
                let _ = forward_handle.await;
                let elapsed = start_time.elapsed().as_millis() as u64;
                TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    Bytes::new(),
                    Bytes::from(format!("Auto-rigging execution error: {e}")),
                    elapsed,
                    Some(format!("Auto-rig error: {e}")),
                )
            }
        }
    }
}

#[cfg(feature = "3d")]
struct WorkloadStopOnDrop(Arc<AtomicBool>);

#[cfg(feature = "3d")]
impl Drop for WorkloadStopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
