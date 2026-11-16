//! Child process supervision, Windows executable/PATH resolution, shell lookup, and tree teardown.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{info, warn};

use rusty_grid_core::task::{TaskId, TaskResult};

use crate::isolation::IsolationLevel;
use crate::process_tree::{configure_command, ProcessTree};
use crate::sandbox::sanitize_relative_path;

use super::capture::{append_note, cancellation, check_cancelled_now, AbortOnDrop, OutputReaders};
use super::{
    TaskRunner, EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_INTERRUPTED,
    EXIT_CODE_TIMEOUT, POST_KILL_DRAIN, REAP_TIMEOUT, SECRET_ENV_VARS,
};

#[cfg(windows)]
pub(super) use rusty_grid_core::capabilities::{
    resolve_windows_executable, resolve_windows_executable_for_isolation_in_dir,
    resolve_windows_executable_in_dir,
};

#[cfg(windows)]
pub(super) fn augment_windows_path(existing_path: &str) -> String {
    let augmented = rusty_grid_core::capabilities::augment_windows_path(existing_path);
    prepend_msvc_bin_if_present(augmented)
}

#[cfg(windows)]
pub(super) fn augment_windows_path_for_isolation(existing_path: &str) -> String {
    let augmented =
        rusty_grid_core::capabilities::augment_windows_path_for_isolation(existing_path);
    prepend_msvc_bin_if_present(augmented)
}

#[cfg(windows)]
fn prepend_msvc_bin_if_present(augmented: String) -> String {
    if let Some((msvc_bin, _)) = crate::isolation::windows::resolve_windows_msvc_env() {
        let msvc_str = msvc_bin.to_string_lossy();
        let lower_existing = augmented.to_ascii_lowercase();
        if !lower_existing.contains(&msvc_str.to_ascii_lowercase()) && msvc_bin.is_dir() {
            if augmented.is_empty() {
                return msvc_str.to_string();
            }
            return format!("{msvc_str};{augmented}");
        }
    }
    augmented
}

/// Resolves the worker's active `rustup` toolchain name so `RUSTUP_TOOLCHAIN` can be pinned for
/// `cargo build --offline` tasks, preventing a task's `rust-toolchain.toml` or `rust-toolchain`
/// file from triggering network access or toolchain installation.
pub(super) fn detect_worker_rustup_toolchain(
    host_dirs: &crate::isolation::HostDirs,
) -> Option<String> {
    if let Ok(tc) = std::env::var("RUSTUP_TOOLCHAIN") {
        let trimmed = tc.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    let settings_path = host_dirs.rustup_home.join("settings.toml");
    let contents = std::fs::read_to_string(settings_path).ok()?;
    for line in contents.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("default_toolchain") {
            let rest = rest.trim_start();
            if let Some(val) = rest.strip_prefix('=') {
                let unquoted = val.trim().trim_matches('"').trim_matches('\'').trim();
                if !unquoted.is_empty() {
                    return Some(unquoted.to_string());
                }
            }
        }
    }
    None
}

/// Validates and filters `compiler_flags` for the `cargo build --offline` path of
/// `TaskSpec::RustCompilation`.
///
/// Refuses `--config`, `-Z`, `--manifest-path`, `--online`, `--target-dir` outside `sandbox_root`,
/// and any flag not on the `cargo build` allowlist. Returns the sanitized flags and an optional
/// in-sandbox `--target-dir` override.
pub(super) fn validate_cargo_compiler_flags(
    compiler_flags: &[String],
    sandbox_root: &Path,
) -> Result<(Vec<String>, Option<PathBuf>), String> {
    const SAFE_ZERO_ARG_FLAGS: &[&str] = &[
        "--release",
        "-r",
        "--all-features",
        "--no-default-features",
        "--locked",
        "--frozen",
        "--lib",
        "--bins",
        "--examples",
        "--tests",
        "--benches",
        "--all-targets",
        "--workspace",
        "--all",
        "--verbose",
        "-v",
        "-vv",
        "--quiet",
        "-q",
        "--keep-going",
        "--timings",
        "--future-incompat-report",
        "--ignore-rust-version",
        "--unit-graph",
        "--build-plan",
    ];
    const SAFE_VALUE_FLAGS: &[&str] = &[
        "--features",
        "-F",
        "--bin",
        "--example",
        "--test",
        "--bench",
        "--package",
        "-p",
        "--profile",
        "--target",
        "--jobs",
        "-j",
        "--color",
        "--message-format",
    ];

    let mut out = Vec::new();
    let mut override_target_dir = None;
    let mut idx = 0;
    while idx < compiler_flags.len() {
        let flag = compiler_flags[idx].as_str();
        if flag == "--offline" {
            // Already passed unconditionally by the runner.
            idx += 1;
            continue;
        }
        if flag == "--config" || flag.starts_with("--config=") {
            return Err(format!(
                "flag '{flag}' is disallowed for cargo build --offline (--config is refused)"
            ));
        }
        if flag == "-Z" || flag.starts_with("-Z") {
            return Err(format!(
                "flag '{flag}' is disallowed for cargo build --offline (-Z unstable flags are refused)"
            ));
        }
        if flag == "--manifest-path" || flag.starts_with("--manifest-path=") {
            return Err(format!(
                "flag '{flag}' is disallowed for cargo build --offline (--manifest-path is refused)"
            ));
        }
        if flag == "--online" || flag.starts_with("--online=") {
            return Err(format!(
                "flag '{flag}' is disallowed for cargo build --offline (builds must stay offline)"
            ));
        }
        if flag == "--target-dir" || flag.starts_with("--target-dir=") {
            let raw_dir = if let Some(rest) = flag.strip_prefix("--target-dir=") {
                idx += 1;
                rest
            } else {
                let Some(next) = compiler_flags.get(idx + 1) else {
                    return Err(
                        "flag '--target-dir' requires a relative path inside the sandbox"
                            .to_string(),
                    );
                };
                idx += 2;
                next.as_str()
            };
            let resolved =
                sanitize_relative_path(sandbox_root, Path::new(raw_dir)).map_err(|e| {
                    format!("--target-dir '{raw_dir}' must stay inside the sandbox: {e}")
                })?;
            override_target_dir = Some(resolved);
            continue;
        }
        if SAFE_ZERO_ARG_FLAGS.contains(&flag) {
            out.push(flag.to_string());
            idx += 1;
            continue;
        }
        if let Some((prefix, val)) = flag.split_once('=') {
            if prefix == "--timings" || SAFE_VALUE_FLAGS.contains(&prefix) {
                if val.is_empty() {
                    return Err(format!("flag '{flag}' has an empty value"));
                }
                if prefix == "--target"
                    && (val.contains('/') || val.contains('\\') || val.ends_with(".json"))
                {
                    return Err(format!(
                        "flag '{flag}' is disallowed: custom target paths or JSON specs are refused"
                    ));
                }
                out.push(flag.to_string());
                idx += 1;
                continue;
            }
        }
        if SAFE_VALUE_FLAGS.contains(&flag) {
            let Some(val) = compiler_flags.get(idx + 1) else {
                return Err(format!("flag '{flag}' is missing its value"));
            };
            if val.is_empty() || val.starts_with('-') {
                return Err(format!("flag '{flag}' has an invalid value '{val}'"));
            }
            if flag == "--target"
                && (val.contains('/') || val.contains('\\') || val.ends_with(".json"))
            {
                return Err(format!(
                    "flag '--target {val}' is disallowed: custom target paths or JSON specs are refused"
                ));
            }
            out.push(flag.to_string());
            out.push(val.clone());
            idx += 2;
            continue;
        }
        return Err(format!(
            "flag '{flag}' is not on the allowlist for cargo build --offline"
        ));
    }
    Ok((out, override_target_dir))
}

/// Shell that runs a `ShellScript` without an explicit interpreter on Unix-like systems.
///
/// `/bin/sh` where it exists; otherwise the first `sh` on `path` (Termux keeps its shell in
/// `$PREFIX/bin`, which is on its PATH); otherwise the Android system shell `/system/bin/sh`;
/// otherwise the bare name `sh`, left to the operating system to resolve.
#[cfg(not(windows))]
pub(super) fn resolve_default_shell(path: &str) -> String {
    pick_shell(&["/bin/sh"], path, &["/system/bin/sh"])
}

/// [`resolve_default_shell`] with the fixed locations as parameters (for tests).
#[cfg(not(windows))]
pub(super) fn pick_shell(preferred: &[&str], path: &str, fallbacks: &[&str]) -> String {
    let existing = |candidate: &str| Path::new(candidate).is_file();
    if let Some(found) = preferred.iter().find(|candidate| existing(candidate)) {
        return found.to_string();
    }
    if let Some(found) = std::env::split_paths(path)
        .map(|dir| dir.join("sh"))
        .find(|candidate| candidate.is_file())
    {
        return found.to_string_lossy().into_owned();
    }
    if let Some(found) = fallbacks.iter().find(|candidate| existing(candidate)) {
        return found.to_string();
    }
    "sh".to_string()
}

/// Human-readable description of how a process ended, as `(exit_code, error)`; see the table in
/// the module docs. Only called for an unsuccessful status.
pub(super) fn describe_exit(status: &std::process::ExitStatus) -> (i32, Option<String>) {
    match status.code() {
        Some(130) => (
            EXIT_CODE_INTERRUPTED,
            Some(format!(
                "Process exited with status 130 (interrupted); reported as {EXIT_CODE_INTERRUPTED} because 130 means \"cancelled\""
            )),
        ),
        Some(code) => (code, None),
        None => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                if let Some(signal) = status.signal() {
                    let name = signal_name(signal);
                    return if signal == 2 {
                        (
                            EXIT_CODE_INTERRUPTED,
                            Some(format!(
                                "Process terminated by signal 2 (SIGINT); reported as {EXIT_CODE_INTERRUPTED} because 130 means \"cancelled\""
                            )),
                        )
                    } else {
                        (128 + signal, Some(format!("Process terminated by signal {signal}{name}")))
                    };
                }
            }
            (
                EXIT_CODE_GENERAL_ERROR,
                Some("Process ended without an exit status".to_string()),
            )
        }
    }
}

/// ` (SIGKILL)`-style suffix for the common signals, empty for the rest.
#[cfg(unix)]
fn signal_name(signal: i32) -> &'static str {
    match signal {
        1 => " (SIGHUP)",
        2 => " (SIGINT)",
        3 => " (SIGQUIT)",
        6 => " (SIGABRT)",
        9 => " (SIGKILL)",
        11 => " (SIGSEGV)",
        13 => " (SIGPIPE)",
        14 => " (SIGALRM)",
        15 => " (SIGTERM)",
        _ => "",
    }
}

/// Extra process context for a task running inside a persistent named workspace.
pub(crate) struct WorkspaceProcessCtx<'a> {
    pub tmp_dir: &'a Path,
    pub workspace_root: &'a Path,
    pub task_env: Vec<(&'static str, String)>,
}

/// Validates and creates `sub` inside `tree_dir`, refusing absolute paths, `..`, NUL, and any
/// symbolic link (or reparse point) along the path so the task's `cwd` never escapes `tree_dir`.
pub(super) fn resolve_workspace_cwd(tree_dir: &Path, sub: &Path) -> Result<PathBuf, String> {
    let target = sanitize_relative_path(tree_dir, sub)
        .map_err(|e| format!("illegal working_dir '{}': {e}", sub.display()))?;
    let rel = target
        .strip_prefix(tree_dir)
        .map_err(|_| format!("illegal working_dir '{}'", sub.display()))?;
    let mut cur = tree_dir.to_path_buf();
    for component in rel.components() {
        let std::path::Component::Normal(part) = component else {
            return Err(format!(
                "illegal working_dir '{}': only normal relative path components are allowed",
                sub.display()
            ));
        };
        cur.push(part);
        match std::fs::symlink_metadata(&cur) {
            Ok(meta) => {
                #[cfg(windows)]
                let is_reparse = {
                    use std::os::windows::fs::MetadataExt;
                    (meta.file_attributes() & 0x0400) != 0
                };
                #[cfg(not(windows))]
                let is_reparse = false;
                if meta.file_type().is_symlink() || is_reparse {
                    return Err(format!(
                        "illegal working_dir '{}': '{}' is a symbolic link",
                        sub.display(),
                        cur.display()
                    ));
                }
                if !meta.is_dir() {
                    return Err(format!(
                        "illegal working_dir '{}': '{}' is not a directory",
                        sub.display(),
                        cur.display()
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&cur).map_err(|e| {
                    format!(
                        "cannot create working_dir '{}' in workspace tree: {e}",
                        sub.display()
                    )
                })?;
            }
            Err(e) => {
                return Err(format!(
                    "cannot inspect working_dir '{}' in workspace tree: {e}",
                    sub.display()
                ));
            }
        }
    }
    if let (Ok(canon_tree), Ok(canon_target)) = (tree_dir.canonicalize(), target.canonicalize()) {
        if !canon_target.starts_with(&canon_tree) {
            return Err(format!(
                "illegal working_dir '{}': resolves outside workspace tree",
                sub.display()
            ));
        }
    }
    Ok(target)
}

impl TaskRunner {
    /// Spawns a child process with deadlock-free bounded I/O, watchdog timeouts, and cooperative cancellation.
    ///
    /// The child leads its own process tree (see [`crate::process_tree`]). Whatever ends the run
    /// (exit, cancellation, timeout, or this future being dropped) the whole tree is killed, so no
    /// grandchild outlives the task. The run is over when the direct child has exited and its
    /// output has been drained for at most `pipe_drain_grace`: a background process that kept the
    /// output pipes open cannot turn a finished task into a timeout.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_process_supervised(
        &self,
        task_id: TaskId,
        sandbox_path: &Path,
        program: &str,
        args: &[String],
        cwd: &Path,
        env: &HashMap<String, String>,
        stdin_data: Option<Vec<u8>>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        self.execute_process_supervised_with_workspace(
            task_id,
            sandbox_path,
            program,
            args,
            cwd,
            env,
            stdin_data,
            timeout,
            child_pid,
            cancel_rx,
            start_time,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_process_supervised_with_workspace(
        &self,
        task_id: TaskId,
        sandbox_path: &Path,
        program: &str,
        args: &[String],
        cwd: &Path,
        env: &HashMap<String, String>,
        stdin_data: Option<Vec<u8>>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
        workspace_ctx: Option<WorkspaceProcessCtx<'_>>,
    ) -> TaskResult {
        if let Some(reason) = check_cancelled_now(&mut cancel_rx) {
            return TaskResult::failure(
                self.worker_id,
                task_id,
                EXIT_CODE_CANCELLED,
                "",
                "",
                start_time.elapsed().as_millis() as u64,
                Some(format!("Cancelled: {reason}")),
            );
        }

        let effective_path = match env.get("PATH") {
            Some(p) => {
                #[cfg(windows)]
                {
                    if self.config.isolation != IsolationLevel::Off {
                        augment_windows_path_for_isolation(p)
                    } else {
                        augment_windows_path(p)
                    }
                }
                #[cfg(not(windows))]
                {
                    p.clone()
                }
            }
            None => {
                let base = std::env::var("PATH").unwrap_or_default();
                #[cfg(windows)]
                {
                    if self.config.isolation != IsolationLevel::Off {
                        augment_windows_path_for_isolation(&base)
                    } else {
                        augment_windows_path(&base)
                    }
                }
                #[cfg(not(windows))]
                {
                    base
                }
            }
        };

        #[cfg(windows)]
        let effective_path = if self.config.isolation != IsolationLevel::Off {
            effective_path
                .split(';')
                .map(|part| {
                    if (part.contains('~') || part.contains('/')) && !part.is_empty() {
                        let p = Path::new(part);
                        if p.exists() && !crate::isolation::windows::is_reparse_point(p) {
                            return crate::isolation::windows::normalize_win_path(p)
                                .to_string_lossy()
                                .into_owned();
                        }
                    }
                    part.to_string()
                })
                .collect::<Vec<_>>()
                .join(";")
        } else {
            effective_path
        };

        #[cfg(windows)]
        let (eff_sandbox_path, eff_cwd, eff_sandbox_base) = if self.config.isolation
            != IsolationLevel::Off
        {
            let sp = if sandbox_path.exists()
                && !crate::isolation::windows::is_reparse_point(sandbox_path)
            {
                crate::isolation::windows::normalize_win_path(sandbox_path)
            } else {
                std::path::absolute(sandbox_path).unwrap_or_else(|_| sandbox_path.to_path_buf())
            };
            let cw = if cwd.exists() && !crate::isolation::windows::is_reparse_point(cwd) {
                crate::isolation::windows::normalize_win_path(cwd)
            } else {
                std::path::absolute(cwd).unwrap_or_else(|_| cwd.to_path_buf())
            };
            let sb = if self.config.sandbox_config.base_dir.exists()
                && !crate::isolation::windows::is_reparse_point(
                    &self.config.sandbox_config.base_dir,
                ) {
                crate::isolation::windows::normalize_win_path(&self.config.sandbox_config.base_dir)
            } else {
                std::path::absolute(&self.config.sandbox_config.base_dir)
                    .unwrap_or_else(|_| self.config.sandbox_config.base_dir.clone())
            };
            (sp, cw, sb)
        } else {
            (
                std::path::absolute(sandbox_path).unwrap_or_else(|_| sandbox_path.to_path_buf()),
                std::path::absolute(cwd).unwrap_or_else(|_| cwd.to_path_buf()),
                std::path::absolute(&self.config.sandbox_config.base_dir)
                    .unwrap_or_else(|_| self.config.sandbox_config.base_dir.clone()),
            )
        };
        #[cfg(not(windows))]
        let (eff_sandbox_path, eff_cwd, eff_sandbox_base) = (
            sandbox_path.to_path_buf(),
            cwd.to_path_buf(),
            self.config.sandbox_config.base_dir.clone(),
        );

        #[cfg(windows)]
        let resolved_program = {
            let res = if self.config.isolation != IsolationLevel::Off {
                resolve_windows_executable_for_isolation_in_dir(
                    program,
                    &effective_path,
                    Some(&eff_cwd),
                    Some(&eff_sandbox_path),
                )
            } else {
                resolve_windows_executable_in_dir(
                    program,
                    &effective_path,
                    Some(&eff_cwd),
                    Some(&eff_sandbox_path),
                )
            };
            match res {
                Ok(p) => p,
                Err(e) => {
                    return TaskResult::failure(
                        self.worker_id,
                        task_id,
                        EXIT_CODE_GENERAL_ERROR,
                        "",
                        format!("Refusing task program '{program}': {e}"),
                        start_time.elapsed().as_millis() as u64,
                        Some(format!("Invalid program path '{program}': {e}")),
                    );
                }
            }
        };
        #[cfg(not(windows))]
        let resolved_program = program.to_string();

        // Under `--isolation fs|fs+net` the task is confined by the platform's mechanism (macOS:
        // `sandbox-exec` wraps the program; Linux: a `pre_exec` hook registered below). When
        // that is impossible the task fails, it never runs plain.
        let ws_root_fallback = self.workspace_root_if_open();
        #[cfg(windows)]
        let (eff_extra_writable_dir, eff_workspace_root) = if self.config.isolation
            != IsolationLevel::Off
        {
            let extra = workspace_ctx.as_ref().map(|w| {
                if w.tmp_dir.exists() && !crate::isolation::windows::is_reparse_point(w.tmp_dir) {
                    crate::isolation::windows::normalize_win_path(w.tmp_dir)
                } else {
                    w.tmp_dir.to_path_buf()
                }
            });
            let ws_r = workspace_ctx
                .as_ref()
                .map(|w| w.workspace_root)
                .or(ws_root_fallback.as_deref())
                .map(|r| {
                    if r.exists() && !crate::isolation::windows::is_reparse_point(r) {
                        crate::isolation::windows::normalize_win_path(r)
                    } else {
                        r.to_path_buf()
                    }
                });
            (extra, ws_r)
        } else {
            (
                workspace_ctx.as_ref().map(|w| w.tmp_dir.to_path_buf()),
                workspace_ctx
                    .as_ref()
                    .map(|w| w.workspace_root.to_path_buf())
                    .or(ws_root_fallback.clone()),
            )
        };
        #[cfg(not(windows))]
        let (eff_extra_writable_dir, eff_workspace_root) = (
            workspace_ctx.as_ref().map(|w| w.tmp_dir.to_path_buf()),
            workspace_ctx
                .as_ref()
                .map(|w| w.workspace_root.to_path_buf())
                .or(ws_root_fallback.clone()),
        );

        let jail = (self.config.isolation != IsolationLevel::Off).then(|| crate::isolation::Jail {
            level: self.config.isolation,
            host: &self.config.host_dirs,
            protected_files: &self.config.protected_files,
            sandbox_base: &eff_sandbox_base,
            sandbox_dir: &eff_sandbox_path,
            extra_writable_dir: eff_extra_writable_dir.as_deref(),
            workspace_root: eff_workspace_root.as_deref(),
        });
        let isolation_failure = |e: &str| {
            TaskResult::failure(
                self.worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                format!(
                    "Cannot run '{program}' with --isolation {}: {e}",
                    self.config.isolation
                ),
                start_time.elapsed().as_millis() as u64,
                Some(format!(
                    "Isolation error for '{program}' ({}): {e}",
                    self.config.isolation
                )),
            )
        };
        let (spawn_program, spawn_args) = match &jail {
            None => (resolved_program.clone(), args.to_vec()),
            Some(jail) => {
                match crate::isolation::wrap_program_in_dir(
                    jail,
                    &eff_cwd,
                    &resolved_program,
                    args,
                    &effective_path,
                ) {
                    Ok(wrapped) => wrapped,
                    Err(e) => return isolation_failure(&e),
                }
            }
        };

        let mut cmd = tokio::process::Command::new(&spawn_program);
        cmd.args(&spawn_args);
        cmd.current_dir(&eff_cwd);

        // A confined task gets an allowlisted environment, not a copy of the worker's: tokens and
        // credentials in the worker's environment are none of its business (design 0033 D1.4).
        if self.config.isolation != IsolationLevel::Off {
            cmd.env_clear();
            for (name, value) in crate::isolation::inherited_environment(std::env::vars_os()) {
                cmd.env(name, value);
            }
        }

        // The worker's own secrets are none of the task's business. Removed before the task's own
        // environment is applied, so a task that is handed one explicitly still gets it.
        for name in SECRET_ENV_VARS {
            cmd.env_remove(name);
        }

        cmd.env("PATH", &effective_path);

        // Inject user environment variables (skipping PATH to preserve effective_path)
        for (k, v) in env {
            if k != "PATH" {
                #[cfg(windows)]
                if self.config.isolation != IsolationLevel::Off
                    && (k.eq_ignore_ascii_case("RUSTC") || k.eq_ignore_ascii_case("CARGO"))
                    && (v.contains('~') || v.contains('/'))
                {
                    let p = Path::new(v);
                    if p.exists() && !crate::isolation::windows::is_reparse_point(p) {
                        cmd.env(k, crate::isolation::windows::normalize_win_path(p));
                        continue;
                    }
                }
                cmd.env(k, v);
            }
        }

        // Inject standard framework diagnostics
        cmd.env("RUSTY_GRID_TASK_ID", task_id.to_string());
        cmd.env("RUSTY_GRID_WORKER_ID", self.worker_id.to_string());
        cmd.env(
            "RUSTY_GRID_SANDBOX_DIR",
            eff_sandbox_path.to_string_lossy().to_string(),
        );

        if self.config.isolation != IsolationLevel::Off {
            let home_and_tmp = eff_extra_writable_dir
                .as_deref()
                .unwrap_or(&eff_sandbox_path);
            let plan =
                crate::isolation::prepare_environment(&self.config.host_dirs, env, home_and_tmp);
            for (name, value) in &plan.set {
                cmd.env(name, value);
            }
            for name in &plan.remove {
                cmd.env_remove(name);
            }
            #[cfg(windows)]
            if !env.contains_key("LIB") {
                if let Some((_, msvc_lib)) = crate::isolation::windows::resolve_windows_msvc_env() {
                    cmd.env("LIB", msvc_lib);
                }
            }
        }

        if let Some(ref ws_ctx) = workspace_ctx {
            let tmp_str = eff_extra_writable_dir
                .as_deref()
                .unwrap_or(ws_ctx.tmp_dir)
                .to_string_lossy()
                .to_string();
            for name in ["TMPDIR", "TEMP", "TMP"] {
                cmd.env(name, &tmp_str);
            }
            for (k, v) in &ws_ctx.task_env {
                if *k == crate::workspace::ENV_WORKSPACE_DIR {
                    cmd.env(k, eff_sandbox_path.to_string_lossy().to_string());
                } else {
                    cmd.env(k, v);
                }
            }
        }

        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        if stdin_data.is_some() {
            cmd.stdin(std::process::Stdio::piped());
        } else {
            cmd.stdin(std::process::Stdio::null());
        }

        // Last line of defence for the direct child if this future is dropped; the process tree
        // below takes care of everything else. On Windows, `ProcessTree` owns termination through
        // `TreeInner::kill_and_release_once` (and falls back to `kill_by_leader` if Job Object
        // attachment failed) so dropping `child` synchronously does not call `TerminateProcess`
        // ahead of `MIN_LIVE_KILL_AGE`.
        #[cfg(not(windows))]
        cmd.kill_on_drop(true);
        let mut configured = configure_command(&mut cmd, self.config.process_priority, task_id);
        // After `configure_command`: the isolation hook (Linux) must run after the process-tree
        // hook, which forks the lifeline supervisor and joins the cgroup, so that the supervisor
        // is never restricted (design 0033 D2).
        if let Some(jail) = &jail {
            if let Err(e) = crate::isolation::confine_command(jail, &mut cmd) {
                return isolation_failure(&e);
            }
        }

        #[cfg(windows)]
        if self.config.isolation != IsolationLevel::Off {
            let level = self.config.isolation;
            let host_dirs = self.config.host_dirs.clone();
            let protected_files = self.config.protected_files.clone();
            let sandbox_base = eff_sandbox_base.clone();
            let sandbox_dir = eff_sandbox_path.clone();
            let extra_writable = eff_extra_writable_dir.clone();
            let ws_root = eff_workspace_root.clone();
            let run_token = configured.run_token();
            let std_cmd = cmd.as_std();
            let prog_for_setup = std_cmd.get_program().to_string_lossy().to_string();
            let mut cmd_cargo_home: Option<PathBuf> = None;
            let mut cmd_rustup_home: Option<PathBuf> = None;
            for (k, v) in std_cmd.get_envs() {
                if k.eq_ignore_ascii_case("CARGO_HOME") {
                    cmd_cargo_home = v.map(PathBuf::from);
                } else if k.eq_ignore_ascii_case("RUSTUP_HOME") {
                    cmd_rustup_home = v.map(PathBuf::from);
                }
            }
            let setup_res = tokio::task::spawn_blocking(move || {
                crate::isolation::windows::prepare_worker_toolchains(
                    &host_dirs,
                    &protected_files,
                    Some(&sandbox_base),
                )
                .map_err(|msg| std::io::Error::new(std::io::ErrorKind::PermissionDenied, msg))?;
                let jail = crate::isolation::Jail {
                    level,
                    host: &host_dirs,
                    protected_files: &protected_files,
                    sandbox_base: &sandbox_base,
                    sandbox_dir: &sandbox_dir,
                    extra_writable_dir: extra_writable.as_deref(),
                    workspace_root: ws_root.as_deref(),
                };
                crate::isolation::windows::WindowsAppContainerGuard::create_blocking(
                    run_token,
                    &jail,
                    &prog_for_setup,
                    cmd_cargo_home.as_deref(),
                    cmd_rustup_home.as_deref(),
                )
            })
            .await
            .unwrap_or_else(|e| {
                Err(std::io::Error::other(format!(
                    "AppContainer setup task panicked: {e}"
                )))
            });

            match setup_res {
                Ok(guard) => configured.set_app_container(guard),
                Err(e) => {
                    return TaskResult::failure(
                        self.worker_id,
                        task_id,
                        EXIT_CODE_GENERAL_ERROR,
                        "",
                        format!("Cannot run '{program}' with --isolation {level}: {e}"),
                        start_time.elapsed().as_millis() as u64,
                        Some(format!("Isolation error for '{program}' ({level}): {e}")),
                    );
                }
            }
        }

        if let Some(reason) = check_cancelled_now(&mut cancel_rx) {
            return TaskResult::failure(
                self.worker_id,
                task_id,
                EXIT_CODE_CANCELLED,
                "",
                "",
                start_time.elapsed().as_millis() as u64,
                Some(format!("Cancelled: {reason}")),
            );
        }

        let spawned = configured.spawn(&mut cmd);
        // The isolation hook owns descriptors until the command is dropped: do not keep them for
        // the whole run of the task.
        drop(cmd);
        let mut child = match spawned {
            Ok(c) => c,
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!(
                        "Failed to spawn process '{program}' (resolved: '{resolved_program}'): {e}"
                    ),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!(
                        "Process spawn error for '{program}' (resolved: '{resolved_program}'): {e}"
                    )),
                );
            }
        };

        // Declared before the readers so it is dropped after them: dropping it kills what is left.
        let tree = configured.attach(&child);
        if !tree.is_tracked() {
            warn!(task_id = %task_id, "Could not place the task process in its own process group or job; only the direct child can be terminated reliably");
        }

        if let (Some(ref holder), Some(pid)) = (child_pid, child.id()) {
            holder.store(pid, Ordering::Release);
        }

        #[cfg(windows)]
        let (child_stdin, child_stdout, child_stderr) =
            (child.take_stdin(), child.take_stdout(), child.take_stderr());
        #[cfg(not(windows))]
        let (child_stdin, child_stdout, child_stderr) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take());

        // Concurrent asynchronous stdin writer
        let _stdin_writer = match (child_stdin, stdin_data) {
            (Some(mut writer), Some(bytes)) => Some(AbortOnDrop(tokio::spawn(async move {
                use tokio::io::AsyncWriteExt;
                let _ = writer.write_all(&bytes).await;
                let _ = writer.flush().await;
                drop(writer); // Drop triggers EOF for the child process
            }))),
            _ => None,
        };

        // Concurrent asynchronous bounded stdout / stderr readers
        let mut readers =
            OutputReaders::spawn(child_stdout, child_stderr, self.config.max_output_bytes);

        enum Ended {
            Exited(std::io::Result<std::process::ExitStatus>),
            Cancelled(String),
            TimedOut,
        }

        let ended = tokio::select! {
            status = child.wait() => Ended::Exited(status),
            reason = cancellation(&mut cancel_rx) => Ended::Cancelled(reason),
            _ = tokio::time::sleep(timeout) => Ended::TimedOut,
        };

        match ended {
            Ended::Exited(status_res) => {
                // The direct child is gone and its output normally follows within moments. A
                // background process that inherited the pipes can hold them open for as long as it
                // lives, so wait a bounded time, then take the tree down and keep what arrived.
                let drained = readers.wait(self.config.pipe_drain_grace).await;
                // Nothing the task started may outlive it: its sandbox is deleted next.
                tree.kill_async(None).await;
                if !drained {
                    readers.wait(POST_KILL_DRAIN).await;
                }
                let (stdout_bytes, mut stderr_bytes) = readers.finish();
                if !drained {
                    warn!(task_id = %task_id, "Background processes kept the output pipes open after the task exited; terminated them");
                    stderr_bytes = append_note(
                        stderr_bytes,
                        "\n[rusty_grid: background processes kept the output pipes open after the task exited and were terminated]\n",
                    );
                }

                let elapsed = start_time.elapsed().as_millis() as u64;
                match status_res {
                    Ok(status) if status.success() => {
                        let mut res = TaskResult::success(
                            self.worker_id,
                            task_id,
                            stdout_bytes,
                            elapsed,
                            false,
                        );
                        res.stderr = stderr_bytes;
                        res
                    }
                    Ok(status) => {
                        let (code, error) = describe_exit(&status);
                        TaskResult::failure(
                            self.worker_id,
                            task_id,
                            code,
                            stdout_bytes,
                            stderr_bytes,
                            elapsed,
                            error,
                        )
                    }
                    Err(e) => TaskResult::failure(
                        self.worker_id,
                        task_id,
                        EXIT_CODE_GENERAL_ERROR,
                        stdout_bytes,
                        stderr_bytes,
                        elapsed,
                        Some(format!("Process wait error: {e}")),
                    ),
                }
            }

            Ended::Cancelled(reason) => {
                info!(task_id = %task_id, "Aborting child process per cancellation signal");
                self.stop_tree(&mut child, &tree).await;
                readers.wait(POST_KILL_DRAIN).await;
                let (stdout_bytes, stderr_bytes) = readers.finish();
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_CANCELLED,
                    stdout_bytes,
                    stderr_bytes,
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Cancelled: {reason}")),
                )
            }

            Ended::TimedOut => {
                warn!(task_id = %task_id, timeout_secs = timeout.as_secs(), "Killing runaway process after watchdog timeout");
                self.stop_tree(&mut child, &tree).await;
                readers.wait(POST_KILL_DRAIN).await;
                let (stdout_bytes, stderr_bytes) = readers.finish();
                let msg = format!("Task timed out after {}s", timeout.as_secs());
                let stderr_bytes = append_note(stderr_bytes, &format!("\n{msg}"));
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_TIMEOUT,
                    stdout_bytes,
                    stderr_bytes,
                    start_time.elapsed().as_millis() as u64,
                    Some(msg),
                )
            }
        }
    }

    /// Ends a task's process tree: a polite stop (`SIGTERM`; Windows has none), a short grace
    /// period for the leader to wind down, then a kill of everything that is left, and the leader
    /// is reaped so that no zombie remains. Never blocks a runtime thread.
    async fn stop_tree(&self, child: &mut crate::process_tree::SpawnedChild, tree: &ProcessTree) {
        if tree.request_stop() {
            let _ = tokio::time::timeout(self.config.stop_grace, child.wait()).await;
        }
        tree.kill_async(child.id()).await;
        // The leader at least, even where no tree could be tracked.
        #[cfg(windows)]
        if !tree.is_tracked() {
            let _ = child.start_kill();
        }
        #[cfg(not(windows))]
        let _ = child.start_kill();
        let _ = tokio::time::timeout(REAP_TIMEOUT, child.wait()).await;
    }
}
