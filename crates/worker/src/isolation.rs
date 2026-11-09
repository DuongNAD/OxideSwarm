//! OS-level isolation of task processes (`--isolation off|fs|fs+net`, design 0033).
//!
//! This module holds what every platform shares: the [`IsolationLevel`] (defined in
//! `rusty_grid_core::placement` because placement needs it too), the preparation of the task's
//! environment ([`prepare_environment`]), the start-up self-check ([`verify_isolation_support`])
//! and the entry point the runner calls for each task ([`wrap_program`]). The enforcement itself
//! is one module per platform:
//!
//! * **macOS** (`macos`, FEAT-3a): the task runs as `/usr/bin/sandbox-exec -p <profile> -D ...
//!   -- <program> <args>`. `sandbox-exec` applies the Seatbelt profile and then `exec`s the
//!   program in place, so the PID the worker tracks is the task itself and the process-group
//!   kill, the timeout and the cancellation work as without isolation.
//! * **Linux** (`linux`, FEAT-3b): Landlock for files, `seccomp` for sockets and a few dangerous
//!   calls, a private user and network namespace under `fs+net`. There is no wrapper program:
//!   [`confine_command`] registers a `pre_exec` hook that runs in the task leader after FEAT-2's
//!   hook (so the lifeline supervisor stays unrestricted) and right before `execve`.
//! * **Windows** (`windows`, FEAT-3c): each task attempt runs inside a per-attempt AppContainer
//!   profile (`oxideswarm.task.<worker8>.<run_token>`) spawned with
//!   `PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES`, explicit NTFS DACLs on its sandbox and
//!   read-only toolchains, zero network capabilities under `fs+net`, and
//!   `DeleteAppContainerProfile` on teardown.
//!
//! A worker that was asked for isolation and cannot enforce it never runs tasks unsandboxed: it
//! refuses to start ([`verify_isolation_support`]), and [`wrap_program`] fails a task rather than
//! spawning it plain.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rusty_grid_core::error::{GridError, GridResult};

pub use rusty_grid_core::placement::IsolationLevel;

/// Local IPC endpoints of the owner's desktop session. A confined task has no use for them, and
/// each one is a way around the file policy (an agent that signs, a bus that starts processes, a
/// daemon that runs containers).
pub const STRIPPED_IPC_ENV_VARS: [&str; 4] = [
    "SSH_AUTH_SOCK",
    "GPG_AGENT_INFO",
    "DBUS_SESSION_BUS_ADDRESS",
    "DOCKER_HOST",
];

/// Variables of the worker's own environment that a confined task still inherits. Everything
/// else (tokens such as `GITHUB_TOKEN`, cloud credentials, proxy passwords, `OXIDESWARM_*`) stays
/// with the worker; the task's own `env`, the moved `HOME`/temp variables, `RUSTUP_HOME`,
/// `CARGO_HOME` and the `RUSTY_GRID_*` variables the runner sets are added on top.
pub const INHERITED_ENV_VARS: [&str; 12] = [
    "PATH",
    "LANG",
    "TERM",
    "TZ",
    "USER",
    "LOGNAME",
    "SHELL",
    "COLORTERM",
    "NO_COLOR",
    "DEVELOPER_DIR",
    "SDKROOT",
    "RUSTUP_TOOLCHAIN",
];

/// Windows system environment variables that native Win32 tools (`cmd.exe`, `powershell.exe`,
/// `cargo.exe`, `rustc.exe`, `link.exe`, Winsock `ws2_32.dll`, and `CreateProcessW`'s
/// `BasepAppContainerEnvironmentExtension`) require to initialize when the child starts from an
/// empty environment (`cmd.env_clear()`). Matched case-insensitively on Windows.
pub const WINDOWS_INHERITED_ENV_VARS: [&str; 18] = [
    "SystemRoot",
    "SystemDrive",
    "WINDIR",
    "COMSPEC",
    "PATHEXT",
    "OS",
    "PROCESSOR_ARCHITECTURE",
    "NUMBER_OF_PROCESSORS",
    "ProgramFiles",
    "ProgramFiles(x86)",
    "ProgramW6432",
    "CommonProgramFiles",
    "CommonProgramFiles(x86)",
    "CommonProgramW6432",
    "ProgramData",
    "PUBLIC",
    "LOCALAPPDATA",
    "APPDATA",
];

/// The part of `vars` (the worker's environment) that a confined task inherits: the names in
/// [`INHERITED_ENV_VARS`] and every `LC_*` (plus [`WINDOWS_INHERITED_ENV_VARS`] on Windows).
pub fn inherited_environment(
    vars: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    #[allow(unused_mut)]
    let mut kept: Vec<(std::ffi::OsString, std::ffi::OsString)> = vars
        .into_iter()
        .filter(|(name, _)| {
            name.to_str().is_some_and(|n| {
                INHERITED_ENV_VARS.contains(&n)
                    || (n.starts_with("LC_") && n.len() > 3)
                    || (cfg!(windows)
                        && (INHERITED_ENV_VARS.iter().any(|w| w.eq_ignore_ascii_case(n))
                            || WINDOWS_INHERITED_ENV_VARS
                                .iter()
                                .any(|w| w.eq_ignore_ascii_case(n))))
            })
        })
        .collect();
    #[cfg(windows)]
    for (name, val) in &mut kept {
        if let (Some(n), Some(v)) = (name.to_str(), val.to_str()) {
            const WIN_PATH_VARS: [&str; 13] = [
                "SystemRoot",
                "WINDIR",
                "COMSPEC",
                "ProgramFiles",
                "ProgramFiles(x86)",
                "ProgramW6432",
                "CommonProgramFiles",
                "CommonProgramFiles(x86)",
                "CommonProgramW6432",
                "ProgramData",
                "PUBLIC",
                "LOCALAPPDATA",
                "APPDATA",
            ];
            if WIN_PATH_VARS.iter().any(|w| w.eq_ignore_ascii_case(n))
                && (v.contains('~') || v.contains('/'))
            {
                let p = Path::new(v);
                if p.exists() && !windows::is_reparse_point(p) {
                    *val = windows::normalize_win_path(p).into_os_string();
                }
            }
        }
    }
    kept
}

/// The directories of the host that the policy names: the owner's home (denied), and the Rust
/// toolchain and Cargo cache (readable, so `cargo build --offline` works, design 0033 D1.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostDirs {
    /// `$HOME` (`%USERPROFILE%` on Windows) of the worker process.
    pub user_home: PathBuf,
    /// `$RUSTUP_HOME`, else `<user_home>/.rustup`.
    pub rustup_home: PathBuf,
    /// `$CARGO_HOME`, else `<user_home>/.cargo`.
    pub cargo_home: PathBuf,
}

impl HostDirs {
    /// Resolves the directories from the environment of this process. Call it before anything
    /// moves `HOME`.
    pub fn detect() -> Self {
        let user_home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .unwrap_or_default();
        Self::from_home(
            user_home,
            std::env::var_os("RUSTUP_HOME").map(PathBuf::from),
            std::env::var_os("CARGO_HOME").map(PathBuf::from),
        )
    }

    /// The directories for `user_home`, with the toolchain locations the environment overrides
    /// (an empty override counts as unset).
    pub fn from_home(
        user_home: PathBuf,
        rustup_home: Option<PathBuf>,
        cargo_home: Option<PathBuf>,
    ) -> Self {
        let non_empty = |p: Option<PathBuf>| p.filter(|p| !p.as_os_str().is_empty());
        #[cfg(windows)]
        let norm = |p: PathBuf| {
            if !p.as_os_str().is_empty() && p.exists() && !windows::is_reparse_point(&p) {
                windows::normalize_win_path(&p)
            } else {
                p
            }
        };
        #[cfg(not(windows))]
        let norm = |p: PathBuf| p;

        let user_home = norm(user_home);
        let rustup_home = norm(non_empty(rustup_home).unwrap_or_else(|| user_home.join(".rustup")));
        let cargo_home = norm(non_empty(cargo_home).unwrap_or_else(|| user_home.join(".cargo")));
        Self {
            user_home,
            rustup_home,
            cargo_home,
        }
    }
}

/// Changes to a task's environment under `fs` / `fs+net` (design 0033 D1.4). Apply them after the
/// task's own variables: the isolation wins over a task that names the same variable, except for
/// `RUSTUP_HOME` and `CARGO_HOME`, which a task may point elsewhere (for example at a Cargo cache
/// inside its sandbox).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EnvPlan {
    /// Variables to set, in order.
    pub set: Vec<(String, String)>,
    /// Variables to remove.
    pub remove: Vec<&'static str>,
}

/// Plans the environment of a confined task: the real toolchain locations are pinned first
/// (`RUSTUP_HOME`, `CARGO_HOME`, unless `task_env` sets them), then the home and temp variables
/// move into the sandbox and the IPC endpoints of the desktop session are removed.
pub fn prepare_environment(
    host: &HostDirs,
    task_env: &HashMap<String, String>,
    sandbox: &Path,
) -> EnvPlan {
    let mut plan = EnvPlan::default();
    let path_text = |p: &Path| {
        #[cfg(windows)]
        if !p.as_os_str().is_empty() && p.exists() && !windows::is_reparse_point(p) {
            return windows::normalize_win_path(p)
                .to_string_lossy()
                .into_owned();
        }
        p.to_string_lossy().into_owned()
    };
    if !task_env.contains_key("RUSTUP_HOME") {
        plan.set
            .push(("RUSTUP_HOME".into(), path_text(&host.rustup_home)));
    }
    if !task_env.contains_key("CARGO_HOME") {
        plan.set
            .push(("CARGO_HOME".into(), path_text(&host.cargo_home)));
    }
    for name in ["HOME", "USERPROFILE", "TMPDIR", "TEMP", "TMP"] {
        plan.set.push((name.into(), path_text(sandbox)));
    }
    #[cfg(windows)]
    if !task_env
        .keys()
        .any(|k| k.eq_ignore_ascii_case("PSModulePath"))
    {
        let sys_root = std::env::var_os("SystemRoot")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Windows"));
        let sys_modules = sys_root
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("Modules");
        plan.set
            .push(("PSModulePath".into(), path_text(&sys_modules)));
    }
    plan.remove.extend(STRIPPED_IPC_ENV_VARS);
    plan
}

/// What the runner knows about one task when it asks for it to be confined. Only the
/// platforms that have an implementation read every field.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    not(any(target_os = "macos", target_os = "linux", windows)),
    allow(dead_code)
)]
pub(crate) struct Jail<'a> {
    pub level: IsolationLevel,
    pub host: &'a HostDirs,
    /// Files the task must not read wherever they are.
    pub protected_files: &'a [PathBuf],
    /// The worker's sandbox base directory (`<base>/<task id>` holds this task's files). Only
    /// the Seatbelt profile and Windows AppContainer setup name it: on Linux everything outside
    /// the sandbox is denied anyway.
    #[cfg_attr(not(any(target_os = "macos", windows)), allow(dead_code))]
    pub sandbox_base: &'a Path,
    /// The task's own sandbox directory (or workspace tree directory).
    pub sandbox_dir: &'a Path,
    /// An optional second writable directory for the task (the per-task `<workspace>/tmp`
    /// directory when running inside a workspace).
    #[cfg_attr(windows, allow(dead_code))]
    pub extra_writable_dir: Option<&'a Path>,
    /// The worker's workspace root directory when `--workspaces` is active, so other workspaces
    /// and workspace metadata (`state.json`, `lock`) are denied.
    #[cfg_attr(windows, allow(dead_code))]
    pub workspace_root: Option<&'a Path>,
}

/// The program and arguments to spawn so that the task runs confined at `jail.level`.
///
/// `program` is resolved against `search_path` first (a bare name is looked up like `execvp`
/// does), so a program that does not exist fails here with a clear message. On macOS the result
/// is the `sandbox-exec` wrapper; on Linux it is the program itself and the confinement is
/// applied by the `pre_exec` hook that [`confine_command`] registers. A platform without an
/// implementation yields an error: a confined task is never silently started plain.
pub(crate) fn wrap_program(
    jail: &Jail<'_>,
    program: &str,
    args: &[String],
    search_path: &str,
) -> Result<(String, Vec<String>), String> {
    debug_assert!(jail.level != IsolationLevel::Off);
    #[cfg(target_os = "macos")]
    {
        let resolved = resolve_program(program, search_path)
            .ok_or_else(|| format!("program '{program}' was not found in PATH"))?;
        macos::wrap(jail, &resolved, args)
    }
    #[cfg(target_os = "linux")]
    {
        let _ = jail;
        let resolved = resolve_program(program, search_path)
            .ok_or_else(|| format!("program '{program}' was not found in PATH"))?;
        Ok((resolved, args.to_vec()))
    }
    #[cfg(windows)]
    {
        let _ = jail;
        let resolved = windows::resolve_program(program, search_path)
            .ok_or_else(|| format!("program '{program}' was not found in PATH"))?;
        Ok((resolved, args.to_vec()))
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    {
        let _ = (jail, program, args, search_path);
        Err(unsupported_message(jail.level))
    }
}

/// Registers the part of the confinement that is not a wrapper program (Linux: Landlock, `seccomp`
/// and the namespaces, as a `pre_exec` hook). Call it right after `configure_command` so the hook
/// runs after FEAT-2's: the lifeline supervisor is forked and the leader is in its cgroup before
/// the leader restricts itself. A no-op on macOS, where `sandbox-exec` does it. The hook holds
/// the policy's descriptors until `cmd` is dropped, so drop `cmd` after `spawn`.
pub(crate) fn confine_command(
    jail: &Jail<'_>,
    cmd: &mut tokio::process::Command,
) -> Result<(), String> {
    debug_assert!(jail.level != IsolationLevel::Off);
    #[cfg(target_os = "linux")]
    {
        let confinement = linux::Confinement::build(jail)?;
        // SAFETY: the hook makes raw system calls only (see `linux::Confinement::apply`).
        unsafe {
            cmd.pre_exec(confinement.into_hook());
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (jail, cmd);
        Ok(())
    }
}

/// Whether a task can enter a private user and network namespace here (`fs+net`; false when
/// unprivileged user namespaces are unavailable or `RUSTY_GRID_ISOLATION_NO_USERNS` is set).
#[cfg(target_os = "linux")]
pub fn private_netns_available() -> bool {
    linux::private_netns_available()
}

/// The Landlock ABI version of the running kernel (`1` is Linux 5.13), or why there is none.
#[cfg(target_os = "linux")]
pub fn landlock_abi() -> std::io::Result<u32> {
    linux::landlock_abi()
}

/// Start-up self-check: proves this host can enforce `level`, or says why not. Runs a probe task
/// in a temporary directory under `sandbox_base`, so a worker never registers promising an
/// isolation it cannot deliver (design 0033 D1.5, G4). `Off` always passes.
pub async fn verify_isolation_support(
    level: IsolationLevel,
    sandbox_base: &Path,
) -> GridResult<()> {
    let empty_host =
        HostDirs::from_home(PathBuf::new(), Some(PathBuf::new()), Some(PathBuf::new()));
    verify_isolation_support_with_config(level, sandbox_base, &empty_host, &[]).await
}

/// Start-up self-check with explicit worker [`HostDirs`] and `protected_files`: validates that no
/// protected file lies inside a granted toolchain directory, prepares the worker's own toolchain
/// ACLs once at start-up (on Windows, using the `oxideswarm.toolchain` capability SID), sweeps
/// leftover per-task AppContainer profiles, and runs the sandbox probe.
pub async fn verify_isolation_support_with_config(
    level: IsolationLevel,
    sandbox_base: &Path,
    host: &HostDirs,
    protected_files: &[PathBuf],
) -> GridResult<()> {
    if level == IsolationLevel::Off {
        return Ok(());
    }
    #[cfg(target_os = "macos")]
    {
        let _ = (host, protected_files);
        macos::probe(level, sandbox_base)
            .await
            .map_err(GridError::Config)
    }
    #[cfg(target_os = "linux")]
    {
        let _ = (host, protected_files);
        linux::probe(level, sandbox_base)
            .await
            .map_err(GridError::Config)
    }
    #[cfg(windows)]
    {
        windows::probe_with_config(level, sandbox_base, host, protected_files)
            .await
            .map_err(GridError::Config)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    {
        let _ = (sandbox_base, host, protected_files);
        Err(GridError::Config(unsupported_message(level)))
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
fn unsupported_message(level: IsolationLevel) -> String {
    let which = if cfg!(windows) {
        "FEAT-3c"
    } else {
        "no FEAT-3 item covers this platform"
    };
    format!(
        "--isolation {level} is not implemented yet on this platform ({}; {which}); start the worker with --isolation off or remove the setting",
        std::env::consts::OS
    )
}

/// Looks `program` up the way `execvp` does: a name with a `/` is used as given, a bare name is
/// searched in the `PATH`-style list (`:`-separated; an empty entry is the current directory).
#[cfg(any(target_os = "macos", target_os = "linux", all(test, unix)))]
fn resolve_program(program: &str, search_path: &str) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    if program.contains('/') {
        return Some(program.to_string());
    }
    std::env::split_paths(search_path)
        .map(|dir| {
            if dir.as_os_str().is_empty() {
                PathBuf::from(".")
            } else {
                dir
            }
            .join(program)
        })
        .find(|candidate| {
            std::fs::metadata(candidate)
                .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
        })
        .map(|p| p.to_string_lossy().into_owned())
}

/// The canonical form of `p`; a path that does not exist (yet) is returned as given, with its
/// longest existing prefix canonicalized.
#[cfg(any(target_os = "macos", target_os = "linux", all(test, unix)))]
fn canonical(p: &Path) -> PathBuf {
    if let Ok(c) = p.canonicalize() {
        return c;
    }
    match (p.parent(), p.file_name()) {
        (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => {
            canonical(parent).join(name)
        }
        _ => p.to_path_buf(),
    }
}

/// Ensures `jail.sandbox_dir` and `jail.extra_writable_dir` are real directories (never symbolic
/// links) and that canonicalization does not follow a link out of `<sandbox_base>` or
/// `<workspace_root>/<name>/{tree,tmp}`.
#[cfg(any(target_os = "macos", target_os = "linux", all(test, unix)))]
fn validate_jail_dirs(jail: &Jail<'_>) -> Result<(), String> {
    let check_real_dir = |label: &str, p: &Path| -> Result<PathBuf, String> {
        let meta = std::fs::symlink_metadata(p)
            .map_err(|e| format!("cannot inspect {label} '{}': {e}", p.display()))?;
        if meta.file_type().is_symlink() || !meta.is_dir() {
            return Err(format!(
                "{label} '{}' is not a real directory (symbolic link or non-directory refused)",
                p.display()
            ));
        }
        p.canonicalize()
            .map_err(|e| format!("cannot canonicalize {label} '{}': {e}", p.display()))
    };

    let canon_sandbox = check_real_dir("task sandbox directory", jail.sandbox_dir)?;

    if let Some(extra_dir) = jail.extra_writable_dir {
        let canon_extra = check_real_dir("workspace temp directory", extra_dir)?;
        let ws_root = jail
            .workspace_root
            .ok_or_else(|| "workspace task jail is missing workspace_root".to_string())?;
        let canon_ws_root = check_real_dir("workspace root directory", ws_root)?;
        let rel_tree = jail.sandbox_dir.strip_prefix(ws_root).map_err(|_| {
            format!(
                "workspace tree '{}' is not inside workspace root '{}'",
                jail.sandbox_dir.display(),
                ws_root.display()
            )
        })?;
        let expected_tree = canon_ws_root.join(rel_tree);
        if canon_sandbox != expected_tree {
            return Err(format!(
                "workspace tree '{}' canonicalized to '{}', expected '{}'",
                jail.sandbox_dir.display(),
                canon_sandbox.display(),
                expected_tree.display()
            ));
        }
        let rel_tmp = extra_dir.strip_prefix(ws_root).map_err(|_| {
            format!(
                "workspace tmp '{}' is not inside workspace root '{}'",
                extra_dir.display(),
                ws_root.display()
            )
        })?;
        let expected_tmp = canon_ws_root.join(rel_tmp);
        if canon_extra != expected_tmp {
            return Err(format!(
                "workspace tmp '{}' canonicalized to '{}', expected '{}'",
                extra_dir.display(),
                canon_extra.display(),
                expected_tmp.display()
            ));
        }
    } else if let Ok(rel) = jail.sandbox_dir.strip_prefix(jail.sandbox_base) {
        let canon_base = check_real_dir("sandbox base directory", jail.sandbox_base)?;
        let expected_sb = canon_base.join(rel);
        if canon_sandbox != expected_sb {
            return Err(format!(
                "task sandbox '{}' canonicalized to '{}', expected '{}'",
                jail.sandbox_dir.display(),
                canon_sandbox.display(),
                expected_sb.display()
            ));
        }
    }
    Ok(())
}

/// The Linux implementation: Landlock, `seccomp` and namespaces, applied by a `pre_exec` hook.
#[cfg(any(target_os = "linux", all(test, unix)))]
mod linux;

/// The macOS implementation: a Seatbelt profile handed to `/usr/bin/sandbox-exec`.
///
/// Facts about Seatbelt that the profile relies on, each checked by experiment on macOS (design
/// 0033 D3):
///
/// * Among the rules that match an operation, the **last** one wins. A broad `deny` followed by
///   narrower `allow`s followed by narrower `deny`s therefore works as written; the reverse
///   order does not.
/// * A `subpath` rule also matches the directory itself, so denying `file-read*` on the base
///   directory denies `stat` on it too, and then `realpath()` of anything below it fails (cargo
///   and rustc canonicalize their paths). `file-read-metadata` of the directories on the way to
///   an allowed path is therefore allowed again; their contents stay unreadable.
/// * Rules are matched against the real path, so a symlink does not get around them, and the
///   `-D` parameters must be canonical (on macOS `/var` is `/private/var`).
/// * `(deny network*)` stops a TCP connect to a loopback listener and Unix-socket connects.
#[cfg(any(target_os = "macos", all(test, unix)))]
mod macos {
    use super::{canonical, IsolationLevel, Jail};
    use std::path::{Path, PathBuf};

    #[cfg(target_os = "macos")]
    pub(super) const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

    /// Devices a confined task may write to (besides its sandbox).
    const WRITABLE_DEVICES: [&str; 8] = [
        "/dev/null",
        "/dev/zero",
        "/dev/random",
        "/dev/urandom",
        "/dev/dtracehelper",
        "/dev/stdout",
        "/dev/stderr",
        "/dev/tty",
    ];

    /// The Seatbelt profile text. All paths arrive as `-D` parameters (`SANDBOX_DIR`,
    /// `EXTRA_WRITABLE_DIR`, `USER_HOME`, `SANDBOX_BASE`, `CACHE_DIR`, `RUSTUP_HOME`,
    /// `CARGO_HOME`, `CARGO_CRED`, `CARGO_CRED_TOML`, `ANCESTOR_<n>` for `ancestors` entries),
    /// so no path is ever spliced into the profile text.
    pub(super) fn profile(
        level: IsolationLevel,
        has_extra_writable: bool,
        ancestors: usize,
        extra_denied: usize,
        protected: usize,
    ) -> String {
        let mut p = String::from("(version 1)\n(allow default)\n(deny file-write* (subpath \"/\"))\n(allow file-write*\n    (subpath (param \"SANDBOX_DIR\"))\n");
        if has_extra_writable {
            p.push_str("    (subpath (param \"EXTRA_WRITABLE_DIR\"))\n");
        }
        for dev in WRITABLE_DEVICES {
            p.push_str(&format!("    (literal \"{dev}\")\n"));
        }
        p.push_str("    (subpath \"/dev/fd\"))\n");
        // Deny unlinking, recreating, or renaming over the sandbox / workspace `tree` and `tmp`
        // directory nodes themselves (Seatbelt mediates `rmdir`, `unlink`, `symlink`, `mkdir`,
        // `rename` and `renamex_np` on the node via `file-write-unlink` and `file-write-create`),
        // while keeping writes inside them allowed.
        p.push_str("(deny file-write-unlink\n    (literal (param \"SANDBOX_DIR\"))");
        if has_extra_writable {
            p.push_str("\n    (literal (param \"EXTRA_WRITABLE_DIR\"))");
        }
        p.push_str(")\n(deny file-write-create\n    (literal (param \"SANDBOX_DIR\"))");
        if has_extra_writable {
            p.push_str("\n    (literal (param \"EXTRA_WRITABLE_DIR\"))");
        }
        p.push_str(")\n");
        p.push_str(
            "(deny file-read*\n    (subpath (param \"USER_HOME\"))\n    (subpath (param \"SANDBOX_BASE\"))\n    (subpath (param \"CACHE_DIR\"))",
        );
        for i in 0..extra_denied {
            p.push_str(&format!("\n    (subpath (param \"DENIED_{i}\"))"));
        }
        p.push_str(")\n");
        p.push_str("(allow file-read*\n    (subpath (param \"SANDBOX_DIR\"))\n");
        if has_extra_writable {
            p.push_str("    (subpath (param \"EXTRA_WRITABLE_DIR\"))\n");
        }
        p.push_str(
            "    (subpath (param \"RUSTUP_HOME\"))\n    (subpath (param \"CARGO_HOME\")))\n",
        );
        p.push_str(
            "(deny file-read*\n    (literal (param \"CARGO_CRED\"))\n    (literal (param \"CARGO_CRED_TOML\")))\n",
        );
        if protected > 0 {
            p.push_str("(deny file-read*");
            for i in 0..protected {
                p.push_str(&format!("\n    (literal (param \"PROTECTED_{i}\"))"));
            }
            p.push_str(")\n");
        }
        if ancestors > 0 {
            p.push_str("(allow file-read-metadata\n");
            for i in 0..ancestors {
                p.push_str(&format!("    (literal (param \"ANCESTOR_{i}\"))\n"));
            }
            p.push_str(")\n");
        }
        // Ways out of the file policy that are not file accesses (design 0033, security review):
        // - LaunchServices and Apple events start an unconfined process on the task's behalf.
        p.push_str("(deny lsopen)\n(deny appleevent-send)\n");
        // - cfprefsd reads and writes the owner's preference files for the task. Reading the
        //   global domain stays possible.
        p.push_str("(deny user-preference-read user-preference-write)\n(allow user-preference-read (preference-domain \"kCFPreferencesAnyApplication\"))\n");
        // - Unix sockets of the desktop session (ssh-agent, gpg-agent, docker); DNS stays.
        p.push_str("(deny network-outbound (remote unix-socket))\n(allow network-outbound (remote unix-socket (path-literal \"/private/var/run/mDNSResponder\")))\n");
        // - The clipboard.
        p.push_str("(deny mach-lookup (global-name-prefix \"com.apple.pasteboard\"))\n");
        // - Keystroke injection into the controlling terminal (TIOCSTI).
        p.push_str("(deny file-ioctl (literal \"/dev/tty\") (regex #\"^/dev/ttys[0-9]+$\"))\n");
        // - Signals to anything but the task's own processes (the worker, other tasks).
        p.push_str("(deny signal)\n(allow signal (target same-sandbox))\n");
        if level == IsolationLevel::FsNet {
            p.push_str("(deny network*)\n");
        }
        p
    }

    /// The home directory the account database names for this user (`getpwuid_r`), which can
    /// differ from `$HOME` when the worker was started with another one.
    #[cfg(target_os = "macos")]
    fn account_home() -> Option<PathBuf> {
        use std::ffi::CStr;
        let mut buf = vec![0u8; 4096];
        let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
        let mut out: *mut libc::passwd = std::ptr::null_mut();
        // SAFETY: the buffer outlives the call and `pwd`/`out` are valid for writes.
        let rc = unsafe {
            libc::getpwuid_r(
                libc::getuid(),
                &mut pwd,
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut out,
            )
        };
        if rc != 0 || out.is_null() || pwd.pw_dir.is_null() {
            return None;
        }
        // SAFETY: `pw_dir` points into `buf`, NUL-terminated by getpwuid_r.
        let dir = unsafe { CStr::from_ptr(pwd.pw_dir) }
            .to_string_lossy()
            .into_owned();
        (!dir.is_empty()).then(|| PathBuf::from(dir))
    }

    #[cfg(not(target_os = "macos"))]
    fn account_home() -> Option<PathBuf> {
        None
    }

    /// The per-user temporary directory (`DARWIN_USER_TEMP_DIR`, `/var/folders/../T`).
    #[cfg(target_os = "macos")]
    fn user_temp_dir() -> Option<PathBuf> {
        let mut buf = vec![0 as libc::c_char; 1024];
        // SAFETY: the buffer is valid for `len` bytes.
        let n =
            unsafe { libc::confstr(libc::_CS_DARWIN_USER_TEMP_DIR, buf.as_mut_ptr(), buf.len()) };
        if n == 0 || n > buf.len() {
            return None;
        }
        // SAFETY: confstr NUL-terminates within the buffer when `n <= len`.
        let text = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        (!text.is_empty()).then(|| PathBuf::from(text))
    }

    #[cfg(not(target_os = "macos"))]
    fn user_temp_dir() -> Option<PathBuf> {
        None
    }

    /// The per-user cache directory (`DARWIN_USER_CACHE_DIR`, `/var/folders/../C`), next to the
    /// temp directory.
    #[cfg(target_os = "macos")]
    fn user_cache_dir() -> Option<PathBuf> {
        let mut buf = vec![0 as libc::c_char; 1024];
        // SAFETY: the buffer is valid for `len` bytes.
        let n =
            unsafe { libc::confstr(libc::_CS_DARWIN_USER_CACHE_DIR, buf.as_mut_ptr(), buf.len()) };
        if n == 0 || n > buf.len() {
            return None;
        }
        // SAFETY: confstr NUL-terminates within the buffer when `n <= len`.
        let text = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        (!text.is_empty()).then(|| PathBuf::from(text))
    }

    #[cfg(not(target_os = "macos"))]
    fn user_cache_dir() -> Option<PathBuf> {
        None
    }

    /// Everything `sandbox-exec` is told: `-p <profile> -D name=value ...`.
    pub(super) fn arguments(jail: &Jail<'_>) -> Vec<String> {
        let sandbox_dir = canonical(jail.sandbox_dir);
        let extra_writable = jail.extra_writable_dir.map(canonical);
        let workspace_root = jail.workspace_root.map(canonical);
        let base = canonical(jail.sandbox_base);
        let home = canonical(&jail.host.user_home);
        let rustup = canonical(&jail.host.rustup_home);
        let cargo = canonical(&jail.host.cargo_home);
        let cache = base.join("cache");

        // Further places whose reads are denied: the account's real home (it differs from
        // `$HOME` when the worker runs with another one), the user temp directory (other tasks'
        // and tools' scratch files, Blender's temp dirs), the workspace root (other workspaces
        // and workspace metadata), and the shared areas a task has no business in. The task's
        // own sandbox (or workspace tree and tmp) is re-allowed after them.
        let mut extra: Vec<PathBuf> = Vec::new();
        let candidates = [
            account_home(),
            user_temp_dir(),
            user_cache_dir(),
            workspace_root.clone(),
            Some(PathBuf::from("/Users/Shared")),
            Some(PathBuf::from("/private/tmp")),
            Some(PathBuf::from("/private/var/tmp")),
            Some(PathBuf::from("/Volumes")),
        ];
        for dir in candidates.into_iter().flatten() {
            let dir = canonical(&dir);
            if dir != home && dir != base && !extra.contains(&dir) {
                extra.push(dir);
            }
        }

        // The cluster secret file and the config file: denied wherever they are, after the allows
        // (a secret file inside the toolchain directories stays denied too).
        let mut protected: Vec<PathBuf> = Vec::new();
        for file in jail.protected_files {
            if file.as_os_str().is_empty() {
                continue;
            }
            let file = canonical(file);
            if !protected.contains(&file) {
                protected.push(file);
            }
        }

        // Directories on the way to an allowed path that the deny rules cover: their metadata
        // must stay readable for `realpath()`.
        let denied: Vec<&PathBuf> = [&home, &base, &cache]
            .into_iter()
            .chain(extra.iter())
            .collect();
        let mut ancestors: Vec<PathBuf> = Vec::new();
        let mut targets = vec![&sandbox_dir, &rustup, &cargo];
        if let Some(ref extra_dir) = extra_writable {
            targets.push(extra_dir);
        }
        for target in targets {
            for dir in target.ancestors().skip(1) {
                if dir.as_os_str().is_empty() || dir == Path::new("/") {
                    continue;
                }
                let covered = denied.iter().any(|d| dir.starts_with(d));
                if covered && !ancestors.iter().any(|a| a == dir) {
                    ancestors.push(dir.to_path_buf());
                }
            }
        }
        // The base itself is the one directory above a sandbox that is always covered.
        if !ancestors.contains(&base) {
            ancestors.push(base.clone());
        }

        let mut args = vec![
            "-p".to_string(),
            profile(
                jail.level,
                extra_writable.is_some(),
                ancestors.len(),
                extra.len(),
                protected.len(),
            ),
        ];
        let mut define = |name: &str, value: &Path| {
            args.push("-D".to_string());
            args.push(format!("{name}={}", value.to_string_lossy()));
        };
        define("SANDBOX_DIR", &sandbox_dir);
        if let Some(ref extra_dir) = extra_writable {
            define("EXTRA_WRITABLE_DIR", extra_dir);
        }
        define("USER_HOME", &home);
        define("SANDBOX_BASE", &base);
        define("CACHE_DIR", &cache);
        define("RUSTUP_HOME", &rustup);
        define("CARGO_HOME", &cargo);
        define("CARGO_CRED", &cargo.join("credentials"));
        define("CARGO_CRED_TOML", &cargo.join("credentials.toml"));
        for (i, dir) in extra.iter().enumerate() {
            define(&format!("DENIED_{i}"), dir);
        }
        for (i, file) in protected.iter().enumerate() {
            define(&format!("PROTECTED_{i}"), file);
        }
        for (i, dir) in ancestors.iter().enumerate() {
            define(&format!("ANCESTOR_{i}"), dir);
        }
        args
    }

    /// `sandbox-exec -p <profile> -D ... -- <program> <args>`.
    #[cfg(target_os = "macos")]
    pub(super) fn wrap(
        jail: &Jail<'_>,
        program: &str,
        args: &[String],
    ) -> Result<(String, Vec<String>), String> {
        super::validate_jail_dirs(jail)?;
        let mut full = arguments(jail);
        full.push("--".to_string());
        full.push(program.to_string());
        full.extend(args.iter().cloned());
        Ok((SANDBOX_EXEC.to_string(), full))
    }

    /// Runs a small program under the profile and checks that the policy is enforced: the task
    /// can write inside its sandbox and cannot write next to it.
    #[cfg(target_os = "macos")]
    pub(super) async fn probe(level: IsolationLevel, sandbox_base: &Path) -> Result<(), String> {
        let fail = |what: String| {
            format!(
                "--isolation {level} cannot be enforced on this host: {what} (is the worker itself running inside a sandbox? nested sandbox-exec is refused)"
            )
        };
        if !Path::new(SANDBOX_EXEC).exists() {
            return Err(format!(
                "--isolation {level} needs {SANDBOX_EXEC}, which this macOS does not have"
            ));
        }
        let probe_dir = sandbox_base.join(format!(".isolation-probe-{}", std::process::id()));
        std::fs::create_dir_all(&probe_dir).map_err(|e| {
            format!(
                "--isolation {level}: cannot create the probe directory {}: {e}",
                probe_dir.display()
            )
        })?;
        let outcome = async {
            let host = super::HostDirs::detect();
            let jail = Jail {
                level,
                host: &host,
                protected_files: &[],
                sandbox_base,
                sandbox_dir: &probe_dir,
                extra_writable_dir: None,
                workspace_root: None,
            };
            // Write inside: must work. Write beside the sandbox: must not.
            let script = "echo ok > \"$1/inside\" || exit 2; if echo no > \"$2/outside\" 2>/dev/null; then rm -f \"$2/outside\"; exit 3; fi";
            let outside = canonical(sandbox_base);
            let (program, args) = wrap(
                &jail,
                "/bin/sh",
                &[
                    "-c".to_string(),
                    script.to_string(),
                    "probe".to_string(),
                    canonical(&probe_dir).to_string_lossy().into_owned(),
                    outside.to_string_lossy().into_owned(),
                ],
            )?;
            let output = tokio::process::Command::new(program)
                .args(args)
                .current_dir(&probe_dir)
                .stdin(std::process::Stdio::null())
                .output()
                .await
                .map_err(|e| fail(format!("could not run {SANDBOX_EXEC}: {e}")))?;
            match output.status.code() {
                Some(0) => Ok(()),
                Some(3) => Err(fail(
                    "the probe wrote outside its sandbox, so the profile was not applied"
                        .to_string(),
                )),
                _ => Err(fail(format!(
                    "the probe failed ({}): {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                ))),
            }
        }
        .await;
        let _ = std::fs::remove_dir_all(&probe_dir);
        outcome
    }
}

/// The Windows implementation (design 0033 D4, FEAT-3c):
/// - Dedicated custom capability SID `oxideswarm.toolchain` derived via
///   `DeriveCapabilitySidsFromName` for read+execute on worker-owned toolchains (`rustup_home`,
///   `cargo_home`, and read-only MSVC/SDK directories), never `S-1-15-2-1` (`ALL APPLICATION PACKAGES`).
/// - One-time worker toolchain preparation from the worker's own `HostDirs`, never from task env
///   or `Cargo.lock`, never touching `<user_home>` or `<user_home>/.cargo`, and refusing to start
///   if any `protected_files` path lies inside a granted toolchain directory.
/// - Per-task AppContainer profile (`oxideswarm.task.<run_token>`), explicit protected DACL on
///   `<sandbox_dir>` granting only `SYSTEM`, `Administrators`, current user, and the per-task SID,
///   non-inherited traverse ACE on `<sandbox_base>` for `oxideswarm.toolchain`, zero network
///   capabilities under `fs+net`, and `DeleteAppContainerProfile` with retry backoff + start-up
///   sweep on cleanup.
#[cfg(windows)]
pub mod windows {
    use super::{HostDirs, IsolationLevel, Jail};
    use std::collections::{BTreeMap, HashMap};
    use std::ffi::{OsStr, OsString};
    use std::io;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use std::os::windows::io::FromRawHandle;
    use std::os::windows::process::ExitStatusExt;
    use std::path::{Component, Path, PathBuf};
    use std::sync::{Arc, Mutex, OnceLock};

    use windows_sys::Win32::Foundation::{
        CloseHandle, LocalFree, SetHandleInformation, ERROR_ALREADY_EXISTS, ERROR_SUCCESS, HANDLE,
        HANDLE_FLAG_INHERIT, HLOCAL, INVALID_HANDLE_VALUE, WAIT_OBJECT_0,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertSecurityDescriptorToStringSecurityDescriptorW, ConvertSidToStringSidW,
        ConvertStringSecurityDescriptorToSecurityDescriptorW, ConvertStringSidToSidW,
        GetNamedSecurityInfoW, GetSecurityInfo, SetEntriesInAclW, SetNamedSecurityInfoW,
        SetSecurityInfo, DENY_ACCESS, EXPLICIT_ACCESS_W, GRANT_ACCESS, SDDL_REVISION_1, SET_ACCESS,
        SE_FILE_OBJECT, SE_WINDOW_OBJECT, TRUSTEE_IS_SID, TRUSTEE_IS_UNKNOWN, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::Isolation::{
        CreateAppContainerProfile, DeleteAppContainerProfile,
        DeriveAppContainerSidFromAppContainerName,
    };
    use windows_sys::Win32::Security::{
        AclSizeInformation, CreateRestrictedToken, CreateWellKnownSid, DeleteAce,
        DeriveCapabilitySidsFromName, DuplicateTokenEx, EqualSid, FreeSid, GetAce,
        GetAclInformation, GetLengthSid, GetSecurityDescriptorControl, GetSecurityDescriptorDacl,
        GetTokenInformation, IsValidSid, SecurityImpersonation, SetFileSecurityW,
        SetTokenInformation, TokenDefaultDacl, TokenElevation, TokenOwner, TokenPrimary, TokenUser,
        WinBuiltinAdministratorsSid, WinBuiltinAnyPackageSid, WinCapabilityInternetClientSid,
        WinCapabilityPrivateNetworkClientServerSid, WinLocalSystemSid, ACCESS_ALLOWED_ACE,
        ACE_FLAGS, ACE_HEADER, ACL, ACL_SIZE_INFORMATION, CONTAINER_INHERIT_ACE,
        DACL_SECURITY_INFORMATION, INHERITED_ACE, INHERIT_ONLY_ACE, LUA_TOKEN, NO_INHERITANCE,
        OBJECT_INHERIT_ACE, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
        SECURITY_ATTRIBUTES, SECURITY_CAPABILITIES, SECURITY_MAX_SID_SIZE, SID_AND_ATTRIBUTES,
        TOKEN_ADJUST_DEFAULT, TOKEN_ADJUST_SESSIONID, TOKEN_ASSIGN_PRIMARY, TOKEN_DEFAULT_DACL,
        TOKEN_DUPLICATE, TOKEN_ELEVATION, TOKEN_OWNER, TOKEN_QUERY, TOKEN_USER,
        UNPROTECTED_DACL_SECURITY_INFORMATION, WELL_KNOWN_SID_TYPE,
    };

    #[link(name = "kernel32")]
    extern "system" {
        fn SetErrorMode(umode: u32) -> u32;
    }

    #[link(name = "advapi32")]
    extern "system" {
        fn LogonUserW(
            lpszusername: *const u16,
            lpszdomain: *const u16,
            lpszpassword: *const u16,
            dwlogontype: u32,
            dwlogonprovider: u32,
            phtoken: *mut HANDLE,
        ) -> i32;
        fn ImpersonateLoggedOnUser(htoken: HANDLE) -> i32;
        fn RevertToSelf() -> i32;
    }

    #[link(name = "user32")]
    extern "system" {
        fn GetProcessWindowStation() -> HANDLE;
        fn CreateWindowStationW(
            lpwinsta: *const u16,
            dwflags: u32,
            dwdesiredaccess: u32,
            lpsa: *const SECURITY_ATTRIBUTES,
        ) -> HANDLE;
        fn SetProcessWindowStation(hwinsta: HANDLE) -> i32;
        fn CreateDesktopW(
            lpszdesktop: *const u16,
            lpszdevice: *const u16,
            pdevmode: *const core::ffi::c_void,
            dwflags: u32,
            dwdesiredaccess: u32,
            lpsa: *const SECURITY_ATTRIBUTES,
        ) -> HANDLE;
        fn CloseDesktop(hdesktop: HANDLE) -> i32;
        fn GetUserObjectInformationW(
            hobj: HANDLE,
            nindex: i32,
            pvinfo: *mut core::ffi::c_void,
            nlength: u32,
            lpnlengthneeded: *mut u32,
        ) -> i32;
    }
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_ALL_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT, FILE_GENERIC_EXECUTE,
        FILE_GENERIC_READ, FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES, FILE_READ_DATA,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TRAVERSE, OPEN_EXISTING, SYNCHRONIZE, WRITE_DAC,
        WRITE_OWNER,
    };
    use windows_sys::Win32::System::Pipes::CreatePipe;
    use windows_sys::Win32::System::SystemServices::{
        ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE, SE_GROUP_ENABLED,
    };
    use windows_sys::Win32::System::Threading::{
        CreateProcessAsUserW, CreateProcessW, DeleteProcThreadAttributeList, GetCurrentProcess,
        GetExitCodeProcess, InitializeProcThreadAttributeList, OpenProcessToken, TerminateProcess,
        UpdateProcThreadAttribute, WaitForSingleObject, CREATE_NO_WINDOW,
        CREATE_UNICODE_ENVIRONMENT, EXTENDED_STARTUPINFO_PRESENT, INFINITE,
        LPPROC_THREAD_ATTRIBUTE_LIST, PROCESS_INFORMATION, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
        PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES, STARTF_USESTDHANDLES, STARTUPINFOEXW,
    };

    const HRESULT_ALREADY_EXISTS: i32 =
        ((1u32 << 31) | (7u32 << 16) | (ERROR_ALREADY_EXISTS & 0xffff)) as i32;
    const WINDOWS_EXEC_EXTS: &[&str] = &[".com", ".exe", ".bat", ".cmd"];

    /// Custom capability name derived via `DeriveCapabilitySidsFromName` and granted only to
    /// OxideSwarm task AppContainers and worker-owned toolchain directories.
    pub const OXIDESWARM_TOOLCHAIN_CAPABILITY_NAME: &str = "oxideswarm.toolchain";
    /// Default filename under `<user_home>/.oxideswarm/` where every path whose DACL is modified
    /// by `prepare_worker_toolchains` is recorded before the modification takes place.
    pub const ACL_GRANT_LOG_FILENAME: &str = "windows_acl_grants.txt";

    /// Serialises toolchain and sandbox_base ACL preparation within the worker process.
    static PREPARE_LOCK: Mutex<()> = Mutex::new(());
    /// Serialises the brief window in `spawn_with_security_capabilities` where the child ends of
    /// the stdio pipes are marked inheritable for `CreateProcessW` and geschlossen in the parent,
    /// as well as the brief `SetProcessWindowStation` switch when creating a per-task desktop.
    /// Also acquired by standard `std::process::Command` spawns (`vswhere.exe`, `taskkill.exe`) so
    /// no concurrent process creation can observe a temporary Window Station switch or inherit a
    /// child pipe handle even if `bInheritHandles` is set (M2, M5, N7).
    pub(crate) static WINDOWS_SPAWN_LOCK: Mutex<()> = Mutex::new(());
    static MSVC_ENV_CACHE: OnceLock<Option<(PathBuf, String, Vec<PathBuf>)>> = OnceLock::new();

    fn to_wide(s: &OsStr) -> Vec<u16> {
        s.encode_wide().chain(std::iter::once(0)).collect()
    }

    fn strip_verbatim_prefix(raw: PathBuf) -> PathBuf {
        let s = raw.to_string_lossy();
        if let Some(stripped) = s.strip_prefix(r"\\?\UNC\") {
            PathBuf::from(format!(r"\\{stripped}"))
        } else if let Some(stripped) = s.strip_prefix(r"\\?\") {
            PathBuf::from(stripped)
        } else {
            raw
        }
    }

    fn lexically_resolve_dots(p: &Path) -> PathBuf {
        let mut out = PathBuf::new();
        for comp in p.components() {
            match comp {
                Component::Prefix(_) | Component::RootDir => out.push(comp.as_os_str()),
                Component::CurDir => {}
                Component::ParentDir => {
                    // Only pop a normal component; never pop past RootDir or Prefix.
                    if matches!(out.components().next_back(), Some(Component::Normal(_))) {
                        out.pop();
                    }
                }
                Component::Normal(c) => out.push(c),
            }
        }
        out
    }

    /// Canonicalizes `p` (or its longest existing ancestor prefix when `p` does not exist yet) and
    /// lexically resolves `.` and `..` components on any non-existent suffix so `<sandbox>\..\..`
    /// cannot bypass `path_starts_with_ci` and 8.3 short names (`RUNNER~1`) are expanded on the
    /// host before paths enter an AppContainer.
    pub fn normalize_win_path(p: &Path) -> PathBuf {
        if let Ok(canon) = std::fs::canonicalize(p) {
            return strip_verbatim_prefix(canon);
        }
        let lexical = lexically_resolve_dots(p);
        if let Ok(canon) = std::fs::canonicalize(&lexical) {
            return strip_verbatim_prefix(canon);
        }
        let mut tail: Vec<OsString> = Vec::new();
        let mut cur: &Path = &lexical;
        while let Some(parent) = cur.parent() {
            if let Some(name) = cur.file_name() {
                tail.push(name.to_os_string());
            }
            if !parent.as_os_str().is_empty() {
                if let Ok(canon_parent) = std::fs::canonicalize(parent) {
                    let mut rebuilt = strip_verbatim_prefix(canon_parent);
                    for part in tail.into_iter().rev() {
                        rebuilt.push(part);
                    }
                    return lexically_resolve_dots(&rebuilt);
                }
            }
            if parent == cur {
                break;
            }
            cur = parent;
        }
        lexical
    }

    /// Computes a deterministic 8-hex-character worker instance identifier from `sandbox_base` so
    /// each worker instance (and each test `Rig`) namespaces its AppContainer profiles under
    /// `oxideswarm.task.<worker8>.<run_token>` and never sweeps live profiles of another worker
    /// or parallel test (N3).
    pub fn worker_profile_prefix(sandbox_base: &Path) -> String {
        let norm = normalize_win_path(sandbox_base);
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for unit in norm.as_os_str().encode_wide() {
            let lower = if (b'A' as u16..=b'Z' as u16).contains(&unit) {
                unit + 32
            } else {
                unit
            };
            hash ^= (lower & 0xff) as u64;
            hash = hash.wrapping_mul(0x0100_0000_01b3);
            hash ^= (lower >> 8) as u64;
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        let folded = ((hash >> 32) as u32) ^ (hash as u32);
        format!("oxideswarm.task.{folded:08x}")
    }

    /// Returns `true` if `path` itself is a symlink or NTFS junction (`FILE_ATTRIBUTE_REPARSE_POINT`).
    /// Uses `symlink_metadata` so it never follows the reparse target.
    pub fn is_reparse_point(path: &Path) -> bool {
        std::fs::symlink_metadata(path)
            .map(|m| (m.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0)
            .unwrap_or(false)
    }

    /// Returns `true` if `child` is lexically or canonically inside `parent` (case-insensitive on
    /// Windows).
    pub fn path_starts_with_ci(child: &Path, parent: &Path) -> bool {
        if child.as_os_str().is_empty() || parent.as_os_str().is_empty() {
            return false;
        }
        let c_norm = normalize_win_path(child);
        let p_norm = normalize_win_path(parent);
        let mut c_iter = c_norm.components();
        for p_comp in p_norm.components() {
            let Some(c_comp) = c_iter.next() else {
                return false;
            };
            let p_str = p_comp.as_os_str().to_string_lossy();
            let c_str = c_comp.as_os_str().to_string_lossy();
            if !p_str.eq_ignore_ascii_case(&c_str) {
                return false;
            }
        }
        true
    }

    /// Queries `vswhere.exe` (when installed) for Visual Studio installation paths with VC tools.
    fn query_vswhere_install_paths() -> Vec<PathBuf> {
        let mut candidates = Vec::new();
        for pf_var in ["ProgramFiles(x86)", "ProgramFiles"] {
            if let Some(pf) = std::env::var_os(pf_var).map(PathBuf::from) {
                candidates.push(
                    pf.join("Microsoft Visual Studio")
                        .join("Installer")
                        .join("vswhere.exe"),
                );
            }
        }
        candidates.push(PathBuf::from(
            r"C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe",
        ));
        for vswhere in candidates {
            if !vswhere.is_file() {
                continue;
            }
            let output_res = {
                let _spawn_guard = WINDOWS_SPAWN_LOCK
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                std::process::Command::new(&vswhere)
                    .args([
                        "-latest",
                        "-products",
                        "*",
                        "-requires",
                        "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
                        "-property",
                        "installationPath",
                    ])
                    .stdin(std::process::Stdio::null())
                    .output()
            };
            let Ok(output) = output_res else {
                continue;
            };
            if !output.status.success() {
                continue;
            }
            let text = String::from_utf8_lossy(&output.stdout);
            let paths: Vec<PathBuf> = text
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(PathBuf::from)
                .filter(|p| p.is_dir())
                .collect();
            if !paths.is_empty() {
                return paths;
            }
        }
        Vec::new()
    }

    fn discover_msvc_details() -> Option<(PathBuf, String, Vec<PathBuf>)> {
        MSVC_ENV_CACHE
            .get_or_init(|| {
                let mut vs_roots = query_vswhere_install_paths();
                vs_roots.push(PathBuf::from(r"C:\BuildTools"));
                for pf_var in ["ProgramFiles", "ProgramFiles(x86)"] {
                    if let Some(pf) = std::env::var_os(pf_var).map(PathBuf::from) {
                        for year in ["2022", "2019"] {
                            for ed in ["Enterprise", "Professional", "Community", "BuildTools"] {
                                let candidate =
                                    pf.join("Microsoft Visual Studio").join(year).join(ed);
                                if !vs_roots.contains(&candidate) {
                                    vs_roots.push(candidate);
                                }
                            }
                        }
                    }
                }
                let mut msvc_ver_dir: Option<PathBuf> = None;
                let mut matched_vs_root: Option<PathBuf> = None;
                for root in vs_roots {
                    let msvc_base = root.join("VC").join("Tools").join("MSVC");
                    if let Ok(entries) = std::fs::read_dir(&msvc_base) {
                        let mut vers: Vec<PathBuf> = entries
                            .flatten()
                            .map(|e| e.path())
                            .filter(|p| p.is_dir())
                            .collect();
                        vers.sort();
                        if let Some(best) = vers.into_iter().rev().find(|v| {
                            v.join("bin")
                                .join("Hostx64")
                                .join("x64")
                                .join("link.exe")
                                .is_file()
                        }) {
                            msvc_ver_dir = Some(best);
                            matched_vs_root = Some(root);
                            break;
                        }
                    }
                }
                let msvc_ver_dir = msvc_ver_dir?;
                let matched_vs_root = matched_vs_root?;
                let msvc_bin = msvc_ver_dir.join("bin").join("Hostx64").join("x64");
                let msvc_lib = msvc_ver_dir.join("lib").join("x64");

                let kits_root = PathBuf::from(r"C:\Program Files (x86)\Windows Kits\10");
                let kits_base = kits_root.join("Lib");
                let mut sdk_ver_dir: Option<PathBuf> = None;
                if let Ok(entries) = std::fs::read_dir(&kits_base) {
                    let mut vers: Vec<PathBuf> = entries
                        .flatten()
                        .map(|e| e.path())
                        .filter(|p| p.is_dir())
                        .collect();
                    vers.sort();
                    sdk_ver_dir = vers.into_iter().rev().find(|v| {
                        v.join("um").join("x64").join("kernel32.lib").is_file()
                            && v.join("ucrt").join("x64").join("ucrt.lib").is_file()
                    });
                }
                let sdk_ver_dir = sdk_ver_dir?;
                let ucrt_lib = sdk_ver_dir.join("ucrt").join("x64");
                let um_lib = sdk_ver_dir.join("um").join("x64");
                let lib_str = format!(
                    "{};{};{}",
                    msvc_lib.display(),
                    ucrt_lib.display(),
                    um_lib.display()
                );
                let grant_roots = vec![matched_vs_root, kits_root];
                Some((msvc_bin, lib_str, grant_roots))
            })
            .clone()
    }

    /// Resolves the host MSVC `bin\Hostx64\x64` directory and `LIB` search path once on Windows
    /// (querying `vswhere.exe` first, then standard Visual Studio installation roots) so `rustc`
    /// inside an AppContainer can locate `link.exe` and Windows SDK `.lib` files via `PATH` and
    /// `LIB`.
    pub fn resolve_windows_msvc_env() -> Option<(PathBuf, String)> {
        let (bin, lib, _) = discover_msvc_details()?;
        Some((bin, lib))
    }

    /// Resolves `program` against `search_path` on Windows. Returns `None` if the executable
    /// does not exist. Expands 8.3 short names on non-reparse paths so `CreateProcessW` inside an
    /// AppContainer never walks denied ancestor directories with `FindFirstFileW`.
    pub(super) fn resolve_program(program: &str, search_path: &str) -> Option<String> {
        let norm_file = |found: &Path| -> String {
            if !is_reparse_point(found) {
                normalize_win_path(found).to_string_lossy().into_owned()
            } else {
                found.to_string_lossy().into_owned()
            }
        };
        let p = Path::new(program);
        let has_sep = program.contains('/') || program.contains('\\') || program.contains(':');
        if has_sep {
            if p.is_file() {
                return Some(norm_file(p));
            }
            for ext in WINDOWS_EXEC_EXTS {
                let candidate = PathBuf::from(format!("{program}{ext}"));
                if candidate.is_file() {
                    return Some(norm_file(&candidate));
                }
            }
            if !program.starts_with("/bin/") && !program.starts_with("/usr/bin/") {
                return None;
            }
        }
        let bare = p.file_name().and_then(|n| n.to_str()).unwrap_or(program);
        for dir in std::env::split_paths(search_path) {
            let base = if dir.as_os_str().is_empty() {
                PathBuf::from(".")
            } else {
                dir
            };
            let direct = base.join(bare);
            if direct.is_file() {
                return Some(norm_file(&direct));
            }
            for ext in WINDOWS_EXEC_EXTS {
                let with_ext = base.join(format!("{bare}{ext}"));
                if with_ext.is_file() {
                    return Some(norm_file(&with_ext));
                }
            }
        }
        None
    }

    /// RAII wrapper for a SID allocated from a well-known SID, current process token, SDDL string,
    /// or `DeriveCapabilitySidsFromName`.
    pub struct OwnedSid(Vec<u8>);

    impl OwnedSid {
        fn from_raw_psid(raw: PSID) -> io::Result<Self> {
            if raw.is_null() || unsafe { IsValidSid(raw) } == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid Windows SID pointer",
                ));
            }
            let len = unsafe { GetLengthSid(raw) } as usize;
            let slice = unsafe { std::slice::from_raw_parts(raw as *const u8, len) };
            Ok(Self(slice.to_vec()))
        }

        pub fn from_sid_string(sid_str: &str) -> io::Result<Self> {
            let wide = to_wide(OsStr::new(sid_str));
            let mut raw_sid: PSID = std::ptr::null_mut();
            let ok = unsafe { ConvertStringSidToSidW(wide.as_ptr(), &mut raw_sid) };
            if ok == 0 || raw_sid.is_null() {
                return Err(io::Error::last_os_error());
            }
            let res = Self::from_raw_psid(raw_sid);
            unsafe {
                LocalFree(raw_sid as HLOCAL);
            }
            res
        }

        pub fn from_well_known(sid_type: WELL_KNOWN_SID_TYPE) -> io::Result<Self> {
            let mut buf = vec![0u8; SECURITY_MAX_SID_SIZE as usize];
            let mut size = buf.len() as u32;
            let ok = unsafe {
                CreateWellKnownSid(
                    sid_type,
                    std::ptr::null_mut(),
                    buf.as_mut_ptr() as PSID,
                    &mut size,
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            buf.truncate(size as usize);
            Ok(Self(buf))
        }

        pub fn current_user() -> io::Result<Self> {
            let mut token: HANDLE = std::ptr::null_mut();
            if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let mut needed = 0u32;
            unsafe {
                let _ = GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut needed);
            }
            let mut buf = vec![0u8; needed as usize];
            let ok = unsafe {
                GetTokenInformation(
                    token,
                    TokenUser,
                    buf.as_mut_ptr() as *mut _,
                    needed,
                    &mut needed,
                )
            };
            unsafe {
                CloseHandle(token);
            }
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            let token_user = unsafe { &*(buf.as_ptr() as *const TOKEN_USER) };
            Self::from_raw_psid(token_user.User.Sid)
        }

        /// Derives the custom capability SID for `oxideswarm.toolchain` using
        /// `DeriveCapabilitySidsFromName`. Unlike `S-1-15-2-1` (`ALL APPLICATION PACKAGES`), no
        /// other AppContainer on the host holds this capability unless explicitly spawned with it.
        pub fn oxideswarm_toolchain() -> io::Result<Self> {
            Self::capability_from_name(OXIDESWARM_TOOLCHAIN_CAPABILITY_NAME)
        }

        pub fn capability_from_name(name: &str) -> io::Result<Self> {
            let wide = to_wide(OsStr::new(name));
            let mut group_sids: *mut PSID = std::ptr::null_mut();
            let mut group_count: u32 = 0;
            let mut cap_sids: *mut PSID = std::ptr::null_mut();
            let mut cap_count: u32 = 0;
            let ok = unsafe {
                DeriveCapabilitySidsFromName(
                    wide.as_ptr(),
                    &mut group_sids,
                    &mut group_count,
                    &mut cap_sids,
                    &mut cap_count,
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            let result = if cap_count > 0 && !cap_sids.is_null() {
                let first = unsafe { *cap_sids };
                Self::from_raw_psid(first)
            } else {
                Err(io::Error::other(
                    "DeriveCapabilitySidsFromName returned zero capability SIDs",
                ))
            };
            unsafe {
                if !group_sids.is_null() {
                    for i in 0..group_count as usize {
                        let s = *group_sids.add(i);
                        if !s.is_null() {
                            LocalFree(s as HLOCAL);
                        }
                    }
                    LocalFree(group_sids as HLOCAL);
                }
                if !cap_sids.is_null() {
                    for i in 0..cap_count as usize {
                        let s = *cap_sids.add(i);
                        if !s.is_null() {
                            LocalFree(s as HLOCAL);
                        }
                    }
                    LocalFree(cap_sids as HLOCAL);
                }
            }
            result
        }

        pub fn to_sid_string(&self) -> io::Result<String> {
            let mut wide_ptr: *mut u16 = std::ptr::null_mut();
            let ok = unsafe { ConvertSidToStringSidW(self.as_psid(), &mut wide_ptr) };
            if ok == 0 || wide_ptr.is_null() {
                return Err(io::Error::last_os_error());
            }
            let mut len = 0usize;
            unsafe {
                while *wide_ptr.add(len) != 0 {
                    len += 1;
                }
            }
            let slice = unsafe { std::slice::from_raw_parts(wide_ptr, len) };
            let s = OsString::from_wide(slice).to_string_lossy().into_owned();
            unsafe {
                LocalFree(wide_ptr as HLOCAL);
            }
            Ok(s)
        }

        pub fn as_psid(&self) -> PSID {
            self.0.as_ptr() as PSID
        }
    }

    /// Returns the string representation (`S-1-15-3-...`) of the `oxideswarm.toolchain` capability
    /// SID on this Windows host.
    pub fn oxideswarm_toolchain_sid_string() -> io::Result<String> {
        OwnedSid::oxideswarm_toolchain()?.to_sid_string()
    }

    fn explicit_access(
        sid: PSID,
        access_mask: u32,
        access_mode: i32,
        inheritance: ACE_FLAGS,
    ) -> EXPLICIT_ACCESS_W {
        EXPLICIT_ACCESS_W {
            grfAccessPermissions: access_mask,
            grfAccessMode: access_mode,
            grfInheritance: inheritance,
            Trustee: TRUSTEE_W {
                pMultipleTrustee: std::ptr::null_mut(),
                MultipleTrusteeOperation: 0,
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_UNKNOWN,
                ptstrName: sid as *mut u16,
            },
        }
    }

    fn dacl_has_ace(
        path: &Path,
        target_sid: PSID,
        ace_type: u32,
        required_mask: u32,
        required_flags: u32,
    ) -> bool {
        let wide_path = to_wide(path.as_os_str());
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        let rc = unsafe {
            GetNamedSecurityInfoW(
                wide_path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut sd,
            )
        };
        if rc != ERROR_SUCCESS || dacl.is_null() {
            if !sd.is_null() {
                unsafe {
                    LocalFree(sd as HLOCAL);
                }
            }
            return false;
        }

        let mut acl_info = ACL_SIZE_INFORMATION::default();
        let ok = unsafe {
            GetAclInformation(
                dacl,
                &mut acl_info as *mut _ as *mut _,
                std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                AclSizeInformation,
            )
        };
        let mut found = false;
        if ok != 0 {
            for idx in 0..acl_info.AceCount {
                let mut ace_ptr: *mut core::ffi::c_void = std::ptr::null_mut();
                if unsafe { GetAce(dacl, idx, &mut ace_ptr) } != 0 && !ace_ptr.is_null() {
                    let header = unsafe { &*(ace_ptr as *const ACE_HEADER) };
                    if header.AceType as u32 == ace_type
                        && (header.AceFlags as u32 & INHERIT_ONLY_ACE) == 0
                        && (header.AceFlags as u32 & required_flags) == required_flags
                    {
                        let ace = unsafe { &*(ace_ptr as *const ACCESS_ALLOWED_ACE) };
                        let ace_sid = (&ace.SidStart as *const u32) as PSID;
                        if unsafe { EqualSid(ace_sid, target_sid) } != 0
                            && (ace.Mask & required_mask) == required_mask
                        {
                            found = true;
                            break;
                        }
                    }
                }
            }
        }
        if !sd.is_null() {
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        found
    }

    pub fn dacl_already_grants_sid_with_flags(
        path: &Path,
        target_sid: PSID,
        required_mask: u32,
        required_flags: u32,
    ) -> bool {
        dacl_has_ace(
            path,
            target_sid,
            ACCESS_ALLOWED_ACE_TYPE,
            required_mask,
            required_flags,
        )
    }

    pub fn dacl_already_grants_sid(path: &Path, target_sid: PSID, required_mask: u32) -> bool {
        dacl_already_grants_sid_with_flags(path, target_sid, required_mask, 0)
    }

    pub fn dacl_already_denies_sid(path: &Path, target_sid: PSID, required_mask: u32) -> bool {
        dacl_has_ace(path, target_sid, ACCESS_DENIED_ACE_TYPE, required_mask, 0)
    }

    /// Merges `entries` into `path`'s existing DACL without disturbing existing ACEs or
    /// inheritance. Refuses to operate on symlinks or NTFS junctions (`is_reparse_point`).
    fn merge_entries_on_path(path: &Path, entries: &[EXPLICIT_ACCESS_W]) -> io::Result<()> {
        if !path.exists() || entries.is_empty() || is_reparse_point(path) {
            return Ok(());
        }
        let wide_path = to_wide(path.as_os_str());
        let mut old_dacl: *mut ACL = std::ptr::null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        let rc = unsafe {
            GetNamedSecurityInfoW(
                wide_path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut old_dacl,
                std::ptr::null_mut(),
                &mut sd,
            )
        };
        if rc != ERROR_SUCCESS {
            if !sd.is_null() {
                unsafe {
                    LocalFree(sd as HLOCAL);
                }
            }
            return Err(io::Error::from_raw_os_error(rc as i32));
        }

        let mut new_dacl: *mut ACL = std::ptr::null_mut();
        let rc = unsafe {
            SetEntriesInAclW(
                entries.len() as u32,
                entries.as_ptr(),
                old_dacl,
                &mut new_dacl,
            )
        };
        if rc != ERROR_SUCCESS {
            if !sd.is_null() {
                unsafe {
                    LocalFree(sd as HLOCAL);
                }
            }
            return Err(io::Error::from_raw_os_error(rc as i32));
        }

        let set_rc = unsafe {
            SetNamedSecurityInfoW(
                wide_path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                new_dacl,
                std::ptr::null(),
            )
        };
        unsafe {
            if !new_dacl.is_null() {
                LocalFree(new_dacl as HLOCAL);
            }
            if !sd.is_null() {
                LocalFree(sd as HLOCAL);
            }
        }
        if set_rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(set_rc as i32));
        }
        Ok(())
    }

    /// Merges `entries` into `path`'s existing DACL while also clearing `SE_DACL_PROTECTED`
    /// (`UNPROTECTED_DACL_SECURITY_INFORMATION`) so inheritable ACEs from `path`'s parent
    /// directory propagate cleanly. Refuses to operate on symlinks or NTFS junctions.
    fn merge_entries_on_path_unprotected(
        path: &Path,
        entries: &[EXPLICIT_ACCESS_W],
    ) -> io::Result<()> {
        if !path.exists() || entries.is_empty() || is_reparse_point(path) {
            return Ok(());
        }
        let wide_path = to_wide(path.as_os_str());
        let mut old_dacl: *mut ACL = std::ptr::null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        let rc = unsafe {
            GetNamedSecurityInfoW(
                wide_path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut old_dacl,
                std::ptr::null_mut(),
                &mut sd,
            )
        };
        if rc != ERROR_SUCCESS {
            if !sd.is_null() {
                unsafe {
                    LocalFree(sd as HLOCAL);
                }
            }
            return Err(io::Error::from_raw_os_error(rc as i32));
        }

        let mut new_dacl: *mut ACL = std::ptr::null_mut();
        let rc = unsafe {
            SetEntriesInAclW(
                entries.len() as u32,
                entries.as_ptr(),
                old_dacl,
                &mut new_dacl,
            )
        };
        if rc != ERROR_SUCCESS {
            if !sd.is_null() {
                unsafe {
                    LocalFree(sd as HLOCAL);
                }
            }
            return Err(io::Error::from_raw_os_error(rc as i32));
        }

        let set_rc = unsafe {
            SetNamedSecurityInfoW(
                wide_path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | UNPROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                new_dacl,
                std::ptr::null(),
            )
        };
        unsafe {
            if !new_dacl.is_null() {
                LocalFree(new_dacl as HLOCAL);
            }
            if !sd.is_null() {
                LocalFree(sd as HLOCAL);
            }
        }
        if set_rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(set_rc as i32));
        }
        Ok(())
    }

    /// Replaces `path`'s DACL with a fresh protected DACL (`PROTECTED_DACL_SECURITY_INFORMATION`)
    /// built purely from `entries`. Refuses to operate on symlinks or NTFS junctions.
    fn set_protected_dacl_on_path(path: &Path, entries: &[EXPLICIT_ACCESS_W]) -> io::Result<()> {
        if is_reparse_point(path) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("refusing to set DACL on reparse point {}", path.display()),
            ));
        }
        let wide_path = to_wide(path.as_os_str());
        let mut new_dacl: *mut ACL = std::ptr::null_mut();
        let rc = unsafe {
            SetEntriesInAclW(
                entries.len() as u32,
                entries.as_ptr(),
                std::ptr::null(),
                &mut new_dacl,
            )
        };
        if rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(rc as i32));
        }
        let set_rc = unsafe {
            SetNamedSecurityInfoW(
                wide_path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                new_dacl,
                std::ptr::null(),
            )
        };
        unsafe {
            if !new_dacl.is_null() {
                LocalFree(new_dacl as HLOCAL);
            }
        }
        if set_rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(set_rc as i32));
        }
        Ok(())
    }

    /// Queries the current DACL of `path` as an SDDL string (`D:...`) via
    /// `GetNamedSecurityInfoW` + `ConvertSecurityDescriptorToStringSecurityDescriptorW`.
    pub fn get_path_dacl_sddl(path: &Path) -> io::Result<String> {
        let wide_path = to_wide(path.as_os_str());
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        let rc = unsafe {
            GetNamedSecurityInfoW(
                wide_path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut sd,
            )
        };
        if rc != ERROR_SUCCESS {
            if !sd.is_null() {
                unsafe {
                    LocalFree(sd as HLOCAL);
                }
            }
            return Err(io::Error::from_raw_os_error(rc as i32));
        }
        let mut sddl_ptr: *mut u16 = std::ptr::null_mut();
        let mut sddl_len: u32 = 0;
        let ok = unsafe {
            ConvertSecurityDescriptorToStringSecurityDescriptorW(
                sd,
                SDDL_REVISION_1,
                DACL_SECURITY_INFORMATION,
                &mut sddl_ptr,
                &mut sddl_len,
            )
        };
        unsafe {
            if !sd.is_null() {
                LocalFree(sd as HLOCAL);
            }
        }
        if ok == 0 || sddl_ptr.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut len = 0usize;
        unsafe {
            while *sddl_ptr.add(len) != 0 {
                len += 1;
            }
        }
        let slice = unsafe { std::slice::from_raw_parts(sddl_ptr, len) };
        let sddl = OsString::from_wide(slice).to_string_lossy().into_owned();
        unsafe {
            LocalFree(sddl_ptr as HLOCAL);
        }
        Ok(sddl)
    }

    /// Restores `path`'s DACL (including its `SE_DACL_PROTECTED` vs unprotected inheritance state)
    /// from an SDDL string previously captured by [`get_path_dacl_sddl`].
    pub fn restore_path_dacl_sddl(path: &Path, sddl: &str) -> io::Result<()> {
        if !path.exists() || is_reparse_point(path) {
            return Ok(());
        }
        let wide_sddl = to_wide(OsStr::new(sddl));
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        let mut sd_size: u32 = 0;
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide_sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut sd,
                &mut sd_size,
            )
        };
        if ok == 0 || sd.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut control: u16 = 0;
        let mut revision: u32 = 0;
        let _ = unsafe { GetSecurityDescriptorControl(sd, &mut control, &mut revision) };
        let is_protected = (control & 0x1000) != 0 || sddl.starts_with("D:P");

        let mut dacl_present: i32 = 0;
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut dacl_defaulted: i32 = 0;
        let dacl_ok = unsafe {
            GetSecurityDescriptorDacl(sd, &mut dacl_present, &mut dacl, &mut dacl_defaulted)
        };
        if dacl_ok == 0 || dacl_present == 0 {
            unsafe {
                LocalFree(sd as HLOCAL);
            }
            return Err(io::Error::last_os_error());
        }

        // When restoring an unprotected DACL (`UNPROTECTED_DACL_SECURITY_INFORMATION`), remove
        // any `INHERITED_ACE` entries from `dacl` before calling `SetNamedSecurityInfoW` so
        // Windows re-computes inherited ACEs cleanly from the parent without duplicating them.
        if !is_protected && !dacl.is_null() {
            let mut acl_info = ACL_SIZE_INFORMATION::default();
            let info_ok = unsafe {
                GetAclInformation(
                    dacl,
                    &mut acl_info as *mut _ as *mut _,
                    std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                    AclSizeInformation,
                )
            };
            if info_ok != 0 {
                for idx in (0..acl_info.AceCount).rev() {
                    let mut ace_ptr: *mut core::ffi::c_void = std::ptr::null_mut();
                    if unsafe { GetAce(dacl, idx, &mut ace_ptr) } != 0 && !ace_ptr.is_null() {
                        let header = unsafe { &*(ace_ptr as *const ACE_HEADER) };
                        if (header.AceFlags as u32 & INHERITED_ACE) != 0 {
                            unsafe {
                                let _ = DeleteAce(dacl, idx);
                            }
                        }
                    }
                }
            }
        }

        let sec_info = DACL_SECURITY_INFORMATION
            | if is_protected {
                PROTECTED_DACL_SECURITY_INFORMATION
            } else {
                UNPROTECTED_DACL_SECURITY_INFORMATION
            };
        let wide_path = to_wide(path.as_os_str());
        let set_rc = unsafe {
            SetNamedSecurityInfoW(
                wide_path.as_ptr(),
                SE_FILE_OBJECT,
                sec_info,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                dacl,
                std::ptr::null(),
            )
        };
        unsafe {
            LocalFree(sd as HLOCAL);
        }
        if set_rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(set_rc as i32));
        }
        // `SetNamedSecurityInfoW` always sets `SE_DACL_AUTO_INHERITED` (`AI`). If the original
        // SDDL did not have `AI` (e.g. `D:(A;OICIID;FA;;;SY)(A;OICIID;FA;;;BA)(A;OICIID;FA;;;LA)`
        // on `windows-latest`), apply the exact parsed security descriptor via `SetFileSecurityW`
        // so `get_path_dacl_sddl` matches `sddl` byte-for-byte.
        if get_path_dacl_sddl(path).ok().as_deref() != Some(sddl) {
            let mut raw_sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
            let mut raw_size: u32 = 0;
            let raw_ok = unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    wide_sddl.as_ptr(),
                    SDDL_REVISION_1,
                    &mut raw_sd,
                    &mut raw_size,
                )
            };
            if raw_ok != 0 && !raw_sd.is_null() {
                unsafe {
                    let _ = SetFileSecurityW(wide_path.as_ptr(), sec_info, raw_sd);
                    LocalFree(raw_sd as HLOCAL);
                }
            }
        }
        Ok(())
    }

    /// Removes all explicit (non-inherited) ACEs — both `ACCESS_ALLOWED_ACE_TYPE` and
    /// `ACCESS_DENIED_ACE_TYPE` — for `target_sid` from `path`'s DACL (`DeleteAce`).
    fn remove_sid_aces_from_path(path: &Path, target_sid: PSID) -> io::Result<()> {
        if !path.exists() || is_reparse_point(path) {
            return Ok(());
        }
        let wide_path = to_wide(path.as_os_str());
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        let rc = unsafe {
            GetNamedSecurityInfoW(
                wide_path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut sd,
            )
        };
        if rc != ERROR_SUCCESS {
            if !sd.is_null() {
                unsafe {
                    LocalFree(sd as HLOCAL);
                }
            }
            return Err(io::Error::from_raw_os_error(rc as i32));
        }
        if dacl.is_null() {
            if !sd.is_null() {
                unsafe {
                    LocalFree(sd as HLOCAL);
                }
            }
            return Ok(());
        }

        let mut acl_info = ACL_SIZE_INFORMATION::default();
        let ok = unsafe {
            GetAclInformation(
                dacl,
                &mut acl_info as *mut _ as *mut _,
                std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                AclSizeInformation,
            )
        };
        let mut removed_any = false;
        if ok != 0 {
            for idx in (0..acl_info.AceCount).rev() {
                let mut ace_ptr: *mut core::ffi::c_void = std::ptr::null_mut();
                if unsafe { GetAce(dacl, idx, &mut ace_ptr) } != 0 && !ace_ptr.is_null() {
                    let header = unsafe { &*(ace_ptr as *const ACE_HEADER) };
                    let ace_type = header.AceType as u32;
                    if ace_type == ACCESS_ALLOWED_ACE_TYPE || ace_type == ACCESS_DENIED_ACE_TYPE {
                        let ace = unsafe { &*(ace_ptr as *const ACCESS_ALLOWED_ACE) };
                        let ace_sid = (&ace.SidStart as *const u32) as PSID;
                        if unsafe { EqualSid(ace_sid, target_sid) } != 0
                            && unsafe { DeleteAce(dacl, idx) } != 0
                        {
                            removed_any = true;
                        }
                    }
                }
            }
        }

        let mut set_rc = ERROR_SUCCESS;
        if removed_any {
            set_rc = unsafe {
                SetNamedSecurityInfoW(
                    wide_path.as_ptr(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    dacl,
                    std::ptr::null(),
                )
            };
        }
        if !sd.is_null() {
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        if set_rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(set_rc as i32));
        }
        Ok(())
    }

    /// Returns the path of the reversible ACL grant manifest file.
    /// Honours `RUSTY_GRID_ACL_GRANT_LOG` if set; otherwise uses
    /// `<user_home>/.oxideswarm/windows_acl_grants.txt`.
    pub fn acl_grant_manifest_path(host: &HostDirs) -> PathBuf {
        if let Some(custom) = std::env::var_os("RUSTY_GRID_ACL_GRANT_LOG") {
            if !custom.is_empty() {
                return PathBuf::from(custom);
            }
        }
        if !host.user_home.as_os_str().is_empty() {
            return host
                .user_home
                .join(".oxideswarm")
                .join(ACL_GRANT_LOG_FILENAME);
        }
        std::env::temp_dir().join(ACL_GRANT_LOG_FILENAME)
    }

    fn record_manifest_line(manifest_path: Option<&Path>, line: &str) -> io::Result<()> {
        let Some(manifest) = manifest_path else {
            return Ok(());
        };
        if let Some(parent) = manifest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let existing = std::fs::read_to_string(manifest).unwrap_or_default();
        if existing
            .lines()
            .any(|l| l.trim().eq_ignore_ascii_case(line))
        {
            return Ok(());
        }
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(manifest)?;
        writeln!(file, "{line}")?;
        Ok(())
    }

    fn record_acl_grant(manifest_path: Option<&Path>, granted_path: &Path) -> io::Result<()> {
        let norm = normalize_win_path(granted_path);
        let line = norm.to_string_lossy().into_owned();
        record_manifest_line(manifest_path, &line)
    }

    fn record_sddl_restore(
        manifest_path: Option<&Path>,
        target_path: &Path,
        orig_sddl: &str,
    ) -> io::Result<()> {
        let Some(manifest) = manifest_path else {
            return Ok(());
        };
        let norm = normalize_win_path(target_path);
        let prefix = format!("SDDL|{}|", norm.to_string_lossy());
        let existing = std::fs::read_to_string(manifest).unwrap_or_default();
        if existing.lines().any(|l| {
            l.trim()
                .to_ascii_lowercase()
                .starts_with(&prefix.to_ascii_lowercase())
        }) {
            return Ok(());
        }
        let line = format!("{prefix}{orig_sddl}");
        record_manifest_line(manifest_path, &line)
    }

    /// Returns `true` if the current worker process token is elevated (`TokenElevation != 0`).
    pub fn is_process_elevated() -> bool {
        unsafe {
            let mut token: HANDLE = std::ptr::null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0
                || token.is_null()
            {
                return false;
            }
            let mut elev = TOKEN_ELEVATION { TokenIsElevated: 0 };
            let mut needed: u32 = 0;
            let ok = GetTokenInformation(
                token,
                TokenElevation,
                &mut elev as *mut TOKEN_ELEVATION as *mut core::ffi::c_void,
                std::mem::size_of::<TOKEN_ELEVATION>() as u32,
                &mut needed,
            );
            CloseHandle(token);
            ok != 0 && elev.TokenIsElevated != 0
        }
    }

    /// Sets `TokenOwner = user_sid` and merges an explicit `CurrentUser:(GENERIC_ALL | FILE_ALL_ACCESS)`
    /// (`0x101F_01FF`, `NO_INHERITANCE`) allow ACE into `token`'s `TokenDefaultDacl` (never adding
    /// `BUILTIN\Users` or `Everyone`).
    unsafe fn apply_current_user_owner_and_default_dacl(token: HANDLE, user_sid: PSID) {
        if token.is_null() {
            return;
        }
        let owner = TOKEN_OWNER { Owner: user_sid };
        let _ = SetTokenInformation(
            token,
            TokenOwner,
            &owner as *const TOKEN_OWNER as *const core::ffi::c_void,
            std::mem::size_of::<TOKEN_OWNER>() as u32,
        );

        let mut needed: u32 = 0;
        let _ = GetTokenInformation(
            token,
            TokenDefaultDacl,
            std::ptr::null_mut(),
            0,
            &mut needed,
        );
        if needed == 0 {
            return;
        }
        let mut buf = vec![0u8; needed as usize];
        if GetTokenInformation(
            token,
            TokenDefaultDacl,
            buf.as_mut_ptr() as *mut core::ffi::c_void,
            needed,
            &mut needed,
        ) == 0
        {
            return;
        }
        let tdd = &*(buf.as_ptr() as *const TOKEN_DEFAULT_DACL);
        if tdd.DefaultDacl.is_null() {
            return;
        }
        const FULL_DEFAULT_MASK: u32 = 0x101F_01FF;
        let entry = explicit_access(user_sid, FULL_DEFAULT_MASK, GRANT_ACCESS, NO_INHERITANCE);
        let mut new_dacl: *mut ACL = std::ptr::null_mut();
        if SetEntriesInAclW(1, &entry, tdd.DefaultDacl, &mut new_dacl) == ERROR_SUCCESS
            && !new_dacl.is_null()
        {
            let new_tdd = TOKEN_DEFAULT_DACL {
                DefaultDacl: new_dacl,
            };
            let _ = SetTokenInformation(
                token,
                TokenDefaultDacl,
                &new_tdd as *const TOKEN_DEFAULT_DACL as *const core::ffi::c_void,
                std::mem::size_of::<TOKEN_DEFAULT_DACL>() as u32,
            );
            LocalFree(new_dacl as HLOCAL);
        }
    }

    /// Creates and caches `(primary_lua_token, logon_token)` when the worker runs elevated
    /// (`is_process_elevated() == true`):
    /// 1. Calls `LogonUserW("OxideTask", ".", "unused", LOGON32_LOGON_NEW_CREDENTIALS (9),
    ///    LOGON32_PROVIDER_WINNT50 (3))` under the worker process token (without prior
    ///    impersonation) so LSASS allocates a fresh logon session (`NewLogonId`) whose per-logon
    ///    DosDevices directory `\Sessions\<S>\DosDevices\<NewLogonId>` does not exist yet
    ///    (`pDeviceMap == NULL`).
    /// 2. Sets `TokenOwner = CurrentUser` and `TokenDefaultDacl` granting `CurrentUser` on
    ///    `logon_token` while `BUILTIN\Administrators` is still enabled on `logon_token`.
    /// 3. Impersonates `logon_token` and opens `"NUL"` via `CreateFileW`. Inside the kernel,
    ///    `SeGetTokenDeviceMap` -> `ObpCreateDosDevicesDirectory` calls `ZwCreateDirectoryObject`
    ///    with `SecurityDescriptor = NULL` while the effective token's `TokenOwner` is
    ///    `CurrentUser`, so `SeAssignSecurityEx` expands the parent `\Sessions\<S>\DosDevices`
    ///    `CREATOR OWNER` (`CO`) ACE into `(A;;CCDCLCSWSDRCWDWO;;;<CurrentUser>)` on
    ///    `\Sessions\<S>\DosDevices\<NewLogonId>` without editing any Object Manager DACL.
    /// 4. Filters `logon_token` with `CreateRestrictedToken(..., LUA_TOKEN, ...)` and duplicates
    ///    it to a `TokenPrimary` token with `TokenOwner = CurrentUser` and `TokenDefaultDacl`
    ///    granting `CurrentUser` (in addition to `SYSTEM` and `Administrators`, never
    ///    `BUILTIN\Users` or `Everyone`).
    ///
    /// Returns `None` when the worker is not elevated.
    fn get_or_init_worker_lua_tokens() -> Option<(HANDLE, HANDLE)> {
        static WORKER_LUA_TOKENS: OnceLock<Option<(usize, usize)>> = OnceLock::new();
        WORKER_LUA_TOKENS
            .get_or_init(|| {
                if !is_process_elevated() {
                    return None;
                }
                ensure_process_token_default_dacl_grants_current_user();
                let user_sid = OwnedSid::current_user().ok()?;
                unsafe {
                    let wide_user = to_wide(OsStr::new("OxideTask"));
                    let wide_domain = to_wide(OsStr::new("."));
                    let wide_pass = to_wide(OsStr::new("unused"));
                    let mut logon_token: HANDLE = std::ptr::null_mut();
                    // LOGON32_LOGON_NEW_CREDENTIALS = 9, LOGON32_PROVIDER_WINNT50 = 3
                    let logon_ok = LogonUserW(
                        wide_user.as_ptr(),
                        wide_domain.as_ptr(),
                        wide_pass.as_ptr(),
                        9,
                        3,
                        &mut logon_token,
                    );
                    if logon_ok != 0 && !logon_token.is_null() {
                        apply_current_user_owner_and_default_dacl(logon_token, user_sid.as_psid());
                        if ImpersonateLoggedOnUser(logon_token) != 0 {
                            let wide_nul = to_wide(OsStr::new("NUL"));
                            let h_nul = CreateFileW(
                                wide_nul.as_ptr(),
                                FILE_READ_ATTRIBUTES,
                                FILE_SHARE_READ | FILE_SHARE_WRITE,
                                std::ptr::null(),
                                OPEN_EXISTING,
                                0,
                                std::ptr::null_mut(),
                            );
                            if !h_nul.is_null() && h_nul != INVALID_HANDLE_VALUE {
                                CloseHandle(h_nul);
                            }
                            let _ = RevertToSelf();
                        }
                        let mut lua_logon: HANDLE = std::ptr::null_mut();
                        let ok = CreateRestrictedToken(
                            logon_token,
                            LUA_TOKEN,
                            0,
                            std::ptr::null(),
                            0,
                            std::ptr::null(),
                            0,
                            std::ptr::null(),
                            &mut lua_logon,
                        );
                        if ok != 0 && !lua_logon.is_null() {
                            apply_current_user_owner_and_default_dacl(
                                lua_logon,
                                user_sid.as_psid(),
                            );
                            let mut primary_token: HANDLE = std::ptr::null_mut();
                            let dup_ok = DuplicateTokenEx(
                                lua_logon,
                                TOKEN_ASSIGN_PRIMARY
                                    | TOKEN_DUPLICATE
                                    | TOKEN_QUERY
                                    | TOKEN_ADJUST_DEFAULT
                                    | TOKEN_ADJUST_SESSIONID,
                                std::ptr::null(),
                                SecurityImpersonation,
                                TokenPrimary,
                                &mut primary_token,
                            );
                            CloseHandle(lua_logon);
                            if dup_ok != 0 && !primary_token.is_null() {
                                apply_current_user_owner_and_default_dacl(
                                    primary_token,
                                    user_sid.as_psid(),
                                );
                                return Some((primary_token as usize, logon_token as usize));
                            }
                        }
                        CloseHandle(logon_token);
                    }

                    let mut proc_token: HANDLE = std::ptr::null_mut();
                    if OpenProcessToken(
                        GetCurrentProcess(),
                        TOKEN_DUPLICATE | TOKEN_ASSIGN_PRIMARY | TOKEN_QUERY | TOKEN_ADJUST_DEFAULT,
                        &mut proc_token,
                    ) == 0
                        || proc_token.is_null()
                    {
                        return None;
                    }
                    let mut restricted_token: HANDLE = std::ptr::null_mut();
                    let ok = CreateRestrictedToken(
                        proc_token,
                        LUA_TOKEN,
                        0,
                        std::ptr::null(),
                        0,
                        std::ptr::null(),
                        0,
                        std::ptr::null(),
                        &mut restricted_token,
                    );
                    CloseHandle(proc_token);
                    if ok == 0 || restricted_token.is_null() {
                        return None;
                    }
                    apply_current_user_owner_and_default_dacl(restricted_token, user_sid.as_psid());

                    let mut primary_token: HANDLE = std::ptr::null_mut();
                    let dup_ok = DuplicateTokenEx(
                        restricted_token,
                        TOKEN_ASSIGN_PRIMARY
                            | TOKEN_DUPLICATE
                            | TOKEN_QUERY
                            | TOKEN_ADJUST_DEFAULT
                            | TOKEN_ADJUST_SESSIONID,
                        std::ptr::null(),
                        SecurityImpersonation,
                        TokenPrimary,
                        &mut primary_token,
                    );
                    CloseHandle(restricted_token);
                    if dup_ok == 0 || primary_token.is_null() {
                        return None;
                    }
                    apply_current_user_owner_and_default_dacl(primary_token, user_sid.as_psid());
                    Some((primary_token as usize, 0))
                }
            })
            .map(|(primary, logon)| (primary as HANDLE, logon as HANDLE))
    }

    fn get_or_init_worker_lua_token() -> Option<HANDLE> {
        get_or_init_worker_lua_tokens().map(|(primary, _)| primary)
    }

    /// When the worker process runs under an elevated Administrator token (such as `runneradmin`
    /// on GitHub Actions `windows-latest`, where Windows sets `TokenOwner = BUILTIN\Administrators`
    /// and `TokenDefaultDacl = D:(A;;GA;;;BA)(A;;GA;;;SY)` with no ACE for `TokenUser`), sets
    /// `TokenOwner = CurrentUser` and adds `CurrentUser` (never `BUILTIN\Users` or `Everyone`) to
    /// the elevated worker token's `TokenDefaultDacl`. Does nothing when the worker process is not
    /// elevated.
    fn ensure_process_token_default_dacl_grants_current_user() {
        static DONE: OnceLock<()> = OnceLock::new();
        DONE.get_or_init(|| unsafe {
            if !is_process_elevated() {
                return;
            }
            let Ok(user_sid) = OwnedSid::current_user() else {
                return;
            };
            let mut token: HANDLE = std::ptr::null_mut();
            if OpenProcessToken(
                GetCurrentProcess(),
                TOKEN_QUERY | TOKEN_ADJUST_DEFAULT,
                &mut token,
            ) != 0
                && !token.is_null()
            {
                apply_current_user_owner_and_default_dacl(token, user_sid.as_psid());
                CloseHandle(token);
            }
        });
    }

    fn grant_rx_non_inherited(
        path: &Path,
        sid: PSID,
        manifest_path: Option<&Path>,
    ) -> io::Result<()> {
        if !path.exists() || is_reparse_point(path) {
            return Ok(());
        }
        let rx_mask = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE | SYNCHRONIZE;
        let check_mask = FILE_READ_ATTRIBUTES | FILE_TRAVERSE | FILE_READ_DATA;
        let user_sid = OwnedSid::current_user()?;
        let has_sid = dacl_already_grants_sid(path, sid, check_mask);
        let has_user = dacl_already_grants_sid(path, user_sid.as_psid(), check_mask);
        if has_sid && has_user {
            return Ok(());
        }
        record_acl_grant(manifest_path, path)?;
        let mut entries = Vec::with_capacity(2);
        if !has_sid {
            entries.push(explicit_access(sid, rx_mask, GRANT_ACCESS, NO_INHERITANCE));
        }
        if !has_user {
            entries.push(explicit_access(
                user_sid.as_psid(),
                rx_mask,
                GRANT_ACCESS,
                NO_INHERITANCE,
            ));
        }
        merge_entries_on_path(path, &entries)
    }

    /// Ensures every existing directory and file under `root` grants `FILE_GENERIC_READ |
    /// FILE_GENERIC_EXECUTE | SYNCHRONIZE` (`check_mask = FILE_READ_ATTRIBUTES | FILE_TRAVERSE |
    /// FILE_READ_DATA`) to both `sid` and `user_sid`. When `SetNamedSecurityInfoW` on `root` has
    /// already propagated `(I)(RX)` to a child, `dacl_already_grants_sid` returns `true` and the
    /// child's DACL is left untouched (preserving N6 single-root-ACE inheritance). If any existing
    /// child did not inherit `(RX)` (e.g. due to a protected DACL or non-auto-inheriting parent),
    /// merges `(RX)` with `UNPROTECTED_DACL_SECURITY_INFORMATION`.
    fn ensure_descendants_grant_rx(root: &Path, sid: PSID, user_sid: PSID) -> io::Result<()> {
        let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
        let check_mask = FILE_READ_ATTRIBUTES | FILE_TRAVERSE | FILE_READ_DATA;
        let rx_mask = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE | SYNCHRONIZE;
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in rd.flatten() {
                let p = entry.path();
                if is_reparse_point(&p) {
                    continue;
                }
                let is_dir = entry.file_type().is_ok_and(|ft| ft.is_dir());
                let c_has_sid = dacl_already_grants_sid(&p, sid, check_mask);
                let c_has_user = dacl_already_grants_sid(&p, user_sid, check_mask);
                if !c_has_sid || !c_has_user {
                    let flags = if is_dir { inherit } else { NO_INHERITANCE };
                    let mut c_entries = Vec::with_capacity(2);
                    if !c_has_sid {
                        c_entries.push(explicit_access(sid, rx_mask, GRANT_ACCESS, flags));
                    }
                    if !c_has_user {
                        c_entries.push(explicit_access(user_sid, rx_mask, GRANT_ACCESS, flags));
                    }
                    let _ = merge_entries_on_path_unprotected(&p, &c_entries);
                }
                if is_dir {
                    stack.push(p);
                }
            }
        }
        Ok(())
    }

    /// Sets a single inheritable read+execute ACE (`OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE`)
    /// on `path` itself and lets Win32 `SetNamedSecurityInfoW` propagate `(I)(OI)(CI)(RX)` to
    /// descendants, then verifies via [`ensure_descendants_grant_rx`] that every existing
    /// file and subdirectory under `path` actually received `(RX)` (N6).
    fn grant_rx_recursive(path: &Path, sid: PSID, manifest_path: Option<&Path>) -> io::Result<()> {
        if !path.exists() || is_reparse_point(path) {
            return Ok(());
        }
        let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
        let check_mask = FILE_READ_ATTRIBUTES | FILE_TRAVERSE | FILE_READ_DATA;
        let user_sid = OwnedSid::current_user()?;
        let has_sid = dacl_already_grants_sid_with_flags(path, sid, check_mask, inherit);
        let has_user =
            dacl_already_grants_sid_with_flags(path, user_sid.as_psid(), check_mask, inherit);
        if !has_sid || !has_user {
            record_acl_grant(manifest_path, path)?;
            let rx_mask = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE | SYNCHRONIZE;
            let mut entries = Vec::with_capacity(2);
            if !has_sid {
                entries.push(explicit_access(sid, rx_mask, GRANT_ACCESS, inherit));
            }
            if !has_user {
                entries.push(explicit_access(
                    user_sid.as_psid(),
                    rx_mask,
                    GRANT_ACCESS,
                    inherit,
                ));
            }
            merge_entries_on_path_unprotected(path, &entries)?;
        }
        if path.is_dir() {
            ensure_descendants_grant_rx(path, sid, user_sid.as_psid())?;
        }
        Ok(())
    }

    /// Grants `oxideswarm.toolchain` read+execute on the existing subdirectories and config files
    /// of `cargo_home` (`bin`, `registry`, `git`, `vendor`, `config.toml`, `config`) while NEVER
    /// placing an inheritable allow ACE on `cargo_home` itself and NEVER granting access to
    /// `credentials` or `credentials.toml`.
    ///
    /// Why: if `cargo_home` had an inheritable allow ACE and `credentials.toml` were later
    /// replaced by atomic rename (`MoveFileExW`), the new file would inherit the allow ACE and
    /// lose any per-file deny ACE. With `NO_INHERITANCE` on `cargo_home`, any file created or
    /// renamed inside `cargo_home` inherits zero `oxideswarm.toolchain` permissions.
    fn prepare_one_cargo_dir(
        cargo: &Path,
        toolchain_sid: PSID,
        manifest_path: Option<&Path>,
    ) -> io::Result<()> {
        if !cargo.exists() || is_reparse_point(cargo) {
            return Ok(());
        }
        grant_rx_non_inherited(cargo, toolchain_sid, manifest_path)?;
        for file in [
            "config.toml",
            "config",
            ".package-cache",
            ".package-cache-lock",
        ] {
            let p = cargo.join(file);
            if p.exists() && !is_reparse_point(&p) {
                grant_rx_non_inherited(&p, toolchain_sid, manifest_path)?;
            }
        }
        for subdir in ["bin", "git", "vendor", "registry"] {
            let p = cargo.join(subdir);
            if p.exists() && !is_reparse_point(&p) {
                grant_rx_recursive(&p, toolchain_sid, manifest_path)?;
            }
        }
        for cred in ["credentials", "credentials.toml"] {
            let p = cargo.join(cred);
            if p.exists() && !is_reparse_point(&p) {
                if dacl_already_grants_sid(&p, toolchain_sid, FILE_READ_ATTRIBUTES) {
                    let _ = remove_sid_aces_from_path(&p, toolchain_sid);
                }
                if !dacl_already_denies_sid(&p, toolchain_sid, FILE_ALL_ACCESS) {
                    record_acl_grant(manifest_path, &p)?;
                    let deny = explicit_access(
                        toolchain_sid,
                        FILE_ALL_ACCESS,
                        DENY_ACCESS,
                        NO_INHERITANCE,
                    );
                    let _ = merge_entries_on_path(&p, &[deny]);
                }
            }
        }
        Ok(())
    }

    /// Validates that no `protected_files` entry (e.g. `--join-token-file`, `--admin-token-file`,
    /// TLS key, or worker config file):
    /// 1. Lies inside any directory that receives an `oxideswarm.toolchain` read+execute grant
    ///    (`rustup_home`, `cargo_home`, or MSVC/SDK) or inside a system directory (`C:\Windows`,
    ///    `C:\Program Files`, `C:\Program Files (x86)`) that AppContainers can read by default.
    /// 2. Carries an on-disk DACL granting read access to `oxideswarm.toolchain`, `S-1-15-2-1`
    ///    (`ALL APPLICATION PACKAGES`), `S-1-15-3-1` (`InternetClient`), or `S-1-15-3-3`
    ///    (`PrivateNetworkClientServer`) (N5).
    pub fn validate_protected_files_outside_toolchains(
        host: &HostDirs,
        protected_files: &[PathBuf],
    ) -> Result<(), String> {
        let mut forbidden_roots: Vec<(&str, PathBuf)> = Vec::new();
        if !host.rustup_home.as_os_str().is_empty() {
            forbidden_roots.push(("RUSTUP_HOME", host.rustup_home.clone()));
        }
        if !host.cargo_home.as_os_str().is_empty() {
            forbidden_roots.push(("CARGO_HOME", host.cargo_home.clone()));
        }
        if let Some((_, _, grant_roots)) = discover_msvc_details() {
            for r in grant_roots {
                forbidden_roots.push(("MSVC/Windows SDK toolchain", r));
            }
        }
        for (label, env_var, fallback) in [
            ("Windows system", "SystemRoot", r"C:\Windows"),
            ("Program Files", "ProgramFiles", r"C:\Program Files"),
            (
                "Program Files (x86)",
                "ProgramFiles(x86)",
                r"C:\Program Files (x86)",
            ),
        ] {
            let root = std::env::var_os(env_var)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(fallback));
            forbidden_roots.push((label, root));
        }

        let check_sids: Vec<(&str, OwnedSid)> = [
            (
                "oxideswarm.toolchain",
                OwnedSid::oxideswarm_toolchain().ok(),
            ),
            (
                "S-1-15-2-1 (ALL APPLICATION PACKAGES)",
                OwnedSid::from_well_known(WinBuiltinAnyPackageSid).ok(),
            ),
            (
                "S-1-15-2-2 (ALL RESTRICTED APPLICATION PACKAGES)",
                OwnedSid::from_sid_string("S-1-15-2-2").ok(),
            ),
            (
                "S-1-15-3-1 (InternetClient)",
                OwnedSid::from_well_known(WinCapabilityInternetClientSid).ok(),
            ),
            (
                "S-1-15-3-3 (PrivateNetworkClientServer)",
                OwnedSid::from_well_known(WinCapabilityPrivateNetworkClientServerSid).ok(),
            ),
        ]
        .into_iter()
        .filter_map(|(name, sid_opt)| sid_opt.map(|s| (name, s)))
        .collect();

        for pf in protected_files {
            if pf.as_os_str().is_empty() {
                continue;
            }
            for (label, root) in &forbidden_roots {
                if path_starts_with_ci(pf, root) {
                    return Err(format!(
                        "refusing to start isolated worker: protected file '{}' lies insideAppContainer-readable {} directory '{}'; move the secret/config file outside toolchain and system directories",
                        pf.display(),
                        label,
                        root.display()
                    ));
                }
            }
            if pf.exists() {
                for (sid_label, sid) in &check_sids {
                    if dacl_already_grants_sid(pf, sid.as_psid(), FILE_READ_DATA)
                        && !dacl_already_denies_sid(pf, sid.as_psid(), FILE_READ_DATA)
                    {
                        return Err(format!(
                            "refusing to start isolated worker: protected file '{}' has a DACL granting read access to {sid_label}",
                            pf.display()
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validates that `sandbox_base` is a dedicated sandbox directory and NOT a drive root,
    /// system directory, user home, toolchain directory, or ancestor thereof before its DACL is
    /// replaced with a protected DACL (N4).
    pub fn validate_sandbox_base_safe(sandbox_base: &Path, host: &HostDirs) -> Result<(), String> {
        let norm = normalize_win_path(sandbox_base);
        if norm.parent().is_none()
            || matches!(
                norm.components().next_back(),
                Some(Component::RootDir | Component::Prefix(_))
            )
        {
            return Err(format!(
                "refusing to use drive root '{}' as sandbox_base; point --sandbox-base-dir at a dedicated subdirectory",
                sandbox_base.display()
            ));
        }
        for (label, dir) in [
            ("USERPROFILE", &host.user_home),
            ("RUSTUP_HOME", &host.rustup_home),
            ("CARGO_HOME", &host.cargo_home),
        ] {
            if !dir.as_os_str().is_empty() && path_starts_with_ci(dir, sandbox_base) {
                return Err(format!(
                    "refusing to rewrite the DACL of '{}' as sandbox_base because {} ('{}') is equal to or inside it; point --sandbox-base-dir at a dedicated OxideSwarm sandbox directory",
                    sandbox_base.display(),
                    label,
                    dir.display()
                ));
            }
        }
        let temp_root = normalize_win_path(&std::env::temp_dir());
        if norm.as_os_str().eq_ignore_ascii_case(temp_root.as_os_str()) {
            return Err(format!(
                "refusing to rewrite the DACL of the shared system temp directory '{}' as sandbox_base; use a subdirectory such as '{}\\rusty_grid\\sandboxes'",
                sandbox_base.display(),
                sandbox_base.display()
            ));
        }
        for env_var in [
            "SystemRoot",
            "ProgramFiles",
            "ProgramFiles(x86)",
            "ProgramData",
            "PUBLIC",
            "LOCALAPPDATA",
            "APPDATA",
        ] {
            if let Some(sys_dir) = std::env::var_os(env_var).map(PathBuf::from) {
                if !sys_dir.as_os_str().is_empty() && path_starts_with_ci(&sys_dir, sandbox_base) {
                    return Err(format!(
                        "refusing to rewrite the DACL of '{}' as sandbox_base because system directory %{env_var}% ('{}') is equal to or inside it",
                        sandbox_base.display(),
                        sys_dir.display()
                    ));
                }
            }
        }
        // Refuse to rewrite the DACL of a folder inside the user's home directory (such as
        // `C:\Users\me\Documents` or `C:\Users\me\Desktop`) unless it is inside OxideSwarm's own
        // `<user_home>\.oxideswarm\sandboxes` directory.
        if !host.user_home.as_os_str().is_empty()
            && path_starts_with_ci(sandbox_base, &host.user_home)
        {
            let allowed_ox = host.user_home.join(".oxideswarm").join("sandboxes");
            if !path_starts_with_ci(sandbox_base, &allowed_ox) {
                return Err(format!(
                    "refusing to rewrite the DACL of user folder '{}' as sandbox_base; point --sandbox-base-dir at a dedicated OxideSwarm subdirectory such as '{}'",
                    sandbox_base.display(),
                    allowed_ox.display()
                ));
            }
        }
        if let Some(env_up) = std::env::var_os("USERPROFILE").map(PathBuf::from) {
            if !env_up.as_os_str().is_empty() && path_starts_with_ci(sandbox_base, &env_up) {
                let allowed_ox = env_up.join(".oxideswarm").join("sandboxes");
                let in_ox_sandboxes = path_starts_with_ci(sandbox_base, &allowed_ox);
                let in_temp = path_starts_with_ci(sandbox_base, &temp_root);
                if !in_ox_sandboxes && !in_temp {
                    return Err(format!(
                        "refusing to rewrite the DACL of user folder '{}' as sandbox_base; point --sandbox-base-dir at a dedicated OxideSwarm subdirectory such as '{}'",
                        sandbox_base.display(),
                        allowed_ox.display()
                    ));
                }
            }
        }
        // If `sandbox_base` already exists on disk, refuse to rewrite its DACL if it contains any
        // entry not created by OxideSwarm (`cache`, `.locks`, `other_task`, `.isolation-probe-*`,
        // `.isolation-outside-*`, `.write_probe_*`, or a UUID task sandbox directory).
        if sandbox_base.exists() {
            if let Ok(entries) = std::fs::read_dir(sandbox_base) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let s = name.to_string_lossy();
                    let is_oxideswarm_entry = s == ".locks"
                        || s == "cache"
                        || s == "other_task"
                        || s.starts_with(".isolation-probe-")
                        || s.starts_with(".isolation-outside-")
                        || s.starts_with(".write_probe_")
                        || uuid::Uuid::try_parse(&s).is_ok();
                    if !is_oxideswarm_entry {
                        return Err(format!(
                            "refusing to rewrite the DACL of existing folder '{}' containing non-sandbox entry '{}'; point --sandbox-base-dir at a dedicated OxideSwarm subdirectory",
                            sandbox_base.display(),
                            s
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validates that `ws_root` is a dedicated OxideSwarm workspace root directory and not a
    /// drive root, home directory, toolchain directory, shared system `%TEMP%` root, or arbitrary
    /// user directory.
    pub fn validate_workspace_root_safe(ws_root: &Path, host: &HostDirs) -> Result<(), String> {
        let norm = normalize_win_path(ws_root);
        if norm.parent().is_none()
            || matches!(
                norm.components().next_back(),
                Some(Component::RootDir | Component::Prefix(_))
            )
        {
            return Err(format!(
                "refusing to use drive root '{}' as workspace_root; point --workspace-root at a dedicated subdirectory",
                ws_root.display()
            ));
        }
        for (label, dir) in [
            ("USERPROFILE", &host.user_home),
            ("RUSTUP_HOME", &host.rustup_home),
            ("CARGO_HOME", &host.cargo_home),
        ] {
            if !dir.as_os_str().is_empty() && path_starts_with_ci(dir, ws_root) {
                return Err(format!(
                    "refusing to rewrite the DACL of '{}' as workspace_root because {} ('{}') is equal to or inside it; point --workspace-root at a dedicated OxideSwarm workspace directory",
                    ws_root.display(),
                    label,
                    dir.display()
                ));
            }
        }
        let temp_root = normalize_win_path(&std::env::temp_dir());
        if norm.as_os_str().eq_ignore_ascii_case(temp_root.as_os_str()) {
            return Err(format!(
                "refusing to rewrite the DACL of the shared system temp directory '{}' as workspace_root; use a subdirectory",
                ws_root.display()
            ));
        }
        for env_var in [
            "SystemRoot",
            "ProgramFiles",
            "ProgramFiles(x86)",
            "ProgramData",
            "PUBLIC",
            "LOCALAPPDATA",
            "APPDATA",
        ] {
            if let Some(sys_dir) = std::env::var_os(env_var).map(PathBuf::from) {
                if !sys_dir.as_os_str().is_empty() && path_starts_with_ci(&sys_dir, ws_root) {
                    return Err(format!(
                        "refusing to rewrite the DACL of '{}' as workspace_root because system directory %{env_var}% ('{}') is equal to or inside it",
                        ws_root.display(),
                        sys_dir.display()
                    ));
                }
            }
        }
        let is_allowed_under_home = |home: &Path| -> bool {
            let allowed1 = home.join(".oxideswarm").join("workspaces");
            let allowed2 = home.join(".cache").join("oxideswarm").join("workspaces");
            let allowed3 = home
                .join("AppData")
                .join("Local")
                .join("oxideswarm")
                .join("workspaces");
            path_starts_with_ci(ws_root, &allowed1)
                || path_starts_with_ci(ws_root, &allowed2)
                || path_starts_with_ci(ws_root, &allowed3)
                || path_starts_with_ci(ws_root, &temp_root)
        };
        if !host.user_home.as_os_str().is_empty()
            && path_starts_with_ci(ws_root, &host.user_home)
            && !is_allowed_under_home(&host.user_home)
        {
            return Err(format!(
                "refusing to rewrite the DACL of user folder '{}' as workspace_root; point --workspace-root at a dedicated OxideSwarm workspace subdirectory",
                ws_root.display()
            ));
        }
        if let Some(env_up) = std::env::var_os("USERPROFILE").map(PathBuf::from) {
            if !env_up.as_os_str().is_empty()
                && path_starts_with_ci(ws_root, &env_up)
                && !is_allowed_under_home(&env_up)
            {
                return Err(format!(
                    "refusing to rewrite the DACL of user folder '{}' as workspace_root; point --workspace-root at a dedicated OxideSwarm workspace subdirectory",
                    ws_root.display()
                ));
            }
        }
        if ws_root.exists() {
            if let Ok(entries) = std::fs::read_dir(ws_root) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let s = name.to_string_lossy();
                    let is_ws_entry = s == "store.lock"
                        || s == ".locks"
                        || rusty_grid_core::task::validate_workspace_name(&s).is_ok();
                    if !is_ws_entry {
                        return Err(format!(
                            "refusing to rewrite the DACL of existing folder '{}' containing non-workspace entry '{}'; point --workspace-root at a dedicated OxideSwarm workspace subdirectory",
                            ws_root.display(),
                            s
                        ));
                    }
                    let entry_path = entry.path();
                    if s != "store.lock" && s != ".locks" && entry_path.is_dir() {
                        if let Ok(sub_entries) = std::fs::read_dir(&entry_path) {
                            for sub in sub_entries.flatten() {
                                let sub_name = sub.file_name();
                                let sub_s = sub_name.to_string_lossy();
                                let valid_sub = matches!(
                                    sub_s.as_ref(),
                                    "tree" | "tmp" | "state.json" | "state.json.tmp" | "lock"
                                );
                                if !valid_sub {
                                    return Err(format!(
                                        "refusing to rewrite the DACL of existing folder '{}' because subdirectory '{}' contains non-workspace entry '{}'",
                                        ws_root.display(),
                                        s,
                                        sub_s
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Protects `sandbox_base` so tasks holding `oxideswarm.toolchain` can traverse `sandbox_base`
    /// (`FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE`, `NO_INHERITANCE`) without being able
    /// to list `sandbox_base` (`FILE_LIST_DIRECTORY` omitted) or read sibling sandboxes.
    /// Records the original DACL SDDL of `sandbox_base` in `manifest_path` before modifying it so
    /// `revoke_oxideswarm_toolchain_acls` and `windows/revoke-isolation-acls.ps1` restore the
    /// exact original DACL and inheritance state (N4).
    fn protect_sandbox_base(
        sandbox_base: &Path,
        toolchain_sid: PSID,
        manifest_path: Option<&Path>,
    ) -> io::Result<()> {
        if !sandbox_base.exists() || is_reparse_point(sandbox_base) {
            return Ok(());
        }
        let traverse_mask = FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
        if dacl_already_grants_sid(sandbox_base, toolchain_sid, traverse_mask)
            && !dacl_already_grants_sid(sandbox_base, toolchain_sid, FILE_LIST_DIRECTORY)
        {
            return Ok(());
        }
        if let Ok(orig_sddl) = get_path_dacl_sddl(sandbox_base) {
            record_sddl_restore(manifest_path, sandbox_base, &orig_sddl)?;
        }
        record_acl_grant(manifest_path, sandbox_base)?;
        let user_sid = OwnedSid::current_user()?;
        let system_sid = OwnedSid::from_well_known(WinLocalSystemSid)?;
        let admins_sid = OwnedSid::from_well_known(WinBuiltinAdministratorsSid)?;
        let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
        let base_entries = [
            explicit_access(user_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
            explicit_access(system_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
            explicit_access(admins_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
            explicit_access(toolchain_sid, traverse_mask, SET_ACCESS, NO_INHERITANCE),
        ];
        set_protected_dacl_on_path(sandbox_base, &base_entries)
    }

    /// Prepares the worker's own toolchain directories (`host.rustup_home` and `host.cargo_home`)
    /// for the `oxideswarm.toolchain` capability SID, and configures `sandbox_base` with
    /// non-inherited traverse-only access.
    ///
    /// Security invariants enforced here:
    /// - Grants `oxideswarm.toolchain` (`S-1-15-3-...`), NEVER `S-1-15-2-1` (`ALL APPLICATION PACKAGES`).
    /// - Never modifies `<user_home>` (`C:\Users\<user>`) or any secret directory under it.
    /// - Never reads task env (`CARGO_HOME`, `RUSTUP_HOME`) or task `Cargo.lock`.
    /// - Refuses if any `protected_files` path lies inside a granted toolchain root or is readable
    ///   by AppContainer SIDs.
    /// - Sets a single inheritable ACE on each root (`<rustup_home>/toolchains`,
    ///   `<cargo_home>/{bin,git,vendor,registry}`) without walking descendants (N6).
    /// - Checks the actual on-disk root DACL (`dacl_already_grants_sid`) in $O(1)$ before writing
    ///   any ACE, with no stale in-memory cache.
    /// - Logs every modified path (plus `SDDL|<sandbox_base>|<orig_sddl>`) to
    ///   `acl_grant_manifest_path(host)` before modifying its DACL.
    pub fn prepare_worker_toolchains(
        host: &HostDirs,
        protected_files: &[PathBuf],
        sandbox_base: Option<&Path>,
    ) -> Result<(), String> {
        ensure_process_token_default_dacl_grants_current_user();
        validate_protected_files_outside_toolchains(host, protected_files)?;
        if let Some(base) = sandbox_base {
            validate_sandbox_base_safe(base, host)?;
        }

        let _guard = PREPARE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let toolchain_sid = OwnedSid::oxideswarm_toolchain().map_err(|e| {
            format!("DeriveCapabilitySidsFromName(oxideswarm.toolchain) failed: {e}")
        })?;

        let manifest = acl_grant_manifest_path(host);
        let manifest_opt = if host.user_home.as_os_str().is_empty()
            && std::env::var_os("RUSTY_GRID_ACL_GRANT_LOG").is_none()
        {
            None
        } else {
            Some(manifest.as_path())
        };

        if let Some(base) = sandbox_base {
            if base.exists() {
                protect_sandbox_base(base, toolchain_sid.as_psid(), manifest_opt).map_err(|e| {
                    format!(
                        "failed to configure sandbox_base DACL on {}: {e}",
                        base.display()
                    )
                })?;
            }
        }

        // Never touch `host.user_home` itself. Windows `SeChangeNotifyPrivilege` (Bypass Traverse
        // Checking, held by AppContainers) allows opening `<user_home>\.rustup` or
        // `<user_home>\.cargo` directly by full path without any ACE on `<user_home>`.
        // If a secret file under `<user_home>\.oxideswarm` was atomically replaced via same-volume
        // `MoveFileExW` (`std::fs::rename`) from a staging location that carried an
        // `oxideswarm.toolchain` allow ACE, strip that allow ACE so the secret stays unreadable.
        if !host.user_home.as_os_str().is_empty() {
            for rel in [".oxideswarm/cluster.secret", ".oxideswarm/p2p_secret.key"] {
                let sec = host.user_home.join(rel);
                if sec.exists()
                    && !is_reparse_point(&sec)
                    && dacl_already_grants_sid(&sec, toolchain_sid.as_psid(), FILE_READ_ATTRIBUTES)
                {
                    let _ = remove_sid_aces_from_path(&sec, toolchain_sid.as_psid());
                }
            }
        }
        if !host.rustup_home.as_os_str().is_empty()
            && host.rustup_home.exists()
            && (host.user_home.as_os_str().is_empty()
                || !path_starts_with_ci(&host.user_home, &host.rustup_home))
        {
            let rustup = &host.rustup_home;
            grant_rx_non_inherited(rustup, toolchain_sid.as_psid(), manifest_opt)
                .map_err(|e| format!("failed to prepare RUSTUP_HOME {}: {e}", rustup.display()))?;
            let settings = rustup.join("settings.toml");
            if settings.exists() {
                let _ = grant_rx_non_inherited(&settings, toolchain_sid.as_psid(), manifest_opt);
            }
            // Grant a single inheritable `(OI)(CI)(RX)` ACE on `<rustup_home>/toolchains` itself
            // so both existing toolchains and any toolchain installed later by `rustup update`
            // inherit read+execute automatically without walking (N6).
            let tc_root = rustup.join("toolchains");
            if tc_root.exists() {
                grant_rx_recursive(&tc_root, toolchain_sid.as_psid(), manifest_opt).map_err(
                    |e| {
                        format!(
                            "failed to prepare RUSTUP_HOME toolchains {}: {e}",
                            tc_root.display()
                        )
                    },
                )?;
            }
        }

        if !host.cargo_home.as_os_str().is_empty()
            && host.cargo_home.exists()
            && (host.user_home.as_os_str().is_empty()
                || !path_starts_with_ci(&host.user_home, &host.cargo_home))
        {
            prepare_one_cargo_dir(&host.cargo_home, toolchain_sid.as_psid(), manifest_opt)
                .map_err(|e| {
                    format!(
                        "failed to prepare CARGO_HOME {}: {e}",
                        host.cargo_home.display()
                    )
                })?;
        }

        Ok(())
    }

    /// Removes all explicit ACEs (both allow and deny) for `oxideswarm.toolchain` from `path`
    /// and any descendants that still carry an explicit `oxideswarm.toolchain` ACE, skipping
    /// reparse points (N4, N6).
    pub fn revoke_toolchain_sid_on_tree(path: &Path, toolchain_sid: PSID) -> io::Result<()> {
        if !path.exists() || is_reparse_point(path) {
            return Ok(());
        }
        remove_sid_aces_from_path(path, toolchain_sid)?;
        if path.is_dir() {
            let mut stack = vec![path.to_path_buf()];
            while let Some(dir) = stack.pop() {
                let Ok(rd) = std::fs::read_dir(&dir) else {
                    continue;
                };
                for entry in rd.flatten() {
                    let p = entry.path();
                    if is_reparse_point(&p) {
                        continue;
                    }
                    if dacl_already_grants_sid(&p, toolchain_sid, FILE_READ_ATTRIBUTES)
                        || dacl_already_denies_sid(&p, toolchain_sid, FILE_READ_ATTRIBUTES)
                    {
                        let _ = remove_sid_aces_from_path(&p, toolchain_sid);
                    }
                    if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                        stack.push(p);
                    }
                }
            }
        }
        Ok(())
    }

    /// Reads the ACL grant manifest at `manifest_path`, revokes `oxideswarm.toolchain` allow and
    /// deny ACEs from every recorded path, and restores any `SDDL|<path>|<orig_sddl>` entries
    /// (such as `sandbox_base`). Returns the list of paths processed.
    pub fn revoke_oxideswarm_toolchain_acls(manifest_path: &Path) -> io::Result<Vec<PathBuf>> {
        if !manifest_path.exists() {
            return Ok(Vec::new());
        }
        let toolchain_sid = OwnedSid::oxideswarm_toolchain()?;
        let content = std::fs::read_to_string(manifest_path)?;
        let mut revoked = Vec::new();
        let mut sddl_restores: Vec<(PathBuf, String)> = Vec::new();
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix("SDDL|") {
                if let Some((path_str, sddl_str)) = rest.split_once('|') {
                    if !path_str.is_empty() && !sddl_str.is_empty() {
                        sddl_restores.push((PathBuf::from(path_str), sddl_str.to_string()));
                    }
                }
                continue;
            }
            let p = PathBuf::from(trimmed);
            if p.exists() {
                revoke_toolchain_sid_on_tree(&p, toolchain_sid.as_psid())?;
                revoked.push(p);
            }
        }
        for (p, orig_sddl) in sddl_restores {
            if p.exists() {
                restore_path_dacl_sddl(&p, &orig_sddl)?;
                if !revoked.contains(&p) {
                    revoked.push(p);
                }
            }
        }
        Ok(revoked)
    }

    struct WorkerPrefixLock {
        _lock_file: std::fs::File,
        active_guards: usize,
    }

    static WORKER_PREFIX_LOCKS: OnceLock<Mutex<HashMap<String, WorkerPrefixLock>>> =
        OnceLock::new();

    fn acquire_worker_prefix_lock(prefix: &str, increment_guard: bool) -> bool {
        let map_mutex = WORKER_PREFIX_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut map = map_mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(state) = map.get_mut(prefix) {
            if increment_guard {
                state.active_guards += 1;
            }
            return state.active_guards == usize::from(increment_guard);
        }
        let lock_path = std::env::temp_dir().join(format!("{prefix}.lock"));
        let Ok(file) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .share_mode(0)
            .open(&lock_path)
        else {
            return false;
        };
        map.insert(
            prefix.to_string(),
            WorkerPrefixLock {
                _lock_file: file,
                active_guards: usize::from(increment_guard),
            },
        );
        true
    }

    fn release_worker_prefix_guard(prefix: &str) {
        if let Some(map_mutex) = WORKER_PREFIX_LOCKS.get() {
            let mut map = map_mutex
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(state) = map.get_mut(prefix) {
                state.active_guards = state.active_guards.saturating_sub(1);
            }
        }
    }

    /// Sweeps leftover `oxideswarm.task.<worker8>.*` AppContainer profiles belonging ONLY to the
    /// worker instance for `sandbox_base`, holding an exclusive OS file lock
    /// (`%TEMP%\oxideswarm.task.<worker8>.lock`, `share_mode(0)`) so two workers with the same
    /// `sandbox_base` or parallel tests never delete each other's live profiles (N3).
    pub fn sweep_leftover_appcontainer_profiles_for_base(sandbox_base: &Path) -> usize {
        let prefix = worker_profile_prefix(sandbox_base);
        if !acquire_worker_prefix_lock(&prefix, false) {
            return 0;
        }
        let match_prefix = format!("{prefix}.");
        let mut deleted = 0usize;
        let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") else {
            return 0;
        };
        let packages_dir = PathBuf::from(local_app_data).join("Packages");
        let Ok(entries) = std::fs::read_dir(&packages_dir) else {
            return 0;
        };
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            let Some(prof_name) = name
                .split('_')
                .next()
                .filter(|s| s.starts_with(&match_prefix))
            else {
                continue;
            };
            let wide = to_wide(OsStr::new(prof_name));
            let hr = unsafe { DeleteAppContainerProfile(wide.as_ptr()) };
            if hr >= 0 {
                deleted += 1;
            }
        }
        deleted
    }

    /// Sweeps leftover AppContainer profiles for the default sandbox base directory.
    pub fn sweep_leftover_appcontainer_profiles() -> usize {
        let default_base = crate::sandbox::SandboxConfig::default().base_dir;
        sweep_leftover_appcontainer_profiles_for_base(&default_base)
    }

    /// Per-task Windows AppContainer state that must outlive `CreateProcessW` and deletes the
    /// AppContainer profile and closes the per-task private Desktop when the task's `ProcessTree`
    /// is dropped.
    pub struct WindowsAppContainerGuard {
        worker_prefix: String,
        wide_profile_name: Vec<u16>,
        app_container_sid: PSID,
        desktop_handle: HANDLE,
        desktop_name_wide: Vec<u16>,
        _capability_sids: Vec<OwnedSid>,
        _capability_attrs: Vec<SID_AND_ATTRIBUTES>,
        security_capabilities: Box<SECURITY_CAPABILITIES>,
        workspace_dirs_to_restore: Vec<PathBuf>,
        cleaned_up: bool,
    }

    // SAFETY: WindowsAppContainerGuard owns its heap allocations and raw pointers are only read
    // synchronously during `CreateProcessW` before `spawn()` returns.
    unsafe impl Send for WindowsAppContainerGuard {}
    unsafe impl Sync for WindowsAppContainerGuard {}

    impl WindowsAppContainerGuard {
        pub(crate) fn create(
            run_token: uuid::Uuid,
            jail: &Jail<'_>,
            cmd: &tokio::process::Command,
        ) -> io::Result<Self> {
            let std_cmd = cmd.as_std();
            let program = std_cmd.get_program().to_string_lossy().to_string();
            let mut cmd_cargo_home: Option<PathBuf> = None;
            let mut cmd_rustup_home: Option<PathBuf> = None;
            for (k, v) in std_cmd.get_envs() {
                if k.eq_ignore_ascii_case("CARGO_HOME") {
                    cmd_cargo_home = v.map(PathBuf::from);
                } else if k.eq_ignore_ascii_case("RUSTUP_HOME") {
                    cmd_rustup_home = v.map(PathBuf::from);
                }
            }
            Self::create_blocking(
                run_token,
                jail,
                &program,
                cmd_cargo_home.as_deref(),
                cmd_rustup_home.as_deref(),
            )
        }

        /// Blocking per-task AppContainer profile + sandbox DACL + private desktop setup (invoked
        /// inside `tokio::task::spawn_blocking` by `TaskRunner::execute_process_supervised`).
        ///
        /// Security invariants:
        /// - Never grants `S-1-15-2-1` (`ALL APPLICATION PACKAGES`) on any path.
        /// - Never modifies any directory named by the task's `CARGO_HOME`, `RUSTUP_HOME`,
        ///   `program`, or `Cargo.lock`.
        /// - Never modifies `<user_home>` or any directory outside `<sandbox_base>` and
        ///   `<sandbox_dir>` (or `<workspace_root>/<ws_name>/{tree,tmp}`).
        /// - Refuses if `<sandbox_dir>`, `<sandbox_base>`, `<extra_writable_dir>`, or
        ///   `<workspace_root>` is a symlink/junction.
        /// - Creates a per-worker, per-task AppContainer profile
        ///   `oxideswarm.task.<worker8>.<run_token_simple>` (N3).
        /// - Creates a per-task private Desktop granting `DESKTOP_TASK_MASK` only to `CurrentUser`
        ///   and this task's `app_container_sid`; fails the task if desktop creation fails (N7).
        pub(crate) fn create_blocking(
            run_token: uuid::Uuid,
            jail: &Jail<'_>,
            program: &str,
            cmd_cargo_home: Option<&Path>,
            cmd_rustup_home: Option<&Path>,
        ) -> io::Result<Self> {
            ensure_process_token_default_dacl_grants_current_user();
            validate_protected_files_outside_toolchains(jail.host, jail.protected_files)
                .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;
            validate_sandbox_base_safe(jail.sandbox_base, jail.host)
                .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;

            if is_reparse_point(jail.sandbox_dir) || is_reparse_point(jail.sandbox_base) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "refusing to configure AppContainer on a reparse-point sandbox path",
                ));
            }
            if let Some(extra) = jail.extra_writable_dir {
                if is_reparse_point(extra) {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "refusing to configure AppContainer on a reparse-point workspace tmp path",
                    ));
                }
            }
            if let Some(ws_root) = jail.workspace_root {
                validate_workspace_root_safe(ws_root, jail.host)
                    .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;
                if is_reparse_point(ws_root) {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "refusing to configure AppContainer on a reparse-point workspace root path",
                    ));
                }
            }

            let in_task_writable = |p: &Path| -> bool {
                path_starts_with_ci(p, jail.sandbox_dir)
                    || jail
                        .extra_writable_dir
                        .is_some_and(|extra| path_starts_with_ci(p, extra))
            };

            // Requirement 2: A task's `CARGO_HOME` or `RUSTUP_HOME` may point only to the worker's
            // own configured toolchain directory or a directory inside the task's sandbox.
            if let Some(ch) = cmd_cargo_home {
                if !ch.as_os_str().is_empty()
                    && !in_task_writable(ch)
                    && (jail.host.cargo_home.as_os_str().is_empty()
                        || normalize_win_path(ch) != normalize_win_path(&jail.host.cargo_home))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "task CARGO_HOME '{}' must point inside the task sandbox '{}' or match the worker's CARGO_HOME",
                            ch.display(),
                            jail.sandbox_dir.display()
                        ),
                    ));
                }
            }
            if let Some(rh) = cmd_rustup_home {
                if !rh.as_os_str().is_empty()
                    && !in_task_writable(rh)
                    && (jail.host.rustup_home.as_os_str().is_empty()
                        || normalize_win_path(rh) != normalize_win_path(&jail.host.rustup_home))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "task RUSTUP_HOME '{}' must point inside the task sandbox '{}' or match the worker's RUSTUP_HOME",
                            rh.display(),
                            jail.sandbox_dir.display()
                        ),
                    ));
                }
            }

            let toolchain_sid = OwnedSid::oxideswarm_toolchain()?;

            // Requirement 4: No grants for program paths. A program must resolve inside the
            // sandbox, the worker's granted toolchain dirs, or system directories that
            // AppContainers can already read (`C:\Windows`, `C:\Program Files`, etc.).
            let prog_path = Path::new(program);
            if prog_path.is_absolute() {
                let in_sandbox = in_task_writable(prog_path);
                let in_cargo = !jail.host.cargo_home.as_os_str().is_empty()
                    && path_starts_with_ci(prog_path, &jail.host.cargo_home);
                let in_rustup = !jail.host.rustup_home.as_os_str().is_empty()
                    && path_starts_with_ci(prog_path, &jail.host.rustup_home);
                let in_msvc = discover_msvc_details().is_some_and(|(_, _, roots)| {
                    roots.iter().any(|r| path_starts_with_ci(prog_path, r))
                });
                let sys_root = std::env::var_os("SystemRoot")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from(r"C:\Windows"));
                let prog_files = std::env::var_os("ProgramFiles")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from(r"C:\Program Files"));
                let prog_files_x86 = std::env::var_os("ProgramFiles(x86)")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from(r"C:\Program Files (x86)"));
                let in_system = path_starts_with_ci(prog_path, &sys_root)
                    || path_starts_with_ci(prog_path, &prog_files)
                    || path_starts_with_ci(prog_path, &prog_files_x86);
                let already_granted = dacl_already_grants_sid(
                    prog_path,
                    toolchain_sid.as_psid(),
                    FILE_READ_ATTRIBUTES,
                );
                if !(in_sandbox || in_cargo || in_rustup || in_msvc || in_system || already_granted)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "program '{}' lies outside the task sandbox, worker toolchain directories, and system directories readable by AppContainers",
                            prog_path.display()
                        ),
                    ));
                }
            }

            let worker_prefix = worker_profile_prefix(jail.sandbox_base);
            let _ = acquire_worker_prefix_lock(&worker_prefix, true);
            let profile_name = format!("{}.{}", worker_prefix, run_token.simple());
            let wide_profile_name = to_wide(OsStr::new(&profile_name));
            let wide_display = to_wide(OsStr::new("OxideSwarm Task Sandbox"));

            let mut app_container_sid: PSID = std::ptr::null_mut();
            let hr = unsafe {
                CreateAppContainerProfile(
                    wide_profile_name.as_ptr(),
                    wide_display.as_ptr(),
                    wide_display.as_ptr(),
                    std::ptr::null(),
                    0,
                    &mut app_container_sid,
                )
            };
            let hr = if hr == HRESULT_ALREADY_EXISTS {
                unsafe {
                    DeriveAppContainerSidFromAppContainerName(
                        wide_profile_name.as_ptr(),
                        &mut app_container_sid,
                    )
                }
            } else {
                hr
            };
            if hr < 0 || app_container_sid.is_null() {
                release_worker_prefix_guard(&worker_prefix);
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("CreateAppContainerProfile({profile_name}) failed: HRESULT 0x{hr:08x}"),
                ));
            }

            type SetupOut = (HANDLE, Vec<u16>, Vec<OwnedSid>, Vec<PathBuf>);
            let setup_res = (|| -> io::Result<SetupOut> {
                let user_sid = OwnedSid::current_user()?;
                let system_sid = OwnedSid::from_well_known(WinLocalSystemSid)?;
                let admins_sid = OwnedSid::from_well_known(WinBuiltinAdministratorsSid)?;

                let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
                let owner_only_entries = [
                    explicit_access(user_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                    explicit_access(system_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                    explicit_access(admins_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                ];

                // On `windows-latest`, `<LOCALAPPDATA>\Packages` has DACL
                // `D:(A;OICIID;FA;;;SY)(A;OICIID;FA;;;BA)(A;OICIID;FA;;;LA)` without `user_sid`.
                // Ensure `<LOCALAPPDATA>\Packages\<profile_name>` and its `AC` / `AC\Temp`
                // subdirectories (which `CreateAppContainerProfile` marks with a protected DACL
                // and `BasepAppContainerEnvironmentExtension` uses inside the AppContainer) grant
                // `user_sid` and this task's `app_container_sid` (never `BUILTIN\Users` or
                // `Everyone`) so Stage 1 and Stage 2 of `SepAccessCheck` both succeed.
                if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA").map(PathBuf::from) {
                    let pkg_dir = local_app_data.join("Packages").join(&profile_name);
                    if pkg_dir.exists() {
                        let pkg_entries = [
                            explicit_access(
                                user_sid.as_psid(),
                                FILE_ALL_ACCESS,
                                GRANT_ACCESS,
                                inherit,
                            ),
                            explicit_access(
                                app_container_sid,
                                FILE_ALL_ACCESS,
                                GRANT_ACCESS,
                                inherit,
                            ),
                        ];
                        let _ = merge_entries_on_path(&pkg_dir, &pkg_entries);
                        for sub in [pkg_dir.join("AC"), pkg_dir.join("AC").join("Temp")] {
                            if sub.exists() {
                                let _ = merge_entries_on_path_unprotected(&sub, &pkg_entries);
                            }
                        }
                    }
                }

                {
                    let _guard = PREPARE_LOCK
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let manifest = acl_grant_manifest_path(jail.host);
                    let manifest_opt = if jail.host.user_home.as_os_str().is_empty()
                        && std::env::var_os("RUSTY_GRID_ACL_GRANT_LOG").is_none()
                    {
                        None
                    } else {
                        Some(manifest.as_path())
                    };
                    protect_sandbox_base(jail.sandbox_base, toolchain_sid.as_psid(), manifest_opt)?;
                    if let Some(ws_root) = jail.workspace_root {
                        protect_sandbox_base(ws_root, toolchain_sid.as_psid(), manifest_opt)?;
                    }
                }

                let mut workspace_dirs_to_restore = Vec::new();
                if jail.workspace_root.is_some() {
                    let traverse_mask = FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
                    if let Some(ws_dir) = jail.sandbox_dir.parent() {
                        if ws_dir.exists() && !is_reparse_point(ws_dir) {
                            let ws_parent_entries = [
                                owner_only_entries[0],
                                owner_only_entries[1],
                                owner_only_entries[2],
                                explicit_access(
                                    toolchain_sid.as_psid(),
                                    traverse_mask,
                                    SET_ACCESS,
                                    NO_INHERITANCE,
                                ),
                            ];
                            set_protected_dacl_on_path(ws_dir, &ws_parent_entries)?;
                            for meta_name in ["state.json", "lock"] {
                                let meta_path = ws_dir.join(meta_name);
                                if meta_path.exists() && !is_reparse_point(&meta_path) {
                                    let _ =
                                        set_protected_dacl_on_path(&meta_path, &owner_only_entries);
                                }
                            }
                        }
                    }
                    const DELETE_ACCESS: u32 = 0x0001_0000;
                    let task_child_mask = FILE_ALL_ACCESS & !(WRITE_DAC | WRITE_OWNER);
                    let task_root_mask =
                        FILE_ALL_ACCESS & !(WRITE_DAC | WRITE_OWNER | DELETE_ACCESS);
                    let ws_dir_entries = [
                        owner_only_entries[0],
                        owner_only_entries[1],
                        owner_only_entries[2],
                        explicit_access(
                            app_container_sid,
                            task_child_mask,
                            SET_ACCESS,
                            OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE | INHERIT_ONLY_ACE,
                        ),
                        explicit_access(
                            app_container_sid,
                            task_root_mask,
                            GRANT_ACCESS,
                            NO_INHERITANCE,
                        ),
                    ];
                    set_protected_dacl_on_path(jail.sandbox_dir, &ws_dir_entries)?;
                    workspace_dirs_to_restore.push(jail.sandbox_dir.to_path_buf());
                    if let Some(extra) = jail.extra_writable_dir {
                        set_protected_dacl_on_path(extra, &ws_dir_entries)?;
                        workspace_dirs_to_restore.push(extra.to_path_buf());
                    }
                } else {
                    // Set a single inheritable protected DACL on `<sandbox_dir>` root granting access
                    // only to CurrentUser, SYSTEM, Administrators, and this task's specific
                    // `app_container_sid` (omitting `WRITE_DAC` and `WRITE_OWNER` so the task cannot
                    // alter its own sandbox ACL). Win32 `SetNamedSecurityInfoW` propagates `(I)` to
                    // staged children inside `<sandbox_dir>` automatically without walking (N6).
                    let task_sandbox_mask = FILE_ALL_ACCESS & !(WRITE_DAC | WRITE_OWNER);
                    let sandbox_entries = [
                        owner_only_entries[0],
                        owner_only_entries[1],
                        owner_only_entries[2],
                        explicit_access(app_container_sid, task_sandbox_mask, SET_ACCESS, inherit),
                    ];
                    set_protected_dacl_on_path(jail.sandbox_dir, &sandbox_entries)?;
                }

                // Create a dedicated per-task private Desktop on the worker's non-interactive
                // Window Station granting `DESKTOP_TASK_MASK` only to `CurrentUser` and this
                // task's `app_container_sid`. Fail the task if desktop creation fails (N7).
                let (desktop_handle, desktop_name_wide) =
                    create_per_task_desktop(app_container_sid, run_token)?;

                let mut capability_sids = vec![toolchain_sid];
                if jail.level == IsolationLevel::Fs {
                    let net_client = OwnedSid::from_well_known(WinCapabilityInternetClientSid)?;
                    let net_private =
                        OwnedSid::from_well_known(WinCapabilityPrivateNetworkClientServerSid)?;
                    capability_sids.push(net_client);
                    capability_sids.push(net_private);
                }
                Ok((
                    desktop_handle,
                    desktop_name_wide,
                    capability_sids,
                    workspace_dirs_to_restore,
                ))
            })();

            let (desktop_handle, desktop_name_wide, capability_sids, workspace_dirs_to_restore) =
                match setup_res {
                    Ok(v) => v,
                    Err(e) => {
                        unsafe {
                            FreeSid(app_container_sid);
                            let _ = DeleteAppContainerProfile(wide_profile_name.as_ptr());
                        }
                        release_worker_prefix_guard(&worker_prefix);
                        return Err(e);
                    }
                };

            let mut capability_attrs: Vec<SID_AND_ATTRIBUTES> = capability_sids
                .iter()
                .map(|sid| SID_AND_ATTRIBUTES {
                    Sid: sid.as_psid(),
                    Attributes: SE_GROUP_ENABLED as u32,
                })
                .collect();

            let security_capabilities = Box::new(SECURITY_CAPABILITIES {
                AppContainerSid: app_container_sid,
                Capabilities: if capability_attrs.is_empty() {
                    std::ptr::null_mut()
                } else {
                    capability_attrs.as_mut_ptr()
                },
                CapabilityCount: capability_attrs.len() as u32,
                Reserved: 0,
            });

            Ok(Self {
                worker_prefix,
                wide_profile_name,
                app_container_sid,
                desktop_handle,
                desktop_name_wide,
                _capability_sids: capability_sids,
                _capability_attrs: capability_attrs,
                security_capabilities,
                workspace_dirs_to_restore,
                cleaned_up: false,
            })
        }

        pub fn spawn_in_appcontainer(
            &self,
            cmd: &mut tokio::process::Command,
            creation_flags: u32,
        ) -> io::Result<AppContainerChild> {
            spawn_with_security_capabilities(
                cmd.as_std(),
                &self.security_capabilities,
                &self.desktop_name_wide,
                creation_flags,
            )
        }

        pub fn cleanup(&mut self) {
            if self.cleaned_up {
                return;
            }
            self.cleaned_up = true;
            if !self.workspace_dirs_to_restore.is_empty() {
                if let (Ok(user_sid), Ok(system_sid), Ok(admins_sid)) = (
                    OwnedSid::current_user(),
                    OwnedSid::from_well_known(WinLocalSystemSid),
                    OwnedSid::from_well_known(WinBuiltinAdministratorsSid),
                ) {
                    let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
                    let owner_only_entries = [
                        explicit_access(user_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                        explicit_access(system_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                        explicit_access(admins_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                    ];
                    for dir in self.workspace_dirs_to_restore.drain(..) {
                        if dir.exists() && !is_reparse_point(&dir) {
                            let _ = set_protected_dacl_on_path(&dir, &owner_only_entries);
                        }
                    }
                }
            }
            unsafe {
                if !self.desktop_handle.is_null() {
                    let _ = CloseDesktop(self.desktop_handle);
                    self.desktop_handle = std::ptr::null_mut();
                }
                if !self.app_container_sid.is_null() {
                    FreeSid(self.app_container_sid);
                    self.app_container_sid = std::ptr::null_mut();
                }
                let hr = DeleteAppContainerProfile(self.wide_profile_name.as_ptr());
                if hr < 0 {
                    let wide_name = std::mem::take(&mut self.wide_profile_name);
                    let prefix = std::mem::take(&mut self.worker_prefix);
                    std::thread::spawn(move || {
                        // Retry up to 50 × 50 ms (~2.5 s) matching Design 0033 D4.1 if kernel
                        // handles from a dying child process are still closing.
                        for _ in 0..50 {
                            std::thread::sleep(std::time::Duration::from_millis(50));
                            if DeleteAppContainerProfile(wide_name.as_ptr()) >= 0 {
                                break;
                            }
                        }
                        release_worker_prefix_guard(&prefix);
                    });
                } else {
                    release_worker_prefix_guard(&self.worker_prefix);
                }
            }
        }
    }

    impl Drop for WindowsAppContainerGuard {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    /// Start-up self-check on Windows with explicit worker [`HostDirs`] and `protected_files`:
    /// 1. Validates that no `protected_files` entry lies inside a granted toolchain or system
    ///    directory or carries an AppContainer read ACE.
    /// 2. Sweeps any leftover `oxideswarm.task.<worker8>.*` AppContainer profiles for this
    ///    `sandbox_base`.
    /// 3. Prepares the worker's own toolchain ACLs once (`oxideswarm.toolchain` capability SID).
    /// 4. Runs a probe command inside a temporary AppContainer at `level` verifying that:
    ///    - the task can write inside its sandbox,
    ///    - the task cannot write into `sandbox_base`, and
    ///    - the task cannot open or read any existing file in `protected_files` (or
    ///      `<host.user_home>/.oxideswarm/cluster.secret`) (N5).
    pub(super) async fn probe_with_config(
        level: IsolationLevel,
        sandbox_base: &Path,
        host: &HostDirs,
        protected_files: &[PathBuf],
    ) -> Result<(), String> {
        let fail =
            |what: String| format!("--isolation {level} cannot be enforced on this host: {what}");

        let host_clone = host.clone();
        let protected_clone = protected_files.to_vec();
        let base_clone = sandbox_base.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let _ = sweep_leftover_appcontainer_profiles_for_base(&base_clone);
            prepare_worker_toolchains(&host_clone, &protected_clone, Some(&base_clone))
        })
        .await
        .map_err(|e| fail(format!("toolchain preparation task panicked: {e}")))?
        .map_err(fail)?;

        let mut existing_protected: Vec<PathBuf> = Vec::new();
        for pf in protected_files {
            if !pf.as_os_str().is_empty() && pf.exists() {
                let norm_pf = if !is_reparse_point(pf) {
                    normalize_win_path(pf)
                } else {
                    pf.clone()
                };
                if !existing_protected.contains(&norm_pf) {
                    existing_protected.push(norm_pf);
                }
            }
        }
        if !host.user_home.as_os_str().is_empty() {
            let default_secret = host.user_home.join(".oxideswarm").join("cluster.secret");
            if default_secret.exists() {
                let norm_def = if !is_reparse_point(&default_secret) {
                    normalize_win_path(&default_secret)
                } else {
                    default_secret
                };
                if !existing_protected.contains(&norm_def) {
                    existing_protected.push(norm_def);
                }
            }
        }

        let probe_dir = sandbox_base.join(format!(".isolation-probe-{}", std::process::id()));
        std::fs::create_dir_all(&probe_dir).map_err(|e| {
            format!(
                "--isolation {level}: cannot create the probe directory {}: {e}",
                probe_dir.display()
            )
        })?;
        let norm_base = if !is_reparse_point(sandbox_base) {
            normalize_win_path(sandbox_base)
        } else {
            sandbox_base.to_path_buf()
        };
        let norm_probe_dir = if !is_reparse_point(&probe_dir) {
            normalize_win_path(&probe_dir)
        } else {
            probe_dir.clone()
        };
        let outside_name = format!(".isolation-outside-{}", std::process::id());
        let outside_file = norm_base.join(&outside_name);
        let outcome = async {
            let empty_host =
                HostDirs::from_home(PathBuf::new(), Some(PathBuf::new()), Some(PathBuf::new()));
            let jail = Jail {
                level,
                host: &empty_host,
                protected_files: &[],
                sandbox_base: &norm_base,
                sandbox_dir: &norm_probe_dir,
                extra_writable_dir: None,
                workspace_root: None,
            };
            let comspec = std::env::var("COMSPEC")
                .unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".to_string());
            let mut cmd_line = format!(
                "echo ok > inside || exit 2 & (echo no > ..\\{outside_name} 2>probe_err && exit 3)"
            );
            for idx in 0..existing_protected.len() {
                let code = 10 + idx;
                cmd_line.push_str(&format!(
                    " & ((ver >probe_sink) < %OXIDE_PROBE_PROTECTED_{idx}% 2>probe_err && exit {code})"
                ));
            }
            cmd_line.push_str(" & exit 0");
            let mut cmd = tokio::process::Command::new(&comspec);
            cmd.args(["/C", &cmd_line]);
            cmd.current_dir(&norm_probe_dir);
            cmd.env_clear();
            for (name, value) in super::inherited_environment(std::env::vars_os()) {
                cmd.env(name, value);
            }
            for (idx, pf) in existing_protected.iter().enumerate() {
                cmd.env(format!("OXIDE_PROBE_PROTECTED_{idx}"), pf.as_os_str());
            }
            let probe_token = uuid::Uuid::new_v4();
            let guard = WindowsAppContainerGuard::create(probe_token, &jail, &cmd)
                .map_err(|e| fail(format!("AppContainer setup failed: {e}")))?;
            let mut child = guard
                .spawn_in_appcontainer(&mut cmd, 0)
                .map_err(|e| fail(format!("could not spawn AppContainer probe: {e}")))?;
            let status = match tokio::time::timeout(
                std::time::Duration::from_secs(15),
                child.wait(),
            )
            .await
            {
                Ok(res) => res
                    .map_err(|e| fail(format!("waiting for AppContainer probe failed: {e}")))?,
                Err(_) => {
                    let _ = child.start_kill();
                    drop(guard);
                    return Err(fail(
                        "AppContainer startup probe timed out after 15s".to_string(),
                    ));
                }
            };
            drop(guard);
            match status.code() {
                Some(0) if probe_dir.join("inside").exists() && !outside_file.exists() => Ok(()),
                Some(3) => Err(fail(
                    "the probe wrote outside its sandbox, so the AppContainer DACL was not enforced"
                        .to_string(),
                )),
                Some(code)
                    if code >= 10 && (code as usize - 10) < existing_protected.len() =>
                {
                    let leaked = &existing_protected[code as usize - 10];
                    Err(fail(format!(
                        "protected secret/config file '{}' is readable from inside the AppContainer; revoke overly broad ACLs (such as S-1-15-2-1) or move the file outside system directories",
                        leaked.display()
                    )))
                }
                _ => Err(fail(format!("the probe failed ({status})"))),
            }
        }
        .await;
        let _ = std::fs::remove_file(&outside_file);
        let _ = std::fs::remove_dir_all(&probe_dir);
        outcome
    }

    struct WinProcessHandle(HANDLE);
    unsafe impl Send for WinProcessHandle {}
    unsafe impl Sync for WinProcessHandle {}

    impl Drop for WinProcessHandle {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    CloseHandle(self.0);
                }
            }
        }
    }

    /// A child process spawned inside a Windows AppContainer via `CreateProcessW` +
    /// `STARTUPINFOEXW` (`PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES`).
    pub struct AppContainerChild {
        process: Arc<WinProcessHandle>,
        pid: u32,
        pub stdin: Option<tokio::fs::File>,
        pub stdout: Option<tokio::fs::File>,
        pub stderr: Option<tokio::fs::File>,
        exited: bool,
    }

    impl AppContainerChild {
        pub fn id(&self) -> Option<u32> {
            if self.exited {
                None
            } else {
                Some(self.pid)
            }
        }

        pub fn raw_handle(&self) -> Option<*mut core::ffi::c_void> {
            if self.exited {
                None
            } else {
                Some(self.process.0)
            }
        }

        pub fn start_kill(&mut self) -> io::Result<()> {
            if !self.exited {
                unsafe {
                    let _ = TerminateProcess(self.process.0, 1);
                }
            }
            Ok(())
        }

        pub async fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
            drop(self.stdin.take());
            let proc_handle = Arc::clone(&self.process);
            let status = tokio::task::spawn_blocking(move || unsafe {
                let wait_rc = WaitForSingleObject(proc_handle.0, INFINITE);
                if wait_rc != WAIT_OBJECT_0 {
                    return Err(io::Error::last_os_error());
                }
                let mut code: u32 = 1;
                if GetExitCodeProcess(proc_handle.0, &mut code) == 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(std::process::ExitStatus::from_raw(code))
            })
            .await
            .map_err(|e| io::Error::other(format!("AppContainer wait join error: {e}")))??;
            self.exited = true;
            Ok(status)
        }
    }

    impl Drop for AppContainerChild {
        fn drop(&mut self) {
            if !self.exited {
                unsafe {
                    let _ = TerminateProcess(self.process.0, 1);
                }
            }
        }
    }

    fn append_windows_quoted_arg(cmd: &mut Vec<u16>, arg: &OsStr, force_quotes: bool) {
        let wide: Vec<u16> = arg.encode_wide().collect();
        let needs_quotes = force_quotes
            || wide.is_empty()
            || wide.iter().any(|&c| {
                c == b' ' as u16
                    || c == b'\t' as u16
                    || c == b'\n' as u16
                    || c == 0x0b
                    || c == b'"' as u16
            });
        if !needs_quotes {
            cmd.extend_from_slice(&wide);
            return;
        }
        cmd.push(b'"' as u16);
        let mut backslashes = 0usize;
        for &ch in &wide {
            if ch == b'\\' as u16 {
                backslashes += 1;
            } else if ch == b'"' as u16 {
                for _ in 0..(backslashes * 2 + 1) {
                    cmd.push(b'\\' as u16);
                }
                cmd.push(b'"' as u16);
                backslashes = 0;
            } else {
                for _ in 0..backslashes {
                    cmd.push(b'\\' as u16);
                }
                backslashes = 0;
                cmd.push(ch);
            }
        }
        for _ in 0..(backslashes * 2) {
            cmd.push(b'\\' as u16);
        }
        cmd.push(b'"' as u16);
    }

    fn build_windows_command_line(std_cmd: &std::process::Command) -> Vec<u16> {
        let mut cmdline = Vec::new();
        let prog = std_cmd.get_program();
        let is_cmd_exe = Path::new(prog)
            .file_stem()
            .and_then(|s| s.to_str())
            .is_some_and(|s| s.eq_ignore_ascii_case("cmd"));
        append_windows_quoted_arg(&mut cmdline, prog, true);
        let args: Vec<&OsStr> = std_cmd.get_args().collect();
        if is_cmd_exe && args.len() == 2 && args[0].eq_ignore_ascii_case("/C") {
            cmdline.extend_from_slice(&[
                b' ' as u16,
                b'/' as u16,
                b'C' as u16,
                b' ' as u16,
                b'"' as u16,
            ]);
            cmdline.extend(args[1].encode_wide());
            cmdline.push(b'"' as u16);
        } else {
            for arg in args {
                cmdline.push(b' ' as u16);
                append_windows_quoted_arg(&mut cmdline, arg, false);
            }
        }
        cmdline.push(0);
        cmdline
    }

    fn build_windows_env_block(std_cmd: &std::process::Command) -> Vec<u16> {
        let mut env_map: BTreeMap<String, (OsString, OsString)> = BTreeMap::new();
        for (k, v_opt) in std_cmd.get_envs() {
            let upper = k.to_string_lossy().to_uppercase();
            match v_opt {
                Some(v) => {
                    env_map.insert(upper, (k.to_os_string(), v.to_os_string()));
                }
                None => {
                    env_map.remove(&upper);
                }
            }
        }
        // `CreateProcessW` with `PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES` calls
        // `KernelBase!BasepAppContainerEnvironmentExtension`, which fails with
        // `ERROR_ENVVAR_NOT_FOUND` (203) if `LOCALAPPDATA` or `SystemRoot` is absent from the
        // child's environment block.
        for req in ["SystemRoot", "LOCALAPPDATA"] {
            let upper = req.to_uppercase();
            if let std::collections::btree_map::Entry::Vacant(e) = env_map.entry(upper) {
                if let Some(val) = std::env::var_os(req) {
                    e.insert((OsString::from(req), val));
                }
            }
        }
        let mut block = Vec::new();
        for (_, (k, v)) in env_map {
            block.extend(k.encode_wide());
            block.push(b'=' as u16);
            block.extend(v.encode_wide());
            block.push(0);
        }
        if block.is_empty() {
            block.push(0);
        }
        block.push(0);
        block
    }

    /// Creates an anonymous pipe pair with `bInheritHandle = 0` so neither end is inheritable by
    /// concurrent `CreateProcess` calls on other threads before `WINDOWS_SPAWN_LOCK` is acquired
    /// (M2).
    fn create_non_inheritable_pipe_pair() -> io::Result<(HANDLE, HANDLE)> {
        let sa = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: std::ptr::null_mut(),
            bInheritHandle: 0,
        };
        let mut read_pipe: HANDLE = std::ptr::null_mut();
        let mut write_pipe: HANDLE = std::ptr::null_mut();
        let ok = unsafe { CreatePipe(&mut read_pipe, &mut write_pipe, &sa, 0) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((read_pipe, write_pipe))
    }

    fn set_window_object_protected_dacl(
        handle: HANDLE,
        entries: &[EXPLICIT_ACCESS_W],
    ) -> io::Result<()> {
        if handle.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "null window object handle",
            ));
        }
        let mut new_dacl: *mut ACL = std::ptr::null_mut();
        let rc = unsafe {
            SetEntriesInAclW(
                entries.len() as u32,
                entries.as_ptr(),
                std::ptr::null(),
                &mut new_dacl,
            )
        };
        if rc != ERROR_SUCCESS || new_dacl.is_null() {
            return Err(io::Error::from_raw_os_error(rc as i32));
        }
        let set_rc = unsafe {
            SetSecurityInfo(
                handle,
                SE_WINDOW_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                new_dacl,
                std::ptr::null(),
            )
        };
        unsafe {
            LocalFree(new_dacl as HLOCAL);
        }
        if set_rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(set_rc as i32));
        }
        Ok(())
    }

    fn grant_sid_on_window_object(handle: HANDLE, sid: PSID, access_mask: u32) -> io::Result<()> {
        if handle.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "null window object handle",
            ));
        }
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        let mut old_dacl: *mut ACL = std::ptr::null_mut();
        let rc = unsafe {
            GetSecurityInfo(
                handle,
                SE_WINDOW_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut old_dacl,
                std::ptr::null_mut(),
                &mut sd,
            )
        };
        if rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(rc as i32));
        }
        let ea = explicit_access(sid, access_mask, GRANT_ACCESS, NO_INHERITANCE);
        let mut new_dacl: *mut ACL = std::ptr::null_mut();
        let rc2 = unsafe { SetEntriesInAclW(1, &ea, old_dacl, &mut new_dacl) };
        let mut set_rc = rc2;
        if rc2 == ERROR_SUCCESS && !new_dacl.is_null() {
            unsafe {
                set_rc = SetSecurityInfo(
                    handle,
                    SE_WINDOW_OBJECT,
                    DACL_SECURITY_INFORMATION,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    new_dacl,
                    std::ptr::null(),
                );
                LocalFree(new_dacl as HLOCAL);
            }
        }
        if !sd.is_null() {
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        if set_rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(set_rc as i32));
        }
        Ok(())
    }

    /// Creates a dedicated non-interactive Window Station for the worker and a unique per-task
    /// private Desktop (`ox_desk_<pid>_<run_token>`) for each task (N7, design 0033 D4.7).
    ///
    /// Security properties:
    /// - Never modifies the user's interactive Window Station (`WinSta0`) or `Default` desktop.
    /// - Holds `WINDOWS_SPAWN_LOCK` around `SetProcessWindowStation(alt_winsta)` ->
    ///   `CreateDesktopW` -> `SetProcessWindowStation(orig_winsta)` and switches back immediately
    ///   before any other spawn can observe it.
    /// - On the non-interactive Window Station, grants `WINSTA_TASK_MASK` (`0x00020123`) to
    ///   `oxideswarm.toolchain`, omitting `WINSTA_ACCESSCLIPBOARD (0x0004)`,
    ///   `WINSTA_CREATEDESKTOP (0x0008)`, `WINSTA_WRITEATTRIBUTES (0x0010)`,
    ///   `WINSTA_EXITWINDOWS (0x0040)`, and `WINSTA_READSCREEN (0x0200)`.
    /// - On the per-task Desktop, sets a protected DACL granting `DESKTOP_TASK_MASK` (`0x000200C7`)
    ///   ONLY to `CurrentUser`, `SYSTEM`, `Administrators`, and this task's specific
    ///   `app_container_sid` (NOT `oxideswarm.toolchain`), so one task cannot open another task's
    ///   desktop or send window messages to another task's windows.
    /// - Returns `Err(...)` if Window Station or Desktop creation fails so the task fails rather
    ///   than inheriting the worker's desktop.
    fn create_per_task_desktop(
        app_container_sid: PSID,
        run_token: uuid::Uuid,
    ) -> io::Result<(HANDLE, Vec<u16>)> {
        static WORKER_WINSTA: OnceLock<Result<(usize, String), String>> = OnceLock::new();

        ensure_process_token_default_dacl_grants_current_user();
        let _spawn_guard = WINDOWS_SPAWN_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let winsta_res = WORKER_WINSTA.get_or_init(|| {
            let toolchain_sid = OwnedSid::oxideswarm_toolchain().map_err(|e| e.to_string())?;
            let user_sid = OwnedSid::current_user().map_err(|e| e.to_string())?;
            let system_sid =
                OwnedSid::from_well_known(WinLocalSystemSid).map_err(|e| e.to_string())?;
            let admins_sid = OwnedSid::from_well_known(WinBuiltinAdministratorsSid)
                .map_err(|e| e.to_string())?;
            unsafe {
                // Always open/create the logon session's built-in non-interactive Window Station
                // (`NULL` -> `Service-0x0-<LogonId>$`). When the worker runs elevated and uses a
                // fresh `LOGON32_LOGON_NEW_CREDENTIALS` logon token for AppContainer spawning,
                // impersonate that logon token around `CreateWindowStationW(NULL, ...)` so
                // `win32kbase!GetProcessLuid` resolves the new logon session's Window Station
                // `Service-0x0-<NewLogonId>$` (matching the `AuthenticationId` of the spawned task
                // and any console grandchildren that resolve `Service-0x0-<NewLogonId>$\Default`).
                let lua_logon_token = get_or_init_worker_lua_tokens()
                    .map(|(_, logon)| logon)
                    .filter(|h| !h.is_null());
                let impersonated = if let Some(h_logon) = lua_logon_token {
                    ImpersonateLoggedOnUser(h_logon) != 0
                } else {
                    false
                };
                let alt_winsta =
                    CreateWindowStationW(std::ptr::null(), 0, 0x000F_037F, std::ptr::null());
                let winsta_err = if alt_winsta.is_null() {
                    Some(io::Error::last_os_error())
                } else {
                    None
                };
                if impersonated {
                    let _ = RevertToSelf();
                }
                if let Some(e) = winsta_err {
                    return Err(format!("CreateWindowStationW failed: {e}"));
                }

                let mut buf = vec![0u16; 256];
                let mut needed: u32 = 0;
                if GetUserObjectInformationW(
                    alt_winsta,
                    2,
                    buf.as_mut_ptr() as *mut core::ffi::c_void,
                    (buf.len() * 2) as u32,
                    &mut needed,
                ) == 0
                {
                    return Err(format!(
                        "GetUserObjectInformationW(UOI_NAME) failed: {}",
                        io::Error::last_os_error()
                    ));
                }
                let mut len = 0usize;
                while len < buf.len() && buf[len] != 0 {
                    len += 1;
                }
                let winsta_name = OsString::from_wide(&buf[..len])
                    .to_string_lossy()
                    .into_owned();

                const WINSTA_TASK_MASK: u32 = 0x0002_0123;
                grant_sid_on_window_object(alt_winsta, user_sid.as_psid(), 0x000F_037F)
                    .map_err(|e| format!("grant user_sid on Window Station failed: {e}"))?;
                grant_sid_on_window_object(alt_winsta, system_sid.as_psid(), 0x000F_037F)
                    .map_err(|e| format!("grant system_sid on Window Station failed: {e}"))?;
                grant_sid_on_window_object(alt_winsta, admins_sid.as_psid(), 0x000F_037F)
                    .map_err(|e| format!("grant admins_sid on Window Station failed: {e}"))?;
                grant_sid_on_window_object(alt_winsta, toolchain_sid.as_psid(), WINSTA_TASK_MASK)
                    .map_err(|e| format!("grant toolchain_sid on Window Station failed: {e}"))?;

                // When `alt_winsta` is a non-interactive Window Station (e.g.
                // `Service-0x0-<LogonId>$` on `windows-latest`), console processes such as
                // `cargo.exe` (which do not link `user32.dll`, so `Win32Process == NULL`) spawn
                // grandchildren (`rustc.exe -vV`) via `std::process::Command::spawn()` with
                // `si.lpDesktop == NULL`. When `si.lpDesktop == NULL` and `Win32Process == NULL`,
                // Windows resolves the `"Default"` desktop of the process Window Station
                // (`Service-0x0-<LogonId>$\Default`). Grant `user_sid` and `toolchain_sid`
                // (`DESKTOP_TASK_MASK`) on `Service-0x0-<LogonId>$\Default` (never touching
                // `WinSta0` or `WinSta0\Default`, N7) so grandchild console processes do not fail
                // with `STATUS_ACCESS_DENIED (os error 5)`.
                if !winsta_name.eq_ignore_ascii_case("WinSta0") {
                    let orig_winsta = GetProcessWindowStation();
                    if !orig_winsta.is_null() && SetProcessWindowStation(alt_winsta) != 0 {
                        let wide_default = to_wide(OsStr::new("Default"));
                        let default_desk = CreateDesktopW(
                            wide_default.as_ptr(),
                            std::ptr::null(),
                            std::ptr::null(),
                            0,
                            0x000F_01FF,
                            std::ptr::null(),
                        );
                        let _ = SetProcessWindowStation(orig_winsta);
                        if !default_desk.is_null() {
                            const DESKTOP_TASK_MASK: u32 = 0x0002_00C7;
                            let _ = grant_sid_on_window_object(
                                default_desk,
                                user_sid.as_psid(),
                                0x000F_01FF,
                            );
                            let _ = grant_sid_on_window_object(
                                default_desk,
                                system_sid.as_psid(),
                                0x000F_01FF,
                            );
                            let _ = grant_sid_on_window_object(
                                default_desk,
                                admins_sid.as_psid(),
                                0x000F_01FF,
                            );
                            let _ = grant_sid_on_window_object(
                                default_desk,
                                toolchain_sid.as_psid(),
                                DESKTOP_TASK_MASK,
                            );
                            let _ = CloseDesktop(default_desk);
                        }
                    }
                }

                Ok((alt_winsta as usize, winsta_name))
            }
        });

        let (alt_winsta_raw, winsta_name) = match winsta_res {
            Ok(v) => (v.0 as HANDLE, v.1.as_str()),
            Err(msg) => return Err(io::Error::new(io::ErrorKind::PermissionDenied, msg.clone())),
        };

        let user_sid = OwnedSid::current_user()?;
        let system_sid = OwnedSid::from_well_known(WinLocalSystemSid)?;
        let admins_sid = OwnedSid::from_well_known(WinBuiltinAdministratorsSid)?;

        let desk_name = format!("ox_desk_{}_{}", std::process::id(), run_token.simple());
        let wide_desk_name = to_wide(OsStr::new(&desk_name));

        let alt_desk = unsafe {
            let orig_winsta = GetProcessWindowStation();
            if orig_winsta.is_null() {
                return Err(io::Error::last_os_error());
            }
            if SetProcessWindowStation(alt_winsta_raw) == 0 {
                return Err(io::Error::last_os_error());
            }
            let desk = CreateDesktopW(
                wide_desk_name.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                0,
                0x000F_01FF,
                std::ptr::null(),
            );
            let desk_err = if desk.is_null() {
                Some(io::Error::last_os_error())
            } else {
                None
            };
            let _ = SetProcessWindowStation(orig_winsta);
            if let Some(e) = desk_err {
                return Err(e);
            }
            desk
        };

        const DESKTOP_TASK_MASK: u32 = 0x0002_00C7;
        let desk_entries = [
            explicit_access(user_sid.as_psid(), 0x000F_01FF, SET_ACCESS, NO_INHERITANCE),
            explicit_access(
                system_sid.as_psid(),
                0x000F_01FF,
                SET_ACCESS,
                NO_INHERITANCE,
            ),
            explicit_access(
                admins_sid.as_psid(),
                0x000F_01FF,
                SET_ACCESS,
                NO_INHERITANCE,
            ),
            explicit_access(
                app_container_sid,
                DESKTOP_TASK_MASK,
                SET_ACCESS,
                NO_INHERITANCE,
            ),
        ];
        if let Err(e) = set_window_object_protected_dacl(alt_desk, &desk_entries) {
            unsafe {
                let _ = CloseDesktop(alt_desk);
            }
            return Err(e);
        }

        let full_desktop = format!("{winsta_name}\\{desk_name}");
        Ok((alt_desk, to_wide(OsStr::new(&full_desktop))))
    }

    fn spawn_with_security_capabilities(
        std_cmd: &std::process::Command,
        sc: &SECURITY_CAPABILITIES,
        desktop_name_wide: &[u16],
        creation_flags: u32,
    ) -> io::Result<AppContainerChild> {
        ensure_process_token_default_dacl_grants_current_user();
        if desktop_name_wide.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "refusing to spawn AppContainer task without a private desktop",
            ));
        }
        let (stdin_read, stdin_write) = create_non_inheritable_pipe_pair()?;
        let (stdout_read, stdout_write) = match create_non_inheritable_pipe_pair() {
            Ok(p) => p,
            Err(e) => {
                unsafe {
                    CloseHandle(stdin_read);
                    CloseHandle(stdin_write);
                }
                return Err(e);
            }
        };
        let (stderr_read, stderr_write) = match create_non_inheritable_pipe_pair() {
            Ok(p) => p,
            Err(e) => {
                unsafe {
                    CloseHandle(stdin_read);
                    CloseHandle(stdin_write);
                    CloseHandle(stdout_read);
                    CloseHandle(stdout_write);
                }
                return Err(e);
            }
        };

        let mut cmdline = build_windows_command_line(std_cmd);
        let env_block = build_windows_env_block(std_cmd);
        let cwd_wide = std_cmd.get_current_dir().map(|p| to_wide(p.as_os_str()));
        let cwd_ptr = cwd_wide.as_ref().map_or(std::ptr::null(), |w| w.as_ptr());

        let mut pi: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
        let flags = creation_flags
            | EXTENDED_STARTUPINFO_PRESENT
            | CREATE_UNICODE_ENVIRONMENT
            | CREATE_NO_WINDOW;

        // Hold `WINDOWS_SPAWN_LOCK` around marking the 3 child pipe ends inheritable,
        // calling `CreateProcessW` with `PROC_THREAD_ATTRIBUTE_HANDLE_LIST`, AND closing the 3
        // child pipe ends in the parent (M2, M5).
        let spawn_err = {
            let _spawn_guard = WINDOWS_SPAWN_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            let mut inherited_handles: [HANDLE; 3] = [stdin_read, stdout_write, stderr_write];
            let mut inherit_ok = true;
            for &h in &inherited_handles {
                if unsafe { SetHandleInformation(h, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) } == 0
                {
                    inherit_ok = false;
                    break;
                }
            }

            let res = if !inherit_ok {
                Some(io::Error::last_os_error())
            } else {
                let mut attr_size: usize = 0;
                unsafe {
                    let _ = InitializeProcThreadAttributeList(
                        std::ptr::null_mut(),
                        2,
                        0,
                        &mut attr_size,
                    );
                }
                let mut attr_buf = vec![0u8; attr_size.max(64)];
                let attr_list = attr_buf.as_mut_ptr() as LPPROC_THREAD_ATTRIBUTE_LIST;
                if unsafe { InitializeProcThreadAttributeList(attr_list, 2, 0, &mut attr_size) }
                    == 0
                {
                    Some(io::Error::last_os_error())
                } else {
                    let sc_ok = unsafe {
                        UpdateProcThreadAttribute(
                            attr_list,
                            0,
                            PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES as usize,
                            sc as *const SECURITY_CAPABILITIES as *const core::ffi::c_void,
                            std::mem::size_of::<SECURITY_CAPABILITIES>(),
                            std::ptr::null_mut(),
                            std::ptr::null(),
                        )
                    };
                    let hl_ok = if sc_ok != 0 {
                        unsafe {
                            UpdateProcThreadAttribute(
                                attr_list,
                                0,
                                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                                inherited_handles.as_mut_ptr() as *const core::ffi::c_void,
                                inherited_handles.len() * std::mem::size_of::<HANDLE>(),
                                std::ptr::null_mut(),
                                std::ptr::null(),
                            )
                        }
                    } else {
                        0
                    };

                    let inner_res = if sc_ok == 0 || hl_ok == 0 {
                        Some(io::Error::last_os_error())
                    } else {
                        let mut desktop_wide = desktop_name_wide.to_vec();
                        let mut si_ex: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
                        si_ex.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
                        si_ex.StartupInfo.lpDesktop = desktop_wide.as_mut_ptr();
                        si_ex.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
                        si_ex.StartupInfo.hStdInput = stdin_read;
                        si_ex.StartupInfo.hStdOutput = stdout_write;
                        si_ex.StartupInfo.hStdError = stderr_write;
                        si_ex.lpAttributeList = attr_list;

                        // Suppress any Windows hard-error modal dialog (`SEM_FAILCRITICALERRORS |
                        // SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX = 0x8003`) so a child
                        // initialization failure exits immediately with a non-zero status instead
                        // of blocking inside `csrss.exe` waiting for an interactive "OK" click.
                        unsafe {
                            let _ = SetErrorMode(0x8003);
                        }

                        // When the worker runs elevated (`is_process_elevated() == true`, e.g.
                        // `runneradmin` on GitHub Actions `windows-latest`), start the
                        // AppContainer child from a primary LUA-filtered token
                        // (`CreateRestrictedToken` with `LUA_TOKEN` + `DuplicateTokenEx` with
                        // `TokenPrimary` + `CreateProcessAsUserW` with the same
                        // `SECURITY_CAPABILITIES`) so the child never inherits administrative
                        // elevation. Clone `cmdline` before `CreateProcessAsUserW` because Win32
                        // `CreateProcessAsUserW` may mutate `lpCommandLine` in place.
                        let lua_token = get_or_init_worker_lua_token();
                        let spawned = unsafe {
                            let res = if let Some(tok) = lua_token {
                                let mut lua_cmdline = cmdline.clone();
                                CreateProcessAsUserW(
                                    tok,
                                    std::ptr::null(),
                                    lua_cmdline.as_mut_ptr(),
                                    std::ptr::null(),
                                    std::ptr::null(),
                                    1,
                                    flags,
                                    env_block.as_ptr() as *const core::ffi::c_void,
                                    cwd_ptr,
                                    &si_ex.StartupInfo,
                                    &mut pi,
                                )
                            } else {
                                0
                            };
                            if res != 0 {
                                res
                            } else {
                                CreateProcessW(
                                    std::ptr::null(),
                                    cmdline.as_mut_ptr(),
                                    std::ptr::null(),
                                    std::ptr::null(),
                                    1,
                                    flags,
                                    env_block.as_ptr() as *const core::ffi::c_void,
                                    cwd_ptr,
                                    &si_ex.StartupInfo,
                                    &mut pi,
                                )
                            }
                        };
                        if spawned == 0 {
                            Some(io::Error::last_os_error())
                        } else {
                            None
                        }
                    };
                    unsafe {
                        DeleteProcThreadAttributeList(attr_list);
                    }
                    inner_res
                }
            };

            unsafe {
                CloseHandle(stdin_read);
                CloseHandle(stdout_write);
                CloseHandle(stderr_write);
            }

            res
        };

        if let Some(err) = spawn_err {
            unsafe {
                CloseHandle(stdin_write);
                CloseHandle(stdout_read);
                CloseHandle(stderr_read);
            }
            return Err(err);
        }

        unsafe {
            CloseHandle(pi.hThread);
        }

        let stdin_file =
            unsafe { tokio::fs::File::from_std(std::fs::File::from_raw_handle(stdin_write as _)) };
        let stdout_file =
            unsafe { tokio::fs::File::from_std(std::fs::File::from_raw_handle(stdout_read as _)) };
        let stderr_file =
            unsafe { tokio::fs::File::from_std(std::fs::File::from_raw_handle(stderr_read as _)) };

        Ok(AppContainerChild {
            process: Arc::new(WinProcessHandle(pi.hProcess)),
            pid: pi.dwProcessId,
            stdin: Some(stdin_file),
            stdout: Some(stdout_file),
            stderr: Some(stderr_file),
            exited: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host() -> HostDirs {
        HostDirs::from_home(PathBuf::from("/home/u"), None, None)
    }

    #[test]
    fn test_host_dirs_default_to_dot_directories_and_honour_overrides() {
        let h = host();
        assert_eq!(h.rustup_home, PathBuf::from("/home/u/.rustup"));
        assert_eq!(h.cargo_home, PathBuf::from("/home/u/.cargo"));
        let h = HostDirs::from_home(
            "/home/u".into(),
            Some("/opt/rustup".into()),
            Some("".into()),
        );
        assert_eq!(h.rustup_home, PathBuf::from("/opt/rustup"));
        assert_eq!(
            h.cargo_home,
            PathBuf::from("/home/u/.cargo"),
            "an empty override is unset"
        );
    }

    #[test]
    fn test_prepare_environment_pins_toolchain_then_moves_home_and_strips_sockets() {
        let sandbox = Path::new("/sb/task1");
        let plan = prepare_environment(&host(), &HashMap::new(), sandbox);
        let get = |name: &str| {
            plan.set
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        };
        let joined = |leaf: &str| {
            Path::new("/home/u")
                .join(leaf)
                .to_string_lossy()
                .into_owned()
        };
        assert_eq!(get("RUSTUP_HOME"), Some(joined(".rustup").as_str()));
        assert_eq!(get("CARGO_HOME"), Some(joined(".cargo").as_str()));
        for name in ["HOME", "USERPROFILE", "TMPDIR", "TEMP", "TMP"] {
            assert_eq!(get(name), Some("/sb/task1"), "{name}");
        }
        // The toolchain is pinned before HOME moves.
        let pos = |n: &str| plan.set.iter().position(|(k, _)| k == n).unwrap();
        assert!(pos("RUSTUP_HOME") < pos("HOME") && pos("CARGO_HOME") < pos("HOME"));
        for name in [
            "SSH_AUTH_SOCK",
            "GPG_AGENT_INFO",
            "DBUS_SESSION_BUS_ADDRESS",
            "DOCKER_HOST",
        ] {
            assert!(plan.remove.contains(&name), "{name}");
        }
    }

    #[test]
    fn test_inherited_environment_is_an_allowlist() {
        let os = |s: &str| std::ffi::OsString::from(s);
        let vars = [
            ("PATH", "/bin"),
            ("LANG", "C"),
            ("LC_ALL", "C"),
            ("LC_", "x"),
            ("GITHUB_TOKEN", "t"),
            ("AWS_SECRET_ACCESS_KEY", "k"),
            ("OXIDESWARM_SECRET", "s"),
            ("OXIDESWARM_SECRET_FILE", "/f"),
            ("HOME", "/Users/u"),
            ("RUSTY_GRID_RUN_TOKEN", "t"),
            ("TERM", "xterm"),
        ]
        .map(|(k, v)| (os(k), os(v)));
        let kept: Vec<String> = inherited_environment(vars)
            .into_iter()
            .map(|(k, _)| k.to_string_lossy().into_owned())
            .collect();
        assert_eq!(kept, ["PATH", "LANG", "LC_ALL", "TERM"]);
    }

    #[test]
    fn test_prepare_environment_keeps_a_task_chosen_toolchain_location() {
        let mut env = HashMap::new();
        env.insert("CARGO_HOME".to_string(), "/sb/task1/.cargo".to_string());
        env.insert("HOME".to_string(), "/elsewhere".to_string());
        let plan = prepare_environment(&host(), &env, Path::new("/sb/task1"));
        assert!(
            plan.set.iter().all(|(k, _)| k != "CARGO_HOME"),
            "the task's CARGO_HOME stays"
        );
        assert!(plan.set.iter().any(|(k, _)| k == "RUSTUP_HOME"));
        assert!(
            plan.set
                .iter()
                .any(|(k, v)| k == "HOME" && v == "/sb/task1"),
            "HOME always moves into the sandbox"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_resolve_program_searches_path_like_execvp() {
        assert_eq!(resolve_program("/bin/sh", ""), Some("/bin/sh".to_string()));
        assert_eq!(resolve_program("./x", ""), Some("./x".to_string()));
        assert_eq!(
            resolve_program("sh", "/nonexistent-dir:/bin"),
            Some("/bin/sh".to_string())
        );
        assert_eq!(
            resolve_program("no-such-program-xyz", "/bin:/usr/bin"),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_seatbelt_profile_orders_deny_allow_deny_and_adds_network_rule_for_fs_net() {
        let fs = macos::profile(IsolationLevel::Fs, false, 2, 1, 1);
        let at = |needle: &str| {
            fs.find(needle)
                .unwrap_or_else(|| panic!("{needle} missing in\n{fs}"))
        };
        // Last match wins: broad deny, then the allows, then the credentials deny.
        let deny_home = at("(deny file-read*\n    (subpath (param \"USER_HOME\"))");
        let allow_tools = at("(allow file-read*\n    (subpath (param \"SANDBOX_DIR\"))");
        let deny_cred = at("(deny file-read*\n    (literal (param \"CARGO_CRED\"))");
        assert!(deny_home < allow_tools && allow_tools < deny_cred);
        assert!(fs.contains("(deny file-write* (subpath \"/\"))"));
        assert!(fs.contains("(literal (param \"ANCESTOR_1\"))"));
        assert!(!fs.contains("ANCESTOR_2"));
        assert!(!fs.contains("EXTRA_WRITABLE_DIR"));
        assert!(
            !fs.contains("(deny network*)"),
            "fs does not close the network, only the Unix sockets"
        );
        for rule in [
            "(deny lsopen)",
            "(deny appleevent-send)",
            "(deny user-preference-read user-preference-write)",
            "(deny network-outbound (remote unix-socket))",
            "(deny mach-lookup (global-name-prefix \"com.apple.pasteboard\"))",
            "(deny file-ioctl",
            "(deny signal)\n(allow signal (target same-sandbox))",
            "(param \"DENIED_0\")",
            "(deny file-read*\n    (literal (param \"PROTECTED_0\")))",
        ] {
            assert!(fs.contains(rule), "{rule} missing in\n{fs}");
        }
        let ws = macos::profile(IsolationLevel::Fs, true, 1, 1, 0);
        let ws_allow_write = ws
            .find("(allow file-write*\n    (subpath (param \"SANDBOX_DIR\"))\n    (subpath (param \"EXTRA_WRITABLE_DIR\"))")
            .expect("workspace allow file-write*");
        let ws_deny_unlink = ws
            .find("(deny file-write-unlink\n    (literal (param \"SANDBOX_DIR\"))\n    (literal (param \"EXTRA_WRITABLE_DIR\")))")
            .expect("workspace deny file-write-unlink on tree and tmp");
        let ws_deny_create = ws
            .find("(deny file-write-create\n    (literal (param \"SANDBOX_DIR\"))\n    (literal (param \"EXTRA_WRITABLE_DIR\")))")
            .expect("workspace deny file-write-create on tree and tmp");
        assert!(ws_allow_write < ws_deny_unlink && ws_deny_unlink < ws_deny_create);
        assert!(ws.contains("(allow file-read*\n    (subpath (param \"SANDBOX_DIR\"))\n    (subpath (param \"EXTRA_WRITABLE_DIR\"))"));
        let net = macos::profile(IsolationLevel::FsNet, false, 0, 0, 0);
        assert!(net.ends_with("(deny network*)\n"));
        assert!(!net.contains("ANCESTOR"));
    }

    #[cfg(unix)]
    #[test]
    fn test_seatbelt_arguments_define_every_parameter_the_profile_uses() {
        let h = HostDirs::from_home(
            PathBuf::from("/nonexistent-home"),
            Some("/nonexistent-home/.rustup".into()),
            None,
        );
        let jail = Jail {
            level: IsolationLevel::Fs,
            host: &h,
            protected_files: &[],
            sandbox_base: Path::new("/nonexistent-base"),
            sandbox_dir: Path::new("/nonexistent-base/t1"),
            extra_writable_dir: None,
            workspace_root: None,
        };
        let args = macos::arguments(&jail);
        assert_eq!(args[0], "-p");
        let defined: Vec<&str> = args
            .windows(2)
            .filter(|w| w[0] == "-D")
            .map(|w| w[1].split('=').next().unwrap())
            .collect();
        let profile = &args[1];
        for param in profile.split("(param \"").skip(1) {
            let name = param.split('"').next().unwrap();
            assert!(defined.contains(&name), "{name} is used but not defined");
        }
        assert!(args.contains(&"CARGO_HOME=/nonexistent-home/.cargo".to_string()));
        assert!(
            args.contains(&"CARGO_CRED_TOML=/nonexistent-home/.cargo/credentials.toml".to_string())
        );
        assert!(
            args.contains(&"ANCESTOR_0=/nonexistent-home".to_string())
                || args.contains(&"ANCESTOR_0=/nonexistent-base".to_string())
        );
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    #[tokio::test]
    async fn test_self_check_refuses_isolation_without_an_implementation() {
        let dir = tempfile::tempdir().unwrap();
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let err = verify_isolation_support(level, dir.path())
                .await
                .unwrap_err();
            let GridError::Config(text) = &err else {
                panic!("expected a Config error, got {err:?}");
            };
            assert!(
                text.contains("not implemented yet on this platform"),
                "{text}"
            );
            assert!(text.contains(&level.to_string()), "{text}");
        }
        verify_isolation_support(IsolationLevel::Off, dir.path())
            .await
            .expect("off needs nothing");
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    #[test]
    fn test_wrap_program_never_returns_the_plain_command() {
        let h = host();
        let jail = Jail {
            level: IsolationLevel::Fs,
            host: &h,
            protected_files: &[],
            sandbox_base: Path::new("/sb"),
            sandbox_dir: Path::new("/sb/t"),
            extra_writable_dir: None,
            workspace_root: None,
        };
        let err = wrap_program(&jail, "sh", &[], "/bin").unwrap_err();
        assert!(err.contains("not implemented yet"), "{err}");
    }

    #[cfg(any(target_os = "macos", windows))]
    #[tokio::test]
    async fn test_self_check_passes_on_supported_host() {
        let dir = tempfile::tempdir().unwrap();
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            verify_isolation_support(level, dir.path())
                .await
                .unwrap_or_else(|e| panic!("{level}: {e}"));
        }
        let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert!(leftovers.is_empty(), "the probe cleans up after itself");
    }

    #[cfg(windows)]
    #[test]
    fn test_wrap_program_resolves_or_rejects_missing_executable_on_windows() {
        let h = host();
        let jail = Jail {
            level: IsolationLevel::Fs,
            host: &h,
            protected_files: &[],
            sandbox_base: Path::new(r"C:\sb"),
            sandbox_dir: Path::new(r"C:\sb\t"),
            extra_writable_dir: None,
            workspace_root: None,
        };
        let err =
            wrap_program(&jail, "no-such-program-feat3", &[], r"C:\Windows\System32").unwrap_err();
        assert!(err.contains("not found in PATH"), "{err}");
        let (resolved, args) = wrap_program(
            &jail,
            "cmd",
            &["/C".into(), "echo".into()],
            r"C:\Windows\System32",
        )
        .expect("cmd.exe resolves in System32");
        assert!(
            resolved.to_ascii_lowercase().ends_with("cmd.exe"),
            "{resolved}"
        );
        assert_eq!(args, vec!["/C".to_string(), "echo".to_string()]);
    }

    #[cfg(windows)]
    #[test]
    fn test_path_starts_with_ci_rejects_dotdot_escape_on_nonexistent_paths() {
        let temp = tempfile::tempdir().unwrap();
        let sandbox = temp.path().join("sandbox_task_1");
        std::fs::create_dir_all(&sandbox).unwrap();

        assert!(
            windows::path_starts_with_ci(&sandbox.join("sub").join("..").join("inside"), &sandbox),
            "normalized path inside sandbox must pass"
        );
        assert!(
            !windows::path_starts_with_ci(&sandbox.join("..").join(".."), &sandbox),
            "<sandbox>\\..\\.. must NOT pass path_starts_with_ci"
        );
        assert!(
            !windows::path_starts_with_ci(
                &sandbox
                    .join("nonexistent_child")
                    .join("..")
                    .join("..")
                    .join("secret"),
                &sandbox
            ),
            "<sandbox>\\nonexistent\\..\\..\\secret must NOT pass path_starts_with_ci"
        );
        assert!(
            !windows::path_starts_with_ci(
                Path::new(r"C:\nonexistent_sb\task1\..\..\Users\victim"),
                Path::new(r"C:\nonexistent_sb\task1")
            ),
            "purely non-existent <sandbox>\\..\\.. escape must NOT pass"
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_validate_sandbox_base_safe_rejects_user_folders_and_non_empty_dirs() {
        let fake_home = tempfile::tempdir().unwrap();
        let host = HostDirs::from_home(fake_home.path().to_path_buf(), None, None);

        let docs = fake_home.path().join("Documents");
        std::fs::create_dir_all(&docs).unwrap();
        let err_docs = windows::validate_sandbox_base_safe(&docs, &host)
            .expect_err("must reject <user_home>\\Documents");
        assert!(err_docs.contains("user folder"), "{err_docs}");

        let ox_sandboxes = fake_home.path().join(".oxideswarm").join("sandboxes");
        std::fs::create_dir_all(&ox_sandboxes).unwrap();
        windows::validate_sandbox_base_safe(&ox_sandboxes, &host)
            .expect("<user_home>\\.oxideswarm\\sandboxes must be accepted");

        let outside = tempfile::tempdir().unwrap();
        let non_empty = outside.path().join("operator_folder");
        std::fs::create_dir_all(&non_empty).unwrap();
        std::fs::write(non_empty.join("important.docx"), "data").unwrap();
        let err_non_empty = windows::validate_sandbox_base_safe(&non_empty, &host)
            .expect_err("must reject existing directory containing non-sandbox files");
        assert!(err_non_empty.contains("important.docx"), "{err_non_empty}");
    }
}
