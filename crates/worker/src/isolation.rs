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
    /// Extra read-only runtime directories explicitly allowlisted for isolated tasks
    /// (`--isolation-read-dir` / `RUSTY_GRID_ISOLATION_READ_DIRS` / `[worker].isolation_read_dirs`).
    pub extra_read_only_dirs: Vec<PathBuf>,
}

/// Environment variable supplying extra read-only runtime directories for isolated tasks.
/// Uses the platform path-list separator (`;` on Windows, `:` on Unix), matching `PATH`.
pub const ISOLATION_READ_DIRS_ENV: &str = "RUSTY_GRID_ISOLATION_READ_DIRS";

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
            extra_read_only_dirs: Vec::new(),
        }
    }

    /// Attaches operator-allowlisted read-only external runtime directories (`design 0074`).
    /// Raw paths are preserved here so [`validate_extra_read_only_dirs`] inspects symlinks and
    /// NTFS junctions before canonicalization.
    pub fn with_extra_read_only_dirs(mut self, dirs: Vec<PathBuf>) -> Self {
        self.extra_read_only_dirs = dirs;
        self
    }
}

/// Default filename under `<OXIDESWARM_HOME>/` or `<user_home>/.oxideswarm/` where every path
/// whose DACL is modified by `prepare_worker_toolchains` on Windows is recorded before the
/// modification takes place.
pub const ACL_GRANT_LOG_FILENAME: &str = "windows_acl_grants.txt";

/// Pure helper for [`acl_grant_manifest_path`] so precedence can be unit-tested on every OS
/// without mutating process-wide environment variables.
pub fn acl_grant_manifest_path_with(
    host: &HostDirs,
    grant_log_env: Option<&std::ffi::OsStr>,
    oxideswarm_home_env: Option<&std::ffi::OsStr>,
) -> PathBuf {
    if let Some(custom) = grant_log_env {
        if !custom.is_empty() {
            return PathBuf::from(custom);
        }
    }
    if let Some(ox_home) = oxideswarm_home_env {
        if !ox_home.is_empty() {
            return PathBuf::from(ox_home).join(ACL_GRANT_LOG_FILENAME);
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

/// Returns the path of the reversible Windows ACL grant manifest file.
///
/// Precedence:
/// 1. `RUSTY_GRID_ACL_GRANT_LOG` (when set and non-empty),
/// 2. `<OXIDESWARM_HOME>/windows_acl_grants.txt` (when `OXIDESWARM_HOME` is set and non-empty),
/// 3. `<user_home>/.oxideswarm/windows_acl_grants.txt`.
pub fn acl_grant_manifest_path(host: &HostDirs) -> PathBuf {
    acl_grant_manifest_path_with(
        host,
        std::env::var_os("RUSTY_GRID_ACL_GRANT_LOG").as_deref(),
        std::env::var_os("OXIDESWARM_HOME").as_deref(),
    )
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
#[allow(dead_code)]
pub(crate) fn wrap_program(
    jail: &Jail<'_>,
    program: &str,
    args: &[String],
    search_path: &str,
) -> Result<(String, Vec<String>), String> {
    wrap_program_in_dir(jail, jail.sandbox_dir, program, args, search_path)
}

/// Like [`wrap_program`], resolving relative Windows task executables against `working_dir`
/// inside `jail.sandbox_dir`.
pub(crate) fn wrap_program_in_dir(
    jail: &Jail<'_>,
    working_dir: &Path,
    program: &str,
    args: &[String],
    search_path: &str,
) -> Result<(String, Vec<String>), String> {
    debug_assert!(jail.level != IsolationLevel::Off);
    #[cfg(target_os = "macos")]
    {
        let _ = working_dir;
        let resolved = resolve_program(program, search_path)
            .ok_or_else(|| format!("program '{program}' was not found in PATH"))?;
        macos::wrap(jail, &resolved, args)
    }
    #[cfg(target_os = "linux")]
    {
        let _ = (jail, working_dir);
        let resolved = resolve_program(program, search_path)
            .ok_or_else(|| format!("program '{program}' was not found in PATH"))?;
        Ok((resolved, args.to_vec()))
    }
    #[cfg(windows)]
    {
        let resolved =
            windows::resolve_program(program, search_path, working_dir, jail.sandbox_dir)?
                .ok_or_else(|| format!("program '{program}' was not found in PATH"))?;
        Ok((resolved, args.to_vec()))
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    {
        let _ = (jail, working_dir, program, args, search_path);
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
    validate_extra_read_only_dirs(
        &host.extra_read_only_dirs,
        host,
        Some(sandbox_base),
        None,
        protected_files,
    )
    .map_err(GridError::Config)?;
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

#[cfg(not(windows))]
fn lexically_clean_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn normalize_policy_path(p: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        windows::normalize_win_path(p)
    }
    #[cfg(not(windows))]
    {
        let clean = lexically_clean_path(p);
        if let Ok(canon) = std::fs::canonicalize(&clean) {
            return canon;
        }
        let mut tail: Vec<std::ffi::OsString> = Vec::new();
        let mut cur: &Path = &clean;
        while let Some(parent) = cur.parent() {
            if let Some(name) = cur.file_name() {
                tail.push(name.to_os_string());
            }
            if !parent.as_os_str().is_empty() {
                if let Ok(canon_parent) = std::fs::canonicalize(parent) {
                    let mut rebuilt = canon_parent;
                    for part in tail.into_iter().rev() {
                        rebuilt.push(part);
                    }
                    return lexically_clean_path(&rebuilt);
                }
            }
            if parent == cur {
                break;
            }
            cur = parent;
        }
        clean
    }
}

fn policy_path_starts_with(child: &Path, parent: &Path) -> bool {
    #[cfg(windows)]
    {
        windows::path_starts_with_ci(child, parent)
    }
    #[cfg(not(windows))]
    {
        if child.as_os_str().is_empty() || parent.as_os_str().is_empty() {
            return false;
        }
        normalize_policy_path(child).starts_with(normalize_policy_path(parent))
    }
}

fn is_symlink_or_reparse(path: &Path) -> bool {
    #[cfg(windows)]
    {
        std::fs::symlink_metadata(path)
            .map(|m| m.file_type().is_symlink() || windows::is_reparse_point(path))
            .unwrap_or(false)
    }
    #[cfg(not(windows))]
    {
        std::fs::symlink_metadata(path)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
    }
}

/// Validates `--isolation-read-dir` / `HostDirs::extra_read_only_dirs` entries across all
/// platforms (`design 0074`). Allows narrow runtime subdirectories (for example
/// `~/.pyenv/versions/3.11.8` or `%LOCALAPPDATA%\Programs\Python\Python311`) while refusing
/// symlinks/junctions, broad home/system/temp containers, credential stores, OxideSwarm state/key
/// directories, `cargo_home`, `sandbox_base`, `workspace_root`, and `protected_files`.
pub fn validate_extra_read_only_dirs(
    dirs: &[PathBuf],
    host: &HostDirs,
    sandbox_base: Option<&Path>,
    workspace_root: Option<&Path>,
    protected_files: &[PathBuf],
) -> Result<(), String> {
    if dirs.is_empty() {
        return Ok(());
    }

    let mut container_roots: Vec<(&'static str, PathBuf)> = Vec::new();
    let mut push_container = |label: &'static str, path: PathBuf| {
        if !path.as_os_str().is_empty() && path.is_absolute() {
            container_roots.push((label, path));
        }
    };

    if !host.user_home.as_os_str().is_empty() {
        push_container("user home", host.user_home.clone());
        for (label, rel) in [
            ("Desktop", "Desktop"),
            ("Documents", "Documents"),
            ("Downloads", "Downloads"),
            ("Pictures", "Pictures"),
            ("Music", "Music"),
            ("Videos", "Videos"),
            ("Movies", "Movies"),
            ("Public", "Public"),
            ("Library", "Library"),
            ("Library/Application Support", "Library/Application Support"),
            ("Library/Caches", "Library/Caches"),
            ("Library/Preferences", "Library/Preferences"),
            (".config", ".config"),
            (".local", ".local"),
            (".local/share", ".local/share"),
            (".cache", ".cache"),
            ("AppData", "AppData"),
            ("AppData/Local", "AppData/Local"),
            ("AppData/Local/Programs", "AppData/Local/Programs"),
            ("AppData/Roaming", "AppData/Roaming"),
            ("AppData/LocalLow", "AppData/LocalLow"),
            ("AppData/Local/Temp", "AppData/Local/Temp"),
        ] {
            push_container(label, host.user_home.join(rel));
        }
    }

    for (label, env_var) in [
        ("USERPROFILE", "USERPROFILE"),
        ("HOME", "HOME"),
        ("LOCALAPPDATA", "LOCALAPPDATA"),
        ("APPDATA", "APPDATA"),
        ("PUBLIC", "PUBLIC"),
        ("ProgramData", "ProgramData"),
        ("ProgramFiles", "ProgramFiles"),
        ("ProgramFiles(x86)", "ProgramFiles(x86)"),
        ("SystemRoot", "SystemRoot"),
        ("XDG_CONFIG_HOME", "XDG_CONFIG_HOME"),
        ("XDG_DATA_HOME", "XDG_DATA_HOME"),
        ("XDG_CACHE_HOME", "XDG_CACHE_HOME"),
        ("TEMP", "TEMP"),
        ("TMP", "TMP"),
        ("TMPDIR", "TMPDIR"),
    ] {
        if let Some(val) = std::env::var_os(env_var).filter(|v| !v.is_empty()) {
            push_container(label, PathBuf::from(val));
        }
    }
    push_container("temp directory", std::env::temp_dir());
    #[cfg(target_os = "macos")]
    {
        if let Some(h) = macos::account_home() {
            push_container("account home", h);
        }
        if let Some(t) = macos::user_temp_dir() {
            push_container("user temp directory", t);
        }
        if let Some(c) = macos::user_cache_dir() {
            push_container("user cache directory", c);
        }
    }
    #[cfg(not(windows))]
    {
        for (label, path) in [
            ("/tmp", "/tmp"),
            ("/private/tmp", "/private/tmp"),
            ("/var/tmp", "/var/tmp"),
            ("/private/var/tmp", "/private/var/tmp"),
            ("/Users/Shared", "/Users/Shared"),
            ("/Volumes", "/Volumes"),
            ("/Users", "/Users"),
            ("/home", "/home"),
            ("/opt", "/opt"),
            ("/etc", "/etc"),
            ("/var", "/var"),
        ] {
            push_container(label, PathBuf::from(path));
        }
    }

    let mut forbidden_subtrees: Vec<(&'static str, PathBuf)> = Vec::new();
    let mut push_forbidden = |label: &'static str, path: PathBuf| {
        if !path.as_os_str().is_empty() && path.is_absolute() {
            forbidden_subtrees.push((label, path));
        }
    };

    push_forbidden("CARGO_HOME", host.cargo_home.clone());
    if let Some(ox_home) = std::env::var_os("OXIDESWARM_HOME").filter(|v| !v.is_empty()) {
        push_forbidden("OXIDESWARM_HOME", PathBuf::from(ox_home));
    }
    let mut homes_to_protect = Vec::new();
    if !host.user_home.as_os_str().is_empty() {
        homes_to_protect.push(host.user_home.clone());
    }
    for env_home in ["HOME", "USERPROFILE"] {
        if let Some(val) = std::env::var_os(env_home).filter(|v| !v.is_empty()) {
            homes_to_protect.push(PathBuf::from(val));
        }
    }
    for home in homes_to_protect {
        for (label, rel) in [
            (".oxideswarm", ".oxideswarm"),
            (".ssh", ".ssh"),
            (".gnupg", ".gnupg"),
            (".aws", ".aws"),
            (".azure", ".azure"),
            (".kube", ".kube"),
            (".docker", ".docker"),
            (".pki", ".pki"),
            (".password-store", ".password-store"),
            (".git-credentials", ".git-credentials"),
            (".netrc", ".netrc"),
            (".npmrc", ".npmrc"),
            (".pypirc", ".pypirc"),
            (".config/gcloud", ".config/gcloud"),
            (".config/gh", ".config/gh"),
            (".config/op", ".config/op"),
            ("Library/Keychains", "Library/Keychains"),
            (
                "AppData/Roaming/Microsoft/Credentials",
                "AppData/Roaming/Microsoft/Credentials",
            ),
            (
                "AppData/Local/Microsoft/Credentials",
                "AppData/Local/Microsoft/Credentials",
            ),
            (
                "AppData/Roaming/Microsoft/Protect",
                "AppData/Roaming/Microsoft/Protect",
            ),
            (
                "AppData/Local/Google/Cloud SDK",
                "AppData/Local/Google/Cloud SDK",
            ),
        ] {
            push_forbidden(label, home.join(rel));
        }
    }
    if let Some(sb) = sandbox_base {
        push_forbidden("sandbox_base", sb.to_path_buf());
    }
    if let Some(wr) = workspace_root {
        push_forbidden("workspace_root", wr.to_path_buf());
    }
    for pf in protected_files {
        push_forbidden("protected file", pf.clone());
    }

    for raw_dir in dirs {
        if raw_dir.as_os_str().is_empty() {
            return Err(
                "refusing empty --isolation-read-dir entry; omit it or pass an absolute runtime directory"
                    .to_string(),
            );
        }
        if !raw_dir.is_absolute() {
            return Err(format!(
                "refusing relative --isolation-read-dir path '{}'; pass an absolute directory path",
                raw_dir.display()
            ));
        }
        if raw_dir
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(format!(
                "refusing --isolation-read-dir '{}' containing '..' traversal components",
                raw_dir.display()
            ));
        }
        let meta = std::fs::symlink_metadata(raw_dir).map_err(|e| {
            format!(
                "refusing --isolation-read-dir '{}': directory is inaccessible ({e})",
                raw_dir.display()
            )
        })?;
        if is_symlink_or_reparse(raw_dir) {
            return Err(format!(
                "refusing --isolation-read-dir '{}': symlinks and NTFS junctions are not allowed",
                raw_dir.display()
            ));
        }
        if !meta.is_dir() {
            return Err(format!(
                "refusing --isolation-read-dir '{}': path must be an existing directory, not a file",
                raw_dir.display()
            ));
        }

        if !host.user_home.as_os_str().is_empty() {
            for anc in raw_dir.ancestors().skip(1) {
                if anc.as_os_str().is_empty() || anc.parent().is_none() {
                    break;
                }
                if anc
                    .parent()
                    .is_some_and(|p| policy_path_starts_with(p, &host.user_home))
                    && is_symlink_or_reparse(anc)
                {
                    return Err(format!(
                        "refusing --isolation-read-dir '{}': parent directory '{}' inside home is a symlink or junction",
                        raw_dir.display(),
                        anc.display()
                    ));
                }
                if policy_path_starts_with(&host.user_home, anc) {
                    break;
                }
            }
        }

        let canon_dir = normalize_policy_path(raw_dir);
        if canon_dir.parent().is_none() {
            return Err(format!(
                "refusing broad filesystem root '{}' as --isolation-read-dir",
                raw_dir.display()
            ));
        }
        if !cfg!(windows) && canon_dir.components().count() <= 2 {
            return Err(format!(
                "refusing broad top-level directory '{}' (resolved '{}') as --isolation-read-dir; specify a narrower runtime subdirectory",
                raw_dir.display(),
                canon_dir.display()
            ));
        }
        for (label, container) in &container_roots {
            if policy_path_starts_with(container, &canon_dir) {
                return Err(format!(
                    "refusing broad {label} container '{}' as --isolation-read-dir; specify a narrow runtime subdirectory such as '%LOCALAPPDATA%\\Programs\\Python\\Python311' or '~/.pyenv/versions/3.11.8'",
                    raw_dir.display()
                ));
            }
        }
        for (label, forbidden) in &forbidden_subtrees {
            if policy_path_starts_with(&canon_dir, forbidden)
                || policy_path_starts_with(forbidden, &canon_dir)
            {
                return Err(format!(
                    "refusing --isolation-read-dir '{}' because it overlaps protected {label} path '{}'",
                    raw_dir.display(),
                    forbidden.display()
                ));
            }
        }
    }

    Ok(())
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
    validate_extra_read_only_dirs(
        &jail.host.extra_read_only_dirs,
        jail.host,
        Some(jail.sandbox_base),
        jail.workspace_root,
        jail.protected_files,
    )?;
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
mod macos;

/// How an AppContainer child starts on Windows (SEC-11, design 0078).
///
/// An elevated worker starts the child from a restricted (LUA) token through
/// `CreateProcessAsUserW`. It never falls back to `CreateProcessW`, because that call derives the
/// child token from the worker's own elevated token. A worker that is not elevated has no LUA
/// token and uses `CreateProcessW`.
#[cfg_attr(not(windows), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenSpawnPlan {
    /// Try `CreateProcessAsUserW` with the LUA token.
    Lua,
    /// Use `CreateProcessW` (the worker is not elevated).
    Plain,
    /// The worker is elevated and has no LUA token: refuse.
    RefuseNoLuaToken,
}

/// Picks the spawn plan from whether the worker is elevated and whether a LUA token exists.
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn plan_token_spawn(elevated: bool, lua_token_available: bool) -> TokenSpawnPlan {
    match (elevated, lua_token_available) {
        (_, true) => TokenSpawnPlan::Lua,
        (true, false) => TokenSpawnPlan::RefuseNoLuaToken,
        (false, false) => TokenSpawnPlan::Plain,
    }
}

/// Message of the refusal. `cause` names what failed, `win32_error` is the last Win32 error
/// (`None` when no call failed, such as a missing LUA token).
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn elevated_token_refusal_message(cause: &str, win32_error: Option<u32>) -> String {
    match win32_error {
        Some(code) => format!(
            "refusing to start the task with the worker's elevated token: {cause} (Win32 error {code})"
        ),
        None => format!("refusing to start the task with the worker's elevated token: {cause}"),
    }
}

/// Decides what to do after `CreateProcessAsUserW` failed with `win32_error`: an elevated worker
/// refuses (returns the message), a worker that is not elevated may use `CreateProcessW`.
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn after_lua_spawn_failure(elevated: bool, win32_error: u32) -> Option<String> {
    elevated.then(|| {
        elevated_token_refusal_message(
            "CreateProcessAsUserW with the restricted (LUA) token failed",
            Some(win32_error),
        )
    })
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
pub mod windows;

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
        let fs = macos::profile(IsolationLevel::Fs, false, 2, 1, 1, 1);
        let at = |needle: &str| {
            fs.find(needle)
                .unwrap_or_else(|| panic!("{needle} missing in\n{fs}"))
        };
        // Last match wins: broad deny, then the allows (including EXTRA_READ_0), then credentials
        // and protected files deny.
        let deny_home = at("(deny file-read*\n    (subpath (param \"USER_HOME\"))");
        let allow_tools = at("(allow file-read*\n    (subpath (param \"SANDBOX_DIR\"))");
        let allow_extra_read = at("(subpath (param \"EXTRA_READ_0\"))");
        let deny_cred = at("(deny file-read*\n    (literal (param \"CARGO_CRED\"))");
        let deny_protected = at("(deny file-read*\n    (literal (param \"PROTECTED_0\")))");
        assert!(
            deny_home < allow_tools
                && allow_tools < allow_extra_read
                && allow_extra_read < deny_cred
                && deny_cred < deny_protected
        );
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
        let ws = macos::profile(IsolationLevel::Fs, true, 1, 1, 0, 0);
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
        // SEC-12: Mach lookups are denied except the Metal compiler, job creation is denied, and
        // the pasteboard deny comes after the allow (last match wins).
        let deny_all = at("(deny mach-lookup)\n");
        let allow_mtl = at("(global-name \"com.apple.MTLCompilerService\")");
        let pasteboard = at("(deny mach-lookup (global-name-prefix \"com.apple.pasteboard\"))");
        assert!(deny_all < allow_mtl && allow_mtl < pasteboard);
        assert!(fs.contains("(deny job-creation)\n"));
        let net = macos::profile(IsolationLevel::FsNet, false, 0, 0, 0, 0);
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
        )
        .with_extra_read_only_dirs(vec![PathBuf::from(
            "/nonexistent-home/.pyenv/versions/3.11.8",
        )]);
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
        assert!(args.contains(&"EXTRA_READ_0=/nonexistent-home/.pyenv/versions/3.11.8".to_string()));
        assert!(
            args.iter()
                .any(|a| a.ends_with("=/nonexistent-home/.pyenv/versions") && a.starts_with("ANCESTOR_")),
            "parent components under USER_HOME must receive ANCESTOR_* metadata traversal: {args:?}"
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

        let docs_sub = docs.join("sub");
        std::fs::create_dir_all(&docs_sub).unwrap();
        let err_docs_sub = windows::validate_sandbox_base_safe(&docs_sub, &host)
            .expect_err("must reject <user_home>\\Documents\\sub as sandbox_base");
        assert!(err_docs_sub.contains("user folder"), "{err_docs_sub}");
        let err_ws_docs_sub = windows::validate_workspace_root_safe(&docs_sub, &host)
            .expect_err("must reject <user_home>\\Documents\\sub as workspace_root");
        assert!(err_ws_docs_sub.contains("user folder"), "{err_ws_docs_sub}");

        let local_ox_sandboxes = fake_home
            .path()
            .join("AppData")
            .join("Local")
            .join("OxideSwarm")
            .join("sandboxes");
        std::fs::create_dir_all(&local_ox_sandboxes).unwrap();
        windows::validate_sandbox_base_safe(&local_ox_sandboxes, &host)
            .expect("<user_home>\\AppData\\Local\\OxideSwarm\\sandboxes must be accepted");

        let ox_sandboxes = fake_home.path().join(".oxideswarm").join("sandboxes");
        std::fs::create_dir_all(&ox_sandboxes).unwrap();
        windows::validate_sandbox_base_safe(&ox_sandboxes, &host)
            .expect("<user_home>\\.oxideswarm\\sandboxes must be accepted");

        let home_temp_root = fake_home.path().join("AppData").join("Local").join("Temp");
        std::fs::create_dir_all(&home_temp_root).unwrap();
        let err_temp_root = windows::validate_sandbox_base_safe(&home_temp_root, &host)
            .expect_err("must reject <user_home>\\AppData\\Local\\Temp itself");
        assert!(err_temp_root.contains("user folder"), "{err_temp_root}");

        let default_temp_sandboxes = home_temp_root.join("rusty_grid").join("sandboxes");
        std::fs::create_dir_all(&default_temp_sandboxes).unwrap();
        windows::validate_sandbox_base_safe(&default_temp_sandboxes, &host).expect(
            "default <user_home>\\AppData\\Local\\Temp\\rusty_grid\\sandboxes must be accepted",
        );

        let outside = tempfile::tempdir().unwrap();
        let non_empty = outside.path().join("operator_folder");
        std::fs::create_dir_all(&non_empty).unwrap();
        std::fs::write(non_empty.join("important.docx"), "data").unwrap();
        let err_non_empty = windows::validate_sandbox_base_safe(&non_empty, &host)
            .expect_err("must reject existing directory containing non-sandbox files");
        assert!(err_non_empty.contains("important.docx"), "{err_non_empty}");
    }

    #[cfg(windows)]
    #[test]
    fn test_bug32_usb_kit_workspace_root_under_localappdata_is_accepted() {
        let fake_profile = tempfile::tempdir().unwrap();
        let install_root = fake_profile
            .path()
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB");
        let ws_root = install_root.join("workspaces");
        std::fs::create_dir_all(&ws_root).unwrap();
        std::fs::write(
            install_root.join(".oxideswarm-usb-install"),
            "Created by an OxideSwarm USB kit install.\n",
        )
        .unwrap();
        std::fs::write(
            ws_root.join(".oxideswarm-usb-workspaces"),
            "Created by an OxideSwarm USB kit install.\n",
        )
        .unwrap();

        let host = HostDirs::from_home(fake_profile.path().to_path_buf(), None, None);
        windows::validate_workspace_root_safe(&ws_root, &host)
            .expect("BUG-32: USB kit <install>\\workspaces under %LOCALAPPDATA% must be accepted");
    }

    #[cfg(windows)]
    #[test]
    fn test_bug32_usb_kit_workspace_marker_file_is_accepted_in_workspace_root() {
        let fake_profile = tempfile::tempdir().unwrap();
        let temp_ws_root = fake_profile
            .path()
            .join("AppData")
            .join("Local")
            .join("Temp")
            .join("oxideswarm-usb")
            .join("workspaces");
        std::fs::create_dir_all(&temp_ws_root).unwrap();

        let host = HostDirs::from_home(fake_profile.path().to_path_buf(), None, None);
        windows::validate_workspace_root_safe(&temp_ws_root, &host)
            .expect("empty %TEMP%\\oxideswarm-usb\\workspaces is accepted");

        std::fs::write(
            temp_ws_root.join(".oxideswarm-usb-workspaces"),
            "Created by an OxideSwarm USB kit install.\n",
        )
        .unwrap();
        windows::validate_workspace_root_safe(&temp_ws_root, &host).expect(
            "BUG-32: USB kit .oxideswarm-usb-workspaces marker inside workspace root must be accepted",
        );

        let legacy_usb_ws = fake_profile
            .path()
            .join("AppData")
            .join("Local")
            .join("OxideSwarm")
            .join("usb-kit")
            .join("workspaces");
        std::fs::create_dir_all(&legacy_usb_ws).unwrap();
        std::fs::write(
            legacy_usb_ws.join(".oxideswarm-usb-workspaces"),
            "Created by an OxideSwarm USB kit install.\n",
        )
        .unwrap();
        windows::validate_workspace_root_safe(&legacy_usb_ws, &host).expect(
            "BUG-32: legacy USB kit AppData\\Local\\OxideSwarm\\usb-kit\\workspaces must be accepted",
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_bug32_refuses_usb_kit_install_root_and_sibling_under_localappdata() {
        let fake_profile = tempfile::tempdir().unwrap();
        let usb_root = fake_profile
            .path()
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB");
        let usb_ws = usb_root.join("workspaces");
        let usb_sibling = usb_root.join("other");
        let usb_evil = usb_root.join("workspaces-evil");
        let legacy_usb_root = fake_profile
            .path()
            .join("AppData")
            .join("Local")
            .join("OxideSwarm")
            .join("usb-kit");
        let legacy_usb_ws = legacy_usb_root.join("workspaces");
        let legacy_usb_sibling = legacy_usb_root.join("other");
        let legacy_usb_evil = legacy_usb_root.join("workspaces-evil");
        for dir in [
            &usb_ws,
            &usb_sibling,
            &usb_evil,
            &legacy_usb_ws,
            &legacy_usb_sibling,
            &legacy_usb_evil,
        ] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let usb_dotdot = usb_ws.join("..").join("other");
        let legacy_dotdot = legacy_usb_ws.join("..").join("other");

        let host = HostDirs::from_home(fake_profile.path().to_path_buf(), None, None);
        for bad in [
            &usb_root,
            &usb_sibling,
            &usb_evil,
            &usb_dotdot,
            &legacy_usb_root,
            &legacy_usb_sibling,
            &legacy_usb_evil,
            &legacy_dotdot,
        ] {
            let err = windows::validate_workspace_root_safe(bad, &host).expect_err(&format!(
                "must refuse non-allowlisted user-profile folder {}",
                bad.display()
            ));
            assert!(
                err.contains("refusing to rewrite the DACL of user folder"),
                "{err}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn test_bug32_refuses_usb_workspaces_marker_as_directory() {
        let fake_profile = tempfile::tempdir().unwrap();
        let ws_root = fake_profile
            .path()
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB")
            .join("workspaces");
        std::fs::create_dir_all(ws_root.join(".oxideswarm-usb-workspaces")).unwrap();

        let host = HostDirs::from_home(fake_profile.path().to_path_buf(), None, None);
        let err = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("must refuse .oxideswarm-usb-workspaces when it is a directory");
        assert!(err.contains(".oxideswarm-usb-workspaces"), "{err}");
    }

    #[cfg(windows)]
    fn require_appcontainer_from_env() -> bool {
        std::env::var_os("RUSTY_GRID_REQUIRE_APPCONTAINER")
            .is_some_and(|v| !v.is_empty() && v != std::ffi::OsStr::new("0"))
    }

    #[cfg(windows)]
    fn handle_windows_isolation_skip(test_name: &str, why: &str, require: bool) {
        assert!(
            !require,
            "RUSTY_GRID_REQUIRE_APPCONTAINER=1 but {test_name} cannot run: {why}"
        );
        eprintln!(
            "skipping {test_name}: {why} (set RUSTY_GRID_REQUIRE_APPCONTAINER=1 to fail instead)"
        );
    }

    #[cfg(windows)]
    fn fail_or_skip_windows_isolation_test(test_name: &str, why: &str) {
        handle_windows_isolation_skip(test_name, why, require_appcontainer_from_env());
    }

    #[cfg(windows)]
    fn mklink_j(link: &Path, dest: &Path) -> Result<(), String> {
        if let Some(parent) = link.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("create parent: {e}"))?;
        }
        let out = std::process::Command::new("cmd")
            .args([
                "/C",
                "mklink",
                "/J",
                &link.to_string_lossy(),
                &dest.to_string_lossy(),
            ])
            .output()
            .map_err(|e| format!("launch mklink /J: {e}"))?;
        if out.status.success() {
            Ok(())
        } else {
            Err(format!(
                "status={} stderr={}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        }
    }

    #[cfg(windows)]
    struct HookResetGuard;

    #[cfg(windows)]
    impl Drop for HookResetGuard {
        fn drop(&mut self) {
            windows::ws_root_recheck_test_hook::clear_all();
        }
    }

    #[cfg(windows)]
    #[test]
    fn test_bug32_refuses_usb_workspaces_marker_as_symlink_or_junction() {
        let fake_profile = tempfile::tempdir().unwrap();
        let ws_root = fake_profile
            .path()
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB")
            .join("workspaces");
        let target_dir = fake_profile.path().join("outside_target_dir");
        let target_file = fake_profile.path().join("outside_target_file.txt");
        std::fs::create_dir_all(&ws_root).unwrap();
        std::fs::create_dir_all(&target_dir).unwrap();
        std::fs::write(&target_file, "target\n").unwrap();

        let host = HostDirs::from_home(fake_profile.path().to_path_buf(), None, None);
        let marker = ws_root.join(".oxideswarm-usb-workspaces");
        let mut tested_any = false;

        match mklink_j(&marker, &target_dir) {
            Ok(()) => {
                tested_any = true;
                let err = windows::validate_workspace_root_safe(&ws_root, &host)
                    .expect_err("must refuse .oxideswarm-usb-workspaces when it is a junction");
                assert!(err.contains(".oxideswarm-usb-workspaces"), "{err}");
                std::fs::remove_dir(&marker).unwrap();
            }
            Err(reason) => {
                eprintln!("note: mklink /J unavailable ({reason})");
            }
        }

        if crate::sandbox::try_create_test_symlink_file(&target_file, &marker) {
            tested_any = true;
            let err = windows::validate_workspace_root_safe(&ws_root, &host)
                .expect_err("must refuse .oxideswarm-usb-workspaces when it is a file symlink");
            assert!(err.contains(".oxideswarm-usb-workspaces"), "{err}");
            std::fs::remove_file(&marker).unwrap();
        }

        if !tested_any {
            fail_or_skip_windows_isolation_test(
                "test_bug32_refuses_usb_workspaces_marker_as_symlink_or_junction",
                "host cannot create junctions or file symlinks",
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn test_bug32_refuses_uppercase_usb_workspaces_marker_name() {
        // `install.ps1` always writes exact lowercase `.oxideswarm-usb-workspaces`, and
        // `validate_workspace_root_safe` checks reserved metadata names (`store.lock`, `.locks`,
        // `.oxideswarm-usb-workspaces`) case-sensitively against `read_dir` entries; therefore
        // `.OXIDESWARM-USB-WORKSPACES` is expected to be refused.
        let fake_profile = tempfile::tempdir().unwrap();
        let ws_root = fake_profile
            .path()
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB")
            .join("workspaces");
        std::fs::create_dir_all(&ws_root).unwrap();
        std::fs::write(
            ws_root.join(".OXIDESWARM-USB-WORKSPACES"),
            "Created by an OxideSwarm USB kit install.\n",
        )
        .unwrap();

        let host = HostDirs::from_home(fake_profile.path().to_path_buf(), None, None);
        let err = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("must refuse upper-case .OXIDESWARM-USB-WORKSPACES marker name");
        assert!(err.contains(".OXIDESWARM-USB-WORKSPACES"), "{err}");
    }

    #[cfg(windows)]
    #[test]
    fn test_sec17_refuses_reparse_point_in_ancestor_from_home_and_leaves_target_dacl_untouched() {
        let root = tempfile::tempdir().unwrap();
        let root_path = windows::normalize_win_path(root.path());
        let fake_profile = root_path.join("profile");
        let victim = root_path.join("victim");
        let victim_ws = victim.join("workspaces");
        let victim_temp_ws = victim.join("ox-test").join("workspaces");
        let victim_local = root_path.join("victim_local");
        let victim_local_usb_ws = victim_local.join("OxideSwarm-USB").join("workspaces");
        let victim_local_ox_ws = victim_local.join("oxideswarm").join("workspaces");
        for dir in [
            &fake_profile,
            &victim_ws,
            &victim_temp_ws,
            &victim_local_usb_ws,
            &victim_local_ox_ws,
        ] {
            std::fs::create_dir_all(dir).unwrap();
        }

        let orig_victim_sddl =
            windows::get_path_dacl_sddl(&victim_ws).expect("read original DACL SDDL of victim_ws");

        let host = HostDirs::from_home(
            fake_profile.clone(),
            Some(PathBuf::new()),
            Some(PathBuf::new()),
        );

        // 1. BUG-32 USB-kit ancestor junction: AppData\Local\OxideSwarm-USB -> victim.
        let usb_root = fake_profile
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB");
        if let Err(reason) = mklink_j(&usb_root, &victim) {
            fail_or_skip_windows_isolation_test(
                "test_sec17_refuses_reparse_point_in_ancestor_from_home_and_leaves_target_dacl_untouched",
                &format!("mklink /J unavailable ({reason})"),
            );
            return;
        }
        let usb_ws = usb_root.join("workspaces");
        let err = windows::validate_workspace_root_safe(&usb_ws, &host)
            .expect_err("SEC-17: junction at AppData\\Local\\OxideSwarm-USB must be refused");
        assert!(
            err.contains("reparse point") || err.contains("junction"),
            "{err}"
        );

        // Also verify WindowsAppContainerGuard::create refuses with the same message before any
        // DACL rewrite occurs.
        let sb_base = root_path.join("sandboxes");
        let sb_dir = sb_base.join(".isolation-probe-sec17");
        std::fs::create_dir_all(&sb_dir).unwrap();
        let jail = Jail {
            level: IsolationLevel::Fs,
            host: &host,
            protected_files: &[],
            sandbox_base: &sb_base,
            sandbox_dir: &sb_dir,
            extra_writable_dir: None,
            workspace_root: Some(&usb_ws),
        };
        let comspec =
            std::env::var("COMSPEC").unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".to_string());
        let mut cmd = tokio::process::Command::new(&comspec);
        cmd.args(["/C", "echo ok"]);
        cmd.current_dir(&sb_dir);
        let guard_err =
            match windows::WindowsAppContainerGuard::create(uuid::Uuid::new_v4(), &jail, &cmd) {
                Ok(_) => panic!("AppContainer guard must refuse ancestor junction workspace root"),
                Err(e) => e,
            };
        assert_eq!(guard_err.kind(), std::io::ErrorKind::PermissionDenied);
        let guard_msg = guard_err.to_string();
        assert!(
            guard_msg.contains("reparse point")
                && guard_msg.contains("OxideSwarm-USB")
                && guard_msg.contains("--workspace-root"),
            "unexpected guard refusal text: {guard_msg}"
        );
        assert_eq!(
            windows::get_path_dacl_sddl(&victim_ws).unwrap(),
            orig_victim_sddl,
            "target DACL behind ancestor junction must stay untouched"
        );
        std::fs::remove_dir(&usb_root).unwrap();

        // 2. Legacy USB-kit and pre-BUG-32 allowed ancestors inside user_home.
        let ancestor_cases: Vec<(PathBuf, PathBuf, &str)> = vec![
            (
                fake_profile
                    .join("AppData")
                    .join("Local")
                    .join("OxideSwarm")
                    .join("usb-kit"),
                fake_profile
                    .join("AppData")
                    .join("Local")
                    .join("OxideSwarm")
                    .join("usb-kit")
                    .join("workspaces"),
                "legacy BUG-32 usb-kit",
            ),
            (
                fake_profile
                    .join("AppData")
                    .join("Local")
                    .join("oxideswarm"),
                fake_profile
                    .join("AppData")
                    .join("Local")
                    .join("oxideswarm")
                    .join("workspaces"),
                "pre-BUG-32 AppData\\Local\\oxideswarm",
            ),
            (
                fake_profile.join(".oxideswarm"),
                fake_profile.join(".oxideswarm").join("workspaces"),
                "pre-BUG-32 .oxideswarm",
            ),
            (
                fake_profile.join(".cache").join("oxideswarm"),
                fake_profile
                    .join(".cache")
                    .join("oxideswarm")
                    .join("workspaces"),
                "pre-BUG-32 .cache\\oxideswarm",
            ),
            (
                fake_profile.join("AppData").join("Local").join("Temp"),
                fake_profile
                    .join("AppData")
                    .join("Local")
                    .join("Temp")
                    .join("ox-test")
                    .join("workspaces"),
                "pre-BUG-32 AppData\\Local\\Temp",
            ),
        ];
        for (link_anc, ws_path, label) in ancestor_cases {
            mklink_j(&link_anc, &victim)
                .unwrap_or_else(|e| panic!("mklink /J failed for {label}: {e}"));
            let err = windows::validate_workspace_root_safe(&ws_path, &host)
                .expect_err(&format!("SEC-17: junction at {label} must be refused"));
            assert!(
                err.contains("reparse point") || err.contains("junction"),
                "{label}: {err}"
            );
            std::fs::remove_dir(&link_anc).unwrap();
            let _ = std::fs::remove_dir_all(fake_profile.join("AppData"));
        }

        // 3. Intermediate ancestor junction at AppData\Local covering both BUG-32 and pre-BUG-32.
        let appdata_local = fake_profile.join("AppData").join("Local");
        let _ = std::fs::remove_dir_all(&appdata_local);
        mklink_j(&appdata_local, &victim_local).expect("mklink /J at AppData\\Local");
        for sub in ["OxideSwarm-USB", "oxideswarm"] {
            let ws_path = appdata_local.join(sub).join("workspaces");
            let err = windows::validate_workspace_root_safe(&ws_path, &host).expect_err(&format!(
                "SEC-17: intermediate AppData\\Local junction for {sub} must be refused"
            ));
            assert!(
                err.contains("reparse point") || err.contains("junction"),
                "{sub}: {err}"
            );
            assert!(
                err.contains("--workspace-root") && err.contains("target"),
                "{sub} refusal must document --workspace-root target workaround: {err}"
            );
        }
        std::fs::remove_dir(&appdata_local).unwrap();
        // Workaround: pointing --workspace-root at the junction's target directly is accepted.
        windows::validate_workspace_root_safe(&victim_local_ox_ws, &host)
            .expect("pointing --workspace-root at AppData\\Local junction target must succeed");

        // 3b. Home folder itself is a junction (e.g. C:\Users\me -> D:\Users\me):
        //     default root under the junction home is refused, while --workspace-root on the target works.
        let real_home_target = root_path.join("real_home_target");
        let real_home_ws = real_home_target
            .join("AppData")
            .join("Local")
            .join("oxideswarm")
            .join("workspaces");
        std::fs::create_dir_all(&real_home_ws).unwrap();
        let junc_home = root_path.join("junc_home");
        mklink_j(&junc_home, &real_home_target).expect("mklink /J for junc_home");
        let junc_home_host = HostDirs::from_home(
            junc_home.clone(),
            Some(PathBuf::new()),
            Some(PathBuf::new()),
        );
        let junc_home_ws = junc_home
            .join("AppData")
            .join("Local")
            .join("oxideswarm")
            .join("workspaces");
        let err_junc_home = windows::validate_workspace_root_safe(&junc_home_ws, &junc_home_host)
            .expect_err("SEC-17: user_home that is itself a junction must be refused");
        assert!(err_junc_home.contains("reparse point"), "{err_junc_home}");
        assert!(
            err_junc_home.contains("--workspace-root") && err_junc_home.contains("target"),
            "junction home refusal must document --workspace-root target workaround: {err_junc_home}"
        );
        windows::validate_workspace_root_safe(&real_home_ws, &junc_home_host)
            .expect("pointing --workspace-root at the junction home's target must succeed");
        std::fs::remove_dir(&junc_home).unwrap();

        // 4. External junction outside user_home targeting an allowed folder inside user_home.
        let real_usb_parent = fake_profile
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB");
        std::fs::create_dir_all(real_usb_parent.join("workspaces")).unwrap();
        let outside_junc = root_path.join("outside_junc");
        mklink_j(&outside_junc, &real_usb_parent).expect("mklink /J outside -> inside");
        let via_outside = outside_junc.join("workspaces");
        let err_ext = windows::validate_workspace_root_safe(&via_outside, &host)
            .expect_err("SEC-17: external junction pointing into user_home must be refused");
        assert!(
            err_ext.contains("reparse point") || err_ext.contains("junction"),
            "{err_ext}"
        );
        std::fs::remove_dir(&outside_junc).unwrap();

        // 4b. External junction above home (e.g. C:\x -> C:\Users with root C:\x\me\AppData\Local\oxideswarm\workspaces):
        //     walking the raw path ancestors above home must still refuse C:\x.
        let fake_users = root_path.join("fake_users");
        let home_under_users = fake_users.join("me");
        let ws_under_users = home_under_users
            .join("AppData")
            .join("Local")
            .join("oxideswarm")
            .join("workspaces");
        std::fs::create_dir_all(&ws_under_users).unwrap();
        let x_to_users = root_path.join("x_to_users");
        mklink_j(&x_to_users, &fake_users).expect("mklink /J x_to_users -> fake_users");
        let host_under_users =
            HostDirs::from_home(home_under_users, Some(PathBuf::new()), Some(PathBuf::new()));
        let via_x_to_users = x_to_users
            .join("me")
            .join("AppData")
            .join("Local")
            .join("oxideswarm")
            .join("workspaces");
        let err_above_home =
            windows::validate_workspace_root_safe(&via_x_to_users, &host_under_users)
                .expect_err("SEC-17: external junction above user_home must be refused");
        assert!(
            err_above_home.contains("reparse point") && err_above_home.contains("x_to_users"),
            "{err_above_home}"
        );
        std::fs::remove_dir(&x_to_users).unwrap();

        // 4c. Workspace root completely outside home (and outside %USERPROFILE%) has no ancestor check
        //     unless ws_root itself is a reparse point.
        let outside_host = HostDirs::from_home(
            fake_profile.clone(),
            Some(PathBuf::new()),
            Some(PathBuf::new()),
        );
        let outside_ws = victim_temp_ws.clone();
        windows::validate_workspace_root_safe(&outside_ws, &outside_host)
            .expect("plain root outside user_home should pass when not a reparse point");
        let outside_self_junc = root_path.join("outside_self_junc");
        mklink_j(&outside_self_junc, &outside_ws).expect("mklink /J outside_self_junc");
        let err_outside_self =
            windows::validate_workspace_root_safe(&outside_self_junc, &outside_host)
                .expect_err("ws_root itself as a reparse point outside home must still be refused");
        assert!(
            err_outside_self.contains("reparse point"),
            "{err_outside_self}"
        );
        std::fs::remove_dir(&outside_self_junc).unwrap();

        // 5. Optional directory symlink test (if host has symlink privilege).
        let dot_ox = fake_profile.join(".oxideswarm");
        if crate::sandbox::try_create_test_symlink_dir(&victim, &dot_ox) {
            let err_sym = windows::validate_workspace_root_safe(&dot_ox.join("workspaces"), &host)
                .expect_err("SEC-17: directory symlink ancestor must be refused");
            assert!(
                err_sym.contains("reparse point") || err_sym.contains("symlink"),
                "{err_sym}"
            );
            std::fs::remove_dir(&dot_ox).unwrap();
        }

        assert_eq!(
            windows::get_path_dacl_sddl(&victim_ws).unwrap(),
            orig_victim_sddl,
            "victim_ws DACL must remain unchanged across all refused ancestor checks"
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_sec17_refuses_hard_linked_usb_marker_and_store_lock() {
        let root = tempfile::tempdir().unwrap();
        let root_path = windows::normalize_win_path(root.path());
        let fake_profile = root_path.join("profile");
        let ws_root = fake_profile
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB")
            .join("workspaces");
        std::fs::create_dir_all(&ws_root).unwrap();
        let host = HostDirs::from_home(fake_profile, Some(PathBuf::new()), Some(PathBuf::new()));

        let marker = ws_root.join(".oxideswarm-usb-workspaces");
        std::fs::write(&marker, "Created by an OxideSwarm USB kit install.\n").unwrap();
        let marker_hl = root_path.join("marker_hardlink");
        if let Err(e) = std::fs::hard_link(&marker, &marker_hl) {
            fail_or_skip_windows_isolation_test(
                "test_sec17_refuses_hard_linked_usb_marker_and_store_lock",
                &format!("host cannot create hard links ({e})"),
            );
            return;
        }
        let err_marker = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("SEC-17: hard-linked .oxideswarm-usb-workspaces must be refused");
        assert!(
            err_marker.contains(".oxideswarm-usb-workspaces"),
            "{err_marker}"
        );
        assert!(err_marker.contains("nlink == 1"), "{err_marker}");
        std::fs::remove_file(&marker_hl).unwrap();
        windows::validate_workspace_root_safe(&ws_root, &host)
            .expect("single-link .oxideswarm-usb-workspaces must be accepted");

        // Verify store.lock is accepted when nlink == 1 (even while locked by WorkspaceStore) and
        // refused when hard-linked (nlink == 2).
        let lock_path = ws_root.join("store.lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&lock_path)
            .unwrap();
        lock_file.lock().unwrap();
        windows::validate_workspace_root_safe(&ws_root, &host)
            .expect("single-link locked store.lock must be accepted");

        let lock_hl = root_path.join("store_lock_hardlink");
        std::fs::hard_link(&lock_path, &lock_hl).expect("hard_link store.lock");
        let err_lock = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("SEC-17: hard-linked store.lock must be refused");
        assert!(err_lock.contains("store.lock"), "{err_lock}");
        assert!(err_lock.contains("nlink == 1"), "{err_lock}");

        std::fs::remove_file(&lock_hl).unwrap();
        windows::validate_workspace_root_safe(&ws_root, &host)
            .expect("store.lock back to nlink == 1 must be accepted");
        drop(lock_file);

        // Optional file symlink check on store.lock (when host supports symlinks).
        std::fs::remove_file(&lock_path).unwrap();
        let target_file = root_path.join("target_file.txt");
        std::fs::write(&target_file, "secret\n").unwrap();
        if crate::sandbox::try_create_test_symlink_file(&target_file, &lock_path) {
            let err_sym = windows::validate_workspace_root_safe(&ws_root, &host)
                .expect_err("SEC-17: symlinked store.lock must be refused");
            assert!(err_sym.contains("store.lock"), "{err_sym}");
            std::fs::remove_file(&lock_path).unwrap();
        }
    }

    #[cfg(windows)]
    #[test]
    fn test_sec17_nlink_check_accepts_zero_volume_serial_and_file_index_and_reports_read_error() {
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL,
            FILE_ATTRIBUTE_REPARSE_POINT,
        };

        let ws_root = Path::new(r"C:\fake\workspaces");
        // SAFETY: `BY_HANDLE_FILE_INFORMATION` is plain integer and `FILETIME` fields.
        let mut refs_like_info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        refs_like_info.dwFileAttributes = FILE_ATTRIBUTE_NORMAL;
        refs_like_info.dwVolumeSerialNumber = 0;
        refs_like_info.nFileIndexHigh = 0;
        refs_like_info.nFileIndexLow = 0;
        refs_like_info.nNumberOfLinks = 1;
        windows::validate_single_link_file_info(ws_root, "store.lock", Ok(refs_like_info))
            .expect("ReFS / Dev Drive / network volume with zero serial or file index and nNumberOfLinks == 1 must be accepted");

        let mut multi_link = refs_like_info;
        multi_link.nNumberOfLinks = 2;
        let err_multi =
            windows::validate_single_link_file_info(ws_root, "store.lock", Ok(multi_link))
                .expect_err("nNumberOfLinks == 2 must be refused");
        assert!(
            err_multi.contains("2 hard links") && err_multi.contains("nlink == 1"),
            "{err_multi}"
        );

        let mut zero_link = refs_like_info;
        zero_link.nNumberOfLinks = 0;
        let err_zero =
            windows::validate_single_link_file_info(ws_root, "store.lock", Ok(zero_link))
                .expect_err("nNumberOfLinks == 0 must be refused");
        assert!(err_zero.contains("nNumberOfLinks == 0"), "{err_zero}");

        let mut reparse = refs_like_info;
        reparse.dwFileAttributes = FILE_ATTRIBUTE_REPARSE_POINT;
        let err_reparse =
            windows::validate_single_link_file_info(ws_root, "store.lock", Ok(reparse))
                .expect_err("reparse point attribute must be refused");
        assert!(err_reparse.contains("reparse point"), "{err_reparse}");

        let mut dir_attr = refs_like_info;
        dir_attr.dwFileAttributes = FILE_ATTRIBUTE_DIRECTORY;
        let err_dir = windows::validate_single_link_file_info(ws_root, "store.lock", Ok(dir_attr))
            .expect_err("directory attribute must be refused");
        assert!(err_dir.contains("not a regular file"), "{err_dir}");

        let err_read = windows::validate_single_link_file_info(
            ws_root,
            "store.lock",
            Err(std::io::Error::from_raw_os_error(50)),
        )
        .expect_err("GetFileInformationByHandle failure must be refused");
        assert!(
            err_read.contains("GetFileInformationByHandle") && !err_read.contains("hard links"),
            "{err_read}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_sec18_require_switch_fails_instead_of_skipping() {
        handle_windows_isolation_skip("probe_test", "host lacks capability", false);
        let panicked = std::panic::catch_unwind(|| {
            handle_windows_isolation_skip("probe_test", "host lacks capability", true);
        });
        let err = panicked.expect_err("require=true must panic instead of skipping");
        let msg = err
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| err.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            msg.contains("RUSTY_GRID_REQUIRE_APPCONTAINER=1")
                && msg.contains("probe_test")
                && msg.contains("host lacks capability"),
            "unexpected panic message: {msg}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_sec18_each_home_walk_is_proved_independently() {
        if std::env::var_os("OX_SEC18_INDEP_CHILD").as_deref() != Some(std::ffi::OsStr::new("1")) {
            let root = tempfile::tempdir().unwrap();
            let env_home = windows::normalize_win_path(root.path()).join("env_home");
            std::fs::create_dir_all(&env_home).unwrap();
            let exe = std::env::current_exe().unwrap();
            let out = std::process::Command::new(exe)
                .args([
                    "--exact",
                    "isolation::tests::test_sec18_each_home_walk_is_proved_independently",
                    "--nocapture",
                ])
                .env("OX_SEC18_INDEP_CHILD", "1")
                .env("USERPROFILE", &env_home)
                .env_remove("OXIDESWARM_HOME")
                .env_remove("RUSTY_GRID_ACL_GRANT_LOG")
                .output()
                .expect("spawn child test for independent home walks");
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            assert!(
                out.status.success() && stdout.contains("1 passed; 0 failed"),
                "child independent-home-walk test failed:\nstatus={:?}\nstdout={stdout}\nstderr={stderr}",
                out.status
            );
            return;
        }

        let child_root = tempfile::tempdir().unwrap();
        let root_path = windows::normalize_win_path(child_root.path());
        let env_home = windows::normalize_win_path(&PathBuf::from(
            std::env::var_os("USERPROFILE").expect("child USERPROFILE"),
        ));
        let host_home = root_path.join("host_home");
        let victim = root_path.join("victim");
        let victim_ws = victim.join("workspaces");
        let sb_base = root_path.join("sandboxes");
        let sb_dir = sb_base.join(".isolation-probe-sec18-indep");
        for dir in [&host_home, &victim_ws, &sb_dir] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let orig_victim_sddl =
            windows::get_path_dacl_sddl(&victim_ws).expect("read original DACL SDDL");

        // Confirm %USERPROFILE% (`env_home`) is disjoint from `host_home`.
        assert!(
            !windows::path_starts_with_ci(&host_home, &env_home)
                && !windows::path_starts_with_ci(&env_home, &host_home),
            "host_home ({}) and env_home ({}) must be disjoint",
            host_home.display(),
            env_home.display()
        );

        let host_usb_root = host_home
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB");
        if let Err(reason) = mklink_j(&host_usb_root, &victim) {
            fail_or_skip_windows_isolation_test(
                "test_sec18_each_home_walk_is_proved_independently",
                &format!("mklink /J unavailable ({reason})"),
            );
            return;
        }
        let host_usb_ws = host_usb_root.join("workspaces");
        let host_only = HostDirs::from_home(
            host_home.clone(),
            Some(PathBuf::new()),
            Some(PathBuf::new()),
        );
        let empty_host =
            HostDirs::from_home(PathBuf::new(), Some(PathBuf::new()), Some(PathBuf::new()));

        // A. Proof for `host.user_home` walk alone: `%USERPROFILE%` (`env_home`) is disjoint.
        let err_host = windows::validate_workspace_root_safe(&host_usb_ws, &host_only)
            .expect_err("host.user_home walk must refuse junction under host_home");
        assert!(
            err_host.contains("reparse point")
                && err_host.contains("OxideSwarm-USB")
                && err_host.contains("--workspace-root"),
            "{err_host}"
        );
        let comspec =
            std::env::var("COMSPEC").unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".to_string());
        let mut cmd = tokio::process::Command::new(&comspec);
        cmd.args(["/C", "echo ok"]);
        cmd.current_dir(&sb_dir);
        let jail_host = Jail {
            level: IsolationLevel::Fs,
            host: &host_only,
            protected_files: &[],
            sandbox_base: &sb_base,
            sandbox_dir: &sb_dir,
            extra_writable_dir: None,
            workspace_root: Some(&host_usb_ws),
        };
        let guard_err_host =
            match windows::WindowsAppContainerGuard::create(uuid::Uuid::new_v4(), &jail_host, &cmd)
            {
                Ok(_) => panic!("guard must refuse junction under host.user_home"),
                Err(e) => e,
            };
        assert_eq!(guard_err_host.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(
            guard_err_host.to_string().contains("reparse point")
                && guard_err_host.to_string().contains("OxideSwarm-USB")
                && guard_err_host.to_string().contains("--workspace-root"),
            "{guard_err_host}"
        );
        assert_eq!(
            windows::get_path_dacl_sddl(&victim_ws).unwrap(),
            orig_victim_sddl
        );

        // Counter-proof: without `host.user_home`, `%USERPROFILE%` (`env_home`) does NOT trigger.
        windows::validate_workspace_root_safe(&host_usb_ws, &empty_host)
            .expect("empty host.user_home must prove %USERPROFILE% did not trigger for host_home");
        std::fs::remove_dir(&host_usb_root).unwrap();

        // B. Proof for `%USERPROFILE%` walk alone: `host.user_home` (`host_home`) is disjoint.
        let env_ox_root = env_home.join("AppData").join("Local").join("oxideswarm");
        mklink_j(&env_ox_root, &victim).expect("mklink /J under env_home");
        let env_ox_ws = env_ox_root.join("workspaces");
        let err_env = windows::validate_workspace_root_safe(&env_ox_ws, &host_only)
            .expect_err("%USERPROFILE% walk must refuse junction under env_home");
        assert!(
            err_env.contains("reparse point")
                && err_env.contains("oxideswarm")
                && err_env.contains("--workspace-root"),
            "{err_env}"
        );
        let jail_env = Jail {
            level: IsolationLevel::Fs,
            host: &host_only,
            protected_files: &[],
            sandbox_base: &sb_base,
            sandbox_dir: &sb_dir,
            extra_writable_dir: None,
            workspace_root: Some(&env_ox_ws),
        };
        let guard_err_env = match windows::WindowsAppContainerGuard::create(
            uuid::Uuid::new_v4(),
            &jail_env,
            &cmd,
        ) {
            Ok(_) => panic!("guard must refuse junction under %USERPROFILE%"),
            Err(e) => e,
        };
        assert_eq!(guard_err_env.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(
            guard_err_env.to_string().contains("reparse point")
                && guard_err_env.to_string().contains("oxideswarm")
                && guard_err_env.to_string().contains("--workspace-root"),
            "{guard_err_env}"
        );
        assert_eq!(
            windows::get_path_dacl_sddl(&victim_ws).unwrap(),
            orig_victim_sddl
        );

        // Counter-proof: with `env_userprofile = None`, neither `host_only` nor `empty_host`
        // refuses `env_ox_ws` (proving `%USERPROFILE%` alone caused the refusal).
        windows::validate_workspace_root_safe_with_env_home(&env_ox_ws, &host_only, None).expect(
            "env_userprofile=None with disjoint host.user_home must prove %USERPROFILE% walk alone ran",
        );
        windows::validate_workspace_root_safe_with_env_home(&env_ox_ws, &empty_host, None)
            .expect("env_userprofile=None with empty host.user_home must accept");
        std::fs::remove_dir(&env_ox_root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn test_sec18_bare_drive_prefix_does_not_match_home_when_cwd_is_home() {
        use std::path::Component;

        if std::env::var_os("OX_SEC18_BARE_PREFIX_CHILD").as_deref()
            != Some(std::ffi::OsStr::new("1"))
        {
            let root = tempfile::tempdir().unwrap();
            let root_path = windows::normalize_win_path(root.path());
            let fake_home = root_path.join("fake_home");
            std::fs::create_dir_all(&fake_home).unwrap();
            let exe = std::env::current_exe().unwrap();
            let out = std::process::Command::new(exe)
                .args([
                    "--exact",
                    "isolation::tests::test_sec18_bare_drive_prefix_does_not_match_home_when_cwd_is_home",
                    "--nocapture",
                ])
                .current_dir(&fake_home)
                .env("OX_SEC18_BARE_PREFIX_CHILD", "1")
                .env("OX_SEC18_TEST_ROOT", &root_path)
                .env("USERPROFILE", &fake_home)
                .env_remove("OXIDESWARM_HOME")
                .env_remove("RUSTY_GRID_ACL_GRANT_LOG")
                .output()
                .expect("spawn child test with CWD inside home");
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            assert!(
                out.status.success() && stdout.contains("1 passed; 0 failed"),
                "child bare-drive-prefix test failed:\nstatus={:?}\nstdout={stdout}\nstderr={stderr}",
                out.status
            );
            return;
        }

        let root_path = PathBuf::from(std::env::var_os("OX_SEC18_TEST_ROOT").expect("test root"));
        let fake_home = windows::normalize_win_path(&std::env::current_dir().unwrap());
        let drive_prefix = match fake_home.components().next() {
            Some(Component::Prefix(p)) => PathBuf::from(p.as_os_str()),
            other => panic!("expected drive prefix in {fake_home:?}, got {other:?}"),
        };
        assert_eq!(
            windows::normalize_win_path(&drive_prefix),
            fake_home,
            "precondition: bare drive prefix must expand to current working directory (fake_home)"
        );

        let outside_target = root_path.join("outside_target");
        std::fs::create_dir_all(outside_target.join("ws")).unwrap();
        let outside_junc = root_path.join("outside_junc");
        if let Err(reason) = mklink_j(&outside_junc, &outside_target) {
            fail_or_skip_windows_isolation_test(
                "test_sec18_bare_drive_prefix_does_not_match_home_when_cwd_is_home",
                &format!("mklink /J unavailable ({reason})"),
            );
            return;
        }

        let host = HostDirs::from_home(
            fake_home.clone(),
            Some(PathBuf::new()),
            Some(PathBuf::new()),
        );
        let outside_ws = outside_junc.join("ws");
        windows::validate_workspace_root_safe(&outside_ws, &host).expect(
            "root outside home with an ancestor junction must not be treated as inside home via bare drive prefix",
        );
        std::fs::remove_dir(&outside_junc).unwrap();

        // Meanwhile junctions inside home or targeting inside home are still refused.
        let in_home_junc = fake_home.join("AppData").join("Local").join("oxideswarm");
        mklink_j(&in_home_junc, &outside_target).expect("mklink /J inside home");
        let err_inside = windows::validate_workspace_root_safe(&in_home_junc.join("ws"), &host)
            .expect_err("junction inside home must still be refused");
        assert!(err_inside.contains("reparse point"), "{err_inside}");
        std::fs::remove_dir(&in_home_junc).unwrap();

        let real_in_home = fake_home.join("AppData").join("Local").join("oxideswarm");
        std::fs::create_dir_all(real_in_home.join("workspaces")).unwrap();
        let ext_into_home = root_path.join("ext_into_home");
        mklink_j(&ext_into_home, &real_in_home).expect("mklink /J into home");
        let err_into =
            windows::validate_workspace_root_safe(&ext_into_home.join("workspaces"), &host)
                .expect_err("external junction pointing into home must still be refused");
        assert!(err_into.contains("reparse point"), "{err_into}");
        std::fs::remove_dir(&ext_into_home).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn test_sec18_refuses_top_level_files_reparse_entries_and_hard_linked_ws_metadata() {
        let _reset = HookResetGuard;
        let root = tempfile::tempdir().unwrap();
        let root_path = windows::normalize_win_path(root.path());
        let fake_profile = root_path.join("profile");
        let ws_root = fake_profile
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB")
            .join("workspaces");
        let victim_dir = root_path.join("victim_dir");
        let victim_file = root_path.join("victim_file.txt");
        let sb_base = root_path.join("sandboxes");
        std::fs::create_dir_all(&ws_root).unwrap();
        std::fs::create_dir_all(&victim_dir).unwrap();
        std::fs::create_dir_all(&sb_base).unwrap();
        std::fs::write(&victim_file, "victim-content\n").unwrap();
        let orig_victim_file_sddl = windows::get_path_dacl_sddl(&victim_file).unwrap();

        let host = HostDirs::from_home(fake_profile, Some(PathBuf::new()), Some(PathBuf::new()));
        std::fs::write(
            ws_root.join(".oxideswarm-usb-workspaces"),
            "Created by an OxideSwarm USB kit install.\n",
        )
        .unwrap();
        let store_lock_path = ws_root.join("store.lock");
        let store_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&store_lock_path)
            .unwrap();
        store_lock.lock().unwrap();

        let locks_dir = ws_root.join(".locks");
        std::fs::create_dir_all(&locks_dir).unwrap();
        std::fs::write(locks_dir.join("good.lock"), "").unwrap();

        let ws_dir = ws_root.join("my_ws");
        let ws_tree = ws_dir.join("tree");
        let ws_tmp = ws_dir.join("tmp");
        std::fs::create_dir_all(&ws_tree).unwrap();
        std::fs::create_dir_all(&ws_tmp).unwrap();
        let ws_state = ws_dir.join("state.json");
        std::fs::write(&ws_state, "{}\n").unwrap();
        let ws_lock_path = ws_dir.join("lock");
        let ws_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&ws_lock_path)
            .unwrap();
        ws_lock.lock().unwrap();

        windows::validate_workspace_root_safe(&ws_root, &host)
            .expect("valid workspace root with regular single-link metadata must pass");

        let comspec =
            std::env::var("COMSPEC").unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".to_string());
        let mut cmd = tokio::process::Command::new(&comspec);
        cmd.args(["/C", "echo ok"]);
        cmd.current_dir(&ws_tree);
        let jail = Jail {
            level: IsolationLevel::Fs,
            host: &host,
            protected_files: &[],
            sandbox_base: &sb_base,
            sandbox_dir: &ws_tree,
            extra_writable_dir: Some(&ws_tmp),
            workspace_root: Some(&ws_root),
        };
        let guard = windows::WindowsAppContainerGuard::create(uuid::Uuid::new_v4(), &jail, &cmd)
            .expect(
                "valid workspace layout (even with locked store.lock and ws lock) must succeed",
            );
        drop(guard);

        // 1. Top-level regular file with valid workspace name (`notes.txt`), plain and hard-linked.
        let notes = ws_root.join("notes.txt");
        std::fs::write(&notes, "note\n").unwrap();
        let err_notes = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("top-level regular file notes.txt must be refused");
        assert!(
            err_notes.contains("notes.txt") && err_notes.contains("not a directory"),
            "{err_notes}"
        );
        std::fs::remove_file(&notes).unwrap();

        let hl_top = ws_root.join("hl_note.txt");
        if let Err(e) = std::fs::hard_link(&victim_file, &hl_top) {
            fail_or_skip_windows_isolation_test(
                "test_sec18_refuses_top_level_files_reparse_entries_and_hard_linked_ws_metadata",
                &format!("hard_link unavailable ({e})"),
            );
            return;
        }
        let err_hl_top =
            match windows::WindowsAppContainerGuard::create(uuid::Uuid::new_v4(), &jail, &cmd) {
                Ok(_) => panic!("guard must refuse top-level hard-linked file"),
                Err(e) => e,
            };
        assert_eq!(err_hl_top.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(
            err_hl_top.to_string().contains("hl_note.txt"),
            "{err_hl_top}"
        );
        assert_eq!(
            windows::get_path_dacl_sddl(&victim_file).unwrap(),
            orig_victim_file_sddl,
            "victim file DACL must remain untouched"
        );
        std::fs::remove_file(&hl_top).unwrap();

        // 2. Top-level `.locks` as regular file, junction, and hard-linked lock file.
        std::fs::remove_dir_all(&locks_dir).unwrap();
        std::fs::write(&locks_dir, "not a dir\n").unwrap();
        let err_locks_file = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("regular file .locks must be refused");
        assert!(
            err_locks_file.contains(".locks") && err_locks_file.contains("not a directory"),
            "{err_locks_file}"
        );
        std::fs::remove_file(&locks_dir).unwrap();

        if let Err(reason) = mklink_j(&locks_dir, &victim_dir) {
            fail_or_skip_windows_isolation_test(
                "test_sec18_refuses_top_level_files_reparse_entries_and_hard_linked_ws_metadata",
                &format!("mklink /J unavailable ({reason})"),
            );
            return;
        }
        let err_locks_junc = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("junction .locks must be refused");
        assert!(
            err_locks_junc.contains(".locks") && err_locks_junc.contains("reparse point"),
            "{err_locks_junc}"
        );
        std::fs::remove_dir(&locks_dir).unwrap();

        std::fs::create_dir_all(&locks_dir).unwrap();
        let bad_lock = locks_dir.join("bad.lock");
        std::fs::hard_link(&victim_file, &bad_lock).unwrap();
        let err_bad_lock = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("hard-linked .locks/bad.lock must be refused");
        assert!(
            err_bad_lock.contains(".locks/bad.lock") && err_bad_lock.contains("nlink == 1"),
            "{err_bad_lock}"
        );
        std::fs::remove_file(&bad_lock).unwrap();

        // 3. Top-level junction workspace directory.
        let junc_ws = ws_root.join("junc_ws");
        mklink_j(&junc_ws, &victim_dir).unwrap();
        let err_junc_ws = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("junction workspace folder must be refused");
        assert!(
            err_junc_ws.contains("junc_ws") && err_junc_ws.contains("reparse point"),
            "{err_junc_ws}"
        );
        std::fs::remove_dir(&junc_ws).unwrap();

        // 4. Hard-linked `state.json`, `lock`, and `state.json.tmp` inside `my_ws`.
        std::fs::remove_file(&ws_state).unwrap();
        std::fs::hard_link(&victim_file, &ws_state).unwrap();
        let err_hl_state = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("hard-linked state.json must be refused");
        assert!(
            err_hl_state.contains("my_ws/state.json") && err_hl_state.contains("nlink == 1"),
            "{err_hl_state}"
        );
        let guard_err_state =
            match windows::WindowsAppContainerGuard::create(uuid::Uuid::new_v4(), &jail, &cmd) {
                Ok(_) => panic!("guard must refuse hard-linked state.json"),
                Err(e) => e,
            };
        assert_eq!(guard_err_state.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            windows::get_path_dacl_sddl(&victim_file).unwrap(),
            orig_victim_file_sddl
        );
        std::fs::remove_file(&ws_state).unwrap();
        std::fs::write(&ws_state, "{}\n").unwrap();

        drop(ws_lock);
        std::fs::remove_file(&ws_lock_path).unwrap();
        std::fs::hard_link(&victim_file, &ws_lock_path).unwrap();
        let err_hl_lock = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("hard-linked lock must be refused");
        assert!(
            err_hl_lock.contains("my_ws/lock") && err_hl_lock.contains("nlink == 1"),
            "{err_hl_lock}"
        );
        std::fs::remove_file(&ws_lock_path).unwrap();
        std::fs::write(&ws_lock_path, "").unwrap();

        let ws_state_tmp = ws_dir.join("state.json.tmp");
        std::fs::hard_link(&victim_file, &ws_state_tmp).unwrap();
        let err_hl_tmp = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("hard-linked state.json.tmp must be refused");
        assert!(
            err_hl_tmp.contains("my_ws/state.json.tmp") && err_hl_tmp.contains("nlink == 1"),
            "{err_hl_tmp}"
        );
        std::fs::remove_file(&ws_state_tmp).unwrap();

        // 5. `my_ws/tmp` as file, `my_ws/state.json` as directory, extra entry in `my_ws`, and
        //    junction at `my_ws/tree`.
        std::fs::remove_dir_all(&ws_tmp).unwrap();
        std::fs::write(&ws_tmp, "not a dir\n").unwrap();
        let err_tmp_file = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("regular file my_ws/tmp must be refused");
        assert!(
            err_tmp_file.contains("my_ws/tmp") && err_tmp_file.contains("not a directory"),
            "{err_tmp_file}"
        );
        std::fs::remove_file(&ws_tmp).unwrap();
        std::fs::create_dir_all(&ws_tmp).unwrap();

        std::fs::remove_file(&ws_state).unwrap();
        std::fs::create_dir_all(&ws_state).unwrap();
        let err_state_dir = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("directory my_ws/state.json must be refused");
        assert!(
            err_state_dir.contains("my_ws/state.json")
                && err_state_dir.contains("not a regular file"),
            "{err_state_dir}"
        );
        std::fs::remove_dir_all(&ws_state).unwrap();
        std::fs::write(&ws_state, "{}\n").unwrap();

        let extra_entry = ws_dir.join("extra.txt");
        std::fs::write(&extra_entry, "x").unwrap();
        let err_extra = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("unexpected entry inside my_ws must be refused");
        assert!(
            err_extra.contains("my_ws") && err_extra.contains("extra.txt"),
            "{err_extra}"
        );
        std::fs::remove_file(&extra_entry).unwrap();

        std::fs::remove_dir_all(&ws_tree).unwrap();
        mklink_j(&ws_tree, &victim_dir).unwrap();
        let err_tree_junc = windows::validate_workspace_root_safe(&ws_root, &host)
            .expect_err("junction my_ws/tree must be refused");
        assert!(
            err_tree_junc.contains("my_ws/tree") && err_tree_junc.contains("reparse point"),
            "{err_tree_junc}"
        );
        std::fs::remove_dir(&ws_tree).unwrap();
        std::fs::create_dir_all(&ws_tree).unwrap();

        // 6. Race hook right before metadata handle DACL write on `state.json` and `lock`.
        for (target_name, race_link_name) in
            [("state.json", "race_hl_state"), ("lock", "race_hl_lock")]
        {
            let target_path = ws_dir.join(target_name);
            let orig_target_sddl = windows::get_path_dacl_sddl(&target_path).unwrap();
            let link_dest = root_path.join(race_link_name);
            let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let called_cb = called.clone();
            let link_dest_cb = link_dest.clone();
            windows::ws_root_recheck_test_hook::set_before_meta_dacl(move |dir| {
                called_cb.store(true, std::sync::atomic::Ordering::SeqCst);
                std::fs::hard_link(dir.join(target_name), &link_dest_cb)
                    .expect("inject hard link before metadata DACL write");
            });
            let race_err = match windows::WindowsAppContainerGuard::create(
                uuid::Uuid::new_v4(),
                &jail,
                &cmd,
            ) {
                Ok(_) => panic!("handle-based metadata DACL write must refuse nlink != 1"),
                Err(e) => e,
            };
            assert!(
                called.load(std::sync::atomic::Ordering::SeqCst),
                "before_meta_dacl hook must have executed"
            );
            assert_eq!(race_err.kind(), std::io::ErrorKind::PermissionDenied);
            assert!(race_err.to_string().contains("nlink == 1"), "{race_err}");
            assert_eq!(
                windows::get_path_dacl_sddl(&link_dest).unwrap(),
                orig_target_sddl,
                "hard-linked race target DACL must stay untouched"
            );
            let _ = std::fs::remove_file(&link_dest);
            // Reset target_name file with fresh single link for a clean next pass.
            let _ = std::fs::remove_file(&target_path);
            std::fs::write(&target_path, "").unwrap();
        }
        drop(store_lock);
    }

    #[cfg(windows)]
    #[test]
    fn test_sec18_handle_dacl_write_and_prepare_lock_recheck_hooks() {
        use windows_sys::Win32::Security::Authorization::SET_ACCESS;
        use windows_sys::Win32::Security::{
            WinBuiltinAdministratorsSid, WinLocalSystemSid, CONTAINER_INHERIT_ACE,
            OBJECT_INHERIT_ACE,
        };
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ALL_ACCESS, FILE_GENERIC_EXECUTE, FILE_GENERIC_READ, FILE_READ_ATTRIBUTES,
            FILE_READ_DATA, SYNCHRONIZE,
        };

        let _reset = HookResetGuard;
        let root = tempfile::tempdir().unwrap();
        let root_path = windows::normalize_win_path(root.path());
        let fake_profile = root_path.join("profile");
        let usb_root = fake_profile
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB");
        let usb_ws = usb_root.join("workspaces");
        let victim = root_path.join("victim");
        let victim_ws = victim.join("workspaces");
        let sb_base = root_path.join("sandboxes");
        let sb_dir = sb_base.join(".isolation-probe-sec18");
        for dir in [&usb_ws, &victim_ws, &sb_dir] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let orig_victim_sddl = windows::get_path_dacl_sddl(&victim_ws).unwrap();

        let host = HostDirs::from_home(fake_profile, Some(PathBuf::new()), Some(PathBuf::new()));
        let comspec =
            std::env::var("COMSPEC").unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".to_string());
        let mut cmd = tokio::process::Command::new(&comspec);
        cmd.args(["/C", "echo ok"]);
        cmd.current_dir(&sb_dir);
        let jail = Jail {
            level: IsolationLevel::Fs,
            host: &host,
            protected_files: &[],
            sandbox_base: &sb_base,
            sandbox_dir: &sb_dir,
            extra_writable_dir: None,
            workspace_root: Some(&usb_ws),
        };

        // Probe mklink /J availability up front (creating a probe junction and deleting it).
        let probe_junc = root_path.join("probe_junc");
        if let Err(reason) = mklink_j(&probe_junc, &victim_ws) {
            fail_or_skip_windows_isolation_test(
                "test_sec18_handle_dacl_write_and_prepare_lock_recheck_hooks",
                &format!("mklink /J unavailable ({reason})"),
            );
            return;
        }
        std::fs::remove_dir(&probe_junc).unwrap();

        // A. Swap ancestor `OxideSwarm-USB` for a junction right before PREPARE_LOCK re-check.
        let hook_a_ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook_a_cb = hook_a_ran.clone();
        let usb_root_a = usb_root.clone();
        let victim_a = victim.clone();
        windows::ws_root_recheck_test_hook::set_before_recheck(move |_ws| {
            hook_a_cb.store(true, std::sync::atomic::Ordering::SeqCst);
            std::fs::remove_dir_all(&usb_root_a).expect("remove clean OxideSwarm-USB dir");
            mklink_j(&usb_root_a, &victim_a).expect("swap OxideSwarm-USB to junction");
        });
        let err_a =
            match windows::WindowsAppContainerGuard::create(uuid::Uuid::new_v4(), &jail, &cmd) {
                Ok(_) => panic!("PREPARE_LOCK re-check must catch ancestor junction swap"),
                Err(e) => e,
            };
        assert!(hook_a_ran.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(err_a.kind(), std::io::ErrorKind::PermissionDenied);
        let msg_a = err_a.to_string();
        assert!(
            msg_a.contains("reparse point")
                && msg_a.contains("OxideSwarm-USB")
                && msg_a.contains("--workspace-root"),
            "{msg_a}"
        );
        assert_eq!(
            windows::get_path_dacl_sddl(&victim_ws).unwrap(),
            orig_victim_sddl
        );
        std::fs::remove_dir(&usb_root).unwrap();
        std::fs::create_dir_all(&usb_ws).unwrap();

        // B. Swap ancestor `OxideSwarm-USB` for a junction AFTER PREPARE_LOCK re-check and right
        //    before `protect_sandbox_base`: handle check compares `GetFinalPathNameByHandleW` vs
        //    expected lexical path and refuses before `SetSecurityInfo`.
        let hook_b_ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook_b_cb = hook_b_ran.clone();
        let usb_root_b = usb_root.clone();
        let victim_b = victim.clone();
        windows::ws_root_recheck_test_hook::set_before_protect(move |_ws| {
            hook_b_cb.store(true, std::sync::atomic::Ordering::SeqCst);
            std::fs::remove_dir_all(&usb_root_b).expect("remove clean OxideSwarm-USB dir");
            mklink_j(&usb_root_b, &victim_b).expect("swap OxideSwarm-USB to junction");
        });
        let err_b =
            match windows::WindowsAppContainerGuard::create(uuid::Uuid::new_v4(), &jail, &cmd) {
                Ok(_) => panic!("GetFinalPathNameByHandleW must catch ancestor swap"),
                Err(e) => e,
            };
        assert!(hook_b_ran.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(err_b.kind(), std::io::ErrorKind::PermissionDenied);
        let msg_b = err_b.to_string();
        assert!(
            msg_b.contains("resolved to") && msg_b.contains("lexical path"),
            "{msg_b}"
        );
        assert_eq!(
            windows::get_path_dacl_sddl(&victim_ws).unwrap(),
            orig_victim_sddl
        );
        std::fs::remove_dir(&usb_root).unwrap();
        std::fs::create_dir_all(&usb_ws).unwrap();

        // C. Swap `workspaces` itself for a junction AFTER PREPARE_LOCK re-check and right before
        //    `protect_sandbox_base`: `FILE_FLAG_OPEN_REPARSE_POINT` handle catches
        //    `FILE_ATTRIBUTE_REPARSE_POINT`.
        let hook_c_ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook_c_cb = hook_c_ran.clone();
        let usb_ws_c = usb_ws.clone();
        let victim_ws_c = victim_ws.clone();
        windows::ws_root_recheck_test_hook::set_before_protect(move |_ws| {
            hook_c_cb.store(true, std::sync::atomic::Ordering::SeqCst);
            std::fs::remove_dir(&usb_ws_c).expect("remove clean workspaces dir");
            mklink_j(&usb_ws_c, &victim_ws_c).expect("swap workspaces to junction");
        });
        let err_c =
            match windows::WindowsAppContainerGuard::create(uuid::Uuid::new_v4(), &jail, &cmd) {
                Ok(_) => panic!("no-follow handle must catch final reparse point"),
                Err(e) => e,
            };
        assert!(hook_c_ran.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(err_c.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(err_c.to_string().contains("reparse point"), "{err_c}");
        assert_eq!(
            windows::get_path_dacl_sddl(&victim_ws).unwrap(),
            orig_victim_sddl
        );
        std::fs::remove_dir(&usb_ws).unwrap();

        // D. Handle-based `SetSecurityInfo` propagates inheritable `(OI)(CI)` ACEs onto a
        //    pre-existing child file.
        let prop_dir = root_path.join("prop_dir");
        std::fs::create_dir_all(&prop_dir).unwrap();
        let child_txt = prop_dir.join("child.txt");
        std::fs::write(&child_txt, "hello\n").unwrap();

        let user_sid = windows::OwnedSid::current_user().unwrap();
        let system_sid = windows::OwnedSid::from_well_known(WinLocalSystemSid).unwrap();
        let admins_sid = windows::OwnedSid::from_well_known(WinBuiltinAdministratorsSid).unwrap();
        let toolchain_sid = windows::OwnedSid::oxideswarm_toolchain().unwrap();
        let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
        let rx_mask = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE | SYNCHRONIZE;
        let entries = [
            windows::explicit_access(user_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
            windows::explicit_access(system_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
            windows::explicit_access(admins_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
            windows::explicit_access(toolchain_sid.as_psid(), rx_mask, SET_ACCESS, inherit),
        ];
        windows::set_protected_dacl_on_path(&prop_dir, &entries)
            .expect("handle-based set_protected_dacl_on_path");
        let check_mask = FILE_READ_ATTRIBUTES | FILE_READ_DATA;
        assert!(
            windows::dacl_already_grants_sid(&prop_dir, toolchain_sid.as_psid(), check_mask),
            "directory itself must carry granted ACE"
        );
        assert!(
            windows::dacl_already_grants_sid(&child_txt, toolchain_sid.as_psid(), check_mask),
            "SetSecurityInfo must propagate inheritable ACE to pre-existing child file"
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_sec18_eviction_not_found_skipped_while_bad_entries_refused() {
        let root = tempfile::tempdir().unwrap();
        let root_path = windows::normalize_win_path(root.path());
        let ws_root = root_path.join("workspaces");
        let evicted_ws = ws_root.join("evicted_ws");
        let active_ws = ws_root.join("active_ws");
        std::fs::create_dir_all(active_ws.join("tree")).unwrap();
        std::fs::create_dir_all(active_ws.join("tmp")).unwrap();
        std::fs::write(active_ws.join("state.json"), "{}").unwrap();
        std::fs::write(active_ws.join("lock"), "").unwrap();

        // Concurrently evicted workspace dir or already-wiped entries (NotFound) must be skipped.
        windows::validate_workspace_dir_entries_safe(&ws_root, "evicted_ws", &evicted_ws)
            .expect("missing workspace dir removed by eviction must be skipped");
        std::fs::remove_dir_all(active_ws.join("tree")).unwrap();
        std::fs::remove_dir_all(active_ws.join("tmp")).unwrap();
        std::fs::remove_file(active_ws.join("state.json")).unwrap();
        windows::validate_workspace_root_entries_safe(&ws_root)
            .expect("evicted workspace entries removed between read_dir and stat must be skipped");

        // Real security violations (junction or hard-linked metadata) stay refused.
        let hardlink_outside = root_path.join("outside_state.json");
        std::fs::write(&hardlink_outside, "{}").unwrap();
        std::fs::hard_link(&hardlink_outside, active_ws.join("state.json")).unwrap();
        let err_hl = windows::validate_workspace_root_entries_safe(&ws_root)
            .expect_err("hard-linked state.json must still be refused");
        assert!(err_hl.contains("nlink == 1"), "{err_hl}");
        std::fs::remove_file(active_ws.join("state.json")).unwrap();

        let victim_dir = root_path.join("victim");
        std::fs::create_dir_all(&victim_dir).unwrap();
        mklink_j(&active_ws.join("tree"), &victim_dir).unwrap();
        let err_junc = windows::validate_workspace_root_entries_safe(&ws_root)
            .expect_err("junction tree must still be refused");
        assert!(err_junc.contains("reparse point"), "{err_junc}");
        std::fs::remove_dir(active_ws.join("tree")).unwrap();
    }

    #[test]
    fn test_acl_grant_manifest_path_precedence_honours_oxideswarm_home() {
        use std::ffi::OsStr;

        let fake_home = PathBuf::from(r"C:\Users\alice");
        let host = HostDirs::from_home(fake_home.clone(), None, None);
        let ox_home = PathBuf::from(r"C:\Users\alice\AppData\Local\OxideSwarm\home");
        let explicit_log = PathBuf::from(r"D:\custom\grants.txt");

        // 1. Default when neither RUSTY_GRID_ACL_GRANT_LOG nor OXIDESWARM_HOME is set:
        //    <user_home>/.oxideswarm/windows_acl_grants.txt
        assert_eq!(
            acl_grant_manifest_path_with(&host, None, None),
            fake_home.join(".oxideswarm").join("windows_acl_grants.txt")
        );

        // Empty env values are ignored and fall through to the next tier.
        assert_eq!(
            acl_grant_manifest_path_with(&host, Some(OsStr::new("")), Some(OsStr::new(""))),
            fake_home.join(".oxideswarm").join("windows_acl_grants.txt")
        );

        // 2. When OXIDESWARM_HOME is set and RUSTY_GRID_ACL_GRANT_LOG is unset/empty:
        //    <OXIDESWARM_HOME>/windows_acl_grants.txt
        assert_eq!(
            acl_grant_manifest_path_with(&host, None, Some(ox_home.as_os_str())),
            ox_home.join("windows_acl_grants.txt")
        );
        assert_eq!(
            acl_grant_manifest_path_with(&host, Some(OsStr::new("")), Some(ox_home.as_os_str()),),
            ox_home.join("windows_acl_grants.txt")
        );

        // 3. When RUSTY_GRID_ACL_GRANT_LOG is set, it overrides OXIDESWARM_HOME and user_home.
        assert_eq!(
            acl_grant_manifest_path_with(
                &host,
                Some(explicit_log.as_os_str()),
                Some(ox_home.as_os_str()),
            ),
            explicit_log
        );
    }

    #[test]
    fn test_validate_extra_read_only_dirs_allows_narrow_runtime_and_refuses_broad_or_secret_paths()
    {
        let root = tempfile::tempdir().unwrap();
        let fake_home = root.path().join("home");
        let outside = root.path().join("outside");
        let sandbox_base = outside.join("sandboxes");
        let workspace_root = outside.join("workspaces");

        let pyenv_runtime = fake_home.join(".pyenv").join("versions").join("3.11.8");
        let win_python = fake_home
            .join("AppData")
            .join("Local")
            .join("Programs")
            .join("Python")
            .join("Python311");
        let mac_blender = fake_home
            .join("Library")
            .join("Application Support")
            .join("Blender")
            .join("4.5");
        let ext_runtime = outside.join("ext_runtime");
        let ssh_dir = fake_home.join(".ssh").join("keys");
        let aws_dir = fake_home.join(".aws");
        let ox_dir = fake_home.join(".oxideswarm");
        let cargo_dir = fake_home.join(".cargo");
        let cargo_bin = cargo_dir.join("bin");
        let sb_task = sandbox_base.join("task_1");
        let secret_parent = outside.join("secret_holder");
        let secret_file = secret_parent.join("cluster.secret");

        for d in [
            &pyenv_runtime,
            &win_python,
            &mac_blender,
            &ext_runtime,
            &ssh_dir,
            &aws_dir,
            &ox_dir,
            &cargo_bin,
            &sb_task,
            &workspace_root,
            &secret_parent,
            &fake_home.join("Desktop"),
            &fake_home.join("Documents"),
            &fake_home.join("Downloads"),
            &fake_home.join(".config"),
            &fake_home.join(".local"),
            &fake_home.join("AppData").join("Local").join("Temp"),
        ] {
            std::fs::create_dir_all(d).unwrap();
        }
        let regular_file = outside.join("not_a_dir.txt");
        std::fs::write(&regular_file, "x").unwrap();
        std::fs::write(&secret_file, "secret").unwrap();

        let host = HostDirs::from_home(fake_home.clone(), None, Some(cargo_dir.clone()));

        assert!(validate_extra_read_only_dirs(
            &[],
            &host,
            Some(&sandbox_base),
            Some(&workspace_root),
            std::slice::from_ref(&secret_file),
        )
        .is_ok());

        for allowed in [&pyenv_runtime, &win_python, &mac_blender, &ext_runtime] {
            validate_extra_read_only_dirs(
                std::slice::from_ref(allowed),
                &host,
                Some(&sandbox_base),
                Some(&workspace_root),
                std::slice::from_ref(&secret_file),
            )
            .unwrap_or_else(|e| panic!("expected {} to be allowed, got: {e}", allowed.display()));
        }

        let refused_paths = [
            fake_home.clone(),
            fake_home.join("Desktop"),
            fake_home.join("Documents"),
            fake_home.join("Downloads"),
            fake_home.join(".config"),
            fake_home.join(".local"),
            fake_home.join("AppData"),
            fake_home.join("AppData").join("Local"),
            fake_home.join("AppData").join("Local").join("Programs"),
            fake_home.join("AppData").join("Local").join("Temp"),
            std::env::temp_dir(),
            fake_home.join(".ssh"),
            ssh_dir,
            aws_dir,
            ox_dir,
            cargo_dir,
            cargo_bin,
            sandbox_base.clone(),
            sb_task,
            outside.clone(),
            workspace_root.clone(),
            secret_parent,
            secret_file.clone(),
            regular_file,
            outside.join("does_not_exist"),
            outside.join("ext_runtime").join("..").join("ext_runtime"),
            PathBuf::from("relative/runtime"),
            PathBuf::new(),
        ];

        for bad in refused_paths {
            let err = validate_extra_read_only_dirs(
                std::slice::from_ref(&bad),
                &host,
                Some(&sandbox_base),
                Some(&workspace_root),
                std::slice::from_ref(&secret_file),
            )
            .expect_err(&format!("path {} must be refused", bad.display()));
            assert!(
                !err.is_empty(),
                "error message for {} must not be empty",
                bad.display()
            );
        }
    }

    #[test]
    fn test_validate_extra_read_only_dirs_refuses_symlinks_and_junctions() {
        let root = tempfile::tempdir().unwrap();
        let fake_home = root.path().join("home");
        let target = root.path().join("target_runtime");
        let target_nested = target.join("nested");
        let real_sub = fake_home.join("real_sub").join("py311");
        std::fs::create_dir_all(&fake_home).unwrap();
        std::fs::create_dir_all(&real_sub).unwrap();
        std::fs::create_dir_all(&target_nested).unwrap();

        let direct_link = fake_home.join("link_runtime");
        let parent_link = fake_home.join("parent_link");
        let home_link = fake_home.join("link_to_home");

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&target, &direct_link).unwrap();
            std::os::unix::fs::symlink(&target, &parent_link).unwrap();
            std::os::unix::fs::symlink(&fake_home, &home_link).unwrap();
        }
        #[cfg(windows)]
        {
            for (link, dest) in [
                (&direct_link, &target),
                (&parent_link, &target),
                (&home_link, &fake_home),
            ] {
                let status = std::process::Command::new("cmd")
                    .args([
                        "/C",
                        "mklink",
                        "/J",
                        &link.to_string_lossy(),
                        &dest.to_string_lossy(),
                    ])
                    .status()
                    .expect("mklink /J must run");
                assert!(status.success(), "mklink /J failed for {}", link.display());
            }
        }

        let host = HostDirs::from_home(fake_home, None, None);
        let err_direct = validate_extra_read_only_dirs(&[direct_link], &host, None, None, &[])
            .expect_err("direct symlink/junction must be refused");
        assert!(
            err_direct.contains("symlink") || err_direct.contains("junction"),
            "{err_direct}"
        );

        let nested_under_link = parent_link.join("nested");
        let err_parent =
            validate_extra_read_only_dirs(&[nested_under_link], &host, None, None, &[])
                .expect_err("ancestor symlink/junction inside home must be refused");
        assert!(
            err_parent.contains("symlink") || err_parent.contains("junction"),
            "{err_parent}"
        );

        let nested_under_home_link = home_link.join("real_sub").join("py311");
        let err_home_link =
            validate_extra_read_only_dirs(&[nested_under_home_link], &host, None, None, &[])
                .expect_err("ancestor symlink/junction pointing to user_home must be refused");
        assert!(
            err_home_link.contains("symlink") || err_home_link.contains("junction"),
            "{err_home_link}"
        );
    }

    #[test]
    fn test_plan_token_spawn_never_falls_back_to_plain_when_elevated() {
        assert_eq!(plan_token_spawn(true, true), TokenSpawnPlan::Lua);
        assert_eq!(plan_token_spawn(false, true), TokenSpawnPlan::Lua);
        assert_eq!(
            plan_token_spawn(true, false),
            TokenSpawnPlan::RefuseNoLuaToken,
            "an elevated worker without a LUA token must refuse"
        );
        assert_eq!(
            plan_token_spawn(false, false),
            TokenSpawnPlan::Plain,
            "a worker that is not elevated keeps CreateProcessW"
        );
    }

    #[test]
    fn test_after_lua_spawn_failure_refuses_only_when_elevated() {
        let msg = after_lua_spawn_failure(true, 1314).expect("elevated must refuse");
        assert!(
            msg.starts_with("refusing to start the task with the worker's elevated token:"),
            "{msg}"
        );
        assert!(msg.contains("CreateProcessAsUserW"), "{msg}");
        assert!(msg.contains("Win32 error 1314"), "{msg}");
        assert!(after_lua_spawn_failure(false, 1314).is_none());
        let none = elevated_token_refusal_message("the LUA token could not be created", None);
        assert!(!none.contains("Win32 error"), "{none}");
    }

    #[cfg(windows)]
    fn run_forced_lua_failure(mode: u8) -> String {
        let home = tempfile::tempdir().unwrap();
        let base = windows::normalize_win_path(home.path());
        let probe_dir = base.join(format!(".isolation-probe-sec11-{mode}"));
        std::fs::create_dir_all(&probe_dir).unwrap();
        let probe_dir = windows::normalize_win_path(&probe_dir);
        let empty_host =
            HostDirs::from_home(PathBuf::new(), Some(PathBuf::new()), Some(PathBuf::new()));
        let jail = Jail {
            level: IsolationLevel::Fs,
            host: &empty_host,
            protected_files: &[],
            sandbox_base: &base,
            sandbox_dir: &probe_dir,
            extra_writable_dir: None,
            workspace_root: None,
        };
        let comspec =
            std::env::var("COMSPEC").unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".to_string());
        let mut cmd = tokio::process::Command::new(&comspec);
        cmd.args(["/C", "echo started > marker"]);
        cmd.current_dir(&probe_dir);
        cmd.env_clear();
        for (name, value) in inherited_environment(std::env::vars_os()) {
            cmd.env(name, value);
        }
        let guard = windows::WindowsAppContainerGuard::create(uuid::Uuid::new_v4(), &jail, &cmd)
            .expect("AppContainer setup");
        windows::lua_test_hook::MODE.with(|m| m.set(mode));
        let res = guard.spawn_in_appcontainer(&mut cmd, 0);
        windows::lua_test_hook::MODE.with(|m| m.set(windows::lua_test_hook::OFF));
        let err = match res {
            Ok(_) => panic!("the spawn must be refused when the LUA path fails"),
            Err(e) => e.to_string(),
        };
        drop(guard);
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert!(
            !probe_dir.join("marker").exists(),
            "no task process may have started"
        );
        err
    }

    #[cfg(windows)]
    #[test]
    fn test_windows_elevated_worker_refuses_when_lua_token_is_missing() {
        let err = run_forced_lua_failure(windows::lua_test_hook::NO_TOKEN);
        assert!(
            err.contains("refusing to start the task with the worker's elevated token"),
            "{err}"
        );
        assert!(err.contains("LUA"), "{err}");
    }

    #[cfg(windows)]
    #[test]
    fn test_windows_elevated_worker_refuses_when_create_process_as_user_fails() {
        let err = run_forced_lua_failure(windows::lua_test_hook::BAD_TOKEN);
        assert!(
            err.contains("refusing to start the task with the worker's elevated token"),
            "{err}"
        );
        assert!(err.contains("CreateProcessAsUserW"), "{err}");
        assert!(err.contains("Win32 error"), "{err}");
    }
}
