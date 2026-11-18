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
