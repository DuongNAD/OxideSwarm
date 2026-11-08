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
//! * **Windows** (per-task AppContainer): FEAT-3c, not implemented yet.
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

/// The part of `vars` (the worker's environment) that a confined task inherits: the names in
/// [`INHERITED_ENV_VARS`] and every `LC_*`.
pub fn inherited_environment(
    vars: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    vars.into_iter()
        .filter(|(name, _)| {
            name.to_str().is_some_and(|n| {
                INHERITED_ENV_VARS.contains(&n) || (n.starts_with("LC_") && n.len() > 3)
            })
        })
        .collect()
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
        Self {
            rustup_home: non_empty(rustup_home).unwrap_or_else(|| user_home.join(".rustup")),
            cargo_home: non_empty(cargo_home).unwrap_or_else(|| user_home.join(".cargo")),
            user_home,
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
    let path_text = |p: &Path| p.to_string_lossy().into_owned();
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
    plan.remove.extend(STRIPPED_IPC_ENV_VARS);
    plan
}

/// What the runner knows about one task when it asks for it to be confined. Only the
/// platforms that have an implementation read every field.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(any(target_os = "macos", target_os = "linux")), allow(dead_code))]
pub(crate) struct Jail<'a> {
    pub level: IsolationLevel,
    pub host: &'a HostDirs,
    /// Files the task must not read wherever they are.
    pub protected_files: &'a [PathBuf],
    /// The worker's sandbox base directory (`<base>/<task id>` holds this task's files).
    pub sandbox_base: &'a Path,
    /// The task's own sandbox directory.
    pub sandbox_dir: &'a Path,
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
        let resolved = resolve_program(program, search_path)
            .ok_or_else(|| format!("program '{program}' was not found in PATH"))?;
        Ok((resolved, args.to_vec()))
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
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
    if level == IsolationLevel::Off {
        return Ok(());
    }
    #[cfg(target_os = "macos")]
    {
        macos::probe(level, sandbox_base)
            .await
            .map_err(GridError::Config)
    }
    #[cfg(target_os = "linux")]
    {
        linux::probe(level, sandbox_base)
            .await
            .map_err(GridError::Config)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = sandbox_base;
        Err(GridError::Config(unsupported_message(level)))
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
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
    /// `USER_HOME`, `SANDBOX_BASE`, `CACHE_DIR`, `RUSTUP_HOME`, `CARGO_HOME`, `CARGO_CRED`,
    /// `CARGO_CRED_TOML`, `ANCESTOR_<n>` for `ancestors` entries), so no path is ever spliced into
    /// the profile text.
    pub(super) fn profile(
        level: IsolationLevel,
        ancestors: usize,
        extra_denied: usize,
        protected: usize,
    ) -> String {
        let mut p = String::from("(version 1)\n(allow default)\n(deny file-write* (subpath \"/\"))\n(allow file-write*\n    (subpath (param \"SANDBOX_DIR\"))\n");
        for dev in WRITABLE_DEVICES {
            p.push_str(&format!("    (literal \"{dev}\")\n"));
        }
        p.push_str("    (subpath \"/dev/fd\"))\n");
        p.push_str(
            "(deny file-read*\n    (subpath (param \"USER_HOME\"))\n    (subpath (param \"SANDBOX_BASE\"))\n    (subpath (param \"CACHE_DIR\"))",
        );
        for i in 0..extra_denied {
            p.push_str(&format!("\n    (subpath (param \"DENIED_{i}\"))"));
        }
        p.push_str(")\n");
        p.push_str(
            "(allow file-read*\n    (subpath (param \"SANDBOX_DIR\"))\n    (subpath (param \"RUSTUP_HOME\"))\n    (subpath (param \"CARGO_HOME\")))\n",
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
        let base = canonical(jail.sandbox_base);
        let home = canonical(&jail.host.user_home);
        let rustup = canonical(&jail.host.rustup_home);
        let cargo = canonical(&jail.host.cargo_home);
        let cache = base.join("cache");

        // Further places whose reads are denied: the account's real home (it differs from
        // `$HOME` when the worker runs with another one), the user temp directory (other tasks'
        // and tools' scratch files, Blender's temp dirs), and the shared areas a task has no
        // business in. The task's own sandbox is re-allowed after them.
        let mut extra: Vec<PathBuf> = Vec::new();
        let candidates = [
            account_home(),
            user_temp_dir(),
            user_cache_dir(),
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
        for target in [&sandbox_dir, &rustup, &cargo] {
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
            profile(jail.level, ancestors.len(), extra.len(), protected.len()),
        ];
        let mut define = |name: &str, value: &Path| {
            args.push("-D".to_string());
            args.push(format!("{name}={}", value.to_string_lossy()));
        };
        define("SANDBOX_DIR", &sandbox_dir);
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
        let fs = macos::profile(IsolationLevel::Fs, 2, 1, 1);
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
        let net = macos::profile(IsolationLevel::FsNet, 0, 0, 0);
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

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
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

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    #[test]
    fn test_wrap_program_never_returns_the_plain_command() {
        let h = host();
        let jail = Jail {
            level: IsolationLevel::Fs,
            host: &h,
            protected_files: &[],
            sandbox_base: Path::new("/sb"),
            sandbox_dir: Path::new("/sb/t"),
        };
        let err = wrap_program(&jail, "sh", &[], "/bin").unwrap_err();
        assert!(err.contains("not implemented yet"), "{err}");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn test_self_check_passes_on_macos() {
        let dir = tempfile::tempdir().unwrap();
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            verify_isolation_support(level, dir.path())
                .await
                .unwrap_or_else(|e| panic!("{level}: {e}"));
        }
        let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert!(leftovers.is_empty(), "the probe cleans up after itself");
    }
}
