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
//! * **Linux** (Landlock, `seccomp`, namespaces): FEAT-3b, not implemented yet.
//! * **Windows** (`windows`, FEAT-3c): each task runs inside a per-task AppContainer profile
//!   (`oxideswarm.task.<task_id>`) spawned with `PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES`,
//!   explicit NTFS DACLs on its sandbox and read-only toolchains, zero network capabilities under
//!   `fs+net`, and `DeleteAppContainerProfile` on teardown.
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
pub const WINDOWS_INHERITED_ENV_VARS: [&str; 16] = [
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
    "CommonProgramFiles",
    "CommonProgramFiles(x86)",
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
    vars.into_iter()
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
#[cfg_attr(not(any(target_os = "macos", windows)), allow(dead_code))]
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
/// does), so a program that does not exist fails here with a clear message. Never returns the
/// unconfined command: a platform without an implementation yields an error.
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
    #[cfg(windows)]
    {
        let _ = jail;
        let resolved = windows::resolve_program(program, search_path)
            .ok_or_else(|| format!("program '{program}' was not found in PATH"))?;
        Ok((resolved, args.to_vec()))
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        let _ = (jail, program, args, search_path);
        Err(unsupported_message(jail.level))
    }
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
    #[cfg(windows)]
    {
        windows::probe(level, sandbox_base)
            .await
            .map_err(GridError::Config)
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        let _ = sandbox_base;
        Err(GridError::Config(unsupported_message(level)))
    }
}

#[cfg(not(any(target_os = "macos", windows)))]
fn unsupported_message(level: IsolationLevel) -> String {
    let which = if cfg!(target_os = "linux") {
        "FEAT-3b"
    } else if cfg!(windows) {
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
#[cfg(any(target_os = "macos", all(test, unix)))]
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
    use super::{IsolationLevel, Jail};
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

    /// The canonical form of `p`; a path that does not exist (yet) is returned as given, with
    /// its longest existing prefix canonicalized.
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

/// The Windows implementation (design 0033 D4, FEAT-3c): per-task AppContainer profile
/// (`oxideswarm.task.<task_id>`), explicit NTFS DACLs on the sandbox and host toolchains,
/// zero network capabilities under `fs+net`, and `DeleteAppContainerProfile` on cleanup.
#[cfg(windows)]
pub(crate) mod windows {
    use super::{HostDirs, IsolationLevel, Jail};
    use std::collections::{BTreeMap, HashSet, VecDeque};
    use std::ffi::{OsStr, OsString};
    use std::io;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::FromRawHandle;
    use std::os::windows::process::ExitStatusExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, OnceLock};

    use windows_sys::Win32::Foundation::{
        CloseHandle, LocalFree, SetHandleInformation, ERROR_ALREADY_EXISTS, ERROR_SUCCESS,
        GENERIC_EXECUTE, GENERIC_READ, HANDLE, HANDLE_FLAG_INHERIT, HLOCAL, WAIT_OBJECT_0,
    };
    use windows_sys::Win32::Security::Authorization::{
        GetNamedSecurityInfoW, GetSecurityInfo, SetEntriesInAclW, SetNamedSecurityInfoW,
        SetSecurityInfo, DENY_ACCESS, EXPLICIT_ACCESS_W, GRANT_ACCESS, SET_ACCESS, SE_FILE_OBJECT,
        SE_WINDOW_OBJECT, TRUSTEE_IS_SID, TRUSTEE_IS_UNKNOWN, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::Isolation::{
        CreateAppContainerProfile, DeleteAppContainerProfile,
        DeriveAppContainerSidFromAppContainerName,
    };
    use windows_sys::Win32::Security::{
        AclSizeInformation, CreateWellKnownSid, EqualSid, FreeSid, GetAce, GetAclInformation,
        GetTokenInformation, TokenUser, WinBuiltinAdministratorsSid, WinBuiltinAnyPackageSid,
        WinCapabilityInternetClientSid, WinCapabilityPrivateNetworkClientServerSid,
        WinLocalSystemSid, ACCESS_ALLOWED_ACE, ACE_FLAGS, ACE_HEADER, ACL, ACL_SIZE_INFORMATION,
        CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, NO_INHERITANCE, OBJECT_INHERIT_ACE,
        PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES,
        SECURITY_CAPABILITIES, SECURITY_MAX_SID_SIZE, SID_AND_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
        WELL_KNOWN_SID_TYPE,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_ADD_FILE, FILE_ADD_SUBDIRECTORY, FILE_ALL_ACCESS, FILE_DELETE_CHILD,
        FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES, FILE_TRAVERSE, FILE_WRITE_EA, SYNCHRONIZE,
        WRITE_DAC, WRITE_OWNER,
    };
    use windows_sys::Win32::System::Pipes::CreatePipe;
    use windows_sys::Win32::System::SystemServices::{
        ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE, SE_GROUP_ENABLED,
    };
    use windows_sys::Win32::System::Threading::{
        CreateProcessW, DeleteProcThreadAttributeList, GetCurrentProcess, GetCurrentThreadId,
        GetExitCodeProcess, InitializeProcThreadAttributeList, OpenProcessToken, TerminateProcess,
        UpdateProcThreadAttribute, WaitForSingleObject, CREATE_UNICODE_ENVIRONMENT,
        EXTENDED_STARTUPINFO_PRESENT, INFINITE, LPPROC_THREAD_ATTRIBUTE_LIST, PROCESS_INFORMATION,
        PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES,
        STARTF_USESTDHANDLES, STARTUPINFOEXW,
    };

    #[link(name = "user32")]
    extern "system" {
        fn GetProcessWindowStation() -> HANDLE;
        fn GetThreadDesktop(dwThreadId: u32) -> HANDLE;
    }

    static WINSTA_PREPARED: OnceLock<()> = OnceLock::new();
    static PREPARE_LOCK: Mutex<()> = Mutex::new(());
    static PREPARED_TOOLCHAIN_PATHS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    static MSVC_ENV_CACHE: OnceLock<Option<(PathBuf, String)>> = OnceLock::new();

    const HRESULT_ALREADY_EXISTS: i32 =
        ((1u32 << 31) | (7u32 << 16) | (ERROR_ALREADY_EXISTS & 0xffff)) as i32;

    /// Directory permissions that allow listing or modifying a directory (denied on `user_home`
    /// and `.oxideswarm` while leaving `FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE`
    /// permitted for path canonicalization).
    const DIR_LIST_AND_WRITE_MASK: u32 = FILE_LIST_DIRECTORY
        | FILE_ADD_FILE
        | FILE_ADD_SUBDIRECTORY
        | FILE_WRITE_EA
        | FILE_DELETE_CHILD
        | DELETE
        | WRITE_DAC
        | WRITE_OWNER;

    const WINDOWS_EXEC_EXTS: &[&str] = &[".com", ".exe", ".bat", ".cmd"];

    fn prepared_cache() -> &'static Mutex<HashSet<PathBuf>> {
        PREPARED_TOOLCHAIN_PATHS.get_or_init(|| Mutex::new(HashSet::new()))
    }

    fn ensure_window_station_and_desktop_access(any_pkg_sid: PSID) {
        WINSTA_PREPARED.get_or_init(|| unsafe {
            // Grant only read/enumerate/atom attributes on the Window Station (`0x0002_0363`),
            // explicitly omitting `WINSTA_READCLIPBOARD` (0x0004), `WINSTA_ACCESSCLIPBOARD`
            // (0x0008), `WINSTA_WRITEATTRIBUTES` (0x0010), `DELETE`, `WRITE_DAC`, and `WRITE_OWNER`.
            let winsta = GetProcessWindowStation();
            grant_window_object_access(winsta, any_pkg_sid, 0x0002_0363);
            // Grant only standard desktop connection rights (`0x0002_01C3`), explicitly omitting
            // `DESKTOP_HOOKCONTROL` (0x0008), `DESKTOP_JOURNALRECORD` (0x0010),
            // `DESKTOP_JOURNALPLAYBACK` (0x0020), `DELETE`, `WRITE_DAC`, and `WRITE_OWNER`.
            let desktop = GetThreadDesktop(GetCurrentThreadId());
            grant_window_object_access(desktop, any_pkg_sid, 0x0002_01C3);
        });
    }

    fn grant_window_object_access(handle: HANDLE, any_pkg_sid: PSID, access_mask: u32) {
        if handle.is_null() {
            return;
        }
        let mut old_dacl: *mut ACL = std::ptr::null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
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
            if !sd.is_null() {
                unsafe {
                    LocalFree(sd as HLOCAL);
                }
            }
            return;
        }
        let entry = explicit_access(any_pkg_sid, access_mask, SET_ACCESS, NO_INHERITANCE);
        let mut new_dacl: *mut ACL = std::ptr::null_mut();
        let rc = unsafe { SetEntriesInAclW(1, &entry, old_dacl, &mut new_dacl) };
        if rc == ERROR_SUCCESS && !new_dacl.is_null() {
            unsafe {
                let _ = SetSecurityInfo(
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
    }

    /// Resolves the host MSVC `bin\Hostx64\x64` directory and `LIB` search path once on Windows so
    /// `rustc` inside an AppContainer (where COM `CLSID_SetupConfiguration` is blocked) can locate
    /// `link.exe` and the Windows SDK `.lib` files directly via `PATH` and `LIB`.
    pub fn resolve_windows_msvc_env() -> Option<(PathBuf, String)> {
        MSVC_ENV_CACHE
            .get_or_init(|| {
                let mut vs_roots = vec![PathBuf::from(r"C:\BuildTools")];
                for pf_var in ["ProgramFiles", "ProgramFiles(x86)"] {
                    if let Some(pf) = std::env::var_os(pf_var).map(PathBuf::from) {
                        for year in ["2022", "2019"] {
                            for ed in ["Enterprise", "Professional", "Community", "BuildTools"] {
                                vs_roots
                                    .push(pf.join("Microsoft Visual Studio").join(year).join(ed));
                            }
                        }
                    }
                }
                let mut msvc_ver_dir: Option<PathBuf> = None;
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
                            break;
                        }
                    }
                }
                let msvc_ver_dir = msvc_ver_dir?;
                let msvc_bin = msvc_ver_dir.join("bin").join("Hostx64").join("x64");
                let msvc_lib = msvc_ver_dir.join("lib").join("x64");

                let kits_base = PathBuf::from(r"C:\Program Files (x86)\Windows Kits\10\Lib");
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
                Some((msvc_bin, lib_str))
            })
            .clone()
    }

    /// Resolves `program` against `search_path` on Windows. Returns `None` if the executable
    /// does not exist.
    pub(super) fn resolve_program(program: &str, search_path: &str) -> Option<String> {
        let p = Path::new(program);
        let has_sep = program.contains('/') || program.contains('\\') || program.contains(':');
        if has_sep {
            if p.is_file() {
                return Some(program.to_string());
            }
            for ext in WINDOWS_EXEC_EXTS {
                let candidate = PathBuf::from(format!("{program}{ext}"));
                if candidate.is_file() {
                    return Some(candidate.to_string_lossy().into_owned());
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
                return Some(direct.to_string_lossy().into_owned());
            }
            for ext in WINDOWS_EXEC_EXTS {
                let with_ext = base.join(format!("{bare}{ext}"));
                if with_ext.is_file() {
                    return Some(with_ext.to_string_lossy().into_owned());
                }
            }
        }
        None
    }

    fn to_wide(s: &OsStr) -> Vec<u16> {
        s.encode_wide().chain(std::iter::once(0)).collect()
    }

    struct OwnedSid(Vec<u8>);

    impl OwnedSid {
        fn from_well_known(sid_type: WELL_KNOWN_SID_TYPE) -> io::Result<Self> {
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

        fn current_user() -> io::Result<Self> {
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
            let sid_ptr = token_user.User.Sid;
            let len = unsafe { windows_sys::Win32::Security::GetLengthSid(sid_ptr) } as usize;
            let slice = unsafe { std::slice::from_raw_parts(sid_ptr as *const u8, len) };
            Ok(Self(slice.to_vec()))
        }

        fn as_psid(&self) -> PSID {
            self.0.as_ptr() as PSID
        }
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

    fn dacl_already_grants_sid_with_flags(
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

    fn dacl_already_grants_sid(path: &Path, target_sid: PSID, required_mask: u32) -> bool {
        dacl_already_grants_sid_with_flags(path, target_sid, required_mask, 0)
    }

    fn dacl_already_denies_sid(path: &Path, target_sid: PSID, required_mask: u32) -> bool {
        dacl_has_ace(path, target_sid, ACCESS_DENIED_ACE_TYPE, required_mask, 0)
    }

    fn merge_entries_on_path(path: &Path, entries: &[EXPLICIT_ACCESS_W]) -> io::Result<()> {
        if !path.exists() || entries.is_empty() {
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

    fn set_protected_dacl_on_path(path: &Path, entries: &[EXPLICIT_ACCESS_W]) -> io::Result<()> {
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

    fn grant_ancestor_traverse(path: &Path, any_pkg_sid: PSID) {
        let traverse_mask = FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
        let mut cur = path.parent();
        while let Some(dir) = cur {
            if dir.parent().is_none() {
                break;
            }
            if !dacl_already_grants_sid(dir, any_pkg_sid, traverse_mask) {
                let entry =
                    explicit_access(any_pkg_sid, traverse_mask, GRANT_ACCESS, NO_INHERITANCE);
                let _ = merge_entries_on_path(dir, &[entry]);
            }
            cur = dir.parent();
        }
    }

    fn deny_list_and_write_non_inherited(dir: &Path, any_pkg_sid: PSID) {
        if !dir.exists() {
            return;
        }
        let traverse_mask = FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
        if !dacl_already_grants_sid(dir, any_pkg_sid, traverse_mask) {
            let allow = explicit_access(any_pkg_sid, traverse_mask, GRANT_ACCESS, NO_INHERITANCE);
            let _ = merge_entries_on_path(dir, &[allow]);
        }
        if dacl_already_grants_sid(dir, any_pkg_sid, FILE_LIST_DIRECTORY)
            && !dacl_already_denies_sid(dir, any_pkg_sid, DIR_LIST_AND_WRITE_MASK)
        {
            let deny = explicit_access(
                any_pkg_sid,
                DIR_LIST_AND_WRITE_MASK,
                DENY_ACCESS,
                NO_INHERITANCE,
            );
            let _ = merge_entries_on_path(dir, &[deny]);
        }
    }

    fn grant_rx_non_inherited(path: &Path, any_pkg_sid: PSID) {
        if !path.exists() {
            return;
        }
        let rx_mask = GENERIC_READ | GENERIC_EXECUTE;
        if dacl_already_grants_sid(path, any_pkg_sid, FILE_READ_ATTRIBUTES) {
            return;
        }
        let entry = explicit_access(any_pkg_sid, rx_mask, GRANT_ACCESS, NO_INHERITANCE);
        let _ = merge_entries_on_path(path, &[entry]);
    }

    fn grant_rx_recursive_cached(path: &Path, any_pkg_sid: PSID) {
        if !path.exists() {
            return;
        }
        if let Ok(mut guard) = prepared_cache().lock() {
            if guard.contains(path) {
                return;
            }
            let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
            if dacl_already_grants_sid_with_flags(
                path,
                any_pkg_sid,
                FILE_READ_ATTRIBUTES | FILE_TRAVERSE,
                inherit,
            ) {
                guard.insert(path.to_path_buf());
                return;
            }
            let rx_mask = GENERIC_READ | GENERIC_EXECUTE;
            let entry = explicit_access(any_pkg_sid, rx_mask, GRANT_ACCESS, inherit);
            if merge_entries_on_path(path, &[entry]).is_ok() {
                guard.insert(path.to_path_buf());
            }
        }
    }

    fn deny_any_package_on_path(path: &Path, any_pkg_sid: PSID) {
        if !path.exists() {
            return;
        }
        if !dacl_already_denies_sid(path, any_pkg_sid, FILE_ALL_ACCESS)
            || dacl_already_grants_sid(path, any_pkg_sid, FILE_READ_ATTRIBUTES)
        {
            let entry = explicit_access(any_pkg_sid, FILE_ALL_ACCESS, DENY_ACCESS, NO_INHERITANCE);
            let _ = merge_entries_on_path(path, &[entry]);
        }
        if path.is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for e in entries.take(64).flatten() {
                    let child = e.path();
                    if child.is_file()
                        && (!dacl_already_denies_sid(&child, any_pkg_sid, FILE_ALL_ACCESS)
                            || dacl_already_grants_sid(&child, any_pkg_sid, FILE_READ_ATTRIBUTES))
                    {
                        let entry = explicit_access(
                            any_pkg_sid,
                            FILE_ALL_ACCESS,
                            DENY_ACCESS,
                            NO_INHERITANCE,
                        );
                        let _ = merge_entries_on_path(&child, &[entry]);
                    }
                }
            }
        }
    }

    fn parse_cargo_lock_packages(sandbox_path: &Path) -> Vec<String> {
        let lock_path = sandbox_path.join("Cargo.lock");
        let Ok(content) = std::fs::read_to_string(&lock_path) else {
            return Vec::new();
        };
        let mut pkgs = Vec::new();
        let mut cur_name: Option<String> = None;
        let mut cur_ver: Option<String> = None;
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed == "[[package]]" {
                if let (Some(n), Some(v)) = (cur_name.take(), cur_ver.take()) {
                    pkgs.push(format!("{n}-{v}"));
                }
            } else if let Some(rest) = trimmed.strip_prefix("name = ") {
                cur_name = Some(rest.trim_matches('"').to_string());
            } else if let Some(rest) = trimmed.strip_prefix("version = ") {
                cur_ver = Some(rest.trim_matches('"').to_string());
            }
        }
        if let (Some(n), Some(v)) = (cur_name, cur_ver) {
            pkgs.push(format!("{n}-{v}"));
        }
        pkgs
    }

    fn index_prefix_relpath(crate_name: &str) -> PathBuf {
        let lower = crate_name.to_ascii_lowercase();
        match lower.len() {
            1 => PathBuf::from("1").join(&lower),
            2 => PathBuf::from("2").join(&lower),
            3 => PathBuf::from("3").join(&lower[..1]).join(&lower),
            _ => PathBuf::from(&lower[..2]).join(&lower[2..4]).join(&lower),
        }
    }

    fn prepare_one_cargo_dir(sandbox_path: &Path, cargo: &Path, any_pkg_sid: PSID) {
        if !cargo.exists() {
            return;
        }
        grant_ancestor_traverse(cargo, any_pkg_sid);
        grant_rx_non_inherited(cargo, any_pkg_sid);
        for file in [
            "config.toml",
            "config",
            ".package-cache",
            ".package-cache-lock",
        ] {
            grant_rx_non_inherited(&cargo.join(file), any_pkg_sid);
        }
        if let Ok(entries) = std::fs::read_dir(cargo) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_file() {
                    let name = entry.file_name();
                    if !name.eq_ignore_ascii_case("credentials")
                        && !name.eq_ignore_ascii_case("credentials.toml")
                    {
                        grant_rx_non_inherited(&p, any_pkg_sid);
                    }
                }
            }
        }
        grant_rx_recursive_cached(&cargo.join("bin"), any_pkg_sid);
        grant_rx_recursive_cached(&cargo.join("git"), any_pkg_sid);
        grant_rx_recursive_cached(&cargo.join("vendor"), any_pkg_sid);

        let registry = cargo.join("registry");
        if registry.exists() {
            grant_rx_non_inherited(&registry, any_pkg_sid);
            let src_dir = registry.join("src");
            let cache_dir = registry.join("cache");
            let index_dir = registry.join("index");
            grant_rx_non_inherited(&src_dir, any_pkg_sid);
            grant_rx_non_inherited(&cache_dir, any_pkg_sid);
            grant_rx_non_inherited(&index_dir, any_pkg_sid);

            let mut pkgs = parse_cargo_lock_packages(sandbox_path);
            // Also include cached leaf crates used by offline build tests when no Cargo.lock
            // exists yet in the sandbox.
            if let Ok(remotes) = std::fs::read_dir(&cache_dir) {
                for remote in remotes.flatten() {
                    if let Ok(files) = std::fs::read_dir(remote.path()) {
                        for f in files.flatten() {
                            if let Some(stem) = f
                                .file_name()
                                .to_str()
                                .and_then(|s| s.strip_suffix(".crate"))
                            {
                                if let Some((name, _)) = stem.rsplit_once('-') {
                                    if [
                                        "cfg-if",
                                        "itoa",
                                        "bytes",
                                        "memchr",
                                        "smallvec",
                                        "once_cell",
                                    ]
                                    .contains(&name)
                                        && !pkgs.iter().any(|p| p == stem)
                                    {
                                        pkgs.push(stem.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if let Ok(remotes) = std::fs::read_dir(&src_dir) {
                for remote in remotes.flatten() {
                    let remote_path = remote.path();
                    grant_rx_non_inherited(&remote_path, any_pkg_sid);
                    for pkg in &pkgs {
                        grant_rx_recursive_cached(&remote_path.join(pkg), any_pkg_sid);
                    }
                }
            }
            if let Ok(remotes) = std::fs::read_dir(&cache_dir) {
                for remote in remotes.flatten() {
                    let remote_path = remote.path();
                    grant_rx_non_inherited(&remote_path, any_pkg_sid);
                    for pkg in &pkgs {
                        grant_rx_non_inherited(
                            &remote_path.join(format!("{pkg}.crate")),
                            any_pkg_sid,
                        );
                    }
                }
            }
            if let Ok(remotes) = std::fs::read_dir(&index_dir) {
                for remote in remotes.flatten() {
                    let remote_path = remote.path();
                    grant_rx_non_inherited(&remote_path, any_pkg_sid);
                    grant_rx_non_inherited(&remote_path.join("config.json"), any_pkg_sid);
                    let dot_cache = remote_path.join(".cache");
                    grant_rx_non_inherited(&dot_cache, any_pkg_sid);
                    for pkg in &pkgs {
                        if let Some((name, _)) = pkg.rsplit_once('-') {
                            let rel = index_prefix_relpath(name);
                            for root in [&remote_path, &dot_cache] {
                                let target = root.join(&rel);
                                if target.exists() {
                                    let mut cur = target.parent();
                                    while let Some(p) = cur {
                                        if p == root {
                                            break;
                                        }
                                        grant_rx_non_inherited(p, any_pkg_sid);
                                        cur = p.parent();
                                    }
                                    grant_rx_non_inherited(&target, any_pkg_sid);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Explicitly deny access to Cargo registry credentials if they would otherwise be
        // readable by `ALL APPLICATION PACKAGES` (design 0033 D4.4).
        for cred in ["credentials.toml", "credentials"] {
            let p = cargo.join(cred);
            if p.exists() && dacl_already_grants_sid(&p, any_pkg_sid, FILE_READ_ATTRIBUTES) {
                deny_any_package_on_path(&p, any_pkg_sid);
            }
        }
    }

    fn prepare_home_and_toolchains(
        jail: &Jail<'_>,
        cmd_cargo_home: Option<&Path>,
        cmd_rustup_home: Option<&Path>,
        any_pkg_sid: PSID,
    ) {
        let home = &jail.host.user_home;
        if !home.as_os_str().is_empty() && home.exists() {
            deny_list_and_write_non_inherited(home, any_pkg_sid);
            let ox_dir = home.join(".oxideswarm");
            if ox_dir.exists() {
                if jail.sandbox_dir.starts_with(&ox_dir) {
                    deny_list_and_write_non_inherited(&ox_dir, any_pkg_sid);
                    if let Ok(entries) = std::fs::read_dir(&ox_dir) {
                        for entry in entries.flatten() {
                            let p = entry.path();
                            if !jail.sandbox_dir.starts_with(&p)
                                && dacl_already_grants_sid(&p, any_pkg_sid, FILE_READ_ATTRIBUTES)
                            {
                                deny_any_package_on_path(&p, any_pkg_sid);
                            }
                        }
                    }
                } else if dacl_already_grants_sid(&ox_dir, any_pkg_sid, FILE_READ_ATTRIBUTES) {
                    deny_any_package_on_path(&ox_dir, any_pkg_sid);
                }
            }
            for dir_name in [".ssh", ".aws", ".gnupg", ".config"] {
                let sub = home.join(dir_name);
                if sub.exists() && dacl_already_grants_sid(&sub, any_pkg_sid, FILE_READ_ATTRIBUTES)
                {
                    deny_any_package_on_path(&sub, any_pkg_sid);
                }
            }
            if let Ok(entries) = std::fs::read_dir(home) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    let is_allowed_ancestor = jail.sandbox_dir.starts_with(&p)
                        || jail.sandbox_base.starts_with(&p)
                        || jail.host.rustup_home.starts_with(&p)
                        || jail.host.cargo_home.starts_with(&p)
                        || cmd_rustup_home.is_some_and(|r| r.starts_with(&p))
                        || cmd_cargo_home.is_some_and(|c| c.starts_with(&p))
                        || entry.file_name().eq_ignore_ascii_case("AppData");
                    if !is_allowed_ancestor
                        && dacl_already_grants_sid(&p, any_pkg_sid, FILE_READ_ATTRIBUTES)
                    {
                        deny_any_package_on_path(&p, any_pkg_sid);
                    }
                }
            }
        }
        if let Some(account_home) = std::env::var_os("USERPROFILE")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
        {
            if account_home != *home && account_home.exists() {
                deny_list_and_write_non_inherited(&account_home, any_pkg_sid);
            }
        }

        for rustup in [Some(jail.host.rustup_home.as_path()), cmd_rustup_home]
            .into_iter()
            .flatten()
        {
            if rustup.exists() {
                grant_ancestor_traverse(rustup, any_pkg_sid);
                grant_rx_non_inherited(rustup, any_pkg_sid);
                grant_rx_non_inherited(&rustup.join("settings.toml"), any_pkg_sid);
                let tc_root = rustup.join("toolchains");
                grant_rx_non_inherited(&tc_root, any_pkg_sid);
                if let Ok(entries) = std::fs::read_dir(&tc_root) {
                    for entry in entries.flatten() {
                        let tc_dir = entry.path();
                        grant_rx_non_inherited(&tc_dir, any_pkg_sid);
                        grant_rx_recursive_cached(&tc_dir.join("bin"), any_pkg_sid);
                        grant_rx_recursive_cached(&tc_dir.join("lib"), any_pkg_sid);
                        grant_rx_recursive_cached(&tc_dir.join("libexec"), any_pkg_sid);
                    }
                }
            }
        }

        prepare_one_cargo_dir(jail.sandbox_dir, &jail.host.cargo_home, any_pkg_sid);
        if let Some(custom_cargo) = cmd_cargo_home {
            if custom_cargo != jail.host.cargo_home {
                prepare_one_cargo_dir(jail.sandbox_dir, custom_cargo, any_pkg_sid);
            }
        }

        let program_data = std::env::var_os("ProgramData")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"));
        let vs_root = program_data.join("Microsoft").join("VisualStudio");
        for sub in [
            vs_root.join("Setup"),
            vs_root.join("Packages").join("_Instances"),
        ] {
            if sub.exists() {
                grant_ancestor_traverse(&sub, any_pkg_sid);
                grant_rx_recursive_cached(&sub, any_pkg_sid);
            }
        }

        // Explicitly deny `ALL APPLICATION PACKAGES` on every protected file (such as a cluster
        // secret file or config file located outside the denied home, or inside a toolchain
        // directory).
        for file in jail.protected_files {
            if !file.as_os_str().is_empty() && file.exists() {
                deny_any_package_on_path(file, any_pkg_sid);
            }
        }
    }

    fn apply_dacl_to_existing_tree(root: &Path, entries: &[EXPLICIT_ACCESS_W]) -> io::Result<()> {
        set_protected_dacl_on_path(root, entries)?;
        let mut queue = VecDeque::new();
        queue.push_back(root.to_path_buf());
        while let Some(dir) = queue.pop_front() {
            let Ok(read_dir) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in read_dir.flatten() {
                let path = entry.path();
                let _ = set_protected_dacl_on_path(&path, entries);
                if path.is_dir() {
                    queue.push_back(path);
                }
            }
        }
        Ok(())
    }

    /// Per-task Windows AppContainer state that must outlive `CreateProcessW` and deletes the
    /// AppContainer profile when the task's `ProcessTree` is dropped.
    pub struct WindowsAppContainerGuard {
        wide_profile_name: Vec<u16>,
        app_container_sid: PSID,
        _capability_sids: Vec<OwnedSid>,
        _capability_attrs: Vec<SID_AND_ATTRIBUTES>,
        security_capabilities: Box<SECURITY_CAPABILITIES>,
        cleaned_up: bool,
    }

    // SAFETY: WindowsAppContainerGuard owns its heap allocations and raw pointers are only read
    // synchronously during `CreateProcessW` before `spawn()` returns.
    unsafe impl Send for WindowsAppContainerGuard {}
    unsafe impl Sync for WindowsAppContainerGuard {}

    impl WindowsAppContainerGuard {
        pub(crate) fn create(
            task_id: rusty_grid_core::TaskId,
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

            let profile_name = format!("oxideswarm.task.{}", task_id.0);
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
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("CreateAppContainerProfile({profile_name}) failed: HRESULT 0x{hr:08x}"),
                ));
            }

            let user_sid = OwnedSid::current_user()?;
            let system_sid = OwnedSid::from_well_known(WinLocalSystemSid)?;
            let admins_sid = OwnedSid::from_well_known(WinBuiltinAdministratorsSid)?;
            let any_pkg_sid = OwnedSid::from_well_known(WinBuiltinAnyPackageSid)?;

            ensure_window_station_and_desktop_access(any_pkg_sid.as_psid());

            let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
            let owner_only_entries = [
                explicit_access(user_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                explicit_access(system_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                explicit_access(admins_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
            ];

            {
                let _guard = PREPARE_LOCK
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());

                // 1. Protect `<sandbox_base>` so sibling sandboxes cannot be listed or read.
                let sandbox_base = jail.sandbox_base;
                if sandbox_base.exists() {
                    grant_ancestor_traverse(sandbox_base, any_pkg_sid.as_psid());
                    let traverse_mask = FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
                    if !dacl_already_grants_sid(sandbox_base, any_pkg_sid.as_psid(), traverse_mask)
                        || dacl_already_grants_sid(
                            sandbox_base,
                            any_pkg_sid.as_psid(),
                            FILE_LIST_DIRECTORY,
                        )
                    {
                        let base_entries = [
                            owner_only_entries[0],
                            owner_only_entries[1],
                            owner_only_entries[2],
                            explicit_access(
                                any_pkg_sid.as_psid(),
                                traverse_mask,
                                SET_ACCESS,
                                NO_INHERITANCE,
                            ),
                        ];
                        let _ = set_protected_dacl_on_path(sandbox_base, &base_entries);
                    }

                    if let Ok(entries) = std::fs::read_dir(sandbox_base) {
                        for entry in entries.flatten() {
                            let p = entry.path();
                            if p == jail.sandbox_dir {
                                continue;
                            }
                            if dacl_already_grants_sid(
                                &p,
                                any_pkg_sid.as_psid(),
                                FILE_READ_ATTRIBUTES,
                            ) {
                                let _ = apply_dacl_to_existing_tree(&p, &owner_only_entries);
                            }
                        }
                    }
                }

                // 2. Grant read+execute on host Rust/Cargo toolchains and deny access to secrets.
                prepare_home_and_toolchains(
                    jail,
                    cmd_cargo_home.as_deref(),
                    cmd_rustup_home.as_deref(),
                    any_pkg_sid.as_psid(),
                );

                // 3. If `program` is an explicit path outside C:\Windows (e.g. Python in
                //    %LOCALAPPDATA%), ensure the executable and its stdlib/DLLs are RX.
                let prog_path = Path::new(&program);
                if prog_path.is_absolute() && prog_path.exists() {
                    grant_ancestor_traverse(prog_path, any_pkg_sid.as_psid());
                    grant_rx_non_inherited(prog_path, any_pkg_sid.as_psid());
                    if let Some(prog_dir) = prog_path.parent() {
                        let is_home = prog_dir == jail.host.user_home;
                        let in_sandbox_base = prog_dir.starts_with(jail.sandbox_base);
                        if !is_home && !in_sandbox_base {
                            grant_rx_non_inherited(prog_dir, any_pkg_sid.as_psid());
                            if let Ok(entries) = std::fs::read_dir(prog_dir) {
                                for entry in entries.flatten() {
                                    let p = entry.path();
                                    if p.is_file() {
                                        grant_rx_non_inherited(&p, any_pkg_sid.as_psid());
                                    } else if p.is_dir() {
                                        let name = entry.file_name();
                                        if name.eq_ignore_ascii_case("Lib") {
                                            grant_rx_non_inherited(&p, any_pkg_sid.as_psid());
                                            if let Ok(lib_entries) = std::fs::read_dir(&p) {
                                                for le in lib_entries.flatten() {
                                                    let lp = le.path();
                                                    let lname = le.file_name();
                                                    if lname.eq_ignore_ascii_case("site-packages")
                                                        || lname.eq_ignore_ascii_case("test")
                                                    {
                                                        continue;
                                                    }
                                                    if lp.is_file() {
                                                        grant_rx_non_inherited(
                                                            &lp,
                                                            any_pkg_sid.as_psid(),
                                                        );
                                                    } else if lp.is_dir() {
                                                        grant_rx_recursive_cached(
                                                            &lp,
                                                            any_pkg_sid.as_psid(),
                                                        );
                                                    }
                                                }
                                            }
                                        } else if name.eq_ignore_ascii_case("DLLs")
                                            || name.eq_ignore_ascii_case("libs")
                                            || name.eq_ignore_ascii_case("bin")
                                        {
                                            grant_rx_recursive_cached(&p, any_pkg_sid.as_psid());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // 4. Set a protected DACL on `<sandbox_dir>` granting full control only to
            //    CurrentUser, SYSTEM, Administrators, and this task's specific AppContainer SID.
            grant_ancestor_traverse(jail.sandbox_dir, any_pkg_sid.as_psid());
            let sandbox_entries = [
                owner_only_entries[0],
                owner_only_entries[1],
                owner_only_entries[2],
                explicit_access(app_container_sid, FILE_ALL_ACCESS, SET_ACCESS, inherit),
            ];
            apply_dacl_to_existing_tree(jail.sandbox_dir, &sandbox_entries)?;

            // 5. Build SECURITY_CAPABILITIES for `Fs` (outbound + LAN client/server) or `FsNet` (0 caps).
            let mut capability_sids = Vec::new();
            let mut capability_attrs = Vec::new();
            if jail.level == IsolationLevel::Fs {
                let net_client = OwnedSid::from_well_known(WinCapabilityInternetClientSid)?;
                let net_private =
                    OwnedSid::from_well_known(WinCapabilityPrivateNetworkClientServerSid)?;
                capability_attrs.push(SID_AND_ATTRIBUTES {
                    Sid: net_client.as_psid(),
                    Attributes: SE_GROUP_ENABLED as u32,
                });
                capability_attrs.push(SID_AND_ATTRIBUTES {
                    Sid: net_private.as_psid(),
                    Attributes: SE_GROUP_ENABLED as u32,
                });
                capability_sids.push(net_client);
                capability_sids.push(net_private);
            }

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
                wide_profile_name,
                app_container_sid,
                _capability_sids: capability_sids,
                _capability_attrs: capability_attrs,
                security_capabilities,
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
                creation_flags,
            )
        }

        pub fn cleanup(&mut self) {
            if self.cleaned_up {
                return;
            }
            self.cleaned_up = true;
            unsafe {
                if !self.app_container_sid.is_null() {
                    FreeSid(self.app_container_sid);
                    self.app_container_sid = std::ptr::null_mut();
                }
                let hr = DeleteAppContainerProfile(self.wide_profile_name.as_ptr());
                if hr < 0 {
                    let wide_name = std::mem::take(&mut self.wide_profile_name);
                    std::thread::spawn(move || {
                        for _ in 0..10 {
                            std::thread::sleep(std::time::Duration::from_millis(25));
                            if DeleteAppContainerProfile(wide_name.as_ptr()) >= 0 {
                                break;
                            }
                        }
                    });
                }
            }
        }
    }

    impl Drop for WindowsAppContainerGuard {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    /// Start-up self-check on Windows: creates a probe AppContainer profile and runs a probe
    /// command inside `sandbox_base/.isolation-probe-<pid>` verifying that the task can write
    /// inside its sandbox and cannot write into `sandbox_base`.
    pub(super) async fn probe(level: IsolationLevel, sandbox_base: &Path) -> Result<(), String> {
        let fail =
            |what: String| format!("--isolation {level} cannot be enforced on this host: {what}");
        let probe_dir = sandbox_base.join(format!(".isolation-probe-{}", std::process::id()));
        std::fs::create_dir_all(&probe_dir).map_err(|e| {
            format!(
                "--isolation {level}: cannot create the probe directory {}: {e}",
                probe_dir.display()
            )
        })?;
        let outside_name = format!(".isolation-outside-{}", std::process::id());
        let outside_file = sandbox_base.join(&outside_name);
        let outcome = async {
            let host =
                HostDirs::from_home(PathBuf::new(), Some(PathBuf::new()), Some(PathBuf::new()));
            let jail = Jail {
                level,
                host: &host,
                protected_files: &[],
                sandbox_base,
                sandbox_dir: &probe_dir,
            };
            let comspec = std::env::var("COMSPEC")
                .unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".to_string());
            let cmd_line = format!(
                "echo ok > inside && (echo no > ..\\{outside_name} 2>nul && exit /b 3 || exit /b 0)"
            );
            let mut cmd = tokio::process::Command::new(&comspec);
            cmd.args(["/C", &cmd_line]);
            cmd.current_dir(&probe_dir);
            cmd.env_clear();
            for (name, value) in super::inherited_environment(std::env::vars_os()) {
                cmd.env(name, value);
            }
            let probe_id = rusty_grid_core::TaskId::new();
            let guard = WindowsAppContainerGuard::create(probe_id, &jail, &cmd)
                .map_err(|e| fail(format!("AppContainer setup failed: {e}")))?;
            let mut child = guard
                .spawn_in_appcontainer(&mut cmd, 0)
                .map_err(|e| fail(format!("could not spawn AppContainer probe: {e}")))?;
            let status = child
                .wait()
                .await
                .map_err(|e| fail(format!("waiting for AppContainer probe failed: {e}")))?;
            drop(guard);
            match status.code() {
                Some(0) if probe_dir.join("inside").exists() && !outside_file.exists() => Ok(()),
                Some(3) => Err(fail(
                    "the probe wrote outside its sandbox, so the AppContainer DACL was not enforced"
                        .to_string(),
                )),
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

    fn create_pipe_pair(child_is_read: bool) -> io::Result<(HANDLE, HANDLE)> {
        let sa = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: std::ptr::null_mut(),
            bInheritHandle: 1,
        };
        let mut read_pipe: HANDLE = std::ptr::null_mut();
        let mut write_pipe: HANDLE = std::ptr::null_mut();
        let ok = unsafe { CreatePipe(&mut read_pipe, &mut write_pipe, &sa, 0) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let parent_handle = if child_is_read { write_pipe } else { read_pipe };
        if unsafe { SetHandleInformation(parent_handle, HANDLE_FLAG_INHERIT, 0) } == 0 {
            let err = io::Error::last_os_error();
            unsafe {
                CloseHandle(read_pipe);
                CloseHandle(write_pipe);
            }
            return Err(err);
        }
        Ok((read_pipe, write_pipe))
    }

    fn spawn_with_security_capabilities(
        std_cmd: &std::process::Command,
        sc: &SECURITY_CAPABILITIES,
        creation_flags: u32,
    ) -> io::Result<AppContainerChild> {
        let (stdin_read, stdin_write) = create_pipe_pair(true)?;
        let (stdout_read, stdout_write) = match create_pipe_pair(false) {
            Ok(p) => p,
            Err(e) => {
                unsafe {
                    CloseHandle(stdin_read);
                    CloseHandle(stdin_write);
                }
                return Err(e);
            }
        };
        let (stderr_read, stderr_write) = match create_pipe_pair(false) {
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

        let mut inherited_handles: [HANDLE; 3] = [stdin_read, stdout_write, stderr_write];
        let mut attr_size: usize = 0;
        unsafe {
            let _ = InitializeProcThreadAttributeList(std::ptr::null_mut(), 2, 0, &mut attr_size);
        }
        let mut attr_buf = vec![0u8; attr_size.max(64)];
        let attr_list = attr_buf.as_mut_ptr() as LPPROC_THREAD_ATTRIBUTE_LIST;
        if unsafe { InitializeProcThreadAttributeList(attr_list, 2, 0, &mut attr_size) } == 0 {
            let err = io::Error::last_os_error();
            unsafe {
                CloseHandle(stdin_read);
                CloseHandle(stdin_write);
                CloseHandle(stdout_read);
                CloseHandle(stdout_write);
                CloseHandle(stderr_read);
                CloseHandle(stderr_write);
            }
            return Err(err);
        }

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
        if sc_ok == 0 || hl_ok == 0 {
            let err = io::Error::last_os_error();
            unsafe {
                DeleteProcThreadAttributeList(attr_list);
                CloseHandle(stdin_read);
                CloseHandle(stdin_write);
                CloseHandle(stdout_read);
                CloseHandle(stdout_write);
                CloseHandle(stderr_read);
                CloseHandle(stderr_write);
            }
            return Err(err);
        }

        let mut cmdline = build_windows_command_line(std_cmd);
        let env_block = build_windows_env_block(std_cmd);
        let cwd_wide = std_cmd.get_current_dir().map(|p| to_wide(p.as_os_str()));
        let cwd_ptr = cwd_wide.as_ref().map_or(std::ptr::null(), |w| w.as_ptr());

        let mut si_ex: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
        si_ex.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
        si_ex.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        si_ex.StartupInfo.hStdInput = stdin_read;
        si_ex.StartupInfo.hStdOutput = stdout_write;
        si_ex.StartupInfo.hStdError = stderr_write;
        si_ex.lpAttributeList = attr_list;

        let mut pi: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
        let flags = creation_flags | EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT;
        let spawned = unsafe {
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
        };
        let spawn_err = if spawned == 0 {
            Some(io::Error::last_os_error())
        } else {
            None
        };

        unsafe {
            DeleteProcThreadAttributeList(attr_list);
            CloseHandle(stdin_read);
            CloseHandle(stdout_write);
            CloseHandle(stderr_write);
        }

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

    #[cfg(not(any(target_os = "macos", windows)))]
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

    #[cfg(not(any(target_os = "macos", windows)))]
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

    #[cfg(any(target_os = "macos", windows))]
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
}
