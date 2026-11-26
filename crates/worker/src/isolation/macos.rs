//! macOS `sandbox-exec` (Seatbelt) process confinement implementation.

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

/// Mach services a confined task may look up; every other lookup is denied (SEC-12).
///
/// Found empirically (denials read from the unified log, `log show --predicate
/// 'eventMessage CONTAINS "mach-lookup"'`) by running the macOS isolation tests, a
/// `cargo build --offline` with and without a cached dependency, `cc` compile and link,
/// `git init/add/commit/log`, `curl` over TLS and a DNS lookup with all lookups denied:
/// `cargo`, `rustc`, `git`, `cc` and `ld` need no service. What stops working: user and
/// group names (`id -un` prints the uid: `com.apple.system.opendirectoryd.libinfo`),
/// `confstr(_CS_DARWIN_USER_TEMP_DIR)` (`com.apple.bsd.dirhelper`) and `os_log`
/// (`com.apple.logd`), none of which a build needs (`TMPDIR` points into the sandbox).
/// Add a name only with the reason, the test that needs it, and what the service could do
/// for a task.
const MACH_SERVICES: &[&str] = &[
    // Metal shader compilation for GPU tasks (design 0074, `gemm_bench` under `fs`): with
    // the lookup denied wgpu fails with "Parent device is lost". The service compiles
    // shaders for the calling process and cannot start arbitrary programs.
    "com.apple.MTLCompilerService",
];

/// The Seatbelt profile text. All paths arrive as `-D` parameters (`SANDBOX_DIR`,
/// `EXTRA_WRITABLE_DIR`, `USER_HOME`, `SANDBOX_BASE`, `CACHE_DIR`, `RUSTUP_HOME`,
/// `CARGO_HOME`, `CARGO_CRED`, `CARGO_CRED_TOML`, `EXTRA_READ_<n>` for `extra_read` entries,
/// `PROTECTED_<n>`, `ANCESTOR_<n>` for `ancestors` entries), so no path is ever spliced into
/// the profile text.
pub(super) fn profile(
    level: IsolationLevel,
    has_extra_writable: bool,
    ancestors: usize,
    extra_denied: usize,
    extra_read: usize,
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
    p.push_str("    (subpath (param \"RUSTUP_HOME\"))\n    (subpath (param \"CARGO_HOME\"))");
    for i in 0..extra_read {
        p.push_str(&format!("\n    (subpath (param \"EXTRA_READ_{i}\"))"));
    }
    p.push_str(")\n");
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
    // - Mach/XPC services (SEC-12): every lookup is denied, so a task cannot ask launchd or
    //   another XPC service to start a process outside the sandbox. `MACH_SERVICES` lists
    //   the exceptions.
    p.push_str("(deny mach-lookup)\n");
    if !MACH_SERVICES.is_empty() {
        p.push_str("(allow mach-lookup\n");
        for name in MACH_SERVICES {
            p.push_str(&format!("    (global-name \"{name}\")\n"));
        }
        p.push_str(")\n");
    }
    // - Creating launchd jobs (`launchctl submit`, `SMJobSubmit`). Seatbelt already refuses
    //   it under `(allow default)` (the log shows `deny(1) job-creation`); named so that the
    //   refusal does not depend on that default.
    p.push_str("(deny job-creation)\n");
    // - The clipboard (stays denied even if an allowed prefix ever covered it).
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
pub(super) fn account_home() -> Option<PathBuf> {
    use std::ffi::CStr;
    let mut buf = vec![0u8; 4096];
    // SAFETY: `libc::passwd` holds only integers and raw pointers, for which all-zero bits (0
    // and null) are a valid value; `getpwuid_r` overwrites it before a field is used.
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
pub(super) fn account_home() -> Option<PathBuf> {
    None
}

/// The per-user temporary directory (`DARWIN_USER_TEMP_DIR`, `/var/folders/../T`).
#[cfg(target_os = "macos")]
pub(super) fn user_temp_dir() -> Option<PathBuf> {
    let mut buf = vec![0 as libc::c_char; 1024];
    // SAFETY: the buffer is valid for `len` bytes.
    let n = unsafe { libc::confstr(libc::_CS_DARWIN_USER_TEMP_DIR, buf.as_mut_ptr(), buf.len()) };
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
pub(super) fn user_temp_dir() -> Option<PathBuf> {
    None
}

/// The per-user cache directory (`DARWIN_USER_CACHE_DIR`, `/var/folders/../C`), next to the
/// temp directory.
#[cfg(target_os = "macos")]
pub(super) fn user_cache_dir() -> Option<PathBuf> {
    let mut buf = vec![0 as libc::c_char; 1024];
    // SAFETY: the buffer is valid for `len` bytes.
    let n = unsafe { libc::confstr(libc::_CS_DARWIN_USER_CACHE_DIR, buf.as_mut_ptr(), buf.len()) };
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
pub(super) fn user_cache_dir() -> Option<PathBuf> {
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

    let mut extra_read: Vec<PathBuf> = Vec::new();
    for dir in &jail.host.extra_read_only_dirs {
        if dir.as_os_str().is_empty() {
            continue;
        }
        let dir = canonical(dir);
        if !extra_read.contains(&dir) {
            extra_read.push(dir);
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
    targets.extend(extra_read.iter());
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
            extra_read.len(),
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
    for (i, dir) in extra_read.iter().enumerate() {
        define(&format!("EXTRA_READ_{i}"), dir);
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
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args)
            .current_dir(&probe_dir)
            .stdin(std::process::Stdio::null());
        let output = rusty_grid_core::process_spawn::output_tokio(&mut cmd)
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
