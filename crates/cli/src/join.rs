//! Join a machine to a cluster with one token (JOIN-1, design record
//! `docs/design/0048-join-token.md`).
//!
//! `oxideswarm secret join-token` (master side) prints one string that carries the master's address,
//! optionally its P2P ticket, and the cluster secret. `oxideswarm join` (new machine) reads that
//! string from standard input, stores the secret in `~/.oxideswarm/cluster.secret`, saves the
//! address (and ticket) in `~/.oxideswarm/rusty-grid.toml`, and runs the `doctor` checks.
//!
//! The token is as sensitive as the secret. Nothing in this module logs, prints or formats a token
//! or a secret: errors describe what is wrong, never the text.

use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use clap::Parser;
use serde::{Deserialize, Serialize};

use rusty_grid_core::cluster_auth::{ClusterSecret, ENV_SECRET, ENV_SECRET_FILE};
use rusty_grid_core::error::{GridError, GridResult};

use crate::config::{default_oxideswarm_dir, ConfigFile, SecretSection};
use crate::connect::{normalize_master_addr, AUTH_EXIT_CODE};
use crate::doctor::{run_doctor, DoctorOptions};
use crate::secret_cmd::SecretOpts;

/// Every join token starts with this prefix (`oxs1` = version 1 of the OxideSwarm secret formats).
pub const TOKEN_PREFIX: &str = "oxs1-join:";
/// Label of the integrity tag: `HMAC-SHA256(secret, TAG_LABEL || payload text)`.
const TAG_LABEL: &str = "oxs1-join-token:";
/// Bytes of the tag kept in the token (rendered as hex).
const TAG_BYTES: usize = 16;
/// The longest token `oxideswarm join` reads from standard input.
const MAX_TOKEN_BYTES: u64 = 8 * 1024;
/// Longest address or ticket a token may carry.
const MAX_FIELD_LEN: usize = 2048;
/// File the address is saved in, inside `~/.oxideswarm`.
const CONFIG_FILE_NAME: &str = "rusty-grid.toml";

/// What a token carries.
pub struct JoinToken {
    /// Address of the master's grid port (`host:port`), if the token has one.
    pub master: Option<String>,
    /// P2P connection ticket of the master, if the token has one.
    pub ticket: Option<String>,
    pub secret: ClusterSecret,
}

impl std::fmt::Debug for JoinToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinToken")
            .field("master", &self.master)
            .field("ticket", &self.ticket.as_ref().map(|_| "<present>"))
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
struct Payload {
    /// Master address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    m: Option<String>,
    /// P2P ticket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    t: Option<String>,
    /// Secret as hex.
    s: String,
}

/// Why a token was refused. The texts never contain the token.
#[derive(Debug, PartialEq, Eq)]
pub enum TokenError {
    NotAToken,
    Corrupt,
    Invalid(&'static str),
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenError::NotAToken => write!(f, "this is not a join token (it must start with {TOKEN_PREFIX})"),
            TokenError::Corrupt => f.write_str(
                "the join token is damaged: its integrity check failed (a character was changed, lost or added while copying). Create it again with `oxideswarm secret join-token` and copy it whole",
            ),
            TokenError::Invalid(why) => write!(f, "the join token is invalid: {why}"),
        }
    }
}

fn tag_of(secret: &ClusterSecret, payload_text: &str) -> String {
    let full = secret.derive(&format!("{TAG_LABEL}{payload_text}"));
    full[..TAG_BYTES]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn field_ok(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_FIELD_LEN && !value.chars().any(char::is_whitespace)
}

impl JoinToken {
    /// The token text: `oxs1-join:<base64url payload>.<hex tag>`.
    pub fn encode(&self) -> String {
        let payload = Payload {
            m: self.master.clone(),
            t: self.ticket.clone(),
            s: self.secret.to_hex(),
        };
        let json = serde_json::to_vec(&payload).expect("a payload of strings serializes");
        let text = URL_SAFE_NO_PAD.encode(json);
        let tag = tag_of(&self.secret, &text);
        format!("{TOKEN_PREFIX}{text}.{tag}")
    }

    /// Parses and verifies a token. Surrounding whitespace is ignored.
    pub fn decode(token: &str) -> Result<Self, TokenError> {
        let body = token
            .trim()
            .strip_prefix(TOKEN_PREFIX)
            .ok_or(TokenError::NotAToken)?;
        let (text, tag) = body.rsplit_once('.').ok_or(TokenError::Corrupt)?;
        let json = URL_SAFE_NO_PAD
            .decode(text)
            .map_err(|_| TokenError::Corrupt)?;
        let payload: Payload = serde_json::from_slice(&json).map_err(|_| TokenError::Corrupt)?;
        let secret = ClusterSecret::from_hex(&payload.s).map_err(|_| TokenError::Corrupt)?;
        // Not a secret-dependent branch worth hiding (the key is inside the token), but compare
        // without early exit anyway.
        let expected = tag_of(&secret, text);
        let same = expected.len() == tag.len()
            && expected
                .bytes()
                .zip(tag.bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0;
        if !same {
            return Err(TokenError::Corrupt);
        }
        if payload.m.is_none() && payload.t.is_none() {
            return Err(TokenError::Invalid(
                "it has neither a master address nor a P2P ticket",
            ));
        }
        if payload.m.as_deref().is_some_and(|m| !field_ok(m)) {
            return Err(TokenError::Invalid("the master address is malformed"));
        }
        if payload.t.as_deref().is_some_and(|t| !field_ok(t)) {
            return Err(TokenError::Invalid("the P2P ticket is malformed"));
        }
        Ok(Self {
            master: payload.m,
            ticket: payload.t,
            secret,
        })
    }
}

fn clean_token(text: &str) -> GridResult<String> {
    if text.len() as u64 > MAX_TOKEN_BYTES {
        return Err(GridError::Config(
            "the input is too long to be a join token".into(),
        ));
    }
    let text = text.trim().to_string();
    if text.is_empty() {
        return Err(GridError::Config(
            "the input is empty: pipe the join token into this command or paste it at the prompt"
                .into(),
        ));
    }
    Ok(text)
}

/// Reads at most [`MAX_TOKEN_BYTES`] from `reader` (piped standard input) and returns the trimmed
/// text. Never echoes it.
pub fn read_token(reader: impl Read) -> GridResult<String> {
    let mut text = String::new();
    reader
        .take(MAX_TOKEN_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|e| {
            GridError::Config(format!(
                "cannot read the join token from standard input: {e}"
            ))
        })?;
    clean_token(&text)
}

/// Switches the terminal behind a file descriptor (Unix) or console handle (Windows) to no-echo
/// and restores the previous mode when dropped. [`EchoOff::stdin`] also restores it when the
/// process is interrupted (SIGINT, SIGTERM, SIGHUP on Unix; Ctrl-C, Ctrl-Break and closing the
/// console on Windows) while the guard lives, and then lets the signal take its normal effect, so
/// the exit status is the conventional one and the terminal is not left without echo.
pub struct EchoOff {
    #[cfg(unix)]
    fd: std::os::fd::RawFd,
    #[cfg(unix)]
    saved: libc::termios,
    #[cfg(unix)]
    signals: Option<unix_signals::Guard>,
    #[cfg(windows)]
    handle: windows_sys::Win32::Foundation::HANDLE,
    #[cfg(windows)]
    saved: u32,
    #[cfg(windows)]
    ctrl: Option<windows_ctrl::Guard>,
}

#[cfg(unix)]
mod unix_signals {
    use std::cell::UnsafeCell;
    use std::mem::MaybeUninit;
    use std::sync::atomic::{AtomicI32, Ordering};

    const SIGNALS: [libc::c_int; 3] = [libc::SIGINT, libc::SIGTERM, libc::SIGHUP];

    struct Saved(UnsafeCell<MaybeUninit<libc::termios>>);
    // SAFETY: written only while `FD` is -1 (before the handlers are installed) and read by the
    // handler only after `FD` was published with Release ordering.
    unsafe impl Sync for Saved {}

    static FD: AtomicI32 = AtomicI32::new(-1);
    static TERMIOS: Saved = Saved(UnsafeCell::new(MaybeUninit::uninit()));

    /// Async-signal-safe: restores the terminal, then re-raises the signal with its default action.
    extern "C" fn restore_and_die(sig: libc::c_int) {
        let fd = FD.load(Ordering::Acquire);
        if fd >= 0 {
            // SAFETY: `tcsetattr` is async-signal-safe; TERMIOS was initialised before FD was set.
            unsafe { libc::tcsetattr(fd, libc::TCSANOW, (*TERMIOS.0.get()).as_ptr()) };
        }
        // SAFETY: `signal` and `raise` are async-signal-safe. The signal is blocked while this
        // handler runs, so the re-raised signal is delivered, with the default action (the
        // process ends with the conventional status of that signal), when the handler returns.
        unsafe {
            libc::signal(sig, libc::SIG_DFL);
            libc::raise(sig);
        }
    }

    /// The handlers installed for the lifetime of the guard; dropping it puts the old ones back.
    pub struct Guard {
        old: Vec<(libc::c_int, libc::sigaction)>,
    }

    impl Guard {
        pub fn install(fd: std::os::fd::RawFd, saved: &libc::termios) -> Self {
            // SAFETY: no handler can run yet (FD is -1), so nothing reads TERMIOS concurrently.
            unsafe { (*TERMIOS.0.get()).write(*saved) };
            FD.store(fd, Ordering::Release);
            let mut old = Vec::new();
            for sig in SIGNALS {
                // SAFETY: `sigaction` is plain data; the handler has the required signature.
                unsafe {
                    let mut action: libc::sigaction = std::mem::zeroed();
                    action.sa_sigaction = restore_and_die as extern "C" fn(libc::c_int) as usize;
                    libc::sigemptyset(&mut action.sa_mask);
                    let mut previous: libc::sigaction = std::mem::zeroed();
                    if libc::sigaction(sig, &action, &mut previous) == 0 {
                        old.push((sig, previous));
                    }
                }
            }
            Self { old }
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            for (sig, previous) in &self.old {
                // SAFETY: puts back the action that `sigaction` returned for this signal.
                unsafe { libc::sigaction(*sig, previous, std::ptr::null_mut()) };
            }
            FD.store(-1, Ordering::Release);
        }
    }
}

#[cfg(unix)]
impl EchoOff {
    /// Turns echo off on the terminal `fd` (the line stays canonical, so Enter still ends it).
    pub fn on_fd(fd: std::os::fd::RawFd) -> std::io::Result<Self> {
        // SAFETY: `termios` is plain data; `tcgetattr` fills it or fails.
        let mut saved: libc::termios = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(fd, &mut saved) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut quiet = saved;
        quiet.c_lflag &= !libc::ECHO;
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &quiet) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            fd,
            saved,
            signals: None,
        })
    }

    /// Standard input, with the restore-on-signal handlers installed until the guard is dropped.
    pub fn stdin() -> std::io::Result<Self> {
        let mut guard = Self::on_fd(0)?;
        guard.signals = Some(unix_signals::Guard::install(0, &guard.saved));
        Ok(guard)
    }
}

#[cfg(unix)]
impl Drop for EchoOff {
    fn drop(&mut self) {
        // SAFETY: restores the attributes read in `on_fd`.
        unsafe { libc::tcsetattr(self.fd, libc::TCSANOW, &self.saved) };
        self.signals = None; // puts the previous signal handlers back
    }
}

#[cfg(windows)]
mod windows_ctrl {
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::Console::{SetConsoleCtrlHandler, SetConsoleMode};

    static HANDLE_VALUE: AtomicUsize = AtomicUsize::new(0);
    static MODE: AtomicU32 = AtomicU32::new(0);

    /// Restores the console mode and returns FALSE, so the next handler (by default: end the
    /// process) still runs.
    unsafe extern "system" fn restore(_ctrl_type: u32) -> i32 {
        let handle = HANDLE_VALUE.load(Ordering::Acquire);
        if handle != 0 {
            // SAFETY: restores the mode saved in `install` on the same handle.
            unsafe { SetConsoleMode(handle as HANDLE, MODE.load(Ordering::Acquire)) };
        }
        0
    }

    pub struct Guard;

    impl Guard {
        pub fn install(handle: HANDLE, mode: u32) -> Self {
            MODE.store(mode, Ordering::Release);
            HANDLE_VALUE.store(handle as usize, Ordering::Release);
            // SAFETY: registers a handler with the required signature.
            unsafe { SetConsoleCtrlHandler(Some(restore), 1) };
            Guard
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            // SAFETY: removes the handler added in `install`.
            unsafe { SetConsoleCtrlHandler(Some(restore), 0) };
            HANDLE_VALUE.store(0, Ordering::Release);
        }
    }
}

#[cfg(windows)]
impl EchoOff {
    /// Standard input: clears `ENABLE_ECHO_INPUT` of the console mode.
    pub fn stdin() -> std::io::Result<Self> {
        use windows_sys::Win32::System::Console::{
            GetConsoleMode, GetStdHandle, SetConsoleMode, ENABLE_ECHO_INPUT, STD_INPUT_HANDLE,
        };
        // SAFETY: plain Win32 calls on the process's own standard input handle.
        unsafe {
            let handle = GetStdHandle(STD_INPUT_HANDLE);
            let mut saved = 0u32;
            if GetConsoleMode(handle, &mut saved) == 0 {
                return Err(std::io::Error::last_os_error());
            }
            if SetConsoleMode(handle, saved & !ENABLE_ECHO_INPUT) == 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Self {
                handle,
                saved,
                ctrl: Some(windows_ctrl::Guard::install(handle, saved)),
            })
        }
    }
}

#[cfg(windows)]
impl Drop for EchoOff {
    fn drop(&mut self) {
        // SAFETY: restores the mode read in `stdin`.
        unsafe { windows_sys::Win32::System::Console::SetConsoleMode(self.handle, self.saved) };
        self.ctrl = None; // removes the Ctrl-C handler
    }
}

/// Prompts on stderr and reads one line from the terminal on standard input with echo off, so a
/// pasted token is not shown on screen. Fails (rather than reading with echo) when echo cannot be
/// turned off.
#[cfg(any(unix, windows))]
pub fn read_token_hidden() -> GridResult<String> {
    use std::io::{BufRead, Write};
    let guard = EchoOff::stdin().map_err(|e| {
        GridError::Config(format!(
            "cannot turn off terminal echo ({e}); pipe the token in instead, for example `pbpaste | oxideswarm join`"
        ))
    })?;
    eprint!("Paste the join token (it will not be shown): ");
    let _ = std::io::stderr().flush();
    let mut line = String::new();
    let read = std::io::stdin()
        .lock()
        .take(MAX_TOKEN_BYTES + 1)
        .read_line(&mut line);
    drop(guard);
    eprintln!();
    read.map_err(|e| GridError::Config(format!("cannot read the join token: {e}")))?;
    clean_token(&line)
}

#[cfg(not(any(unix, windows)))]
pub fn read_token_hidden() -> GridResult<String> {
    Err(GridError::Config(
        "reading from a terminal is not supported here; pipe the token in".into(),
    ))
}

/// The result of planning the edit of the config file.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ConfigPlan {
    /// The new text of the file (equal to the old text when nothing changes).
    pub text: String,
    /// The keys that are set or removed, as `[section].key` (never values).
    pub changes: Vec<String>,
    /// Keys that already hold a different master or ticket, as `[section].key`.
    pub conflicts: Vec<String>,
}

fn table_of<'a>(
    doc: &'a mut toml_edit::DocumentMut,
    name: &str,
    path: &Path,
) -> GridResult<&'a mut toml_edit::Table> {
    if !doc.contains_key(name) {
        doc[name] = toml_edit::Item::Table(toml_edit::Table::new());
    }
    doc[name].as_table_mut().ok_or_else(|| {
        GridError::Config(format!(
            "config file '{}' has a `{name}` key that is not a table; fix it by hand, then join again",
            path.display()
        ))
    })
}

/// Plans the edit of the config file text `existing` (`None`: no file yet) that saves the master
/// address and ticket. Only the keys below are touched; comments, order and formatting of
/// everything else are kept (`toml_edit`).
///
/// * `[worker] master`, `[submit] master` and `[status] master` get the address (when the token has
///   one).
/// * `[worker] p2p_ticket` gets the ticket. A token without a ticket removes an existing one,
///   because the worker prefers a ticket from the config file and an old one would win over the
///   new address.
///
/// A key that already holds a *different* value is a conflict, reported in `conflicts`; the caller
/// decides whether to go on (`--replace-master`).
pub fn plan_target(
    existing: Option<&str>,
    master: Option<&str>,
    ticket: Option<&str>,
    path: &Path,
) -> GridResult<ConfigPlan> {
    let mut doc: toml_edit::DocumentMut = existing.unwrap_or("").parse().map_err(|e| {
        GridError::Config(format!(
            "config file '{}' is not valid TOML ({e}); fix or move it, then join again",
            path.display()
        ))
    })?;
    let mut plan = ConfigPlan::default();
    if let Some(master) = master {
        for name in ["worker", "submit", "status"] {
            let table = table_of(&mut doc, name, path)?;
            let current = table.get("master").and_then(|v| v.as_str());
            let current_normalized = current.map(normalize_master_addr);
            if current_normalized.as_deref() == Some(master) {
                continue;
            }
            if table.contains_key("master") {
                plan.conflicts.push(format!("[{name}].master"));
            }
            table["master"] = toml_edit::value(master);
            plan.changes.push(format!("[{name}].master"));
        }
    }
    let worker = table_of(&mut doc, "worker", path)?;
    let current_ticket = worker
        .get("p2p_ticket")
        .map(|v| v.as_str().map(str::to_string));
    match (ticket, current_ticket) {
        (Some(t), Some(Some(cur))) if cur == t => {}
        (Some(t), current) => {
            if current.is_some() {
                plan.conflicts.push("[worker].p2p_ticket".into());
            }
            worker["p2p_ticket"] = toml_edit::value(t);
            plan.changes.push("[worker].p2p_ticket".into());
        }
        // Only a token that names an address replaces a ticket: a ticket-less, address-less token
        // cannot exist, but keep the rule explicit.
        (None, Some(_)) if master.is_some() => {
            plan.conflicts.push("[worker].p2p_ticket".into());
            worker.remove("p2p_ticket");
            plan.changes.push("[worker].p2p_ticket (removed)".into());
        }
        (None, _) => {}
    }
    // An empty table that only this function created is not worth writing.
    for name in ["worker", "submit", "status"] {
        if doc
            .get(name)
            .and_then(|i| i.as_table())
            .is_some_and(|t| t.is_empty())
        {
            doc.remove(name);
        }
    }
    plan.text = if plan.changes.is_empty() {
        existing.unwrap_or("").to_string()
    } else {
        doc.to_string()
    };
    Ok(plan)
}

/// Reads the config file at `path` for planning: `None` when it does not exist. A symbolic link is
/// refused (writing through it would change a file somewhere else, and a rename would replace the
/// link).
pub fn read_config_for_plan(path: &Path) -> GridResult<Option<String>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(GridError::Config(format!(
            "config file '{}' is a symbolic link; join does not edit through links. Replace it with a regular file or edit it by hand",
            path.display()
        ))),
        Ok(_) => std::fs::read_to_string(path).map(Some).map_err(|e| {
            GridError::Config(format!("cannot read config file '{}': {e}", path.display()))
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(GridError::Config(format!(
            "cannot read config file '{}': {e}",
            path.display()
        ))),
    }
}

/// Creates a file that only the owner can read (mode 600 on Unix, the protected DACL of design
/// 0047 on Windows) and fills it.
fn write_private_new(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?
    };
    #[cfg(windows)]
    let mut file = rusty_grid_core::private_file::create_private_new(path)?;
    #[cfg(not(any(unix, windows)))]
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(data)?;
    file.sync_all()
}

/// Writes the planned config file: a private backup `<name>.bak-<unix seconds>` of the old file
/// first (when there is one), then the new text through a temporary file and a rename. Returns the
/// backup path.
pub fn apply_plan(
    path: &Path,
    existing: Option<&str>,
    plan: &ConfigPlan,
) -> GridResult<Option<PathBuf>> {
    let err = |what: &str, e: &dyn std::fmt::Display| {
        GridError::Config(format!("{what} '{}': {e}", path.display()))
    };
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|e| err("cannot create the directory of", &e))?;
    }
    let mut backup = None;
    if let Some(old) = existing {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| CONFIG_FILE_NAME.to_string());
        let mut candidate = path.with_file_name(format!("{name}.bak-{stamp}"));
        let mut n = 1;
        while candidate.exists() {
            candidate = path.with_file_name(format!("{name}.bak-{stamp}-{n}"));
            n += 1;
        }
        write_private_new(&candidate, old.as_bytes())
            .map_err(|e| err("cannot write a backup of", &e))?;
        backup = Some(candidate);
    }
    let tmp = path.with_extension(format!("toml.{}.tmp", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    // The temporary file is private from its creation (mode 600; on Windows the protected owner-only
    // DACL, which the renamed file keeps). Only then, on Unix, does it take the mode the owner had
    // given the old file, so no moment exists at which the content is readable more widely than
    // the old file was.
    let result = write_private_new(&tmp, plan.text.as_bytes())
        .and_then(|()| {
            #[cfg(unix)]
            if let Ok(meta) = std::fs::metadata(path) {
                return std::fs::set_permissions(&tmp, meta.permissions());
            }
            Ok(())
        })
        .and_then(|()| std::fs::rename(&tmp, path));
    result.map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        err("cannot write config file", &e)
    })?;
    Ok(backup)
}

/// What [`store_secret`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum SecretStored {
    Created,
    AlreadyPresent,
    /// The file held this secret but was readable by group or others (Unix); it is now mode 600.
    AlreadyPresentTightened,
    Replaced,
}

/// Puts `secret` into the secret file `path`. A different existing secret (or an unreadable file)
/// is replaced only with `replace`.
pub fn store_secret(
    path: &Path,
    secret: &ClusterSecret,
    replace: bool,
) -> GridResult<SecretStored> {
    if !path.exists() {
        secret.save_file(path, false)?;
        return Ok(SecretStored::Created);
    }
    if ClusterSecret::load_file(path).is_ok_and(|existing| existing.to_hex() == secret.to_hex()) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = std::fs::metadata(path) {
                if meta.permissions().mode() & 0o077 != 0 {
                    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                        .map_err(|e| {
                            GridError::Config(format!(
                                "cannot set mode 600 on '{}': {e}; run `chmod 600` on it",
                                path.display()
                            ))
                        })?;
                    return Ok(SecretStored::AlreadyPresentTightened);
                }
            }
        }
        // On Windows `ClusterSecret::load_file` above already warned, with the `icacls` command
        // that fixes it, when the file's ACL grants anyone else access.
        return Ok(SecretStored::AlreadyPresent);
    }
    if !replace {
        return Err(GridError::Config(format!(
            "'{}' already holds a different cluster secret (or one that cannot be read). Joining would cut this machine off from the cluster it uses now. If that is what you want, run `oxideswarm join --replace-secret`",
            path.display()
        )));
    }
    secret.save_file(path, true)?;
    Ok(SecretStored::Replaced)
}

/// Looks for a cluster secret configured *outside* `cluster.secret` that outranks the file and
/// differs from the token's (`--secret`, `--secret-file`, `OXIDESWARM_SECRET`,
/// `OXIDESWARM_SECRET_FILE`, the `secret` / `secret_file` keys of the config file, resolved in the
/// order the CLI uses, for every section that `join` configures). Returns a message that names the
/// source and never a value. A source that cannot be read counts as a conflict too: the machine
/// would not use the token's secret either.
pub fn conflicting_secret_source(
    opts: &SecretOpts,
    config: Option<&ConfigFile>,
    env: &impl Fn(&str) -> Option<String>,
    token_secret: &ClusterSecret,
) -> Option<String> {
    let source = |section: SecretSection| -> String {
        if opts.value.is_some() {
            "the --secret flag".into()
        } else if opts.file.is_some() {
            "the --secret-file flag".into()
        } else if env(ENV_SECRET).is_some() {
            format!("the {ENV_SECRET} environment variable")
        } else if env(ENV_SECRET_FILE).is_some() {
            format!("the {ENV_SECRET_FILE} environment variable")
        } else {
            let name = match section {
                SecretSection::Master => "[master]",
                SecretSection::Worker => "[worker]",
                SecretSection::Submit => "[submit]",
                SecretSection::Status => "[status]",
            };
            let path = config
                .and_then(|c| c.source_path.0.as_ref())
                .map(|p| format!(" in {}", p.display()))
                .unwrap_or_default();
            format!(
                "the `secret` or `secret_file` key (top level or {name}) of the config file{path}"
            )
        }
    };
    for section in [
        SecretSection::Worker,
        SecretSection::Submit,
        SecretSection::Status,
        SecretSection::Master,
    ] {
        // The default file is withheld: it is the file this command writes.
        match opts.resolve_with(config, section, env, None) {
            Ok(None) => {}
            Ok(Some(found)) if found.to_hex() == token_secret.to_hex() => {}
            Ok(Some(_)) => {
                return Some(format!(
                    "{} sets a different cluster secret and outranks ~/.oxideswarm/cluster.secret, so this machine would keep using it and not the one in the token",
                    source(section)
                ))
            }
            Err(e) => {
                return Some(format!(
                    "{} names a cluster secret that cannot be used ({e})",
                    source(section)
                ))
            }
        }
    }
    None
}

/// `oxideswarm join`: reads a join token from standard input.
#[derive(Parser, Debug, Clone)]
pub struct JoinArgs {
    #[arg(
        long,
        help = "Replace a different cluster secret that this machine already has"
    )]
    pub replace_secret: bool,

    #[arg(
        long,
        help = "Replace a different master address or P2P ticket that the config file already names"
    )]
    pub replace_master: bool,

    #[arg(
        long,
        value_name = "N",
        default_value = "0",
        help = "The doctor check fails unless at least N workers are connected (default 0: this machine's worker usually starts after joining)"
    )]
    pub expect_workers: usize,

    #[arg(
        long,
        help = "Save the secret and the address but do not run the doctor checks"
    )]
    pub no_doctor: bool,

    /// Catches a token typed as an argument so that it can be refused without being echoed back
    /// (clap would print an unexpected argument in its error message).
    #[arg(hide = true, num_args = 0.., value_name = "TOKEN")]
    pub refused_argument: Vec<String>,
}

/// Runs `oxideswarm join`.
pub async fn run_join(
    args: JoinArgs,
    secret_opts: &SecretOpts,
    config: Option<&ConfigFile>,
) -> ExitCode {
    if !args.refused_argument.is_empty() {
        eprintln!("A join token must not be given as an argument: it would stay in your shell history and be visible in the process list. Pipe it in instead, for example `pbpaste | oxideswarm join`, or `ssh master oxideswarm secret join-token | oxideswarm join`.");
        return ExitCode::FAILURE;
    }
    let stdin = std::io::stdin();
    let read = if stdin.is_terminal() {
        read_token_hidden()
    } else {
        read_token(stdin.lock())
    };
    let text = match read {
        Ok(text) => text,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let token = match JoinToken::decode(&text) {
        Ok(token) => token,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    drop(text);

    // A secret configured elsewhere would outrank the file this command writes.
    let env = |name: &str| std::env::var_os(name).map(|v| v.to_string_lossy().into_owned());
    if let Some(problem) = conflicting_secret_source(secret_opts, config, &env, &token.secret) {
        eprintln!(
            "{problem}. Remove or change it (or run join where it is not set), then join again; nothing was changed."
        );
        return ExitCode::FAILURE;
    }

    let Some(secret_path) = ClusterSecret::default_path() else {
        eprintln!("cannot determine the home directory for ~/.oxideswarm/cluster.secret");
        return ExitCode::FAILURE;
    };
    let config_path = default_oxideswarm_dir()
        .map(|d| d.join(CONFIG_FILE_NAME))
        .unwrap_or_else(|| PathBuf::from(CONFIG_FILE_NAME));
    let master = token.master.as_deref().map(normalize_master_addr);

    // Plan the config edit before anything is written: a refusal must leave the machine untouched.
    let existing = match read_config_for_plan(&config_path) {
        Ok(text) => text,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let plan = match plan_target(
        existing.as_deref(),
        master.as_deref(),
        token.ticket.as_deref(),
        &config_path,
    ) {
        Ok(plan) => plan,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    if !plan.conflicts.is_empty() && !args.replace_master {
        eprintln!(
            "{} already names a different master or P2P ticket (keys: {}). Joining would point this machine at another cluster. If that is what you want, run `oxideswarm join --replace-master`; a backup of the file is kept.",
            config_path.display(),
            plan.conflicts.join(", ")
        );
        return ExitCode::FAILURE;
    }
    match store_secret(&secret_path, &token.secret, args.replace_secret) {
        Ok(SecretStored::Created) => println!("Cluster secret saved to {}", secret_path.display()),
        Ok(SecretStored::AlreadyPresent) => println!(
            "{} already holds this cluster's secret; left as it is",
            secret_path.display()
        ),
        Ok(SecretStored::AlreadyPresentTightened) => {
            println!(
                "{} already holds this cluster's secret; left as it is",
                secret_path.display()
            );
            eprintln!(
                "The file was readable by other users; its mode is now 600 (it held the secret before this join)."
            );
        }
        Ok(SecretStored::Replaced) => println!(
            "The cluster secret in {} was replaced",
            secret_path.display()
        ),
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    }
    if plan.changes.is_empty() {
        println!("{} already names this master", config_path.display());
    } else {
        match apply_plan(&config_path, existing.as_deref(), &plan) {
            Ok(backup) => {
                eprintln!(
                    "Changed in {}: {}",
                    config_path.display(),
                    plan.changes.join(", ")
                );
                if let Some(backup) = backup {
                    eprintln!("Previous file kept as {} (owner-only)", backup.display());
                }
            }
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    }
    if let Some(master) = &master {
        println!("Master: {master}");
    }
    if token.ticket.is_some() {
        println!("The master's P2P ticket was saved as the worker's `p2p_ticket`.");
    }
    println!("Start a worker here with: oxideswarm worker");

    let Some(master) = master else {
        println!(
            "The token has no master address, so the doctor checks were skipped (they need one)."
        );
        return ExitCode::SUCCESS;
    };
    if args.no_doctor {
        return ExitCode::SUCCESS;
    }
    println!("Checking the cluster ...");
    let options = DoctorOptions {
        expect_workers: args.expect_workers,
        probe_timeout: Duration::from_secs(60),
        ..DoctorOptions::default()
    };
    let report = run_doctor(&master, Some(&token.secret), &options).await;
    print!("{}", report.render());
    if report.auth_failed {
        ExitCode::from(AUTH_EXIT_CODE)
    } else if report.failed() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret() -> ClusterSecret {
        ClusterSecret::from_hex(&"ab".repeat(32)).unwrap()
    }

    fn token() -> JoinToken {
        JoinToken {
            master: Some("192.168.1.5:8088".into()),
            ticket: Some("ticketvalue".into()),
            secret: secret(),
        }
    }

    #[test]
    fn test_round_trip_keeps_every_field() {
        let text = token().encode();
        assert!(text.starts_with(TOKEN_PREFIX));
        assert!(
            !text.contains(&secret().to_hex()),
            "the hex is not visible in the token"
        );
        let back = JoinToken::decode(&format!("  {text}\n")).unwrap();
        assert_eq!(back.master.as_deref(), Some("192.168.1.5:8088"));
        assert_eq!(back.ticket.as_deref(), Some("ticketvalue"));
        assert_eq!(back.secret.to_hex(), secret().to_hex());
    }

    #[test]
    fn test_every_single_character_change_is_refused() {
        let text = token().encode();
        let start = TOKEN_PREFIX.len();
        for i in 0..text.len() {
            let mut bytes = text.clone().into_bytes();
            bytes[i] = if bytes[i] == b'A' { b'B' } else { b'A' };
            let changed = String::from_utf8(bytes).unwrap();
            let result = JoinToken::decode(&changed);
            if i < start {
                assert_eq!(result.err(), Some(TokenError::NotAToken), "position {i}");
            } else {
                assert!(result.is_err(), "a change at position {i} was accepted");
            }
        }
        assert!(
            JoinToken::decode(&text[..text.len() - 1]).is_err(),
            "truncated"
        );
        assert!(JoinToken::decode(&format!("{text}0")).is_err(), "extended");
    }

    #[test]
    fn test_a_token_without_a_target_or_with_a_bad_field_is_invalid() {
        let mut t = token();
        t.master = None;
        t.ticket = None;
        assert!(matches!(
            JoinToken::decode(&t.encode()),
            Err(TokenError::Invalid(_))
        ));
        let mut t = token();
        t.master = Some("has space:8088".into());
        assert!(matches!(
            JoinToken::decode(&t.encode()),
            Err(TokenError::Invalid(_))
        ));
        let mut t = token();
        t.master = None;
        assert!(
            JoinToken::decode(&t.encode()).is_ok(),
            "a ticket alone is a target"
        );
    }

    #[test]
    fn test_errors_and_debug_never_show_the_token_or_secret() {
        let text = token().encode();
        let shown = format!("{:?}", token());
        assert!(!shown.contains(&secret().to_hex()) && !shown.contains("ticketvalue"));
        let mut changed = text.clone().into_bytes();
        let last = changed.len() - 1;
        changed[last] = if changed[last] == b'0' { b'1' } else { b'0' };
        let err = JoinToken::decode(&String::from_utf8(changed).unwrap())
            .unwrap_err()
            .to_string();
        assert!(!err.contains(&text[TOKEN_PREFIX.len()..]) && !err.contains(&secret().to_hex()));
    }

    #[test]
    fn test_read_token_trims_and_bounds_its_input() {
        assert_eq!(read_token("  abc\n".as_bytes()).unwrap(), "abc");
        assert!(read_token("  \n".as_bytes()).is_err());
        let long = "x".repeat(MAX_TOKEN_BYTES as usize + 1);
        assert!(read_token(long.as_bytes()).is_err());
    }

    #[test]
    fn test_store_secret_creates_keeps_and_refuses_to_replace() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("cluster.secret");
        assert_eq!(
            store_secret(&path, &secret(), false).unwrap(),
            SecretStored::Created
        );
        assert_eq!(
            store_secret(&path, &secret(), false).unwrap(),
            SecretStored::AlreadyPresent
        );
        let other = ClusterSecret::from_hex(&"cd".repeat(32)).unwrap();
        let err = store_secret(&path, &other, false).unwrap_err().to_string();
        assert!(err.contains("--replace-secret"));
        assert_eq!(
            ClusterSecret::load_file(&path).unwrap().to_hex(),
            secret().to_hex()
        );
        assert_eq!(
            store_secret(&path, &other, true).unwrap(),
            SecretStored::Replaced
        );
        assert_eq!(
            ClusterSecret::load_file(&path).unwrap().to_hex(),
            other.to_hex()
        );
    }

    fn plan(existing: Option<&str>, master: Option<&str>, ticket: Option<&str>) -> ConfigPlan {
        plan_target(existing, master, ticket, Path::new("rusty-grid.toml")).unwrap()
    }

    #[test]
    fn test_plan_keeps_comments_and_order_and_touches_only_its_keys() {
        let old = "# my cluster\n[worker]\nname = \"box\"  # keep me\n\n[master]\nlisten = \"0.0.0.0:8088\"\n";
        let p = plan(Some(old), Some("10.0.0.2:8088"), None);
        assert!(p.conflicts.is_empty());
        assert_eq!(
            p.changes,
            ["[worker].master", "[submit].master", "[status].master"]
        );
        assert!(p.text.contains("# my cluster"));
        assert!(p.text.contains("name = \"box\"  # keep me"));
        assert!(p.text.contains("listen = \"0.0.0.0:8088\""));
        let cfg: crate::config::ConfigFile = toml::from_str(&p.text).unwrap();
        assert_eq!(cfg.worker.unwrap().master.as_deref(), Some("10.0.0.2:8088"));
        assert_eq!(cfg.submit.unwrap().master.as_deref(), Some("10.0.0.2:8088"));
        assert_eq!(cfg.status.unwrap().master.as_deref(), Some("10.0.0.2:8088"));
        // Planning again on the result changes nothing and leaves the text as it is.
        let again = plan(Some(&p.text), Some("10.0.0.2:8088"), None);
        assert!(again.changes.is_empty() && again.conflicts.is_empty());
        assert_eq!(again.text, p.text);
    }

    #[test]
    fn test_plan_reports_a_different_master_or_ticket_as_a_conflict() {
        let old = "[worker]\nmaster = \"10.0.0.9:8088\"\np2p_ticket = \"old\"\n";
        let p = plan(Some(old), Some("10.0.0.2:8088"), None);
        assert!(p.conflicts.contains(&"[worker].master".to_string()));
        assert!(p.conflicts.contains(&"[worker].p2p_ticket".to_string()));
        assert!(!p.text.contains("p2p_ticket"), "removed in the plan");
        assert!(p
            .changes
            .contains(&"[worker].p2p_ticket (removed)".to_string()));
        // A bare host in the file equals the normalised address of the token.
        let same = plan(
            Some("[worker]\nmaster = \"10.0.0.2\"\n"),
            Some("10.0.0.2:8088"),
            None,
        );
        assert!(same.conflicts.iter().all(|c| !c.starts_with("[worker]")));
        // A different ticket is a conflict, the same ticket is not.
        let t = plan(Some("[worker]\np2p_ticket = \"a\"\n"), None, Some("b"));
        assert_eq!(t.conflicts, ["[worker].p2p_ticket"]);
        let t = plan(Some("[worker]\np2p_ticket = \"a\"\n"), None, Some("a"));
        assert!(t.changes.is_empty());
    }

    #[test]
    fn test_plan_refuses_a_file_that_is_not_toml_or_has_a_non_table_section() {
        assert!(plan_target(
            Some("this is = = not toml"),
            Some("h:1"),
            None,
            Path::new("c")
        )
        .is_err());
        assert!(plan_target(Some("worker = 3\n"), Some("h:1"), None, Path::new("c")).is_err());
    }

    #[test]
    fn test_apply_plan_backs_up_privately_and_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rusty-grid.toml");
        let old = "# keep\n[worker]\nname = \"x\"\n";
        std::fs::write(&path, old).unwrap();
        let p = plan(Some(old), Some("10.0.0.2:8088"), None);
        let backup = apply_plan(&path, Some(old), &p).unwrap().expect("a backup");
        assert!(backup
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("rusty-grid.toml.bak-"));
        assert_eq!(std::fs::read_to_string(&backup).unwrap(), old);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&backup).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), p.text);
        // A second backup never overwrites the first.
        let again = apply_plan(&path, Some(old), &p).unwrap().unwrap();
        assert_ne!(again, backup);
        // A new file has nothing to back up.
        let fresh = dir.path().join("sub").join("rusty-grid.toml");
        assert!(apply_plan(&fresh, None, &plan(None, Some("h:8088"), None))
            .unwrap()
            .is_none());
        assert!(fresh.is_file());
    }

    #[cfg(unix)]
    #[test]
    fn test_a_symbolic_link_config_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real.toml");
        std::fs::write(&real, "").unwrap();
        let link = dir.path().join("rusty-grid.toml");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(read_config_for_plan(&link)
            .unwrap_err()
            .to_string()
            .contains("symbolic link"));
        assert_eq!(
            read_config_for_plan(&dir.path().join("none.toml")).unwrap(),
            None
        );
        assert_eq!(read_config_for_plan(&real).unwrap().as_deref(), Some(""));
    }

    #[cfg(unix)]
    #[test]
    fn test_echo_off_clears_echo_and_restores_it_on_a_terminal() {
        let (mut master, mut slave) = (0, 0);
        // SAFETY: openpty fills both descriptors on success; they are closed below.
        let rc = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, 0, "openpty");
        let echo = |fd| {
            let mut t: libc::termios = unsafe { std::mem::zeroed() };
            assert_eq!(unsafe { libc::tcgetattr(fd, &mut t) }, 0);
            t.c_lflag & libc::ECHO != 0
        };
        assert!(echo(slave), "a new terminal echoes");
        {
            let _guard = EchoOff::on_fd(slave).unwrap();
            assert!(!echo(slave), "echo is off while the guard lives");
        }
        assert!(echo(slave), "echo is back after the guard");
        // A descriptor that is not a terminal is an error, not a silent echo.
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::File::create(dir.path().join("f")).unwrap();
        use std::os::fd::AsRawFd;
        assert!(EchoOff::on_fd(file.as_raw_fd()).is_err());
        unsafe {
            libc::close(master);
            libc::close(slave);
        }
    }

    #[test]
    fn test_a_secret_configured_elsewhere_that_differs_is_a_conflict() {
        let token_secret = secret();
        let other_hex = "cd".repeat(32);
        let none_env = |_: &str| None::<String>;
        let opts = SecretOpts::default();
        assert_eq!(
            conflicting_secret_source(&opts, None, &none_env, &token_secret),
            None
        );

        // The environment.
        let env = |name: &str| (name == ENV_SECRET).then(|| other_hex.clone());
        let msg = conflicting_secret_source(&opts, None, &env, &token_secret).unwrap();
        assert!(msg.contains("OXIDESWARM_SECRET") && !msg.contains(&other_hex));
        // The same secret in the environment is fine.
        let same = |name: &str| (name == ENV_SECRET).then(|| token_secret.to_hex());
        assert_eq!(
            conflicting_secret_source(&opts, None, &same, &token_secret),
            None
        );

        // A key of the config file.
        let cfg: ConfigFile =
            toml::from_str(&format!("[worker]\nsecret = \"{other_hex}\"\n")).unwrap();
        let msg = conflicting_secret_source(&opts, Some(&cfg), &none_env, &token_secret).unwrap();
        assert!(msg.contains("config file") && !msg.contains(&other_hex));
        let cfg: ConfigFile =
            toml::from_str(&format!("secret = \"{}\"\n", token_secret.to_hex())).unwrap();
        assert_eq!(
            conflicting_secret_source(&opts, Some(&cfg), &none_env, &token_secret),
            None
        );

        // A secret file, and an unreadable one.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("other.secret");
        std::fs::write(&file, format!("{other_hex}\n")).unwrap();
        let by_flag = SecretOpts {
            value: None,
            file: Some(file),
        };
        let msg = conflicting_secret_source(&by_flag, None, &none_env, &token_secret).unwrap();
        assert!(msg.contains("--secret-file") && !msg.contains(&other_hex));
        let missing = SecretOpts {
            value: None,
            file: Some(dir.path().join("missing")),
        };
        assert!(conflicting_secret_source(&missing, None, &none_env, &token_secret).is_some());
    }

    #[cfg(unix)]
    #[test]
    fn test_an_existing_secret_file_with_loose_permissions_is_tightened() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.secret");
        std::fs::write(&path, format!("{}\n", secret().to_hex())).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            store_secret(&path, &secret(), false).unwrap(),
            SecretStored::AlreadyPresentTightened
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            store_secret(&path, &secret(), false).unwrap(),
            SecretStored::AlreadyPresent
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_the_temporary_config_is_private_from_the_start_and_the_old_mode_is_kept() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rusty-grid.toml");
        std::fs::write(&path, "[worker]\nname = \"x\"\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let old = std::fs::read_to_string(&path).unwrap();
        let p = plan(Some(&old), Some("h:8088"), None);
        apply_plan(&path, Some(&old), &p).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
        // A new file is private.
        let fresh = dir.path().join("new.toml");
        apply_plan(&fresh, None, &plan(None, Some("h:8088"), None)).unwrap();
        assert_eq!(
            std::fs::metadata(&fresh).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
