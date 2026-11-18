//! AppContainer child process creation, restricted LUA token handling, and stdio pipes.

use super::acl::{explicit_access, OwnedSid};
use super::path::to_wide;
use super::WINDOWS_SPAWN_LOCK;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::FromRawHandle;
use std::os::windows::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use windows_sys::Win32::Foundation::{
    CloseHandle, LocalFree, SetHandleInformation, ERROR_SUCCESS, HANDLE, HANDLE_FLAG_INHERIT,
    HLOCAL, WAIT_OBJECT_0,
};
use windows_sys::Win32::Security::Authorization::{SetEntriesInAclW, GRANT_ACCESS};
use windows_sys::Win32::Security::{
    CreateRestrictedToken, DuplicateTokenEx, GetTokenInformation, SecurityImpersonation,
    SetTokenInformation, TokenDefaultDacl, TokenElevation, TokenOwner, TokenPrimary, ACL,
    LUA_TOKEN, NO_INHERITANCE, PSID, SECURITY_ATTRIBUTES, SECURITY_CAPABILITIES,
    TOKEN_ADJUST_DEFAULT, TOKEN_ADJUST_SESSIONID, TOKEN_ASSIGN_PRIMARY, TOKEN_DEFAULT_DACL,
    TOKEN_DUPLICATE, TOKEN_ELEVATION, TOKEN_OWNER, TOKEN_QUERY,
};
use windows_sys::Win32::System::Pipes::CreatePipe;
use windows_sys::Win32::System::Threading::{
    CreateProcessAsUserW, CreateProcessW, DeleteProcThreadAttributeList, GetCurrentProcess,
    GetExitCodeProcess, InitializeProcThreadAttributeList, OpenProcessToken, TerminateProcess,
    UpdateProcThreadAttribute, WaitForSingleObject, CREATE_NO_WINDOW, CREATE_UNICODE_ENVIRONMENT,
    EXTENDED_STARTUPINFO_PRESENT, INFINITE, LPPROC_THREAD_ATTRIBUTE_LIST, PROCESS_INFORMATION,
    PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES,
    STARTF_USESTDHANDLES, STARTUPINFOEXW,
};

#[link(name = "kernel32")]
extern "system" {
    fn SetErrorMode(umode: u32) -> u32;
}

/// Logs once per worker process why an elevated worker refuses to use its own token.
fn log_elevated_token_refusal_once(message: &str) {
    static LOGGED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !LOGGED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        tracing::warn!("{message}");
    }
}

/// Returns `true` if the current worker process token is elevated (`TokenElevation != 0`).
pub fn is_process_elevated() -> bool {
    // SAFETY: `GetCurrentProcess()` returns the current process pseudo-handle;
    // `OpenProcessToken` yields an owned handle closed once below when non-null, and
    // `GetTokenInformation` writes `size_of::<TOKEN_ELEVATION>()` bytes into `elev`.
    unsafe {
        let mut token: HANDLE = std::ptr::null_mut();
        if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 || token.is_null() {
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
///
/// # Safety
/// - `token` must be null (no-op) or a live handle to an opened process/restricted token with
///   `TOKEN_QUERY` and `TOKEN_ADJUST_DEFAULT` access.
/// - `user_sid` must point to a valid Win32 `SID` buffer that remains live for this call.
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

/// Creates and caches a primary LUA-filtered child token (`CreateRestrictedToken` with
/// `LUA_TOKEN` + `DuplicateTokenEx` with `TokenPrimary`, `TokenOwner = CurrentUser`, and
/// `TokenDefaultDacl` granting `CurrentUser`, `SYSTEM`, and `Administrators` — never
/// `BUILTIN\Users` or `Everyone`) when the worker runs elevated (`is_process_elevated() == true`).
///
/// Note on elevated sessions (`runneradmin` on GitHub Actions `windows-latest`):
/// - `CreateProcessAsUserW` without `SeAssignPrimaryTokenPrivilege` is only permitted by
///   `ntoskrnl.exe!SeIsTokenAssignableToProcess` when the primary token is a restricted child
///   token sharing the calling process's `AuthenticationId` (`AdminLogonId`); a token with a
///   different `AuthenticationId` (such as `LogonUserW`) is rejected with
///   `ERROR_PRIVILEGE_NOT_HELD (os error 1314)`.
/// - Under an elevated split-token logon session, `\Sessions\<S>\DosDevices\<AdminLogonId>` is
///   created by the kernel before the worker starts with `TokenOwner = BUILTIN\Administrators`
///   (`O:BA D:(A;OICI;...;;;SY)(A;OICI;...;;;BA)`), which has no ACE for `CurrentUser`. Because
///   OxideSwarm never edits Object Manager DACLs (`\??`, `\GLOBAL??`, `\Device\Null`) or grants
///   broad SIDs, opening `\??\NUL` inside an AppContainer under an elevated runner returns
///   `ERROR_ACCESS_DENIED (5)`, whereas on normal non-elevated Windows workers
///   `\Sessions\<S>\DosDevices\<LogonId>` has `(A;;CCDCLCSWSDRCWDWO;;;<CurrentUser>)` and
///   `\??\NUL` works natively.
///
/// Returns `None` when the worker is not elevated.
fn get_or_init_worker_lua_token() -> Option<HANDLE> {
    static WORKER_LUA_TOKEN: OnceLock<Option<usize>> = OnceLock::new();
    WORKER_LUA_TOKEN
        .get_or_init(|| {
            if !is_process_elevated() {
                return None;
            }
            ensure_process_token_default_dacl_grants_current_user();
            let user_sid = OwnedSid::current_user().ok()?;
            // SAFETY: `proc_token` and `restricted_token` are closed once via `CloseHandle`; `user_sid`
            // borrows a live `OwnedSid` across `apply_current_user_owner_and_default_dacl`; and
            // `primary_token` stays open for the worker's lifetime in `WORKER_LUA_TOKEN`.
            unsafe {
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
                Some(primary_token as usize)
            }
        })
        .map(|primary| primary as HANDLE)
}

/// When the worker process runs under an elevated Administrator token (such as `runneradmin`
/// on GitHub Actions `windows-latest`, where Windows sets `TokenOwner = BUILTIN\Administrators`
/// and `TokenDefaultDacl = D:(A;;GA;;;BA)(A;;GA;;;SY)` with no ACE for `TokenUser`), sets
/// `TokenOwner = CurrentUser` and adds `CurrentUser` (never `BUILTIN\Users` or `Everyone`) to
/// the elevated worker token's `TokenDefaultDacl`. Does nothing when the worker process is not
/// elevated.
pub(super) fn ensure_process_token_default_dacl_grants_current_user() {
    static DONE: OnceLock<()> = OnceLock::new();
    // SAFETY: `GetCurrentProcess()` returns the current process pseudo-handle; `token` is
    // closed once via `CloseHandle`; and `user_sid` borrows a live `OwnedSid` during
    // `apply_current_user_owner_and_default_dacl`.
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

struct WinProcessHandle(HANDLE);
// SAFETY: `WinProcessHandle` solely owns an OS process handle that is safe to transfer
// across threads and only closed once in `Drop`.
unsafe impl Send for WinProcessHandle {}
// SAFETY: `&WinProcessHandle` only exposes the raw handle for synchronized Win32 waits and
// termination (`WaitForSingleObject`, `GetExitCodeProcess`, `TerminateProcess`).
unsafe impl Sync for WinProcessHandle {}

impl Drop for WinProcessHandle {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: `self.0` is an owned non-null child process handle closed once on drop.
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
            // SAFETY: While `!self.exited`, `self.process` retains an open process handle (`self.0`).
            unsafe {
                let _ = TerminateProcess(self.process.0, 1);
            }
        }
        Ok(())
    }

    pub async fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        drop(self.stdin.take());
        let proc_handle = Arc::clone(&self.process);
        // SAFETY: `proc_handle` holds an `Arc<WinProcessHandle>` keeping `proc_handle.0` open
        // across blocking `WaitForSingleObject` and `GetExitCodeProcess(&mut code)`.
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
            // SAFETY: `self.process` still keeps the OS process handle open before drop cleanup.
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
    // SAFETY: `sa.nLength` equals `size_of::<SECURITY_ATTRIBUTES>()`, `lpSecurityDescriptor` is
    // null, and `read_pipe`/`write_pipe` receive newly created handles closed or wrapped by callers.
    let ok = unsafe { CreatePipe(&mut read_pipe, &mut write_pipe, &sa, 0) };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((read_pipe, write_pipe))
}

pub(super) fn spawn_with_security_capabilities(
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
    let prog_path = Path::new(std_cmd.get_program());
    if !prog_path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "refusing to spawn non-absolute program path '{}' inside AppContainer",
                prog_path.display()
            ),
        ));
    }
    let (stdin_read, stdin_write) = create_non_inheritable_pipe_pair()?;
    let (stdout_read, stdout_write) = match create_non_inheritable_pipe_pair() {
        Ok(p) => p,
        Err(e) => {
            // SAFETY: `stdin_read` and `stdin_write` are newly opened pipe handles closed once here.
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
            // SAFETY: All four pipe handles created above are still open and closed once here.
            unsafe {
                CloseHandle(stdin_read);
                CloseHandle(stdin_write);
                CloseHandle(stdout_read);
                CloseHandle(stdout_write);
            }
            return Err(e);
        }
    };
    let is_batch_script = prog_path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("bat") || ext.eq_ignore_ascii_case("cmd"));
    let (app_name_wide, mut cmdline) = if is_batch_script {
        let sys_root = std::env::var_os("SystemRoot")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Windows"));
        let cmd_exe = sys_root.join("System32").join("cmd.exe");
        let inner_cmdline = build_windows_command_line(std_cmd);
        let inner_len = inner_cmdline
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(inner_cmdline.len());
        let mut wrapped_cmd = Vec::new();
        append_windows_quoted_arg(&mut wrapped_cmd, cmd_exe.as_os_str(), true);
        wrapped_cmd.extend(" /d /s /c \"".encode_utf16());
        wrapped_cmd.extend_from_slice(&inner_cmdline[..inner_len]);
        wrapped_cmd.push(b'"' as u16);
        wrapped_cmd.push(0);
        (to_wide(cmd_exe.as_os_str()), wrapped_cmd)
    } else {
        (
            to_wide(prog_path.as_os_str()),
            build_windows_command_line(std_cmd),
        )
    };
    let env_block = build_windows_env_block(std_cmd);
    let cwd_wide = std_cmd.get_current_dir().map(|p| to_wide(p.as_os_str()));
    let cwd_ptr = cwd_wide.as_ref().map_or(std::ptr::null(), |w| w.as_ptr());

    // SAFETY: `PROCESS_INFORMATION` is a plain Win32 C struct for which zero-initialization is valid.
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
            // SAFETY: Each `h` in `inherited_handles` is a live pipe handle created above and not yet closed.
            if unsafe { SetHandleInformation(h, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) } == 0 {
                inherit_ok = false;
                break;
            }
        }

        let res = if !inherit_ok {
            Some(io::Error::last_os_error())
        } else {
            let mut attr_size: usize = 0;
            // SAFETY: Win32 size query with null list pointer writes required byte count into `attr_size`.
            unsafe {
                let _ =
                    InitializeProcThreadAttributeList(std::ptr::null_mut(), 2, 0, &mut attr_size);
            }
            let mut attr_buf = vec![0u8; attr_size.max(64)];
            let attr_list = attr_buf.as_mut_ptr() as LPPROC_THREAD_ATTRIBUTE_LIST;
            // SAFETY: `attr_buf` allocates `attr_size.max(64)` writable bytes (aligned by the system
            // allocator) matching `attr_size`, and `attr_list` is freed via `DeleteProcThreadAttributeList`.
            if unsafe { InitializeProcThreadAttributeList(attr_list, 2, 0, &mut attr_size) } == 0 {
                Some(io::Error::last_os_error())
            } else {
                // SAFETY: `attr_list` was initialized above, and `sc` (size `size_of::<SECURITY_CAPABILITIES>()`
                // plus its inner capability arrays) borrows from `WindowsAppContainerGuard` across launch.
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
                    // SAFETY: `attr_list` was initialized above and `inherited_handles` stays live until
                    // `CreateProcessW`/`CreateProcessAsUserW` returns (`3 * size_of::<HANDLE>()` bytes).
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
                    // SAFETY: `STARTUPINFOEXW` is a plain Win32 C struct for which all-zero bytes are valid.
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
                    // SAFETY: `SetErrorMode` takes a bitmask of valid `SEM_*` flags (`0x8003`).
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
                    #[cfg(test)]
                    let hook_mode = lua_test_hook::mode();
                    #[cfg(test)]
                    let elevated = is_process_elevated() || hook_mode != lua_test_hook::OFF;
                    #[cfg(not(test))]
                    let elevated = is_process_elevated();
                    #[cfg(test)]
                    let lua_token = match hook_mode {
                        lua_test_hook::NO_TOKEN => None,
                        // An invalid handle makes `CreateProcessAsUserW` fail for real.
                        lua_test_hook::BAD_TOKEN => Some(usize::MAX as HANDLE),
                        _ => get_or_init_worker_lua_token(),
                    };
                    #[cfg(not(test))]
                    let lua_token = get_or_init_worker_lua_token();
                    let mut refusal: Option<io::Error> = None;
                    // SAFETY: Under `WINDOWS_SPAWN_LOCK`, `app_name_wide`, `cmdline`/`lua_cmdline`,
                    // double-NUL `env_block`, `cwd_wide`, `desktop_wide`, `si_ex` (`attr_list` + `sc` +
                    // `inherited_handles`), `&mut pi`, and `lua_token` stay live through launch.
                    let spawned = unsafe {
                        match crate::isolation::plan_token_spawn(elevated, lua_token.is_some()) {
                            crate::isolation::TokenSpawnPlan::Lua => {
                                let mut lua_cmdline = cmdline.clone();
                                let res = CreateProcessAsUserW(
                                    lua_token.expect("plan Lua implies a token"),
                                    app_name_wide.as_ptr(),
                                    lua_cmdline.as_mut_ptr(),
                                    std::ptr::null(),
                                    std::ptr::null(),
                                    1,
                                    flags,
                                    env_block.as_ptr() as *const core::ffi::c_void,
                                    cwd_ptr,
                                    &si_ex.StartupInfo,
                                    &mut pi,
                                );
                                if res == 0 {
                                    let code =
                                        io::Error::last_os_error().raw_os_error().unwrap_or(0)
                                            as u32;
                                    if let Some(msg) =
                                        crate::isolation::after_lua_spawn_failure(elevated, code)
                                    {
                                        log_elevated_token_refusal_once(&msg);
                                        refusal = Some(io::Error::new(
                                            io::ErrorKind::PermissionDenied,
                                            msg,
                                        ));
                                        // Not 0: the error is carried in `refusal`.
                                        1
                                    } else {
                                        CreateProcessW(
                                            app_name_wide.as_ptr(),
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
                                } else {
                                    res
                                }
                            }
                            crate::isolation::TokenSpawnPlan::RefuseNoLuaToken => {
                                let msg = crate::isolation::elevated_token_refusal_message(
                                    "the restricted (LUA) token could not be created",
                                    None,
                                );
                                log_elevated_token_refusal_once(&msg);
                                refusal =
                                    Some(io::Error::new(io::ErrorKind::PermissionDenied, msg));
                                1
                            }
                            crate::isolation::TokenSpawnPlan::Plain => CreateProcessW(
                                app_name_wide.as_ptr(),
                                cmdline.as_mut_ptr(),
                                std::ptr::null(),
                                std::ptr::null(),
                                1,
                                flags,
                                env_block.as_ptr() as *const core::ffi::c_void,
                                cwd_ptr,
                                &si_ex.StartupInfo,
                                &mut pi,
                            ),
                        }
                    };
                    if let Some(err) = refusal {
                        Some(err)
                    } else if spawned == 0 {
                        Some(io::Error::last_os_error())
                    } else {
                        None
                    }
                };
                // SAFETY: `attr_list` was initialized by `InitializeProcThreadAttributeList` above
                // while `attr_buf` is still live, is no longer referenced, and is destroyed once here.
                unsafe {
                    DeleteProcThreadAttributeList(attr_list);
                }
                inner_res
            }
        };

        // SAFETY: The three child pipe ends are owned solely by this function and closed once in
        // the parent while `WINDOWS_SPAWN_LOCK` is held.
        unsafe {
            CloseHandle(stdin_read);
            CloseHandle(stdout_write);
            CloseHandle(stderr_write);
        }

        res
    };

    if let Some(err) = spawn_err {
        // SAFETY: On spawn failure the three parent pipe ends are not wrapped into `File`s and
        // are closed once here.
        unsafe {
            CloseHandle(stdin_write);
            CloseHandle(stdout_read);
            CloseHandle(stderr_read);
        }
        return Err(err);
    }

    // SAFETY: Spawn succeeded, so `pi.hThread` is a valid thread handle closed once here.
    unsafe {
        CloseHandle(pi.hThread);
    }

    // SAFETY: On success `stdin_write` is a live parent pipe handle whose sole ownership
    // transfers to `std::fs::File` here.
    let stdin_file =
        unsafe { tokio::fs::File::from_std(std::fs::File::from_raw_handle(stdin_write as _)) };
    // SAFETY: `stdout_read` is an open parent pipe handle whose sole ownership transfers here.
    let stdout_file =
        unsafe { tokio::fs::File::from_std(std::fs::File::from_raw_handle(stdout_read as _)) };
    // SAFETY: `stderr_read` is an open parent pipe handle whose sole ownership transfers here.
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

/// Test-only failure injection for the LUA spawn path (SEC-11). Never compiled into a
/// release build.
#[cfg(test)]
pub(crate) mod lua_test_hook {
    use std::cell::Cell;
    pub(crate) const OFF: u8 = 0;
    /// Behave as an elevated worker whose LUA token could not be created.
    pub(crate) const NO_TOKEN: u8 = 1;
    /// Behave as an elevated worker whose `CreateProcessAsUserW` fails (invalid token).
    pub(crate) const BAD_TOKEN: u8 = 2;
    // Per thread, so a test that injects a failure cannot affect tests running in parallel.
    thread_local! {
        pub(crate) static MODE: Cell<u8> = const { Cell::new(OFF) };
    }
    pub(crate) fn mode() -> u8 {
        MODE.with(Cell::get)
    }
}
