//! Isolated per-task Windows Window Station and Desktop creation.

use super::acl::{explicit_access, OwnedSid};
use super::path::to_wide;
use super::spawn::ensure_process_token_default_dacl_grants_current_user;
use super::WINDOWS_SPAWN_LOCK;
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::windows::ffi::OsStringExt;
use std::sync::OnceLock;
use windows_sys::Win32::Foundation::{LocalFree, ERROR_SUCCESS, HANDLE, HLOCAL};
use windows_sys::Win32::Security::Authorization::{
    GetSecurityInfo, SetEntriesInAclW, SetSecurityInfo, EXPLICIT_ACCESS_W, GRANT_ACCESS,
    SET_ACCESS, SE_WINDOW_OBJECT,
};
use windows_sys::Win32::Security::{
    WinBuiltinAdministratorsSid, WinLocalSystemSid, ACL, DACL_SECURITY_INFORMATION, NO_INHERITANCE,
    PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES,
};

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
    pub(super) fn CloseDesktop(hdesktop: HANDLE) -> i32;
    fn GetUserObjectInformationW(
        hobj: HANDLE,
        nindex: i32,
        pvinfo: *mut core::ffi::c_void,
        nlength: u32,
        lpnlengthneeded: *mut u32,
    ) -> i32;
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
    // SAFETY: `entries.as_ptr()` points to `entries.len()` initialized `EXPLICIT_ACCESS_W` values
    // borrowing live SIDs for the duration of this call; on success `new_dacl` receives a
    // `LocalAlloc` ACL freed via `LocalFree` below.
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
    // SAFETY: `handle` was checked non-null above and identifies a live Window Station or Desktop
    // opened by this process, and `new_dacl` is the non-null ACL allocated by `SetEntriesInAclW`.
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
    // SAFETY: `new_dacl` is a non-null heap buffer allocated by `SetEntriesInAclW`, no longer
    // referenced after `SetSecurityInfo` returns, and freed once here.
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
    // SAFETY: `handle` was checked non-null above and refers to a live window object; `old_dacl`
    // and `sd` are valid out-pointers (`sd` owns the descriptor memory freed via `LocalFree` below).
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
    // SAFETY: `&ea` points to one initialized `EXPLICIT_ACCESS_W` borrowing a valid `sid`;
    // `old_dacl` is either null or points into the still-live `sd` allocation from `GetSecurityInfo`.
    let rc2 = unsafe { SetEntriesInAclW(1, &ea, old_dacl, &mut new_dacl) };
    let mut set_rc = rc2;
    if rc2 == ERROR_SUCCESS && !new_dacl.is_null() {
        // SAFETY: `new_dacl` is the valid non-null ACL returned by `SetEntriesInAclW`, live for
        // `SetSecurityInfo(handle, ...)` and then freed once via `LocalFree`.
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
        // SAFETY: `sd` is non-null, allocated by `GetSecurityInfo`, no longer borrowed by
        // `old_dacl` after `SetEntriesInAclW`, and freed once here.
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
pub(super) fn create_per_task_desktop(
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
        let system_sid = OwnedSid::from_well_known(WinLocalSystemSid).map_err(|e| e.to_string())?;
        let admins_sid =
            OwnedSid::from_well_known(WinBuiltinAdministratorsSid).map_err(|e| e.to_string())?;
        // SAFETY: Under `WINDOWS_SPAWN_LOCK`, `buf` (`512` B) backs `GetUserObjectInformationW`,
        // `wide_default` is NUL-terminated, `orig_winsta` is restored, `default_desk` is closed
        // via `CloseDesktop`, and `alt_winsta` is cached in `WORKER_WINSTA` for process lifetime.
        unsafe {
            // Open/create the logon session's built-in non-interactive Window Station
            // (`NULL` -> `Service-0x0-<LogonId>$`).
            let alt_winsta =
                CreateWindowStationW(std::ptr::null(), 0, 0x000F_037F, std::ptr::null());
            if alt_winsta.is_null() {
                return Err(format!(
                    "CreateWindowStationW failed: {}",
                    io::Error::last_os_error()
                ));
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

    // SAFETY: Under `WINDOWS_SPAWN_LOCK`, `alt_winsta_raw` comes from `WORKER_WINSTA`,
    // `wide_desk_name` is NUL-terminated for `CreateDesktopW`, `orig_winsta` is restored,
    // and the returned desktop handle transfers to `WindowsAppContainerGuard`.
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
        // SAFETY: `alt_desk` is a valid non-null desktop handle created above and is closed once
        // here because `create_per_task_desktop` fails before transferring ownership.
        unsafe {
            let _ = CloseDesktop(alt_desk);
        }
        return Err(e);
    }

    let full_desktop = format!("{winsta_name}\\{desk_name}");
    Ok((alt_desk, to_wide(OsStr::new(&full_desktop))))
}
