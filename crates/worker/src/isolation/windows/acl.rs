//! Windows SID management, DACL/SDDL manipulation, and ACL grant manifest logging.

use super::path::{
    components_ci_eq, expected_lexical_long_path, is_reparse_point, normalize_win_path,
    strip_verbatim_prefix, to_wide,
};
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::windows::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use windows_sys::Win32::Foundation::{
    CloseHandle, LocalFree, ERROR_SUCCESS, HANDLE, HLOCAL, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSecurityDescriptorToStringSecurityDescriptorW, ConvertSidToStringSidW,
    ConvertStringSecurityDescriptorToSecurityDescriptorW, ConvertStringSidToSidW,
    GetNamedSecurityInfoW, GetSecurityInfo, SetEntriesInAclW, SetNamedSecurityInfoW,
    SetSecurityInfo, EXPLICIT_ACCESS_W, SDDL_REVISION_1, SE_FILE_OBJECT, TRUSTEE_IS_SID,
    TRUSTEE_IS_UNKNOWN, TRUSTEE_W,
};
use windows_sys::Win32::Security::{
    AclSizeInformation, CreateWellKnownSid, DeleteAce, DeriveCapabilitySidsFromName, EqualSid,
    GetAce, GetAclInformation, GetLengthSid, GetSecurityDescriptorControl,
    GetSecurityDescriptorDacl, GetTokenInformation, IsValidSid, SetFileSecurityW, TokenUser,
    ACCESS_ALLOWED_ACE, ACE_FLAGS, ACE_HEADER, ACL, ACL_SIZE_INFORMATION,
    DACL_SECURITY_INFORMATION, INHERITED_ACE, INHERIT_ONLY_ACE,
    PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SECURITY_MAX_SID_SIZE,
    TOKEN_QUERY, TOKEN_USER, UNPROTECTED_DACL_SECURITY_INFORMATION, WELL_KNOWN_SID_TYPE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, GetFileInformationByHandle, GetFinalPathNameByHandleW, BY_HANDLE_FILE_INFORMATION,
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
    FILE_FLAG_OPEN_REPARSE_POINT, FILE_NAME_NORMALIZED, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING, READ_CONTROL, VOLUME_NAME_DOS, WRITE_DAC,
};
use windows_sys::Win32::System::SystemServices::{ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// Custom capability name derived via `DeriveCapabilitySidsFromName` and granted only to
/// OxideSwarm task AppContainers and worker-owned toolchain directories.
pub const OXIDESWARM_TOOLCHAIN_CAPABILITY_NAME: &str = "oxideswarm.toolchain";

/// RAII wrapper for a SID allocated from a well-known SID, current process token, SDDL string,
/// or `DeriveCapabilitySidsFromName`.
pub struct OwnedSid(Vec<u8>);

impl OwnedSid {
    fn from_raw_psid(raw: PSID) -> io::Result<Self> {
        // SAFETY: `raw` is checked non-null first and points to the caller's live SID allocation.
        if raw.is_null() || unsafe { IsValidSid(raw) } == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Windows SID pointer",
            ));
        }
        // SAFETY: `IsValidSid(raw)` returned non-zero above, so `GetLengthSid` reads its valid SID header.
        let len = unsafe { GetLengthSid(raw) } as usize;
        // SAFETY: `raw` points to a validated Win32 SID allocation of `len` readable bytes alive for this copy.
        let slice = unsafe { std::slice::from_raw_parts(raw as *const u8, len) };
        Ok(Self(slice.to_vec()))
    }

    pub fn from_sid_string(sid_str: &str) -> io::Result<Self> {
        let wide = to_wide(OsStr::new(sid_str));
        let mut raw_sid: PSID = std::ptr::null_mut();
        // SAFETY: `wide` is a NUL-terminated UTF-16 `Vec<u16>` alive for this call, and `&mut raw_sid`
        // receives a `LocalAlloc` SID freed via `LocalFree` below.
        let ok = unsafe { ConvertStringSidToSidW(wide.as_ptr(), &mut raw_sid) };
        if ok == 0 || raw_sid.is_null() {
            return Err(io::Error::last_os_error());
        }
        let res = Self::from_raw_psid(raw_sid);
        // SAFETY: `raw_sid` was allocated by `ConvertStringSidToSidW`, copied above, and freed once.
        unsafe {
            LocalFree(raw_sid as HLOCAL);
        }
        res
    }

    pub fn from_well_known(sid_type: WELL_KNOWN_SID_TYPE) -> io::Result<Self> {
        let mut buf = vec![0u8; SECURITY_MAX_SID_SIZE as usize];
        let mut size = buf.len() as u32;
        // SAFETY: `buf` provides `SECURITY_MAX_SID_SIZE` writable bytes matching `size`.
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
        // SAFETY: `GetCurrentProcess()` returns the current process pseudo-handle, and `&mut token`
        // receives an owned token handle closed below.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut needed = 0u32;
        // SAFETY: `token` is a valid handle from `OpenProcessToken`; null/0 performs the Win32 size query.
        unsafe {
            let _ = GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut needed);
        }
        let mut buf = vec![0u8; needed as usize];
        // SAFETY: `buf` holds `needed` writable bytes matching the length parameter passed to `GetTokenInformation`.
        let ok = unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                buf.as_mut_ptr() as *mut _,
                needed,
                &mut needed,
            )
        };
        // SAFETY: `token` was opened by `OpenProcessToken` above and is closed once here.
        unsafe {
            CloseHandle(token);
        }
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `GetTokenInformation(TokenUser)` succeeded, populating `TOKEN_USER` (and its trailing SID)
        // inside the heap-allocated `buf`.
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
        // SAFETY: `wide` is NUL-terminated UTF-16 alive for this call; `group_sids` and `cap_sids`
        // receive `LocalAlloc` SID pointer arrays freed below.
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
            // SAFETY: `cap_sids` is non-null with `cap_count > 0`, so index 0 points to a live SID.
            let first = unsafe { *cap_sids };
            Self::from_raw_psid(first)
        } else {
            Err(io::Error::other(
                "DeriveCapabilitySidsFromName returned zero capability SIDs",
            ))
        };
        // SAFETY: `group_sids` and `cap_sids` (plus `group_count`/`cap_count` element SIDs) were
        // allocated by `DeriveCapabilitySidsFromName` and are freed once here after copying.
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
        // SAFETY: `self.as_psid()` points to this `OwnedSid`'s validated SID buffer, and `wide_ptr`
        // receives a NUL-terminated `LocalAlloc` UTF-16 string freed via `LocalFree` below.
        let ok = unsafe { ConvertSidToStringSidW(self.as_psid(), &mut wide_ptr) };
        if ok == 0 || wide_ptr.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut len = 0usize;
        // SAFETY: `ConvertSidToStringSidW` guarantees `wide_ptr` is NUL-terminated when it succeeds.
        unsafe {
            while *wide_ptr.add(len) != 0 {
                len += 1;
            }
        }
        // SAFETY: `wide_ptr` points to `len` initialized `u16` elements before its trailing NUL.
        let slice = unsafe { std::slice::from_raw_parts(wide_ptr, len) };
        let s = OsString::from_wide(slice).to_string_lossy().into_owned();
        // SAFETY: `wide_ptr` was allocated by `ConvertSidToStringSidW` and is freed once after copying.
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

pub(in crate::isolation) fn explicit_access(
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

/// # Safety
/// Caller must ensure `dacl` (when non-null) points to a valid Windows `ACL` inside a live
/// security descriptor and `target_sid` is a valid Win32 SID pointer.
unsafe fn dacl_ptr_has_ace(
    dacl: *mut ACL,
    target_sid: PSID,
    ace_type: u32,
    required_mask: u32,
    required_flags: u32,
) -> bool {
    if dacl.is_null() {
        return false;
    }
    let mut acl_info = ACL_SIZE_INFORMATION::default();
    // SAFETY: `dacl` is non-null inside a live descriptor, and `acl_info` has `size_of::<ACL_SIZE_INFORMATION>()` writable bytes.
    let ok = unsafe {
        GetAclInformation(
            dacl,
            &mut acl_info as *mut _ as *mut _,
            std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        )
    };
    if ok == 0 {
        return false;
    }
    for idx in 0..acl_info.AceCount {
        let mut ace_ptr: *mut core::ffi::c_void = std::ptr::null_mut();
        // SAFETY: `dacl` is live and `idx < acl_info.AceCount`.
        if unsafe { GetAce(dacl, idx, &mut ace_ptr) } != 0 && !ace_ptr.is_null() {
            // SAFETY: `GetAce` succeeded with non-null `ace_ptr` pointing to a DWORD-aligned ACE header inside `dacl`.
            let header = unsafe { &*(ace_ptr as *const ACE_HEADER) };
            if header.AceType as u32 == ace_type
                && (header.AceFlags as u32 & INHERIT_ONLY_ACE) == 0
                && (header.AceFlags as u32 & required_flags) == required_flags
            {
                // SAFETY: `ace_type` is allow or deny (both start with the `ACCESS_ALLOWED_ACE` layout),
                // DWORD-aligned inside live `dacl`.
                let ace = unsafe { &*(ace_ptr as *const ACCESS_ALLOWED_ACE) };
                let ace_sid = (&ace.SidStart as *const u32) as PSID;
                // SAFETY: `ace_sid` lies inside the live ACE and `target_sid` points to the caller's SID.
                if unsafe { EqualSid(ace_sid, target_sid) } != 0
                    && (ace.Mask & required_mask) == required_mask
                {
                    return true;
                }
            }
        }
    }
    false
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
    // SAFETY: `wide_path` is NUL-terminated UTF-16 alive for this call; `dacl` points into `sd`,
    // which owns the returned `LocalAlloc` descriptor freed below.
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
            // SAFETY: `sd` was allocated by `GetNamedSecurityInfoW` above and is freed once here.
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        return false;
    }

    // SAFETY: `dacl` lies inside live `sd`, and `target_sid` is the caller's valid SID pointer.
    let found =
        unsafe { dacl_ptr_has_ace(dacl, target_sid, ace_type, required_mask, required_flags) };
    if !sd.is_null() {
        // SAFETY: `sd` is non-null, allocated by `GetNamedSecurityInfoW`, no longer borrowed by
        // `dacl` after `dacl_ptr_has_ace`, and freed once here.
        unsafe {
            LocalFree(sd as HLOCAL);
        }
    }
    found
}

pub(super) fn handle_dacl_grants_sid(handle: HANDLE, target_sid: PSID, required_mask: u32) -> bool {
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: `handle` is an open object handle with `READ_CONTROL`; `dacl` borrows from `sd`,
    // which owns the returned `LocalAlloc` descriptor freed below.
    let rc = unsafe {
        GetSecurityInfo(
            handle,
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
            // SAFETY: `sd` was allocated by `GetSecurityInfo` above and is freed once here.
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        return false;
    }

    // SAFETY: `dacl` lies inside live `sd`, and `target_sid` is the caller's valid SID pointer.
    let found =
        unsafe { dacl_ptr_has_ace(dacl, target_sid, ACCESS_ALLOWED_ACE_TYPE, required_mask, 0) };
    if !sd.is_null() {
        // SAFETY: `sd` is non-null, allocated by `GetSecurityInfo`, no longer borrowed by `dacl`,
        // and freed once here.
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
pub(super) fn merge_entries_on_path(path: &Path, entries: &[EXPLICIT_ACCESS_W]) -> io::Result<()> {
    if !path.exists() || entries.is_empty() || is_reparse_point(path) {
        return Ok(());
    }
    let wide_path = to_wide(path.as_os_str());
    let mut old_dacl: *mut ACL = std::ptr::null_mut();
    let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: `wide_path` is NUL-terminated UTF-16 alive for this call; `old_dacl` borrows from `sd`,
    // which owns the descriptor buffer freed via `LocalFree` below.
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
            // SAFETY: `sd` was allocated by `GetNamedSecurityInfoW` above and is freed once here.
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        return Err(io::Error::from_raw_os_error(rc as i32));
    }

    let mut new_dacl: *mut ACL = std::ptr::null_mut();
    // SAFETY: `entries.as_ptr()` points to `entries.len()` initialized `EXPLICIT_ACCESS_W` values
    // borrowing valid SIDs, `old_dacl` is null or inside still-live `sd`, and `new_dacl` receives
    // an ACL freed via `LocalFree` below.
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
            // SAFETY: `sd` is freed once here on `SetEntriesInAclW` failure.
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        return Err(io::Error::from_raw_os_error(rc as i32));
    }

    // SAFETY: `wide_path` is live NUL-terminated UTF-16 and `new_dacl` was allocated by `SetEntriesInAclW`.
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
    // SAFETY: `new_dacl` and `sd` are no longer referenced after `SetNamedSecurityInfoW` and are each freed once.
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
pub(super) fn merge_entries_on_path_unprotected(
    path: &Path,
    entries: &[EXPLICIT_ACCESS_W],
) -> io::Result<()> {
    if !path.exists() || entries.is_empty() || is_reparse_point(path) {
        return Ok(());
    }
    let wide_path = to_wide(path.as_os_str());
    let mut old_dacl: *mut ACL = std::ptr::null_mut();
    let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: `wide_path` is NUL-terminated UTF-16 alive for this call; `old_dacl` borrows from `sd`,
    // which is freed via `LocalFree` below.
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
            // SAFETY: `sd` was allocated by `GetNamedSecurityInfoW` above and is freed once here.
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        return Err(io::Error::from_raw_os_error(rc as i32));
    }

    let mut new_dacl: *mut ACL = std::ptr::null_mut();
    // SAFETY: `entries.as_ptr()` points to `entries.len()` initialized `EXPLICIT_ACCESS_W` values,
    // `old_dacl` is null or inside live `sd`, and `new_dacl` receives an ACL freed below.
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
            // SAFETY: `sd` is freed once here on `SetEntriesInAclW` failure.
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        return Err(io::Error::from_raw_os_error(rc as i32));
    }

    // SAFETY: `wide_path` is live NUL-terminated UTF-16 and `new_dacl` was allocated by `SetEntriesInAclW`.
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
    // SAFETY: `new_dacl` and `sd` are no longer needed after `SetNamedSecurityInfoW` and are each freed once.
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

/// RAII wrapper closing an owned Win32 `HANDLE` on drop.
pub(super) struct OwnedHandle(pub(super) HANDLE);

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
            // SAFETY: `self.0` is an owned non-null Win32 handle opened by `CreateFileW`.
            unsafe {
                let _ = CloseHandle(self.0);
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ExpectedHandleKind {
    Directory { allow_missing: bool },
    SingleLinkFile { allow_missing: bool },
}

fn final_path_from_handle(handle: HANDLE) -> io::Result<PathBuf> {
    let mut buf = vec![0u16; 260];
    // SAFETY: `handle` is a valid open file/directory handle and `buf` has `buf.len()` writable `u16` elements.
    let mut len = unsafe {
        GetFinalPathNameByHandleW(
            handle,
            buf.as_mut_ptr(),
            buf.len() as u32,
            FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
        )
    };
    if len == 0 {
        return Err(io::Error::last_os_error());
    }
    if (len as usize) >= buf.len() {
        buf.resize(len as usize + 1, 0);
        // SAFETY: `handle` remains open and `buf` was resized to hold `len` UTF-16 code units.
        len = unsafe {
            GetFinalPathNameByHandleW(
                handle,
                buf.as_mut_ptr(),
                buf.len() as u32,
                FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
            )
        };
    }
    if len == 0 || (len as usize) >= buf.len() {
        return Err(io::Error::last_os_error());
    }
    let path = OsString::from_wide(&buf[..len as usize]);
    Ok(strip_verbatim_prefix(PathBuf::from(path)))
}

/// Opens `path` without following a final reparse point (`FILE_FLAG_BACKUP_SEMANTICS |
/// FILE_FLAG_OPEN_REPARSE_POINT`), reads `GetFileInformationByHandle` to verify non-reparse type
/// and (for files) `nNumberOfLinks == 1`, and compares `GetFinalPathNameByHandleW` against the
/// lexical long path so ancestor symlinks or junctions swapped before open are refused before
/// touching the handle's DACL.
pub(super) fn open_verified_dacl_handle(
    path: &Path,
    expect: ExpectedHandleKind,
) -> io::Result<Option<(OwnedHandle, BY_HANDLE_FILE_INFORMATION)>> {
    let allow_missing = match expect {
        ExpectedHandleKind::Directory { allow_missing }
        | ExpectedHandleKind::SingleLinkFile { allow_missing } => allow_missing,
    };
    let wide_path = to_wide(path.as_os_str());
    // SAFETY: `wide_path` is a NUL-terminated UTF-16 `Vec<u16>` alive across `CreateFileW`.
    let raw = unsafe {
        CreateFileW(
            wide_path.as_ptr(),
            WRITE_DAC | READ_CONTROL | FILE_READ_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null_mut(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if raw == INVALID_HANDLE_VALUE {
        let err = io::Error::last_os_error();
        if allow_missing && err.kind() == io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(err);
    }
    let handle = OwnedHandle(raw);
    // SAFETY: `BY_HANDLE_FILE_INFORMATION` contains only plain integer and `FILETIME` fields.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: `handle.0` is an open handle from `CreateFileW` and `&mut info` is writable.
    if unsafe { GetFileInformationByHandle(handle.0, &mut info) } == 0 {
        let err = io::Error::last_os_error();
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "refusing to set DACL on '{}' because file information (GetFileInformationByHandle) could not be read: {err}",
                path.display()
            ),
        ));
    }
    if (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("refusing to set DACL on reparse point '{}'", path.display()),
        ));
    }
    match expect {
        ExpectedHandleKind::Directory { .. } => {
            if (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "refusing to set directory DACL on non-directory '{}'",
                        path.display()
                    ),
                ));
            }
        }
        ExpectedHandleKind::SingleLinkFile { .. } => {
            if (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "refusing to set file DACL on directory '{}'",
                        path.display()
                    ),
                ));
            }
            if info.nNumberOfLinks == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "refusing to set DACL on '{}' because it reported nNumberOfLinks == 0 (cannot confirm a single hard link, nlink == 1)",
                        path.display()
                    ),
                ));
            }
            if info.nNumberOfLinks != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "refusing to set DACL on '{}' because it has {} hard links (must be a single-link regular file, nlink == 1)",
                        path.display(),
                        info.nNumberOfLinks
                    ),
                ));
            }
        }
    }
    let final_path = final_path_from_handle(handle.0).map_err(|e| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "refusing to set DACL on '{}' because GetFinalPathNameByHandleW failed: {e}",
                path.display()
            ),
        )
    })?;
    let lexical_path = expected_lexical_long_path(path);
    if !components_ci_eq(&final_path, &lexical_path) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "refusing to set DACL on '{}' because the opened handle resolved to '{}' instead of the lexical path '{}' (ancestor symlink or junction); pass the target directory directly",
                path.display(),
                final_path.display(),
                lexical_path.display()
            ),
        ));
    }
    Ok(Some((handle, info)))
}

/// Replaces `handle`'s DACL with a fresh protected DACL (`PROTECTED_DACL_SECURITY_INFORMATION`)
/// built purely from `entries` via `SetSecurityInfo`.
pub(super) fn set_protected_dacl_on_handle(
    handle: HANDLE,
    entries: &[EXPLICIT_ACCESS_W],
) -> io::Result<()> {
    let mut new_dacl: *mut ACL = std::ptr::null_mut();
    // SAFETY: `entries.as_ptr()` points to `entries.len()` initialized `EXPLICIT_ACCESS_W` values
    // with null existing ACL, and `new_dacl` receives a `LocalAlloc` ACL freed below.
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
    // SAFETY: `handle` is an open object handle with `WRITE_DAC` and `new_dacl` is a live ACL
    // created by `SetEntriesInAclW`.
    let set_rc = unsafe {
        SetSecurityInfo(
            handle,
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            new_dacl,
            std::ptr::null(),
        )
    };
    // SAFETY: `new_dacl` is no longer referenced after `SetSecurityInfo` and is freed once.
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

/// Replaces directory `path`'s DACL with a fresh protected DACL through a verified no-follow
/// handle (`open_verified_dacl_handle` + `SetSecurityInfo`), refusing symlinks, junctions,
/// non-directories, and ancestor reparse-point swaps.
pub(in crate::isolation) fn set_protected_dacl_on_path(
    path: &Path,
    entries: &[EXPLICIT_ACCESS_W],
) -> io::Result<()> {
    let Some((handle, _)) = open_verified_dacl_handle(
        path,
        ExpectedHandleKind::Directory {
            allow_missing: false,
        },
    )?
    else {
        return Ok(());
    };
    set_protected_dacl_on_handle(handle.0, entries)
}

/// Replaces regular single-link file `path`'s DACL with a fresh protected DACL through a verified
/// no-follow handle when `path` exists (`Ok(())` if missing), refusing symlinks, directories,
/// hard-linked files (`nlink != 1`), and ancestor reparse-point swaps.
pub(super) fn set_protected_dacl_on_single_link_file_if_exists(
    path: &Path,
    entries: &[EXPLICIT_ACCESS_W],
) -> io::Result<()> {
    let Some((handle, _)) = open_verified_dacl_handle(
        path,
        ExpectedHandleKind::SingleLinkFile {
            allow_missing: true,
        },
    )?
    else {
        return Ok(());
    };
    set_protected_dacl_on_handle(handle.0, entries)
}

/// # Safety
/// Caller must ensure `sd` is a valid live `PSECURITY_DESCRIPTOR` returned by a Win32 security API.
unsafe fn sd_to_dacl_sddl(sd: PSECURITY_DESCRIPTOR) -> io::Result<String> {
    let mut sddl_ptr: *mut u16 = std::ptr::null_mut();
    let mut sddl_len: u32 = 0;
    // SAFETY: `sd` is a live security descriptor from the caller, and `sddl_ptr` receives a
    // NUL-terminated `LocalAlloc` UTF-16 string freed via `LocalFree` below.
    let ok = unsafe {
        ConvertSecurityDescriptorToStringSecurityDescriptorW(
            sd,
            SDDL_REVISION_1,
            DACL_SECURITY_INFORMATION,
            &mut sddl_ptr,
            &mut sddl_len,
        )
    };
    if ok == 0 || sddl_ptr.is_null() {
        return Err(io::Error::last_os_error());
    }
    let mut len = 0usize;
    // SAFETY: On success `ConvertSecurityDescriptorToStringSecurityDescriptorW` writes a
    // NUL-terminated UTF-16 string at `sddl_ptr`.
    unsafe {
        while *sddl_ptr.add(len) != 0 {
            len += 1;
        }
    }
    // SAFETY: `sddl_ptr` has at least `len` initialized `u16` elements before its trailing NUL.
    let slice = unsafe { std::slice::from_raw_parts(sddl_ptr, len) };
    let sddl = OsString::from_wide(slice).to_string_lossy().into_owned();
    // SAFETY: `sddl_ptr` was allocated by `ConvertSecurityDescriptorToStringSecurityDescriptorW`
    // and is freed once after copying.
    unsafe {
        LocalFree(sddl_ptr as HLOCAL);
    }
    Ok(sddl)
}

pub(super) fn get_handle_dacl_sddl(handle: HANDLE) -> io::Result<String> {
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: `handle` is an open object handle with `READ_CONTROL`; `sd` receives an owned
    // `LocalAlloc` security descriptor freed via `LocalFree` below.
    let rc = unsafe {
        GetSecurityInfo(
            handle,
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut dacl,
            std::ptr::null_mut(),
            &mut sd,
        )
    };
    if rc != ERROR_SUCCESS || sd.is_null() {
        if !sd.is_null() {
            // SAFETY: `sd` was allocated by `GetSecurityInfo` above and is freed once here.
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        return Err(if rc != ERROR_SUCCESS {
            io::Error::from_raw_os_error(rc as i32)
        } else {
            io::Error::last_os_error()
        });
    }
    // SAFETY: `sd` is the live non-null security descriptor returned by `GetSecurityInfo`.
    let res = unsafe { sd_to_dacl_sddl(sd) };
    // SAFETY: `sd` is no longer needed after conversion and is freed once here.
    unsafe {
        LocalFree(sd as HLOCAL);
    }
    res
}

/// Queries the current DACL of `path` as an SDDL string (`D:...`) via
/// `GetNamedSecurityInfoW` + `ConvertSecurityDescriptorToStringSecurityDescriptorW`.
pub fn get_path_dacl_sddl(path: &Path) -> io::Result<String> {
    let wide_path = to_wide(path.as_os_str());
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: `wide_path` is NUL-terminated UTF-16 alive for this call; `sd` receives an owned
    // `LocalAlloc` security descriptor freed via `LocalFree` below.
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
    if rc != ERROR_SUCCESS || sd.is_null() {
        if !sd.is_null() {
            // SAFETY: `sd` was allocated by `GetNamedSecurityInfoW` above and is freed once here.
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        return Err(if rc != ERROR_SUCCESS {
            io::Error::from_raw_os_error(rc as i32)
        } else {
            io::Error::last_os_error()
        });
    }
    // SAFETY: `sd` is the live non-null security descriptor returned by `GetNamedSecurityInfoW`.
    let res = unsafe { sd_to_dacl_sddl(sd) };
    // SAFETY: `sd` is no longer needed after conversion and is freed once here.
    unsafe {
        LocalFree(sd as HLOCAL);
    }
    res
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
    // SAFETY: `wide_sddl` is a NUL-terminated UTF-16 string alive for this call; `sd` receives an
    // owned self-relative descriptor freed via `LocalFree` below.
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
    // SAFETY: `sd` is non-null from `ConvertStringSecurityDescriptorToSecurityDescriptorW` and
    // `control`/`revision` are valid out-pointers.
    let _ = unsafe { GetSecurityDescriptorControl(sd, &mut control, &mut revision) };
    let is_protected = (control & 0x1000) != 0 || sddl.starts_with("D:P");

    let mut dacl_present: i32 = 0;
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut dacl_defaulted: i32 = 0;
    // SAFETY: `sd` is non-null and live, and `dacl` (when `dacl_present != 0`) points inside `sd`.
    let dacl_ok =
        unsafe { GetSecurityDescriptorDacl(sd, &mut dacl_present, &mut dacl, &mut dacl_defaulted) };
    if dacl_ok == 0 || dacl_present == 0 {
        // SAFETY: `sd` is non-null from `ConvertStringSecurityDescriptorToSecurityDescriptorW` and
        // is freed once here on failure.
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
        // SAFETY: `dacl` is a valid non-null ACL inside live `sd`, and `acl_info` has `size_of::<ACL_SIZE_INFORMATION>()` writable bytes.
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
                // SAFETY: `dacl` is live inside `sd` and `idx < acl_info.AceCount` (iterating downward prevents index shift).
                if unsafe { GetAce(dacl, idx, &mut ace_ptr) } != 0 && !ace_ptr.is_null() {
                    // SAFETY: `GetAce` returned a valid non-null ACE header inside `dacl`.
                    let header = unsafe { &*(ace_ptr as *const ACE_HEADER) };
                    if (header.AceFlags as u32 & INHERITED_ACE) != 0 {
                        // SAFETY: `dacl` is a valid mutable ACL inside `sd` and `idx` is within bounds.
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
    // SAFETY: `wide_path` is NUL-terminated UTF-16 and `dacl` (if non-null) stays live inside `sd`.
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
    // SAFETY: `sd` is no longer borrowed by `dacl` after `SetNamedSecurityInfoW` and is freed once here.
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
        // SAFETY: `wide_sddl` is a live NUL-terminated UTF-16 buffer; `raw_sd` receives a `LocalAlloc`
        // security descriptor freed below.
        let raw_ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide_sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut raw_sd,
                &mut raw_size,
            )
        };
        if raw_ok != 0 && !raw_sd.is_null() {
            // SAFETY: `wide_path` is live NUL-terminated UTF-16 and `raw_sd` is a valid security
            // descriptor freed once via `LocalFree` after `SetFileSecurityW` returns.
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
pub(super) fn remove_sid_aces_from_path(path: &Path, target_sid: PSID) -> io::Result<()> {
    if !path.exists() || is_reparse_point(path) {
        return Ok(());
    }
    let wide_path = to_wide(path.as_os_str());
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: `wide_path` is NUL-terminated UTF-16 alive for this call; `dacl` points into `sd`,
    // which is freed via `LocalFree` below.
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
            // SAFETY: `sd` was allocated by `GetNamedSecurityInfoW` above and is freed once here.
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        return Err(io::Error::from_raw_os_error(rc as i32));
    }
    if dacl.is_null() {
        if !sd.is_null() {
            // SAFETY: `sd` is freed once here before returning early.
            unsafe {
                LocalFree(sd as HLOCAL);
            }
        }
        return Ok(());
    }

    let mut acl_info = ACL_SIZE_INFORMATION::default();
    // SAFETY: `dacl` is a non-null ACL inside live `sd`, and `acl_info` has `size_of::<ACL_SIZE_INFORMATION>()` writable bytes.
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
            // SAFETY: `dacl` is live inside `sd` and `idx < acl_info.AceCount` (iterating downward prevents index shift).
            if unsafe { GetAce(dacl, idx, &mut ace_ptr) } != 0 && !ace_ptr.is_null() {
                // SAFETY: `GetAce` returned a non-null DWORD-aligned `ACE_HEADER` inside `dacl`.
                let header = unsafe { &*(ace_ptr as *const ACE_HEADER) };
                let ace_type = header.AceType as u32;
                if ace_type == ACCESS_ALLOWED_ACE_TYPE || ace_type == ACCESS_DENIED_ACE_TYPE {
                    // SAFETY: Allow/deny ACEs share the `ACCESS_ALLOWED_ACE` header+SID prefix inside `dacl`.
                    let ace = unsafe { &*(ace_ptr as *const ACCESS_ALLOWED_ACE) };
                    let ace_sid = (&ace.SidStart as *const u32) as PSID;
                    // SAFETY: `ace_sid` is inside live `dacl` and callers pass a valid `target_sid` (`OwnedSid`).
                    if unsafe { EqualSid(ace_sid, target_sid) } != 0
                        // SAFETY: `dacl` is a valid mutable ACL inside `sd` and `idx < acl_info.AceCount`.
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
        // SAFETY: `wide_path` is NUL-terminated UTF-16 and `dacl` remains valid inside still-live `sd`.
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
        // SAFETY: `sd` is no longer needed after updating the DACL and is freed once here.
        unsafe {
            LocalFree(sd as HLOCAL);
        }
    }
    if set_rc != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(set_rc as i32));
    }
    Ok(())
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

pub(super) fn record_acl_grant(
    manifest_path: Option<&Path>,
    granted_path: &Path,
) -> io::Result<()> {
    let norm = normalize_win_path(granted_path);
    let line = norm.to_string_lossy().into_owned();
    record_manifest_line(manifest_path, &line)
}

pub(super) fn record_sddl_restore(
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
