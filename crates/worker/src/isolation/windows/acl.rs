//! Windows SID management, DACL/SDDL manipulation, and ACL grant manifest logging.

use super::path::{is_reparse_point, normalize_win_path, to_wide};
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::windows::ffi::OsStringExt;
use std::path::Path;
use windows_sys::Win32::Foundation::{CloseHandle, LocalFree, ERROR_SUCCESS, HANDLE, HLOCAL};
use windows_sys::Win32::Security::Authorization::{
    ConvertSecurityDescriptorToStringSecurityDescriptorW, ConvertSidToStringSidW,
    ConvertStringSecurityDescriptorToSecurityDescriptorW, ConvertStringSidToSidW,
    GetNamedSecurityInfoW, SetEntriesInAclW, SetNamedSecurityInfoW, EXPLICIT_ACCESS_W,
    SDDL_REVISION_1, SE_FILE_OBJECT, TRUSTEE_IS_SID, TRUSTEE_IS_UNKNOWN, TRUSTEE_W,
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

pub(super) fn explicit_access(
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
pub(super) fn merge_entries_on_path(path: &Path, entries: &[EXPLICIT_ACCESS_W]) -> io::Result<()> {
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
pub(super) fn set_protected_dacl_on_path(
    path: &Path,
    entries: &[EXPLICIT_ACCESS_W],
) -> io::Result<()> {
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
    let dacl_ok =
        unsafe { GetSecurityDescriptorDacl(sd, &mut dacl_present, &mut dacl, &mut dacl_defaulted) };
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
pub(super) fn remove_sid_aces_from_path(path: &Path, target_sid: PSID) -> io::Result<()> {
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
