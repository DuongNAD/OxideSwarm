//! Private files on Windows (SEC-9, design 0047).
//!
//! On Unix a file that holds key material is created with mode 0600. On Windows a new file
//! inherits the DACL of its directory, and a user's home commonly grants `Everyone` and
//! `BUILTIN\Users` read access with inheritance, so the cluster secret and the master P2P key were
//! readable by every local account. This module closes that gap:
//!
//! * [`create_private_new`] creates a file whose DACL is *protected* (no inherited entries) and
//!   grants full control only to the current user, `SYSTEM` and `BUILTIN\Administrators`. The
//!   descriptor is passed to `CreateFileW`, so the file never exists with a wider DACL.
//! * [`create_private_dir_all`] does the same for a directory that holds such files.
//! * [`warn_if_readable_by_others`] logs a warning, with the `icacls` command that fixes it, when
//!   an existing file grants access to anyone else.
//!
//! The decision logic ([`open_grants`], [`private_sddl`], [`open_acl_warning`]) is pure and builds
//! on every platform; only the functions that call Win32 are `#[cfg(windows)]`.

use std::path::Path;

/// `SYSTEM`.
pub const SID_SYSTEM: &str = "S-1-5-18";
/// `BUILTIN\Administrators`.
pub const SID_ADMINISTRATORS: &str = "S-1-5-32-544";
/// `CREATOR OWNER`.
pub const SID_CREATOR_OWNER: &str = "S-1-3-0";
/// `OWNER RIGHTS`.
pub const SID_OWNER_RIGHTS: &str = "S-1-3-4";
/// `Everyone`.
pub const SID_EVERYONE: &str = "S-1-1-0";
/// `NT AUTHORITY\NETWORK`.
pub const SID_NETWORK: &str = "S-1-5-2";
/// `NT AUTHORITY\INTERACTIVE`.
pub const SID_INTERACTIVE: &str = "S-1-5-4";
/// `NT AUTHORITY\ANONYMOUS LOGON`.
pub const SID_ANONYMOUS_LOGON: &str = "S-1-5-7";
/// `NT AUTHORITY\Authenticated Users`.
pub const SID_AUTHENTICATED_USERS: &str = "S-1-5-11";
/// `BUILTIN\Users`.
pub const SID_USERS: &str = "S-1-5-32-545";
/// `BUILTIN\Guests`.
pub const SID_GUESTS: &str = "S-1-5-32-546";
/// `BUILTIN\Remote Desktop Users`.
pub const SID_REMOTE_DESKTOP_USERS: &str = "S-1-5-32-555";
/// `ALL APPLICATION PACKAGES`.
pub const SID_ALL_APP_PACKAGES: &str = "S-1-15-2-1";

/// Access-mask bits that make an entry a problem for a private file: read, write, append, change
/// the DACL, take ownership, or any of the generic rights.
const SENSITIVE_MASK: u32 = 0x0000_0001 // FILE_READ_DATA / FILE_LIST_DIRECTORY
    | 0x0000_0002 // FILE_WRITE_DATA
    | 0x0000_0004 // FILE_APPEND_DATA
    | 0x0004_0000 // WRITE_DAC
    | 0x0008_0000 // WRITE_OWNER
    | 0x1000_0000 // GENERIC_ALL
    | 0x2000_0000 // GENERIC_EXECUTE
    | 0x4000_0000 // GENERIC_WRITE
    | 0x8000_0000; // GENERIC_READ

/// Access-mask bits that let a trustee list, create, delete, or change entries or metadata inside
/// a private task directory. Leaves out traverse-only (`FILE_TRAVERSE | FILE_READ_ATTRIBUTES |
/// SYNCHRONIZE` = `0x0010_00A0`), which task isolation (`protect_sandbox_base`) grants to
/// `oxideswarm.toolchain` without inheritance so an AppContainer child can enter its own sandbox
/// without listing the base directory.
const DIR_SENSITIVE_MASK: u32 = SENSITIVE_MASK
    | 0x0000_0008 // FILE_READ_EA
    | 0x0000_0010 // FILE_WRITE_EA
    | 0x0000_0040 // FILE_DELETE_CHILD
    | 0x0000_0100 // FILE_WRITE_ATTRIBUTES
    | 0x0001_0000; // DELETE

/// One allow entry extracted from a DACL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaclAce {
    /// The trustee as an SDDL string SID (`S-1-5-...`).
    pub sid: String,
    /// The access mask the entry allows.
    pub mask: u32,
}

/// The SDDL text of the protected descriptor: full control for `user_sid`, `SYSTEM` and
/// `BUILTIN\Administrators`, nothing inherited. A directory's entries also flow to the files and
/// subdirectories created in it.
pub fn private_sddl(user_sid: &str, directory: bool) -> String {
    let flags = if directory { "OICI" } else { "" };
    format!("D:P(A;{flags};FA;;;{user_sid})(A;{flags};FA;;;SY)(A;{flags};FA;;;BA)")
}

/// True when `owner_sid` is `current_user_sid`, `SYSTEM` (`S-1-5-18`) or `BUILTIN\Administrators`
/// (`S-1-5-32-544`).
pub fn is_trusted_owner_sid(owner_sid: &str, current_user_sid: &str) -> bool {
    owner_sid.eq_ignore_ascii_case(current_user_sid)
        || owner_sid.eq_ignore_ascii_case(SID_SYSTEM)
        || owner_sid.eq_ignore_ascii_case(SID_ADMINISTRATORS)
}

/// The SIDs in `aces` that are granted access to a private file and are not `current_user_sid`,
/// `SYSTEM` or `BUILTIN\Administrators`. Deny entries are not passed in, and a SID appears once.
pub fn open_grants(aces: &[DaclAce], current_user_sid: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for ace in aces {
        if ace.mask & SENSITIVE_MASK == 0 {
            continue;
        }
        if !is_trusted_owner_sid(&ace.sid, current_user_sid)
            && !out.iter().any(|s| s.eq_ignore_ascii_case(&ace.sid))
        {
            out.push(ace.sid.clone());
        }
    }
    out
}

/// The SIDs in `aces` granted sensitive directory access (`DIR_SENSITIVE_MASK`) that are not
/// `current_user_sid`, `SYSTEM`, `BUILTIN\Administrators`, `CREATOR OWNER` (`S-1-3-0`) or
/// `OWNER RIGHTS` (`S-1-3-4`), each once. Catches broad groups (`Everyone`, `BUILTIN\Users`,
/// `Authenticated Users`, `INTERACTIVE`, `BUILTIN\Guests`, `Domain Users`, `NETWORK`,
/// `ANONYMOUS LOGON`, `Remote Desktop Users`, `ALL APPLICATION PACKAGES`) as well as any other
/// local or domain user SID, while ignoring traverse-only isolation ACEs (`0x0010_00A0`).
pub fn dir_open_grants(aces: &[DaclAce], current_user_sid: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for ace in aces {
        if ace.mask & DIR_SENSITIVE_MASK == 0 {
            continue;
        }
        let allowed = is_trusted_owner_sid(&ace.sid, current_user_sid)
            || ace.sid.eq_ignore_ascii_case(SID_CREATOR_OWNER)
            || ace.sid.eq_ignore_ascii_case(SID_OWNER_RIGHTS);
        if !allowed && !out.iter().any(|s| s.eq_ignore_ascii_case(&ace.sid)) {
            out.push(ace.sid.clone());
        }
    }
    out
}

/// A readable name for well-known SIDs (and `Domain Users`), the SID itself otherwise.
pub fn sid_label(sid: &str) -> String {
    if sid.eq_ignore_ascii_case(SID_SYSTEM) {
        return "SYSTEM".to_string();
    }
    if sid.eq_ignore_ascii_case(SID_ADMINISTRATORS) {
        return "BUILTIN\\Administrators".to_string();
    }
    if sid.eq_ignore_ascii_case(SID_EVERYONE) {
        return "Everyone".to_string();
    }
    if sid.eq_ignore_ascii_case(SID_USERS) {
        return "BUILTIN\\Users".to_string();
    }
    if sid.eq_ignore_ascii_case(SID_AUTHENTICATED_USERS) {
        return "Authenticated Users".to_string();
    }
    if sid.eq_ignore_ascii_case(SID_INTERACTIVE) {
        return "INTERACTIVE".to_string();
    }
    if sid.eq_ignore_ascii_case(SID_GUESTS) {
        return "BUILTIN\\Guests".to_string();
    }
    if sid.eq_ignore_ascii_case(SID_NETWORK) {
        return "NETWORK".to_string();
    }
    if sid.eq_ignore_ascii_case(SID_ANONYMOUS_LOGON) {
        return "ANONYMOUS LOGON".to_string();
    }
    if sid.eq_ignore_ascii_case(SID_REMOTE_DESKTOP_USERS) {
        return "BUILTIN\\Remote Desktop Users".to_string();
    }
    if sid.eq_ignore_ascii_case(SID_ALL_APP_PACKAGES) {
        return "ALL APPLICATION PACKAGES".to_string();
    }
    let upper = sid.to_ascii_uppercase();
    if upper.starts_with("S-1-5-21-") && upper.ends_with("-513") {
        return format!("Domain Users ({sid})");
    }
    sid.to_string()
}

/// The warning for a private file whose DACL grants access to `offenders`, with the `icacls`
/// command that makes it private. `None` when `offenders` is empty.
pub fn open_acl_warning(
    path: &Path,
    what: &str,
    offenders: &[String],
    user_name: &str,
) -> Option<String> {
    if offenders.is_empty() {
        return None;
    }
    let who: Vec<String> = offenders.iter().map(|s| sid_label(s)).collect();
    Some(format!(
        "{what} '{path}' is accessible by other accounts ({who}); to make it private run: \
         icacls \"{path}\" /inheritance:r /grant:r \"{user_name}:F\" SYSTEM:F Administrators:F",
        path = path.display(),
        who = who.join(", "),
    ))
}

/// The warning for an existing task directory whose DACL grants sensitive access to `offenders`,
/// with a locale-independent `icacls` command using `*<user_sid>`, `*S-1-5-18` and
/// `*S-1-5-32-544`. `None` when `offenders` is empty.
pub fn open_dir_acl_warning(path: &Path, offenders: &[String], user_sid: &str) -> Option<String> {
    if offenders.is_empty() {
        return None;
    }
    let who: Vec<String> = offenders.iter().map(|s| sid_label(s)).collect();
    Some(format!(
        "'{path}' lets other accounts ({who}) read or change task files; to make it private run: \
         icacls \"{path}\" /inheritance:r /grant:r \"*{user_sid}:(OI)(CI)F\" \
         \"*S-1-5-18:(OI)(CI)F\" \"*S-1-5-32-544:(OI)(CI)F\"",
        path = path.display(),
        who = who.join(", "),
    ))
}

#[cfg(windows)]
pub use win::{
    create_or_open_private_dir_all, create_or_open_private_dir_all_with_hooks,
    create_private_dir_all, create_private_new, current_user_sid, inspect_dir_security_no_follow,
    read_dacl, warn_if_readable_by_others, write_private_replace, DirSecuritySnapshot,
    PrivateDirOutcome,
};

#[cfg(windows)]
mod win {
    use super::{open_acl_warning, open_grants, private_sddl, DaclAce};
    use std::ffi::c_void;
    use std::fs::File;
    use std::io;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::FromRawHandle;
    use std::path::Path;
    use std::ptr::null_mut;

    use windows_sys::Win32::Foundation::{
        CloseHandle, LocalFree, ERROR_ALREADY_EXISTS, HANDLE, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        GetNamedSecurityInfoW, GetSecurityInfo, SDDL_REVISION_1, SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::{
        AclSizeInformation, GetAce, GetAclInformation, GetLengthSid, GetSecurityDescriptorControl,
        GetTokenInformation, IsValidSid, TokenUser, ACE_HEADER, ACL, ACL_SIZE_INFORMATION,
        DACL_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
        SECURITY_ATTRIBUTES, SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateDirectoryW, CreateFileW, GetFileInformationByHandle, GetVolumeInformationByHandleW,
        BY_HANDLE_FILE_INFORMATION, CREATE_NEW, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL,
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_GENERIC_WRITE, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
        READ_CONTROL,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0x00;
    const ACCESS_ALLOWED_COMPOUND_ACE_TYPE: u8 = 0x04;
    const ACCESS_ALLOWED_OBJECT_ACE_TYPE: u8 = 0x05;
    const ACCESS_ALLOWED_CALLBACK_ACE_TYPE: u8 = 0x09;
    const ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE: u8 = 0x0B;

    const ACE_OBJECT_TYPE_PRESENT: u32 = 0x0000_0001;
    const ACE_INHERITED_OBJECT_TYPE_PRESENT: u32 = 0x0000_0002;
    const GUID_BYTES: usize = 16;
    const SID_MAX_SUB_AUTHORITIES: usize = 15;
    const FILE_PERSISTENT_ACLS: u32 = 0x0000_0008;

    /// `Everyone`: what a NULL DACL (no protection at all) amounts to.
    const FULL_ACCESS: u32 = 0x001F_01FF;

    /// Whether [`create_or_open_private_dir_all`] created the target directory itself or found an
    /// existing file-system entry there (including when another creator won a race).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PrivateDirOutcome {
        /// `CreateDirectoryW` created the directory with the protected owner-only descriptor.
        Created,
        /// An entry already existed at the path before or during `CreateDirectoryW`.
        Existing,
    }

    /// Security and volume attributes of a directory (or file/reparse point at its path), read by
    /// handle with `FILE_FLAG_OPEN_REPARSE_POINT` so links and junctions are never followed.
    #[derive(Debug)]
    pub struct DirSecuritySnapshot {
        /// `FILE_ATTRIBUTE_REPARSE_POINT` is set (junction, symbolic link, mount point).
        pub is_reparse_point: bool,
        /// `FILE_ATTRIBUTE_DIRECTORY` is set.
        pub is_dir: bool,
        /// `FILE_PERSISTENT_ACLS` is reported by `GetVolumeInformationByHandleW`.
        pub persistent_acls: bool,
        /// Owner SID (`S-1-5-...`) from `OWNER_SECURITY_INFORMATION`.
        pub owner_sid: io::Result<String>,
        /// `(aces, protected)` from `DACL_SECURITY_INFORMATION`.
        pub dacl: io::Result<(Vec<DaclAce>, bool)>,
    }

    /// Owns a Win32 `HANDLE` and closes it on drop.
    struct OwnedHandle(HANDLE);

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
                // SAFETY: `self.0` is an open Win32 handle owned exclusively by `OwnedHandle`.
                unsafe { CloseHandle(self.0) };
            }
        }
    }

    /// Owns a buffer allocated by the system with `LocalAlloc`.
    struct LocalBuf(*mut c_void);

    impl Drop for LocalBuf {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: the pointer came from a Win32 call documented to allocate with
                // `LocalAlloc`, and it is freed exactly once, here.
                unsafe { LocalFree(self.0) };
            }
        }
    }

    fn wide(path: &Path) -> io::Result<Vec<u16>> {
        let mut v: Vec<u16> = path.as_os_str().encode_wide().collect();
        if v.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path contains a NUL character",
            ));
        }
        v.push(0);
        Ok(v)
    }

    fn sid_to_string(sid: PSID) -> io::Result<String> {
        let mut raw: *mut u16 = null_mut();
        // SAFETY: `sid` is a valid SID and `raw` receives a system allocation freed below.
        if unsafe { ConvertSidToStringSidW(sid, &mut raw) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let _guard = LocalBuf(raw.cast());
        let mut len = 0;
        // SAFETY: `raw` is a NUL-terminated wide string.
        while unsafe { *raw.add(len) } != 0 {
            len += 1;
        }
        // SAFETY: `len` elements were just read from the same allocation.
        let slice = unsafe { std::slice::from_raw_parts(raw, len) };
        Ok(String::from_utf16_lossy(slice))
    }

    /// The SID of the user this process runs as, as an SDDL string.
    pub fn current_user_sid() -> io::Result<String> {
        let mut token: HANDLE = null_mut();
        // SAFETY: the pseudo handle of the current process is valid; `token` is closed below.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let result = (|| {
            let mut needed = 0u32;
            // SAFETY: a size query with a null buffer; failure with the needed size is expected.
            unsafe { GetTokenInformation(token, TokenUser, null_mut(), 0, &mut needed) };
            if needed == 0 {
                return Err(io::Error::last_os_error());
            }
            // u64 elements keep the buffer aligned for TOKEN_USER.
            let mut buf = vec![0u64; (needed as usize).div_ceil(8)];
            // SAFETY: `buf` holds at least `needed` bytes.
            if unsafe {
                GetTokenInformation(
                    token,
                    TokenUser,
                    buf.as_mut_ptr().cast(),
                    needed,
                    &mut needed,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: the buffer now holds a TOKEN_USER whose SID points into the same buffer.
            let sid = unsafe { (*buf.as_ptr().cast::<TOKEN_USER>()).User.Sid };
            sid_to_string(sid)
        })();
        // SAFETY: `token` was opened above and is closed once.
        unsafe { CloseHandle(token) };
        result
    }

    /// Builds the protected security descriptor for `user_sid` (see [`private_sddl`]).
    fn private_descriptor(directory: bool) -> io::Result<LocalBuf> {
        let sddl = private_sddl(&current_user_sid()?, directory);
        let wide_sddl: Vec<u16> = sddl.encode_utf16().chain(std::iter::once(0)).collect();
        let mut sd: PSECURITY_DESCRIPTOR = null_mut();
        // SAFETY: `wide_sddl` is NUL-terminated; `sd` receives a system allocation.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide_sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut sd,
                null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(LocalBuf(sd))
    }

    fn attributes(sd: &LocalBuf) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: sd.0,
            bInheritHandle: 0,
        }
    }

    /// Creates `path` exclusively (it must not exist) with a protected DACL that grants full
    /// control to the current user, `SYSTEM` and `Administrators` only, and opens it for writing.
    /// The descriptor is applied by `CreateFileW` itself, so there is no moment at which the file
    /// is readable by anyone else. Fails (and creates nothing) if the descriptor cannot be built.
    pub fn create_private_new(path: &Path) -> io::Result<File> {
        let name = wide(path)?;
        let sd = private_descriptor(false)?;
        let sa = attributes(&sd);
        // SAFETY: `name` is NUL-terminated and `sa` points at a live descriptor.
        let handle = unsafe {
            CreateFileW(
                name.as_ptr(),
                FILE_GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                &sa,
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL,
                null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a valid handle that this call owns; `File` closes it.
        Ok(unsafe { File::from_raw_handle(handle) })
    }

    /// Creates `dir` (and missing ancestors) like `create_dir_all`; when `dir` itself is new it
    /// gets the protected DACL of [`create_private_new`], inherited by what is created inside it.
    /// An existing directory is left as it is.
    pub fn create_private_dir_all(dir: &Path) -> io::Result<()> {
        if dir.is_dir() {
            return Ok(());
        }
        if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let name = wide(dir)?;
        let sd = private_descriptor(true)?;
        let sa = attributes(&sd);
        // SAFETY: `name` is NUL-terminated and `sa` points at a live descriptor.
        if unsafe { CreateDirectoryW(name.as_ptr(), &sa) } == 0 {
            let err = io::Error::last_os_error();
            // Lost a race with another creator: theirs stands.
            if err.raw_os_error() == Some(ERROR_ALREADY_EXISTS as i32) && dir.is_dir() {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }

    /// Like [`create_private_dir_all`], returning whether `dir` was newly created by this call or
    /// already existed (including when another creator won the `ERROR_ALREADY_EXISTS` race). Uses
    /// `symlink_metadata` rather than `is_dir` so a dangling junction, symbolic link, or regular
    /// file at `dir` returns [`PrivateDirOutcome::Existing`] for the caller to inspect and refuse.
    pub fn create_or_open_private_dir_all(dir: &Path) -> io::Result<PrivateDirOutcome> {
        create_or_open_private_dir_all_with_hooks(dir, |_| {}, |_| {})
    }

    /// [`create_or_open_private_dir_all`] with pre- and post-`CreateDirectoryW` hooks so unit tests
    /// can deterministically exercise creation races (`ERROR_ALREADY_EXISTS`) and filesystems that
    /// drop the security descriptor.
    pub fn create_or_open_private_dir_all_with_hooks(
        dir: &Path,
        before_create: impl FnOnce(&Path),
        after_create: impl FnOnce(&Path),
    ) -> io::Result<PrivateDirOutcome> {
        if std::fs::symlink_metadata(dir).is_ok() {
            return Ok(PrivateDirOutcome::Existing);
        }
        if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let name = wide(dir)?;
        let sd = private_descriptor(true)?;
        let sa = attributes(&sd);
        before_create(dir);
        // SAFETY: `name` is NUL-terminated and `sa` points at a live descriptor.
        if unsafe { CreateDirectoryW(name.as_ptr(), &sa) } == 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_ALREADY_EXISTS as i32)
                && std::fs::symlink_metadata(dir).is_ok()
            {
                return Ok(PrivateDirOutcome::Existing);
            }
            return Err(err);
        }
        after_create(dir);
        Ok(PrivateDirOutcome::Created)
    }

    /// Writes `data` to `path`, replacing an existing file, through a private temporary file in
    /// the same directory (so the result always carries the protected DACL).
    pub fn write_private_replace(path: &Path, data: &[u8]) -> io::Result<()> {
        use std::io::Write;
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "private".to_string());
        let tmp = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let result = create_private_new(&tmp).and_then(|mut f| {
            f.write_all(data)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, path)
        });
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        result
    }

    fn sid_from_ace_bytes(ace_bytes: &[u8], offset: usize) -> io::Result<(String, usize)> {
        if ace_bytes.len() < offset.saturating_add(8) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ACE truncated before SID header",
            ));
        }
        let sub_count = usize::from(ace_bytes[offset + 1]);
        if sub_count > SID_MAX_SUB_AUTHORITIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ACE SID exceeds maximum subauthority count",
            ));
        }
        let sid_len = 8usize + sub_count * 4;
        if ace_bytes.len() < offset.saturating_add(sid_len) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ACE truncated inside SID",
            ));
        }
        // Copy into an 8-byte-aligned stack buffer (maximum valid SID is 68 bytes <= 72 bytes).
        let mut aligned = [0u64; 9];
        // SAFETY: `aligned` is 72 bytes and `sid_len <= 68`.
        unsafe {
            std::ptr::copy_nonoverlapping(
                ace_bytes.as_ptr().add(offset),
                aligned.as_mut_ptr().cast::<u8>(),
                sid_len,
            );
        }
        let sid_ptr: PSID = aligned.as_mut_ptr().cast();
        // SAFETY: `sid_ptr` points to `sid_len` initialized bytes in `aligned`.
        if unsafe { IsValidSid(sid_ptr) } == 0
            || unsafe { GetLengthSid(sid_ptr) } as usize != sid_len
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ACE contains an invalid SID",
            ));
        }
        Ok((sid_to_string(sid_ptr)?, sid_len))
    }

    /// Extracts `(sid, mask)` grants from one raw ACE slice across all Windows allow ACE types:
    /// - `ACCESS_ALLOWED_ACE_TYPE` (`0x00`) and `ACCESS_ALLOWED_CALLBACK_ACE_TYPE` (`0x09`)
    /// - `ACCESS_ALLOWED_COMPOUND_ACE_TYPE` (`0x04`, both server and client SIDs)
    /// - `ACCESS_ALLOWED_OBJECT_ACE_TYPE` (`0x05`) and
    ///   `ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE` (`0x0B`, skipping optional `ObjectType` and
    ///   `InheritedObjectType` GUIDs)
    ///
    /// Conditional callback expressions and object GUID filters are treated conservatively as
    /// allowing `Mask` to the trustee, so conditional or object-scoped grants are never missed.
    pub(super) fn parse_ace_bytes(ace_bytes: &[u8]) -> io::Result<Vec<DaclAce>> {
        if ace_bytes.len() < std::mem::size_of::<ACE_HEADER>() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ACE smaller than ACE_HEADER",
            ));
        }
        let ace_type = ace_bytes[0];
        match ace_type {
            ACCESS_ALLOWED_ACE_TYPE | ACCESS_ALLOWED_CALLBACK_ACE_TYPE => {
                if ace_bytes.len() < 8 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "allow ACE truncated before SidStart",
                    ));
                }
                let mask = u32::from_le_bytes(ace_bytes[4..8].try_into().unwrap());
                let (sid, _) = sid_from_ace_bytes(ace_bytes, 8)?;
                Ok(vec![DaclAce { sid, mask }])
            }
            ACCESS_ALLOWED_COMPOUND_ACE_TYPE => {
                if ace_bytes.len() < 12 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "compound allow ACE truncated before SidStart",
                    ));
                }
                let mask = u32::from_le_bytes(ace_bytes[4..8].try_into().unwrap());
                let (server_sid, server_len) = sid_from_ace_bytes(ace_bytes, 12)?;
                let mut out = vec![DaclAce {
                    sid: server_sid,
                    mask,
                }];
                let client_offset = 12 + server_len;
                if client_offset < ace_bytes.len() {
                    let (client_sid, _) = sid_from_ace_bytes(ace_bytes, client_offset)?;
                    out.push(DaclAce {
                        sid: client_sid,
                        mask,
                    });
                }
                Ok(out)
            }
            ACCESS_ALLOWED_OBJECT_ACE_TYPE | ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE => {
                if ace_bytes.len() < 12 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "object allow ACE truncated before Flags",
                    ));
                }
                let mask = u32::from_le_bytes(ace_bytes[4..8].try_into().unwrap());
                let flags = u32::from_le_bytes(ace_bytes[8..12].try_into().unwrap());
                let mut sid_offset = 12usize;
                if flags & ACE_OBJECT_TYPE_PRESENT != 0 {
                    sid_offset += GUID_BYTES;
                }
                if flags & ACE_INHERITED_OBJECT_TYPE_PRESENT != 0 {
                    sid_offset += GUID_BYTES;
                }
                let (sid, _) = sid_from_ace_bytes(ace_bytes, sid_offset)?;
                Ok(vec![DaclAce { sid, mask }])
            }
            _ => Ok(Vec::new()),
        }
    }

    pub(super) fn parse_dacl_aces(dacl: *const ACL) -> io::Result<Vec<DaclAce>> {
        if dacl.is_null() {
            return Ok(vec![DaclAce {
                sid: super::SID_EVERYONE.to_string(),
                mask: FULL_ACCESS,
            }]);
        }
        let mut info = ACL_SIZE_INFORMATION {
            AceCount: 0,
            AclBytesInUse: 0,
            AclBytesFree: 0,
        };
        // SAFETY: `info` is large enough for `AclSizeInformation`.
        if unsafe {
            GetAclInformation(
                dacl,
                (&mut info as *mut ACL_SIZE_INFORMATION).cast(),
                std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                AclSizeInformation,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut aces = Vec::new();
        for i in 0..info.AceCount {
            let mut ace: *mut c_void = null_mut();
            // SAFETY: `i` is below the ACE count of `dacl`.
            if unsafe { GetAce(dacl, i, &mut ace) } == 0 || ace.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: every ACE starts with an `ACE_HEADER`.
            let header = unsafe { *(ace as *const ACE_HEADER) };
            let ace_size = usize::from(header.AceSize);
            if ace_size < std::mem::size_of::<ACE_HEADER>()
                || ace_size > info.AclBytesInUse as usize
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid ACE size in DACL",
                ));
            }
            // SAFETY: `GetAce` returned a valid ACE within `dacl` of `header.AceSize` bytes.
            let ace_bytes = unsafe { std::slice::from_raw_parts(ace as *const u8, ace_size) };
            aces.extend(parse_ace_bytes(ace_bytes)?);
        }
        Ok(aces)
    }

    fn parse_dacl_from_sd(
        sd: PSECURITY_DESCRIPTOR,
        dacl: *const ACL,
    ) -> io::Result<(Vec<DaclAce>, bool)> {
        let mut control = 0u16;
        let mut revision = 0u32;
        // SAFETY: `sd` is a valid security descriptor returned by Win32.
        if unsafe { GetSecurityDescriptorControl(sd, &mut control, &mut revision) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let protected = control & SE_DACL_PROTECTED != 0;
        let aces = parse_dacl_aces(dacl)?;
        Ok((aces, protected))
    }

    fn read_dacl_handle(handle: HANDLE) -> io::Result<(Vec<DaclAce>, bool)> {
        let mut dacl: *mut ACL = null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = null_mut();
        // SAFETY: `handle` is an open handle with `READ_CONTROL`; `sd` is freed below.
        let status = unsafe {
            GetSecurityInfo(
                handle,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                &mut dacl,
                null_mut(),
                &mut sd,
            )
        };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        let _guard = LocalBuf(sd);
        parse_dacl_from_sd(sd, dacl)
    }

    /// Reads the DACL of `path`: its allow entries (across all Windows allow ACE types) and
    /// whether it is protected from inheritance. A NULL DACL (full access for everyone) is
    /// reported as an `Everyone` entry.
    pub fn read_dacl(path: &Path) -> io::Result<(Vec<DaclAce>, bool)> {
        let name = wide(path)?;
        let mut dacl: *mut ACL = null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = null_mut();
        // SAFETY: `name` is NUL-terminated; the out pointers are valid; `sd` is freed below.
        let status = unsafe {
            GetNamedSecurityInfoW(
                name.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                &mut dacl,
                null_mut(),
                &mut sd,
            )
        };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        let _guard = LocalBuf(sd);
        parse_dacl_from_sd(sd, dacl)
    }

    /// Inspects `path` by handle with `FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT`,
    /// never following junctions or symbolic links.
    pub fn inspect_dir_security_no_follow(path: &Path) -> io::Result<DirSecuritySnapshot> {
        let name = wide(path)?;
        let share = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
        let flags = FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT;

        // Open with desired access 0 first so attributes and volume capabilities are readable even
        // when the entry's DACL denies `READ_CONTROL`.
        // SAFETY: `name` is NUL-terminated.
        let raw_meta = unsafe {
            CreateFileW(
                name.as_ptr(),
                0,
                share,
                null_mut(),
                OPEN_EXISTING,
                flags,
                null_mut(),
            )
        };
        if raw_meta == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let meta_handle = OwnedHandle(raw_meta);

        // SAFETY: `BY_HANDLE_FILE_INFORMATION` is plain integer fields.
        let mut file_info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        // SAFETY: `meta_handle` is a valid open handle and `file_info` is writable.
        if unsafe { GetFileInformationByHandle(meta_handle.0, &mut file_info) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let is_reparse_point = (file_info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0;
        let is_dir = (file_info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;

        let mut fs_flags = 0u32;
        // SAFETY: `meta_handle` is valid; optional name buffers are null with length 0.
        let vol_ok = unsafe {
            GetVolumeInformationByHandleW(
                meta_handle.0,
                null_mut(),
                0,
                null_mut(),
                null_mut(),
                &mut fs_flags,
                null_mut(),
                0,
            )
        };
        let persistent_acls = vol_ok != 0 && (fs_flags & FILE_PERSISTENT_ACLS) != 0;

        // Open with `READ_CONTROL` (still no-follow) to read owner and DACL on the same target.
        // SAFETY: `name` is NUL-terminated.
        let raw_sec = unsafe {
            CreateFileW(
                name.as_ptr(),
                READ_CONTROL,
                share,
                null_mut(),
                OPEN_EXISTING,
                flags,
                null_mut(),
            )
        };
        if raw_sec == INVALID_HANDLE_VALUE {
            let err_owner = io::Error::last_os_error();
            let err_dacl = io::Error::from_raw_os_error(err_owner.raw_os_error().unwrap_or(5));
            return Ok(DirSecuritySnapshot {
                is_reparse_point,
                is_dir,
                persistent_acls,
                owner_sid: Err(err_owner),
                dacl: Err(err_dacl),
            });
        }
        let sec_handle = OwnedHandle(raw_sec);

        let owner_sid = (|| {
            let mut owner: PSID = null_mut();
            let mut sd: PSECURITY_DESCRIPTOR = null_mut();
            // SAFETY: `sec_handle` has `READ_CONTROL`; `sd` is freed below.
            let status = unsafe {
                GetSecurityInfo(
                    sec_handle.0,
                    SE_FILE_OBJECT,
                    OWNER_SECURITY_INFORMATION,
                    &mut owner,
                    null_mut(),
                    null_mut(),
                    null_mut(),
                    &mut sd,
                )
            };
            if status != 0 {
                return Err(io::Error::from_raw_os_error(status as i32));
            }
            let _guard = LocalBuf(sd);
            if owner.is_null() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "security descriptor has no owner SID",
                ));
            }
            sid_to_string(owner)
        })();

        let dacl = read_dacl_handle(sec_handle.0);

        Ok(DirSecuritySnapshot {
            is_reparse_point,
            is_dir,
            persistent_acls,
            owner_sid,
            dacl,
        })
    }

    /// Logs a warning (never an error) when the DACL of the existing file `path` grants access to
    /// an account other than the current user, `SYSTEM` and `Administrators`. `what` names the
    /// file in the message ("cluster secret file").
    pub fn warn_if_readable_by_others(path: &Path, what: &str) {
        let checked = current_user_sid().and_then(|user| {
            let (aces, _) = read_dacl(path)?;
            Ok(open_grants(&aces, &user))
        });
        match checked {
            Ok(offenders) => {
                let user = std::env::var("USERNAME").unwrap_or_else(|_| "%USERNAME%".to_string());
                if let Some(msg) = open_acl_warning(path, what, &offenders, &user) {
                    tracing::warn!("{msg}");
                }
            }
            Err(e) => tracing::debug!(
                path = %path.display(),
                error = %e,
                "could not read the DACL of a private file"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ace(sid: &str, mask: u32) -> DaclAce {
        DaclAce {
            sid: sid.to_string(),
            mask,
        }
    }

    const ME: &str = "S-1-5-21-1-2-3-1001";
    const OTHER_LOCAL_USER: &str = "S-1-5-21-1-2-3-1002";
    const DOMAIN_USERS: &str = "S-1-5-21-1-2-3-513";
    const TOOLCHAIN_CAPABILITY: &str = "S-1-15-3-1024-111-222-333-444-555-666-777-888";

    #[test]
    fn sddl_is_protected_and_has_three_entries() {
        let file = private_sddl(ME, false);
        assert_eq!(file, format!("D:P(A;;FA;;;{ME})(A;;FA;;;SY)(A;;FA;;;BA)"));
        let dir = private_sddl(ME, true);
        assert!(dir.starts_with("D:P(A;OICI;FA;;;"), "{dir}");
        assert_eq!(dir.matches("OICI").count(), 3);
    }

    #[test]
    fn trusted_owners_are_current_user_system_and_administrators_only() {
        assert!(is_trusted_owner_sid(ME, ME));
        assert!(is_trusted_owner_sid("s-1-5-21-1-2-3-1001", ME));
        assert!(is_trusted_owner_sid(SID_SYSTEM, ME));
        assert!(is_trusted_owner_sid(SID_ADMINISTRATORS, ME));
        assert!(!is_trusted_owner_sid(OTHER_LOCAL_USER, ME));
        assert!(!is_trusted_owner_sid(SID_USERS, ME));
        assert!(!is_trusted_owner_sid(SID_CREATOR_OWNER, ME));
    }

    #[test]
    fn private_dacl_has_no_open_grants() {
        let aces = [
            ace(ME, 0x001F_01FF),
            ace(SID_SYSTEM, 0x001F_01FF),
            ace(SID_ADMINISTRATORS, 0x001F_01FF),
        ];
        assert!(open_grants(&aces, ME).is_empty());
        assert!(dir_open_grants(&aces, ME).is_empty());
    }

    #[test]
    fn everyone_and_users_read_are_flagged_once() {
        let aces = [
            ace(ME, 0x001F_01FF),
            ace(SID_USERS, 0x0012_00A9),
            ace(SID_EVERYONE, 0x0012_00A9),
            ace(SID_EVERYONE, 0x0000_0001),
            ace(SID_AUTHENTICATED_USERS, 0x8000_0000),
        ];
        assert_eq!(
            open_grants(&aces, ME),
            vec![SID_USERS, SID_EVERYONE, SID_AUTHENTICATED_USERS]
        );
    }

    #[test]
    fn dir_open_grants_catches_any_untrusted_sid_and_allows_creator_owner_owner_rights_and_traverse(
    ) {
        // Allowed on a private base / workspace root:
        // - current user, SYSTEM, Administrators
        // - CREATOR OWNER (S-1-3-0) and OWNER RIGHTS (S-1-3-4)
        // - FEAT-3d `oxideswarm.toolchain` traverse-only ACE (`0x0010_00A0`)
        let allowed_aces = [
            ace(ME, 0x001F_01FF),
            ace(SID_SYSTEM, 0x001F_01FF),
            ace(SID_ADMINISTRATORS, 0x001F_01FF),
            ace(SID_CREATOR_OWNER, 0x1000_0000),
            ace(SID_OWNER_RIGHTS, 0x0002_0000 | 0x0004_0000),
            ace(TOOLCHAIN_CAPABILITY, 0x0010_00A0),
        ];
        assert!(
            dir_open_grants(&allowed_aces, ME).is_empty(),
            "trusted owners, CREATOR OWNER, OWNER RIGHTS and traverse-only isolation ACE must pass"
        );

        // Flagged when granted sensitive directory access:
        let untrusted = [
            ace(OTHER_LOCAL_USER, 0x0012_00A9),    // another local user (RX)
            ace(DOMAIN_USERS, 0x0012_00A9),        // Domain Users (RX)
            ace(SID_NETWORK, 0x0012_00A9),         // NETWORK
            ace(SID_ANONYMOUS_LOGON, 0x0000_0001), // ANONYMOUS LOGON (list dir)
            ace(SID_REMOTE_DESKTOP_USERS, 0x0013_01BF), // Remote Desktop Users (Modify)
            ace(SID_ALL_APP_PACKAGES, 0x0012_00A9), // ALL APPLICATION PACKAGES (RX)
            ace(SID_USERS, 0x0000_0040),           // FILE_DELETE_CHILD only
            ace(SID_GUESTS, 0x0001_0000),          // DELETE only
            ace(OTHER_LOCAL_USER, 0x001F_01FF),    // duplicate SID appears once
        ];
        assert_eq!(
            dir_open_grants(&untrusted, ME),
            vec![
                OTHER_LOCAL_USER.to_string(),
                DOMAIN_USERS.to_string(),
                SID_NETWORK.to_string(),
                SID_ANONYMOUS_LOGON.to_string(),
                SID_REMOTE_DESKTOP_USERS.to_string(),
                SID_ALL_APP_PACKAGES.to_string(),
                SID_USERS.to_string(),
                SID_GUESTS.to_string(),
            ]
        );
        assert_eq!(sid_label(SID_INTERACTIVE), "INTERACTIVE");
        assert_eq!(sid_label(SID_GUESTS), "BUILTIN\\Guests");
        assert_eq!(sid_label(SID_NETWORK), "NETWORK");
        assert_eq!(sid_label(SID_ANONYMOUS_LOGON), "ANONYMOUS LOGON");
        assert_eq!(
            sid_label(SID_REMOTE_DESKTOP_USERS),
            "BUILTIN\\Remote Desktop Users"
        );
        assert_eq!(sid_label(SID_ALL_APP_PACKAGES), "ALL APPLICATION PACKAGES");
        assert_eq!(
            sid_label(DOMAIN_USERS),
            format!("Domain Users ({DOMAIN_USERS})")
        );
    }

    #[test]
    fn entries_without_data_access_are_ignored() {
        // READ_CONTROL | SYNCHRONIZE | FILE_READ_ATTRIBUTES only.
        let aces = [ace(SID_EVERYONE, 0x0010_0000 | 0x0002_0000 | 0x80)];
        assert!(open_grants(&aces, ME).is_empty());
        assert!(dir_open_grants(&aces, ME).is_empty());
    }

    #[test]
    fn current_user_sid_match_ignores_case() {
        let aces = [ace("s-1-5-21-1-2-3-1001", 0x001F_01FF)];
        assert!(open_grants(&aces, ME).is_empty());
        assert!(dir_open_grants(&aces, ME).is_empty());
    }

    #[test]
    fn warning_names_the_accounts_and_the_icacls_fix() {
        let path = Path::new(r"C:\Users\me\.oxideswarm\cluster.secret");
        assert!(open_acl_warning(path, "cluster secret file", &[], "me").is_none());
        let msg = open_acl_warning(
            path,
            "cluster secret file",
            &[SID_EVERYONE.to_string(), SID_USERS.to_string()],
            "me",
        )
        .unwrap();
        assert!(msg.contains("Everyone, BUILTIN\\Users"), "{msg}");
        assert!(
            msg.contains(
                r#"icacls "C:\Users\me\.oxideswarm\cluster.secret" /inheritance:r /grant:r "me:F" SYSTEM:F Administrators:F"#
            ),
            "{msg}"
        );
    }

    #[test]
    fn dir_warning_uses_unlocalized_sids_for_icacls() {
        let path = Path::new(r"E:\OxideSwarm\work");
        assert!(open_dir_acl_warning(path, &[], ME).is_none());
        let msg = open_dir_acl_warning(
            path,
            &[
                SID_AUTHENTICATED_USERS.to_string(),
                OTHER_LOCAL_USER.to_string(),
            ],
            ME,
        )
        .unwrap();
        assert!(
            msg.contains("Authenticated Users, S-1-5-21-1-2-3-1002"),
            "{msg}"
        );
        assert!(
            msg.contains(&format!(
                r#"icacls "E:\OxideSwarm\work" /inheritance:r /grant:r "*{ME}:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" "*S-1-5-32-544:(OI)(CI)F""#
            )),
            "{msg}"
        );
        assert!(
            !msg.contains("\"SYSTEM:") && !msg.contains("\"Administrators:"),
            "icacls hint must use *S-1-5-18 and *S-1-5-32-544 rather than localized names: {msg}"
        );
    }
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::io::Write;
    use std::process::Command;

    /// A temp directory whose DACL grants `Everyone` read with inheritance, like a home folder.
    fn open_parent() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let status = Command::new("icacls")
            .arg(dir.path())
            .args(["/grant", "*S-1-1-0:(OI)(CI)R", "*S-1-5-32-545:(OI)(CI)R"])
            .status()
            .expect("icacls");
        assert!(status.success());
        dir
    }

    fn allowed_set(user: &str) -> BTreeSet<String> {
        [user, SID_SYSTEM, SID_ADMINISTRATORS]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// Encodes a `S-1-<auth>-<sub0>-...` SID into its binary WinNT representation.
    fn encode_sid_bytes(authority: u8, subs: &[u32]) -> Vec<u8> {
        let mut out = vec![1u8, subs.len() as u8, 0, 0, 0, 0, 0, authority];
        for &sub in subs {
            out.extend_from_slice(&sub.to_le_bytes());
        }
        out
    }

    #[test]
    fn plain_file_in_an_open_parent_is_flagged() {
        let parent = open_parent();
        let path = parent.path().join("plain.txt");
        std::fs::write(&path, b"x").unwrap();
        let (aces, protected) = read_dacl(&path).unwrap();
        assert!(!protected);
        let user = current_user_sid().unwrap();
        let open = open_grants(&aces, &user);
        assert!(open.iter().any(|s| s == SID_EVERYONE), "{open:?}");
    }

    #[test]
    fn private_file_has_a_protected_three_entry_dacl() {
        let parent = open_parent();
        let path = parent.path().join("cluster.secret");
        let mut f = create_private_new(&path).unwrap();
        f.write_all(b"secret\n").unwrap();
        drop(f);

        let user = current_user_sid().unwrap();
        let (aces, protected) = read_dacl(&path).unwrap();
        assert!(protected, "the DACL must not inherit");
        let sids: BTreeSet<String> = aces.iter().map(|a| a.sid.clone()).collect();
        assert_eq!(sids, allowed_set(&user), "{aces:?}");
        assert_eq!(aces.len(), 3, "{aces:?}");
        assert!(!sids.contains(SID_EVERYONE) && !sids.contains(SID_USERS));
        assert!(open_grants(&aces, &user).is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), b"secret\n");
    }

    #[test]
    fn private_new_refuses_an_existing_file() {
        let parent = open_parent();
        let path = parent.path().join("exists");
        std::fs::write(&path, b"x").unwrap();
        let err = create_private_new(&path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&path).unwrap(), b"x");
    }

    #[test]
    fn private_dir_is_protected_and_its_files_inherit_only_the_three() {
        let parent = open_parent();
        let dir = parent.path().join("a").join(".oxideswarm");
        create_private_dir_all(&dir).unwrap();
        create_private_dir_all(&dir).unwrap();
        let user = current_user_sid().unwrap();
        let (aces, protected) = read_dacl(&dir).unwrap();
        assert!(protected);
        let sids: BTreeSet<String> = aces.iter().map(|a| a.sid.clone()).collect();
        assert_eq!(sids, allowed_set(&user), "{aces:?}");

        let inner = dir.join("inherits.txt");
        std::fs::write(&inner, b"x").unwrap();
        let (inner_aces, _) = read_dacl(&inner).unwrap();
        assert!(open_grants(&inner_aces, &user).is_empty(), "{inner_aces:?}");
    }

    #[test]
    fn write_private_replace_replaces_and_stays_private() {
        let parent = open_parent();
        let path = parent.path().join("ticket.txt");
        write_private_replace(&path, b"one").unwrap();
        write_private_replace(&path, b"two").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        let user = current_user_sid().unwrap();
        let (aces, protected) = read_dacl(&path).unwrap();
        assert!(protected);
        assert!(open_grants(&aces, &user).is_empty());
    }

    #[test]
    fn parse_dacl_aces_extracts_all_five_windows_allow_ace_types() {
        use windows_sys::Win32::Security::{AddAce, InitializeAcl, ACL};

        let sid_everyone = encode_sid_bytes(1, &[0]); // S-1-1-0
        let sid_network = encode_sid_bytes(5, &[2]); // S-1-5-2
        let sid_anon = encode_sid_bytes(5, &[7]); // S-1-5-7
        let sid_system = encode_sid_bytes(5, &[18]); // S-1-5-18
        let sid_rdp = encode_sid_bytes(5, &[32, 555]); // S-1-5-32-555
        let sid_app_pkg = encode_sid_bytes(15, &[2, 1]); // S-1-15-2-1

        let make_header = |ace_type: u8, total_len: usize| -> [u8; 4] {
            let size = (total_len as u16).to_le_bytes();
            [ace_type, 0, size[0], size[1]]
        };

        // 1. ACCESS_ALLOWED_ACE_TYPE (0x00) for S-1-1-0
        let mut ace_std = Vec::new();
        ace_std.extend_from_slice(&make_header(0x00, 8 + sid_everyone.len()));
        ace_std.extend_from_slice(&0x0012_00A9u32.to_le_bytes());
        ace_std.extend_from_slice(&sid_everyone);

        // 2. ACCESS_ALLOWED_CALLBACK_ACE_TYPE (0x09) for S-1-15-2-1 + 8 bytes condition data
        let mut ace_cb = Vec::new();
        ace_cb.extend_from_slice(&make_header(0x09, 8 + sid_app_pkg.len() + 8));
        ace_cb.extend_from_slice(&0x0012_0089u32.to_le_bytes());
        ace_cb.extend_from_slice(&sid_app_pkg);
        ace_cb.extend_from_slice(&[0xAA; 8]);

        // 3. ACCESS_ALLOWED_COMPOUND_ACE_TYPE (0x04): server S-1-5-18 impersonating client S-1-5-2
        let mut ace_comp = Vec::new();
        ace_comp.extend_from_slice(&make_header(
            0x04,
            12 + sid_system.len() + sid_network.len(),
        ));
        ace_comp.extend_from_slice(&0x0013_01BFu32.to_le_bytes());
        ace_comp.extend_from_slice(&1u16.to_le_bytes()); // COMPOUND_ACE_IMPERSONATION
        ace_comp.extend_from_slice(&0u16.to_le_bytes()); // Reserved
        ace_comp.extend_from_slice(&sid_system);
        ace_comp.extend_from_slice(&sid_network);

        // 4. ACCESS_ALLOWED_OBJECT_ACE_TYPE (0x05) with 1 GUID (Flags = 0x1) for S-1-5-7
        let mut ace_obj1 = Vec::new();
        ace_obj1.extend_from_slice(&make_header(0x05, 12 + 16 + sid_anon.len()));
        ace_obj1.extend_from_slice(&0x0000_0001u32.to_le_bytes());
        ace_obj1.extend_from_slice(&0x0000_0001u32.to_le_bytes()); // ACE_OBJECT_TYPE_PRESENT
        ace_obj1.extend_from_slice(&[0x11; 16]);
        ace_obj1.extend_from_slice(&sid_anon);

        // 5. ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE (0x0B) with 2 GUIDs (Flags = 0x3) + callback
        let mut ace_cb_obj2 = Vec::new();
        ace_cb_obj2.extend_from_slice(&make_header(0x0B, 12 + 32 + sid_rdp.len() + 4));
        ace_cb_obj2.extend_from_slice(&0x1000_0000u32.to_le_bytes());
        ace_cb_obj2.extend_from_slice(&0x0000_0003u32.to_le_bytes()); // both GUIDs present
        ace_cb_obj2.extend_from_slice(&[0x22; 32]);
        ace_cb_obj2.extend_from_slice(&sid_rdp);
        ace_cb_obj2.extend_from_slice(&[0xBB; 4]);

        // 6. ACCESS_DENIED_ACE_TYPE (0x01) must be ignored by `parse_dacl_aces`.
        let mut ace_deny = Vec::new();
        ace_deny.extend_from_slice(&make_header(0x01, 8 + sid_everyone.len()));
        ace_deny.extend_from_slice(&0x001F_01FFu32.to_le_bytes());
        ace_deny.extend_from_slice(&sid_everyone);

        let mut all_raw = Vec::new();
        for a in [
            &ace_std,
            &ace_cb,
            &ace_comp,
            &ace_obj1,
            &ace_cb_obj2,
            &ace_deny,
        ] {
            all_raw.extend_from_slice(a);
        }

        // Build a real Win32 ACL (ACL_REVISION_DS = 4) and parse it via GetAclInformation/GetAce.
        let mut acl_buf = vec![0u32; 256];
        let acl_ptr = acl_buf.as_mut_ptr().cast::<ACL>();
        let acl_bytes = (acl_buf.len() * 4) as u32;
        assert_ne!(unsafe { InitializeAcl(acl_ptr, acl_bytes, 4) }, 0);
        assert_ne!(
            unsafe {
                AddAce(
                    acl_ptr,
                    4,
                    u32::MAX,
                    all_raw.as_ptr().cast(),
                    all_raw.len() as u32,
                )
            },
            0
        );

        let parsed = win::parse_dacl_aces(acl_ptr).unwrap();
        assert_eq!(
            parsed,
            vec![
                DaclAce {
                    sid: SID_EVERYONE.to_string(),
                    mask: 0x0012_00A9
                },
                DaclAce {
                    sid: SID_ALL_APP_PACKAGES.to_string(),
                    mask: 0x0012_0089
                },
                DaclAce {
                    sid: SID_SYSTEM.to_string(),
                    mask: 0x0013_01BF
                },
                DaclAce {
                    sid: SID_NETWORK.to_string(),
                    mask: 0x0013_01BF
                },
                DaclAce {
                    sid: SID_ANONYMOUS_LOGON.to_string(),
                    mask: 0x0000_0001
                },
                DaclAce {
                    sid: SID_REMOTE_DESKTOP_USERS.to_string(),
                    mask: 0x1000_0000
                },
            ]
        );
        let offenders = dir_open_grants(&parsed, "S-1-5-21-1-2-3-1001");
        assert_eq!(
            offenders,
            vec![
                SID_EVERYONE.to_string(),
                SID_ALL_APP_PACKAGES.to_string(),
                SID_NETWORK.to_string(),
                SID_ANONYMOUS_LOGON.to_string(),
                SID_REMOTE_DESKTOP_USERS.to_string(),
            ]
        );

        // Truncated allow ACE is rejected rather than reading out of bounds.
        assert!(win::parse_ace_bytes(&ace_obj1[..18]).is_err());
    }

    #[test]
    fn icacls_sid_remedy_makes_an_open_dir_private() {
        let parent = open_parent();
        let dir = parent.path().join("inherited_open");
        std::fs::create_dir(&dir).unwrap();
        let user = current_user_sid().unwrap();
        let (before_aces, before_prot) = read_dacl(&dir).unwrap();
        assert!(!before_prot);
        assert!(!dir_open_grants(&before_aces, &user).is_empty());

        let status = Command::new("icacls")
            .arg(&dir)
            .args([
                "/inheritance:r",
                "/grant:r",
                &format!("*{user}:(OI)(CI)F"),
                "*S-1-5-18:(OI)(CI)F",
                "*S-1-5-32-544:(OI)(CI)F",
            ])
            .status()
            .expect("icacls");
        assert!(status.success());

        let (after_aces, after_prot) = read_dacl(&dir).unwrap();
        assert!(after_prot);
        assert!(
            dir_open_grants(&after_aces, &user).is_empty(),
            "{after_aces:?}"
        );
    }
}
