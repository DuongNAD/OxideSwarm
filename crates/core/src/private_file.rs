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
/// `Everyone`.
pub const SID_EVERYONE: &str = "S-1-1-0";
/// `BUILTIN\Users`.
pub const SID_USERS: &str = "S-1-5-32-545";
/// `NT AUTHORITY\Authenticated Users`.
pub const SID_AUTHENTICATED_USERS: &str = "S-1-5-11";

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

/// One `ACCESS_ALLOWED` entry of a DACL.
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

/// The SIDs in `aces` that are granted access but are not `current_user_sid`, `SYSTEM` or
/// `BUILTIN\Administrators`. Deny entries are not passed in, and a SID appears once.
pub fn open_grants(aces: &[DaclAce], current_user_sid: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for ace in aces {
        if ace.mask & SENSITIVE_MASK == 0 {
            continue;
        }
        let allowed = ace.sid.eq_ignore_ascii_case(current_user_sid)
            || ace.sid == SID_SYSTEM
            || ace.sid == SID_ADMINISTRATORS;
        if !allowed && !out.contains(&ace.sid) {
            out.push(ace.sid.clone());
        }
    }
    out
}

/// A readable name for the well-known SIDs, the SID itself otherwise.
pub fn sid_label(sid: &str) -> String {
    match sid {
        SID_EVERYONE => "Everyone".to_string(),
        SID_USERS => "BUILTIN\\Users".to_string(),
        SID_AUTHENTICATED_USERS => "Authenticated Users".to_string(),
        other => other.to_string(),
    }
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

#[cfg(windows)]
pub use win::{
    create_private_dir_all, create_private_new, current_user_sid, read_dacl,
    warn_if_readable_by_others, write_private_replace,
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
        GetNamedSecurityInfoW, SDDL_REVISION_1, SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::{
        AclSizeInformation, GetAce, GetAclInformation, GetSecurityDescriptorControl,
        GetTokenInformation, TokenUser, ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION,
        DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES,
        SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateDirectoryW, CreateFileW, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_WRITE,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
    /// `Everyone`: what a NULL DACL (no protection at all) amounts to.
    const FULL_ACCESS: u32 = 0x001F_01FF;

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

    /// Reads the DACL of `path`: its `ACCESS_ALLOWED` entries and whether it is protected from
    /// inheritance. A NULL DACL (full access for everyone) is reported as an `Everyone` entry.
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

        let mut control = 0u16;
        let mut revision = 0u32;
        // SAFETY: `sd` is the descriptor returned above.
        if unsafe { GetSecurityDescriptorControl(sd, &mut control, &mut revision) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let protected = control & SE_DACL_PROTECTED != 0;

        if dacl.is_null() {
            return Ok((
                vec![DaclAce {
                    sid: super::SID_EVERYONE.to_string(),
                    mask: FULL_ACCESS,
                }],
                protected,
            ));
        }
        let mut info = ACL_SIZE_INFORMATION {
            AceCount: 0,
            AclBytesInUse: 0,
            AclBytesFree: 0,
        };
        // SAFETY: `info` is large enough for the requested class.
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
            // SAFETY: every ACE starts with an ACE_HEADER.
            let header = unsafe { *(ace as *const ACE_HEADER) };
            if header.AceType != ACCESS_ALLOWED_ACE_TYPE {
                continue;
            }
            let allowed = ace as *const ACCESS_ALLOWED_ACE;
            // SAFETY: for an allowed ACE the SID starts at `SidStart` and lives as long as `sd`.
            let (mask, sid) = unsafe {
                (
                    (*allowed).Mask,
                    std::ptr::addr_of!((*allowed).SidStart) as *mut c_void,
                )
            };
            aces.push(DaclAce {
                sid: sid_to_string(sid)?,
                mask,
            });
        }
        Ok((aces, protected))
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

    #[test]
    fn sddl_is_protected_and_has_three_entries() {
        let file = private_sddl(ME, false);
        assert_eq!(file, format!("D:P(A;;FA;;;{ME})(A;;FA;;;SY)(A;;FA;;;BA)"));
        let dir = private_sddl(ME, true);
        assert!(dir.starts_with("D:P(A;OICI;FA;;;"), "{dir}");
        assert_eq!(dir.matches("OICI").count(), 3);
    }

    #[test]
    fn private_dacl_has_no_open_grants() {
        let aces = [
            ace(ME, 0x001F_01FF),
            ace(SID_SYSTEM, 0x001F_01FF),
            ace(SID_ADMINISTRATORS, 0x001F_01FF),
        ];
        assert!(open_grants(&aces, ME).is_empty());
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
    fn entries_without_data_access_are_ignored() {
        // READ_CONTROL | SYNCHRONIZE | FILE_READ_ATTRIBUTES only.
        let aces = [ace(SID_EVERYONE, 0x0010_0000 | 0x0002_0000 | 0x80)];
        assert!(open_grants(&aces, ME).is_empty());
    }

    #[test]
    fn current_user_sid_match_ignores_case() {
        let aces = [ace("s-1-5-21-1-2-3-1001", 0x001F_01FF)];
        assert!(open_grants(&aces, ME).is_empty());
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
}
