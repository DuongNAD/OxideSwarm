//! Windows worker toolchain permission setup, workspace/sandbox validation, and ACL revocation.

use super::acl::{
    dacl_already_denies_sid, dacl_already_grants_sid, dacl_already_grants_sid_with_flags,
    explicit_access, get_path_dacl_sddl, merge_entries_on_path, merge_entries_on_path_unprotected,
    record_acl_grant, record_sddl_restore, remove_sid_aces_from_path, restore_path_dacl_sddl,
    set_protected_dacl_on_path, OwnedSid,
};
use super::path::{
    discover_msvc_details, is_reparse_point, lexically_resolve_dots, normalize_win_path,
    path_starts_with_ci, strip_verbatim_prefix,
};
use super::spawn::ensure_process_token_default_dacl_grants_current_user;
use super::PREPARE_LOCK;
use crate::isolation::{acl_grant_manifest_path, HostDirs};
use std::io;
use std::os::windows::fs::MetadataExt;
use std::os::windows::io::AsRawHandle;
use std::path::{Component, Path, PathBuf};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Security::Authorization::{DENY_ACCESS, GRANT_ACCESS, SET_ACCESS};
use windows_sys::Win32::Security::{
    WinBuiltinAdministratorsSid, WinBuiltinAnyPackageSid, WinCapabilityInternetClientSid,
    WinCapabilityPrivateNetworkClientServerSid, WinLocalSystemSid, CONTAINER_INHERIT_ACE,
    NO_INHERITANCE, OBJECT_INHERIT_ACE, PSID,
};
use windows_sys::Win32::Storage::FileSystem::{
    GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_ALL_ACCESS,
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_GENERIC_EXECUTE,
    FILE_GENERIC_READ, FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES, FILE_READ_DATA, FILE_TRAVERSE,
    SYNCHRONIZE,
};

fn grant_rx_non_inherited(path: &Path, sid: PSID, manifest_path: Option<&Path>) -> io::Result<()> {
    if !path.exists() || is_reparse_point(path) {
        return Ok(());
    }
    let rx_mask = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE | SYNCHRONIZE;
    let check_mask = FILE_READ_ATTRIBUTES | FILE_TRAVERSE | FILE_READ_DATA;
    let user_sid = OwnedSid::current_user()?;
    let has_sid = dacl_already_grants_sid(path, sid, check_mask);
    let has_user = dacl_already_grants_sid(path, user_sid.as_psid(), check_mask);
    if has_sid && has_user {
        return Ok(());
    }
    record_acl_grant(manifest_path, path)?;
    let mut entries = Vec::with_capacity(2);
    if !has_sid {
        entries.push(explicit_access(sid, rx_mask, GRANT_ACCESS, NO_INHERITANCE));
    }
    if !has_user {
        entries.push(explicit_access(
            user_sid.as_psid(),
            rx_mask,
            GRANT_ACCESS,
            NO_INHERITANCE,
        ));
    }
    merge_entries_on_path(path, &entries)
}

/// Ensures every existing directory and file under `root` grants `FILE_GENERIC_READ |
/// FILE_GENERIC_EXECUTE | SYNCHRONIZE` (`check_mask = FILE_READ_ATTRIBUTES | FILE_TRAVERSE |
/// FILE_READ_DATA`) to both `sid` and `user_sid`. When `SetNamedSecurityInfoW` on `root` has
/// already propagated `(I)(RX)` to a child, `dacl_already_grants_sid` returns `true` and the
/// child's DACL is left untouched (preserving N6 single-root-ACE inheritance). If any existing
/// child did not inherit `(RX)` (e.g. due to a protected DACL or non-auto-inheriting parent),
/// merges `(RX)` with `UNPROTECTED_DACL_SECURITY_INFORMATION`.
fn ensure_descendants_grant_rx(root: &Path, sid: PSID, user_sid: PSID) -> io::Result<()> {
    let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
    let check_mask = FILE_READ_ATTRIBUTES | FILE_TRAVERSE | FILE_READ_DATA;
    let rx_mask = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE | SYNCHRONIZE;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if is_reparse_point(&p) {
                continue;
            }
            let is_dir = entry.file_type().is_ok_and(|ft| ft.is_dir());
            let c_has_sid = dacl_already_grants_sid(&p, sid, check_mask);
            let c_has_user = dacl_already_grants_sid(&p, user_sid, check_mask);
            if !c_has_sid || !c_has_user {
                let flags = if is_dir { inherit } else { NO_INHERITANCE };
                let mut c_entries = Vec::with_capacity(2);
                if !c_has_sid {
                    c_entries.push(explicit_access(sid, rx_mask, GRANT_ACCESS, flags));
                }
                if !c_has_user {
                    c_entries.push(explicit_access(user_sid, rx_mask, GRANT_ACCESS, flags));
                }
                let _ = merge_entries_on_path_unprotected(&p, &c_entries);
            }
            if is_dir {
                stack.push(p);
            }
        }
    }
    Ok(())
}

/// Sets a single inheritable read+execute ACE (`OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE`)
/// on `path` itself and lets Win32 `SetNamedSecurityInfoW` propagate `(I)(OI)(CI)(RX)` to
/// descendants, then verifies via [`ensure_descendants_grant_rx`] that every existing
/// file and subdirectory under `path` actually received `(RX)` (N6).
fn grant_rx_recursive(path: &Path, sid: PSID, manifest_path: Option<&Path>) -> io::Result<()> {
    if !path.exists() || is_reparse_point(path) {
        return Ok(());
    }
    let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
    let check_mask = FILE_READ_ATTRIBUTES | FILE_TRAVERSE | FILE_READ_DATA;
    let user_sid = OwnedSid::current_user()?;
    let has_sid = dacl_already_grants_sid_with_flags(path, sid, check_mask, inherit);
    let has_user =
        dacl_already_grants_sid_with_flags(path, user_sid.as_psid(), check_mask, inherit);
    if !has_sid || !has_user {
        record_acl_grant(manifest_path, path)?;
        let rx_mask = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE | SYNCHRONIZE;
        let mut entries = Vec::with_capacity(2);
        if !has_sid {
            entries.push(explicit_access(sid, rx_mask, GRANT_ACCESS, inherit));
        }
        if !has_user {
            entries.push(explicit_access(
                user_sid.as_psid(),
                rx_mask,
                GRANT_ACCESS,
                inherit,
            ));
        }
        merge_entries_on_path_unprotected(path, &entries)?;
    }
    if path.is_dir() {
        ensure_descendants_grant_rx(path, sid, user_sid.as_psid())?;
    }
    Ok(())
}

/// Grants `oxideswarm.toolchain` read+execute on the existing subdirectories and config files
/// of `cargo_home` (`bin`, `registry`, `git`, `vendor`, `config.toml`, `config`) while NEVER
/// placing an inheritable allow ACE on `cargo_home` itself and NEVER granting access to
/// `credentials` or `credentials.toml`.
///
/// Why: if `cargo_home` had an inheritable allow ACE and `credentials.toml` were later
/// replaced by atomic rename (`MoveFileExW`), the new file would inherit the allow ACE and
/// lose any per-file deny ACE. With `NO_INHERITANCE` on `cargo_home`, any file created or
/// renamed inside `cargo_home` inherits zero `oxideswarm.toolchain` permissions.
fn prepare_one_cargo_dir(
    cargo: &Path,
    toolchain_sid: PSID,
    manifest_path: Option<&Path>,
) -> io::Result<()> {
    if !cargo.exists() || is_reparse_point(cargo) {
        return Ok(());
    }
    grant_rx_non_inherited(cargo, toolchain_sid, manifest_path)?;
    for file in [
        "config.toml",
        "config",
        ".package-cache",
        ".package-cache-lock",
    ] {
        let p = cargo.join(file);
        if p.exists() && !is_reparse_point(&p) {
            grant_rx_non_inherited(&p, toolchain_sid, manifest_path)?;
        }
    }
    for subdir in ["bin", "git", "vendor", "registry"] {
        let p = cargo.join(subdir);
        if p.exists() && !is_reparse_point(&p) {
            grant_rx_recursive(&p, toolchain_sid, manifest_path)?;
        }
    }
    for cred in ["credentials", "credentials.toml"] {
        let p = cargo.join(cred);
        if p.exists() && !is_reparse_point(&p) {
            if dacl_already_grants_sid(&p, toolchain_sid, FILE_READ_ATTRIBUTES) {
                let _ = remove_sid_aces_from_path(&p, toolchain_sid);
            }
            if !dacl_already_denies_sid(&p, toolchain_sid, FILE_ALL_ACCESS) {
                record_acl_grant(manifest_path, &p)?;
                let deny =
                    explicit_access(toolchain_sid, FILE_ALL_ACCESS, DENY_ACCESS, NO_INHERITANCE);
                let _ = merge_entries_on_path(&p, &[deny]);
            }
        }
    }
    Ok(())
}

/// Validates that no `protected_files` entry (e.g. `--join-token-file`, `--admin-token-file`,
/// TLS key, or worker config file):
/// 1. Lies inside any directory that receives an `oxideswarm.toolchain` read+execute grant
///    (`rustup_home`, `cargo_home`, or MSVC/SDK) or inside a system directory (`C:\Windows`,
///    `C:\Program Files`, `C:\Program Files (x86)`) that AppContainers can read by default.
/// 2. Carries an on-disk DACL granting read access to `oxideswarm.toolchain`, `S-1-15-2-1`
///    (`ALL APPLICATION PACKAGES`), `S-1-15-3-1` (`InternetClient`), or `S-1-15-3-3`
///    (`PrivateNetworkClientServer`) (N5).
pub fn validate_protected_files_outside_toolchains(
    host: &HostDirs,
    protected_files: &[PathBuf],
) -> Result<(), String> {
    let mut forbidden_roots: Vec<(&str, PathBuf)> = Vec::new();
    if !host.rustup_home.as_os_str().is_empty() {
        forbidden_roots.push(("RUSTUP_HOME", host.rustup_home.clone()));
    }
    if !host.cargo_home.as_os_str().is_empty() {
        forbidden_roots.push(("CARGO_HOME", host.cargo_home.clone()));
    }
    for extra_dir in &host.extra_read_only_dirs {
        if !extra_dir.as_os_str().is_empty() {
            forbidden_roots.push(("isolation read dir", extra_dir.clone()));
        }
    }
    if let Some((_, _, grant_roots)) = discover_msvc_details() {
        for r in grant_roots {
            forbidden_roots.push(("MSVC/Windows SDK toolchain", r));
        }
    }
    for (label, env_var, fallback) in [
        ("Windows system", "SystemRoot", r"C:\Windows"),
        ("Program Files", "ProgramFiles", r"C:\Program Files"),
        (
            "Program Files (x86)",
            "ProgramFiles(x86)",
            r"C:\Program Files (x86)",
        ),
    ] {
        let root = std::env::var_os(env_var)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(fallback));
        forbidden_roots.push((label, root));
    }

    let check_sids: Vec<(&str, OwnedSid)> = [
        (
            "oxideswarm.toolchain",
            OwnedSid::oxideswarm_toolchain().ok(),
        ),
        (
            "S-1-15-2-1 (ALL APPLICATION PACKAGES)",
            OwnedSid::from_well_known(WinBuiltinAnyPackageSid).ok(),
        ),
        (
            "S-1-15-2-2 (ALL RESTRICTED APPLICATION PACKAGES)",
            OwnedSid::from_sid_string("S-1-15-2-2").ok(),
        ),
        (
            "S-1-15-3-1 (InternetClient)",
            OwnedSid::from_well_known(WinCapabilityInternetClientSid).ok(),
        ),
        (
            "S-1-15-3-3 (PrivateNetworkClientServer)",
            OwnedSid::from_well_known(WinCapabilityPrivateNetworkClientServerSid).ok(),
        ),
    ]
    .into_iter()
    .filter_map(|(name, sid_opt)| sid_opt.map(|s| (name, s)))
    .collect();

    for pf in protected_files {
        if pf.as_os_str().is_empty() {
            continue;
        }
        for (label, root) in &forbidden_roots {
            if path_starts_with_ci(pf, root) {
                return Err(format!(
                    "refusing to start isolated worker: protected file '{}' lies insideAppContainer-readable {} directory '{}'; move the secret/config file outside toolchain and system directories",
                    pf.display(),
                    label,
                    root.display()
                ));
            }
        }
        if pf.exists() {
            for (sid_label, sid) in &check_sids {
                if dacl_already_grants_sid(pf, sid.as_psid(), FILE_READ_DATA)
                    && !dacl_already_denies_sid(pf, sid.as_psid(), FILE_READ_DATA)
                {
                    return Err(format!(
                        "refusing to start isolated worker: protected file '{}' has a DACL granting read access to {sid_label}",
                        pf.display()
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Validates that `sandbox_base` is a dedicated sandbox directory and NOT a drive root,
/// system directory, user home, toolchain directory, or ancestor thereof before its DACL is
/// replaced with a protected DACL (N4).
pub fn validate_sandbox_base_safe(sandbox_base: &Path, host: &HostDirs) -> Result<(), String> {
    let norm = normalize_win_path(sandbox_base);
    if norm.parent().is_none()
        || matches!(
            norm.components().next_back(),
            Some(Component::RootDir | Component::Prefix(_))
        )
    {
        return Err(format!(
            "refusing to use drive root '{}' as sandbox_base; point --sandbox-base-dir at a dedicated subdirectory",
            sandbox_base.display()
        ));
    }
    for (label, dir) in [
        ("USERPROFILE", &host.user_home),
        ("RUSTUP_HOME", &host.rustup_home),
        ("CARGO_HOME", &host.cargo_home),
    ] {
        if !dir.as_os_str().is_empty() && path_starts_with_ci(dir, sandbox_base) {
            return Err(format!(
                "refusing to rewrite the DACL of '{}' as sandbox_base because {} ('{}') is equal to or inside it; point --sandbox-base-dir at a dedicated OxideSwarm sandbox directory",
                sandbox_base.display(),
                label,
                dir.display()
            ));
        }
    }
    for extra_dir in &host.extra_read_only_dirs {
        if !extra_dir.as_os_str().is_empty()
            && (path_starts_with_ci(extra_dir, sandbox_base)
                || path_starts_with_ci(sandbox_base, extra_dir))
        {
            return Err(format!(
                "refusing '{}' as sandbox_base because it overlaps --isolation-read-dir '{}'",
                sandbox_base.display(),
                extra_dir.display()
            ));
        }
    }
    let temp_root = normalize_win_path(&std::env::temp_dir());
    if norm.as_os_str().eq_ignore_ascii_case(temp_root.as_os_str()) {
        return Err(format!(
            "refusing to rewrite the DACL of the shared system temp directory '{}' as sandbox_base; use a subdirectory such as '{}\\rusty_grid\\sandboxes'",
            sandbox_base.display(),
            sandbox_base.display()
        ));
    }
    for env_var in [
        "SystemRoot",
        "ProgramFiles",
        "ProgramFiles(x86)",
        "ProgramData",
        "PUBLIC",
        "LOCALAPPDATA",
        "APPDATA",
    ] {
        if let Some(sys_dir) = std::env::var_os(env_var).map(PathBuf::from) {
            if !sys_dir.as_os_str().is_empty() && path_starts_with_ci(&sys_dir, sandbox_base) {
                return Err(format!(
                    "refusing to rewrite the DACL of '{}' as sandbox_base because system directory %{env_var}% ('{}') is equal to or inside it",
                    sandbox_base.display(),
                    sys_dir.display()
                ));
            }
        }
    }
    // Refuse to rewrite the DACL of a folder inside the user's home directory (such as
    // `C:\Users\me\Documents` or `C:\Users\me\Desktop`) unless it is inside OxideSwarm's own
    // `<user_home>\.oxideswarm\sandboxes` directory or a dedicated subdirectory under `%TEMP%`
    // (such as the default `%TEMP%\rusty_grid\sandboxes`).
    let is_allowed_under_home = |home: &Path| -> bool {
        let allowed_ox = home.join(".oxideswarm").join("sandboxes");
        let allowed_local_ox = home
            .join("AppData")
            .join("Local")
            .join("oxideswarm")
            .join("sandboxes");
        let allowed_home_temp = home.join("AppData").join("Local").join("Temp");
        let in_home_temp = path_starts_with_ci(sandbox_base, &allowed_home_temp)
            && !path_starts_with_ci(&allowed_home_temp, sandbox_base);
        let temp_is_inside_home = !temp_root.as_os_str().is_empty()
            && path_starts_with_ci(&temp_root, home)
            && !path_starts_with_ci(home, &temp_root);
        let in_env_temp = temp_is_inside_home
            && path_starts_with_ci(sandbox_base, &temp_root)
            && !path_starts_with_ci(&temp_root, sandbox_base);
        path_starts_with_ci(sandbox_base, &allowed_ox)
            || path_starts_with_ci(sandbox_base, &allowed_local_ox)
            || in_home_temp
            || in_env_temp
    };
    if !host.user_home.as_os_str().is_empty()
        && path_starts_with_ci(sandbox_base, &host.user_home)
        && !is_allowed_under_home(&host.user_home)
    {
        let allowed_ox = host.user_home.join(".oxideswarm").join("sandboxes");
        return Err(format!(
            "refusing to rewrite the DACL of user folder '{}' as sandbox_base; point --sandbox-base-dir at a dedicated OxideSwarm subdirectory such as '{}'",
            sandbox_base.display(),
            allowed_ox.display()
        ));
    }
    if let Some(env_up) = std::env::var_os("USERPROFILE").map(PathBuf::from) {
        if !env_up.as_os_str().is_empty()
            && path_starts_with_ci(sandbox_base, &env_up)
            && !is_allowed_under_home(&env_up)
        {
            let allowed_ox = env_up.join(".oxideswarm").join("sandboxes");
            return Err(format!(
                "refusing to rewrite the DACL of user folder '{}' as sandbox_base; point --sandbox-base-dir at a dedicated OxideSwarm subdirectory such as '{}'",
                sandbox_base.display(),
                allowed_ox.display()
            ));
        }
    }
    // If `sandbox_base` already exists on disk, refuse to rewrite its DACL if it contains any
    // entry not created by OxideSwarm (`cache`, `.locks`, `other_task`, `.isolation-probe-*`,
    // `.isolation-outside-*`, `.write_probe_*`, or a UUID task sandbox directory).
    if sandbox_base.exists() {
        if let Ok(entries) = std::fs::read_dir(sandbox_base) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let s = name.to_string_lossy();
                let is_oxideswarm_entry = s == ".locks"
                    || s == "cache"
                    || s == "other_task"
                    || s.starts_with(".isolation-probe-")
                    || s.starts_with(".isolation-outside-")
                    || s.starts_with(".write_probe_")
                    || uuid::Uuid::try_parse(&s).is_ok();
                if !is_oxideswarm_entry {
                    return Err(format!(
                        "refusing to rewrite the DACL of existing folder '{}' containing non-sandbox entry '{}'; point --sandbox-base-dir at a dedicated OxideSwarm subdirectory",
                        sandbox_base.display(),
                        s
                    ));
                }
            }
        }
    }
    Ok(())
}

fn abs_win_no_canon(p: &Path) -> PathBuf {
    let abs = std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf());
    strip_verbatim_prefix(abs)
}

fn components_ci_eq(a: &Path, b: &Path) -> bool {
    let mut ia = a.components();
    let mut ib = b.components();
    loop {
        match (ia.next(), ib.next()) {
            (None, None) => return true,
            (Some(ca), Some(cb)) => {
                if !ca
                    .as_os_str()
                    .to_string_lossy()
                    .eq_ignore_ascii_case(&cb.as_os_str().to_string_lossy())
                {
                    return false;
                }
            }
            _ => return false,
        }
    }
}

/// Checks every folder along `chain` from `home` down to the end of `chain` for NTFS reparse
/// points (symlinks or junctions), without calling `canonicalize` below `home`. Returns `Ok(true)`
/// if `chain` reaches or traverses `home` cleanly.
fn check_chain_from_home(
    chain: &Path,
    ws_display: &Path,
    home_lex: &Path,
    home_norm: &Path,
) -> Result<bool, String> {
    let mut cur = PathBuf::new();
    let mut in_home = false;
    let mut matched_home = false;
    let mut depth_below_home: usize = 0;

    for comp in chain.components() {
        match comp {
            Component::Prefix(_) | Component::RootDir | Component::CurDir => {
                cur.push(comp.as_os_str());
            }
            Component::ParentDir => {
                if in_home {
                    if depth_below_home == 0 {
                        in_home = false;
                        cur.pop();
                    } else {
                        depth_below_home -= 1;
                        cur.pop();
                    }
                } else {
                    cur.push(comp.as_os_str());
                }
                continue;
            }
            Component::Normal(c) => {
                cur.push(c);
                if in_home {
                    depth_below_home += 1;
                }
            }
        }

        if !in_home
            && (components_ci_eq(&cur, home_lex)
                || (!is_reparse_point(&cur)
                    && components_ci_eq(&normalize_win_path(&cur), home_norm)))
        {
            in_home = true;
            matched_home = true;
            depth_below_home = 0;
        }

        if in_home && is_reparse_point(&cur) {
            return Err(format!(
                "refusing '{}' as workspace_root because '{}' is a symlink or junction (reparse point); if your user home or AppData\\Local is redirected via a junction, pass --workspace-root pointing directly at the junction's target directory",
                ws_display.display(),
                cur.display()
            ));
        }
    }

    Ok(matched_home)
}

fn check_any_reparse_in_chain(chain: &Path, ws_display: &Path) -> Result<(), String> {
    let mut cur = PathBuf::new();
    for comp in chain.components() {
        match comp {
            Component::Prefix(_) | Component::RootDir | Component::CurDir => {
                cur.push(comp.as_os_str());
            }
            Component::ParentDir => {
                cur.pop();
            }
            Component::Normal(c) => {
                cur.push(c);
                if is_reparse_point(&cur) {
                    return Err(format!(
                        "refusing '{}' as workspace_root because '{}' is a symlink or junction (reparse point); if your user home or AppData\\Local is redirected via a junction, pass --workspace-root pointing directly at the junction's target directory",
                        ws_display.display(),
                        cur.display()
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Refuses `ws_root` when any directory from `home` down to `ws_root` (or any ancestor along
/// the raw, lexical, or canonical path when `ws_root` is under or resolves into `home`) is a
/// symlink or junction (`FILE_ATTRIBUTE_REPARSE_POINT`), before `normalize_win_path` /
/// `path_starts_with_ci` canonicalises past it or `SetNamedSecurityInfoW` rewrites the
/// target's DACL. Roots outside `home` that do not resolve into `home` have no ancestor check
/// (only `ws_root` itself is checked).
fn check_no_reparse_from_home(
    ws_root: &Path,
    raw_abs: &Path,
    lex_abs: &Path,
    home: &Path,
) -> Result<(), String> {
    if home.as_os_str().is_empty() {
        return Ok(());
    }
    let home_lex = lexically_resolve_dots(&abs_win_no_canon(home));
    let home_norm = normalize_win_path(home);
    let raw_matched = check_chain_from_home(raw_abs, ws_root, &home_lex, &home_norm)?;
    let lex_matched = check_chain_from_home(lex_abs, ws_root, &home_lex, &home_norm)?;
    if raw_matched || lex_matched || path_starts_with_ci(ws_root, home) {
        check_any_reparse_in_chain(raw_abs, ws_root)?;
        check_any_reparse_in_chain(lex_abs, ws_root)?;
        check_any_reparse_in_chain(&normalize_win_path(ws_root), ws_root)?;
    }
    Ok(())
}

fn read_windows_file_info(file: &std::fs::File) -> io::Result<BY_HANDLE_FILE_INFORMATION> {
    let handle = file.as_raw_handle() as HANDLE;
    // SAFETY: `BY_HANDLE_FILE_INFORMATION` contains only plain integer and `FILETIME` fields.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: `file` keeps `handle` valid for the duration of this call, and `&mut info` is a
    // valid writable `BY_HANDLE_FILE_INFORMATION`.
    if unsafe { GetFileInformationByHandle(handle, &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info)
}

pub(in crate::isolation) fn validate_single_link_file_info(
    ws_root: &Path,
    entry_name: &str,
    info: io::Result<BY_HANDLE_FILE_INFORMATION>,
) -> Result<(), String> {
    let info = match info {
        Ok(info) => info,
        Err(e) => {
            return Err(format!(
                "refusing to rewrite the DACL of existing folder '{}' because file link information (GetFileInformationByHandle) for '{}' could not be read: {e}",
                ws_root.display(),
                entry_name
            ));
        }
    };
    if (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
        return Err(format!(
            "refusing to rewrite the DACL of existing folder '{}' because '{}' is a symlink or reparse point",
            ws_root.display(),
            entry_name
        ));
    }
    if (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 {
        return Err(format!(
            "refusing to rewrite the DACL of existing folder '{}' because '{}' is not a regular file",
            ws_root.display(),
            entry_name
        ));
    }
    if info.nNumberOfLinks == 0 {
        return Err(format!(
            "refusing to rewrite the DACL of existing folder '{}' because '{}' reported nNumberOfLinks == 0 (cannot confirm a single hard link, nlink == 1)",
            ws_root.display(),
            entry_name
        ));
    }
    if info.nNumberOfLinks != 1 {
        return Err(format!(
            "refusing to rewrite the DACL of existing folder '{}' because '{}' has {} hard links (must be a single-link regular file, nlink == 1)",
            ws_root.display(),
            entry_name,
            info.nNumberOfLinks
        ));
    }
    Ok(())
}

/// Validates that `entry_path` is an existing regular non-reparse file with `nNumberOfLinks == 1`,
/// reading `nNumberOfLinks` directly from `GetFileInformationByHandle` so volumes with zero
/// 64-bit file index or volume serial (ReFS, Dev Drive, network volumes) are still accepted.
fn check_single_link_regular_file(
    ws_root: &Path,
    entry_name: &str,
    entry_path: &Path,
) -> Result<(), String> {
    let meta = std::fs::symlink_metadata(entry_path).map_err(|e| {
        format!(
            "refusing to rewrite the DACL of existing folder '{}' because metadata for '{}' could not be read: {e}",
            ws_root.display(),
            entry_name
        )
    })?;
    if (meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0 || meta.file_type().is_symlink()
    {
        return Err(format!(
            "refusing to rewrite the DACL of existing folder '{}' because '{}' is a symlink or reparse point",
            ws_root.display(),
            entry_name
        ));
    }
    if !meta.file_type().is_file() {
        return Err(format!(
            "refusing to rewrite the DACL of existing folder '{}' because '{}' is not a regular file",
            ws_root.display(),
            entry_name
        ));
    }
    let file = match rusty_grid_core::blob::open_regular_file_nofollow(entry_path) {
        Ok((file, _)) => file,
        Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
            let msg = e.to_string();
            if msg.contains("symbolic links") {
                return Err(format!(
                    "refusing to rewrite the DACL of existing folder '{}' because '{}' is a symlink or reparse point",
                    ws_root.display(),
                    entry_name
                ));
            }
            return Err(format!(
                "refusing to rewrite the DACL of existing folder '{}' because '{}' is not a regular file",
                ws_root.display(),
                entry_name
            ));
        }
        Err(e) => {
            return Err(format!(
                "refusing to rewrite the DACL of existing folder '{}' because '{}' could not be opened without following reparse points: {e}",
                ws_root.display(),
                entry_name
            ));
        }
    };
    validate_single_link_file_info(ws_root, entry_name, read_windows_file_info(&file))
}

/// Refuses `ws_root` if it is a reparse point or has a reparse point in any ancestor when it is
/// under or resolves into `host.user_home` or `%USERPROFILE%`.
pub(super) fn validate_workspace_root_ancestors_safe(
    ws_root: &Path,
    host: &HostDirs,
) -> Result<(), String> {
    let raw_abs = abs_win_no_canon(ws_root);
    let lex_abs = lexically_resolve_dots(&raw_abs);
    if is_reparse_point(ws_root) || is_reparse_point(&raw_abs) || is_reparse_point(&lex_abs) {
        return Err(format!(
            "refusing '{}' as workspace_root because it is a symlink or junction (reparse point); pass --workspace-root pointing directly at the target directory",
            ws_root.display()
        ));
    }
    check_no_reparse_from_home(ws_root, &raw_abs, &lex_abs, &host.user_home)?;
    if let Some(env_up) = std::env::var_os("USERPROFILE").map(PathBuf::from) {
        check_no_reparse_from_home(ws_root, &raw_abs, &lex_abs, &env_up)?;
    }
    Ok(())
}

/// Validates that `ws_root` is a dedicated OxideSwarm workspace root directory and not a
/// drive root, home directory, toolchain directory, shared system `%TEMP%` root, or arbitrary
/// user directory.
pub fn validate_workspace_root_safe(ws_root: &Path, host: &HostDirs) -> Result<(), String> {
    validate_workspace_root_ancestors_safe(ws_root, host)?;

    let norm = normalize_win_path(ws_root);
    if norm.parent().is_none()
        || matches!(
            norm.components().next_back(),
            Some(Component::RootDir | Component::Prefix(_))
        )
    {
        return Err(format!(
            "refusing to use drive root '{}' as workspace_root; point --workspace-root at a dedicated subdirectory",
            ws_root.display()
        ));
    }
    for (label, dir) in [
        ("USERPROFILE", &host.user_home),
        ("RUSTUP_HOME", &host.rustup_home),
        ("CARGO_HOME", &host.cargo_home),
    ] {
        if !dir.as_os_str().is_empty() && path_starts_with_ci(dir, ws_root) {
            return Err(format!(
                "refusing to rewrite the DACL of '{}' as workspace_root because {} ('{}') is equal to or inside it; point --workspace-root at a dedicated OxideSwarm workspace directory",
                ws_root.display(),
                label,
                dir.display()
            ));
        }
    }
    for extra_dir in &host.extra_read_only_dirs {
        if !extra_dir.as_os_str().is_empty()
            && (path_starts_with_ci(extra_dir, ws_root) || path_starts_with_ci(ws_root, extra_dir))
        {
            return Err(format!(
                "refusing '{}' as workspace_root because it overlaps --isolation-read-dir '{}'",
                ws_root.display(),
                extra_dir.display()
            ));
        }
    }
    let temp_root = normalize_win_path(&std::env::temp_dir());
    if norm.as_os_str().eq_ignore_ascii_case(temp_root.as_os_str()) {
        return Err(format!(
            "refusing to rewrite the DACL of the shared system temp directory '{}' as workspace_root; use a subdirectory",
            ws_root.display()
        ));
    }
    for env_var in [
        "SystemRoot",
        "ProgramFiles",
        "ProgramFiles(x86)",
        "ProgramData",
        "PUBLIC",
        "LOCALAPPDATA",
        "APPDATA",
    ] {
        if let Some(sys_dir) = std::env::var_os(env_var).map(PathBuf::from) {
            if !sys_dir.as_os_str().is_empty() && path_starts_with_ci(&sys_dir, ws_root) {
                return Err(format!(
                    "refusing to rewrite the DACL of '{}' as workspace_root because system directory %{env_var}% ('{}') is equal to or inside it",
                    ws_root.display(),
                    sys_dir.display()
                ));
            }
        }
    }
    let is_allowed_under_home = |home: &Path| -> bool {
        let allowed1 = home.join(".oxideswarm").join("workspaces");
        let allowed2 = home.join(".cache").join("oxideswarm").join("workspaces");
        let allowed3 = home
            .join("AppData")
            .join("Local")
            .join("oxideswarm")
            .join("workspaces");
        let allowed_usb = home
            .join("AppData")
            .join("Local")
            .join("OxideSwarm-USB")
            .join("workspaces");
        let allowed_usb_legacy = home
            .join("AppData")
            .join("Local")
            .join("OxideSwarm")
            .join("usb-kit")
            .join("workspaces");
        let allowed_home_temp = home.join("AppData").join("Local").join("Temp");
        let in_home_temp = path_starts_with_ci(ws_root, &allowed_home_temp)
            && !path_starts_with_ci(&allowed_home_temp, ws_root);
        let temp_is_inside_home = !temp_root.as_os_str().is_empty()
            && path_starts_with_ci(&temp_root, home)
            && !path_starts_with_ci(home, &temp_root);
        let in_env_temp = temp_is_inside_home
            && path_starts_with_ci(ws_root, &temp_root)
            && !path_starts_with_ci(&temp_root, ws_root);
        path_starts_with_ci(ws_root, &allowed1)
            || path_starts_with_ci(ws_root, &allowed2)
            || path_starts_with_ci(ws_root, &allowed3)
            || path_starts_with_ci(ws_root, &allowed_usb)
            || path_starts_with_ci(ws_root, &allowed_usb_legacy)
            || in_home_temp
            || in_env_temp
    };
    if !host.user_home.as_os_str().is_empty()
        && path_starts_with_ci(ws_root, &host.user_home)
        && !is_allowed_under_home(&host.user_home)
    {
        return Err(format!(
            "refusing to rewrite the DACL of user folder '{}' as workspace_root; point --workspace-root at a dedicated OxideSwarm workspace subdirectory",
            ws_root.display()
        ));
    }
    if let Some(env_up) = std::env::var_os("USERPROFILE").map(PathBuf::from) {
        if !env_up.as_os_str().is_empty()
            && path_starts_with_ci(ws_root, &env_up)
            && !is_allowed_under_home(&env_up)
        {
            return Err(format!(
                "refusing to rewrite the DACL of user folder '{}' as workspace_root; point --workspace-root at a dedicated OxideSwarm workspace subdirectory",
                ws_root.display()
            ));
        }
    }
    if ws_root.exists() {
        if let Ok(entries) = std::fs::read_dir(ws_root) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let s = name.to_string_lossy();
                let entry_path = entry.path();
                if s == ".oxideswarm-usb-workspaces" || s == "store.lock" {
                    check_single_link_regular_file(ws_root, &s, &entry_path)?;
                    continue;
                }
                let is_ws_entry =
                    s == ".locks" || rusty_grid_core::task::validate_workspace_name(&s).is_ok();
                if !is_ws_entry {
                    return Err(format!(
                        "refusing to rewrite the DACL of existing folder '{}' containing non-workspace entry '{}'; point --workspace-root at a dedicated OxideSwarm workspace subdirectory",
                        ws_root.display(),
                        s
                    ));
                }
                if s != ".locks" && entry_path.is_dir() {
                    if let Ok(sub_entries) = std::fs::read_dir(&entry_path) {
                        for sub in sub_entries.flatten() {
                            let sub_name = sub.file_name();
                            let sub_s = sub_name.to_string_lossy();
                            let valid_sub = matches!(
                                sub_s.as_ref(),
                                "tree" | "tmp" | "state.json" | "state.json.tmp" | "lock"
                            );
                            if !valid_sub {
                                return Err(format!(
                                    "refusing to rewrite the DACL of existing folder '{}' because subdirectory '{}' contains non-workspace entry '{}'",
                                    ws_root.display(),
                                    s,
                                    sub_s
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Protects `sandbox_base` so tasks holding `oxideswarm.toolchain` can traverse `sandbox_base`
/// (`FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE`, `NO_INHERITANCE`) without being able
/// to list `sandbox_base` (`FILE_LIST_DIRECTORY` omitted) or read sibling sandboxes.
/// Records the original DACL SDDL of `sandbox_base` in `manifest_path` before modifying it so
/// `revoke_oxideswarm_toolchain_acls` and `windows/revoke-isolation-acls.ps1` restore the
/// exact original DACL and inheritance state (N4).
pub(super) fn protect_sandbox_base(
    sandbox_base: &Path,
    toolchain_sid: PSID,
    manifest_path: Option<&Path>,
) -> io::Result<()> {
    if !sandbox_base.exists() || is_reparse_point(sandbox_base) {
        return Ok(());
    }
    let traverse_mask = FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
    if dacl_already_grants_sid(sandbox_base, toolchain_sid, traverse_mask)
        && !dacl_already_grants_sid(sandbox_base, toolchain_sid, FILE_LIST_DIRECTORY)
    {
        return Ok(());
    }
    if let Ok(orig_sddl) = get_path_dacl_sddl(sandbox_base) {
        record_sddl_restore(manifest_path, sandbox_base, &orig_sddl)?;
    }
    record_acl_grant(manifest_path, sandbox_base)?;
    let user_sid = OwnedSid::current_user()?;
    let system_sid = OwnedSid::from_well_known(WinLocalSystemSid)?;
    let admins_sid = OwnedSid::from_well_known(WinBuiltinAdministratorsSid)?;
    let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
    let base_entries = [
        explicit_access(user_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
        explicit_access(system_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
        explicit_access(admins_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
        explicit_access(toolchain_sid, traverse_mask, SET_ACCESS, NO_INHERITANCE),
    ];
    set_protected_dacl_on_path(sandbox_base, &base_entries)
}

/// Prepares the worker's own toolchain directories (`host.rustup_home` and `host.cargo_home`)
/// for the `oxideswarm.toolchain` capability SID, and configures `sandbox_base` with
/// non-inherited traverse-only access.
///
/// Security invariants enforced here:
/// - Grants `oxideswarm.toolchain` (`S-1-15-3-...`), NEVER `S-1-15-2-1` (`ALL APPLICATION PACKAGES`).
/// - Never modifies `<user_home>` (`C:\Users\<user>`) or any secret directory under it.
/// - Never reads task env (`CARGO_HOME`, `RUSTUP_HOME`) or task `Cargo.lock`.
/// - Refuses if any `protected_files` path lies inside a granted toolchain root or is readable
///   by AppContainer SIDs.
/// - Sets a single inheritable ACE on each root (`<rustup_home>/toolchains`,
///   `<cargo_home>/{bin,git,vendor,registry}`) without walking descendants (N6).
/// - Checks the actual on-disk root DACL (`dacl_already_grants_sid`) in $O(1)$ before writing
///   any ACE, with no stale in-memory cache.
/// - Logs every modified path (plus `SDDL|<sandbox_base>|<orig_sddl>`) to
///   `acl_grant_manifest_path(host)` before modifying its DACL.
pub fn prepare_worker_toolchains(
    host: &HostDirs,
    protected_files: &[PathBuf],
    sandbox_base: Option<&Path>,
) -> Result<(), String> {
    ensure_process_token_default_dacl_grants_current_user();
    crate::isolation::validate_extra_read_only_dirs(
        &host.extra_read_only_dirs,
        host,
        sandbox_base,
        None,
        protected_files,
    )?;
    validate_protected_files_outside_toolchains(host, protected_files)?;
    if let Some(base) = sandbox_base {
        validate_sandbox_base_safe(base, host)?;
    }

    let _guard = PREPARE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let toolchain_sid = OwnedSid::oxideswarm_toolchain()
        .map_err(|e| format!("DeriveCapabilitySidsFromName(oxideswarm.toolchain) failed: {e}"))?;

    let manifest = acl_grant_manifest_path(host);
    let manifest_opt = if host.user_home.as_os_str().is_empty()
        && std::env::var_os("RUSTY_GRID_ACL_GRANT_LOG")
            .filter(|v| !v.is_empty())
            .is_none()
        && std::env::var_os("OXIDESWARM_HOME")
            .filter(|v| !v.is_empty())
            .is_none()
    {
        None
    } else {
        Some(manifest.as_path())
    };

    if let Some(base) = sandbox_base {
        if base.exists() {
            protect_sandbox_base(base, toolchain_sid.as_psid(), manifest_opt).map_err(|e| {
                format!(
                    "failed to configure sandbox_base DACL on {}: {e}",
                    base.display()
                )
            })?;
        }
    }

    // Never touch `host.user_home` itself. Windows `SeChangeNotifyPrivilege` (Bypass Traverse
    // Checking, held by AppContainers) allows opening `<user_home>\.rustup` or
    // `<user_home>\.cargo` directly by full path without any ACE on `<user_home>`.
    // If a secret file under `<user_home>\.oxideswarm` or `<OXIDESWARM_HOME>` was atomically
    // replaced via same-volume `MoveFileExW` (`std::fs::rename`) from a staging location that
    // carried an `oxideswarm.toolchain` allow ACE, strip that allow ACE so the secret stays
    // unreadable.
    if let Some(ox_home) = std::env::var_os("OXIDESWARM_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
    {
        for rel in [
            "cluster.secret",
            "node.key",
            "p2p_secret.key",
            "master_noise.key",
        ] {
            let sec = ox_home.join(rel);
            if sec.exists()
                && !is_reparse_point(&sec)
                && dacl_already_grants_sid(&sec, toolchain_sid.as_psid(), FILE_READ_ATTRIBUTES)
            {
                let _ = remove_sid_aces_from_path(&sec, toolchain_sid.as_psid());
            }
        }
    }
    if !host.user_home.as_os_str().is_empty() {
        for rel in [
            ".oxideswarm/cluster.secret",
            ".oxideswarm/node.key",
            ".oxideswarm/p2p_secret.key",
            ".oxideswarm/master_noise.key",
        ] {
            let sec = host.user_home.join(rel);
            if sec.exists()
                && !is_reparse_point(&sec)
                && dacl_already_grants_sid(&sec, toolchain_sid.as_psid(), FILE_READ_ATTRIBUTES)
            {
                let _ = remove_sid_aces_from_path(&sec, toolchain_sid.as_psid());
            }
        }
    }
    if !host.rustup_home.as_os_str().is_empty()
        && host.rustup_home.exists()
        && (host.user_home.as_os_str().is_empty()
            || !path_starts_with_ci(&host.user_home, &host.rustup_home))
    {
        let rustup = &host.rustup_home;
        grant_rx_non_inherited(rustup, toolchain_sid.as_psid(), manifest_opt)
            .map_err(|e| format!("failed to prepare RUSTUP_HOME {}: {e}", rustup.display()))?;
        let settings = rustup.join("settings.toml");
        if settings.exists() {
            let _ = grant_rx_non_inherited(&settings, toolchain_sid.as_psid(), manifest_opt);
        }
        // Grant a single inheritable `(OI)(CI)(RX)` ACE on `<rustup_home>/toolchains` itself
        // so both existing toolchains and any toolchain installed later by `rustup update`
        // inherit read+execute automatically without walking (N6).
        let tc_root = rustup.join("toolchains");
        if tc_root.exists() {
            grant_rx_recursive(&tc_root, toolchain_sid.as_psid(), manifest_opt).map_err(|e| {
                format!(
                    "failed to prepare RUSTUP_HOME toolchains {}: {e}",
                    tc_root.display()
                )
            })?;
        }
    }

    if !host.cargo_home.as_os_str().is_empty()
        && host.cargo_home.exists()
        && (host.user_home.as_os_str().is_empty()
            || !path_starts_with_ci(&host.user_home, &host.cargo_home))
    {
        prepare_one_cargo_dir(&host.cargo_home, toolchain_sid.as_psid(), manifest_opt).map_err(
            |e| {
                format!(
                    "failed to prepare CARGO_HOME {}: {e}",
                    host.cargo_home.display()
                )
            },
        )?;
    }

    for dir in &host.extra_read_only_dirs {
        if dir.as_os_str().is_empty() {
            continue;
        }
        if !dir.exists() || is_reparse_point(dir) {
            return Err(format!(
                "refusing to grant AppContainer ACL on missing or reparse-point --isolation-read-dir {}",
                dir.display()
            ));
        }
        let norm_dir = normalize_win_path(dir);
        if !host.user_home.as_os_str().is_empty() && path_starts_with_ci(&host.user_home, &norm_dir)
        {
            return Err(format!(
                "refusing to grant AppContainer ACL on '{}' because USERPROFILE '{}' is equal to or inside it",
                norm_dir.display(),
                host.user_home.display()
            ));
        }
        grant_rx_recursive(&norm_dir, toolchain_sid.as_psid(), manifest_opt).map_err(|e| {
            format!(
                "failed to prepare --isolation-read-dir {}: {e}",
                dir.display()
            )
        })?;
    }

    Ok(())
}

/// Removes all explicit ACEs (both allow and deny) for `oxideswarm.toolchain` from `path`
/// and any descendants that still carry an explicit `oxideswarm.toolchain` ACE, skipping
/// reparse points (N4, N6).
pub fn revoke_toolchain_sid_on_tree(path: &Path, toolchain_sid: PSID) -> io::Result<()> {
    if !path.exists() || is_reparse_point(path) {
        return Ok(());
    }
    remove_sid_aces_from_path(path, toolchain_sid)?;
    if path.is_dir() {
        let mut stack = vec![path.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in rd.flatten() {
                let p = entry.path();
                if is_reparse_point(&p) {
                    continue;
                }
                if dacl_already_grants_sid(&p, toolchain_sid, FILE_READ_ATTRIBUTES)
                    || dacl_already_denies_sid(&p, toolchain_sid, FILE_READ_ATTRIBUTES)
                {
                    let _ = remove_sid_aces_from_path(&p, toolchain_sid);
                }
                if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                    stack.push(p);
                }
            }
        }
    }
    Ok(())
}

/// Reads the ACL grant manifest at `manifest_path`, revokes `oxideswarm.toolchain` allow and
/// deny ACEs from every recorded path, and restores any `SDDL|<path>|<orig_sddl>` entries
/// (such as `sandbox_base`). Returns the list of paths processed.
pub fn revoke_oxideswarm_toolchain_acls(manifest_path: &Path) -> io::Result<Vec<PathBuf>> {
    if !manifest_path.exists() {
        return Ok(Vec::new());
    }
    let toolchain_sid = OwnedSid::oxideswarm_toolchain()?;
    let content = std::fs::read_to_string(manifest_path)?;
    let mut revoked = Vec::new();
    let mut sddl_restores: Vec<(PathBuf, String)> = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("SDDL|") {
            if let Some((path_str, sddl_str)) = rest.split_once('|') {
                if !path_str.is_empty() && !sddl_str.is_empty() {
                    sddl_restores.push((PathBuf::from(path_str), sddl_str.to_string()));
                }
            }
            continue;
        }
        let p = PathBuf::from(trimmed);
        if p.exists() {
            revoke_toolchain_sid_on_tree(&p, toolchain_sid.as_psid())?;
            revoked.push(p);
        }
    }
    for (p, orig_sddl) in sddl_restores {
        if p.exists() {
            restore_path_dacl_sddl(&p, &orig_sddl)?;
            if !revoked.contains(&p) {
                revoked.push(p);
            }
        }
    }
    Ok(revoked)
}
