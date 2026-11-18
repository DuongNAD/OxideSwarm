//! The Windows implementation (design 0033 D4, FEAT-3c):
//! - Dedicated custom capability SID `oxideswarm.toolchain` derived via
//!   `DeriveCapabilitySidsFromName` for read+execute on worker-owned toolchains (`rustup_home`,
//!   `cargo_home`, and read-only MSVC/SDK directories), never `S-1-15-2-1` (`ALL APPLICATION PACKAGES`).
//! - One-time worker toolchain preparation from the worker's own `HostDirs`, never from task env
//!   or `Cargo.lock`, never touching `<user_home>` or `<user_home>/.cargo`, and refusing to start
//!   if any `protected_files` path lies inside a granted toolchain directory.
//! - Per-task AppContainer profile (`oxideswarm.task.<run_token>`), explicit protected DACL on
//!   `<sandbox_dir>` granting only `SYSTEM`, `Administrators`, current user, and the per-task SID,
//!   non-inherited traverse ACE on `<sandbox_base>` for `oxideswarm.toolchain`, zero network
//!   capabilities under `fs+net`, and `DeleteAppContainerProfile` with retry backoff + start-up
//!   sweep on cleanup.

mod acl;
mod desktop;
mod path;
mod spawn;
mod toolchain;

use super::{HostDirs, IsolationLevel, Jail};
use acl::{
    explicit_access, merge_entries_on_path, merge_entries_on_path_unprotected,
    set_protected_dacl_on_path,
};
use desktop::{create_per_task_desktop, CloseDesktop};
use path::{discover_msvc_details, to_wide};
use spawn::{
    ensure_process_token_default_dacl_grants_current_user, spawn_with_security_capabilities,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::os::windows::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use toolchain::protect_sandbox_base;
use windows_sys::Win32::Foundation::{ERROR_ALREADY_EXISTS, HANDLE};
use windows_sys::Win32::Security::Authorization::{GRANT_ACCESS, SET_ACCESS};
use windows_sys::Win32::Security::Isolation::{
    CreateAppContainerProfile, DeleteAppContainerProfile, DeriveAppContainerSidFromAppContainerName,
};
use windows_sys::Win32::Security::{
    FreeSid, WinBuiltinAdministratorsSid, WinCapabilityInternetClientSid,
    WinCapabilityPrivateNetworkClientServerSid, WinLocalSystemSid, CONTAINER_INHERIT_ACE,
    INHERIT_ONLY_ACE, NO_INHERITANCE, OBJECT_INHERIT_ACE, PSID, SECURITY_CAPABILITIES,
    SID_AND_ATTRIBUTES,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ALL_ACCESS, FILE_READ_ATTRIBUTES, FILE_TRAVERSE, SYNCHRONIZE, WRITE_DAC, WRITE_OWNER,
};
use windows_sys::Win32::System::SystemServices::SE_GROUP_ENABLED;

pub use acl::{
    dacl_already_denies_sid, dacl_already_grants_sid, dacl_already_grants_sid_with_flags,
    get_path_dacl_sddl, oxideswarm_toolchain_sid_string, restore_path_dacl_sddl, OwnedSid,
    OXIDESWARM_TOOLCHAIN_CAPABILITY_NAME,
};
pub(super) use path::resolve_program;
pub use path::{
    is_reparse_point, normalize_win_path, path_starts_with_ci, resolve_windows_msvc_env,
    worker_profile_prefix,
};
pub use spawn::{is_process_elevated, AppContainerChild};
pub use toolchain::{
    prepare_worker_toolchains, revoke_oxideswarm_toolchain_acls, revoke_toolchain_sid_on_tree,
    validate_protected_files_outside_toolchains, validate_sandbox_base_safe,
    validate_workspace_root_safe,
};

const HRESULT_ALREADY_EXISTS: i32 =
    ((1u32 << 31) | (7u32 << 16) | (ERROR_ALREADY_EXISTS & 0xffff)) as i32;

pub use super::{acl_grant_manifest_path, acl_grant_manifest_path_with, ACL_GRANT_LOG_FILENAME};

/// Serialises toolchain and sandbox_base ACL preparation within the worker process.
pub(super) static PREPARE_LOCK: Mutex<()> = Mutex::new(());
/// Serialises the brief window in `spawn_with_security_capabilities` where the child ends of
/// the stdio pipes are marked inheritable for `CreateProcessW` and geschlossen in the parent,
/// as well as the brief `SetProcessWindowStation` switch when creating a per-task desktop.
/// Also acquired by standard `std::process::Command` spawns (`vswhere.exe`, `taskkill.exe`) so
/// no concurrent process creation can observe a temporary Window Station switch or inherit a
/// child pipe handle even if `bInheritHandles` is set (M2, M5, N7).
pub(crate) static WINDOWS_SPAWN_LOCK: Mutex<()> = Mutex::new(());

struct WorkerPrefixLock {
    _lock_file: std::fs::File,
    active_guards: usize,
}

static WORKER_PREFIX_LOCKS: OnceLock<Mutex<HashMap<String, WorkerPrefixLock>>> = OnceLock::new();

fn acquire_worker_prefix_lock(prefix: &str, increment_guard: bool) -> bool {
    let map_mutex = WORKER_PREFIX_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = map_mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(state) = map.get_mut(prefix) {
        if increment_guard {
            state.active_guards += 1;
        }
        return state.active_guards == usize::from(increment_guard);
    }
    let lock_path = std::env::temp_dir().join(format!("{prefix}.lock"));
    let Ok(file) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0)
        .open(&lock_path)
    else {
        return false;
    };
    map.insert(
        prefix.to_string(),
        WorkerPrefixLock {
            _lock_file: file,
            active_guards: usize::from(increment_guard),
        },
    );
    true
}

fn release_worker_prefix_guard(prefix: &str) {
    if let Some(map_mutex) = WORKER_PREFIX_LOCKS.get() {
        let mut map = map_mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(state) = map.get_mut(prefix) {
            state.active_guards = state.active_guards.saturating_sub(1);
        }
    }
}

/// Sweeps leftover `oxideswarm.task.<worker8>.*` AppContainer profiles belonging ONLY to the
/// worker instance for `sandbox_base`, holding an exclusive OS file lock
/// (`%TEMP%\oxideswarm.task.<worker8>.lock`, `share_mode(0)`) so two workers with the same
/// `sandbox_base` or parallel tests never delete each other's live profiles (N3).
pub fn sweep_leftover_appcontainer_profiles_for_base(sandbox_base: &Path) -> usize {
    let prefix = worker_profile_prefix(sandbox_base);
    if !acquire_worker_prefix_lock(&prefix, false) {
        return 0;
    }
    let match_prefix = format!("{prefix}.");
    let mut deleted = 0usize;
    let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") else {
        return 0;
    };
    let packages_dir = PathBuf::from(local_app_data).join("Packages");
    let Ok(entries) = std::fs::read_dir(&packages_dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        let Some(prof_name) = name
            .split('_')
            .next()
            .filter(|s| s.starts_with(&match_prefix))
        else {
            continue;
        };
        let wide = to_wide(OsStr::new(prof_name));
        let hr = unsafe { DeleteAppContainerProfile(wide.as_ptr()) };
        if hr >= 0 {
            deleted += 1;
        }
    }
    deleted
}

/// Sweeps leftover AppContainer profiles for the default sandbox base directory.
pub fn sweep_leftover_appcontainer_profiles() -> usize {
    let default_base = crate::sandbox::SandboxConfig::default().base_dir;
    sweep_leftover_appcontainer_profiles_for_base(&default_base)
}

/// Per-task Windows AppContainer state that must outlive `CreateProcessW` and deletes the
/// AppContainer profile and closes the per-task private Desktop when the task's `ProcessTree`
/// is dropped.
pub struct WindowsAppContainerGuard {
    worker_prefix: String,
    wide_profile_name: Vec<u16>,
    app_container_sid: PSID,
    desktop_handle: HANDLE,
    desktop_name_wide: Vec<u16>,
    _capability_sids: Vec<OwnedSid>,
    _capability_attrs: Vec<SID_AND_ATTRIBUTES>,
    security_capabilities: Box<SECURITY_CAPABILITIES>,
    workspace_dirs_to_restore: Vec<PathBuf>,
    cleaned_up: bool,
}

// SAFETY: WindowsAppContainerGuard owns its heap allocations and raw pointers are only read
// synchronously during `CreateProcessW` before `spawn()` returns.
unsafe impl Send for WindowsAppContainerGuard {}
unsafe impl Sync for WindowsAppContainerGuard {}

impl WindowsAppContainerGuard {
    pub(crate) fn create(
        run_token: uuid::Uuid,
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
        Self::create_blocking(
            run_token,
            jail,
            &program,
            cmd_cargo_home.as_deref(),
            cmd_rustup_home.as_deref(),
        )
    }

    /// Blocking per-task AppContainer profile + sandbox DACL + private desktop setup (invoked
    /// inside `tokio::task::spawn_blocking` by `TaskRunner::execute_process_supervised`).
    ///
    /// Security invariants:
    /// - Never grants `S-1-15-2-1` (`ALL APPLICATION PACKAGES`) on any path.
    /// - Never modifies any directory named by the task's `CARGO_HOME`, `RUSTUP_HOME`,
    ///   `program`, or `Cargo.lock`.
    /// - Never modifies `<user_home>` or any directory outside `<sandbox_base>` and
    ///   `<sandbox_dir>` (or `<workspace_root>/<ws_name>/{tree,tmp}`).
    /// - Refuses if `<sandbox_dir>`, `<sandbox_base>`, `<extra_writable_dir>`, or
    ///   `<workspace_root>` is a symlink/junction.
    /// - Creates a per-worker, per-task AppContainer profile
    ///   `oxideswarm.task.<worker8>.<run_token_simple>` (N3).
    /// - Creates a per-task private Desktop granting `DESKTOP_TASK_MASK` only to `CurrentUser`
    ///   and this task's `app_container_sid`; fails the task if desktop creation fails (N7).
    pub(crate) fn create_blocking(
        run_token: uuid::Uuid,
        jail: &Jail<'_>,
        program: &str,
        cmd_cargo_home: Option<&Path>,
        cmd_rustup_home: Option<&Path>,
    ) -> io::Result<Self> {
        ensure_process_token_default_dacl_grants_current_user();
        super::validate_extra_read_only_dirs(
            &jail.host.extra_read_only_dirs,
            jail.host,
            Some(jail.sandbox_base),
            jail.workspace_root,
            jail.protected_files,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;
        validate_protected_files_outside_toolchains(jail.host, jail.protected_files)
            .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;
        validate_sandbox_base_safe(jail.sandbox_base, jail.host)
            .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;

        if is_reparse_point(jail.sandbox_dir) || is_reparse_point(jail.sandbox_base) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "refusing to configure AppContainer on a reparse-point sandbox path",
            ));
        }
        if let Some(extra) = jail.extra_writable_dir {
            if is_reparse_point(extra) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "refusing to configure AppContainer on a reparse-point workspace tmp path",
                ));
            }
        }
        if let Some(ws_root) = jail.workspace_root {
            validate_workspace_root_safe(ws_root, jail.host)
                .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;
            if is_reparse_point(ws_root) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "refusing to configure AppContainer on a reparse-point workspace root path",
                ));
            }
        }

        let in_task_writable = |p: &Path| -> bool {
            path_starts_with_ci(p, jail.sandbox_dir)
                || jail
                    .extra_writable_dir
                    .is_some_and(|extra| path_starts_with_ci(p, extra))
        };

        // Requirement 2: A task's `CARGO_HOME` or `RUSTUP_HOME` may point only to the worker's
        // own configured toolchain directory or a directory inside the task's sandbox.
        if let Some(ch) = cmd_cargo_home {
            if !ch.as_os_str().is_empty()
                && !in_task_writable(ch)
                && (jail.host.cargo_home.as_os_str().is_empty()
                    || normalize_win_path(ch) != normalize_win_path(&jail.host.cargo_home))
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "task CARGO_HOME '{}' must point inside the task sandbox '{}' or match the worker's CARGO_HOME",
                        ch.display(),
                        jail.sandbox_dir.display()
                    ),
                ));
            }
        }
        if let Some(rh) = cmd_rustup_home {
            if !rh.as_os_str().is_empty()
                && !in_task_writable(rh)
                && (jail.host.rustup_home.as_os_str().is_empty()
                    || normalize_win_path(rh) != normalize_win_path(&jail.host.rustup_home))
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "task RUSTUP_HOME '{}' must point inside the task sandbox '{}' or match the worker's RUSTUP_HOME",
                        rh.display(),
                        jail.sandbox_dir.display()
                    ),
                ));
            }
        }

        let toolchain_sid = OwnedSid::oxideswarm_toolchain()?;

        // Requirement 4: No grants for program paths. A program must resolve inside the
        // sandbox, the worker's granted toolchain dirs, or system directories that
        // AppContainers can already read (`C:\Windows`, `C:\Program Files`, etc.).
        let prog_path = Path::new(program);
        if prog_path.is_absolute() {
            let in_sandbox = in_task_writable(prog_path);
            let in_cargo = !jail.host.cargo_home.as_os_str().is_empty()
                && path_starts_with_ci(prog_path, &jail.host.cargo_home);
            let in_rustup = !jail.host.rustup_home.as_os_str().is_empty()
                && path_starts_with_ci(prog_path, &jail.host.rustup_home);
            let in_msvc = discover_msvc_details().is_some_and(|(_, _, roots)| {
                roots.iter().any(|r| path_starts_with_ci(prog_path, r))
            });
            let sys_root = std::env::var_os("SystemRoot")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(r"C:\Windows"));
            let prog_files = std::env::var_os("ProgramFiles")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(r"C:\Program Files"));
            let prog_files_x86 = std::env::var_os("ProgramFiles(x86)")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(r"C:\Program Files (x86)"));
            let in_system = path_starts_with_ci(prog_path, &sys_root)
                || path_starts_with_ci(prog_path, &prog_files)
                || path_starts_with_ci(prog_path, &prog_files_x86);
            let in_extra_read = jail
                .host
                .extra_read_only_dirs
                .iter()
                .any(|d| !d.as_os_str().is_empty() && path_starts_with_ci(prog_path, d));
            let already_granted =
                dacl_already_grants_sid(prog_path, toolchain_sid.as_psid(), FILE_READ_ATTRIBUTES);
            if !(in_sandbox
                || in_cargo
                || in_rustup
                || in_msvc
                || in_system
                || in_extra_read
                || already_granted)
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "program '{}' lies outside the task sandbox, worker toolchain directories, and system directories readable by AppContainers",
                        prog_path.display()
                    ),
                ));
            }
        }

        let worker_prefix = worker_profile_prefix(jail.sandbox_base);
        let _ = acquire_worker_prefix_lock(&worker_prefix, true);
        let profile_name = format!("{}.{}", worker_prefix, run_token.simple());
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
            release_worker_prefix_guard(&worker_prefix);
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("CreateAppContainerProfile({profile_name}) failed: HRESULT 0x{hr:08x}"),
            ));
        }

        type SetupOut = (HANDLE, Vec<u16>, Vec<OwnedSid>, Vec<PathBuf>);
        let setup_res = (|| -> io::Result<SetupOut> {
            let user_sid = OwnedSid::current_user()?;
            let system_sid = OwnedSid::from_well_known(WinLocalSystemSid)?;
            let admins_sid = OwnedSid::from_well_known(WinBuiltinAdministratorsSid)?;

            let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
            let owner_only_entries = [
                explicit_access(user_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                explicit_access(system_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                explicit_access(admins_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
            ];

            // On `windows-latest`, `<LOCALAPPDATA>\Packages` has DACL
            // `D:(A;OICIID;FA;;;SY)(A;OICIID;FA;;;BA)(A;OICIID;FA;;;LA)` without `user_sid`.
            // Ensure `<LOCALAPPDATA>\Packages\<profile_name>` and its `AC` / `AC\Temp`
            // subdirectories (which `CreateAppContainerProfile` marks with a protected DACL
            // and `BasepAppContainerEnvironmentExtension` uses inside the AppContainer) grant
            // `user_sid` and this task's `app_container_sid` (never `BUILTIN\Users` or
            // `Everyone`) so Stage 1 and Stage 2 of `SepAccessCheck` both succeed.
            if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA").map(PathBuf::from) {
                let pkg_dir = local_app_data.join("Packages").join(&profile_name);
                if pkg_dir.exists() {
                    let pkg_entries = [
                        explicit_access(user_sid.as_psid(), FILE_ALL_ACCESS, GRANT_ACCESS, inherit),
                        explicit_access(app_container_sid, FILE_ALL_ACCESS, GRANT_ACCESS, inherit),
                    ];
                    let _ = merge_entries_on_path(&pkg_dir, &pkg_entries);
                    for sub in [pkg_dir.join("AC"), pkg_dir.join("AC").join("Temp")] {
                        if sub.exists() {
                            let _ = merge_entries_on_path_unprotected(&sub, &pkg_entries);
                        }
                    }
                }
            }

            {
                let _guard = PREPARE_LOCK
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let manifest = acl_grant_manifest_path(jail.host);
                let manifest_opt = if jail.host.user_home.as_os_str().is_empty()
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
                protect_sandbox_base(jail.sandbox_base, toolchain_sid.as_psid(), manifest_opt)?;
                if let Some(ws_root) = jail.workspace_root {
                    protect_sandbox_base(ws_root, toolchain_sid.as_psid(), manifest_opt)?;
                }
            }

            let mut workspace_dirs_to_restore = Vec::new();
            if jail.workspace_root.is_some() {
                let traverse_mask = FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
                if let Some(ws_dir) = jail.sandbox_dir.parent() {
                    if ws_dir.exists() && !is_reparse_point(ws_dir) {
                        let ws_parent_entries = [
                            owner_only_entries[0],
                            owner_only_entries[1],
                            owner_only_entries[2],
                            explicit_access(
                                toolchain_sid.as_psid(),
                                traverse_mask,
                                SET_ACCESS,
                                NO_INHERITANCE,
                            ),
                        ];
                        set_protected_dacl_on_path(ws_dir, &ws_parent_entries)?;
                        for meta_name in ["state.json", "lock"] {
                            let meta_path = ws_dir.join(meta_name);
                            if meta_path.exists() && !is_reparse_point(&meta_path) {
                                let _ = set_protected_dacl_on_path(&meta_path, &owner_only_entries);
                            }
                        }
                    }
                }
                const DELETE_ACCESS: u32 = 0x0001_0000;
                let task_child_mask = FILE_ALL_ACCESS & !(WRITE_DAC | WRITE_OWNER);
                let task_root_mask = FILE_ALL_ACCESS & !(WRITE_DAC | WRITE_OWNER | DELETE_ACCESS);
                let ws_dir_entries = [
                    owner_only_entries[0],
                    owner_only_entries[1],
                    owner_only_entries[2],
                    explicit_access(
                        app_container_sid,
                        task_child_mask,
                        SET_ACCESS,
                        OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE | INHERIT_ONLY_ACE,
                    ),
                    explicit_access(
                        app_container_sid,
                        task_root_mask,
                        GRANT_ACCESS,
                        NO_INHERITANCE,
                    ),
                ];
                set_protected_dacl_on_path(jail.sandbox_dir, &ws_dir_entries)?;
                workspace_dirs_to_restore.push(jail.sandbox_dir.to_path_buf());
                if let Some(extra) = jail.extra_writable_dir {
                    set_protected_dacl_on_path(extra, &ws_dir_entries)?;
                    workspace_dirs_to_restore.push(extra.to_path_buf());
                }
            } else {
                // Set a single inheritable protected DACL on `<sandbox_dir>` root granting access
                // only to CurrentUser, SYSTEM, Administrators, and this task's specific
                // `app_container_sid` (omitting `WRITE_DAC` and `WRITE_OWNER` so the task cannot
                // alter its own sandbox ACL). Win32 `SetNamedSecurityInfoW` propagates `(I)` to
                // staged children inside `<sandbox_dir>` automatically without walking (N6).
                let task_sandbox_mask = FILE_ALL_ACCESS & !(WRITE_DAC | WRITE_OWNER);
                let sandbox_entries = [
                    owner_only_entries[0],
                    owner_only_entries[1],
                    owner_only_entries[2],
                    explicit_access(app_container_sid, task_sandbox_mask, SET_ACCESS, inherit),
                ];
                set_protected_dacl_on_path(jail.sandbox_dir, &sandbox_entries)?;
            }

            // Create a dedicated per-task private Desktop on the worker's non-interactive
            // Window Station granting `DESKTOP_TASK_MASK` only to `CurrentUser` and this
            // task's `app_container_sid`. Fail the task if desktop creation fails (N7).
            let (desktop_handle, desktop_name_wide) =
                create_per_task_desktop(app_container_sid, run_token)?;

            let mut capability_sids = vec![toolchain_sid];
            if jail.level == IsolationLevel::Fs {
                let net_client = OwnedSid::from_well_known(WinCapabilityInternetClientSid)?;
                let net_private =
                    OwnedSid::from_well_known(WinCapabilityPrivateNetworkClientServerSid)?;
                capability_sids.push(net_client);
                capability_sids.push(net_private);
            }
            Ok((
                desktop_handle,
                desktop_name_wide,
                capability_sids,
                workspace_dirs_to_restore,
            ))
        })();

        let (desktop_handle, desktop_name_wide, capability_sids, workspace_dirs_to_restore) =
            match setup_res {
                Ok(v) => v,
                Err(e) => {
                    unsafe {
                        FreeSid(app_container_sid);
                        let _ = DeleteAppContainerProfile(wide_profile_name.as_ptr());
                    }
                    release_worker_prefix_guard(&worker_prefix);
                    return Err(e);
                }
            };

        let mut capability_attrs: Vec<SID_AND_ATTRIBUTES> = capability_sids
            .iter()
            .map(|sid| SID_AND_ATTRIBUTES {
                Sid: sid.as_psid(),
                Attributes: SE_GROUP_ENABLED as u32,
            })
            .collect();

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
            worker_prefix,
            wide_profile_name,
            app_container_sid,
            desktop_handle,
            desktop_name_wide,
            _capability_sids: capability_sids,
            _capability_attrs: capability_attrs,
            security_capabilities,
            workspace_dirs_to_restore,
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
            &self.desktop_name_wide,
            creation_flags,
        )
    }

    pub fn cleanup(&mut self) {
        if self.cleaned_up {
            return;
        }
        self.cleaned_up = true;
        if !self.workspace_dirs_to_restore.is_empty() {
            match (
                OwnedSid::current_user(),
                OwnedSid::from_well_known(WinLocalSystemSid),
                OwnedSid::from_well_known(WinBuiltinAdministratorsSid),
            ) {
                (Ok(user_sid), Ok(system_sid), Ok(admins_sid)) => {
                    let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
                    let owner_only_entries = [
                        explicit_access(user_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                        explicit_access(system_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                        explicit_access(admins_sid.as_psid(), FILE_ALL_ACCESS, SET_ACCESS, inherit),
                    ];
                    for dir in self.workspace_dirs_to_restore.drain(..) {
                        if dir.exists() && !is_reparse_point(&dir) {
                            if let Err(err) = set_protected_dacl_on_path(&dir, &owner_only_entries)
                            {
                                tracing::warn!(
                                    path = %dir.display(),
                                    error = %err,
                                    "Failed to restore owner-only DACL on workspace directory during AppContainer cleanup"
                                );
                            }
                        }
                    }
                }
                (u_res, s_res, a_res) => {
                    for dir in self.workspace_dirs_to_restore.drain(..) {
                        tracing::warn!(
                            path = %dir.display(),
                            user_sid_err = ?u_res.as_ref().err(),
                            system_sid_err = ?s_res.as_ref().err(),
                            admins_sid_err = ?a_res.as_ref().err(),
                            "Failed to resolve owner-only SIDs to restore workspace directory DACL during AppContainer cleanup"
                        );
                    }
                }
            }
        }
        unsafe {
            if !self.desktop_handle.is_null() {
                let _ = CloseDesktop(self.desktop_handle);
                self.desktop_handle = std::ptr::null_mut();
            }
            if !self.app_container_sid.is_null() {
                FreeSid(self.app_container_sid);
                self.app_container_sid = std::ptr::null_mut();
            }
            let hr = DeleteAppContainerProfile(self.wide_profile_name.as_ptr());
            if hr < 0 {
                let wide_name = std::mem::take(&mut self.wide_profile_name);
                let prefix = std::mem::take(&mut self.worker_prefix);
                std::thread::spawn(move || {
                    // Retry up to 50 × 50 ms (~2.5 s) matching Design 0033 D4.1 if kernel
                    // handles from a dying child process are still closing.
                    for _ in 0..50 {
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        if DeleteAppContainerProfile(wide_name.as_ptr()) >= 0 {
                            break;
                        }
                    }
                    release_worker_prefix_guard(&prefix);
                });
            } else {
                release_worker_prefix_guard(&self.worker_prefix);
            }
        }
    }
}

impl Drop for WindowsAppContainerGuard {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Start-up self-check on Windows with explicit worker [`HostDirs`] and `protected_files`:
/// 1. Validates that no `protected_files` entry lies inside a granted toolchain or system
///    directory or carries an AppContainer read ACE.
/// 2. Sweeps any leftover `oxideswarm.task.<worker8>.*` AppContainer profiles for this
///    `sandbox_base`.
/// 3. Prepares the worker's own toolchain ACLs once (`oxideswarm.toolchain` capability SID).
/// 4. Runs a probe command inside a temporary AppContainer at `level` verifying that:
///    - the task can write inside its sandbox,
///    - the task cannot write into `sandbox_base`, and
///    - the task cannot open or read any existing file in `protected_files` (or
///      `<host.user_home>/.oxideswarm/cluster.secret`) (N5).
pub(super) async fn probe_with_config(
    level: IsolationLevel,
    sandbox_base: &Path,
    host: &HostDirs,
    protected_files: &[PathBuf],
) -> Result<(), String> {
    let fail =
        |what: String| format!("--isolation {level} cannot be enforced on this host: {what}");

    let host_clone = host.clone();
    let protected_clone = protected_files.to_vec();
    let base_clone = sandbox_base.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let _ = sweep_leftover_appcontainer_profiles_for_base(&base_clone);
        prepare_worker_toolchains(&host_clone, &protected_clone, Some(&base_clone))
    })
    .await
    .map_err(|e| fail(format!("toolchain preparation task panicked: {e}")))?
    .map_err(fail)?;

    let mut existing_protected: Vec<PathBuf> = Vec::new();
    for pf in protected_files {
        if !pf.as_os_str().is_empty() && pf.exists() {
            let norm_pf = if !is_reparse_point(pf) {
                normalize_win_path(pf)
            } else {
                pf.clone()
            };
            if !existing_protected.contains(&norm_pf) {
                existing_protected.push(norm_pf);
            }
        }
    }
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
            let ox_secret = ox_home.join(rel);
            if ox_secret.exists() {
                let norm_ox = if !is_reparse_point(&ox_secret) {
                    normalize_win_path(&ox_secret)
                } else {
                    ox_secret
                };
                if !existing_protected.contains(&norm_ox) {
                    existing_protected.push(norm_ox);
                }
            }
        }
    }
    if !host.user_home.as_os_str().is_empty() {
        for rel in [
            "cluster.secret",
            "node.key",
            "p2p_secret.key",
            "master_noise.key",
        ] {
            let default_secret = host.user_home.join(".oxideswarm").join(rel);
            if default_secret.exists() {
                let norm_def = if !is_reparse_point(&default_secret) {
                    normalize_win_path(&default_secret)
                } else {
                    default_secret
                };
                if !existing_protected.contains(&norm_def) {
                    existing_protected.push(norm_def);
                }
            }
        }
    }

    let probe_dir = sandbox_base.join(format!(".isolation-probe-{}", std::process::id()));
    std::fs::create_dir_all(&probe_dir).map_err(|e| {
        format!(
            "--isolation {level}: cannot create the probe directory {}: {e}",
            probe_dir.display()
        )
    })?;
    let norm_base = if !is_reparse_point(sandbox_base) {
        normalize_win_path(sandbox_base)
    } else {
        sandbox_base.to_path_buf()
    };
    let norm_probe_dir = if !is_reparse_point(&probe_dir) {
        normalize_win_path(&probe_dir)
    } else {
        probe_dir.clone()
    };
    let outside_name = format!(".isolation-outside-{}", std::process::id());
    let outside_file = norm_base.join(&outside_name);
    let outcome = async {
        let empty_host =
            HostDirs::from_home(PathBuf::new(), Some(PathBuf::new()), Some(PathBuf::new()));
        let jail = Jail {
            level,
            host: &empty_host,
            protected_files: &[],
            sandbox_base: &norm_base,
            sandbox_dir: &norm_probe_dir,
            extra_writable_dir: None,
            workspace_root: None,
        };
        let comspec = std::env::var("COMSPEC")
            .unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".to_string());
        let mut cmd_line = format!(
            "echo ok > inside || exit 2 & (echo no > ..\\{outside_name} 2>probe_err && exit 3)"
        );
        for idx in 0..existing_protected.len() {
            let code = 10 + idx;
            cmd_line.push_str(&format!(
                " & ((ver >probe_sink) < %OXIDE_PROBE_PROTECTED_{idx}% 2>probe_err && exit {code})"
            ));
        }
        cmd_line.push_str(" & exit 0");
        let mut cmd = tokio::process::Command::new(&comspec);
        cmd.args(["/C", &cmd_line]);
        cmd.current_dir(&norm_probe_dir);
        cmd.env_clear();
        for (name, value) in super::inherited_environment(std::env::vars_os()) {
            cmd.env(name, value);
        }
        for (idx, pf) in existing_protected.iter().enumerate() {
            cmd.env(format!("OXIDE_PROBE_PROTECTED_{idx}"), pf.as_os_str());
        }
        let probe_token = uuid::Uuid::new_v4();
        let guard = WindowsAppContainerGuard::create(probe_token, &jail, &cmd)
            .map_err(|e| fail(format!("AppContainer setup failed: {e}")))?;
        let mut child = guard
            .spawn_in_appcontainer(&mut cmd, 0)
            .map_err(|e| fail(format!("could not spawn AppContainer probe: {e}")))?;
        let status = match tokio::time::timeout(
            std::time::Duration::from_secs(15),
            child.wait(),
        )
        .await
        {
            Ok(res) => res
                .map_err(|e| fail(format!("waiting for AppContainer probe failed: {e}")))?,
            Err(_) => {
                let _ = child.start_kill();
                drop(guard);
                return Err(fail(
                    "AppContainer startup probe timed out after 15s".to_string(),
                ));
            }
        };
        drop(guard);
        match status.code() {
            Some(0) if probe_dir.join("inside").exists() && !outside_file.exists() => Ok(()),
            Some(3) => Err(fail(
                "the probe wrote outside its sandbox, so the AppContainer DACL was not enforced"
                    .to_string(),
            )),
            Some(code)
                if code >= 10 && (code as usize - 10) < existing_protected.len() =>
            {
                let leaked = &existing_protected[code as usize - 10];
                Err(fail(format!(
                    "protected secret/config file '{}' is readable from inside the AppContainer; revoke overly broad ACLs (such as S-1-15-2-1) or move the file outside system directories",
                    leaked.display()
                )))
            }
            _ => Err(fail(format!("the probe failed ({status})"))),
        }
    }
    .await;
    let _ = std::fs::remove_file(&outside_file);
    let _ = std::fs::remove_dir_all(&probe_dir);
    outcome
}

#[cfg(test)]
pub(crate) use spawn::lua_test_hook;
