//! Sandbox directory management, path traversal isolation, and POSIX permissions.

use std::path::{Component, Path, PathBuf};
use tracing::{debug, warn};

use rusty_grid_core::task::{
    StagedFile, TaskId, MAX_AGGREGATE_STAGED_BYTES, MAX_STAGED_FILE_BYTES,
};

/// Windows reserved DOS device names that cannot be created or accessed as files.
pub const WINDOWS_RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Normalizes a relative path string into a canonical forward-slash separated path,
/// enforcing strict isolation boundaries:
/// - Rejects path traversal (`..`)
/// - Rejects absolute paths (`/`, `\`, drive letters `C:`)
/// - Rejects colons / alternate data streams (`:`)
/// - Rejects null bytes (`\0`)
/// - Rejects Windows reserved device names (`CON`, `PRN`, `AUX`, `NUL`, `COM1..9`, `LPT1..9`)
/// - Normalizes Windows backslashes `\` to `/`
pub fn normalize_relative_path_str(raw: &str) -> Result<String, SandboxError> {
    let trimmed_raw = raw.trim();
    if trimmed_raw.is_empty() {
        return Err(SandboxError::PathTraversal {
            task_id: TaskId::default(),
            path: raw.to_string(),
            reason: "Empty path is prohibited".to_string(),
        });
    }

    if raw.contains('\0') {
        return Err(SandboxError::PathTraversal {
            task_id: TaskId::default(),
            path: raw.to_string(),
            reason: "Path contains illegal null byte".to_string(),
        });
    }

    // Prohibit leading slashes (absolute paths on Unix or Windows UNC)
    if trimmed_raw.starts_with('/') || trimmed_raw.starts_with('\\') {
        return Err(SandboxError::PathTraversal {
            task_id: TaskId::default(),
            path: raw.to_string(),
            reason: "Absolute paths with leading slash are prohibited".to_string(),
        });
    }

    // Prohibit Windows drive letters, alternate data streams (ADS), or colons
    if raw.contains(':') {
        return Err(SandboxError::PathTraversal {
            task_id: TaskId::default(),
            path: raw.to_string(),
            reason: "Colons and drive prefixes are prohibited in relative paths".to_string(),
        });
    }

    // Normalize backslashes to forward slashes
    let normalized = trimmed_raw.replace('\\', "/");

    let mut segments = Vec::new();
    for segment in normalized.split('/') {
        let seg_trimmed = segment.trim();
        if seg_trimmed.is_empty() || seg_trimmed == "." {
            continue;
        }
        if seg_trimmed == ".." {
            return Err(SandboxError::PathTraversal {
                task_id: TaskId::default(),
                path: raw.to_string(),
                reason: "Path traversal '..' component detected".to_string(),
            });
        }

        // Check for segments consisting solely of dots and/or spaces (e.g. "...", "....")
        let cleaned = seg_trimmed.trim_end_matches([' ', '.']);
        if cleaned.is_empty() {
            return Err(SandboxError::PathTraversal {
                task_id: TaskId::default(),
                path: raw.to_string(),
                reason: format!(
                    "Illegal path component '{seg_trimmed}' consisting only of dots/spaces"
                ),
            });
        }

        // Check for Windows reserved DOS device names:
        // Strip trailing spaces and dots which Windows Win32 API strips
        let stem = cleaned
            .split('.')
            .next()
            .unwrap_or(cleaned)
            .trim_end_matches([' ', '.'])
            .to_ascii_uppercase();
        if WINDOWS_RESERVED_NAMES.contains(&stem.as_str()) {
            return Err(SandboxError::PathTraversal {
                task_id: TaskId::default(),
                path: raw.to_string(),
                reason: format!("Windows reserved device name '{seg_trimmed}' is prohibited"),
            });
        }

        segments.push(seg_trimmed);
    }

    if segments.is_empty() {
        return Err(SandboxError::PathTraversal {
            task_id: TaskId::default(),
            path: raw.to_string(),
            reason: "Path resolves to empty root".to_string(),
        });
    }

    Ok(segments.join("/"))
}

/// Validates that a requested path does not escape the sandbox root directory.
pub fn sanitize_relative_path(base_dir: &Path, rel_path: &Path) -> Result<PathBuf, SandboxError> {
    if rel_path.is_absolute() {
        return Err(SandboxError::PathTraversal {
            task_id: TaskId::default(),
            path: rel_path.display().to_string(),
            reason: "Absolute paths are prohibited in sandbox".to_string(),
        });
    }

    for comp in rel_path.components() {
        match comp {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(SandboxError::PathTraversal {
                    task_id: TaskId::default(),
                    path: rel_path.display().to_string(),
                    reason: "Path traversal '..' component detected".to_string(),
                });
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(SandboxError::PathTraversal {
                    task_id: TaskId::default(),
                    path: rel_path.display().to_string(),
                    reason: "Root or drive prefix detected in relative path".to_string(),
                });
            }
        }
    }

    let raw_str = rel_path
        .to_str()
        .ok_or_else(|| SandboxError::PathTraversal {
            task_id: TaskId::default(),
            path: rel_path.display().to_string(),
            reason: "Path contains non-UTF-8 characters".to_string(),
        })?;

    let normalized = normalize_relative_path_str(raw_str)?;
    Ok(base_dir.join(normalized))
}

/// Environment variable that names the sandbox base directory itself (task sandboxes are created
/// directly inside it).
pub const SANDBOX_BASE_DIR_ENV: &str = "RUSTY_GRID_SANDBOX_BASE_DIR";

/// Configuration options for worker sandbox environment.
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    /// Base directory where per-task sandbox folders are placed.
    pub base_dir: PathBuf,
    /// If true, sandbox directories are preserved after task completion for debugging.
    pub keep_sandboxes: bool,
    /// The `<temp>/rusty_grid-<euid>` directory that holds the default base on Unix and must stay
    /// private to this user (design 0021). `None` for a directory the operator named on Unix; on
    /// Windows `checked_private_root` uses `base_dir` itself.
    #[cfg_attr(windows, allow(dead_code))]
    private_root: Option<PathBuf>,
}

/// Where task sandboxes go when nothing names a directory explicitly, in order of precedence:
///
/// 1. `RUSTY_GRID_SANDBOX_BASE_DIR` (used as the base directory itself),
/// 2. `<TMPDIR>/<root>/sandboxes`,
/// 3. `<platform temp dir>/<root>/sandboxes`,
///
/// where `<root>` is `rusty_grid-<euid>` on Unix (one per user, see design 0021) and `rusty_grid`
/// on other systems. An explicit directory (`SandboxConfig::new`,
/// `WorkerConfig::with_sandbox_base_dir`, the `--sandbox-base-dir` flag) outranks all three. `env`
/// looks a variable up and `platform_temp` is the platform's temp dir; both are parameters so the
/// order can be tested without touching the process environment. Empty variables count as unset.
pub fn resolve_default_base_dir(
    env: impl Fn(&str) -> Option<String>,
    platform_temp: PathBuf,
) -> PathBuf {
    default_base(env, platform_temp).0
}

/// Name of the directory inside the temp dir that holds the default base. On Unix it carries the
/// effective user id, so the users of one host never share it.
fn default_root_name() -> String {
    #[cfg(unix)]
    {
        format!("rusty_grid-{}", current_euid())
    }
    #[cfg(not(unix))]
    {
        "rusty_grid".to_string()
    }
}

/// [`resolve_default_base_dir`] plus the private root to check: `<temp>/rusty_grid-<euid>` on
/// Unix, `None` on other systems and when `RUSTY_GRID_SANDBOX_BASE_DIR` names the base.
fn default_base(
    env: impl Fn(&str) -> Option<String>,
    platform_temp: PathBuf,
) -> (PathBuf, Option<PathBuf>) {
    let non_empty = |name: &str| env(name).filter(|v| !v.trim().is_empty());
    if let Some(dir) = non_empty(SANDBOX_BASE_DIR_ENV) {
        return (PathBuf::from(dir), None);
    }
    let temp = non_empty("TMPDIR")
        .map(PathBuf::from)
        .unwrap_or(platform_temp);
    let root = temp.join(default_root_name());
    (root.join("sandboxes"), cfg!(unix).then_some(root))
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self::from_env(|name| std::env::var(name).ok(), std::env::temp_dir())
    }
}

impl SandboxConfig {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            keep_sandboxes: false,
            private_root: None,
        }
    }

    /// The default configuration as if `TMPDIR` named `temp_dir`: base
    /// `<temp_dir>/rusty_grid-<euid>/sandboxes` on Unix, with its private root (design 0021). For
    /// callers and tests that choose the temp dir themselves.
    pub fn default_under(temp_dir: impl Into<PathBuf>) -> Self {
        Self::from_env(|_| None, temp_dir.into())
    }

    /// The default configuration for an environment lookup and a platform temp dir (see
    /// [`resolve_default_base_dir`]); parameters so tests need not touch the process environment.
    pub(crate) fn from_env(env: impl Fn(&str) -> Option<String>, platform_temp: PathBuf) -> Self {
        let (base_dir, private_root) = default_base(env, platform_temp);
        Self {
            base_dir,
            keep_sandboxes: false,
            private_root,
        }
    }

    pub fn with_keep_sandboxes(mut self, keep: bool) -> Self {
        self.keep_sandboxes = keep;
        self
    }

    /// Checks that task sandboxes can actually be created: creates the base directory if needed
    /// and writes (then removes) a probe file in it. Call it when the worker starts, so a base
    /// directory the process may not write to (on Android the default `/data/local/tmp` is closed
    /// to apps) is one clear error instead of a failure of every process task.
    ///
    /// For the default base on Unix it first creates the private root (`<temp>/rusty_grid-<euid>`,
    /// mode 700) and refuses one another local user could change (`SandboxError::BaseDirUnsafe`,
    /// design 0021). On Windows `ensure_private_root` checks or privately creates `base_dir`
    /// itself.
    ///
    /// Also removes legacy `<base_dir>/cache/objects` and `<base_dir>/cache/tmp` directories left
    /// behind by older worker builds, logging a warning if removal fails and leaving
    /// `<base_dir>/cache/blobs` untouched.
    pub async fn ensure_usable(&self) -> Result<(), SandboxError> {
        self.ensure_private_root().await?;
        let unusable = |source: std::io::Error| SandboxError::BaseDirUnusable {
            path: self.base_dir.clone(),
            source,
        };
        #[cfg(not(windows))]
        tokio::fs::create_dir_all(&self.base_dir)
            .await
            .map_err(unusable)?;
        let probe = self.base_dir.join(format!(
            ".write_probe_{}_{}",
            std::process::id(),
            TaskId::new()
        ));
        tokio::fs::write(&probe, b"probe").await.map_err(unusable)?;
        let _ = tokio::fs::remove_file(&probe).await;
        self.remove_legacy_staging_cache().await;
        if !self.keep_sandboxes {
            self.sweep_leftover_sandboxes().await;
        }
        Ok(())
    }

    /// Creates and checks the private root of the default base on Unix while `base_dir` lies
    /// inside it (doing nothing for an explicit directory on Unix), and on Windows creates or
    /// checks `base_dir` itself (see `ensure_private_dir`). Call it before anything touches the
    /// base: `WorkerClient` does when a task is assigned and again before it fetches the task's
    /// input files, and `Sandbox::create` does too (design 0021).
    pub async fn ensure_private_root(&self) -> Result<(), SandboxError> {
        let Some(root) = self.checked_private_root().map(Path::to_path_buf) else {
            return Ok(());
        };
        let path = root.clone();
        tokio::task::spawn_blocking(move || ensure_private_dir(&root))
            .await
            .unwrap_or_else(|e| {
                Err(SandboxError::BaseDirUnusable {
                    path,
                    source: std::io::Error::other(e.to_string()),
                })
            })
    }

    /// `ensure_private_root` without a runtime, for a caller that must answer synchronously (the
    /// Android bridge's start check, or lazy blob-cache creation).
    pub fn ensure_private_root_blocking(&self) -> Result<(), SandboxError> {
        match self.checked_private_root() {
            Some(root) => ensure_private_dir(root),
            None => Ok(()),
        }
    }

    fn checked_private_root(&self) -> Option<&Path> {
        #[cfg(windows)]
        {
            Some(&self.base_dir)
        }
        #[cfg(not(windows))]
        {
            self.private_root
                .as_deref()
                .filter(|root| self.base_dir.starts_with(root))
        }
    }

    /// Removes legacy `<base_dir>/cache/objects` and `<base_dir>/cache/tmp` directories left
    /// behind by older worker builds.
    ///
    /// Refuses to remove anything unless `<base_dir>` and `<base_dir>/cache` are real directories
    /// (not symbolic links or junctions, and on Unix owned by the current effective user), and
    /// deletes only the direct `objects` and `tmp` subdirectories of `<base_dir>/cache` without
    /// following links. Logs a warning on failure and leaves `<base_dir>/cache/blobs` alone.
    pub async fn remove_legacy_staging_cache(&self) {
        match tokio::fs::symlink_metadata(&self.base_dir).await {
            Ok(meta) if is_real_owned_dir(&meta) => {}
            Ok(meta) => {
                warn!(
                    path = %self.base_dir.display(),
                    is_symlink = meta.file_type().is_symlink(),
                    is_dir = meta.file_type().is_dir(),
                    "Skipping legacy staging cache cleanup: sandbox base is not a real directory owned by the current user"
                );
                return;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                warn!(
                    path = %self.base_dir.display(),
                    error = %e,
                    "Failed to inspect sandbox base directory for legacy staging cache cleanup"
                );
                return;
            }
        }

        let cache_dir = self.base_dir.join("cache");
        match tokio::fs::symlink_metadata(&cache_dir).await {
            Ok(meta) if is_real_owned_dir(&meta) => {}
            Ok(meta) => {
                warn!(
                    path = %cache_dir.display(),
                    is_symlink = meta.file_type().is_symlink(),
                    is_dir = meta.file_type().is_dir(),
                    "Skipping legacy staging cache cleanup: cache path is not a real directory owned by the current user"
                );
                return;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                warn!(
                    path = %cache_dir.display(),
                    error = %e,
                    "Failed to inspect legacy staging cache directory"
                );
                return;
            }
        }

        for name in ["objects", "tmp"] {
            let path = cache_dir.join(name);
            match tokio::fs::symlink_metadata(&path).await {
                Ok(meta) if is_real_owned_dir(&meta) => {
                    if let Err(e) = tokio::fs::remove_dir_all(&path).await {
                        warn!(
                            path = %path.display(),
                            error = %e,
                            "Failed to remove legacy staging cache directory"
                        );
                    } else {
                        debug!(
                            path = %path.display(),
                            "Removed legacy staging cache directory"
                        );
                    }
                }
                Ok(meta) => {
                    warn!(
                        path = %path.display(),
                        is_symlink = meta.file_type().is_symlink(),
                        is_dir = meta.file_type().is_dir(),
                        "Skipping legacy staging cache child: not a real directory owned by the current user"
                    );
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to inspect legacy staging cache child"
                    );
                }
            }
        }
    }

    /// Sweeps unlocked leftover `<base_dir>/<uuid>` task sandbox directories and orphan
    /// `<base_dir>/.locks/<uuid>.lock` files left behind by crashed worker processes or earlier
    /// `--keep-sandboxes` runs (design 0044 D5).
    ///
    /// Lock files live in `<base_dir>/.locks/<task_id>.lock` outside the task-writable directory
    /// `<base_dir>/<task_id>/`, so a running task that deletes everything inside its own sandbox
    /// still holds its advisory lock and is never swept by a second worker sharing `base_dir`.
    pub async fn sweep_leftover_sandboxes(&self) {
        match tokio::fs::symlink_metadata(&self.base_dir).await {
            Ok(meta) if is_real_owned_dir(&meta) => {}
            Ok(meta) => {
                warn!(
                    path = %self.base_dir.display(),
                    is_symlink = meta.file_type().is_symlink(),
                    is_dir = meta.file_type().is_dir(),
                    "Skipping leftover sandbox sweep: sandbox base is not a real directory owned by the current user"
                );
                return;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                warn!(
                    path = %self.base_dir.display(),
                    error = %e,
                    "Failed to inspect sandbox base directory for leftover sandbox sweep"
                );
                return;
            }
        }

        let mut swept_sandboxes: usize = 0;

        let mut dir_entries = match tokio::fs::read_dir(&self.base_dir).await {
            Ok(rd) => rd,
            Err(e) => {
                warn!(
                    path = %self.base_dir.display(),
                    error = %e,
                    "Failed to read sandbox base directory during leftover sandbox sweep"
                );
                return;
            }
        };

        while let Ok(Some(entry)) = dir_entries.next_entry().await {
            let file_name = entry.file_name();
            let Some(name_str) = file_name.to_str() else {
                continue;
            };
            let Ok(uuid) = uuid::Uuid::parse_str(name_str) else {
                continue;
            };

            let entry_path = entry.path();
            match tokio::fs::symlink_metadata(&entry_path).await {
                Ok(meta) if is_real_owned_dir(&meta) => {}
                _ => continue,
            }

            let task_id = TaskId::from_uuid(uuid);
            match open_and_try_lock_task(&self.base_dir, task_id) {
                Ok(Some((lock_path, lock_file))) => {
                    if let Err(e) = tokio::fs::remove_dir_all(&entry_path).await {
                        warn!(
                            task_id = %task_id,
                            path = %entry_path.display(),
                            error = %e,
                            "Failed to remove leftover task sandbox directory"
                        );
                    } else {
                        swept_sandboxes += 1;
                    }
                    let _ = tokio::fs::remove_file(&lock_path).await;
                    drop(lock_file);
                }
                Ok(None) => {
                    // Held by a live worker / Sandbox; leave the sandbox directory untouched.
                }
                Err(e) => {
                    warn!(
                        task_id = %task_id,
                        path = %entry_path.display(),
                        error = %e,
                        "Skipping leftover sandbox directory: failed to inspect or lock .locks entry"
                    );
                }
            }
        }

        let locks_dir = self.base_dir.join(".locks");
        match tokio::fs::symlink_metadata(&locks_dir).await {
            Ok(meta) if is_real_owned_dir(&meta) => {}
            Ok(meta) => {
                warn!(
                    path = %locks_dir.display(),
                    is_symlink = meta.file_type().is_symlink(),
                    is_dir = meta.file_type().is_dir(),
                    "Skipping orphan lock sweep: .locks path is not a real directory owned by the current user"
                );
                return;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if swept_sandboxes > 0 {
                    tracing::info!(
                        swept_sandboxes,
                        base_dir = %self.base_dir.display(),
                        "Swept leftover task sandboxes at worker startup"
                    );
                }
                return;
            }
            Err(e) => {
                warn!(
                    path = %locks_dir.display(),
                    error = %e,
                    "Failed to inspect .locks directory during leftover sandbox sweep"
                );
                return;
            }
        }

        if let Ok(mut lock_entries) = tokio::fs::read_dir(&locks_dir).await {
            while let Ok(Some(entry)) = lock_entries.next_entry().await {
                let file_name = entry.file_name();
                let Some(name_str) = file_name.to_str() else {
                    continue;
                };
                let Some(stem) = name_str.strip_suffix(".lock") else {
                    continue;
                };
                let Ok(uuid) = uuid::Uuid::parse_str(stem) else {
                    continue;
                };

                let sandbox_dir = self.base_dir.join(uuid.to_string());
                match tokio::fs::symlink_metadata(&sandbox_dir).await {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    _ => continue,
                }

                let lock_path = entry.path();
                match tokio::fs::symlink_metadata(&lock_path).await {
                    Ok(meta) if is_real_owned_file(&meta) => {}
                    _ => continue,
                }

                if let Ok(Some((locked_path, lock_file))) =
                    open_and_try_lock_task(&self.base_dir, TaskId::from_uuid(uuid))
                {
                    // Re-check that the sandbox directory was not created concurrently before
                    // unlinking the orphan lock file.
                    if matches!(
                        tokio::fs::symlink_metadata(&sandbox_dir).await,
                        Err(ref e) if e.kind() == std::io::ErrorKind::NotFound
                    ) {
                        let _ = tokio::fs::remove_file(&locked_path).await;
                    }
                    drop(lock_file);
                }
            }
        }

        if swept_sandboxes > 0 {
            tracing::info!(
                swept_sandboxes,
                base_dir = %self.base_dir.display(),
                "Swept leftover task sandboxes at worker startup"
            );
        }
    }
}

/// Errors occurring during sandbox setup, isolation, file I/O, or cleanup.
#[derive(Debug, thiserror::Error)]
pub enum SandboxError {
    #[error("I/O error during sandbox operation for task {task_id}: {source}")]
    Io {
        task_id: TaskId,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "Path traversal security violation in task {task_id}: illegal path '{path}': {reason}"
    )]
    PathTraversal {
        task_id: TaskId,
        path: String,
        reason: String,
    },

    #[error("Permission error for task {task_id} on '{path}': {source}")]
    Permission {
        task_id: TaskId,
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Sandbox directory already exists for task {task_id}: {path}")]
    AlreadyExists { task_id: TaskId, path: PathBuf },

    #[error(
        "The sandbox base directory '{}' is not writable: {source}. Task sandboxes cannot be \
         created there; point the worker at a writable directory with --sandbox-base-dir, \
         RUSTY_GRID_SANDBOX_BASE_DIR or TMPDIR (on Android: the app's cache directory)",
        path.display()
    )]
    BaseDirUnusable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "Task sandboxes cannot use '{}' safely: {reason}, so another local user could swap task \
         files there. {remedy}, or point the worker at a private directory with \
         --sandbox-base-dir or RUSTY_GRID_SANDBOX_BASE_DIR",
        path.display()
    )]
    BaseDirUnsafe {
        path: PathBuf,
        reason: String,
        remedy: String,
    },
}

/// Effective user id of the worker process.
#[cfg(unix)]
fn current_euid() -> u32 {
    // SAFETY: `geteuid()` is thread-safe and has no preconditions.
    unsafe { libc::geteuid() }
}

/// Creates `root` with mode 700 if it is missing and refuses it, as
/// `SandboxError::BaseDirUnsafe`, when another local user could change what is inside it
/// (design 0021): see `parent_refusal` and `root_refusal`. The mode of an existing `root` is never
/// changed, because another user may already have put links into one that was open.
///
/// On Windows `root` is checked without following junctions or symbolic links:
/// - a missing `root` is created with the protected owner-only DACL of SEC-9 (current user,
///   `SYSTEM` and `Administrators`) and verified after creation;
/// - any reparse point (junction, symlink), non-directory, volume without `FILE_PERSISTENT_ACLS`
///   (`FAT`, `exFAT`), or directory owned by an account outside `{current user, SYSTEM,
///   Administrators}` is refused with `SandboxError::BaseDirUnsafe` (including when another
///   creator wins the `ERROR_ALREADY_EXISTS` race);
/// - an existing real directory on a persistent-ACL volume owned by `{current user, SYSTEM,
///   Administrators}` is kept so in-place upgrades keep starting, logging a `WARN` with a
///   locale-independent SID `icacls` command when any other trustee has sensitive access (or when
///   the DACL cannot be read).
pub(crate) fn ensure_private_dir(root: &Path) -> Result<(), SandboxError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;

        let unusable = |source: std::io::Error| SandboxError::BaseDirUnusable {
            path: root.to_path_buf(),
            source,
        };
        let euid = current_euid();

        if let Some(parent) = root.parent() {
            std::fs::create_dir_all(parent).map_err(unusable)?;
            let meta = std::fs::metadata(parent).map_err(unusable)?;
            if let Some(reason) = parent_refusal(&meta, euid) {
                return Err(SandboxError::BaseDirUnsafe {
                    path: parent.to_path_buf(),
                    reason,
                    remedy: "Set TMPDIR to a directory only this user can write, or add the \
                             sticky bit to this one"
                        .to_string(),
                });
            }
        }
        match std::fs::DirBuilder::new().mode(0o700).create(root) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(unusable(e)),
        }
        let meta = std::fs::symlink_metadata(root).map_err(unusable)?;
        match root_refusal(&meta, euid) {
            Some(reason) => Err(SandboxError::BaseDirUnsafe {
                path: root.to_path_buf(),
                reason,
                remedy: "Remove it (this may need its owner or root)".to_string(),
            }),
            None => Ok(()),
        }
    }
    #[cfg(windows)]
    {
        use rusty_grid_core::private_file::{
            create_or_open_private_dir_all, current_user_sid, inspect_dir_security_no_follow,
        };
        ensure_private_dir_windows_with(
            root,
            current_user_sid,
            create_or_open_private_dir_all,
            inspect_dir_security_no_follow,
        )
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = root;
        Ok(())
    }
}

#[cfg(windows)]
fn ensure_private_dir_windows_with(
    root: &Path,
    current_sid: impl FnOnce() -> std::io::Result<String>,
    create_or_open: impl FnOnce(
        &Path,
    )
        -> std::io::Result<rusty_grid_core::private_file::PrivateDirOutcome>,
    inspect: impl FnOnce(&Path) -> std::io::Result<rusty_grid_core::private_file::DirSecuritySnapshot>,
) -> Result<(), SandboxError> {
    use rusty_grid_core::private_file::{
        dir_open_grants, is_trusted_owner_sid, open_dir_acl_warning, PrivateDirOutcome,
    };

    let unusable = |source: std::io::Error| SandboxError::BaseDirUnusable {
        path: root.to_path_buf(),
        source,
    };
    let remove_remedy = "Remove it (this may need its owner or an Administrator)".to_string();

    let user_sid = current_sid().map_err(unusable)?;
    let outcome = create_or_open(root).map_err(unusable)?;
    let snap = inspect(root).map_err(unusable)?;

    if snap.is_reparse_point {
        return Err(SandboxError::BaseDirUnsafe {
            path: root.to_path_buf(),
            reason: "it is a link or reparse point".to_string(),
            remedy: remove_remedy,
        });
    }
    if !snap.is_dir {
        return Err(SandboxError::BaseDirUnsafe {
            path: root.to_path_buf(),
            reason: "it is not a directory".to_string(),
            remedy: remove_remedy,
        });
    }
    if !snap.persistent_acls {
        return Err(SandboxError::BaseDirUnsafe {
            path: root.to_path_buf(),
            reason: "its volume does not support persistent ACLs (such as FAT or exFAT)"
                .to_string(),
            remedy: "Place the directory on an NTFS volume".to_string(),
        });
    }

    match &snap.owner_sid {
        Ok(owner) if is_trusted_owner_sid(owner, &user_sid) => {}
        Ok(owner) => {
            return Err(SandboxError::BaseDirUnsafe {
                path: root.to_path_buf(),
                reason: format!(
                    "it belongs to {owner}, not to {user_sid}, SYSTEM or Administrators"
                ),
                remedy: remove_remedy,
            });
        }
        Err(e) => {
            return Err(SandboxError::BaseDirUnsafe {
                path: root.to_path_buf(),
                reason: format!("its owner could not be verified ({e})"),
                remedy: remove_remedy,
            });
        }
    }

    match outcome {
        PrivateDirOutcome::Created => match snap.dacl {
            Ok((aces, true)) if dir_open_grants(&aces, &user_sid).is_empty() => Ok(()),
            Ok((aces, protected)) => {
                let offenders = dir_open_grants(&aces, &user_sid);
                Err(SandboxError::BaseDirUnsafe {
                    path: root.to_path_buf(),
                    reason: format!(
                        "the filesystem dropped or widened the private DACL on creation \
                         (protected={protected}, extra_grants={offenders:?})"
                    ),
                    remedy: "Place the directory on an NTFS volume that preserves security \
                             descriptors"
                        .to_string(),
                })
            }
            Err(e) => Err(SandboxError::BaseDirUnsafe {
                path: root.to_path_buf(),
                reason: format!(
                    "the DACL could not be verified after creating the directory ({e})"
                ),
                remedy: "Place the directory on an NTFS volume that preserves security \
                         descriptors"
                    .to_string(),
            }),
        },
        PrivateDirOutcome::Existing => {
            match snap.dacl {
                Ok((aces, _)) => {
                    let open = dir_open_grants(&aces, &user_sid);
                    if let Some(msg) = open_dir_acl_warning(root, &open, &user_sid) {
                        tracing::warn!("{msg}");
                    }
                }
                Err(e) => tracing::warn!(
                    path = %root.display(),
                    error = %e,
                    "could not read the DACL of task directory '{}'",
                    root.display()
                ),
            }
            Ok(())
        }
    }
}

/// Why the temp dir that holds the private root (`meta` following links) is unsafe for the user
/// `euid`: it belongs to a user other than root or `euid`, or group or others may write it without
/// the sticky bit. Either way another user could rename the root away and put their own in its
/// place.
#[cfg(unix)]
fn parent_refusal(meta: &std::fs::Metadata, euid: u32) -> Option<String> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if meta.uid() != 0 && meta.uid() != euid {
        return Some(format!(
            "it holds the private sandbox root but belongs to user {}",
            meta.uid()
        ));
    }
    let mode = meta.permissions().mode();
    if mode & 0o022 != 0 && mode & 0o1000 == 0 {
        return Some(format!(
            "it holds the private sandbox root, but its mode is {:o}: writable by other users, \
             without the sticky bit",
            mode & 0o7777
        ));
    }
    None
}

/// Why the private root (`meta` from `symlink_metadata`) is unsafe for the user `euid`: it is a
/// symbolic link, is not a directory, belongs to another user, or has a group or other permission
/// bit.
#[cfg(unix)]
fn root_refusal(meta: &std::fs::Metadata, euid: u32) -> Option<String> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if meta.file_type().is_symlink() {
        return Some("it is a symbolic link".to_string());
    }
    if !meta.is_dir() {
        return Some("it is not a directory".to_string());
    }
    if meta.uid() != euid {
        return Some(format!(
            "it belongs to user {}, not to user {euid} that runs this worker",
            meta.uid()
        ));
    }
    let mode = meta.permissions().mode() & 0o7777;
    if mode & 0o077 != 0 {
        return Some(format!(
            "its mode is {mode:o}: other users can enter it (it must be 700)"
        ));
    }
    None
}

#[cfg(unix)]
pub fn set_permissions(path: &Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
pub fn set_permissions(_path: &Path, _mode: u32) -> std::io::Result<()> {
    Ok(())
}

fn is_real_owned_dir(meta: &std::fs::Metadata) -> bool {
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return false;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if meta.file_attributes() & 0x0400 != 0 {
            return false;
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.uid() != current_euid() {
            return false;
        }
    }
    true
}

fn is_real_owned_file(meta: &std::fs::Metadata) -> bool {
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return false;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if meta.file_attributes() & 0x0400 != 0 {
            return false;
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.uid() != current_euid() {
            return false;
        }
    }
    true
}

fn ensure_locks_dir(base_dir: &Path) -> std::io::Result<PathBuf> {
    #[cfg(windows)]
    ensure_private_dir(base_dir).map_err(|e| match e {
        SandboxError::BaseDirUnusable { source, .. } => source,
        other => std::io::Error::new(std::io::ErrorKind::PermissionDenied, other.to_string()),
    })?;
    #[cfg(not(windows))]
    std::fs::create_dir_all(base_dir)?;
    let base_meta = std::fs::symlink_metadata(base_dir)?;
    if !is_real_owned_dir(&base_meta) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "sandbox base directory is a symbolic link or not owned by the current user",
        ));
    }

    let locks_dir = base_dir.join(".locks");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        match std::fs::DirBuilder::new().mode(0o700).create(&locks_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
    }
    #[cfg(not(unix))]
    {
        match std::fs::create_dir(&locks_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
    }

    let locks_meta = std::fs::symlink_metadata(&locks_dir)?;
    if !is_real_owned_dir(&locks_meta) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "sandbox .locks directory is a symbolic link or not owned by the current user",
        ));
    }
    let _ = set_permissions(&locks_dir, 0o700);
    Ok(locks_dir)
}

fn open_and_try_lock_task(
    base_dir: &Path,
    task_id: TaskId,
) -> std::io::Result<Option<(PathBuf, std::fs::File)>> {
    let locks_dir = ensure_locks_dir(base_dir)?;
    let lock_path = locks_dir.join(format!("{task_id}.lock"));

    if let Ok(meta) = std::fs::symlink_metadata(&lock_path) {
        if !is_real_owned_file(&meta) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "sandbox lock path is a symbolic link or not a regular file owned by the current user",
            ));
        }
    }

    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }

    let file = opts.open(&lock_path)?;
    let file_meta = file.metadata()?;
    if !is_real_owned_file(&file_meta) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "opened sandbox lock descriptor is not a regular file owned by the current user",
        ));
    }

    match file.try_lock() {
        Ok(()) => Ok(Some((lock_path, file))),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(e)) => Err(e),
    }
}

/// An isolated sandbox environment for executing a single task.
pub struct Sandbox {
    task_id: TaskId,
    path: PathBuf,
    lock_path: PathBuf,
    lock_file: Option<std::fs::File>,
    keep_sandboxes: bool,
    destroyed: bool,
}

impl Sandbox {
    /// Creates and initializes a private directory `<base_dir>/<task_id>` with 0o700 permissions,
    /// acquiring `<base_dir>/.locks/<task_id>.lock` before creating `<base_dir>/<task_id>`
    /// (design 0044 D5).
    ///
    /// For the default base on Unix the private root is checked again first, because a temp
    /// cleaner may have removed it since start-up and another user may have created one
    /// (design 0021).
    pub async fn create(config: &SandboxConfig, task_id: TaskId) -> Result<Self, SandboxError> {
        config.ensure_private_root().await?;
        let path = config.base_dir.join(task_id.to_string());
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return Err(SandboxError::AlreadyExists {
                task_id,
                path: path.clone(),
            });
        }

        let (lock_path, lock_file) = match open_and_try_lock_task(&config.base_dir, task_id)
            .map_err(|e| SandboxError::Io { task_id, source: e })?
        {
            Some(pair) => pair,
            None => {
                return Err(SandboxError::AlreadyExists {
                    task_id,
                    path: path.clone(),
                });
            }
        };

        if let Err(e) = tokio::fs::create_dir_all(&path).await {
            let _ = tokio::fs::remove_file(&lock_path).await;
            drop(lock_file);
            return Err(SandboxError::Io { task_id, source: e });
        }

        if let Err(e) = set_permissions(&path, 0o700) {
            warn!(task_id = %task_id, error = %e, "Failed to set 0o700 permissions on sandbox directory");
        }

        debug!(task_id = %task_id, path = %path.display(), "Sandbox initialized");

        Ok(Self {
            task_id,
            path,
            lock_path,
            lock_file: Some(lock_file),
            keep_sandboxes: config.keep_sandboxes,
            destroyed: false,
        })
    }

    /// Absolute filesystem path to the sandbox root.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// TaskId associated with this sandbox.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Writes raw content into the sandbox with path sanitization and 0o600 permissions.
    pub async fn write_file(
        &self,
        rel_path: impl AsRef<Path>,
        content: &[u8],
    ) -> Result<PathBuf, SandboxError> {
        let rel = rel_path.as_ref();
        let target = sanitize_relative_path(&self.path, rel).map_err(|mut err| {
            if let SandboxError::PathTraversal {
                ref mut task_id, ..
            } = err
            {
                *task_id = self.task_id;
            }
            err
        })?;

        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| SandboxError::Io {
                    task_id: self.task_id,
                    source: e,
                })?;
            let _ = set_permissions(parent, 0o700);
        }

        tokio::fs::write(&target, content)
            .await
            .map_err(|e| SandboxError::Io {
                task_id: self.task_id,
                source: e,
            })?;

        if let Err(e) = set_permissions(&target, 0o600) {
            warn!(task_id = %self.task_id, path = %target.display(), error = %e, "Failed to set 0o600 permissions");
        }

        Ok(target)
    }

    /// Writes an executable script with 0o700 permissions.
    pub async fn write_script(&self, name: &str, content: &str) -> Result<PathBuf, SandboxError> {
        let target = self.write_file(name, content.as_bytes()).await?;
        if let Err(e) = set_permissions(&target, 0o700) {
            warn!(task_id = %self.task_id, path = %target.display(), error = %e, "Failed to set 0o700 permissions on script");
        }
        Ok(target)
    }

    /// Materializes staged files directly into the sandbox directory.
    ///
    /// Each sandbox receives its own independent file (`nlink == 1`) written from `staged.data`
    /// so in-place edits or permission changes by a task never affect subsequent tasks.
    pub async fn stage_files(&self, staged_files: &[StagedFile]) -> Result<(), SandboxError> {
        let mut total_bytes: u64 = 0;
        for staged in staged_files {
            if staged.size > MAX_STAGED_FILE_BYTES as u64 {
                return Err(SandboxError::PathTraversal {
                    task_id: self.task_id,
                    path: staged.path.clone(),
                    reason: format!("Staged file size {} exceeds limit of 32 MiB", staged.size),
                });
            }
            total_bytes = total_bytes.saturating_add(staged.size);
            if total_bytes > MAX_AGGREGATE_STAGED_BYTES as u64 {
                return Err(SandboxError::PathTraversal {
                    task_id: self.task_id,
                    path: staged.path.clone(),
                    reason: "Aggregate staged file size exceeds 32 MiB".to_string(),
                });
            }
            if !staged.verify_checksum() {
                return Err(SandboxError::PathTraversal {
                    task_id: self.task_id,
                    path: staged.path.clone(),
                    reason: "Checksum verification failed for staged file".to_string(),
                });
            }
        }

        for staged in staged_files {
            let rel_path = normalize_relative_path_str(&staged.path).map_err(|mut err| {
                if let SandboxError::PathTraversal {
                    ref mut task_id, ..
                } = err
                {
                    *task_id = self.task_id;
                }
                err
            })?;

            let target = self.path.join(&rel_path);
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| SandboxError::Io {
                        task_id: self.task_id,
                        source: e,
                    })?;
            }

            tokio::fs::write(&target, staged.data.as_bytes())
                .await
                .map_err(|e| SandboxError::Io {
                    task_id: self.task_id,
                    source: e,
                })?;

            // Apply executable permissions
            let mode = if staged.executable { 0o755 } else { 0o644 };
            if let Err(e) = set_permissions(&target, mode) {
                warn!(task_id = %self.task_id, path = %target.display(), error = %e, "Failed to set staged file permissions");
            }
        }

        Ok(())
    }

    /// Explicit asynchronous cleanup of the sandbox directory.
    pub async fn destroy(&mut self) -> Result<(), SandboxError> {
        if self.destroyed {
            return Ok(());
        }
        self.destroyed = true;

        if self.keep_sandboxes {
            self.lock_file = None;
            debug!(task_id = %self.task_id, path = %self.path.display(), "Sandbox retained per keep_sandboxes config");
            return Ok(());
        }

        if tokio::fs::try_exists(&self.path).await.unwrap_or(false) {
            tokio::fs::remove_dir_all(&self.path)
                .await
                .map_err(|e| SandboxError::Io {
                    task_id: self.task_id,
                    source: e,
                })?;
        }
        let _ = tokio::fs::remove_file(&self.lock_path).await;
        self.lock_file = None;
        debug!(task_id = %self.task_id, "Sandbox destroyed");
        Ok(())
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        if !self.destroyed {
            if !self.keep_sandboxes {
                if self.path.exists() {
                    let _ = std::fs::remove_dir_all(&self.path);
                }
                let _ = std::fs::remove_file(&self.lock_path);
            }
            self.lock_file = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sandbox_creation_and_permissions() {
        let temp_dir = std::env::temp_dir().join(format!("test_sb_{}", TaskId::new()));
        let config = SandboxConfig::new(&temp_dir);
        let task_id = TaskId::new();

        let mut sandbox = Sandbox::create(&config, task_id).await.unwrap();
        assert!(sandbox.path().exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(sandbox.path()).unwrap();
            let mode = metadata.permissions().mode() & 0o777;
            assert_eq!(mode, 0o700);
        }

        sandbox.destroy().await.unwrap();
        assert!(!sandbox.path().exists());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_sandbox_path_traversal_rejection() {
        let temp_dir = std::env::temp_dir().join(format!("test_sb_{}", TaskId::new()));
        let config = SandboxConfig::new(&temp_dir);
        let task_id = TaskId::new();
        let mut sandbox = Sandbox::create(&config, task_id).await.unwrap();

        // 1. Absolute path
        let abs_res = sandbox.write_file("/etc/passwd", b"bad").await;
        assert!(abs_res.is_err());
        assert!(matches!(
            abs_res.unwrap_err(),
            SandboxError::PathTraversal { .. }
        ));

        // 2. Parent directory traversal
        let parent_res = sandbox.write_file("../../evil.txt", b"bad").await;
        assert!(parent_res.is_err());
        assert!(matches!(
            parent_res.unwrap_err(),
            SandboxError::PathTraversal { .. }
        ));

        // 3. Middle traversal
        let mid_res = sandbox.write_file("sub/../../evil.txt", b"bad").await;
        assert!(mid_res.is_err());
        assert!(matches!(
            mid_res.unwrap_err(),
            SandboxError::PathTraversal { .. }
        ));

        sandbox.destroy().await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_sandbox_nested_file_writing() {
        let temp_dir = std::env::temp_dir().join(format!("test_sb_{}", TaskId::new()));
        let config = SandboxConfig::new(&temp_dir);
        let task_id = TaskId::new();
        let mut sandbox = Sandbox::create(&config, task_id).await.unwrap();

        let path = sandbox
            .write_file("src/nested/mod.rs", b"pub fn hello() {}")
            .await
            .unwrap();
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "pub fn hello() {}");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(&path).unwrap();
            let mode = metadata.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }

        sandbox.destroy().await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_sandbox_script_writing_executable() {
        let temp_dir = std::env::temp_dir().join(format!("test_sb_{}", TaskId::new()));
        let config = SandboxConfig::new(&temp_dir);
        let task_id = TaskId::new();
        let mut sandbox = Sandbox::create(&config, task_id).await.unwrap();

        let script_path = sandbox
            .write_script("run.sh", "#!/bin/sh\necho hello")
            .await
            .unwrap();
        assert!(script_path.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(&script_path).unwrap();
            let mode = metadata.permissions().mode() & 0o777;
            assert_eq!(mode, 0o700);
        }

        sandbox.destroy().await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_sandbox_raii_cleanup() {
        let temp_dir = std::env::temp_dir().join(format!("test_sb_{}", TaskId::new()));
        let config = SandboxConfig::new(&temp_dir);
        let task_id = TaskId::new();
        let sb_path;

        {
            let sandbox = Sandbox::create(&config, task_id).await.unwrap();
            sb_path = sandbox.path().to_path_buf();
            assert!(sb_path.exists());
            // Drop without destroy
        }

        assert!(!sb_path.exists(), "RAII Drop should clean up sandbox dir");
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    fn lookup<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            vars.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn test_default_base_dir_resolution_order() {
        let platform = PathBuf::from("/platform/tmp");
        let nested = |root: &str| {
            PathBuf::from(root)
                .join(default_root_name())
                .join("sandboxes")
        };
        #[cfg(unix)]
        assert_eq!(
            default_root_name(),
            format!("rusty_grid-{}", current_euid()),
            "one default root per user"
        );

        // 3. nothing set: the platform temp dir
        assert_eq!(
            resolve_default_base_dir(lookup(&[]), platform.clone()),
            nested("/platform/tmp")
        );
        // 2. TMPDIR beats the platform temp dir
        assert_eq!(
            resolve_default_base_dir(
                lookup(&[("TMPDIR", "/data/user/0/app/cache")]),
                platform.clone()
            ),
            nested("/data/user/0/app/cache")
        );
        // 1. RUSTY_GRID_SANDBOX_BASE_DIR beats both and is used as the base itself
        assert_eq!(
            resolve_default_base_dir(
                lookup(&[("TMPDIR", "/tmp/x"), (SANDBOX_BASE_DIR_ENV, "/big/disk/sb")]),
                platform.clone()
            ),
            PathBuf::from("/big/disk/sb")
        );
        // empty values do not count
        assert_eq!(
            resolve_default_base_dir(
                lookup(&[("TMPDIR", "  "), (SANDBOX_BASE_DIR_ENV, "")]),
                platform
            ),
            nested("/platform/tmp")
        );
    }

    #[tokio::test]
    async fn test_ensure_usable_creates_the_base_dir_and_leaves_no_probe_behind() {
        let root = std::env::temp_dir().join(format!("test_sb_{}", TaskId::new()));
        let base = root.join("deep").join("base");
        let config = SandboxConfig::new(&base);

        config
            .ensure_usable()
            .await
            .expect("a creatable directory is usable");
        assert!(base.is_dir());
        assert_eq!(
            std::fs::read_dir(&base).unwrap().count(),
            0,
            "the probe file must be removed"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn test_ensure_usable_gives_a_clear_error_for_an_unwritable_base_dir() {
        let root = std::env::temp_dir().join(format!("test_sb_{}", TaskId::new()));
        std::fs::create_dir_all(&root).unwrap();
        // A regular file in the way: nothing can be created below it, whoever runs the test.
        let blocker = root.join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let config = SandboxConfig::new(blocker.join("sandboxes"));

        let err = config
            .ensure_usable()
            .await
            .expect_err("must not be usable");
        assert!(
            matches!(err, SandboxError::BaseDirUnusable { .. }),
            "{err:?}"
        );
        let text = err.to_string();
        assert!(
            text.contains("blocker"),
            "the message must name the directory: {text}"
        );
        assert!(
            text.contains("--sandbox-base-dir") && text.contains("RUSTY_GRID_SANDBOX_BASE_DIR"),
            "the message must say how to fix it: {text}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    /// A default configuration whose temp dir is a fresh directory below the platform temp dir,
    /// as `TMPDIR` would give it. Returns the temp dir, the private root and the configuration.
    #[cfg(unix)]
    fn default_config_in_fresh_temp() -> (PathBuf, PathBuf, SandboxConfig) {
        use std::os::unix::fs::PermissionsExt;
        let temp = std::env::temp_dir().join(format!("test_sb_temp_{}", TaskId::new()));
        std::fs::create_dir_all(&temp).unwrap();
        std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = SandboxConfig::from_env(
            lookup(&[("TMPDIR", temp.to_str().unwrap())]),
            PathBuf::from("/unused/platform/tmp"),
        );
        let root = temp.join(default_root_name());
        (temp, root, config)
    }

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::symlink_metadata(path)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777
    }

    #[cfg(unix)]
    fn assert_unsafe(err: SandboxError, dir: &Path, reason_part: &str) {
        let text = err.to_string();
        assert!(
            matches!(&err, SandboxError::BaseDirUnsafe { path, reason, .. }
                if path == dir && reason.contains(reason_part)),
            "expected BaseDirUnsafe for {} containing {reason_part:?}, got {err:?}",
            dir.display()
        );
        assert!(
            text.contains(&dir.display().to_string())
                && text.contains("--sandbox-base-dir")
                && text.contains("RUSTY_GRID_SANDBOX_BASE_DIR"),
            "the message must name the directory and say how to fix it: {text}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_default_base_is_created_private() {
        let (temp, root, config) = default_config_in_fresh_temp();
        assert_eq!(config.base_dir, root.join("sandboxes"));

        config
            .ensure_usable()
            .await
            .expect("a fresh private root is usable");
        assert_eq!(
            mode_of(&root),
            0o700,
            "the private root must be created 700"
        );
        assert!(config.base_dir.is_dir());

        let _ = std::fs::remove_dir_all(&temp);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_default_base_refuses_an_open_root() {
        use std::os::unix::fs::PermissionsExt;
        let (temp, root, config) = default_config_in_fresh_temp();
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();

        let err = config.ensure_usable().await.expect_err("mode 755 is open");
        assert_unsafe(err, &root, "mode is 755");
        assert_eq!(mode_of(&root), 0o755, "the worker must not change the mode");
        assert!(!config.base_dir.exists(), "nothing may be created inside");

        let _ = std::fs::remove_dir_all(&temp);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_default_base_refuses_a_symlinked_root() {
        let (temp, root, config) = default_config_in_fresh_temp();
        let elsewhere = temp.join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, &root).unwrap();

        let err = config.ensure_usable().await.expect_err("a link is refused");
        assert_unsafe(err, &root, "symbolic link");
        assert_eq!(
            std::fs::read_dir(&elsewhere).unwrap().count(),
            0,
            "nothing may be created behind the link"
        );

        let _ = std::fs::remove_dir_all(&temp);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_default_base_refuses_a_root_that_is_a_file() {
        let (temp, root, config) = default_config_in_fresh_temp();
        std::fs::write(&root, b"x").unwrap();

        let err = config.ensure_usable().await.expect_err("a file is refused");
        assert_unsafe(err, &root, "not a directory");

        let _ = std::fs::remove_dir_all(&temp);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_default_base_refuses_a_root_or_temp_dir_owned_by_another_user() {
        let (temp, root, config) = default_config_in_fresh_temp();
        config.ensure_usable().await.expect("usable");
        // Both directories belong to the test's user. Checking them for any other user stands in
        // for directories that another user created.
        let euid = current_euid();
        let other = euid.wrapping_add(1);

        let root_meta = std::fs::symlink_metadata(&root).unwrap();
        assert_eq!(root_refusal(&root_meta, euid), None);
        let reason = root_refusal(&root_meta, other).expect("a root of another user is refused");
        assert!(
            reason.contains(&format!("belongs to user {euid}")),
            "{reason}"
        );

        let temp_meta = std::fs::metadata(&temp).unwrap();
        assert_eq!(parent_refusal(&temp_meta, euid), None);
        // Run as root, the temp dir belongs to root, which every user may rely on.
        if euid != 0 {
            let reason =
                parent_refusal(&temp_meta, other).expect("a temp dir of another user is refused");
            assert!(
                reason.contains(&format!("belongs to user {euid}")),
                "{reason}"
            );
        }
        // A temp dir that belongs to root, such as /tmp, is fine for every user.
        let slash_meta = std::fs::metadata("/").unwrap();
        assert_eq!(parent_refusal(&slash_meta, other), None);

        let _ = std::fs::remove_dir_all(&temp);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_default_base_refuses_an_open_temp_dir_without_sticky_bit() {
        use std::os::unix::fs::PermissionsExt;
        let (temp, root, config) = default_config_in_fresh_temp();

        // Writable by others, or by the group only: both could rename the root away.
        for mode in [0o777, 0o770] {
            std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(mode)).unwrap();
            let err = config
                .ensure_usable()
                .await
                .expect_err("others could rename the root away");
            assert_unsafe(err, &temp, &format!("mode is {mode:o}"));
            assert!(!root.exists(), "nothing may be created there");
        }

        // The sticky bit keeps others from renaming entries they do not own, as in /tmp.
        std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o1777)).unwrap();
        config
            .ensure_usable()
            .await
            .expect("a sticky temp dir like /tmp is fine");
        assert_eq!(mode_of(&root), 0o700);

        let _ = std::fs::remove_dir_all(&temp);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_sandbox_create_rechecks_the_private_root() {
        let (temp, root, config) = default_config_in_fresh_temp();
        config.ensure_usable().await.expect("usable at start-up");

        // A temp cleaner removed the root after start-up: the next sandbox creates it again, private.
        std::fs::remove_dir_all(&root).unwrap();
        let mut sandbox = Sandbox::create(&config, TaskId::new())
            .await
            .expect("a removed root is created again");
        assert_eq!(mode_of(&root), 0o700);
        sandbox.destroy().await.unwrap();

        // Another user put their own directory there (a link stands in for it): no sandbox.
        std::fs::remove_dir_all(&root).unwrap();
        let elsewhere = temp.join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, &root).unwrap();
        let err = match Sandbox::create(&config, TaskId::new()).await {
            Ok(_) => panic!("a sandbox must not be created behind a replaced root"),
            Err(err) => err,
        };
        assert_unsafe(err, &root, "symbolic link");
        assert_eq!(std::fs::read_dir(&elsewhere).unwrap().count(), 0);

        let _ = std::fs::remove_dir_all(&temp);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_explicit_base_dir_is_used_as_given() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("test_sb_explicit_{}", TaskId::new()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).unwrap();

        // Named by the operator: no private-root check, as before design 0021.
        let config = SandboxConfig::new(&dir);
        config
            .ensure_usable()
            .await
            .expect("an explicit directory is used as given");
        let mut sandbox = Sandbox::create(&config, TaskId::new()).await.unwrap();
        sandbox.destroy().await.unwrap();

        // A default configuration whose base was replaced by another directory skips the check too.
        let (temp, root, mut default_config) = default_config_in_fresh_temp();
        default_config.base_dir = dir.clone();
        default_config
            .ensure_usable()
            .await
            .expect("the private root applies only while the base lies inside it");
        assert!(
            !root.exists(),
            "the unused private root must not be created"
        );

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&temp);
    }

    #[test]
    fn test_base_from_the_variable_has_no_private_root() {
        let config = SandboxConfig::from_env(
            lookup(&[("TMPDIR", "/tmp/x"), (SANDBOX_BASE_DIR_ENV, "/big/disk/sb")]),
            PathBuf::from("/platform/tmp"),
        );
        assert_eq!(config.base_dir, PathBuf::from("/big/disk/sb"));
        assert_eq!(config.private_root, None);
        assert_eq!(SandboxConfig::new("/srv/sb").private_root, None);
        #[cfg(unix)]
        assert_eq!(
            SandboxConfig::from_env(lookup(&[("TMPDIR", "/tmp/x")]), PathBuf::from("/p"))
                .private_root,
            Some(PathBuf::from("/tmp/x").join(default_root_name()))
        );
    }

    #[tokio::test]
    async fn test_sandbox_keep_sandboxes() {
        let temp_dir = std::env::temp_dir().join(format!("test_sb_{}", TaskId::new()));
        let config = SandboxConfig::new(&temp_dir).with_keep_sandboxes(true);
        let task_id = TaskId::new();
        let sb_path;

        {
            let mut sandbox = Sandbox::create(&config, task_id).await.unwrap();
            sb_path = sandbox.path().to_path_buf();
            assert!(sb_path.exists());
            sandbox.destroy().await.unwrap();
        }

        assert!(
            sb_path.exists(),
            "Sandbox should be kept when keep_sandboxes is true"
        );
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_ensure_usable_removes_legacy_cache_dirs_and_leaves_cache_blobs_alone() {
        let root = std::env::temp_dir().join(format!("test_sb_legacy_cache_{}", TaskId::new()));
        let cache_dir = root.join("cache");
        let objects_dir = cache_dir.join("objects");
        let tmp_dir = cache_dir.join("tmp");
        let blobs_objects_dir = cache_dir.join("blobs").join("objects");
        let blobs_tmp_dir = cache_dir.join("blobs").join("tmp");

        std::fs::create_dir_all(&objects_dir).unwrap();
        std::fs::create_dir_all(&tmp_dir).unwrap();
        std::fs::create_dir_all(&blobs_objects_dir).unwrap();
        std::fs::create_dir_all(&blobs_tmp_dir).unwrap();

        std::fs::write(objects_dir.join("deadbeef"), b"old_object").unwrap();
        std::fs::write(tmp_dir.join("partial.tmp"), b"old_tmp").unwrap();
        let blob_file = blobs_objects_dir.join("cd1234");
        let blob_tmp_file = blobs_tmp_dir.join("inflight.part");
        std::fs::write(&blob_file, b"keep_blob").unwrap();
        std::fs::write(&blob_tmp_file, b"keep_blob_tmp").unwrap();

        let config = SandboxConfig::new(&root);
        config
            .ensure_usable()
            .await
            .expect("ensure_usable succeeds and cleans legacy cache dirs");

        assert!(
            !objects_dir.exists(),
            "cache/objects must be removed at worker start"
        );
        assert!(
            !tmp_dir.exists(),
            "cache/tmp must be removed at worker start"
        );
        assert!(
            blob_file.is_file(),
            "cache/blobs/objects must be left untouched at worker start"
        );
        assert!(
            blob_tmp_file.is_file(),
            "cache/blobs/tmp must be left untouched at worker start"
        );
        assert_eq!(std::fs::read(&blob_file).unwrap(), b"keep_blob");
        assert_eq!(std::fs::read(&blob_tmp_file).unwrap(), b"keep_blob_tmp");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn test_ensure_usable_does_not_delete_through_symlinked_cache_dir() {
        let root = std::env::temp_dir().join(format!("test_sb_symlink_cache_{}", TaskId::new()));
        let outside =
            std::env::temp_dir().join(format!("test_sb_outside_target_{}", TaskId::new()));
        let outside_objects = outside.join("objects");
        let outside_tmp = outside.join("tmp");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&outside_objects).unwrap();
        std::fs::create_dir_all(&outside_tmp).unwrap();

        let outside_obj_file = outside_objects.join("pack-1234");
        let outside_tmp_file = outside_tmp.join("scratch");
        std::fs::write(&outside_obj_file, b"git_object_data").unwrap();
        std::fs::write(&outside_tmp_file, b"scratch_data").unwrap();

        let cache_link = root.join("cache");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, &cache_link).unwrap();
        #[cfg(windows)]
        if std::os::windows::fs::symlink_dir(&outside, &cache_link).is_err() {
            let status = std::process::Command::new("cmd")
                .args(["/C", "mklink", "/J"])
                .arg(&cache_link)
                .arg(&outside)
                .status()
                .expect("run mklink /J");
            assert!(status.success(), "mklink /J must succeed");
        }

        let config = SandboxConfig::new(&root);
        config
            .ensure_usable()
            .await
            .expect("ensure_usable succeeds when cache is a symlink");

        assert!(
            outside_obj_file.is_file(),
            "symlinked cache target objects/ must not be deleted"
        );
        assert!(
            outside_tmp_file.is_file(),
            "symlinked cache target tmp/ must not be deleted"
        );
        assert_eq!(
            std::fs::read(&outside_obj_file).unwrap(),
            b"git_object_data"
        );

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[tokio::test]
    async fn test_staged_file_in_place_mutation_does_not_corrupt_subsequent_tasks() {
        use rusty_grid_core::task::Bytes;
        use tokio::io::AsyncWriteExt;

        let temp_dir = std::env::temp_dir().join(format!("test_sb_bug1_{}", TaskId::new()));
        let config = SandboxConfig::new(&temp_dir);

        let original = b"original\n";
        let staged = StagedFile::new("input.txt", Bytes::from_static(original), false);

        // Task 1 stages and reads the file
        let mut sb1 = Sandbox::create(&config, TaskId::new()).await.unwrap();
        sb1.stage_files(std::slice::from_ref(&staged))
            .await
            .unwrap();
        let file1 = sb1.path().join("input.txt");
        assert_eq!(tokio::fs::read(&file1).await.unwrap(), original);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(std::fs::metadata(&file1).unwrap().nlink(), 1);
        }

        // Task 2 stages the same file and appends in place
        let mut sb2 = Sandbox::create(&config, TaskId::new()).await.unwrap();
        sb2.stage_files(std::slice::from_ref(&staged))
            .await
            .unwrap();
        let file2 = sb2.path().join("input.txt");
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(std::fs::metadata(&file2).unwrap().nlink(), 1);
        }
        let mut f = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&file2)
            .await
            .unwrap();
        f.write_all(b"appended\n").await.unwrap();
        f.flush().await.unwrap();
        drop(f);
        assert_eq!(
            tokio::fs::read(&file2).await.unwrap(),
            b"original\nappended\n"
        );

        // Task 3 stages the same file and must still read the original bytes with nlink == 1
        let mut sb3 = Sandbox::create(&config, TaskId::new()).await.unwrap();
        sb3.stage_files(std::slice::from_ref(&staged))
            .await
            .unwrap();
        let file3 = sb3.path().join("input.txt");
        assert_eq!(tokio::fs::read(&file3).await.unwrap(), original);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(std::fs::metadata(&file3).unwrap().nlink(), 1);
        }
        assert!(
            !temp_dir.join("cache").join("objects").exists(),
            "inline stage_files must not populate cache/objects"
        );

        sb1.destroy().await.unwrap();
        sb2.destroy().await.unwrap();
        sb3.destroy().await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    /// Design 0044 D5 (1): `ensure_usable` removes unlocked leftover `<base_dir>/<uuid>`
    /// directories and their `<base_dir>/.locks/<uuid>.lock` files (plus orphan lock files) when
    /// `keep_sandboxes == false`.
    #[tokio::test]
    async fn test_ensure_usable_sweeps_unlocked_leftover_uuid_sandboxes_and_locks() {
        let temp = tempfile::tempdir().unwrap();
        let base_dir = temp.path().join("sandboxes");
        let config = SandboxConfig::new(&base_dir);

        // Simulate two crashed task sandboxes whose lock files are no longer held by any process,
        // plus one orphan .lock file whose directory was already removed.
        let crashed_a = TaskId::new();
        let crashed_b = TaskId::new();
        let orphan_lock_id = TaskId::new();

        let dir_a = base_dir.join(crashed_a.to_string());
        let dir_b = base_dir.join(crashed_b.to_string());
        let locks_dir = base_dir.join(".locks");
        tokio::fs::create_dir_all(&dir_a).await.unwrap();
        tokio::fs::write(dir_a.join("partial.out"), b"leftover")
            .await
            .unwrap();
        tokio::fs::create_dir_all(&dir_b).await.unwrap();
        tokio::fs::create_dir_all(&locks_dir).await.unwrap();

        let lock_a = locks_dir.join(format!("{crashed_a}.lock"));
        let orphan_lock = locks_dir.join(format!("{orphan_lock_id}.lock"));
        tokio::fs::write(&lock_a, b"").await.unwrap();
        tokio::fs::write(&orphan_lock, b"").await.unwrap();

        config.ensure_usable().await.unwrap();

        assert!(!dir_a.exists(), "unlocked leftover sandbox A must be swept");
        assert!(!dir_b.exists(), "unlocked leftover sandbox B must be swept");
        assert!(
            !lock_a.exists(),
            "lock file for swept sandbox A must be removed"
        );
        assert!(!orphan_lock.exists(), "orphan lock file must be removed");
        assert!(locks_dir.is_dir(), ".locks directory itself must remain");
    }

    /// Design 0044 D5 (2): `ensure_usable` leaves a live `Sandbox` untouched even when a task
    /// deletes **everything** inside `<base_dir>/<task_id>` and a second `SandboxConfig` sharing
    /// the same `base_dir` runs `ensure_usable`.
    #[tokio::test]
    async fn test_ensure_usable_spares_live_sandbox_even_when_emptied() {
        let temp = tempfile::tempdir().unwrap();
        let base_dir = temp.path().join("sandboxes");
        let config_worker_1 = SandboxConfig::new(&base_dir);
        config_worker_1.ensure_usable().await.unwrap();

        let live_id = TaskId::new();
        let mut live_sb = Sandbox::create(&config_worker_1, live_id).await.unwrap();
        live_sb.write_file("work.txt", b"in-flight").await.unwrap();

        // Simulate a task deleting everything inside its own `<base_dir>/<task_id>` directory
        // (e.g. `rm -rf * .*`). Because the advisory lock lives in `<base_dir>/.locks/<task_id>.lock`,
        // the lock remains held.
        let mut entries = tokio::fs::read_dir(live_sb.path()).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let p = entry.path();
            if p.is_dir() {
                tokio::fs::remove_dir_all(&p).await.unwrap();
            } else {
                tokio::fs::remove_file(&p).await.unwrap();
            }
        }
        assert!(
            std::fs::read_dir(live_sb.path()).unwrap().next().is_none(),
            "live sandbox directory is completely empty"
        );

        // Also place an unlocked crashed UUID sandbox alongside the live one to prove the sweep
        // actually ran.
        let crashed_id = TaskId::new();
        let crashed_dir = base_dir.join(crashed_id.to_string());
        tokio::fs::create_dir_all(&crashed_dir).await.unwrap();

        // A second worker sharing the same `base_dir` starts up and runs `ensure_usable`.
        let config_worker_2 = SandboxConfig::new(&base_dir);
        config_worker_2.ensure_usable().await.unwrap();

        assert!(
            !crashed_dir.exists(),
            "unlocked crashed sandbox must be swept"
        );
        assert!(
            live_sb.path().is_dir(),
            "live sandbox directory must survive even when completely emptied"
        );
        assert!(
            base_dir
                .join(".locks")
                .join(format!("{live_id}.lock"))
                .is_file(),
            "live sandbox external lock file must survive"
        );

        // The live task can still write outputs and clean up normally.
        live_sb.write_file("result.txt", b"done").await.unwrap();
        live_sb.destroy().await.unwrap();
        assert!(!base_dir.join(live_id.to_string()).exists());
    }

    /// Design 0044 D5 (3): `ensure_usable` leaves `<base_dir>/.locks`, `<base_dir>/cache/blobs`,
    /// `worker.id`, non-UUID directories, regular files with UUID names, and symbolic links
    /// untouched.
    #[tokio::test]
    async fn test_ensure_usable_leaves_locks_cache_blobs_worker_id_non_uuid_and_symlinks_untouched()
    {
        let temp = tempfile::tempdir().unwrap();
        let base_dir = temp.path().join("sandboxes");
        let outside_dir = temp.path().join("outside_target");
        tokio::fs::create_dir_all(&outside_dir).await.unwrap();
        tokio::fs::write(outside_dir.join(" precious.txt"), b"keep")
            .await
            .unwrap();

        let locks_dir = base_dir.join(".locks");
        tokio::fs::create_dir_all(&locks_dir).await.unwrap();
        let blobs_dir = base_dir.join("cache").join("blobs");
        tokio::fs::create_dir_all(&blobs_dir).await.unwrap();
        let blob_file = blobs_dir.join("deadbeef");
        tokio::fs::write(&blob_file, b"cached-blob").await.unwrap();

        let worker_id_file = base_dir.join("worker.id");
        tokio::fs::write(&worker_id_file, b"11111111-1111-1111-1111-111111111111")
            .await
            .unwrap();

        let custom_dir = base_dir.join("not-a-uuid-dir");
        tokio::fs::create_dir_all(&custom_dir).await.unwrap();

        let uuid_file = base_dir.join(TaskId::new().to_string());
        tokio::fs::write(&uuid_file, b"regular-file-with-uuid-name")
            .await
            .unwrap();

        #[cfg(unix)]
        let symlink_uuid = {
            let link_path = base_dir.join(TaskId::new().to_string());
            std::os::unix::fs::symlink(&outside_dir, &link_path).unwrap();
            link_path
        };

        let config = SandboxConfig::new(&base_dir);
        config.ensure_usable().await.unwrap();

        assert!(base_dir.join(".locks").is_dir());
        assert!(blob_file.is_file());
        assert!(worker_id_file.is_file());
        assert!(custom_dir.is_dir());
        assert!(uuid_file.is_file());
        assert!(outside_dir.join(" precious.txt").is_file());
        #[cfg(unix)]
        assert!(
            std::fs::symlink_metadata(&symlink_uuid)
                .unwrap()
                .file_type()
                .is_symlink(),
            "UUID-named symlink must not be followed or removed"
        );
    }

    /// Design 0044 D5 (4): `ensure_usable` removes nothing when `keep_sandboxes == true`, and a
    /// subsequent `ensure_usable` with `keep_sandboxes == false` sweeps those retained unlocked
    /// sandboxes.
    #[tokio::test]
    async fn test_ensure_usable_keep_sandboxes_retains_and_subsequent_default_sweeps() {
        let temp = tempfile::tempdir().unwrap();
        let base_dir = temp.path().join("sandboxes");

        let keep_config = SandboxConfig::new(&base_dir).with_keep_sandboxes(true);
        keep_config.ensure_usable().await.unwrap();

        let task_id = TaskId::new();
        let mut sb = Sandbox::create(&keep_config, task_id).await.unwrap();
        let sb_path = sb.path().to_path_buf();
        sb.write_file("debug.log", b"trace").await.unwrap();
        sb.destroy().await.unwrap();
        assert!(
            sb_path.is_dir(),
            "destroy() with keep_sandboxes == true must retain the sandbox directory"
        );

        // Restarting with keep_sandboxes == true must also leave retained sandboxes intact.
        keep_config.ensure_usable().await.unwrap();
        assert!(
            sb_path.is_dir(),
            "ensure_usable() with keep_sandboxes == true must not sweep retained sandboxes"
        );

        // Restarting without --keep-sandboxes (keep_sandboxes == false) sweeps the retained
        // unlocked sandbox directory and its lock file.
        let default_config = SandboxConfig::new(&base_dir).with_keep_sandboxes(false);
        default_config.ensure_usable().await.unwrap();
        assert!(
            !sb_path.exists(),
            "subsequent ensure_usable() with keep_sandboxes == false must sweep retained unlocked sandboxes"
        );
        assert!(
            !base_dir
                .join(".locks")
                .join(format!("{task_id}.lock"))
                .exists(),
            "lock file of swept retained sandbox must be removed"
        );
    }
}

#[cfg(all(test, windows))]
mod windows_private_dir_tests {
    use super::{
        ensure_locks_dir, ensure_private_dir, ensure_private_dir_windows_with, Sandbox,
        SandboxConfig, SandboxError,
    };
    use rusty_grid_core::private_file::{
        create_or_open_private_dir_all_with_hooks, current_user_sid, dir_open_grants,
        inspect_dir_security_no_follow, read_dacl, DirSecuritySnapshot, PrivateDirOutcome,
        SID_ALL_APP_PACKAGES, SID_ANONYMOUS_LOGON, SID_NETWORK, SID_REMOTE_DESKTOP_USERS,
    };
    use rusty_grid_core::task::TaskId;
    use std::path::Path;
    use std::process::Command;
    use std::sync::{Arc, Mutex};

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

    fn make_junction(link: &Path, target: &Path) {
        let status = Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(link)
            .arg(target)
            .output()
            .expect("mklink /J")
            .status;
        assert!(status.success(), "mklink /J failed");
    }

    fn capture_thread_warnings<R>(f: impl FnOnce() -> R) -> (R, Vec<String>) {
        #[derive(Default)]
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "message" {
                    self.0 = value.to_string();
                }
            }
        }
        struct WarnCollector {
            warnings: Arc<Mutex<Vec<String>>>,
            thread_id: std::thread::ThreadId,
        }
        impl tracing::Subscriber for WarnCollector {
            fn register_callsite(
                &self,
                metadata: &'static tracing::Metadata<'static>,
            ) -> tracing::subscriber::Interest {
                if *metadata.level() <= tracing::Level::WARN {
                    tracing::subscriber::Interest::always()
                } else {
                    tracing::subscriber::Interest::never()
                }
            }
            fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
                *metadata.level() <= tracing::Level::WARN
            }
            fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
                Some(tracing::metadata::LevelFilter::WARN)
            }
            fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {
            }
            fn event(&self, event: &tracing::Event<'_>) {
                if *event.metadata().level() == tracing::Level::WARN
                    && std::thread::current().id() == self.thread_id
                {
                    let mut v = Visitor::default();
                    event.record(&mut v);
                    self.warnings.lock().unwrap().push(v.0);
                }
            }
            fn enter(&self, _span: &tracing::span::Id) {}
            fn exit(&self, _span: &tracing::span::Id) {}
        }

        let warnings = Arc::new(Mutex::new(Vec::new()));
        let thread_id = std::thread::current().id();
        let dispatch = tracing::Dispatch::new(WarnCollector {
            warnings: Arc::clone(&warnings),
            thread_id,
        });
        let _second_dispatch = tracing::Dispatch::new(WarnCollector {
            warnings: Arc::clone(&warnings),
            thread_id,
        });
        let out = tracing::dispatcher::with_default(&dispatch, || {
            tracing::callsite::rebuild_interest_cache();
            f()
        });
        let collected = warnings.lock().unwrap().clone();
        (out, collected)
    }

    #[test]
    fn a_missing_dir_is_created_with_the_protected_owner_only_dacl() {
        let tmp = open_parent();
        let base = tmp.path().join("work").join("sandboxes");
        ensure_private_dir(&base).unwrap();
        assert!(base.is_dir());
        let user = current_user_sid().unwrap();
        let (aces, protected) = read_dacl(&base).unwrap();
        assert!(
            protected,
            "the DACL must not inherit entries from its parent"
        );
        assert!(dir_open_grants(&aces, &user).is_empty(), "{aces:?}");
        assert!(
            aces.iter().any(|a| a.sid.eq_ignore_ascii_case(&user)),
            "{aces:?}"
        );
        // A task folder created inside inherits only the private entries.
        let task = base.join("task");
        std::fs::create_dir(&task).unwrap();
        let (task_aces, _) = read_dacl(&task).unwrap();
        assert!(
            dir_open_grants(&task_aces, &user).is_empty(),
            "{task_aces:?}"
        );
    }

    #[test]
    fn an_existing_dir_is_used_as_it_is() {
        let tmp = tempfile::tempdir().unwrap();
        let before = read_dacl(tmp.path()).unwrap();
        ensure_private_dir(tmp.path()).unwrap();
        assert_eq!(read_dacl(tmp.path()).unwrap(), before);
    }

    #[tokio::test]
    async fn s1_junction_and_file_bases_are_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let elsewhere = tmp.path().join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        let junction = tmp.path().join("junction_base");
        make_junction(&junction, &elsewhere);

        let err = SandboxConfig::new(&junction)
            .ensure_usable()
            .await
            .expect_err("a junction base must be refused");
        assert!(
            matches!(&err, SandboxError::BaseDirUnsafe { path, reason, .. }
                if path == &junction && reason.contains("reparse point")),
            "{err:?}"
        );
        assert_eq!(
            std::fs::read_dir(&elsewhere).unwrap().count(),
            0,
            "nothing may be created inside the junction target"
        );

        // Even a dangling junction (target removed) is refused as BaseDirUnsafe, never overwritten.
        std::fs::remove_dir(&elsewhere).unwrap();
        let err_dangling = ensure_private_dir(&junction).expect_err("dangling junction refused");
        assert!(
            matches!(&err_dangling, SandboxError::BaseDirUnsafe { reason, .. }
                if reason.contains("reparse point")),
            "{err_dangling:?}"
        );

        // A regular file at the base path is refused as BaseDirUnsafe.
        let file_base = tmp.path().join("file_base");
        std::fs::write(&file_base, b"not a dir").unwrap();
        let err_file = ensure_private_dir(&file_base).expect_err("file base refused");
        assert!(
            matches!(&err_file, SandboxError::BaseDirUnsafe { path, reason, .. }
                if path == &file_base && reason.contains("not a directory")),
            "{err_file:?}"
        );
    }

    #[test]
    fn s1_base_owned_by_untrusted_sid_is_refused() {
        // 1. Real read-only check on `%SystemRoot%` (`C:\Windows`), which is owned by
        //    `NT SERVICE\TrustedInstaller` (`S-1-5-80-...`), outside {user, SYSTEM, Administrators}.
        if let Some(sys_root) = std::env::var_os("SystemRoot") {
            let sys_path = std::path::PathBuf::from(sys_root);
            if let Ok(snap) = inspect_dir_security_no_follow(&sys_path) {
                if let Ok(ref owner) = snap.owner_sid {
                    let me = current_user_sid().unwrap();
                    if !rusty_grid_core::private_file::is_trusted_owner_sid(owner, &me) {
                        let err = ensure_private_dir(&sys_path)
                            .expect_err("TrustedInstaller-owned directory must be refused");
                        assert!(
                            matches!(&err, SandboxError::BaseDirUnsafe { path, reason, .. }
                                if path == &sys_path && reason.contains(owner)),
                            "{err:?}"
                        );
                    }
                }
            }
        }

        // 2. Deterministic check for another local user SID (`S-1-5-21-1-2-3-1002`) without elevation.
        let tmp = tempfile::tempdir().unwrap();
        let me = "S-1-5-21-1-2-3-1001";
        let other_local = "S-1-5-21-1-2-3-1002";
        let err = ensure_private_dir_windows_with(
            tmp.path(),
            || Ok(me.to_string()),
            |_| Ok(PrivateDirOutcome::Existing),
            |_| {
                Ok(DirSecuritySnapshot {
                    is_reparse_point: false,
                    is_dir: true,
                    persistent_acls: true,
                    owner_sid: Ok(other_local.to_string()),
                    dacl: Ok((Vec::new(), true)),
                })
            },
        )
        .expect_err("foreign local user owner must be refused");
        assert!(
            matches!(&err, SandboxError::BaseDirUnsafe { reason, .. }
                if reason.contains(other_local)),
            "{err:?}"
        );
    }

    #[test]
    fn s1_existing_dir_warns_for_untrusted_sids_and_when_dacl_cannot_be_read() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("existing_open");
        ensure_private_dir(&dir).unwrap();
        let user = current_user_sid().unwrap();

        // Grant sensitive access to SIDs outside the old 5-group list:
        // NETWORK, ANONYMOUS LOGON, Remote Desktop Users, and ALL APPLICATION PACKAGES.
        let status = Command::new("icacls")
            .arg(&dir)
            .args([
                "/grant",
                &format!("*{SID_NETWORK}:(OI)(CI)R"),
                &format!("*{SID_ANONYMOUS_LOGON}:(OI)(CI)R"),
                &format!("*{SID_REMOTE_DESKTOP_USERS}:(OI)(CI)M"),
                &format!("*{SID_ALL_APP_PACKAGES}:(OI)(CI)RX"),
            ])
            .status()
            .expect("icacls");
        assert!(status.success());

        let (res, warnings) = capture_thread_warnings(|| ensure_private_dir(&dir));
        res.expect("existing trusted-owner directory warns rather than refusing");
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        let msg = &warnings[0];
        for expected in [
            "NETWORK",
            "ANONYMOUS LOGON",
            "BUILTIN\\Remote Desktop Users",
            "ALL APPLICATION PACKAGES",
            &format!("*{user}:(OI)(CI)F"),
            "*S-1-5-18:(OI)(CI)F",
            "*S-1-5-32-544:(OI)(CI)F",
        ] {
            assert!(
                msg.contains(expected),
                "expected {expected:?} in warning: {msg}"
            );
        }

        // Unreadable DACL on an existing trusted-owner directory must emit WARN (not DEBUG).
        let (res_unreadable, warn_unreadable) = capture_thread_warnings(|| {
            ensure_private_dir_windows_with(
                &dir,
                || Ok(user.clone()),
                |_| Ok(PrivateDirOutcome::Existing),
                |_| {
                    Ok(DirSecuritySnapshot {
                        is_reparse_point: false,
                        is_dir: true,
                        persistent_acls: true,
                        owner_sid: Ok(user.clone()),
                        dacl: Err(std::io::Error::from_raw_os_error(5)),
                    })
                },
            )
        });
        res_unreadable.unwrap();
        assert_eq!(warn_unreadable.len(), 1, "{warn_unreadable:?}");
        assert!(
            warn_unreadable[0].contains("could not read the DACL of task directory"),
            "{warn_unreadable:?}"
        );
    }

    #[tokio::test]
    async fn s2_per_task_recheck_runs_and_deleted_base_is_recreated_privately() {
        let parent = open_parent();
        let base = parent.path().join("sandboxes");
        let config = SandboxConfig::new(&base);
        config.ensure_usable().await.unwrap();
        let user = current_user_sid().unwrap();

        // 1. Delete the base after startup: `Sandbox::create` must recreate it via
        //    `ensure_private_dir` (protected owner-only DACL), NOT `create_dir_all` (inherited).
        std::fs::remove_dir_all(&base).unwrap();
        let mut sb = Sandbox::create(&config, TaskId::new()).await.unwrap();
        let (aces, protected) = read_dacl(&base).unwrap();
        assert!(protected, "recreated base must have a protected DACL");
        assert!(dir_open_grants(&aces, &user).is_empty(), "{aces:?}");
        sb.destroy().await.unwrap();

        // 2. Delete the base and call `ensure_locks_dir` directly: still recreated privately.
        std::fs::remove_dir_all(&base).unwrap();
        ensure_locks_dir(&base).unwrap();
        let (locks_base_aces, locks_base_protected) = read_dacl(&base).unwrap();
        assert!(locks_base_protected);
        assert!(
            dir_open_grants(&locks_base_aces, &user).is_empty(),
            "{locks_base_aces:?}"
        );

        // 3. Replace the base with a junction after startup: `Sandbox::create` refuses it.
        std::fs::remove_dir_all(&base).unwrap();
        let elsewhere = parent.path().join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        make_junction(&base, &elsewhere);
        let err = match Sandbox::create(&config, TaskId::new()).await {
            Ok(_) => panic!("Sandbox::create must refuse a junction-replaced base"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, SandboxError::BaseDirUnsafe { path, reason, .. }
                if path == &base && reason.contains("reparse point")),
            "{err:?}"
        );
        assert_eq!(std::fs::read_dir(&elsewhere).unwrap().count(), 0);
    }

    #[test]
    fn s3_creation_race_runs_the_existing_directory_checks() {
        let parent = open_parent();
        let user = current_user_sid().unwrap();

        // 1. Race lost to a junction (`CreateDirectoryW` hits real `ERROR_ALREADY_EXISTS`): refused.
        let elsewhere = parent.path().join("race_target");
        std::fs::create_dir(&elsewhere).unwrap();
        let race_junction = parent.path().join("race_junction");
        let err_j = ensure_private_dir_windows_with(
            &race_junction,
            current_user_sid,
            |p| {
                create_or_open_private_dir_all_with_hooks(
                    p,
                    |target| make_junction(target, &elsewhere),
                    |_| {},
                )
            },
            inspect_dir_security_no_follow,
        )
        .expect_err("junction winning creation race must be refused");
        assert!(
            matches!(&err_j, SandboxError::BaseDirUnsafe { reason, .. }
                if reason.contains("reparse point")),
            "{err_j:?}"
        );

        // 2. Race lost to another user's directory (`ERROR_ALREADY_EXISTS`): refused.
        let race_foreign = parent.path().join("race_foreign");
        let err_f = ensure_private_dir_windows_with(
            &race_foreign,
            || Ok(user.clone()),
            |p| {
                create_or_open_private_dir_all_with_hooks(
                    p,
                    |target| std::fs::create_dir(target).unwrap(),
                    |_| {},
                )
            },
            |p| {
                let mut snap = inspect_dir_security_no_follow(p)?;
                snap.owner_sid = Ok("S-1-5-21-1-2-3-1002".to_string());
                Ok(snap)
            },
        )
        .expect_err("foreign-owned directory winning creation race must be refused");
        assert!(
            matches!(&err_f, SandboxError::BaseDirUnsafe { reason, .. }
                if reason.contains("S-1-5-21-1-2-3-1002")),
            "{err_f:?}"
        );

        // 3. Race lost to same user's open directory (`ERROR_ALREADY_EXISTS`): checked and warns.
        let race_open = parent.path().join("race_open");
        let (res_open, warnings) = capture_thread_warnings(|| {
            ensure_private_dir_windows_with(
                &race_open,
                current_user_sid,
                |p| {
                    create_or_open_private_dir_all_with_hooks(
                        p,
                        |target| std::fs::create_dir(target).unwrap(),
                        |_| {},
                    )
                },
                inspect_dir_security_no_follow,
            )
        });
        res_open.unwrap();
        assert_eq!(
            warnings.len(),
            1,
            "open winner must still trigger ACL warning"
        );
    }

    #[test]
    fn s4_non_acl_volume_and_dropped_security_descriptor_are_refused() {
        let parent = open_parent();
        let user = current_user_sid().unwrap();

        // 1. Volume without `FILE_PERSISTENT_ACLS` (FAT / exFAT) is refused.
        let fat_dir = parent.path().join("simulated_exfat");
        let err_fat = ensure_private_dir_windows_with(
            &fat_dir,
            || Ok(user.clone()),
            |p| create_or_open_private_dir_all_with_hooks(p, |_| {}, |_| {}),
            |p| {
                let mut snap = inspect_dir_security_no_follow(p)?;
                snap.persistent_acls = false;
                Ok(snap)
            },
        )
        .expect_err("volume without persistent ACLs must be refused");
        assert!(
            matches!(&err_fat, SandboxError::BaseDirUnsafe { reason, .. }
                if reason.contains("persistent ACLs")),
            "{err_fat:?}"
        );

        // 2. Cloud/virtual volume that reports `FILE_PERSISTENT_ACLS` yet silently drops the
        //    protected descriptor on `CreateDirectoryW` is caught by the post-create read-back.
        let dropped_sd_dir = parent.path().join("dropped_sd");
        let err_drop = ensure_private_dir_windows_with(
            &dropped_sd_dir,
            current_user_sid,
            |p| {
                create_or_open_private_dir_all_with_hooks(
                    p,
                    |_| {},
                    |created| {
                        let status = Command::new("icacls")
                            .arg(created)
                            .arg("/inheritance:e")
                            .status()
                            .expect("icacls /inheritance:e");
                        assert!(status.success());
                    },
                )
            },
            inspect_dir_security_no_follow,
        )
        .expect_err("dropped security descriptor on newly created dir must be refused");
        assert!(
            matches!(&err_drop, SandboxError::BaseDirUnsafe { reason, .. }
                if reason.contains("dropped or widened the private DACL")),
            "{err_drop:?}"
        );

        // 3. Live read-only check when the test's working directory itself is on a non-ACL volume
        //    (e.g. exFAT): `current_dir()` already exists so nothing is written there.
        if let Ok(cwd) = std::env::current_dir() {
            if let Ok(snap) = inspect_dir_security_no_follow(&cwd) {
                if !snap.persistent_acls && !snap.is_reparse_point && snap.is_dir {
                    let err_live = ensure_private_dir(&cwd)
                        .expect_err("live exFAT/FAT directory must be refused");
                    assert!(
                        matches!(&err_live, SandboxError::BaseDirUnsafe { path, reason, .. }
                            if path == &cwd && reason.contains("persistent ACLs")),
                        "{err_live:?}"
                    );
                }
            }
        }
    }
}
