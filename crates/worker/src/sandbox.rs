//! Sandbox directory management, path traversal isolation, and POSIX permissions.

use std::path::{Component, Path, PathBuf};
use tracing::{debug, warn};

use rusty_grid_core::task::{
    StagedFile, TaskId, TaskSpec, MAX_AGGREGATE_STAGED_BYTES, MAX_STAGED_FILE_BYTES,
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
    /// private to this user (design 0021). `None` for a directory the operator named, which is
    /// used as given. Checked only while `base_dir` lies inside it.
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
    /// design 0021).
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
        Ok(())
    }

    /// Creates and checks the private root of the default base (see `ensure_private_dir`) while
    /// `base_dir` lies inside it, and does nothing for a directory the operator named. Call it
    /// before anything touches the base: `WorkerClient` does when a task is assigned and again
    /// before it fetches the task's input files, and `Sandbox::create` does too (design 0021).
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
    /// Android bridge's start check).
    pub fn ensure_private_root_blocking(&self) -> Result<(), SandboxError> {
        match self.checked_private_root() {
            Some(root) => ensure_private_dir(root),
            None => Ok(()),
        }
    }

    fn checked_private_root(&self) -> Option<&Path> {
        self.private_root
            .as_deref()
            .filter(|root| self.base_dir.starts_with(root))
    }

    /// Removes legacy `<base_dir>/cache/objects` and `<base_dir>/cache/tmp` directories left
    /// behind by older worker builds.
    ///
    /// Refuses to remove anything unless `<base_dir>` and `<base_dir>/cache` are real directories
    /// (not symbolic links, and on Unix owned by the current effective user), and deletes only the
    /// direct `objects` and `tmp` subdirectories of `<base_dir>/cache` without following links.
    /// Logs a warning on failure and leaves `<base_dir>/cache/blobs` alone.
    pub async fn remove_legacy_staging_cache(&self) {
        let is_real_owned_dir = |meta: &std::fs::Metadata| -> bool {
            if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                return false;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if meta.uid() != current_euid() {
                    return false;
                }
            }
            true
        };

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
/// changed, because another user may already have put links into one that was open. Does nothing
/// on systems without Unix modes.
fn ensure_private_dir(root: &Path) -> Result<(), SandboxError> {
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
    #[cfg(not(unix))]
    {
        let _ = root;
        Ok(())
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
            "its mode is {mode:o}, so other users can enter it (it must be 700)"
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

/// An isolated sandbox environment for executing a single task.
pub struct Sandbox {
    task_id: TaskId,
    path: PathBuf,
    keep_sandboxes: bool,
    destroyed: bool,
}

impl Sandbox {
    /// Creates and initializes a private directory `<base_dir>/<task_id>` with 0o700 permissions.
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

        tokio::fs::create_dir_all(&path)
            .await
            .map_err(|e| SandboxError::Io { task_id, source: e })?;

        if let Err(e) = set_permissions(&path, 0o700) {
            warn!(task_id = %task_id, error = %e, "Failed to set 0o700 permissions on sandbox directory");
        }

        debug!(task_id = %task_id, path = %path.display(), "Sandbox initialized");

        Ok(Self {
            task_id,
            path,
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

    /// Materializes files necessary for a given TaskSpec variant.
    pub async fn prepare_spec(&self, spec: &TaskSpec) -> Result<(), SandboxError> {
        match spec {
            TaskSpec::ShellScript { script, .. } => {
                self.write_script("task_script.sh", script).await?;
            }
            TaskSpec::RustCompilation { source_files, .. } => {
                for (rel_path, content) in source_files {
                    self.write_file(rel_path, content.as_bytes()).await?;
                }
            }
            _ => {}
        }
        Ok(())
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
        debug!(task_id = %self.task_id, "Sandbox destroyed");
        Ok(())
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        if !self.destroyed && !self.keep_sandboxes && self.path.exists() {
            let _ = std::fs::remove_dir_all(&self.path);
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
        std::os::windows::fs::symlink_dir(&outside, &cache_link).unwrap();

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
}
