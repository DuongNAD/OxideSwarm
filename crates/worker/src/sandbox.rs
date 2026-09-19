//! Sandbox directory management, path traversal isolation, and POSIX permissions.

use std::path::{Component, Path, PathBuf};
use tracing::{debug, warn};

use rusty_grid_core::task::{TaskId, TaskSpec};

/// Configuration options for worker sandbox environment.
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    /// Base directory where per-task sandbox folders are placed.
    pub base_dir: PathBuf,
    /// If true, sandbox directories are preserved after task completion for debugging.
    pub keep_sandboxes: bool,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            base_dir: std::env::temp_dir().join("rusty_grid").join("sandboxes"),
            keep_sandboxes: false,
        }
    }
}

impl SandboxConfig {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            keep_sandboxes: false,
        }
    }

    pub fn with_keep_sandboxes(mut self, keep: bool) -> Self {
        self.keep_sandboxes = keep;
        self
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

    Ok(base_dir.join(rel_path))
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
    pub async fn create(config: &SandboxConfig, task_id: TaskId) -> Result<Self, SandboxError> {
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
}
